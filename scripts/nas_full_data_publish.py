#!/usr/bin/env python3
"""Publish one immutable full-data generation to the configured rclone remote.

The input generation is an already-created, fixed local snapshot. This command
validates every local artifact before remote writes, then verifies every remote
object by streaming it back. Only after the global generation index is verified
does it compare-and-update latest.json.
"""
import argparse
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
import fcntl
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import selectors
import stat
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
from datetime import datetime, timezone


GENERATION_ID_RE = re.compile(r"[0-9]{8}T[0-9]{6}Z-[0-9a-f]{8}\Z", re.ASCII)
REMOTE_NAME_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]*\Z", re.ASCII)
SHA256_RE = re.compile(r"[0-9a-f]{64}\Z", re.ASCII)
MODE_FILE_RE = re.compile(r"[a-z0-9_]{1,64}\.sqlite3\Z", re.ASCII)
RULE_FILE_RE = re.compile(r"[A-Za-z0-9_]+\.sqlite3\Z", re.ASCII)
XLSX_FILE_RE = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,200}\.xlsx\Z", re.ASCII)
MAX_CONTROL_BYTES = 32 * 1024 * 1024
MAX_RCLONE_TEXT_BYTES = 64 * 1024
RETRY_LIMIT = 5
STATE_VERSION = 1
UNCLASSIFIED_ANALYSIS_SET = "unclassified"


class PublishError(Exception):
    def __init__(self, category):
        super().__init__(category)
        self.category = category


class TemporaryRemoteError(PublishError):
    pass


def now_utc():
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def valid_generation_id(value):
    if not isinstance(value, str) or not GENERATION_ID_RE.fullmatch(value):
        return False
    try:
        parsed = datetime.strptime(value[:16], "%Y%m%dT%H%M%SZ")
    except ValueError:
        return False
    return parsed.strftime("%Y%m%dT%H%M%SZ") == value[:16]


def valid_sha(value):
    return isinstance(value, str) and bool(SHA256_RE.fullmatch(value))


def parse_utc_timestamp(value):
    if not isinstance(value, str) or not value:
        raise PublishError("PLAN_INVALID")
    if not (value.endswith("Z") or value.endswith("+00:00")):
        raise PublishError("PLAN_INVALID")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00" if value.endswith("Z") else value)
    except ValueError as exc:
        raise PublishError("PLAN_INVALID") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        raise PublishError("PLAN_INVALID")
    return value


def verify_no_symlink_components(path):
    path = Path(path).expanduser()
    if ".." in path.parts:
        raise PublishError("PATH_SAFETY_ERROR")
    absolute = path.absolute()
    for component in (absolute, *absolute.parents):
        try:
            mode = component.lstat().st_mode
        except FileNotFoundError:
            continue
        except OSError as exc:
            raise PublishError("PATH_SAFETY_ERROR") from exc
        if stat.S_ISLNK(mode):
            raise PublishError("PATH_SAFETY_ERROR")


def ensure_private_state_dir(path):
    path = Path(path).expanduser()
    verify_no_symlink_components(path)
    if not path.exists():
        path.mkdir(parents=True, mode=0o700, exist_ok=True)
    verify_no_symlink_components(path)
    try:
        info = path.lstat()
    except OSError as exc:
        raise PublishError("STATE_ERROR") from exc
    if not stat.S_ISDIR(info.st_mode) or info.st_mode & 0o077:
        raise PublishError("STATE_ERROR")
    return path.absolute()


def generation_root(path):
    path = Path(path).expanduser()
    verify_no_symlink_components(path)
    try:
        info = path.lstat()
    except OSError as exc:
        raise PublishError("GENERATION_MISSING") from exc
    if not stat.S_ISDIR(info.st_mode):
        raise PublishError("PATH_SAFETY_ERROR")
    return path.absolute()


def safe_rel_path(value):
    if not isinstance(value, str) or not value or "\\" in value or ":" in value:
        raise PublishError("PATH_SAFETY_ERROR")
    if any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise PublishError("PATH_SAFETY_ERROR")
    if value.startswith("/"):
        raise PublishError("PATH_SAFETY_ERROR")
    parts = value.split("/")
    if any(part in ("", ".", "..") for part in parts):
        raise PublishError("PATH_SAFETY_ERROR")
    pure = PurePosixPath(value)
    if pure.is_absolute() or str(pure) != value:
        raise PublishError("PATH_SAFETY_ERROR")
    return value


def local_path_for(root, rel):
    safe_rel_path(rel)
    current = root
    for part in rel.split("/"):
        current = current / part
        verify_no_symlink_components(current)
    return current


def hash_regular_file(path, *, max_bytes=None):
    verify_no_symlink_components(path)
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
    try:
        fd = os.open(path, flags)
    except OSError as exc:
        raise PublishError("LOCAL_FILE_INVALID") from exc
    digest = hashlib.sha256()
    total = 0
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode):
            raise PublishError("LOCAL_FILE_INVALID")
        with os.fdopen(fd, "rb", closefd=False) as source:
            while True:
                block = source.read(1024 * 1024)
                if not block:
                    break
                total += len(block)
                if max_bytes is not None and total > max_bytes:
                    raise PublishError("CONTROL_FILE_TOO_LARGE")
                digest.update(block)
    finally:
        os.close(fd)
    return total, digest.hexdigest()


def read_control_file(path):
    size, digest = hash_regular_file(path, max_bytes=MAX_CONTROL_BYTES)
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
    try:
        fd = os.open(path, flags)
        with os.fdopen(fd, "rb") as source:
            data = source.read(MAX_CONTROL_BYTES + 1)
    except OSError as exc:
        raise PublishError("LOCAL_FILE_INVALID") from exc
    if len(data) != size or hashlib.sha256(data).hexdigest() != digest:
        raise PublishError("LOCAL_FILE_CHANGED")
    return data, size, digest


def _json_object(data):
    def no_duplicate_pairs(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate json key")
            result[key] = value
        return result

    try:
        value = json.loads(data, object_pairs_hook=no_duplicate_pairs)
    except (UnicodeError, json.JSONDecodeError, ValueError) as exc:
        raise PublishError("METADATA_INVALID") from exc
    if not isinstance(value, dict):
        raise PublishError("METADATA_INVALID")
    return value


def _int(value, *, minimum=0):
    return type(value) is int and value >= minimum


def _safe_mode_rel(value):
    return _safe_slice_rel(value) and value.startswith("slices/by-mode/")


def _safe_rule_rel(value):
    return _safe_slice_rel(value) and value.startswith("slices/by-rule/")


def _safe_slice_rel(value):
    safe_rel_path(value)
    if not value.startswith("slices/"):
        return False
    parts = value.split("/")
    if value in ("slices/manifest.json", "slices/verification.json",
                 "slices/selectors-verification.json"):
        return True
    if len(parts) == 3 and parts[1] == "by-mode":
        return bool(MODE_FILE_RE.fullmatch(parts[2]))
    if len(parts) == 3 and parts[1] == "by-rule":
        return bool(RULE_FILE_RE.fullmatch(parts[2]))
    if (len(parts) == 4 and parts[1] == "shared"
            and bool(re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,160}", parts[2], re.ASCII))
            and bool(re.fullmatch(r"part[0-9]{6}\.sqlite3", parts[3], re.ASCII))):
        return True
    if (len(parts) == 5 and parts[1] == "shared"
            and bool(re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,160}", parts[2], re.ASCII))
            and parts[3] == "value"
            and bool(re.fullmatch(r"part[0-9]{6}\.sqlite3", parts[4], re.ASCII))):
        return True
    if len(parts) >= 3 and parts[1] == "values":
        return all(bool(re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,220}", part, re.ASCII))
                   for part in parts[2:])
    return False


def _safe_xlsx_piece_rel(value):
    safe_rel_path(value)
    return value.startswith("xlsx/") and bool(XLSX_FILE_RE.fullmatch(value.removeprefix("xlsx/")))


def _safe_ascii_manifest_name(value):
    return isinstance(value, str) and bool(re.fullmatch(r"[A-Za-z0-9_.-]{1,220}", value, re.ASCII))


def _expected_remote(generation_id, local):
    if local == "source.sqlite3":
        return f"unified/generations/{generation_id}/archive.sqlite3"
    if local == "source-manifest.json":
        return f"unified/generations/{generation_id}/source-manifest.json"
    if _safe_slice_rel(local):
        suffix = local.removeprefix("slices/")
        if suffix == "manifest.json":
            suffix = "manifest.json"
        return f"slices/generations/{generation_id}/{suffix}"
    if local == "xlsx/manifest.json":
        return f"xlsx-full/generations/{generation_id}/manifest.json"
    if local == "xlsx/index.json":
        return f"xlsx-full/generations/{generation_id}/index.json"
    if local == "xlsx/verification.json":
        return f"xlsx-full/generations/{generation_id}/verification.json"
    if _safe_xlsx_piece_rel(local):
        return f"xlsx-full/generations/{generation_id}/{local.removeprefix('xlsx/')}"
    raise PublishError("PLAN_INVALID")


def _read_plan(root):
    path = local_path_for(root, "generation-plan.json")
    data, size, digest = read_control_file(path)
    return _json_object(data), size, digest


def _load_json_artifact(root, rel):
    data, size, digest = read_control_file(local_path_for(root, rel))
    return _json_object(data), data, size, digest


def _verify_source_manifest(doc, source):
    raw = doc.get("raw_snapshot")
    if not isinstance(raw, dict):
        raise PublishError("SOURCE_MANIFEST_INVALID")
    basename = raw.get("basename")
    if (not isinstance(basename, str) or not basename or basename in (".", "..")
            or "/" in basename or "\\" in basename or any(ord(ch) < 32 for ch in basename)):
        raise PublishError("SOURCE_MANIFEST_INVALID")
    if not _int(raw.get("bytes"), minimum=1) or raw.get("bytes") != source["bytes"]:
        raise PublishError("SOURCE_MANIFEST_INVALID")
    if not valid_sha(raw.get("sha256")) or raw.get("sha256") != source["sha256"]:
        raise PublishError("SOURCE_MANIFEST_INVALID")
    verification = doc.get("verification")
    if (not isinstance(verification, dict)
            or verification.get("sha256_match") is not True
            or verification.get("quick_check") != "ok"):
        raise PublishError("SOURCE_MANIFEST_INVALID")
    if raw.get("quick_check") != "ok":
        raise PublishError("SOURCE_MANIFEST_INVALID")
    if doc.get("storage") != "plaintext" or doc.get("encryption") not in (None,):
        raise PublishError("SOURCE_MANIFEST_INVALID")


def _verify_xlsx_metadata(index, verification, generation_id, source, source_path):
    if index.get("status") != "verified" or index.get("verification_status") != "verified":
        raise PublishError("XLSX_NOT_VERIFIED")
    if (type(verification.get("version")) is not int or verification.get("version") != 1
            or verification.get("status") != "verified"):
        raise PublishError("XLSX_NOT_VERIFIED")
    if (index.get('row_identity_format') != 'source_rowid_column_v1'
            or verification.get('source_rowids_verified') is not True):
        raise PublishError('XLSX_ROW_IDENTITIES_NOT_VERIFIED')
    for doc in (index, verification):
        identity = doc.get("source")
        if (not isinstance(identity, dict)
                or set(identity) != {"path", "bytes", "sha256"}
                or not isinstance(identity.get("path"), str)
                or not identity["path"]):
            raise PublishError("XLSX_NOT_VERIFIED")
        if (not _int(identity.get("bytes"), minimum=1)
                or not valid_sha(identity.get("sha256"))
                or identity.get("bytes") != source["bytes"]
                or identity.get("sha256") != source["sha256"]):
            raise PublishError("XLSX_SOURCE_MISMATCH")
    if index["source"]["path"] != verification["source"]["path"]:
        raise PublishError("XLSX_SOURCE_MISMATCH")
    if index["source"]["path"] != str(source_path):
        raise PublishError("XLSX_SOURCE_MISMATCH")
    if verification.get("snapshot_identifier") != generation_id:
        raise PublishError("XLSX_GENERATION_MISMATCH")
    if verification.get("snapshot_sha256") != source["sha256"]:
        raise PublishError("XLSX_SOURCE_MISMATCH")
    if index.get("snapshot_identifier") != generation_id:
        raise PublishError("XLSX_GENERATION_MISMATCH")
    if index.get("snapshot_id") != generation_id:
        raise PublishError("XLSX_GENERATION_MISMATCH")
    if index.get("snapshot_sha256") != source["sha256"]:
        raise PublishError("XLSX_SOURCE_MISMATCH")
    if index.get("verification_receipt") != "verification.json":
        raise PublishError("XLSX_INDEX_INVALID")

    counts = verification.get("counts")
    count_fields = ("exported_tables", "exported_rows", "exported_cells", "chunks", "pieces")
    if (not isinstance(counts, dict)
            or any(not _int(counts.get(key)) for key in count_fields)
            or not _int(counts.get("source_bytes"), minimum=1)
            or counts["source_bytes"] != source["bytes"]):
        raise PublishError("XLSX_COUNTS_INVALID")

    pieces = index.get("pieces")
    if not isinstance(pieces, list):
        raise PublishError("XLSX_INDEX_INVALID")
    if index.get("piece_names") is not None:
        names = index.get("piece_names")
        if not isinstance(names, list) or names != [row.get("name") if isinstance(row, dict) else None for row in pieces]:
            raise PublishError("XLSX_INDEX_INVALID")
    if index.get("counts") != verification.get("counts"):
        raise PublishError("XLSX_INDEX_INVALID")
    mapped = {}
    for row in pieces:
        if not isinstance(row, dict):
            raise PublishError("XLSX_INDEX_INVALID")
        name = row.get("name")
        if not isinstance(name, str) or not XLSX_FILE_RE.fullmatch(name):
            raise PublishError("XLSX_INDEX_INVALID")
        if name in mapped:
            raise PublishError("XLSX_INDEX_INVALID")
        if not _int(row.get("bytes"), minimum=1) or not valid_sha(row.get("sha256")):
            raise PublishError("XLSX_INDEX_INVALID")
        mapped[name] = (row["bytes"], row["sha256"])
    if counts["pieces"] != len(mapped):
        raise PublishError("XLSX_COUNTS_INVALID")
    return mapped


def _verify_xlsx_manifest(manifest, verification, index, pieces, generation_id, source,
                          index_size, index_sha):
    if type(manifest.get("version")) is not int or manifest.get("version") != 1:
        raise PublishError("XLSX_MANIFEST_MISMATCH")
    if manifest.get("status") != "verified":
        raise PublishError("XLSX_NOT_VERIFIED")
    if manifest.get("snapshot_identifier") != generation_id:
        raise PublishError("XLSX_GENERATION_MISMATCH")
    if manifest.get("snapshot_sha256") != source["sha256"]:
        raise PublishError("XLSX_SOURCE_MISMATCH")
    if manifest.get("source") != verification.get("source"):
        raise PublishError("XLSX_SOURCE_MISMATCH")
    if manifest.get("counts") != verification.get("counts"):
        raise PublishError("XLSX_MANIFEST_MISMATCH")
    if verification.get("manifest") != "manifest.json" or verification.get("index") != "index.json":
        raise PublishError("XLSX_MANIFEST_MISMATCH")
    if manifest.get("verification_receipt") != "verification.json":
        raise PublishError("XLSX_MANIFEST_MISMATCH")
    manifest_files = manifest.get("files")
    receipt_files = verification.get("files")
    if not isinstance(manifest_files, list) or manifest_files != receipt_files:
        raise PublishError("XLSX_MANIFEST_MISMATCH")
    expected_files = {**pieces, "index.json": (index_size, index_sha)}
    actual_files = {}
    for row in manifest_files:
        if not isinstance(row, dict) or set(row) != {"name", "bytes", "sha256"}:
            raise PublishError("XLSX_MANIFEST_MISMATCH")
        name = row.get("name")
        if name == "index.json":
            pass
        elif not isinstance(name, str) or not XLSX_FILE_RE.fullmatch(name):
            raise PublishError("XLSX_MANIFEST_MISMATCH")
        if name in actual_files or not _int(row.get("bytes"), minimum=1) or not valid_sha(row.get("sha256")):
            raise PublishError("XLSX_MANIFEST_MISMATCH")
        actual_files[name] = (row["bytes"], row["sha256"])
    if actual_files != expected_files:
        raise PublishError("XLSX_MANIFEST_MISMATCH")


def _canonical_json_sha(value):
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _manifest_slice_path(value, expected_area):
    if not isinstance(value, str):
        raise PublishError("SLICE_MANIFEST_INVALID")
    if value.startswith("slices/"):
        local = value
    else:
        local = "slices/" + value
    if not _safe_slice_rel(local):
        raise PublishError("SLICE_MANIFEST_INVALID")
    if expected_area == "shared" and not local.startswith("slices/shared/"):
        raise PublishError("SLICE_MANIFEST_INVALID")
    if expected_area == "values":
        value_area = local.startswith("slices/values/")
        shared_value_area = (
            len(local.split("/")) == 5
            and local.startswith("slices/shared/")
            and bool(re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,160}", local.split("/")[2], re.ASCII))
            and local.split("/")[3] == "value"
            and bool(re.fullmatch(r"part[0-9]{6}\.sqlite3", local.rsplit("/", 1)[-1], re.ASCII))
        )
        if not value_area and not shared_value_area:
            raise PublishError("SLICE_MANIFEST_INVALID")
    if expected_area == "by-mode" and not local.startswith("slices/by-mode/"):
        raise PublishError("SLICE_MANIFEST_INVALID")
    if expected_area == "by-rule" and not local.startswith("slices/by-rule/"):
        raise PublishError("SLICE_MANIFEST_INVALID")
    return local


def _validate_schema_objects(objects):
    if not isinstance(objects, list):
        raise PublishError("SLICE_SCHEMA_INVALID")
    normalized = []
    for row in objects:
        if (not isinstance(row, dict)
                or set(row) != {"name", "type", "tbl_name", "sql"}
                or not isinstance(row.get("name"), str) or not row["name"]
                or not isinstance(row.get("type"), str) or not row["type"]
                or not isinstance(row.get("tbl_name"), str)
                or (row.get("sql") is not None and not isinstance(row.get("sql"), str))):
            raise PublishError("SLICE_SCHEMA_INVALID")
        normalized.append((row["type"], row["name"]))
    if normalized != sorted(normalized) or len(set(normalized)) != len(normalized):
        raise PublishError("SLICE_SCHEMA_INVALID")
    return {row["name"] for row in objects if row["type"] == "table"}


def _validate_column_schema(table):
    columns = table.get("columns")
    column_schema = table.get("column_schema")
    if (not isinstance(columns, list) or not all(isinstance(name, str) for name in columns)
            or not isinstance(column_schema, list) or not column_schema):
        raise PublishError("SLICE_SCHEMA_INVALID")
    required = {"cid", "name", "type", "notnull", "dflt_value", "pk", "hidden"}
    for column in column_schema:
        if (not isinstance(column, dict) or set(column) != required
                or not _int(column.get("cid")) or not isinstance(column.get("name"), str)
                or not isinstance(column.get("type"), str)
                or type(column.get("notnull")) is not int or column["notnull"] not in (0, 1)
                or (column.get("dflt_value") is not None
                    and not isinstance(column["dflt_value"], (str, int, float)))
                or not _int(column.get("pk")) or not _int(column.get("hidden"))):
            raise PublishError("SLICE_SCHEMA_INVALID")
    if [row["cid"] for row in column_schema] != list(range(len(column_schema))):
        raise PublishError("SLICE_SCHEMA_INVALID")
    visible_columns = [row["name"] for row in column_schema if row["hidden"] != 1]
    if columns != visible_columns or len(columns) != len(set(columns)):
        raise PublishError("SLICE_SCHEMA_INVALID")
    return columns


def _validate_lossless_manifest(slices_doc, sqlite_verification, source, generation_id):
    if type(slices_doc.get("version")) is not int or slices_doc.get("version") != 2:
        raise PublishError("SLICE_MANIFEST_INVALID")
    if slices_doc.get("role") != "lossless_sqlite_shards":
        raise PublishError("SLICE_ROLE_INVALID")
    if slices_doc.get("snapshot_identifier") != generation_id:
        raise PublishError("SLICE_GENERATION_MISMATCH")
    if slices_doc.get("source_sha256") != source["sha256"]:
        raise PublishError("SLICE_SOURCE_MISMATCH")

    objects = slices_doc.get("schema_objects")
    schema_tables = _validate_schema_objects(objects)
    schema_sha = _canonical_json_sha(objects)
    if not valid_sha(slices_doc.get("source_schema_sha256")):
        raise PublishError("SLICE_SCHEMA_INVALID")
    if slices_doc["source_schema_sha256"] != schema_sha:
        raise PublishError("SLICE_SCHEMA_HASH_MISMATCH")
    if sqlite_verification.get("source_schema_sha256") != schema_sha:
        raise PublishError("SLICE_SCHEMA_HASH_MISMATCH")

    row_counts = sqlite_verification.get("row_counts")
    if not isinstance(row_counts, dict) or any(
            not isinstance(name, str) or not name or not _int(value)
            for name, value in row_counts.items()):
        raise PublishError("SQLITE_VERIFICATION_INVALID")
    tables = slices_doc.get("tables")
    if not isinstance(tables, list) or len(tables) != len(row_counts):
        raise PublishError("SLICE_MANIFEST_INVALID")
    if (not _int(sqlite_verification.get("table_count"), minimum=1)
            or sqlite_verification["table_count"] != len(tables)
            or schema_tables != set(row_counts)):
        raise PublishError("SQLITE_VERIFICATION_INVALID")

    data_files = {}
    table_names = set()
    table_ids = set()
    for table in tables:
        if (not isinstance(table, dict)
                or not {"name", "table_id", "columns", "column_schema", "row_count", "parts"}.issubset(table)):
            raise PublishError("SLICE_MANIFEST_INVALID")
        name = table.get("name")
        table_id = table.get("table_id")
        row_count = table.get("row_count")
        if (not isinstance(name, str) or not name or name in table_names
                or not isinstance(table_id, str)
                or not re.fullmatch(r"t[0-9]{4,}_[0-9a-f]{12}", table_id, re.ASCII)
                or table_id in table_ids or not _int(row_count)
                or row_counts.get(name) != row_count):
            raise PublishError("SLICE_MANIFEST_INVALID")
        table_names.add(name)
        table_ids.add(table_id)
        _validate_column_schema(table)
        parts = table.get("parts")
        if not isinstance(parts, list) or not parts:
            raise PublishError("SLICE_PARTS_INVALID")
        if "foreign_keys" in table:
            foreign_keys = table["foreign_keys"]
            fields = {"id", "seq", "table", "from", "to", "on_update", "on_delete", "match"}
            if not isinstance(foreign_keys, list):
                raise PublishError("SLICE_SCHEMA_INVALID")
            for item in foreign_keys:
                if (not isinstance(item, dict) or set(item) != fields
                        or not _int(item.get("id")) or not _int(item.get("seq"))
                        or not isinstance(item.get("table"), str)
                        or (item.get("from") is not None and not isinstance(item["from"], str))
                        or (item.get("to") is not None and not isinstance(item["to"], str))
                        or any(not isinstance(item.get(key), str)
                               for key in ("on_update", "on_delete", "match"))):
                    raise PublishError("SLICE_SCHEMA_INVALID")
        if "row_stream_sha256" in table and not valid_sha(table["row_stream_sha256"]):
            raise PublishError("SLICE_SCHEMA_INVALID")
        if "archive_metadata_tables" in table:
            metadata_tables = table["archive_metadata_tables"]
            if (not isinstance(metadata_tables, dict)
                    or not isinstance(metadata_tables.get("rows"), str)
                    or not metadata_tables["rows"]
                    or not isinstance(metadata_tables.get("external_cells"), str)
                    or not metadata_tables["external_cells"]):
                raise PublishError("SLICE_SCHEMA_INVALID")
        if "rowid_kind" in table and table["rowid_kind"] not in (
                "rowid", "without_rowid_primary_key", "shadowed_rowid_aliases"):
            raise PublishError("SLICE_SCHEMA_INVALID")
        if ("source_rowid_column" in table
                and (not isinstance(table["source_rowid_column"], str)
                     or not table["source_rowid_column"])):
            raise PublishError("SLICE_SCHEMA_INVALID")
        if "rowid_aliases_shadowed" in table and table["rowid_aliases_shadowed"] is not True:
            raise PublishError("SLICE_SCHEMA_INVALID")
        cursor = 0
        for part in parts:
            if (not isinstance(part, dict)
                    or not {"file", "bytes", "sha256", "row_start", "row_end"}.issubset(part)):
                raise PublishError("SLICE_PARTS_INVALID")
            local = _manifest_slice_path(part.get("file"), "shared")
            if local not in data_files:
                pass
            else:
                raise PublishError("SLICE_PARTS_DUPLICATE")
            expected_prefix = f"slices/shared/{table_id}/"
            if (not local.startswith(expected_prefix)
                    or not re.fullmatch(r"part[0-9]{6}\.sqlite3", local.rsplit("/", 1)[-1], re.ASCII)):
                raise PublishError("SLICE_PARTS_INVALID")
            size = part.get("bytes")
            digest = part.get("sha256")
            start = part.get("row_start")
            end = part.get("row_end")
            if (not _int(size, minimum=1) or not valid_sha(digest)
                    or not _int(start) or not _int(end) or start != cursor or end < start
                    or (row_count > 0 and end == start)):
                raise PublishError("SLICE_PARTS_INVALID")
            if "table_id" in part and part["table_id"] != table_id:
                raise PublishError("SLICE_PARTS_INVALID")
            if "row_count" in part and (
                    not _int(part["row_count"]) or part["row_count"] != end - start):
                raise PublishError("SLICE_PARTS_INVALID")
            if any(key in part and not _int(part[key], minimum=1)
                   for key in ("page_count", "page_size")):
                raise PublishError("SLICE_PARTS_INVALID")
            cursor = end
            data_files[local] = {"bytes": size, "sha256": digest}
        if cursor != row_count or (row_count == 0 and len(parts) != 1):
            raise PublishError("SLICE_PART_COVERAGE_INVALID")
    if table_names != schema_tables:
        raise PublishError("SLICE_TABLE_SET_MISMATCH")

    external_values = slices_doc.get("external_values")
    if not isinstance(external_values, list):
        raise PublishError("SLICE_MANIFEST_INVALID")
    for item in external_values:
        if not isinstance(item, dict):
            raise PublishError("SLICE_MANIFEST_INVALID")
        local = _manifest_slice_path(item.get("file"), "values")
        size = item.get("bytes")
        digest = item.get("sha256")
        if (local in data_files or not _int(size, minimum=1) or not valid_sha(digest)):
            raise PublishError("SLICE_MANIFEST_INVALID")
        if "table_id" in item and item["table_id"] not in table_ids:
            raise PublishError("SLICE_MANIFEST_INVALID")
        if "table_name" in item and (
                not isinstance(item["table_name"], str) or item["table_name"] not in table_names):
            raise PublishError("SLICE_MANIFEST_INVALID")
        if any(key in item and not _int(item[key], minimum=1)
               for key in ("chunk_count", "cell_count", "page_count", "page_size")):
            raise PublishError("SLICE_MANIFEST_INVALID")
        if (local.startswith("slices/shared/") and "table_id" in item
                and not local.startswith(f"slices/shared/{item['table_id']}/value/")):
            raise PublishError("SLICE_MANIFEST_INVALID")
        data_files[local] = {"bytes": size, "sha256": digest}

    if (not _int(sqlite_verification.get("file_count"))
            or sqlite_verification["file_count"] != len(data_files)):
        raise PublishError("SQLITE_VERIFICATION_INVALID")
    if sqlite_verification.get("snapshot_identifier") != generation_id:
        raise PublishError("SQLITE_GENERATION_MISMATCH")
    if sqlite_verification.get("source_sha256") != source["sha256"]:
        raise PublishError("SQLITE_SOURCE_MISMATCH")
    if not valid_sha(sqlite_verification.get("source_schema_sha256")):
        raise PublishError("SQLITE_VERIFICATION_INVALID")
    if sqlite_verification.get("status") != "verified":
        raise PublishError("SQLITE_NOT_VERIFIED")
    coverage = sqlite_verification.get("coverage")
    if not isinstance(coverage, dict) or any(
            coverage.get(key) is not True for key in
            ("all_tables", "all_rows", "all_columns", "all_values", "external_values")):
        raise PublishError("SQLITE_COVERAGE_INCOMPLETE")
    return data_files, schema_sha


def _selector_path(row, kind):
    if not isinstance(row, dict):
        raise PublishError("SLICE_SELECTOR_INVALID")
    raw = row.get("file")
    if not isinstance(raw, str):
        raise PublishError("SLICE_SELECTOR_INVALID")
    if raw.startswith("slices/"):
        local = raw
    elif raw.startswith(kind + "/"):
        local = "slices/" + raw
    else:
        local = f"slices/{kind}/{raw}"
    if not ((kind == "by-mode" and _safe_mode_rel(local))
            or (kind == "by-rule" and _safe_rule_rel(local))):
        raise PublishError("SLICE_SELECTOR_INVALID")
    return local


def _verify_selector_shared_files(path, data_files):
    expected = {
        (local.removeprefix("slices/"), item["bytes"], item["sha256"])
        for local, item in data_files.items()
    }
    connection = None
    try:
        connection = sqlite3.connect(path.as_uri() + "?mode=ro&immutable=1", uri=True)
        rows = connection.execute(
            "SELECT file,bytes,sha256 FROM shared_files ORDER BY file"
        ).fetchall()
    except sqlite3.Error as exc:
        raise PublishError("SLICE_SELECTOR_DEPENDENCIES_INVALID") from exc
    finally:
        if connection is not None:
            connection.close()
    actual = set()
    for row in rows:
        if (not isinstance(row[0], str) or not _int(row[1], minimum=1)
                or not valid_sha(row[2])):
            raise PublishError("SLICE_SELECTOR_DEPENDENCIES_INVALID")
        actual.add((row[0], row[1], row[2]))
    if len(actual) != len(rows) or actual != expected:
        raise PublishError("SELECTOR_REACHABILITY_FAILED")


def _verify_selector_manifest(root, slices_doc, selector_verification, generation_id,
                              source, plan_counts, data_files):
    modes = slices_doc.get("by_mode")
    rules = slices_doc.get("by_rule")
    counts = slices_doc.get("counts")
    if not isinstance(modes, list) or not isinstance(rules, list) or not isinstance(counts, dict):
        raise PublishError("SLICE_SELECTOR_INVALID")
    if selector_verification.get("status") != "verified":
        raise PublishError("SELECTOR_NOT_VERIFIED")
    if selector_verification.get("snapshot_identifier") != generation_id:
        raise PublishError("SELECTOR_GENERATION_MISMATCH")
    if selector_verification.get("source_sha256") != source["sha256"]:
        raise PublishError("SELECTOR_SOURCE_MISMATCH")
    if selector_verification.get("all_shared_files_reachable") is not True:
        raise PublishError("SELECTOR_REACHABILITY_FAILED")
    if selector_verification.get("all_mode_matches") is not True or selector_verification.get("all_rule_matches") is not True:
        raise PublishError("SELECTOR_MATCHES_INCOMPLETE")

    mode_paths = set()
    rule_paths = set()
    selector_metadata = {}
    for rows, kind, paths in ((modes, "by-mode", mode_paths), (rules, "by-rule", rule_paths)):
        for row in rows:
            expected_keys = ({"file", "bytes", "sha256", "analysis_set", "matches"}
                             | ({"rule_raw"} if kind == "by-rule" else set()))
            if not isinstance(row, dict) or set(row) != expected_keys:
                raise PublishError("SLICE_SELECTOR_INVALID")
            local = _selector_path(row, kind)
            if local in paths:
                raise PublishError("SLICE_SELECTOR_INVALID")
            paths.add(local)
            if (not _safe_ascii_manifest_name(row.get("analysis_set"))
                    or not _int(row.get("matches"))
                    or not _int(row.get("bytes"), minimum=1)
                    or not valid_sha(row.get("sha256"))):
                raise PublishError("SLICE_SELECTOR_INVALID")
            if kind == "by-rule" and row.get("rule_raw") is not None and not isinstance(row["rule_raw"], str):
                raise PublishError("SLICE_SELECTOR_INVALID")
            selector_metadata[local] = {"bytes": row["bytes"], "sha256": row["sha256"]}
            _verify_selector_shared_files(local_path_for(root, local), data_files)
    if mode_paths & rule_paths:
        raise PublishError("SLICE_SELECTOR_INVALID")

    mode_files = len(mode_paths)
    rule_files = len(rule_paths)
    # The unclassified selector is a valid entry point, but it has no
    # match_classification rows and therefore no by-rule selector products.
    distinct_modes = sum(
        1 for row in modes
        if row["matches"] > 0 and row["analysis_set"] != UNCLASSIFIED_ANALYSIS_SET
    )
    distinct_rules = len({row.get("rule_raw") for row in rules})
    product = distinct_modes * distinct_rules
    expected = {
        "mode_files": mode_files,
        "rule_files": rule_files,
        "distinct_modes_with_matches": distinct_modes,
        "distinct_rules": distinct_rules,
        "rule_mode_product": product,
    }
    for key, value in expected.items():
        if not _int(counts.get(key)) or counts[key] != value:
            raise PublishError("SLICE_COUNTS_MISMATCH")
        if not _int(selector_verification.get(key)) or selector_verification[key] != value:
            raise PublishError("SELECTOR_COUNTS_MISMATCH")
        if plan_counts.get(key) != value:
            raise PublishError("PLAN_COUNTS_MISMATCH")
    if rule_files != product:
        raise PublishError("SLICE_COUNTS_MISMATCH")
    return mode_paths, rule_paths, expected, selector_metadata


def _scan_generation_tree(root):
    actual_dirs = {"."}
    actual_files = set()
    for current, dirs, files in os.walk(root, topdown=True, followlinks=False):
        current_path = Path(current)
        rel_dir = current_path.relative_to(root).as_posix()
        if rel_dir == "":
            rel_dir = "."
        for name in list(dirs):
            child = current_path / name
            try:
                mode = child.lstat().st_mode
            except OSError as exc:
                raise PublishError("PATH_SAFETY_ERROR") from exc
            if not stat.S_ISDIR(mode):
                raise PublishError("PATH_SAFETY_ERROR")
            rel = child.relative_to(root).as_posix()
            actual_dirs.add(rel)
        for name in files:
            child = current_path / name
            try:
                mode = child.lstat().st_mode
            except OSError as exc:
                raise PublishError("PATH_SAFETY_ERROR") from exc
            if not stat.S_ISREG(mode):
                raise PublishError("PATH_SAFETY_ERROR")
            actual_files.add(child.relative_to(root).as_posix())
    if "generation-plan.json" not in actual_files:
        raise PublishError("GENERATION_LAYOUT_INVALID")
    return actual_files - {"generation-plan.json"}, actual_dirs


def _allowed_local_artifact(local):
    if local in ("source.sqlite3", "source-manifest.json"):
        return True
    if _safe_slice_rel(local):
        return True
    if local in ("xlsx/index.json", "xlsx/manifest.json", "xlsx/verification.json") or _safe_xlsx_piece_rel(local):
        return True
    return False


def _expected_generation_dirs(files):
    expected = {".", "slices", "slices/by-mode", "slices/by-rule", "xlsx"}
    for local in files:
        parts = local.split("/")[:-1]
        for end in range(2, len(parts) + 1):
            expected.add("/".join(parts[:end]))
    return expected


def validate_generation(root, generation_id, plan, plan_size, plan_sha):
    if not valid_generation_id(generation_id) or not isinstance(plan, dict):
        raise PublishError("PLAN_INVALID")
    if type(plan.get("version")) is not int or plan["version"] != 1:
        raise PublishError("PLAN_INVALID")
    if plan.get("generation_id") != generation_id:
        raise PublishError("PLAN_GENERATION_MISMATCH")

    captured_at = plan.get("captured_at")
    captured_kind = plan.get("captured_at_kind")
    if captured_at is None:
        if captured_kind != "unknown_legacy_snapshot":
            raise PublishError("PLAN_CAPTURE_TIME_INVALID")
    else:
        parse_utc_timestamp(captured_at)
        if captured_kind != "pinned_read_transaction":
            raise PublishError("PLAN_CAPTURE_TIME_INVALID")

    source = plan.get("source")
    if (not isinstance(source, dict) or not _int(source.get("bytes"), minimum=1)
            or not valid_sha(source.get("sha256"))):
        raise PublishError("PLAN_SOURCE_INVALID")

    counts = plan.get("counts")
    count_keys = ("mode_files", "rule_files", "distinct_modes_with_matches",
                  "distinct_rules", "rule_mode_product", "xlsx_pieces")
    if not isinstance(counts, dict) or any(not _int(counts.get(key)) for key in count_keys):
        raise PublishError("PLAN_COUNTS_INVALID")

    files = plan.get("files")
    if not isinstance(files, list) or not files:
        raise PublishError("PLAN_FILES_INVALID")
    by_local = {}
    by_remote = set()
    for row in files:
        if not isinstance(row, dict) or set(row) != {"local", "remote", "bytes", "sha256"}:
            raise PublishError("PLAN_FILES_INVALID")
        local = safe_rel_path(row.get("local"))
        remote_rel = safe_rel_path(row.get("remote"))
        if not _allowed_local_artifact(local):
            raise PublishError("PLAN_FILE_PATH_INVALID")
        if local in by_local or remote_rel in by_remote:
            raise PublishError("PLAN_FILES_DUPLICATE")
        if remote_rel != _expected_remote(generation_id, local):
            raise PublishError("PLAN_REMOTE_PATH_INVALID")
        size = row.get("bytes")
        digest = row.get("sha256")
        if not _int(size, minimum=1) or not valid_sha(digest):
            raise PublishError("PLAN_FILE_METADATA_INVALID")
        by_local[local] = {"local": local, "remote": remote_rel, "bytes": size, "sha256": digest}
        by_remote.add(remote_rel)

    actual_local, actual_dirs = _scan_generation_tree(root)
    if set(by_local) != actual_local:
        raise PublishError("PLAN_FILE_SET_MISMATCH")
    if actual_dirs != _expected_generation_dirs(by_local):
        raise PublishError("GENERATION_LAYOUT_INVALID")
    required_controls = {
        "source.sqlite3", "source-manifest.json", "slices/manifest.json",
        "slices/verification.json", "slices/selectors-verification.json",
        "xlsx/index.json", "xlsx/manifest.json", "xlsx/verification.json",
    }
    if not required_controls.issubset(by_local):
        raise PublishError("GENERATION_LAYOUT_INVALID")

    source_manifest, _source_manifest_bytes, _source_manifest_size, _source_manifest_sha = _load_json_artifact(root, "source-manifest.json")
    _verify_source_manifest(source_manifest, source)
    slices_doc, _slices_bytes, _slices_size, _slices_sha = _load_json_artifact(root, "slices/manifest.json")
    sqlite_verification, _sqlite_receipt_bytes, _sqlite_receipt_size, _sqlite_receipt_sha = _load_json_artifact(root, "slices/verification.json")
    selector_verification, _selector_receipt_bytes, _selector_receipt_size, _selector_receipt_sha = _load_json_artifact(root, "slices/selectors-verification.json")
    if plan.get("sqlite_verification") != sqlite_verification:
        raise PublishError("SQLITE_VERIFICATION_MISMATCH")
    if plan.get("selector_verification") != selector_verification:
        raise PublishError("SELECTOR_VERIFICATION_MISMATCH")
    if plan.get("sqlite_verification_path") != "slices/verification.json":
        raise PublishError("SQLITE_VERIFICATION_PATH_INVALID")
    if (sqlite_verification.get("status") != "verified"
            or sqlite_verification.get("snapshot_identifier") != generation_id
            or sqlite_verification.get("source_sha256") != source["sha256"]):
        raise PublishError("SQLITE_NOT_VERIFIED")
    data_files, source_schema_sha = _validate_lossless_manifest(
        slices_doc, sqlite_verification, source, generation_id)
    mode_paths, rule_paths, _selector_counts, selector_metadata = _verify_selector_manifest(
        root, slices_doc, selector_verification, generation_id, source, counts, data_files)
    if (counts["mode_files"] != len(mode_paths) or counts["rule_files"] != len(rule_paths)
            or counts["xlsx_pieces"] < 0):
        raise PublishError("PLAN_COUNTS_MISMATCH")

    xlsx_index, _xlsx_index_bytes, _xlsx_index_size, _xlsx_index_sha = _load_json_artifact(root, "xlsx/index.json")
    xlsx_manifest, _xlsx_manifest_bytes, _xlsx_manifest_size, _xlsx_manifest_sha = _load_json_artifact(root, "xlsx/manifest.json")
    xlsx_receipt, _xlsx_receipt_bytes, _xlsx_receipt_size, _xlsx_receipt_sha = _load_json_artifact(root, "xlsx/verification.json")
    if plan.get("xlsx_verification") != xlsx_receipt:
        raise PublishError("XLSX_VERIFICATION_MISMATCH")
    piece_map = _verify_xlsx_metadata(
        xlsx_index, xlsx_receipt, generation_id, source,
        local_path_for(root, "source.sqlite3"))
    _verify_xlsx_manifest(xlsx_manifest, xlsx_receipt, xlsx_index, piece_map,
                          generation_id, source, _xlsx_index_size, _xlsx_index_sha)
    if counts["xlsx_pieces"] != len(piece_map):
        raise PublishError("PLAN_COUNTS_MISMATCH")

    mode_locals = {local for local in by_local if _safe_mode_rel(local)}
    rule_locals = {local for local in by_local if _safe_rule_rel(local)}
    xlsx_locals = {local for local in by_local if _safe_xlsx_piece_rel(local)}
    if mode_locals != mode_paths or rule_locals != rule_paths:
        raise PublishError("SLICE_SELECTOR_FILE_SET_MISMATCH")
    if xlsx_locals != {f"xlsx/{name}" for name in piece_map}:
        raise PublishError("XLSX_FILE_SET_MISMATCH")
    expected_slice_files = (set(data_files) | mode_paths | rule_paths
                            | {"slices/manifest.json", "slices/verification.json",
                               "slices/selectors-verification.json"})
    actual_slice_files = {local for local in by_local if local.startswith("slices/")}
    if actual_slice_files != expected_slice_files:
        raise PublishError("SLICE_FILE_SET_MISMATCH")

    source_entry = by_local["source.sqlite3"]
    if source_entry["bytes"] != source["bytes"] or source_entry["sha256"] != source["sha256"]:
        raise PublishError("PLAN_SOURCE_INVALID")
    control_metadata = {
        "source-manifest.json": (_source_manifest_size, _source_manifest_sha),
        "slices/manifest.json": (_slices_size, _slices_sha),
        "slices/verification.json": (_sqlite_receipt_size, _sqlite_receipt_sha),
        "slices/selectors-verification.json": (_selector_receipt_size, _selector_receipt_sha),
        "xlsx/manifest.json": (_xlsx_manifest_size, _xlsx_manifest_sha),
        "xlsx/index.json": (_xlsx_index_size, _xlsx_index_sha),
        "xlsx/verification.json": (_xlsx_receipt_size, _xlsx_receipt_sha),
    }
    for local, (size, digest) in control_metadata.items():
        row = by_local[local]
        if (row["bytes"], row["sha256"]) != (size, digest):
            raise PublishError("PLAN_FILE_METADATA_MISMATCH")
    for local, expected in data_files.items():
        row = by_local.get(local)
        if row is None or (row["bytes"], row["sha256"]) != (expected["bytes"], expected["sha256"]):
            raise PublishError("PLAN_FILE_METADATA_MISMATCH")
    for local, expected in selector_metadata.items():
        row = by_local.get(local)
        if row is None or (row["bytes"], row["sha256"]) != (expected["bytes"], expected["sha256"]):
            raise PublishError("SLICE_SELECTOR_FILE_SET_MISMATCH")
    for name, (size, digest) in piece_map.items():
        row = by_local[f"xlsx/{name}"]
        if (row["bytes"], row["sha256"]) != (size, digest):
            raise PublishError("PLAN_FILE_METADATA_MISMATCH")

    local_verified = {}
    for local, row in by_local.items():
        actual_size, actual_sha = hash_regular_file(local_path_for(root, local))
        if (actual_size, actual_sha) != (row["bytes"], row["sha256"]):
            raise PublishError("LOCAL_FILE_MISMATCH")
        local_verified[local] = row

    return {
        "generation_id": generation_id,
        "captured_at": captured_at,
        "captured_at_kind": captured_kind,
        "source": {"bytes": source["bytes"], "sha256": source["sha256"]},
        "counts": {key: counts[key] for key in count_keys},
        "sqlite_verification": sqlite_verification,
        "selector_verification": selector_verification,
        "xlsx_verification": xlsx_receipt,
        "source_schema_sha256": source_schema_sha,
        "files": normalized_file_order(local_verified),
        "plan_sha256": plan_sha,
        "plan_bytes": plan_size,
        "local_files": local_verified,
    }


def normalized_file_order(files):
    def rank(local):
        if local == "source.sqlite3":
            return (0, local)
        if (local.startswith("slices/shared/") or local.startswith("slices/values/")
                or local.startswith("slices/by-mode/") or local.startswith("slices/by-rule/")):
            return (1, local)
        if local.startswith("xlsx/") and local.endswith(".xlsx"):
            return (2, local)
        if local == "source-manifest.json":
            return (3, local)
        if local == "slices/manifest.json":
            return (4, local)
        if local in ("slices/verification.json", "slices/selectors-verification.json",
                     "xlsx/manifest.json"):
            return (5, local)
        if local == "xlsx/verification.json":
            return (6, local)
        if local == "xlsx/index.json":
            return (7, local)
        return (99, local)
    return [files[key] for key in sorted(files, key=rank)]


def validate_remote(remote):
    if not isinstance(remote, str) or remote.count(":") != 1:
        raise PublishError("CONFIG_ERROR")
    name, path = remote.split(":", 1)
    if not REMOTE_NAME_RE.fullmatch(name) or path != "database":
        raise PublishError("CONFIG_ERROR")
    return remote.rstrip("/")


def join_remote(remote, relative):
    safe_rel_path(relative)
    return f"{remote}/{relative}"


def _read_bounded_stream(stream, limit):
    data = bytearray()
    too_large = False
    while True:
        block = stream.read(8192)
        if not block:
            break
        room = limit - len(data)
        if room > 0:
            data.extend(block[:room])
        if len(block) > room:
            too_large = True
            break
    return bytes(data), too_large


def run_stat_process(command):
    try:
        proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=False)
    except OSError as exc:
        raise PublishError("REMOTE_ERROR") from exc
    selector = selectors.DefaultSelector()
    buffers = {"stdout": bytearray(), "stderr": bytearray()}
    too_large = False
    for name, stream in (("stdout", proc.stdout), ("stderr", proc.stderr)):
        selector.register(stream, selectors.EVENT_READ, name)
    try:
        while selector.get_map():
            for key, _events in selector.select():
                name = key.data
                block = os.read(key.fileobj.fileno(), 8192)
                if not block:
                    selector.unregister(key.fileobj)
                    key.fileobj.close()
                    continue
                room = MAX_RCLONE_TEXT_BYTES - len(buffers[name])
                if room > 0:
                    buffers[name].extend(block[:room])
                if len(block) > room:
                    too_large = True
                    proc.kill()
                    break
            if too_large:
                break
        if too_large:
            proc.wait()
            raise PublishError("REMOTE_RESPONSE_TOO_LARGE")
        return subprocess.CompletedProcess(command, proc.wait(), bytes(buffers["stdout"]), bytes(buffers["stderr"]))
    finally:
        selector.close()
        for stream in (proc.stdout, proc.stderr):
            if stream and not stream.closed:
                stream.close()
        if proc.poll() is None:
            proc.kill()
            proc.wait()


def classify_transient(stderr):
    text = stderr.decode("utf-8", errors="replace").lower()
    return any(marker in text for marker in (
        "ratelimitexceeded", "userratelimitexceeded", "rate limit exceeded",
        "quota exceeded", "quotaexceeded", "too many requests", "http 429",
        "connection reset", "connection refused", "connection timed out",
        "i/o timeout", "timed out", "temporary failure", "network is unreachable",
        "no route to host", "no such host", "server misbehaving", "network error",
        "connection aborted", "unexpected eof", "broken pipe", "tls handshake timeout",
        "http 500", "http 502", "http 503", "http 504", "service unavailable",
    ))


def stream_remote_hash(command, *, capture_limit=None):
    try:
        proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=False)
    except OSError as exc:
        raise PublishError("REMOTE_ERROR") from exc
    selector = selectors.DefaultSelector()
    stderr_data = bytearray()
    captured = bytearray() if capture_limit is not None else None
    digest = hashlib.sha256()
    total = 0
    too_large = False
    if proc.stdout is None or proc.stderr is None:
        proc.kill()
        proc.wait()
        raise PublishError("REMOTE_READ_FAILED")
    selector.register(proc.stdout, selectors.EVENT_READ, "stdout")
    selector.register(proc.stderr, selectors.EVENT_READ, "stderr")
    try:
        while selector.get_map():
            for key, _events in selector.select():
                name = key.data
                block = os.read(key.fileobj.fileno(), 1024 * 1024 if name == "stdout" else 8192)
                if not block:
                    selector.unregister(key.fileobj)
                    key.fileobj.close()
                    continue
                if name == "stdout":
                    total += len(block)
                    digest.update(block)
                    if captured is not None:
                        if total > capture_limit:
                            too_large = True
                            proc.kill()
                            break
                        captured.extend(block)
                else:
                    room = MAX_RCLONE_TEXT_BYTES - len(stderr_data)
                    if room > 0:
                        stderr_data.extend(block[:room])
                    if len(block) > room:
                        too_large = True
                        proc.kill()
                        break
            if too_large:
                break
        if too_large:
            proc.wait()
            raise PublishError("REMOTE_RESPONSE_TOO_LARGE")
        returncode = proc.wait()
    finally:
        selector.close()
        for stream in (proc.stdout, proc.stderr):
            if stream and not stream.closed:
                stream.close()
        if proc.poll() is None:
            proc.kill()
            proc.wait()
    if returncode:
        if classify_transient(bytes(stderr_data)):
            raise TemporaryRemoteError("REMOTE_TEMPORARY")
        raise PublishError("REMOTE_READ_FAILED")
    return total, digest.hexdigest(), bytes(captured) if captured is not None else None


def is_missing_stat(returncode, stderr):
    if returncode not in (3, 4) or classify_transient(stderr):
        return False
    text = stderr.decode("utf-8", errors="replace").lower()
    return any(marker in text for marker in (
        "object not found", "dir not found", "directory not found",
    ))


def retry_remote(operation):
    for attempt in range(RETRY_LIMIT):
        try:
            return operation()
        except TemporaryRemoteError:
            if attempt + 1 >= RETRY_LIMIT:
                raise PublishError("REMOTE_RETRY_EXHAUSTED")
            time.sleep(30 * (attempt + 1))
    raise PublishError("REMOTE_RETRY_EXHAUSTED")


class Rclone:
    def __init__(self, binary="rclone", *, drive_folder_cache=None):
        self.binary = binary
        if drive_folder_cache is None:
            configured = os.environ.get("IKARING_ARCHIVE_DRIVE_FOLDER_CACHE", "0")
            if configured not in ("0", "1"):
                raise PublishError("CONFIG_ERROR")
            drive_folder_cache = configured == "1"
        if type(drive_folder_cache) is not bool:
            raise PublishError("CONFIG_ERROR")
        self._folder_cache = None
        if drive_folder_cache:
            from rclone_drive_folders import DriveFolderCache
            self._folder_cache = DriveFolderCache(self._directory_stat, PublishError)

    def _directory_stat(self, remote_path, parent_folder_id=None):
        # rclone 1.60 synthesizes directory --stat output without its Drive ID.
        # Resolve the exact child from its parent's one-level directory listing.
        name, relative = remote_path.split(":", 1)
        parent, separator, basename = relative.rpartition("/")
        if not separator:
            basename, parent = relative, ""
        if not basename:
            raise PublishError("REMOTE_DIRECTORY_INVALID")
        def attempt():
            command = [self.binary, "lsjson", "--dirs-only", "--no-modtime",
                       "--no-mimetype", name + ":" + parent]
            if parent_folder_id is not None:
                command.extend(["--drive-root-folder-id", parent_folder_id])
            result = run_stat_process(command)
            if result.returncode == 0:
                try:
                    entries = json.loads(result.stdout)
                except (UnicodeError, json.JSONDecodeError) as exc:
                    raise PublishError("REMOTE_DIRECTORY_INVALID") from exc
                if not isinstance(entries, list) or any(not isinstance(item, dict) for item in entries):
                    raise PublishError("REMOTE_DIRECTORY_INVALID")
                matches = [item for item in entries if item.get("Name") == basename]
                if len(matches) > 1:
                    raise PublishError("REMOTE_DIRECTORY_AMBIGUOUS")
                return matches[0] if matches else None
            if classify_transient(result.stderr):
                raise TemporaryRemoteError("REMOTE_TEMPORARY")
            if is_missing_stat(result.returncode, result.stderr):
                return None
            raise PublishError("REMOTE_STAT_FAILED")
        return retry_remote(attempt)

    def _raw_stat(self, remote_path, folder_id=None, *, directory=False):
        def attempt():
            command = [self.binary, "lsjson", "--stat", "--hash", remote_path]
            if folder_id is not None:
                command.extend(["--drive-root-folder-id", folder_id])
            result = run_stat_process(command)
            if result.returncode == 0:
                try:
                    metadata = json.loads(result.stdout)
                except (UnicodeError, json.JSONDecodeError) as exc:
                    raise PublishError("REMOTE_STAT_INVALID") from exc
                if not isinstance(metadata, dict):
                    raise PublishError("REMOTE_STAT_INVALID")
                if directory:
                    # Directory Size can be -1. The cache validates IsDir and ID.
                    return metadata
                if metadata.get("IsDir") is not False or not _int(metadata.get("Size")):
                    raise PublishError("REMOTE_STAT_INVALID")
                return metadata
            if classify_transient(result.stderr):
                raise TemporaryRemoteError("REMOTE_TEMPORARY")
            if is_missing_stat(result.returncode, result.stderr):
                return None
            raise PublishError("REMOTE_STAT_FAILED")
        return retry_remote(attempt)

    def _route(self, remote_path):
        if self._folder_cache is None:
            return remote_path, None
        return self._folder_cache.route(remote_path)

    def _command(self, operation, remote_path, *options):
        routed, folder_id = self._route(remote_path)
        command = [self.binary, operation, *options, routed]
        if folder_id is not None:
            command.extend(["--drive-root-folder-id", folder_id])
        return command

    def verify_directory_bindings(self):
        if self._folder_cache is not None:
            self._folder_cache.verify_bindings()

    def stat(self, remote_path):
        routed, folder_id = self._route(remote_path)
        return self._raw_stat(routed, folder_id)

    def readback(self, remote_path):
        def attempt():
            total, digest, _captured = stream_remote_hash(self._command("cat", remote_path))
            return total, digest
        return retry_remote(attempt)

    def readback_bytes(self, remote_path, expected_bytes, expected_sha):
        def attempt():
            total, digest, data = stream_remote_hash(
                self._command("cat", remote_path), capture_limit=MAX_CONTROL_BYTES)
            if (total, digest) != (expected_bytes, expected_sha) or data is None:
                raise PublishError("REMOTE_READBACK_MISMATCH")
            return data
        return retry_remote(attempt)

    def copyto(self, source, remote_path, *, immutable):
        routed, folder_id = self._route(remote_path)
        command = [self.binary, "copyto"]
        if immutable:
            command.append("--immutable")
        command.extend([str(source), routed])
        if folder_id is not None:
            command.extend(["--drive-root-folder-id", folder_id])
        try:
            proc = subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, shell=False)
        except OSError as exc:
            raise PublishError("REMOTE_ERROR") from exc
        stderr, too_large = _read_bounded_stream(proc.stderr, MAX_RCLONE_TEXT_BYTES)
        if too_large:
            proc.kill()
            proc.wait()
            raise PublishError("REMOTE_RESPONSE_TOO_LARGE")
        proc.stderr.close()
        returncode = proc.wait()
        if returncode == 0:
            return
        if classify_transient(stderr):
            raise TemporaryRemoteError("REMOTE_TEMPORARY")
        raise PublishError("REMOTE_UPLOAD_FAILED")


def verify_remote_directories(client):
    verifier = getattr(client, "verify_directory_bindings", None)
    if callable(verifier):
        verifier()


def remote_object_state(client, remote_path, expected_bytes, expected_sha):
    metadata = client.stat(remote_path)
    if metadata is None:
        return False
    if metadata.get("Size") != expected_bytes:
        raise PublishError("REMOTE_SIZE_MISMATCH")
    actual = client.readback(remote_path)
    if actual != (expected_bytes, expected_sha):
        raise PublishError("REMOTE_READBACK_MISMATCH")
    return True


def verify_remote_bytes(client, remote_path, expected_bytes, expected_sha):
    metadata = client.stat(remote_path)
    if metadata is None:
        raise PublishError("REMOTE_OBJECT_MISSING")
    if metadata.get("Size") != expected_bytes:
        raise PublishError("REMOTE_SIZE_MISMATCH")
    if client.readback(remote_path) != (expected_bytes, expected_sha):
        raise PublishError("REMOTE_READBACK_MISMATCH")


def write_temp_bytes(state_dir, data, prefix):
    fd, name = tempfile.mkstemp(prefix=prefix, dir=state_dir)
    path = Path(name)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        fsync_dir(state_dir)
        return path
    except Exception:
        try:
            os.close(fd)
        except OSError:
            pass
        try:
            path.unlink()
        except OSError:
            pass
        raise


def fsync_dir(path):
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def atomic_write(path, data):
    path = Path(path)
    verify_no_symlink_components(path)
    temp = write_temp_bytes(path.parent, data, ".tmp-full-publish-")
    try:
        os.replace(temp, path)
        fsync_dir(path.parent)
    except OSError as exc:
        try:
            temp.unlink()
        except OSError:
            pass
        raise PublishError("STATE_ERROR") from exc


def atomic_json(path, value):
    data = (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
    atomic_write(path, data)


def read_private_json(path, *, max_bytes=MAX_CONTROL_BYTES):
    path = Path(path)
    verify_no_symlink_components(path)
    if not path.exists():
        return None
    data, _size, _digest = read_control_file(path)
    value = _json_object(data)
    return value


def state_file_path(state_dir, name):
    if not re.fullmatch(r"[A-Za-z0-9_.-]+\.json", name, re.ASCII):
        raise PublishError("STATE_ERROR")
    return state_dir / name


class PublishLock:
    def __init__(self, state_dir):
        self.path = Path(state_dir) / ".nas-full-publish.lock"
        self.fd = None

    def __enter__(self):
        verify_no_symlink_components(self.path)
        try:
            self.fd = os.open(self.path, os.O_CREAT | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0), 0o600)
            fcntl.flock(self.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (OSError, BlockingIOError) as exc:
            if self.fd is not None:
                os.close(self.fd)
                self.fd = None
            raise PublishError("LOCK_BUSY") from exc
        return self

    def __exit__(self, *_args):
        if self.fd is not None:
            try:
                fcntl.flock(self.fd, fcntl.LOCK_UN)
            finally:
                os.close(self.fd)
                self.fd = None


def _validate_current_state(value):
    if value is None:
        return {"version": STATE_VERSION, "last_attempt": None, "last_success": None,
                "last_failure": None, "phase": "idle", "generation_id": None}
    if (not isinstance(value, dict) or type(value.get("version")) is not int
            or value.get("version") != STATE_VERSION):
        raise PublishError("STATE_ERROR")
    for key in ("last_attempt", "last_success"):
        if value.get(key) is not None and not isinstance(value.get(key), str):
            raise PublishError("STATE_ERROR")
    failure = value.get("last_failure")
    if failure is not None and not isinstance(failure, dict):
        raise PublishError("STATE_ERROR")
    if not isinstance(value.get("phase"), str):
        raise PublishError("STATE_ERROR")
    if value.get("generation_id") is not None and not valid_generation_id(value["generation_id"]):
        raise PublishError("STATE_ERROR")
    return value


def _load_progress(path, generation_id, plan_sha):
    data = read_private_json(path)
    if data is None:
        return {"version": STATE_VERSION, "generation_id": generation_id,
                "plan_sha256": plan_sha, "receipts": {}, "prepared_index": None,
                "latest_payload": None}
    if (type(data.get("version")) is not int or data["version"] != STATE_VERSION
            or data.get("generation_id") != generation_id or data.get("plan_sha256") != plan_sha
            or not isinstance(data.get("receipts"), dict)):
        raise PublishError("PROGRESS_STATE_MISMATCH")
    for key, receipt in data["receipts"].items():
        if key not in ("generation_index", "previous_latest"):
            safe_rel_path(key)
        if (not isinstance(receipt, dict) or set(receipt) != {"bytes", "sha256", "verified"}
                or not _int(receipt.get("bytes"), minimum=1)
                or not valid_sha(receipt.get("sha256")) or receipt.get("verified") is not True):
            raise PublishError("PROGRESS_STATE_INVALID")
    if data.get("prepared_index") is not None:
        prepared = data["prepared_index"]
        if not isinstance(prepared, dict) or not isinstance(prepared.get("completed_at"), str):
            raise PublishError("PROGRESS_STATE_INVALID")
        parse_utc_timestamp(prepared["completed_at"])
        if not valid_sha(prepared.get("sha256")):
            raise PublishError("PROGRESS_STATE_INVALID")
    if data.get("latest_payload") is not None:
        payload = data["latest_payload"]
        if (not isinstance(payload, dict) or type(payload.get("version")) is not int
                or payload.get("version") != 1
                or payload.get("generation_id") != generation_id
                or not isinstance(payload.get("last_success"), str)):
            raise PublishError("PROGRESS_STATE_INVALID")
        parse_utc_timestamp(payload["last_success"])
        if not valid_sha(payload.get("index_sha256")):
            raise PublishError("PROGRESS_STATE_INVALID")
        if payload.get("captured_at") is not None:
            parse_utc_timestamp(payload["captured_at"])
        if payload.get("captured_at_kind") not in ("unknown_legacy_snapshot", "pinned_read_transaction"):
            raise PublishError("PROGRESS_STATE_INVALID")
    if data.get("latest_payload_sha256") is not None and not valid_sha(data["latest_payload_sha256"]):
        raise PublishError("PROGRESS_STATE_INVALID")
    return data


def _save_progress(path, progress):
    lock = _progress_runtime_lock(progress)
    with lock:
        atomic_json(path, {key: value for key, value in progress.items()
                           if key not in ("path", "remote", "_runtime_lock")})


def _progress_runtime_lock(progress):
    """Return a process-local lock, never serialized into progress JSON."""
    lock = progress.get("_runtime_lock")
    if lock is None or not callable(getattr(lock, "acquire", None)):
        lock = threading.RLock()
        progress["_runtime_lock"] = lock
    return lock


def _record_file_receipt(progress, key, item):
    lock = _progress_runtime_lock(progress)
    with lock:
        progress.setdefault("receipts", {})[key] = {
            "bytes": item["bytes"], "sha256": item["sha256"], "verified": True,
        }
        _save_progress(progress["path"], progress)


def _publish_worker_count():
    raw = os.environ.get("IKARING_ARCHIVE_PUBLISH_FILE_WORKERS", "1")
    if not isinstance(raw, str) or not re.fullmatch(r"[1-4]", raw, re.ASCII):
        raise PublishError("CONFIG_ERROR")
    return int(raw)


def _publish_files(client, root, state_dir, items, progress, *, on_verified=None):
    """Publish a bounded inventory and verify every object before returning.

    Only a small fixed window is submitted at once. Once a task fails, no new
    work is queued; already-running tasks are joined so their verified receipts
    remain durable before the first failure is re-raised.
    """
    workers = _publish_worker_count()
    inventory = list(items)
    seen_local = set()
    seen_remote = set()
    for item in inventory:
        if not isinstance(item, dict) or not isinstance(item.get("local"), str) or not isinstance(item.get("remote"), str):
            raise PublishError("FILE_INVENTORY_INVALID")
        local = item["local"]
        remote = join_remote(progress.get("remote", ""), item["remote"])
        if local in seen_local or remote in seen_remote:
            raise PublishError("FILE_INVENTORY_DUPLICATE")
        seen_local.add(local)
        seen_remote.add(remote)

    # Initialize before submitting anything, so workers never race to create
    # the runtime-only lock on their first receipt write.
    _progress_runtime_lock(progress)

    if workers == 1:
        verified = 0
        for item in inventory:
            _publish_file(client, root, state_dir, item, progress)
            verified += 1
            if on_verified is not None:
                on_verified(verified, len(inventory), item)
        return verified

    executor = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="archive-publish")
    pending = {}
    next_index = 0
    verified = 0
    failure = None
    try:
        while next_index < len(inventory) and len(pending) < workers:
            item = inventory[next_index]
            pending[executor.submit(_publish_file, client, root, state_dir, item, progress)] = (next_index, item)
            next_index += 1

        while pending:
            completed, _ = wait(pending, return_when=FIRST_COMPLETED)
            for future in sorted(completed, key=lambda value: pending[value][0]):
                _index, item = pending.pop(future)
                try:
                    future.result()
                except BaseException as exc:
                    if failure is None:
                        failure = exc
                else:
                    verified += 1
                    if on_verified is not None:
                        try:
                            on_verified(verified, len(inventory), item)
                        except BaseException as exc:
                            if failure is None:
                                failure = exc
            if failure is None:
                while next_index < len(inventory) and len(pending) < workers:
                    item = inventory[next_index]
                    pending[executor.submit(_publish_file, client, root, state_dir, item, progress)] = (next_index, item)
                    next_index += 1
    finally:
        executor.shutdown(wait=True)
    if failure is not None:
        raise failure
    return verified


def _prepared_generation_index(plan_info, progress):
    prepared = progress.get("prepared_index")
    if prepared is None:
        completed_at = now_utc()
    else:
        completed_at = prepared["completed_at"]
    receipt_paths = {
        "sqlite": "slices/verification.json",
        "selectors": "slices/selectors-verification.json",
        "xlsx": "xlsx/verification.json",
    }
    receipt_files = {}
    for name, local in receipt_paths.items():
        row = plan_info["local_files"][local]
        receipt_files[name] = {
            "local": row["local"],
            "remote": row["remote"],
            "bytes": row["bytes"],
            "sha256": row["sha256"],
        }
    document = {
        "version": 1,
        "generation_id": plan_info["generation_id"],
        "captured_at": plan_info["captured_at"],
        "captured_at_kind": plan_info["captured_at_kind"],
        "completed_at": completed_at,
        "source": plan_info["source"],
        "counts": plan_info["counts"],
        "source_schema_sha256": plan_info["source_schema_sha256"],
        "sqlite_verification": plan_info["sqlite_verification"],
        "selector_verification": plan_info["selector_verification"],
        "xlsx_verification": plan_info["xlsx_verification"],
        "verification_receipt_files": receipt_files,
        "files": plan_info["files"],
        "verification": {"full_readback": True, "file_count": len(plan_info["files"])},
    }
    data = (json.dumps(document, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
    digest = hashlib.sha256(data).hexdigest()
    if prepared is not None and prepared["sha256"] != digest:
        raise PublishError("PROGRESS_STATE_MISMATCH")
    if prepared is None:
        progress["prepared_index"] = {"completed_at": completed_at, "sha256": digest}
    return document, data, digest


def _prepare_latest(plan_info, index_sha, progress):
    existing = progress.get("latest_payload")
    if existing is not None:
        payload = existing
        if (payload.get("version") != 1
                or payload.get("generation_id") != plan_info["generation_id"]
                or payload.get("index_sha256") != index_sha
                or payload.get("captured_at") != plan_info["captured_at"]
                or payload.get("captured_at_kind") != plan_info["captured_at_kind"]):
            raise PublishError("PROGRESS_STATE_MISMATCH")
        return payload
    payload = {
        "version": 1,
        "last_success": now_utc(),
        "generation_id": plan_info["generation_id"],
        "index_sha256": index_sha,
        "captured_at": plan_info["captured_at"],
        "captured_at_kind": plan_info["captured_at_kind"],
    }
    progress["latest_payload"] = payload
    return payload


def _write_preserved_latest(state_dir, generation_id, raw, digest):
    history_dir = state_dir / "history"
    verify_no_symlink_components(history_dir)
    if not history_dir.exists():
        history_dir.mkdir(mode=0o700)
    verify_no_symlink_components(history_dir)
    if history_dir.stat().st_mode & 0o077:
        raise PublishError("STATE_ERROR")
    path = history_dir / f"{generation_id}-previous-latest.json"
    verify_no_symlink_components(path)
    if path.exists():
        size, current_sha = hash_regular_file(path)
        if (size, current_sha) != (len(raw), digest):
            raise PublishError("HISTORY_CONFLICT")
        with path.open("rb") as file:
            if file.read() != raw:
                raise PublishError("HISTORY_CONFLICT")
        return path
    temp = write_temp_bytes(state_dir, raw, ".tmp-previous-latest-")
    try:
        try:
            os.link(temp, path, follow_symlinks=False)
        except FileExistsError:
            size, current_sha = hash_regular_file(path)
            if (size, current_sha) != (len(raw), digest):
                raise PublishError("HISTORY_CONFLICT")
        fsync_dir(history_dir)
    except OSError as exc:
        raise PublishError("STATE_ERROR") from exc
    finally:
        try:
            temp.unlink()
        except OSError:
            pass
    return path


def _ensure_immutable_bytes(client, state_dir, remote_path, raw, expected_sha, *, progress, receipt_key):
    verify_remote_directories(client)
    expected = (len(raw), expected_sha)
    if remote_object_state(client, remote_path, *expected):
        _record_file_receipt(progress, receipt_key, {"bytes": expected[0], "sha256": expected_sha})
        return
    temp = write_temp_bytes(state_dir, raw, ".tmp-remote-object-")
    try:
        for attempt in range(RETRY_LIMIT):
            try:
                client.copyto(temp, remote_path, immutable=True)
                break
            except TemporaryRemoteError:
                # Resolve an ambiguous transfer before retrying. A matching
                # object is complete; a differing partial object is retained.
                try:
                    if remote_object_state(client, remote_path, *expected):
                        break
                except PublishError:
                    raise
                if attempt + 1 >= RETRY_LIMIT:
                    raise PublishError("REMOTE_RETRY_EXHAUSTED")
                time.sleep(30 * (attempt + 1))
            except PublishError as exc:
                if (exc.category == "REMOTE_UPLOAD_FAILED"
                        and remote_object_state(client, remote_path, *expected)):
                    break
                raise
        verify_remote_bytes(client, remote_path, *expected)
        _record_file_receipt(progress, receipt_key, {"bytes": expected[0], "sha256": expected_sha})
    finally:
        try:
            temp.unlink()
        except OSError:
            pass


def _publish_file(client, root, state_dir, item, progress):
    remote_path = join_remote(progress["remote"], item["remote"])
    if remote_object_state(client, remote_path, item["bytes"], item["sha256"]):
        _record_file_receipt(progress, item["local"], item)
        return
    source = local_path_for(root, item["local"])
    for attempt in range(RETRY_LIMIT):
        try:
            client.copyto(source, remote_path, immutable=True)
            break
        except TemporaryRemoteError:
            # A failed copy may still have created an object. Read it back
            # before deciding whether another immutable upload is safe.
            if remote_object_state(client, remote_path, item["bytes"], item["sha256"]):
                break
            if attempt + 1 >= RETRY_LIMIT:
                raise PublishError("REMOTE_RETRY_EXHAUSTED")
            time.sleep(30 * (attempt + 1))
        except PublishError as exc:
            if (exc.category == "REMOTE_UPLOAD_FAILED"
                    and remote_object_state(client, remote_path, item["bytes"], item["sha256"])):
                break
            raise
    verify_remote_bytes(client, remote_path, item["bytes"], item["sha256"])
    _record_file_receipt(progress, item["local"], item)


def _publish_latest(client, state_dir, remote, plan_info, index_doc, index_bytes, index_sha,
                    progress):
    latest_rel = "latest.json"
    latest_remote = join_remote(remote, latest_rel)
    previous_remote = join_remote(remote,
                                  f"generations/{plan_info['generation_id']}/previous-latest.json")
    latest_payload = _prepare_latest(plan_info, index_sha, progress)
    latest_bytes = (json.dumps(latest_payload, ensure_ascii=False, sort_keys=True,
                               separators=(",", ":")) + "\n").encode("utf-8")
    latest_sha = hashlib.sha256(latest_bytes).hexdigest()
    verify_remote_directories(client)
    if progress.get("latest_payload_sha256") not in (None, latest_sha):
        raise PublishError("PROGRESS_STATE_MISMATCH")
    progress["latest_payload_sha256"] = latest_sha
    _save_progress(progress["path"], progress)

    initial_metadata = client.stat(latest_remote)
    initial = None
    if initial_metadata is not None:
        if not _int(initial_metadata.get("Size")):
            raise PublishError("LATEST_INVALID")
        initial_size, initial_sha = client.readback(latest_remote)
        if initial_size != initial_metadata["Size"]:
            raise PublishError("LATEST_CHANGED")
        initial = (initial_size, initial_sha)
        # A previous attempt may have completed the update before its local
        # success record was written. Verify and finalize it idempotently.
        if initial == (len(latest_bytes), latest_sha):
            return latest_payload

    if initial is not None:
        initial_bytes = read_remote_bytes(client, latest_remote, initial[0], initial[1])
        _write_preserved_latest(state_dir, plan_info["generation_id"], initial_bytes, initial[1])
        _ensure_immutable_bytes(client, state_dir, previous_remote, initial_bytes, initial[1],
                                progress=progress, receipt_key="previous_latest")

    # Compare again immediately before replacing latest. rclone has no remote
    # conditional PUT; this is a precondition check with a known race window.
    current_meta = client.stat(latest_remote)
    if initial is None:
        if current_meta is not None:
            raise PublishError("LATEST_CHANGED")
    else:
        if current_meta is None or current_meta.get("Size") != initial[0]:
            raise PublishError("LATEST_CHANGED")
        if client.readback(latest_remote) != initial:
            raise PublishError("LATEST_CHANGED")

    verify_remote_directories(client)
    temp = write_temp_bytes(state_dir, latest_bytes, ".tmp-latest-")
    try:
        for attempt in range(RETRY_LIMIT):
            try:
                client.copyto(temp, latest_remote, immutable=False)
                break
            except TemporaryRemoteError:
                current_meta = client.stat(latest_remote)
                if current_meta is not None:
                    if current_meta.get("Size") == len(latest_bytes) and client.readback(latest_remote) == (len(latest_bytes), latest_sha):
                        break
                    if initial is None or current_meta.get("Size") != initial[0] or client.readback(latest_remote) != initial:
                        raise PublishError("LATEST_CHANGED")
                elif initial is not None:
                    raise PublishError("LATEST_CHANGED")
                if attempt + 1 >= RETRY_LIMIT:
                    raise PublishError("REMOTE_RETRY_EXHAUSTED")
                time.sleep(30 * (attempt + 1))
        verify_remote_bytes(client, latest_remote, len(latest_bytes), latest_sha)
    finally:
        try:
            temp.unlink()
        except OSError:
            pass
    return latest_payload


def read_remote_bytes(client, remote_path, expected_bytes, expected_sha):
    metadata = client.stat(remote_path)
    if metadata is None or metadata.get("Size") != expected_bytes:
        raise PublishError("REMOTE_READBACK_MISMATCH")
    if hasattr(client, "readback_bytes"):
        return client.readback_bytes(remote_path, expected_bytes, expected_sha)
    def attempt():
        total, digest, data = stream_remote_hash(
            [client.binary, "cat", remote_path], capture_limit=MAX_CONTROL_BYTES)
        if (total, digest) != (expected_bytes, expected_sha) or data is None:
            raise PublishError("REMOTE_READBACK_MISMATCH")
        return data
    return retry_remote(attempt)


def _load_progress_state(state_dir, generation_id, plan_sha, remote):
    path = state_file_path(state_dir, f"{generation_id}.progress.json")
    progress = _load_progress(path, generation_id, plan_sha)
    progress["path"] = path
    progress["remote"] = remote
    return progress


def _current_state(state_dir):
    path = state_file_path(state_dir, "current_state.json")
    value = read_private_json(path)
    return path, _validate_current_state(value)


def publish_generation(generation_dir, generation_id, remote, state_dir, *, rclone_bin="rclone"):
    root = generation_root(generation_dir)
    remote = validate_remote(remote)
    state_dir = ensure_private_state_dir(state_dir)
    state_path, current = _current_state(state_dir)
    started_at = now_utc()
    current = dict(current)
    current.update({"last_attempt": started_at, "phase": "preflight", "generation_id": generation_id})
    atomic_json(state_path, current)

    plan, plan_size, plan_sha = _read_plan(root)
    plan_info = validate_generation(root, generation_id, plan, plan_size, plan_sha)
    progress = _load_progress_state(state_dir, generation_id, plan_sha, remote)
    _save_progress(progress["path"], progress)
    client = Rclone(rclone_bin)

    current["phase"] = "publishing_artifacts"
    atomic_json(state_path, current)
    _publish_files(client, root, state_dir, plan_info["files"], progress)

    # All data and local manifests have now been fully read back and verified.
    index_doc, index_bytes, index_sha = _prepared_generation_index(plan_info, progress)
    progress["prepared_index"] = {
        "completed_at": index_doc["completed_at"], "sha256": index_sha,
    }
    _save_progress(progress["path"], progress)
    current["phase"] = "publishing_generation_index"
    atomic_json(state_path, current)
    index_remote = join_remote(remote, f"generations/{generation_id}/index.json")
    _ensure_immutable_bytes(client, state_dir, index_remote, index_bytes, index_sha,
                            progress=progress, receipt_key="generation_index")

    current["phase"] = "publishing_latest"
    atomic_json(state_path, current)
    latest_payload = _publish_latest(client, state_dir, remote, plan_info, index_doc,
                                    index_bytes, index_sha, progress)

    verify_remote_directories(client)
    current["last_success"] = latest_payload["last_success"]
    current["phase"] = "complete"
    atomic_json(state_path, current)
    return {
        "status": "complete",
        "generation_id": generation_id,
        "counts": plan_info["counts"],
        "source_sha256": plan_info["source"]["sha256"],
        "index_sha256": index_sha,
    }


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--generation-dir", required=True, type=Path)
    parser.add_argument("--generation-id", required=True)
    parser.add_argument("--remote", required=True)
    parser.add_argument("--state-dir", required=True, type=Path)
    args = parser.parse_args(argv)
    phase = "configuration"
    state_dir = None
    state_path = None
    try:
        if not valid_generation_id(args.generation_id):
            raise PublishError("CONFIG_ERROR")
        remote = validate_remote(args.remote)
        state_dir = ensure_private_state_dir(args.state_dir)
        with PublishLock(state_dir):
            state_path, _current = _current_state(state_dir)
            try:
                result = publish_generation(args.generation_dir, args.generation_id, remote,
                                            state_dir, rclone_bin="rclone")
            except Exception as exc:
                phase = "failed"
                try:
                    _path, current = _current_state(state_dir)
                    current = dict(current)
                    at = current.get("last_attempt") or now_utc()
                    category = exc.category if isinstance(exc, PublishError) else "INTERNAL_ERROR"
                    current["last_failure"] = {"at": at, "category": category}
                    current["phase"] = phase
                    current["generation_id"] = args.generation_id
                    atomic_json(state_path, current)
                except Exception:
                    pass
                raise
            print(json.dumps(result, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
            return 0
    except PublishError as exc:
        sys.stderr.write(f"PUBLISH_FAILED {exc.category}\n")
        return 1
    except Exception:
        sys.stderr.write("PUBLISH_FAILED INTERNAL_ERROR\n")
        return 1


if __name__ == "__main__":
    sys.exit(main())
