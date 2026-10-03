"""Read a verified lossless SQLite baseline plus an ordered delta chain.

The reader overlays only tables requested by the caller in a private SQLite
cache. It never assembles the unified source database and does not access NAS,
Drive, or the source database.
"""

from __future__ import annotations

import copy
from contextlib import closing, contextmanager
import hashlib
import itertools
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import sqlite3
import stat
import tempfile
from typing import Any, Callable, Generator, Iterable, Iterator, Optional, Sequence

from . import delta_transport as _delta
from . import lossless_sqlite as _lossless
from .change_feed import CHANGE_TABLE
from .native_candidates import NativeTransportCandidates
from .shard_reader import LosslessShardReader
from .slice_selectors import UNCLASSIFIED, rule_token as _rule_token
from .verified_files import verify_files


_GENERATION = re.compile(r"[0-9]{8}T[0-9]{6}Z-[0-9a-f]{8}\Z", re.ASCII)
_SHA256 = re.compile(r"[0-9a-f]{64}\Z", re.ASCII)
_RELATIVE = re.compile(r"[A-Za-z0-9._/-]+\Z", re.ASCII)
_MODE = re.compile(r"[a-z0-9_]{1,64}\Z", re.ASCII)
_RULE = re.compile(r"[A-Za-z0-9_]{1,64}\Z", re.ASCII)
_XLSX_PIECE_NAME = re.compile(r"[A-Za-z0-9_.-]+\.xlsx\Z", re.ASCII)
_PLAN_KEYS = {
    "version", "status", "role", "generation_id", "baseline_generation_id",
    "baseline_source_sha256", "parent_generation_id", "expected_previous_generation_id",
    "replaces_delta_chain", "supersedes_generation_id", "kind", "captured_at",
    "after_event_id", "through_event_id", "source_schema_sha256", "source_row_counts",
    "source_table_columns", "source_foreign_keys", "schemas", "metadata", "transport",
    "transport_database", "value_verification", "xlsx_verification", "files",
    "max_part_bytes", "coverage",
}
_PLAN_COVERAGE = {
    "all_changed_values": True,
    "all_source_tables_metadata": True,
    "full_schema": True,
    "original_row_identities": True,
    "baseline_gap_reconciled": None,
}
_XINFO_KEYS = {"cid", "name", "type", "notnull", "dflt_value", "pk", "hidden"}
_FOREIGN_KEYS = {"id", "seq", "table", "from", "to", "on_update", "on_delete", "match"}
_OPERATION_FIELDS = (
    "operation_ordinal", "table_name", "operation", "source_rowid", "identity_json",
    "row_ordinal", "transport_rowid", "identity_key",
)
_GENERATION_INDEX_KEYS = {
    "version", "role", "generation_id", "plan_sha256", "baseline_generation_id",
    "baseline_source_sha256", "parent_generation_id", "expected_previous_generation_id",
    "replaces_delta_chain", "supersedes_generation_id", "kind", "captured_at",
    "captured_at_kind", "completed_at", "after_event_id", "through_event_id",
    "source_schema_sha256", "source_row_counts", "source_table_columns",
    "source_foreign_keys", "schemas", "coverage", "transport", "transport_database",
    "value_verification", "xlsx_verification", "files", "verification",
}


class DeltaReaderError(ValueError):
    """A stable error code without leaking source cell contents."""

    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


@contextmanager
def _close_iterator_preserving_exception(iterator: Any) -> Iterator[Any]:
    """Close a candidate iterator without masking its active exception."""
    primary_error = False
    try:
        yield iterator
    except BaseException:
        primary_error = True
        raise
    finally:
        close = getattr(iterator, "close", None)
        if callable(close):
            try:
                close()
            except BaseException:
                if not primary_error:
                    raise


def _canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _same_json(left: Any, right: Any) -> bool:
    try:
        return _canonical(left) == _canonical(right)
    except (TypeError, ValueError, UnicodeError):
        return False


def _json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _read_json_raw(path: Path, code: str) -> tuple[dict[str, Any], bytes]:
    try:
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        try:
            if not stat.S_ISREG(os.fstat(descriptor).st_mode):
                raise ValueError(code)
            with os.fdopen(descriptor, "rb", closefd=False) as stream:
                raw = stream.read()
        finally:
            os.close(descriptor)
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=_json_object)
    except Exception as exc:
        raise ValueError(code) from exc
    if not isinstance(value, dict):
        raise ValueError(code)
    return value, raw


def _read_json(path: Path, code: str) -> dict[str, Any]:
    return _read_json_raw(path, code)[0]


def _safe_root(value: Any, code: str) -> Path:
    try:
        raw = Path(os.fspath(value))
    except (TypeError, ValueError, OSError) as exc:
        raise ValueError(code) from exc
    if not raw.parts or ".." in raw.parts:
        raise ValueError(code)
    absolute = Path(os.path.abspath(os.fspath(raw)))
    for component in (absolute, *absolute.parents):
        try:
            info = component.lstat()
        except FileNotFoundError:
            continue
        except OSError as exc:
            raise ValueError(code) from exc
        if stat.S_ISLNK(info.st_mode):
            raise ValueError(code)
    if not absolute.is_dir():
        raise ValueError(code)
    return absolute.resolve(strict=True)


def _safe_relative(value: Any, code: str) -> str:
    if (not isinstance(value, str) or not value or "\\" in value or "\x00" in value
            or not _RELATIVE.fullmatch(value)):
        raise ValueError(code)
    path = PurePosixPath(value)
    if path.is_absolute() or path.as_posix() != value or any(part in ("", ".", "..") for part in path.parts):
        raise ValueError(code)
    return value


def _safe_file(root: Path, relative: str, code: str) -> Path:
    relative = _safe_relative(relative, code)
    target = root.joinpath(*PurePosixPath(relative).parts)
    try:
        target.relative_to(root)
    except ValueError as exc:
        raise ValueError(code) from exc
    cursor = root
    for part in PurePosixPath(relative).parts:
        cursor = cursor / part
        try:
            mode = cursor.lstat().st_mode
        except OSError as exc:
            raise ValueError(code) from exc
        if stat.S_ISLNK(mode):
            raise ValueError(code)
    if not stat.S_ISREG(target.lstat().st_mode):
        raise ValueError(code)
    return target


def _file_digest(path: Path) -> dict[str, Any]:
    digest = hashlib.sha256()
    total = 0
    try:
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    except OSError as exc:
        raise ValueError("GENERATION_FILE_UNREADABLE") from exc
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise ValueError("GENERATION_FILE_UNREADABLE")
        with os.fdopen(descriptor, "rb", closefd=False) as stream:
            for block in iter(lambda: stream.read(1024 * 1024), b""):
                total += len(block)
                digest.update(block)
    finally:
        os.close(descriptor)
    return {"bytes": total, "sha256": digest.hexdigest()}


def _file_inventory(root: Path) -> set[str]:
    found: set[str] = set()
    for current, directories, files in os.walk(root, followlinks=False):
        current_path = Path(current)
        for name in list(directories):
            candidate = current_path / name
            try:
                if stat.S_ISLNK(candidate.lstat().st_mode) or not stat.S_ISDIR(candidate.lstat().st_mode):
                    raise ValueError("GENERATION_TREE_UNSAFE")
            except OSError as exc:
                raise ValueError("GENERATION_TREE_UNSAFE") from exc
        for name in files:
            candidate = current_path / name
            try:
                mode = candidate.lstat().st_mode
            except OSError as exc:
                raise ValueError("GENERATION_TREE_UNSAFE") from exc
            if not stat.S_ISREG(mode):
                raise ValueError("GENERATION_TREE_UNSAFE")
            found.add(candidate.relative_to(root).as_posix())
    return found


def _require_generation(value: Any) -> str:
    if not isinstance(value, str) or not _GENERATION.fullmatch(value):
        raise ValueError("GENERATION_ID_INVALID")
    return value


def _require_sha(value: Any) -> str:
    if not isinstance(value, str) or not _SHA256.fullmatch(value):
        raise ValueError("SHA256_INVALID")
    return value


def _typed_value(value: Any) -> tuple[str, Any]:
    if value is None:
        return "null", None
    if type(value) is int:
        if not _delta._MIN_INT64 <= value <= _delta._MAX_INT64:
            raise ValueError("ROW_VALUE_INVALID")
        return "integer", str(value)
    if type(value) is float:
        if value != value:
            raise ValueError("ROW_VALUE_INVALID")
        # Change-feed identity JSON stores SQLite's 17-digit decimal rendering;
        # converting it back to a float yields the exact same binary value.
        return "real", format(value, ".17g")
    if type(value) is str:
        return "text", value
    if type(value) is bytes:
        return "blob", value.hex().upper()
    raise ValueError("ROW_VALUE_INVALID")


def _identity_key_for_values(
    table: str, source_rowid: Optional[int], values: Sequence[Any],
    columns: Sequence[str], column_schema: Sequence[dict[str, Any]],
    *, synthetic_suffix: Optional[str] = None,
) -> str:
    if source_rowid is not None:
        if type(source_rowid) is not int or not _delta._MIN_INT64 <= source_rowid <= _delta._MAX_INT64:
            raise ValueError("ROW_IDENTITY_INVALID")
        return _delta._identity_key(table, source_rowid, None)  # type: ignore[return-value]
    primary = sorted((item for item in column_schema if item["pk"]), key=lambda item: item["pk"])
    if primary:
        parts = []
        indexes = {name: index for index, name in enumerate(columns)}
        for column in primary:
            name = column["name"]
            if name not in indexes:
                raise ValueError("ROW_IDENTITY_INVALID")
            kind, encoded = _typed_value(values[indexes[name]])
            parts.append({"column": name, "sqlite_type": kind, "value": encoded})
        identity_json = _canonical(parts)
        return _delta._identity_key(table, None, identity_json)  # type: ignore[return-value]
    if synthetic_suffix is None:
        raise ValueError("ROW_IDENTITY_INVALID")
    return _canonical([table, "replacement_ordinal", synthetic_suffix])


def _identity_key_for_record(record: dict[str, Any]) -> str:
    key = _delta._identity_key(record["table_name"], record["source_rowid"], record["identity_json"])
    if key is None:
        raise ValueError("ROW_IDENTITY_INVALID")
    return key


def _expected_delta_document(plan: dict[str, Any], root: Path) -> tuple[dict[str, Any], list[str], dict[str, list[dict[str, Any]]], dict[str, list[str]]]:
    document = _read_json(root / "transport-document.json", "TRANSPORT_DOCUMENT_INVALID")
    metadata = plan["metadata"]
    table_names, xinfo_by_table, visible_by_table = _delta._table_specs(metadata)
    required = {
        "version", "status", "generation_id", "parent_generation_id", "baseline_generation_id",
        "kind", "captured_at", "after_event_id", "through_event_id", "source_schema_sha256",
        "source_row_counts", "source_table_columns", "schemas", "metadata", "internal_names",
        "table_names", "upsert_counts_by_table", "operation_counts", "database",
    }
    if (set(document) != required or type(document.get("version")) is not int
            or document.get("version") != 1 or document.get("status") != "built"):
        raise ValueError("TRANSPORT_DOCUMENT_INVALID")
    expected_internal = _delta._internal_names(metadata, table_names)
    duplicate_fields = {
        "generation_id": metadata["generation_id"],
        "parent_generation_id": metadata["parent_generation_id"],
        "baseline_generation_id": metadata["baseline_generation_id"],
        "kind": metadata["kind"],
        "captured_at": metadata["captured_at"],
        "after_event_id": metadata["after_event_id"],
        "through_event_id": metadata["through_event_id"],
        "source_schema_sha256": metadata["source_schema_sha256"],
        "source_row_counts": metadata["source_row_counts"],
        "source_table_columns": metadata["source_table_columns"],
        "schemas": metadata["schemas"],
        "metadata": metadata,
        "internal_names": expected_internal,
        "table_names": table_names,
    }
    if any(not _same_json(document.get(key), value) for key, value in duplicate_fields.items()):
        raise ValueError("TRANSPORT_DOCUMENT_INVALID")
    database = document.get("database")
    if (not isinstance(database, dict) or set(database) != {"path", "bytes", "sha256"}
            or not isinstance(database.get("path"), str) or not database["path"]
            or type(database.get("bytes")) is not int or database["bytes"] <= 0
            or not isinstance(database.get("sha256"), str) or not _SHA256.fullmatch(database["sha256"])):
        raise ValueError("TRANSPORT_DOCUMENT_INVALID")
    upsert_counts = document.get("upsert_counts_by_table")
    operation_counts = document.get("operation_counts")
    if (not isinstance(upsert_counts, dict)
            or any(name not in table_names or type(count) is not int or count <= 0
                   for name, count in upsert_counts.items())
            or not isinstance(operation_counts, dict)
            or set(operation_counts) != {"upsert", "delete", "clear_table"}
            or any(type(count) is not int or count < 0 for count in operation_counts.values())):
        raise ValueError("TRANSPORT_DOCUMENT_INVALID")
    return document, table_names, xinfo_by_table, visible_by_table


def _validate_xlsx_bundle(root: Path, plan: dict[str, Any], file_map: dict[str, dict[str, Any]]) -> set[str]:
    index_path = _safe_file(root, "xlsx/index.json", "XLSX_INDEX_INVALID")
    manifest_path = _safe_file(root, "xlsx/manifest.json", "XLSX_MANIFEST_INVALID")
    proof_path = _safe_file(root, "xlsx/verification.json", "XLSX_PROOF_INVALID")
    index = _read_json(index_path, "XLSX_INDEX_INVALID")
    manifest = _read_json(manifest_path, "XLSX_MANIFEST_INVALID")
    proof = _read_json(proof_path, "XLSX_PROOF_INVALID")
    generation = plan["generation_id"]
    database = plan["transport_database"]
    expected_schema, expected_xinfo = _expected_transport_schema(plan["metadata"])
    expected_table_names = {item["name"] for item in expected_schema if item["type"] == "table"}
    if (type(index.get("version")) is not int or index.get("version") != 2
            or index.get("snapshot_id") != generation
            or not _same_json(index.get("schema_objects"), expected_schema)):
        raise ValueError("XLSX_INDEX_INVALID")
    if (proof.get("version") != 1 or proof.get("status") != "verified"
            or proof.get("source_rowids_verified") is not True
            or proof.get("snapshot_identifier") != generation
            or proof.get("snapshot_sha256") != database["sha256"]
            or proof.get("source") != {
                "path": index.get("source", {}).get("path"),
                "bytes": database["bytes"], "sha256": database["sha256"],
            }
            or proof.get("manifest") != "manifest.json" or proof.get("index") != "index.json"):
        raise ValueError("XLSX_PROOF_INVALID")
    source = index.get("source")
    if (index.get("status") != "verified" or index.get("verification_status") != "verified"
            or index.get("snapshot_identifier") != generation
            or index.get("snapshot_sha256") != database["sha256"]
            or index.get("row_identity_format") != "source_rowid_column_v1"
            or index.get("verification_receipt") != "verification.json"
            or not isinstance(source, dict) or source.get("bytes") != database["bytes"]
            or source.get("sha256") != database["sha256"]):
        raise ValueError("XLSX_INDEX_INVALID")
    if (manifest.get("version") != 1 or manifest.get("status") != "verified"
            or manifest.get("snapshot_identifier") != generation
            or manifest.get("snapshot_sha256") != database["sha256"]
            or manifest.get("source") != source
            or manifest.get("counts") != index.get("counts")
            or manifest.get("files") != proof.get("files")
            or manifest.get("verification_receipt") != "verification.json"):
        raise ValueError("XLSX_MANIFEST_INVALID")
    if proof.get("counts") != index.get("counts"):
        raise ValueError("XLSX_PROOF_INVALID")
    pieces = index.get("pieces")
    table_map = index.get("tables")
    if (not isinstance(pieces, list) or not isinstance(table_map, dict)
            or set(table_map) != expected_table_names):
        raise ValueError("XLSX_INDEX_INVALID")
    # The transport database document has no row-count map. Counts are instead
    # derived from operation receipts for source tables and the fixed metadata
    # and operation tables in the native transport schema.
    metadata = plan["metadata"]
    source_tables, _xinfo, visible = _delta._table_specs(metadata)
    internal = _delta._internal_names(metadata, source_tables)
    operation_counts = plan["value_verification"]["operation_counts"]
    expected_rows = {name: plan["value_verification"]["upsert_counts_by_table"].get(name, 0)
                     for name in source_tables}
    expected_rows[internal["operations_table"]] = sum(operation_counts.values())
    expected_rows[internal["metadata_table"]] = 1
    if set(expected_rows) != expected_table_names:
        raise ValueError("XLSX_INDEX_INVALID")
    piece_names: list[str] = []
    piece_by_name: dict[str, dict[str, Any]] = {}
    for piece in pieces:
        if not isinstance(piece, dict):
            raise ValueError("XLSX_INDEX_INVALID")
        name = piece.get("name")
        if (not isinstance(name, str) or not _XLSX_PIECE_NAME.fullmatch(name)
                or Path(name).name != name or name in piece_by_name
                or type(piece.get("bytes")) is not int or piece["bytes"] <= 0
                or not isinstance(piece.get("sha256"), str) or not _SHA256.fullmatch(piece["sha256"])
                or type(piece.get("chunk_count")) is not int or piece["chunk_count"] < 0):
            raise ValueError("XLSX_INDEX_INVALID")
        piece_names.append(name)
        piece_by_name[name] = piece
    if index.get("piece_names") != piece_names:
        raise ValueError("XLSX_INDEX_INVALID")
    seen_table_pieces: list[str] = []
    for table, info in table_map.items():
        if (not isinstance(table, str) or not isinstance(info, dict)
                or not isinstance(info.get("pieces"), list)):
            raise ValueError("XLSX_INDEX_INVALID")
        expected_columns = [item["name"] for item in expected_xinfo[table] if item["hidden"] != 1]
        expected_sql = next((item["sql"] for item in expected_schema
                             if item["type"] == "table" and item["name"] == table), None)
        row_count = expected_rows[table]
        expected_cells = row_count * len(expected_columns)
        if (info.get("table") != table
                or info.get("columns") != expected_columns
                or info.get("column_schema") != expected_xinfo[table]
                or info.get("ddl") != expected_sql or info.get("table_ddl") != expected_sql
                or type(info.get("source_row_count")) is not int
                or info["source_row_count"] != row_count
                or type(info.get("read_row_count")) is not int
                or info["read_row_count"] != row_count
                or type(info.get("source_cell_count")) is not int
                or info["source_cell_count"] != expected_cells
                or type(info.get("read_cell_count")) is not int
                or info["read_cell_count"] != expected_cells):
            raise ValueError("XLSX_INDEX_INVALID")
        table_names = []
        for item in info["pieces"]:
            if not isinstance(item, dict) or item.get("table") != table:
                raise ValueError("XLSX_INDEX_INVALID")
            name = item.get("name")
            if name not in piece_by_name or not _same_json(item, piece_by_name[name]):
                raise ValueError("XLSX_INDEX_INVALID")
            table_names.append(name)
        if info.get("piece_names") != table_names:
            raise ValueError("XLSX_INDEX_INVALID")
        seen_table_pieces.extend(table_names)
    if sorted(seen_table_pieces) != sorted(piece_names) or len(seen_table_pieces) != len(set(seen_table_pieces)):
        raise ValueError("XLSX_INDEX_INVALID")
    index_counts = index.get("counts")
    expected_exported_rows = sum(expected_rows.values())
    expected_exported_cells = sum(
        expected_rows[name] * len([item for item in expected_xinfo[name] if item["hidden"] != 1])
        for name in expected_table_names
    )
    expected_chunks = sum(piece["chunk_count"] for piece in pieces)
    if (not isinstance(index_counts, dict)
            or type(index_counts.get("exported_tables")) is not int
            or index_counts["exported_tables"] != len(expected_table_names)
            or type(index_counts.get("exported_rows")) is not int
            or index_counts["exported_rows"] != expected_exported_rows
            or type(index_counts.get("exported_cells")) is not int
            or index_counts["exported_cells"] != expected_exported_cells
            or type(index_counts.get("chunks")) is not int
            or index_counts["chunks"] != expected_chunks
            or type(index_counts.get("pieces")) is not int
            or index_counts["pieces"] != len(pieces)
            or index_counts.get("source_rowids_verified") is not True
            or type(index_counts.get("source_rowid_rows_checked")) is not int
            or not 0 <= index_counts["source_rowid_rows_checked"] <= expected_exported_rows
            or index_counts.get("source_bytes") != database["bytes"]):
        raise ValueError("XLSX_INDEX_INVALID")
    output_files = proof.get("files")
    if not isinstance(output_files, list) or manifest.get("files") != output_files:
        raise ValueError("XLSX_PROOF_INVALID")
    expected_output = {
        (piece["name"], piece["bytes"], piece["sha256"]) for piece in pieces
    }
    index_record = _file_digest(index_path)
    expected_output.add(("index.json", index_record["bytes"], index_record["sha256"]))
    found_output = set()
    for item in output_files:
        if (not isinstance(item, dict) or set(item) != {"name", "bytes", "sha256"}
                or not isinstance(item.get("name"), str) or type(item.get("bytes")) is not int
                or item["bytes"] <= 0 or not isinstance(item.get("sha256"), str)
                or not _SHA256.fullmatch(item["sha256"])):
            raise ValueError("XLSX_PROOF_INVALID")
        found_output.add((item["name"], item["bytes"], item["sha256"]))
    if found_output != expected_output or len(found_output) != len(output_files):
        raise ValueError("XLSX_PROOF_INVALID")
    expected_local = {"xlsx/index.json", "xlsx/manifest.json", "xlsx/verification.json"}
    for name, piece in piece_by_name.items():
        relative = f"xlsx/{name}"
        entry = file_map.get(relative)
        if entry != {"bytes": piece["bytes"], "sha256": piece["sha256"]}:
            raise ValueError("XLSX_FILE_HASH_MISMATCH")
        expected_local.add(relative)
        path = _safe_file(root, relative, "XLSX_FILE_INVALID")
        if _file_digest(path) != entry:
            raise ValueError("XLSX_FILE_HASH_MISMATCH")
    for relative in ("xlsx/index.json", "xlsx/manifest.json", "xlsx/verification.json"):
        if relative not in file_map:
            raise ValueError("XLSX_FILE_MISSING")
    return expected_local


def _validate_value_proof(plan: dict[str, Any], document: dict[str, Any], proof: dict[str, Any]) -> None:
    metadata = plan["metadata"]
    if (not _same_json(proof, plan["value_verification"])
            or proof.get("version") != 1 or proof.get("status") != "verified"
            or proof.get("generation_id") != plan["generation_id"]
            or proof.get("parent_generation_id") != plan["parent_generation_id"]
            or proof.get("baseline_generation_id") != plan["baseline_generation_id"]
            or proof.get("kind") != plan["kind"]
            or proof.get("captured_at") != plan["captured_at"]
            or proof.get("source_schema_sha256") != plan["source_schema_sha256"]
            or not _same_json(proof.get("source_row_counts"), plan["source_row_counts"])
            or not _same_json(proof.get("database"), plan["transport_database"])
            or not _same_json(proof.get("operation_counts"), document.get("operation_counts"))
            or not _same_json(proof.get("upsert_counts_by_table"), document.get("upsert_counts_by_table"))
            or document.get("database") != plan["transport_database"]
            or not _same_json(document.get("metadata"), metadata)):
        raise ValueError("VALUE_PROOF_INVALID")


def _validate_shard_proof(root: Path, plan: dict[str, Any], file_map: dict[str, dict[str, Any]],
                          document: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    transport = plan["transport"]
    if (not isinstance(transport, dict) or set(transport) != {"kind", "path", "verification"}
            or transport.get("kind") != "lossless_sqlite_shards"
            or transport.get("path") != "transport/manifest.json"):
        raise ValueError("TRANSPORT_SHARD_REFERENCE_INVALID")
    package_root = root / "transport"
    manifest_path = _safe_file(root, transport["path"], "TRANSPORT_SHARD_MANIFEST_INVALID")
    proof_path = _safe_file(root, "transport/verification.json", "TRANSPORT_SHARD_PROOF_INVALID")
    manifest = _read_json(manifest_path, "TRANSPORT_SHARD_MANIFEST_INVALID")
    proof = _read_json(proof_path, "TRANSPORT_SHARD_PROOF_INVALID")
    if (not _same_json(proof, transport["verification"])
            or manifest.get("version") != _lossless.MANIFEST_VERSION
            or manifest.get("role") != _lossless.MANIFEST_ROLE
            or manifest.get("snapshot_identifier") != plan["generation_id"]
            or manifest.get("source_schema_sha256") != _lossless._schema_sha256(manifest.get("schema_objects"))):
        raise ValueError("TRANSPORT_SHARD_PROOF_INVALID")
    coverage = manifest.get("coverage")
    if not isinstance(coverage, dict) or any(coverage.get(key) is not True for key in (
            "all_tables", "all_rows", "all_columns", "all_values", "external_values")):
        raise ValueError("TRANSPORT_SHARD_COVERAGE_INVALID")
    tables = _lossless._manifest_tables(manifest)
    part_files, external_files, selector_files = _lossless._declared_files(manifest, package_root)
    if selector_files:
        raise ValueError("TRANSPORT_SHARD_SELECTOR_UNEXPECTED")
    declared = set(part_files) | set(external_files)
    _lossless._check_manifest_disk(package_root, manifest)
    _lossless._check_unlisted_sqlite_files(package_root, declared)
    if _file_inventory(package_root) != declared | {"manifest.json", "verification.json"}:
        raise ValueError("TRANSPORT_SHARD_INVENTORY_INVALID")
    for relative, entry in {**part_files, **external_files}.items():
        if _file_digest(_safe_file(package_root, relative, "TRANSPORT_SHARD_FILE_INVALID")) != {
                "bytes": entry["bytes"], "sha256": entry["sha256"]}:
            raise ValueError("TRANSPORT_SHARD_FILE_HASH_MISMATCH")
        local = f"transport/{relative}"
        if file_map.get(local) != {"bytes": entry["bytes"], "sha256": entry["sha256"]}:
            raise ValueError("TRANSPORT_SHARD_FILE_HASH_MISMATCH")
    for relative in ("transport/manifest.json", "transport/verification.json"):
        if relative not in file_map or _file_digest(_safe_file(root, relative, "TRANSPORT_SHARD_FILE_INVALID")) != file_map[relative]:
            raise ValueError("TRANSPORT_SHARD_FILE_HASH_MISMATCH")
    _validate_shard_manifest_shape(
        package_root, manifest, proof, tables, part_files, external_files, document
    )
    return manifest, proof


def _validate_shard_manifest_shape(
    package_root: Path, manifest: dict[str, Any], proof: dict[str, Any], tables: list[dict[str, Any]],
    part_files: dict[str, dict[str, Any]], external_files: dict[str, dict[str, Any]],
    document: dict[str, Any],
) -> None:
    source_tables, xinfo, visible = _delta._table_specs(document["metadata"])
    internal = _delta._internal_names(document["metadata"], source_tables)
    expected_names = set(source_tables) | {internal["operations_table"], internal["metadata_table"]}
    if {item["name"] for item in tables} != expected_names:
        raise ValueError("TRANSPORT_SHARD_TABLE_INVENTORY_INVALID")
    table_docs = {item["name"]: item for item in tables}
    expected_objects, expected_xinfo = _expected_transport_schema(document["metadata"])
    if not _same_json(manifest.get("schema_objects"), expected_objects):
        raise ValueError("TRANSPORT_SHARD_SCHEMA_INVALID")
    expected_counts = dict(document["upsert_counts_by_table"])
    for name in source_tables:
        expected_counts.setdefault(name, 0)
    expected_counts[internal["operations_table"]] = sum(document["operation_counts"].values())
    expected_counts[internal["metadata_table"]] = 1
    for table in source_tables:
        item = table_docs[table]
        if (item.get("columns") != visible[table]
                or item.get("column_schema") != expected_xinfo[table]
                or item.get("foreign_keys") != []):
            raise ValueError("TRANSPORT_SHARD_COLUMNS_INVALID")
        if type(item.get("row_count")) is not int or item["row_count"] != expected_counts[table]:
            raise ValueError("TRANSPORT_SHARD_COUNTS_INVALID")
    ops_name, metadata_name = internal["operations_table"], internal["metadata_table"]
    for name, code in ((ops_name, "TRANSPORT_SHARD_OPERATIONS_INVALID"),
                       (metadata_name, "TRANSPORT_SHARD_METADATA_INVALID")):
        if (table_docs[name].get("columns") != [item["name"] for item in expected_xinfo[name]
                                               if item["hidden"] != 1]
                or table_docs[name].get("column_schema") != expected_xinfo[name]
                or table_docs[name].get("foreign_keys") != []
                or type(table_docs[name].get("row_count")) is not int
                or table_docs[name]["row_count"] != expected_counts[name]):
            raise ValueError(code)
    expected_schema_sha = _lossless._schema_sha256(manifest.get("schema_objects"))
    if manifest.get("source_schema_sha256") != expected_schema_sha:
        raise ValueError("TRANSPORT_SHARD_SCHEMA_INVALID")
    # Per-value-file cell_count records incidences: a single source cell is
    # counted once in every value file containing one of its chunks. Count the
    # original cells from the immutable part metadata instead of summing those
    # incidences or trusting a manifest/proof counter.
    expected_external_cells = 0
    expected_external_schema = [
        ("table_id", "TEXT"),
        ("row_ordinal", "INTEGER"),
        ("column_ordinal", "INTEGER"),
        ("sqlite_type", "TEXT"),
        ("byte_length", "INTEGER"),
        ("sha256", "TEXT"),
        ("chunk_total", "INTEGER"),
        ("value_files_json", "TEXT"),
    ]
    for table in tables:
        metadata_tables = table.get("archive_metadata_tables")
        if (not isinstance(metadata_tables, dict)
                or not isinstance(metadata_tables.get("external_cells"), str)
                or not metadata_tables["external_cells"]
                or "\x00" in metadata_tables["external_cells"]):
            raise ValueError("TRANSPORT_SHARD_EXTERNAL_METADATA_INVALID")
        external_table = metadata_tables["external_cells"]
        for part in table["parts"]:
            part_path = _lossless._safe_file(package_root, part["file"])
            try:
                with closing(_lossless._open_readonly(part_path)) as connection:
                    external_schema = [
                        (row[1], row[2])
                        for row in connection.execute(
                            f"PRAGMA table_xinfo({_quote(external_table)})"
                        )
                    ]
                    if external_schema != expected_external_schema:
                        raise ValueError("TRANSPORT_SHARD_EXTERNAL_METADATA_INVALID")
                    count = connection.execute(
                        f"SELECT COUNT(*) FROM {_quote(external_table)}"
                    ).fetchone()[0]
            except (OSError, sqlite3.Error, ValueError) as exc:
                if isinstance(exc, ValueError) and str(exc) == "TRANSPORT_SHARD_EXTERNAL_METADATA_INVALID":
                    raise
                raise ValueError("TRANSPORT_SHARD_EXTERNAL_METADATA_INVALID") from exc
            if type(count) is not int or count < 0:
                raise ValueError("TRANSPORT_SHARD_EXTERNAL_METADATA_INVALID")
            expected_external_cells += count

    if any(type(item.get("cell_count")) is not int or item["cell_count"] <= 0
           for item in external_files.values()):
        raise ValueError("TRANSPORT_SHARD_EXTERNAL_METADATA_INVALID")
    external_incidences = sum(item["cell_count"] for item in external_files.values())
    if (expected_external_cells > external_incidences
            or (external_incidences > 0 and expected_external_cells == 0)):
        raise ValueError("TRANSPORT_SHARD_EXTERNAL_METADATA_INVALID")
    receipt_counts = {
        "table_count": len(tables),
        "row_counts": {item["name"]: item["row_count"] for item in tables},
        "row_count": sum(item["row_count"] for item in tables),
        "cell_count": sum(item["row_count"] * len(item["columns"]) for item in tables),
        "external_cell_count": expected_external_cells,
        "value_chunk_count": sum(item.get("chunk_count", 0) for item in external_files.values()),
        "file_count": len(part_files) + len(external_files),
    }
    if (type(proof.get("version")) is not int or proof.get("version") != _lossless.MANIFEST_VERSION
            or proof.get("role") != _lossless.MANIFEST_ROLE
            or proof.get("status") != "verified"
            or proof.get("snapshot_identifier") != manifest.get("snapshot_identifier")
            or proof.get("source_schema_sha256") != manifest.get("source_schema_sha256")
            or any(type(proof.get(key)) is not int or proof.get(key) != value
                   for key, value in receipt_counts.items() if key != "row_counts")
            or not _same_json(proof.get("row_counts"), receipt_counts["row_counts"])
            or not isinstance(proof.get("coverage"), dict)
            or any(proof["coverage"].get(key) is not True for key in (
                "all_tables", "all_rows", "all_columns", "all_values", "external_values"))):
        raise ValueError("TRANSPORT_SHARD_PROOF_INVALID")
    manifest_counts = manifest.get("counts")
    if (not isinstance(manifest_counts, dict)
            or manifest_counts.get("exported_tables") != len(tables)
            or manifest_counts.get("exported_rows") != receipt_counts["row_count"]
            or type(manifest_counts.get("external_cells")) is not int
            or manifest_counts["external_cells"] != receipt_counts["external_cell_count"]
            or manifest_counts.get("files") != receipt_counts["file_count"]):
        raise ValueError("TRANSPORT_SHARD_MANIFEST_COUNTS_INVALID")


def _expected_transport_schema(metadata: dict[str, Any]) -> tuple[list[dict[str, Any]], dict[str, list[dict[str, Any]]]]:
    """Build an in-memory empty transport skeleton for exact DDL validation."""
    table_names, xinfo_by_table, visible = _delta._table_specs(metadata)
    internal = _delta._internal_names(metadata, table_names)
    connection = sqlite3.connect(":memory:")
    try:
        _delta._create_source_tables(connection, table_names, xinfo_by_table, visible)
        ops = internal["operations_table"]
        meta = internal["metadata_table"]
        connection.execute(
            f"CREATE TABLE {_quote(ops)} ("
            '"operation_ordinal" INTEGER PRIMARY KEY, "table_name" TEXT NOT NULL, '
            '"operation" TEXT NOT NULL, "source_rowid" INTEGER, "identity_json" TEXT, '
            '"row_ordinal" INTEGER, "transport_rowid" INTEGER, "identity_key" TEXT)'
        )
        connection.execute(
            f"CREATE TABLE {_quote(meta)} (\"name\" TEXT NOT NULL, \"value\" TEXT NOT NULL)"
        )
        connection.execute(
            f"CREATE UNIQUE INDEX {_quote(internal['identity_unique_index'])} "
            f"ON {_quote(ops)} (\"identity_key\") WHERE \"identity_key\" IS NOT NULL"
        )
        connection.execute(
            f"CREATE UNIQUE INDEX {_quote(internal['row_ordinal_unique_index'])} "
            f"ON {_quote(ops)} (\"table_name\", \"row_ordinal\") WHERE \"row_ordinal\" IS NOT NULL"
        )
        connection.execute(
            f"INSERT INTO {_quote(meta)} (\"name\",\"value\") VALUES (?,?)",
            ("source_metadata", _canonical(metadata)),
        )
        _delta._validate_database_inventory(connection, metadata, internal, table_names,
                                            xinfo_by_table, visible)
        objects = [
            {"type": row[0], "name": row[1], "tbl_name": row[2], "sql": row[3]}
            for row in connection.execute(
                "SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name"
            )
        ]
        xinfo: dict[str, list[dict[str, Any]]] = {}
        for table in (*table_names, ops, meta):
            xinfo[table] = [
                {"cid": row[0], "name": row[1], "type": row[2], "notnull": row[3],
                 "dflt_value": row[4], "pk": row[5], "hidden": row[6]}
                for row in connection.execute(f"PRAGMA table_xinfo({_quote(table)})")
            ]
        return objects, xinfo
    finally:
        connection.close()


def _validate_optional_generation_index(root: Path, plan: dict[str, Any], plan_raw: bytes) -> Optional[bytes]:
    index_path = root / "index.json"
    try:
        index_path.lstat()
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise ValueError("DELTA_INDEX_INVALID") from exc

    index_path = _safe_file(root, "index.json", "DELTA_INDEX_INVALID")
    index, _index_raw = _read_json_raw(index_path, "DELTA_INDEX_INVALID")
    if (set(index) != _GENERATION_INDEX_KEYS or type(index.get("version")) is not int
            or index.get("version") != 1 or index.get("role") != "lossless_delta_generation"):
        raise ValueError("DELTA_INDEX_INVALID")

    for key in (
        "generation_id", "baseline_generation_id", "baseline_source_sha256",
        "parent_generation_id", "expected_previous_generation_id", "replaces_delta_chain",
        "supersedes_generation_id", "kind", "captured_at", "after_event_id",
        "through_event_id", "source_schema_sha256", "source_row_counts",
        "source_table_columns", "source_foreign_keys", "schemas", "coverage", "transport",
        "transport_database", "value_verification", "xlsx_verification",
    ):
        if not _same_json(index.get(key), plan.get(key)):
            raise ValueError("DELTA_INDEX_PLAN_MISMATCH")

    expected_files = [*plan["files"], {
        "local": "delta-plan.json",
        "remote": f"deltas/generations/{plan['generation_id']}/delta-plan.json",
        "bytes": len(plan_raw),
        "sha256": hashlib.sha256(plan_raw).hexdigest(),
    }]
    if (index.get("plan_sha256") != hashlib.sha256(plan_raw).hexdigest()
            or not _same_json(index.get("files"), expected_files)
            or index.get("captured_at_kind") != "pinned_read_transaction"
            or not isinstance(index.get("completed_at"), str)
            or not _delta._timestamp(index["completed_at"])):
        raise ValueError("DELTA_INDEX_PLAN_MISMATCH")

    verification = index.get("verification")
    if (not isinstance(verification, dict)
            or set(verification) != {"full_readback", "file_count"}
            or verification.get("full_readback") is not True
            or type(verification.get("file_count")) is not int
            or verification["file_count"] != len(expected_files)):
        raise ValueError("DELTA_INDEX_PROOF_INCOMPLETE")
    return _index_raw


def _validate_generation_plan(
    root: Path,
) -> tuple[dict[str, Any], dict[str, Any], Optional[dict[str, Any]], bool,
           list[tuple[Any, dict[str, dict[str, Any]]]]]:
    plan_path = _safe_file(root, "delta-plan.json", "DELTA_PLAN_INVALID")
    plan, plan_raw = _read_json_raw(plan_path, "DELTA_PLAN_INVALID")
    if (set(plan) != _PLAN_KEYS or type(plan.get("version")) is not int or plan["version"] != 1
            or plan.get("status") != "prepared" or plan.get("role") != "lossless_delta_generation"):
        raise ValueError("DELTA_PLAN_INVALID")
    generation = _require_generation(plan.get("generation_id"))
    baseline = _require_generation(plan.get("baseline_generation_id"))
    _require_sha(plan.get("baseline_source_sha256"))
    parent = _require_generation(plan.get("parent_generation_id"))
    _require_generation(plan.get("expected_previous_generation_id"))
    if plan.get("supersedes_generation_id") is not None:
        _require_generation(plan["supersedes_generation_id"])
    reset = plan.get("replaces_delta_chain")
    if type(reset) is not bool:
        raise ValueError("DELTA_PLAN_INVALID")
    kind = plan.get("kind")
    if kind not in ("change_feed", "baseline_reconciliation") or reset != (kind == "baseline_reconciliation"):
        raise ValueError("DELTA_CHAIN_KIND_INVALID")
    if ((not reset and plan.get("supersedes_generation_id") is not None)
            or (reset and parent != baseline)):
        raise ValueError("DELTA_CHAIN_KIND_INVALID")
    expected_previous = baseline if reset and plan.get("supersedes_generation_id") is None else (
        plan.get("supersedes_generation_id") if reset else parent
    )
    if plan.get("expected_previous_generation_id") != expected_previous:
        raise ValueError("DELTA_PARENT_CHAIN_INVALID")
    captured = plan.get("captured_at")
    if not isinstance(captured, str) or not _delta._timestamp(captured):
        raise ValueError("DELTA_CAPTURE_TIME_INVALID")
    for key in ("after_event_id", "through_event_id"):
        if type(plan.get(key)) is not int or plan[key] < 0:
            raise ValueError("DELTA_EVENT_RANGE_INVALID")
    if plan["after_event_id"] > plan["through_event_id"]:
        raise ValueError("DELTA_EVENT_RANGE_INVALID")
    _require_sha(plan.get("source_schema_sha256"))
    metadata = plan.get("metadata")
    if not isinstance(metadata, dict):
        raise ValueError("DELTA_PLAN_METADATA_INVALID")
    if not reset:
        writer_guard = metadata.get("writer_guard_status")
        if (metadata.get("all_writers_contract_enforced") is not True
                or not isinstance(writer_guard, dict)
                or writer_guard.get("status") != "verified"
                or writer_guard.get("all_writers_contract_enforced") is not True):
            raise ValueError("DELTA_WRITER_GUARDS_UNVERIFIED")
    table_names, _, _ = _delta._table_specs(metadata)
    for key, expected in (
        ("generation_id", generation), ("baseline_generation_id", baseline),
        ("parent_generation_id", parent), ("kind", kind), ("captured_at", captured),
        ("source_schema_sha256", plan["source_schema_sha256"]),
        ("source_row_counts", plan.get("source_row_counts")),
        ("source_table_columns", plan.get("source_table_columns")),
        ("source_foreign_keys", plan.get("source_foreign_keys")),
        ("schemas", plan.get("schemas")),
    ):
        if not _same_json(metadata.get(key), expected):
            raise ValueError("DELTA_PLAN_METADATA_INVALID")
    if (metadata.get("baseline_source_sha256") != plan["baseline_source_sha256"]
            or metadata.get("after_event_id") != plan["after_event_id"]
            or metadata.get("through_event_id") != plan["through_event_id"]
            or metadata.get("replaces_delta_chain") is not reset
            or metadata.get("captured_at_kind") != "pinned_read_transaction"
            or metadata.get("schema_comparison_required") is not True):
        raise ValueError("DELTA_PLAN_METADATA_INVALID")
    if (not isinstance(plan.get("source_row_counts"), dict)
            or set(plan["source_row_counts"]) != set(table_names)
            or any(type(value) is not int or value < 0 for value in plan["source_row_counts"].values())):
        raise ValueError("DELTA_ROW_COUNTS_INVALID")
    if not _same_json(plan.get("coverage"), {
            **_PLAN_COVERAGE, "baseline_gap_reconciled": reset}):
        raise ValueError("DELTA_COVERAGE_INVALID")
    if type(plan.get("max_part_bytes")) is not int or not 1 <= plan["max_part_bytes"] <= _lossless.MAX_ALLOWED_BYTES:
        raise ValueError("DELTA_PLAN_INVALID")
    if not _same_json(plan.get("metadata"), metadata):
        raise ValueError("DELTA_PLAN_METADATA_INVALID")

    files = plan.get("files")
    if not isinstance(files, list) or not files:
        raise ValueError("DELTA_FILE_INVENTORY_INVALID")
    file_map: dict[str, dict[str, Any]] = {}
    remotes: set[str] = set()
    for item in files:
        if not isinstance(item, dict) or set(item) != {"local", "remote", "bytes", "sha256"}:
            raise ValueError("DELTA_FILE_INVENTORY_INVALID")
        local = _safe_relative(item.get("local"), "DELTA_FILE_PATH_INVALID")
        remote_expected = f"deltas/generations/{generation}/{local}"
        if (item.get("remote") != remote_expected or local in file_map
                or type(item.get("bytes")) is not int or item["bytes"] <= 0
                or not isinstance(item.get("sha256"), str) or not _SHA256.fullmatch(item["sha256"])):
            raise ValueError("DELTA_FILE_INVENTORY_INVALID")
        if item["remote"] in remotes:
            raise ValueError("DELTA_FILE_INVENTORY_INVALID")
        file_map[local] = {"bytes": item["bytes"], "sha256": item["sha256"]}
        remotes.add(item["remote"])
    required_docs = {"transport-document.json", "value-verification.json"}
    if not required_docs <= set(file_map):
        raise ValueError("DELTA_FILE_INVENTORY_INVALID")
    for local in file_map:
        _safe_file(root, local, "DELTA_FILE_PATH_INVALID")
    try:
        artifact_guard = verify_files(root, file_map)
    except Exception as exc:
        if getattr(exc, "category", None) == "VERIFIED_FILES_HASH_MISMATCH":
            raise ValueError("DELTA_FILE_HASH_MISMATCH") from None
        if getattr(exc, "category", None) in {
                "VERIFIED_FILES_CHANGED", "VERIFIED_FILES_SYMLINK", "VERIFIED_FILES_ROOT_INVALID"}:
            raise ValueError("DELTA_FILE_PATH_INVALID") from None
        raise
    document, table_names, _xinfo, _visible = _expected_delta_document(plan, root)
    if not _same_json(document.get("database"), plan.get("transport_database")):
        raise ValueError("TRANSPORT_DOCUMENT_INVALID")
    proof = _read_json(root / "value-verification.json", "VALUE_PROOF_INVALID")
    _validate_value_proof(plan, document, proof)
    xlsx_files = _validate_xlsx_bundle(root, plan, file_map)
    transport = plan.get("transport")
    if not isinstance(transport, dict) or transport.get("kind") not in ("native_sqlite", "lossless_sqlite_shards"):
        raise ValueError("TRANSPORT_REPRESENTATION_INVALID")
    expected_local = set(required_docs) | xlsx_files
    shard_manifest = None
    if transport["kind"] == "native_sqlite":
        if set(transport) != {"kind", "path"} or transport.get("path") != "changes.sqlite3":
            raise ValueError("TRANSPORT_REPRESENTATION_INVALID")
        if "changes.sqlite3" not in file_map:
            raise ValueError("TRANSPORT_FILE_MISSING")
        expected_local.add("changes.sqlite3")
        if file_map["changes.sqlite3"] != {
                "bytes": plan["transport_database"]["bytes"],
                "sha256": plan["transport_database"]["sha256"]}:
            raise ValueError("TRANSPORT_FILE_HASH_MISMATCH")
        local_db = _safe_file(root, "changes.sqlite3", "TRANSPORT_FILE_INVALID")
        if local_db.stat().st_size != plan["transport_database"]["bytes"]:
            raise ValueError("TRANSPORT_FILE_HASH_MISMATCH")
        adjusted = copy.deepcopy(document)
        adjusted["database"]["path"] = str(local_db)
        # The transport reader validates the database hash and all database
        # inventory details while streaming every operation.
        try:
            iterator = _delta.iter_delta_records(local_db, adjusted)
            for _ in iterator:
                pass
        except Exception as exc:
            raise ValueError("TRANSPORT_DATABASE_INVALID") from exc
    else:
        shard_manifest, _ = _validate_shard_proof(root, plan, file_map, document)
        expected_local.add("transport/manifest.json")
        expected_local.add("transport/verification.json")
        data_parts, external, _ = _lossless._declared_files(shard_manifest, root / "transport")
        expected_local.update(f"transport/{path}" for path in set(data_parts) | set(external))
    if set(file_map) != expected_local:
        raise ValueError("DELTA_FILE_INVENTORY_INVALID")
    generation_index_raw = _validate_optional_generation_index(root, plan, plan_raw)
    generation_index_present = generation_index_raw is not None
    control_records = {
        "delta-plan.json": {
            "bytes": len(plan_raw),
            "sha256": hashlib.sha256(plan_raw).hexdigest(),
        },
    }
    if generation_index_raw is not None:
        control_records["index.json"] = {
            "bytes": len(generation_index_raw),
            "sha256": hashlib.sha256(generation_index_raw).hexdigest(),
        }
    try:
        control_guard = verify_files(root, control_records)
    except Exception as exc:
        if getattr(exc, "category", None) == "VERIFIED_FILES_HASH_MISMATCH":
            raise ValueError("DELTA_INDEX_PLAN_MISMATCH") from None
        raise
    actual = _file_inventory(root)
    allowed_extra = set()
    # The local preparer retains the full transport SQLite after fragmentation;
    # a cloud package may omit it because its manifest and verified shards are
    # the declared transport representation.
    if transport["kind"] == "lossless_sqlite_shards" and "changes.sqlite3" in actual:
        digest = _file_digest(_safe_file(root, "changes.sqlite3", "TRANSPORT_FILE_INVALID"))
        if digest != {"bytes": plan["transport_database"]["bytes"],
                      "sha256": plan["transport_database"]["sha256"]}:
            raise ValueError("TRANSPORT_FILE_HASH_MISMATCH")
        allowed_extra.add("changes.sqlite3")
    expected_actual = set(file_map) | {"delta-plan.json"} | allowed_extra
    if generation_index_present:
        expected_actual.add("index.json")
    if actual != expected_actual:
        raise ValueError("DELTA_FILE_INVENTORY_INVALID")
    if (plan.get("xlsx_verification") is None
            or not _same_json(plan["xlsx_verification"], _read_json(root / "xlsx/verification.json", "XLSX_PROOF_INVALID"))):
        raise ValueError("XLSX_PROOF_INVALID")
    return plan, document, shard_manifest, generation_index_present, [
        (artifact_guard, file_map),
        (control_guard, control_records),
    ]


def _schema_by_table(schemas: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {item["name"]: item for item in schemas if item["type"] == "table"}


class DeltaChainReader:
    """Overlay an ordered generation chain on an already-open baseline reader.

    By default this reader accepts prepared local generations. Set
    ``require_published_deltas=True`` to require a strict, full-readback
    ``index.json`` for every generation in the effective chain. This proves
    only those per-generation indexes; it does not verify a baseline or a
    global latest pointer.

    Usage::

        with LosslessShardReader(baseline_path, expected_generation) as baseline:
            with DeltaChainReader(baseline, [delta_a, delta_b]) as reader:
                for ordinal, source_rowid, values in reader.iter_rows("responses"):
                    ...
    """

    def __init__(
        self,
        baseline: LosslessShardReader,
        generations: Iterable[str | os.PathLike[str]] = (),
        *,
        require_published_deltas: bool = False,
    ):
        if type(require_published_deltas) is not bool:
            raise TypeError("require_published_deltas must be a bool")
        if not isinstance(baseline, LosslessShardReader):
            raise TypeError("baseline must be an open LosslessShardReader")
        self._baseline = baseline
        self._requested_generations = tuple(generations)
        self._active = False
        self._temp: Optional[tempfile.TemporaryDirectory[str]] = None
        self._temp_root: Optional[Path] = None
        self._connection: Optional[sqlite3.Connection] = None
        self._plans: list[dict[str, Any]] = []
        self._documents: list[dict[str, Any]] = []
        self._shard_manifests: list[Optional[dict[str, Any]]] = []
        self._require_published_deltas = require_published_deltas
        self._published_generation_indexes_complete = False
        self._published_file_guards: list[
            tuple[Path, list[tuple[Any, dict[str, dict[str, Any]]]]]
        ] = []
        self._iterators: set[Generator[Any, None, None]] = set()
        self._cache_names: dict[str, str] = {}
        self._state_table_counters: dict[str, int] = {}
        self._scratch_counter = 0
        self._selector_counter = 0
        self._materialized: set[str] = set()
        self._final_metadata: dict[str, Any] = {}
        self._final_tables: dict[str, dict[str, Any]] = {}
        self._final_generation_id: Optional[str] = None
        self._lookup_operations_cache: Optional[str] = None
        self._lookup_operations_cache_ready = False
        self._lookup_transport_packages: list[dict[str, Any]] = []
        self._lookup_map_tables: dict[str, str] = {}
        self._lookup_baseline_enabled: dict[str, bool] = {}
        self._lookup_fallback_tables: set[str] = set()

    def __enter__(self) -> "DeltaChainReader":
        if self._active:
            raise RuntimeError("DeltaChainReader is already open")
        try:
            baseline_id = _require_generation(self._baseline.snapshot_identifier)
            baseline_sha = _require_sha(self._baseline.source_sha256)
            baseline_schema_sha = _require_sha(self._baseline.source_schema_sha256)
            baseline_tables = self._baseline.tables()
            baseline_schemas = self._baseline.schema_objects()
            baseline_table_map = {item["name"]: item for item in baseline_tables}
            if len(baseline_table_map) != len(baseline_tables):
                raise ValueError("BASELINE_TABLE_INVENTORY_INVALID")
            roots = [_safe_root(item, "DELTA_GENERATION_PATH_INVALID") for item in self._requested_generations]
            raw_plans = []
            for root in roots:
                plan_path = _safe_file(root, "delta-plan.json", "DELTA_PLAN_INVALID")
                raw_plans.append(_read_json(plan_path, "DELTA_PLAN_INVALID"))
            last_reset = max((index for index, plan in enumerate(raw_plans)
                              if plan.get("replaces_delta_chain") is True), default=-1)
            selected_roots = roots[last_reset:] if last_reset >= 0 else roots
            loaded = [_validate_generation_plan(root) for root in selected_roots]
            if (self._require_published_deltas
                    and any(not item[3] for item in loaded)):
                raise ValueError("DELTA_PUBLISHED_INDEX_REQUIRED")
            self._plans = [item[0] for item in loaded]
            for plan, root in zip(self._plans, selected_roots):
                plan["_root"] = str(root)
            self._documents = [item[1] for item in loaded]
            self._shard_manifests = [item[2] for item in loaded]
            self._published_generation_indexes_complete = bool(self._plans) and all(
                item[3] for item in loaded
            )
            self._published_file_guards = [
                (root, item[4])
                for root, item in zip(selected_roots, loaded)
            ]
            current_generation = baseline_id
            current_schema_sha = baseline_schema_sha
            current_schemas = baseline_schemas
            current_xinfo = {name: item["column_schema"] for name, item in baseline_table_map.items()}
            current_fks = {name: item["foreign_keys"] for name, item in baseline_table_map.items()}
            current_counts = {name: item["row_count"] for name, item in baseline_table_map.items()}
            previous_plan = None
            for plan in self._plans:
                if (plan["baseline_generation_id"] != baseline_id
                        or plan["baseline_source_sha256"] != baseline_sha):
                    raise ValueError("DELTA_BASELINE_BINDING_MISMATCH")
                if plan["replaces_delta_chain"]:
                    if (plan["kind"] != "baseline_reconciliation"
                            or plan["parent_generation_id"] != baseline_id):
                        raise ValueError("DELTA_RESET_PARENT_INVALID")
                    # A reset replaces earlier delta state, but remains bound
                    # to the same immutable full-data baseline generation.
                    current_generation = baseline_id
                else:
                    if previous_plan is None or plan["parent_generation_id"] != current_generation:
                        raise ValueError("DELTA_PARENT_CHAIN_INVALID")
                    if (plan["kind"] != "change_feed"
                            or plan["after_event_id"] != previous_plan["through_event_id"]
                            or plan["source_schema_sha256"] != current_schema_sha
                            or not _same_json(plan["schemas"], current_schemas)
                            or not _same_json(plan["source_table_columns"], current_xinfo)
                            or not _same_json(plan["source_foreign_keys"], current_fks)):
                        raise ValueError("DELTA_SCHEMA_CHAIN_INVALID")
                if previous_plan is not None and not plan["replaces_delta_chain"]:
                    if plan["parent_generation_id"] != previous_plan["generation_id"]:
                        raise ValueError("DELTA_PARENT_CHAIN_INVALID")
                if plan["replaces_delta_chain"]:
                    self._validate_reset_transition(
                        plan, current_schemas, current_xinfo, current_counts,
                    )
                current_generation = plan["generation_id"]
                current_schema_sha = plan["source_schema_sha256"]
                current_schemas = plan["schemas"]
                current_xinfo = plan["source_table_columns"]
                current_fks = plan["source_foreign_keys"]
                current_counts = plan["source_row_counts"]
                previous_plan = plan
            if self._plans and self._plans[-1]["replaces_delta_chain"]:
                pass
            self._final_generation_id = current_generation
            self._final_metadata = {
                "schemas": copy.deepcopy(current_schemas),
                "source_schema_sha256": current_schema_sha,
                "source_table_columns": copy.deepcopy(current_xinfo),
                "source_foreign_keys": copy.deepcopy(current_fks),
                "source_row_counts": dict(current_counts),
            }
            self._final_tables = _schema_by_table(current_schemas)
            self._temp = tempfile.TemporaryDirectory(prefix="ikarchive-delta-reader-")
            temp_root = Path(self._temp.name).resolve(strict=True)
            self._temp_root = temp_root
            os.chmod(temp_root, 0o700)
            cache_path = temp_root / "overlay.sqlite3"
            self._connection = sqlite3.connect(cache_path)
            self._connection.execute("PRAGMA journal_mode=DELETE")
            self._connection.execute("PRAGMA synchronous=FULL")
            self._connection.execute("PRAGMA temp_store=FILE")
            os.chmod(cache_path, 0o600)
            self._active = True
            if self._published_generation_indexes_complete:
                self._assert_published_inputs_unchanged()
            return self
        except BaseException as exc:
            self._cleanup()
            if not isinstance(exc, Exception):
                raise
            if isinstance(exc, DeltaReaderError):
                raise
            code = str(exc) if isinstance(exc, ValueError) and re.fullmatch(r"[A-Z0-9_]+", str(exc)) else "DELTA_CHAIN_INVALID"
            raise DeltaReaderError(code) from None

    def __exit__(self, exc_type, exc, traceback) -> None:
        close_error: Optional[BaseException] = None
        guard_error: Optional[BaseException] = None
        cleanup_error: Optional[BaseException] = None
        try:
            for iterator in tuple(self._iterators):
                try:
                    iterator.close()
                except BaseException as error:
                    if close_error is None:
                        close_error = error
            if self._active and self._published_generation_indexes_complete:
                try:
                    self._assert_public_read_state()
                except BaseException as error:
                    guard_error = error
        finally:
            try:
                self._cleanup()
            except BaseException as error:
                cleanup_error = error
        # A body exception (including KeyboardInterrupt/SystemExit) is the
        # primary failure.  Iterator close, the final published-input guard,
        # and cleanup have all been attempted above, but a secondary failure
        # must not replace the original exception.
        if exc is not None:
            return None
        if guard_error is not None:
            raise guard_error
        if close_error is not None:
            raise close_error
        if cleanup_error is not None:
            raise cleanup_error

    def _cleanup(self) -> None:
        self._active = False
        if self._connection is not None:
            try:
                self._connection.close()
            except BaseException:
                pass
            self._connection = None
        if self._temp is not None:
            try:
                self._temp.cleanup()
            except BaseException:
                pass
            self._temp = None
        self._temp_root = None
        self._cache_names = {}
        self._state_table_counters = {}
        self._scratch_counter = 0
        self._selector_counter = 0
        self._materialized = set()
        self._lookup_operations_cache = None
        self._lookup_operations_cache_ready = False
        self._lookup_transport_packages = []
        self._lookup_map_tables = {}
        self._lookup_baseline_enabled = {}
        self._lookup_fallback_tables = set()
        self._iterators.clear()
        self._published_generation_indexes_complete = False
        self._published_file_guards = []

    def _require_open(self) -> sqlite3.Connection:
        if not self._active or self._connection is None:
            raise RuntimeError("DeltaChainReader must be used inside a with block")
        return self._connection

    def _assert_published_inputs_unchanged(self) -> None:
        try:
            self._baseline._assert_verified_package()
        except Exception:
            raise DeltaReaderError("DELTA_PUBLISHED_INPUT_CHANGED") from None
        for root, guards in self._published_file_guards:
            for token, records in guards:
                try:
                    token.assert_matches(root, records)
                except Exception:
                    raise DeltaReaderError("DELTA_PUBLISHED_INPUT_CHANGED") from None

    def _assert_public_read_state(self) -> None:
        self._require_open()
        if self._published_generation_indexes_complete:
            self._assert_published_inputs_unchanged()

    def _track_iterator(self, factory: Callable[[], Iterator[Any]]) -> Generator[Any, None, None]:
        """Own nested cursors and recheck published inputs at iterator boundaries."""
        def rows() -> Generator[Any, None, None]:
            inner = None
            try:
                self._assert_public_read_state()
                inner = iter(factory())
                while True:
                    self._require_open()
                    try:
                        item = next(inner)
                    except StopIteration:
                        return
                    yield item
            finally:
                close_error: Optional[BaseException] = None
                guard_error: Optional[BaseException] = None
                try:
                    if inner is not None and hasattr(inner, "close"):
                        inner.close()
                except BaseException as error:
                    close_error = error
                try:
                    self._assert_public_read_state()
                except BaseException as error:
                    guard_error = error
                finally:
                    self._iterators.discard(outer)
                if guard_error is not None:
                    raise guard_error
                if close_error is not None:
                    raise close_error

        outer = rows()
        self._iterators.add(outer)
        return outer

    @property
    def published_deltas_verified(self) -> bool:
        """Whether the open, nonempty effective chain has intact strict indexes.

        The per-generation index proofs are checked on open. Their complete
        file/path fingerprints and the baseline package are rechecked at
        published-chain API and iterator boundaries. An empty chain returns
        False: this property does not certify a global latest pointer.
        """
        self._assert_public_read_state()
        return self._published_generation_indexes_complete

    def _validate_reset_transition(
        self, plan: dict[str, Any], old_schemas: list[dict[str, Any]],
        old_xinfo: dict[str, list[dict[str, Any]]], old_counts: dict[str, int],
    ) -> None:
        new_tables = set(plan["source_table_columns"])
        old_tables = set(old_xinfo)
        if not set(plan["metadata"].get("removed_tables", [])).issubset(old_tables - new_tables):
            raise ValueError("DELTA_REMOVED_TABLES_INVALID")
        if old_tables - new_tables != set(plan["metadata"].get("removed_tables", [])):
            raise ValueError("DELTA_REMOVED_TABLES_INVALID")
        if not _same_json(plan["metadata"].get("baseline_row_counts"), old_counts):
            raise ValueError("DELTA_BASELINE_COUNTS_INVALID")
        # The reconciliation stream itself is checked again as it is consumed;
        # this preflight verifies the complete before/after table inventories.
        if any(type(value) is not int or value < 0 for value in old_counts.values()):
            raise ValueError("DELTA_BASELINE_COUNTS_INVALID")

    @property
    def generation_id(self) -> str:
        self._assert_public_read_state()
        assert self._final_generation_id is not None
        result = self._final_generation_id
        self._assert_public_read_state()
        return result

    @property
    def source_schema_sha256(self) -> str:
        self._assert_public_read_state()
        result = self._final_metadata["source_schema_sha256"]
        self._assert_public_read_state()
        return result

    def schema_objects(self) -> list[dict[str, Any]]:
        self._assert_public_read_state()
        result = copy.deepcopy(self._final_metadata["schemas"])
        self._assert_public_read_state()
        return result

    def tables(self) -> list[dict[str, Any]]:
        self._assert_public_read_state()
        result = []
        counts = self._final_metadata["source_row_counts"]
        for name, schema in self._final_tables.items():
            xinfo = self._final_metadata["source_table_columns"][name]
            result.append({
                "name": name,
                "columns": [item["name"] for item in xinfo if item["hidden"] != 1],
                "column_schema": copy.deepcopy(xinfo),
                "foreign_keys": copy.deepcopy(self._final_metadata["source_foreign_keys"][name]),
                "row_count": counts[name],
                "schema": copy.deepcopy(schema),
            })
        self._assert_public_read_state()
        return result

    def columns(self, table: str) -> list[dict[str, Any]]:
        self._assert_public_read_state()
        if table not in self._final_tables:
            raise KeyError("table is not in the selected generation")
        result = copy.deepcopy(self._final_metadata["source_table_columns"][table])
        self._assert_public_read_state()
        return result

    def foreign_keys(self, table: str) -> list[dict[str, Any]]:
        self._assert_public_read_state()
        if table not in self._final_tables:
            raise KeyError("table is not in the selected generation")
        result = copy.deepcopy(self._final_metadata["source_foreign_keys"][table])
        self._assert_public_read_state()
        return result

    def row_count(self, table: str) -> int:
        self._assert_public_read_state()
        if table not in self._final_tables:
            raise KeyError("table is not in the selected generation")
        self._materialize(table)
        conn = self._require_open()
        cache = self._cache_names[table]
        cursor = conn.execute(f"SELECT COUNT(*) FROM {_quote(cache)} WHERE \"live\"=1")
        try:
            result = int(cursor.fetchone()[0])
        finally:
            cursor.close()
        self._assert_public_read_state()
        return result

    def _state_table(self, table: str, columns: Sequence[str]) -> str:
        conn = self._require_open()
        existing = self._cache_names.get(table)
        if existing is not None:
            return existing
        digest = hashlib.sha256(table.encode("utf-8", "surrogatepass")).hexdigest()[:24]
        counter = self._state_table_counters.get(table, 0)
        name = f"state_{digest}_{counter:08d}"
        self._state_table_counters[table] = counter + 1
        value_names = [f"v{index:06d}" for index in range(len(columns))]
        definitions = ['"sequence" INTEGER PRIMARY KEY AUTOINCREMENT', '"live" INTEGER NOT NULL',
                       '"identity_key" TEXT', '"source_rowid"']
        definitions.extend(_quote(item) for item in value_names)
        conn.execute(f"CREATE TABLE {_quote(name)} ({','.join(definitions)})")
        conn.execute(
            f"CREATE UNIQUE INDEX {_quote(name + '_identity_uq')} ON {_quote(name)} (\"identity_key\") "
            "WHERE \"live\"=1 AND \"identity_key\" IS NOT NULL"
        )
        self._cache_names[table] = name
        return name

    def _drop_state_table(self, table: str) -> None:
        conn = self._require_open()
        self._materialized.discard(table)
        name = self._cache_names.get(table)
        if name is not None:
            # Public row iterators can hold a SELECT cursor on this database
            # while another table is materialized. Keep the schema stable and
            # make dropping a state table a logical reset using DML only.
            conn.execute(f"DELETE FROM {_quote(name)}")
            self._cache_names.pop(table, None)

    def _insert_row(self, table: str, columns: Sequence[str], values: Sequence[Any],
                    source_rowid: Optional[int], identity_key: str) -> None:
        conn = self._require_open()
        cache = self._state_table(table, columns)
        value_names = [f"v{index:06d}" for index in range(len(columns))]
        conn.execute(
            f"INSERT INTO {_quote(cache)} (\"live\",\"identity_key\",\"source_rowid\","
            f"{','.join(_quote(name) for name in value_names)}) "
            f"VALUES (1,?,?,{','.join('?' for _ in columns)})",
            (identity_key, source_rowid, *values),
        )

    def _apply_upsert(self, table: str, columns: Sequence[str], record: dict[str, Any],
                      *, identity_key: Optional[str] = None) -> None:
        conn = self._require_open()
        cache = self._state_table(table, columns)
        key = identity_key or _identity_key_for_record(record)
        value_names = [f"v{index:06d}" for index in range(len(columns))]
        exists = conn.execute(
            f"SELECT \"sequence\" FROM {_quote(cache)} WHERE \"live\"=1 AND \"identity_key\"=?",
            (key,),
        ).fetchone()
        assignments = ["\"identity_key\"=?", "\"source_rowid\"=?"]
        assignments.extend(f"{_quote(name)}=?" for name in value_names)
        parameters = (key, record["source_rowid"], *record["values"])
        if exists:
            conn.execute(
                f"UPDATE {_quote(cache)} SET {','.join(assignments)} WHERE \"sequence\"=?",
                (*parameters, exists[0]),
            )
        else:
            self._insert_row(table, columns, record["values"], record["source_rowid"], key)

    def _apply_delete(self, table: str, record: dict[str, Any]) -> None:
        conn = self._require_open()
        cache = self._cache_names.get(table)
        if cache is None:
            return
        key = _identity_key_for_record(record)
        conn.execute(
            f"UPDATE {_quote(cache)} SET \"live\"=0 WHERE \"live\"=1 AND \"identity_key\"=?",
            (key,),
        )

    def _baseline_table(self, table: str) -> Optional[dict[str, Any]]:
        for item in self._baseline.tables():
            if item["name"] == table:
                return item
        return None

    def _materialize(self, table: str) -> None:
        self._require_open()
        if table in self._materialized:
            return
        self._drop_state_table(table)
        try:
            self._materialize_uncached(table)
        except DeltaReaderError:
            self._drop_state_table(table)
            raise
        except Exception as exc:
            self._drop_state_table(table)
            code = str(exc) if isinstance(exc, ValueError) and re.fullmatch(r"[A-Z0-9_]+", str(exc)) else "DELTA_ROW_READ_FAILED"
            raise DeltaReaderError(code) from None
        self._materialized.add(table)

    def _materialize_uncached(self, table: str) -> None:
        conn = self._require_open()
        final_xinfo = self._final_metadata["source_table_columns"][table]
        final_columns = [item["name"] for item in final_xinfo if item["hidden"] != 1]
        baseline_table = self._baseline_table(table)
        current_xinfo = baseline_table["column_schema"] if baseline_table is not None else None
        current_columns = baseline_table["columns"] if baseline_table is not None else None
        if baseline_table is not None:
            self._state_table(table, current_columns)
            for ordinal, source_rowid, values in self._baseline.iter_rows_with_identity(table):
                if ordinal < 0 or len(values) != len(current_columns):
                    raise ValueError("BASELINE_ROW_INVALID")
                key = _identity_key_for_values(
                    table, source_rowid, values, current_columns, current_xinfo,
                    synthetic_suffix=f"baseline:{self._baseline.snapshot_identifier}:{ordinal}",
                )
                self._insert_row(table, current_columns, values, source_rowid, key)
            baseline_count = int(conn.execute(
                f"SELECT COUNT(*) FROM {_quote(self._cache_names[table])} WHERE \"live\"=1"
            ).fetchone()[0])
            if baseline_count != baseline_table["row_count"]:
                raise ValueError("BASELINE_ROW_COUNT_MISMATCH")
        else:
            self._state_table(table, final_columns)

        for plan, document, manifest in zip(self._plans, self._documents, self._shard_manifests):
            generation_xinfo = plan["source_table_columns"].get(table)
            generation_columns = ([item["name"] for item in generation_xinfo if item["hidden"] != 1]
                                 if generation_xinfo is not None else None)
            existed_before = current_xinfo is not None
            schema_changed = (generation_xinfo is not None
                              and (not existed_before or not _same_json(generation_xinfo, current_xinfo)))
            if generation_xinfo is None:
                self._drop_state_table(table)
                current_xinfo = None
                current_columns = None
                continue
            if not plan["replaces_delta_chain"] and (not existed_before or not _same_json(generation_xinfo, current_xinfo)):
                raise ValueError("DELTA_SCHEMA_CHAIN_INVALID")
            if schema_changed and existed_before:
                # Rebuild the state schema before opening the generation's
                # operations cursor. Sharded replay yields clear_table while
                # that cursor is active on this connection, so DDL in the
                # clear handler would lock the database.
                self._drop_state_table(table)
                self._state_table(table, generation_columns)
            elif table not in self._cache_names:
                self._state_table(table, generation_columns)
            saw_operation = False
            saw_clear = False
            for record in self._iter_generation_records(plan, document, manifest, table):
                if record["table_name"] != table:
                    continue
                if not saw_operation:
                    saw_operation = True
                    if schema_changed and record["operation"] != "clear_table":
                        raise ValueError("DELTA_SCHEMA_RESET_MISSING_CLEAR")
                if record["operation"] == "clear_table":
                    saw_clear = True
                    replacement_ordinal = 0
                    cache = self._cache_names[table]
                    conn.execute(f"DELETE FROM {_quote(cache)}")
                elif record["operation"] == "delete":
                    self._apply_delete(table, record)
                else:
                    key = None
                    if record["source_rowid"] is None and record["identity_json"] is None:
                        if not saw_clear:
                            raise ValueError("DELTA_IDENTITY_REQUIRED")
                        key = _identity_key_for_values(
                            table, None, record["values"], generation_columns, generation_xinfo,
                            synthetic_suffix=(f"replacement:{plan['generation_id']}:{replacement_ordinal}"),
                        )
                        replacement_ordinal += 1
                    self._apply_upsert(table, generation_columns, record, identity_key=key)
            if schema_changed and not saw_clear:
                raise ValueError("DELTA_SCHEMA_RESET_MISSING_CLEAR")
            current_xinfo = generation_xinfo
            current_columns = generation_columns
            expected_count = plan["source_row_counts"][table]
            actual_count = int(conn.execute(
                f"SELECT COUNT(*) FROM {_quote(self._cache_names[table])} WHERE \"live\"=1"
            ).fetchone()[0])
            if actual_count != expected_count:
                raise ValueError("DELTA_ROW_COUNT_MISMATCH")
        if current_xinfo is None or not _same_json(current_xinfo, final_xinfo) or current_columns != final_columns:
            raise ValueError("FINAL_TABLE_SCHEMA_MISMATCH")
        actual_count = int(conn.execute(
            f"SELECT COUNT(*) FROM {_quote(self._cache_names[table])} WHERE \"live\"=1"
        ).fetchone()[0])
        if actual_count != self._final_metadata["source_row_counts"][table]:
            raise ValueError("DELTA_ROW_COUNT_MISMATCH")

    def _iter_generation_records(
        self, plan: dict[str, Any], document: dict[str, Any],
        manifest: Optional[dict[str, Any]], requested_table: str,
    ) -> Iterator[dict[str, Any]]:
        transport = plan["transport"]
        if transport["kind"] == "native_sqlite":
            local = _safe_file(Path(plan["_root"]), transport["path"], "TRANSPORT_FILE_INVALID")
            adjusted = copy.deepcopy(document)
            adjusted["database"]["path"] = str(local)
            for record in _delta.iter_delta_records(local, adjusted):
                yield record
            return
        if manifest is None:
            raise ValueError("TRANSPORT_SHARD_MANIFEST_INVALID")
        yield from self._iter_sharded_records(plan, document, manifest, requested_table)

    def _iter_sharded_records(
        self, plan: dict[str, Any], document: dict[str, Any],
        manifest: dict[str, Any], requested_table: str,
    ) -> Iterator[dict[str, Any]]:
        def records() -> Generator[dict[str, Any], None, None]:
            self._require_open()
            temp_root = self._temp_root
            if temp_root is None:
                raise RuntimeError("DeltaChainReader temporary root is unavailable")
            self._scratch_counter += 1
            cache_path = temp_root / f"transport-cache-{self._scratch_counter:08d}.sqlite3"
            cache_conn: Optional[sqlite3.Connection] = None
            try:
                descriptor = os.open(
                    cache_path, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600,
                )
                os.close(descriptor)
                cache_conn = sqlite3.connect(cache_path)
                cache_conn.execute("PRAGMA journal_mode=DELETE")
                cache_conn.execute("PRAGMA synchronous=FULL")
                cache_conn.execute("PRAGMA temp_store=FILE")
                os.chmod(cache_path, 0o600)
                yield from self._iter_sharded_records_with_cache(
                    plan, document, manifest, requested_table, cache_conn,
                )
            finally:
                if cache_conn is not None:
                    try:
                        cache_conn.close()
                    except sqlite3.Error:
                        pass
                for scratch_path in (
                    cache_path,
                    Path(str(cache_path) + "-wal"),
                    Path(str(cache_path) + "-shm"),
                    Path(str(cache_path) + "-journal"),
                ):
                    try:
                        scratch_path.unlink(missing_ok=True)
                    except OSError:
                        pass

        return records()

    def _iter_sharded_records_with_cache(
        self, plan: dict[str, Any], document: dict[str, Any],
        manifest: dict[str, Any], requested_table: str,
        conn: sqlite3.Connection,
    ) -> Iterator[dict[str, Any]]:
        root = Path(plan["_root"])
        package_root = root / "transport"
        source_tables, xinfo, visible = _delta._table_specs(plan["metadata"])
        internal = _delta._internal_names(plan["metadata"], source_tables)
        ops_name, meta_name = internal["operations_table"], internal["metadata_table"]
        names = {item["name"] for item in manifest["tables"]}
        if names != set(source_tables) | {ops_name, meta_name}:
            raise ValueError("TRANSPORT_SHARD_TABLE_INVENTORY_INVALID")
        table_map = {item["name"]: item for item in manifest["tables"]}
        value_cache = "_delta_values_" + hashlib.sha256(
            (plan["generation_id"] + "\0" + requested_table).encode("utf-8")
        ).hexdigest()[:24]
        ops_cache = "_delta_ops_" + hashlib.sha256(
            plan["generation_id"].encode("utf-8")
        ).hexdigest()[:24]
        columns = visible[requested_table]
        defs = ['"ordinal" INTEGER PRIMARY KEY', '"source_rowid"']
        defs.extend(f'"v{index:06d}"' for index in range(len(columns)))
        conn.execute(f"CREATE TABLE {_quote(value_cache)} ({','.join(defs)})")
        conn.execute(
            f"CREATE TABLE {_quote(ops_cache)} ("
            '"operation_ordinal" INTEGER PRIMARY KEY, "table_name" TEXT, "operation" TEXT, '
            '"source_rowid", "identity_json" TEXT, "row_ordinal" INTEGER, '
            '"transport_rowid" INTEGER, "identity_key" TEXT)'
        )
        # File hashes, complete table inventory, and exporter verification are
        # checked during preflight. Materialize only the requested value table;
        # a large transport package must not be replayed once per source table.
        expected_values = document["upsert_counts_by_table"].get(requested_table, 0)
        insert = f"INSERT INTO {_quote(value_cache)} VALUES ({','.join('?' for _ in range(2 + len(columns)))} )"
        actual_values = 0
        for ordinal, source_rowid, values in _lossless.iter_table_rows_with_identity(
                package_root, manifest, requested_table):
            if ordinal != actual_values or len(values) != len(columns):
                raise ValueError("TRANSPORT_SHARD_ROW_INVALID")
            conn.execute(insert, (ordinal, source_rowid, *values))
            actual_values += 1
        if (actual_values != expected_values
                or table_map[requested_table]["row_count"] != expected_values):
            raise ValueError("TRANSPORT_SHARD_COUNTS_INVALID")
        metadata_rows = list(_lossless.iter_table_rows_with_identity(package_root, manifest, meta_name))
        if len(metadata_rows) != 1 or metadata_rows[0][2] != (
                "source_metadata", _canonical(plan["metadata"])):
            raise ValueError("TRANSPORT_SHARD_METADATA_INVALID")
        op_columns = list(_OPERATION_FIELDS)
        ops_iterator = _lossless.iter_table_rows_with_identity(package_root, manifest, ops_name)
        insert_op = f"INSERT INTO {_quote(ops_cache)} VALUES ({','.join('?' for _ in _OPERATION_FIELDS)})"
        row_count = 0
        for ordinal, source_rowid, values in ops_iterator:
            if len(values) != len(op_columns) or values[0] != ordinal:
                raise ValueError("TRANSPORT_SHARD_OPERATIONS_INVALID")
            if source_rowid is not None and source_rowid != ordinal:
                raise ValueError("TRANSPORT_SHARD_OPERATIONS_INVALID")
            conn.execute(insert_op, tuple(values))
            row_count += 1
        if row_count != sum(document["operation_counts"].values()):
            raise ValueError("TRANSPORT_SHARD_OPERATIONS_INVALID")

        cleared: set[str] = set()
        clear_closed: set[str] = set()
        seen_table_ops: set[str] = set()
        active_clear: Optional[str] = None
        upsert_ordinals = {table: 0 for table in source_tables}
        op_counts = {"upsert": 0, "delete": 0, "clear_table": 0}
        seen_identity_keys = "_delta_seen_" + hashlib.sha256(
            plan["generation_id"].encode("utf-8")
        ).hexdigest()[:24]
        conn.execute(
            f"CREATE TABLE {_quote(seen_identity_keys)} (\"identity_key\" TEXT PRIMARY KEY) WITHOUT ROWID"
        )
        cursor = None
        try:
            cursor = conn.execute(f"SELECT * FROM {_quote(ops_cache)} ORDER BY \"operation_ordinal\"")
            for op_row in cursor:
                (operation_ordinal, table, op, source_rowid, identity_json,
                 row_ordinal, transport_rowid, identity_key) = op_row
                if (type(operation_ordinal) is not int or operation_ordinal != sum(op_counts.values())
                        or table not in source_tables or op not in ("upsert", "delete", "clear_table")):
                    raise ValueError("TRANSPORT_SHARD_OPERATIONS_INVALID")
                table_columns = visible[table]
                values = None
                if op == "upsert":
                    expected_ordinal = upsert_ordinals[table]
                    if (type(row_ordinal) is not int or row_ordinal != expected_ordinal
                            or type(transport_rowid) is not int or transport_rowid <= 0):
                        raise ValueError("TRANSPORT_SHARD_OPERATIONS_INVALID")
                    upsert_ordinals[table] += 1
                    if table == requested_table:
                        cached = conn.execute(
                            f"SELECT \"source_rowid\",{','.join(_quote(f'v{i:06d}') for i in range(len(table_columns)))} "
                            f"FROM {_quote(value_cache)} WHERE \"ordinal\"=?", (row_ordinal,),
                        ).fetchone()
                        if cached is None:
                            raise ValueError("TRANSPORT_SHARD_VALUE_REFERENCE_INVALID")
                        cache_rowid, *cached_values = cached
                        if cache_rowid is not None and cache_rowid != transport_rowid:
                            raise ValueError("TRANSPORT_SHARD_VALUE_REFERENCE_INVALID")
                        values = tuple(cached_values)
                    else:
                        values = tuple(None for _ in table_columns)
                    if (source_rowid is not None and (type(source_rowid) is not int
                            or not _delta._MIN_INT64 <= source_rowid <= _delta._MAX_INT64)):
                        raise ValueError("TRANSPORT_SHARD_OPERATIONS_INVALID")
                    if identity_json is not None and not isinstance(identity_json, str):
                        raise ValueError("TRANSPORT_SHARD_OPERATIONS_INVALID")
                else:
                    if row_ordinal is not None or transport_rowid is not None:
                        raise ValueError("TRANSPORT_SHARD_OPERATIONS_INVALID")
                    if op == "clear_table":
                        if identity_key is not None:
                            raise ValueError("TRANSPORT_SHARD_OPERATIONS_INVALID")
                    elif ((source_rowid is None) == (identity_json is None)
                          or (source_rowid is not None and type(source_rowid) is not int)
                          or (identity_json is not None and not isinstance(identity_json, str))):
                        raise ValueError("TRANSPORT_SHARD_OPERATIONS_INVALID")
                record = {
                    "table_name": table, "columns": table_columns, "source_rowid": source_rowid,
                    "identity_json": identity_json, "operation": op, "values": values,
                }
                try:
                    _normalized, computed_key, active_clear = _delta._validate_record(
                        record, set(source_tables), visible, cleared, clear_closed,
                        active_clear, seen_table_ops,
                    )
                except ValueError as exc:
                    raise ValueError("TRANSPORT_SHARD_OPERATIONS_INVALID") from exc
                if computed_key != identity_key:
                    raise ValueError("TRANSPORT_SHARD_OPERATIONS_INVALID")
                if identity_key is not None:
                    try:
                        conn.execute(f"INSERT INTO {_quote(seen_identity_keys)} VALUES (?)", (identity_key,))
                    except sqlite3.IntegrityError as exc:
                        raise ValueError("TRANSPORT_SHARD_DUPLICATE_IDENTITY") from exc
                op_counts[op] += 1
                if table == requested_table and op == "upsert":
                    record["_row_ordinal"] = row_ordinal
                yield record
            for table in source_tables:
                if upsert_ordinals[table] != document["upsert_counts_by_table"].get(table, 0):
                    raise ValueError("TRANSPORT_SHARD_COUNTS_INVALID")
            if op_counts != document["operation_counts"]:
                raise ValueError("TRANSPORT_SHARD_OPERATIONS_INVALID")
        finally:
            if cursor is not None:
                try:
                    cursor.close()
                except sqlite3.Error:
                    pass

    def _lookup_fast_eligible(self, table: str) -> bool:
        """Whether rowid overlay can resolve ``table`` without values cache."""
        if table in self._lookup_fallback_tables:
            return False
        baseline_manifest = self._baseline._manifest
        if not isinstance(baseline_manifest, dict):
            return False
        baseline_tables = {
            item.get("name"): item
            for item in baseline_manifest.get("tables", [])
            if isinstance(item, dict)
        }
        baseline_table = baseline_tables.get(table)
        if not isinstance(baseline_table, dict) or baseline_table.get("rowid_kind") != "rowid":
            return False
        current_xinfo = self._final_metadata.get("source_table_columns", {}).get(table)
        if not isinstance(current_xinfo, list) or not _same_json(
            baseline_table.get("column_schema"), current_xinfo
        ):
            return False

        def table_object(objects: Any) -> Optional[dict[str, Any]]:
            if not isinstance(objects, list):
                return None
            matches = [
                item for item in objects
                if isinstance(item, dict)
                and item.get("type") == "table"
                and item.get("name") == table
                and item.get("tbl_name") == table
            ]
            return matches[0] if len(matches) == 1 else None

        baseline_object = table_object(baseline_manifest.get("schema_objects"))
        if baseline_object is None:
            return False
        for plan in self._plans:
            transport_kind = plan.get("transport", {}).get("kind")
            if transport_kind not in ("native_sqlite", "lossless_sqlite_shards"):
                return False
            if transport_kind == "native_sqlite":
                try:
                    _names, _xinfo, visible = _delta._table_specs(plan["metadata"])
                except Exception:
                    return False
                if (table not in visible
                        or _delta._transport_rowid_alias(visible[table]) is None):
                    # The native fast path needs a stable transport rowid for
                    # the requested source table.  Keep the existing complete
                    # materialization path for tables that shadow every alias.
                    return False
            if not _same_json(plan.get("source_table_columns", {}).get(table), current_xinfo):
                return False
            if not _same_json(table_object(plan.get("schemas")), baseline_object):
                return False
        return True

    def _lookup_transport_package(self, generation_index: int) -> dict[str, Any]:
        """Bind one preverified native DB or sharded transport package."""
        if generation_index < len(self._lookup_transport_packages):
            return self._lookup_transport_packages[generation_index]
        if generation_index != len(self._lookup_transport_packages):
            raise DeltaReaderError("DELTA_LOOKUP_TRANSPORT_INVALID")
        if (generation_index >= len(self._plans)
                or generation_index >= len(self._shard_manifests)
                or generation_index >= len(self._published_file_guards)):
            raise DeltaReaderError("DELTA_LOOKUP_TRANSPORT_INVALID")

        plan = self._plans[generation_index]
        manifest = self._shard_manifests[generation_index]
        generation_root = Path(plan["_root"])
        try:
            generation_root_guard, guards = self._published_file_guards[generation_index]
            if generation_root_guard != generation_root or not guards:
                raise ValueError("DELTA_LOOKUP_TRANSPORT_INVALID")
            artifact_guard, artifact_records = guards[0]
            plan_records = {
                item["local"]: {"bytes": item["bytes"], "sha256": item["sha256"]}
                for item in plan["files"]
            }
            if artifact_records != plan_records:
                raise ValueError("DELTA_LOOKUP_TRANSPORT_INVALID")

            transport_kind = plan["transport"].get("kind")
            document = self._documents[generation_index]
            table_names, _xinfo, visible = _delta._table_specs(document["metadata"])
            internal = _delta._internal_names(document["metadata"], table_names)
            expected_table_names = set(table_names) | {
                internal["operations_table"], internal["metadata_table"],
            }
            if document.get("table_names") != table_names:
                raise ValueError("DELTA_LOOKUP_TRANSPORT_INVALID")

            if transport_kind == "native_sqlite":
                if manifest is not None or plan["transport"].get("path") != "changes.sqlite3":
                    raise ValueError("DELTA_LOOKUP_TRANSPORT_INVALID")
                database_record = plan_records.get("changes.sqlite3")
                if (database_record is None
                        or database_record != {
                            "bytes": document.get("database", {}).get("bytes"),
                            "sha256": document.get("database", {}).get("sha256"),
                        }
                        or not _same_json(document.get("database"), plan.get("transport_database"))):
                    raise ValueError("DELTA_LOOKUP_TRANSPORT_INVALID")
                adapter = NativeTransportCandidates(
                    generation_root,
                    "changes.sqlite3",
                    document,
                    verified_files=artifact_guard,
                    verified_records=artifact_records,
                )
                if (adapter.table_names != table_names
                        or adapter.visible != visible
                        or adapter.operations_table != internal["operations_table"]
                        or adapter.metadata_table != internal["metadata_table"]):
                    raise ValueError("DELTA_LOOKUP_TRANSPORT_INVALID")
                table_documents = adapter.table_documents
                if set(table_documents) != expected_table_names:
                    raise ValueError("DELTA_LOOKUP_TRANSPORT_INVALID")
                for name in table_names:
                    if table_documents[name].get("columns") != visible[name]:
                        raise ValueError("DELTA_LOOKUP_TRANSPORT_INVALID")
                if (table_documents[internal["operations_table"]].get("columns")
                        != list(_OPERATION_FIELDS)
                        or table_documents[internal["metadata_table"]].get("columns")
                        != ["name", "value"]):
                    raise ValueError("DELTA_LOOKUP_TRANSPORT_INVALID")
                package = {
                    "kind": "native_sqlite",
                    "root": generation_root,
                    "manifest": None,
                    "token": artifact_guard,
                    "records": artifact_records,
                    "table_names": table_names,
                    "visible": visible,
                    "operations_table": internal["operations_table"],
                    "metadata_table": internal["metadata_table"],
                    "table_documents": table_documents,
                    "adapter": adapter,
                }
                self._lookup_transport_packages.append(package)
                return package

            if transport_kind != "lossless_sqlite_shards" or not isinstance(manifest, dict):
                raise ValueError("DELTA_LOOKUP_TRANSPORT_INVALID")
            package_root = generation_root / "transport"
            parts, external_docs, selector_files = _lossless._declared_files(
                manifest, package_root, verify_hashes=False
            )
            if selector_files:
                raise ValueError("DELTA_LOOKUP_TRANSPORT_INVALID")
            plan_files = {
                item["local"]: {"bytes": item["bytes"], "sha256": item["sha256"]}
                for item in plan["files"]
            }
            records: dict[str, dict[str, Any]] = {}
            for relative, declared in {**parts, **external_docs}.items():
                record = {"bytes": declared.get("bytes"), "sha256": declared.get("sha256")}
                if plan_files.get(f"transport/{relative}") != record:
                    raise ValueError("DELTA_LOOKUP_TRANSPORT_INVALID")
                records[relative] = record
            for relative in ("manifest.json", "verification.json"):
                source_relative = f"transport/{relative}"
                record = plan_files.get(source_relative)
                if not isinstance(record, dict):
                    raise ValueError("DELTA_LOOKUP_TRANSPORT_INVALID")
                records[relative] = dict(record)

            selected_records = {
                f"transport/{relative}": record for relative, record in records.items()
            }
            transport_token = artifact_guard.derive("transport", records)
            # Keep the exact plan inventory associated with the derived token;
            # its parent was already checked at entry and is checked again at
            # every public lookup boundary.
            if any(artifact_records.get(path) != item for path, item in selected_records.items()):
                raise ValueError("DELTA_LOOKUP_TRANSPORT_INVALID")
            table_documents = {
                item["name"]: {
                    "name": item["name"],
                    "columns": list(item["columns"]),
                    "row_count": item["row_count"],
                }
                for item in manifest.get("tables", [])
                if isinstance(item, dict) and isinstance(item.get("name"), str)
            }
            package = {
                "kind": "lossless_sqlite_shards",
                "root": package_root,
                "manifest": manifest,
                "token": transport_token,
                "records": records,
                "table_names": table_names,
                "visible": visible,
                "operations_table": internal["operations_table"],
                "metadata_table": internal["metadata_table"],
                "table_documents": table_documents,
                "adapter": None,
            }
            self._lookup_transport_packages.append(package)
            return package
        except DeltaReaderError:
            raise
        except Exception:
            raise DeltaReaderError("DELTA_LOOKUP_TRANSPORT_INVALID") from None

    def _iter_lookup_transport_candidates(
        self,
        package: dict[str, Any],
        table: str,
        criteria_by_column: dict[int, Any],
        *,
        candidate_row_filter: Optional[Callable[[int, Optional[int]], bool]] = None,
    ) -> Iterator[tuple[int, Optional[int], tuple[Any, ...]]]:
        """Dispatch typed candidate scans to the package's verified reader."""
        package_kind = package.get("kind")
        if package_kind == "native_sqlite":
            adapter = package.get("adapter")
            if not isinstance(adapter, NativeTransportCandidates):
                raise DeltaReaderError("DELTA_LOOKUP_TRANSPORT_INVALID")
            return adapter.iter_table_candidates(
                table, criteria_by_column, candidate_row_filter=candidate_row_filter,
            )
        if package_kind == "lossless_sqlite_shards":
            manifest = package.get("manifest")
            if not isinstance(manifest, dict):
                raise DeltaReaderError("DELTA_LOOKUP_TRANSPORT_INVALID")
            return _lossless._iter_table_candidates(
                package["root"], manifest, table, criteria_by_column,
                verified_files=package["token"], verified_records=package["records"],
                candidate_package_kind="transport", candidate_row_filter=candidate_row_filter,
            )
        raise DeltaReaderError("DELTA_LOOKUP_TRANSPORT_INVALID")

    def _build_lookup_operations_cache(self) -> str:
        """Validate each small operation stream once and retain only references."""
        connection = self._require_open()
        if self._lookup_operations_cache_ready:
            if self._lookup_operations_cache is None:
                raise DeltaReaderError("DELTA_LOOKUP_CACHE_INVALID")
            return self._lookup_operations_cache
        self._scratch_counter += 1
        suffix = f"{self._scratch_counter:08d}"
        cache_table = f"_lookup_operations_{suffix}"
        identity_index = f"_lookup_identity_uq_{suffix}"
        ordinal_index = f"_lookup_ordinal_uq_{suffix}"
        created = False
        packages: list[dict[str, Any]] = []
        try:
            connection.execute(
                f"CREATE TABLE {_quote(cache_table)} ("
                '"generation_index" INTEGER NOT NULL, "operation_ordinal" INTEGER NOT NULL, '
                '"table_name" TEXT NOT NULL, "operation" TEXT NOT NULL, '
                '"source_rowid" INTEGER, "identity_json" TEXT, "row_ordinal" INTEGER, '
                '"transport_rowid" INTEGER, "identity_key" TEXT, '
                'PRIMARY KEY ("generation_index", "operation_ordinal"))'
            )
            created = True
            connection.execute(
                f"CREATE UNIQUE INDEX {_quote(identity_index)} ON {_quote(cache_table)} "
                '("generation_index", "identity_key") WHERE "identity_key" IS NOT NULL'
            )
            connection.execute(
                f"CREATE UNIQUE INDEX {_quote(ordinal_index)} ON {_quote(cache_table)} "
                '("generation_index", "table_name", "row_ordinal") '
                'WHERE "row_ordinal" IS NOT NULL'
            )
            insert_sql = (
                f"INSERT INTO {_quote(cache_table)} VALUES "
                "(?,?,?,?,?,?,?,?,?)"
            )
            for generation_index, (plan, document, _manifest) in enumerate(
                zip(self._plans, self._documents, self._shard_manifests)
            ):
                package = self._lookup_transport_package(generation_index)
                packages.append(package)
                table_names = package["table_names"]
                visible = package["visible"]
                operations_table = package["operations_table"]
                metadata_table = package["metadata_table"]
                table_docs = package["table_documents"]
                expected_table_names = set(table_names) | {operations_table, metadata_table}
                if set(table_docs) != expected_table_names:
                    raise ValueError("DELTA_LOOKUP_TRANSPORT_INVALID")
                if document.get("table_names") != table_names:
                    raise ValueError("DELTA_LOOKUP_TRANSPORT_INVALID")

                counts = document.get("upsert_counts_by_table")
                operation_counts = document.get("operation_counts")
                if (not isinstance(counts, dict)
                        or set(counts) - set(table_names)
                        or any(type(value) is not int or value <= 0 for value in counts.values())
                        or not isinstance(operation_counts, dict)
                        or set(operation_counts) != {"upsert", "delete", "clear_table"}
                        or any(type(value) is not int or value < 0
                               for value in operation_counts.values())):
                    raise ValueError("DELTA_LOOKUP_TRANSPORT_INVALID")
                expected_counts = {name: counts.get(name, 0) for name in table_names}
                expected_counts[operations_table] = sum(operation_counts.values())
                expected_counts[metadata_table] = 1
                for name in expected_table_names:
                    row_count = table_docs[name].get("row_count")
                    if type(row_count) is not int or row_count != expected_counts[name]:
                        raise ValueError("DELTA_LOOKUP_TRANSPORT_INVALID")
                if (table_docs[operations_table].get("columns") != list(_OPERATION_FIELDS)
                        or table_docs[metadata_table].get("columns") != ["name", "value"]):
                    raise ValueError("DELTA_LOOKUP_TRANSPORT_INVALID")

                metadata_iterator = self._iter_lookup_transport_candidates(
                    package, metadata_table, {},
                )
                with _close_iterator_preserving_exception(metadata_iterator):
                    metadata_row = next(metadata_iterator, None)
                    extra_metadata_row = next(metadata_iterator, None)
                if (metadata_row is None or extra_metadata_row is not None
                        or metadata_row[0] != 0
                        or metadata_row[2] != ("source_metadata", _canonical(plan["metadata"]))):
                    raise ValueError("DELTA_LOOKUP_TRANSPORT_INVALID")

                cleared: set[str] = set()
                clear_closed: set[str] = set()
                seen_table_ops: set[str] = set()
                active_clear: Optional[str] = None
                upsert_ordinals = {name: 0 for name in table_names}
                actual_operation_counts = {"upsert": 0, "delete": 0, "clear_table": 0}
                operation_iterator = self._iter_lookup_transport_candidates(
                    package, operations_table, {},
                )
                with _close_iterator_preserving_exception(operation_iterator):
                    for operation_ordinal, operations_rowid, values in operation_iterator:
                        if (type(operation_ordinal) is not int
                                or operation_ordinal != sum(actual_operation_counts.values())
                                or operations_rowid is not None and operations_rowid != operation_ordinal
                                or len(values) != len(_OPERATION_FIELDS)
                                or values[0] != operation_ordinal):
                            raise ValueError("DELTA_LOOKUP_TRANSPORT_INVALID")
                        record_fields = dict(zip(_OPERATION_FIELDS, values))
                        table_name = record_fields["table_name"]
                        operation = record_fields["operation"]
                        source_rowid = record_fields["source_rowid"]
                        identity_json = record_fields["identity_json"]
                        row_ordinal = record_fields["row_ordinal"]
                        transport_rowid = record_fields["transport_rowid"]
                        identity_key = record_fields["identity_key"]
                        if table_name not in table_names or operation not in actual_operation_counts:
                            raise ValueError("DELTA_LOOKUP_TRANSPORT_INVALID")
                        if operation == "upsert":
                            expected_ordinal = upsert_ordinals[table_name]
                            if (type(row_ordinal) is not int or row_ordinal != expected_ordinal
                                    or type(transport_rowid) is not int or transport_rowid <= 0):
                                raise ValueError("DELTA_LOOKUP_TRANSPORT_INVALID")
                            upsert_ordinals[table_name] += 1
                            placeholder_values: Optional[tuple[Any, ...]] = tuple(
                                None for _ in visible[table_name]
                            )
                        else:
                            if row_ordinal is not None or transport_rowid is not None:
                                raise ValueError("DELTA_LOOKUP_TRANSPORT_INVALID")
                            placeholder_values = None
                        if identity_key is not None and not isinstance(identity_key, str):
                            raise ValueError("DELTA_LOOKUP_TRANSPORT_INVALID")
                        record = {
                            "table_name": table_name,
                            "columns": visible[table_name],
                            "source_rowid": source_rowid,
                            "identity_json": identity_json,
                            "operation": operation,
                            "values": placeholder_values,
                        }
                        normalized, computed_key, active_clear = _delta._validate_record(
                            record, set(table_names), visible, cleared, clear_closed,
                            active_clear, seen_table_ops,
                        )
                        if computed_key != identity_key:
                            raise ValueError("DELTA_LOOKUP_TRANSPORT_INVALID")
                        connection.execute(insert_sql, (
                            generation_index, operation_ordinal, table_name, operation,
                            source_rowid, identity_json, row_ordinal, transport_rowid,
                            identity_key,
                        ))
                        actual_operation_counts[operation] += 1

                actual_upsert_counts = {
                    name: count for name, count in upsert_ordinals.items() if count
                }
                if (actual_operation_counts != operation_counts
                        or actual_upsert_counts != counts
                        or any(table_docs[name]["row_count"] != upsert_ordinals[name]
                               for name in table_names)):
                    raise ValueError("DELTA_LOOKUP_TRANSPORT_INVALID")

            self._lookup_operations_cache = cache_table
            self._lookup_operations_cache_ready = True
            self._lookup_transport_packages = packages
            return cache_table
        except BaseException as exc:
            if created:
                try:
                    connection.execute(f"DROP TABLE IF EXISTS {_quote(cache_table)}")
                except BaseException:
                    pass
                for index_name in (identity_index, ordinal_index):
                    try:
                        connection.execute(f"DROP INDEX IF EXISTS {_quote(index_name)}")
                    except BaseException:
                        pass
            self._lookup_operations_cache = None
            self._lookup_operations_cache_ready = False
            self._lookup_transport_packages = []
            if not isinstance(exc, Exception):
                raise
            if isinstance(exc, DeltaReaderError):
                raise
            code = (str(exc) if isinstance(exc, ValueError)
                    and re.fullmatch(r"[A-Z0-9_]+", str(exc))
                    else "DELTA_LOOKUP_TRANSPORT_INVALID")
            raise DeltaReaderError(code) from None

    def _verify_lookup_transport_references(self, table: str, operations_cache: str) -> None:
        """Bind every value ordinal to its operation row without decoding cells."""
        connection = self._require_open()
        for generation_index, package in enumerate(self._lookup_transport_packages):
            document = self._documents[generation_index]
            expected = document["upsert_counts_by_table"].get(table, 0)
            table_doc = package["table_documents"].get(table)
            if (type(expected) is not int or expected < 0 or table_doc is None
                    or type(table_doc.get("row_count")) is not int
                    or table_doc["row_count"] != expected):
                raise DeltaReaderError("DELTA_LOOKUP_TRANSPORT_REFERENCE_INVALID")

            scanned = 0
            reference_error = False

            def reject_after_reference_check(
                ordinal: int, transport_rowid: Optional[int], *, _index=generation_index,
            ) -> bool:
                nonlocal scanned, reference_error
                scanned += 1
                if type(ordinal) is not int or ordinal < 0:
                    reference_error = True
                    return False
                reference = connection.execute(
                    f"SELECT \"operation\", \"transport_rowid\" FROM {_quote(operations_cache)} "
                    'WHERE "generation_index"=? AND "table_name"=? AND "row_ordinal"=?',
                    (_index, table, ordinal),
                ).fetchone()
                if (reference is None or reference[0] != "upsert"
                        or type(reference[1]) is not int or reference[1] <= 0
                        or type(transport_rowid) is not int
                        or transport_rowid != reference[1]):
                    reference_error = True
                # This pass validates every historical/superseded reference,
                # but never asks the shard reader to restore an external cell.
                return False

            candidates = self._iter_lookup_transport_candidates(
                package, table, {}, candidate_row_filter=reject_after_reference_check,
            )
            yielded = False
            with _close_iterator_preserving_exception(candidates):
                for _entry in candidates:
                    yielded = True
                    break
            if reference_error or yielded or scanned != expected:
                raise DeltaReaderError("DELTA_LOOKUP_TRANSPORT_REFERENCE_INVALID")

    def _build_lookup_row_map(self, table: str, operations_cache: str) -> Optional[str]:
        """Index final rowid operations; None requests safe full materialization."""
        connection = self._require_open()
        if table in self._lookup_map_tables:
            return self._lookup_map_tables[table]
        self._verify_lookup_transport_references(table, operations_cache)
        self._scratch_counter += 1
        suffix = f"{self._scratch_counter:08d}"
        map_table = f"_lookup_rows_{suffix}"
        index_name = f"_lookup_rows_genord_{suffix}"
        created = False
        unsupported_identity = False
        try:
            connection.execute(
                f"CREATE TABLE {_quote(map_table)} ("
                '"source_rowid" INTEGER PRIMARY KEY, "generation_index" INTEGER NOT NULL, '
                '"operation_ordinal" INTEGER NOT NULL, "operation" TEXT NOT NULL, '
                '"row_ordinal" INTEGER, "transport_rowid" INTEGER, "identity_key" TEXT, '
                '"active" INTEGER NOT NULL)'
            )
            created = True
            connection.execute(
                f"CREATE UNIQUE INDEX {_quote(index_name)} ON {_quote(map_table)} "
                '("generation_index", "row_ordinal") '
                'WHERE "operation"=\'upsert\' AND "active"=1'
            )

            baseline_root, baseline_manifest = self._baseline._require_open()
            baseline_table = next(
                (item for item in baseline_manifest["tables"] if item["name"] == table), None
            )
            if (baseline_table is None or type(baseline_table.get("row_count")) is not int
                    or baseline_table["row_count"] < 0):
                raise DeltaReaderError("DELTA_LOOKUP_ROW_COUNT_MISMATCH")
            baseline_rows = 0
            baseline_error = False

            def index_baseline_identity(ordinal: int, source_rowid: Optional[int]) -> bool:
                nonlocal baseline_rows, baseline_error
                if (type(ordinal) is not int or ordinal != baseline_rows
                        or type(source_rowid) is not int
                        or not _delta._MIN_INT64 <= source_rowid <= _delta._MAX_INT64):
                    baseline_error = True
                else:
                    try:
                        connection.execute(
                            f"INSERT INTO {_quote(map_table)} VALUES (?,?,?,?,?,?,?,?)",
                            (source_rowid, -1, ordinal, "baseline", None, None, None, 1),
                        )
                    except sqlite3.IntegrityError:
                        baseline_error = True
                baseline_rows += 1
                return False

            baseline_candidates = _lossless._iter_table_candidates(
                baseline_root, baseline_manifest, table, {},
                verified_files=self._baseline._verified_files,
                verified_records=self._baseline._verified_records,
                candidate_package_kind="baseline",
                candidate_row_filter=index_baseline_identity,
            )
            baseline_yielded = False
            with _close_iterator_preserving_exception(baseline_candidates):
                for _entry in baseline_candidates:
                    baseline_yielded = True
                    break
            if (baseline_error or baseline_yielded
                    or baseline_rows != baseline_table["row_count"]):
                raise DeltaReaderError("DELTA_LOOKUP_ROW_COUNT_MISMATCH")

            baseline_enabled = True
            for generation_index, plan in enumerate(self._plans):
                cursor = connection.execute(
                    f"SELECT operation_ordinal, table_name, operation, source_rowid, "
                    f"identity_json, row_ordinal, transport_rowid, identity_key "
                    f"FROM {_quote(operations_cache)} WHERE \"generation_index\"=? "
                    "ORDER BY \"operation_ordinal\"",
                    (generation_index,),
                )
                with _close_iterator_preserving_exception(cursor):
                    for (operation_ordinal, op_table, operation, source_rowid,
                         identity_json, row_ordinal, transport_rowid, identity_key) in cursor:
                        if op_table != table:
                            continue
                        if operation == "clear_table":
                            connection.execute(f"DELETE FROM {_quote(map_table)}")
                            baseline_enabled = False
                            continue
                        # The fast overlay is keyed by original source rowid. A
                        # legal PK-only or post-clear ordinal identity cannot be
                        # translated to that key without reading old values.
                        if (type(source_rowid) is not int or identity_json is not None
                                or operation not in ("upsert", "delete")):
                            unsupported_identity = True
                            break
                        active = 1 if operation == "upsert" else 0
                        connection.execute(
                            f"INSERT INTO {_quote(map_table)} VALUES (?,?,?,?,?,?,?,?) "
                            "ON CONFLICT(\"source_rowid\") DO UPDATE SET "
                            '"generation_index"=excluded."generation_index", '
                            '"operation_ordinal"=excluded."operation_ordinal", '
                            '"operation"=excluded."operation", "row_ordinal"=excluded."row_ordinal", '
                            '"transport_rowid"=excluded."transport_rowid", '
                            '"identity_key"=excluded."identity_key", "active"=excluded."active"',
                            (source_rowid, generation_index, operation_ordinal, operation,
                             row_ordinal, transport_rowid, identity_key, active),
                        )
                if unsupported_identity:
                    break
                expected_count = plan.get("source_row_counts", {}).get(table)
                actual_count = connection.execute(
                    f"SELECT COUNT(*) FROM {_quote(map_table)} WHERE \"active\"=1"
                ).fetchone()[0]
                if type(expected_count) is not int or actual_count != expected_count:
                    raise DeltaReaderError("DELTA_LOOKUP_ROW_COUNT_MISMATCH")
            if unsupported_identity:
                connection.execute(f"DROP TABLE IF EXISTS {_quote(map_table)}")
                connection.execute(f"DROP INDEX IF EXISTS {_quote(index_name)}")
                self._lookup_fallback_tables.add(table)
                return None
            final_expected = self._final_metadata.get("source_row_counts", {}).get(table)
            final_actual = connection.execute(
                f"SELECT COUNT(*) FROM {_quote(map_table)} WHERE \"active\"=1"
            ).fetchone()[0]
            if type(final_expected) is not int or final_actual != final_expected:
                raise DeltaReaderError("DELTA_LOOKUP_ROW_COUNT_MISMATCH")
            self._lookup_map_tables[table] = map_table
            self._lookup_baseline_enabled[table] = baseline_enabled
            return map_table
        except BaseException as exc:
            if created:
                try:
                    connection.execute(f"DROP TABLE IF EXISTS {_quote(map_table)}")
                except BaseException:
                    pass
                try:
                    connection.execute(f"DROP INDEX IF EXISTS {_quote(index_name)}")
                except BaseException:
                    pass
            self._lookup_map_tables.pop(table, None)
            self._lookup_baseline_enabled.pop(table, None)
            if not isinstance(exc, Exception):
                raise
            if isinstance(exc, DeltaReaderError):
                raise
            code = (str(exc) if isinstance(exc, ValueError)
                    and re.fullmatch(r"[A-Z0-9_]+", str(exc))
                    else "DELTA_LOOKUP_CACHE_INVALID")
            raise DeltaReaderError(code) from None

    def _lookup_materialized(
        self, table: str, criteria_indexes: dict[int, Any],
    ) -> Optional[dict[str, Any]]:
        connection = self._require_open()
        self._materialize(table)
        cache_table = self._cache_names[table]
        column_count = sum(
            item["hidden"] != 1
            for item in self._final_metadata["source_table_columns"][table]
        )
        columns = [f'"v{index:06d}"' for index in range(column_count)]
        projection = '"source_rowid"' + (f", {', '.join(columns)}" if columns else "")
        query = (
            f"SELECT {projection} "
            f"FROM {_quote(cache_table)} WHERE \"live\"=1 ORDER BY \"sequence\""
        )
        cursor = connection.execute(query)
        found: Optional[dict[str, Any]] = None
        try:
            for row in cursor:
                source_rowid, *values = row
                typed_values = tuple(values)
                if not all(
                    _lossless._same_sqlite_value(typed_values[index], expected)
                    for index, expected in criteria_indexes.items()
                ):
                    continue
                if found is not None:
                    raise DeltaReaderError("DELTA_LOOKUP_NOT_UNIQUE")
                found = {"source_rowid": source_rowid, "values": typed_values}
        finally:
            cursor.close()
        return found

    def _lookup_fast_candidates(
        self,
        table: str,
        criteria_indexes: dict[int, Any],
        map_table: str,
    ) -> Optional[dict[str, Any]]:
        connection = self._require_open()
        columns = [item["name"] for item in self._final_metadata["source_table_columns"][table]
                   if item["hidden"] != 1]
        found: Optional[dict[str, Any]] = None

        def accept(values: tuple[Any, ...]) -> bool:
            if len(values) != len(columns) or any(not _delta._type_ok(value) for value in values):
                raise DeltaReaderError("DELTA_LOOKUP_CANDIDATE_INVALID")
            return all(
                _lossless._same_sqlite_value(values[index], expected)
                for index, expected in criteria_indexes.items()
            )

        def record(source_rowid: Any, values: tuple[Any, ...]) -> None:
            nonlocal found
            if not accept(values):
                raise DeltaReaderError("DELTA_LOOKUP_CANDIDATE_INVALID")
            if found is not None:
                raise DeltaReaderError("DELTA_LOOKUP_NOT_UNIQUE")
            found = {"source_rowid": source_rowid, "values": values}

        baseline_root, baseline_manifest = self._baseline._require_open()
        if self._lookup_baseline_enabled.get(table, True):
            def baseline_filter(_ordinal: int, source_rowid: Optional[int]) -> bool:
                if type(source_rowid) is not int:
                    raise ValueError("baseline row identity is invalid")
                state = connection.execute(
                    f"SELECT \"operation\", \"active\" FROM {_quote(map_table)} "
                    'WHERE "source_rowid"=?',
                    (source_rowid,),
                ).fetchone()
                return state is not None and state[0] == "baseline" and state[1] == 1

            candidates = _lossless._iter_table_candidates(
                baseline_root, baseline_manifest, table, criteria_indexes,
                verified_files=self._baseline._verified_files,
                verified_records=self._baseline._verified_records,
                candidate_package_kind="baseline", candidate_row_filter=baseline_filter,
            )
            with _close_iterator_preserving_exception(candidates):
                for _ordinal, source_rowid, values in candidates:
                    if not accept(values):
                        raise DeltaReaderError("DELTA_LOOKUP_CANDIDATE_INVALID")
                    if type(source_rowid) is not int:
                        raise DeltaReaderError("DELTA_LOOKUP_CANDIDATE_INVALID")
                    record(source_rowid, values)

        for generation_index, package in enumerate(self._lookup_transport_packages):
            active_count = connection.execute(
                f"SELECT COUNT(*) FROM {_quote(map_table)} "
                'WHERE "generation_index"=? AND "operation"=\'upsert\' AND "active"=1',
                (generation_index,),
            ).fetchone()[0]
            if active_count == 0:
                continue
            reference_error = False

            def delta_filter(ordinal: int, transport_rowid: Optional[int], *, _index=generation_index) -> bool:
                nonlocal reference_error
                if type(ordinal) is not int:
                    reference_error = True
                    return False
                reference = connection.execute(
                    f"SELECT \"source_rowid\", \"transport_rowid\", \"operation\", \"active\" "
                    f"FROM {_quote(map_table)} WHERE \"generation_index\"=? AND \"row_ordinal\"=?",
                    (_index, ordinal),
                ).fetchone()
                if reference is None:
                    return False
                _source_rowid, expected_transport_rowid, operation, active = reference
                if (operation != "upsert" or active != 1
                        or type(transport_rowid) is not int
                        or transport_rowid != expected_transport_rowid):
                    reference_error = True
                    return False
                return True

            candidates = self._iter_lookup_transport_candidates(
                package, table, criteria_indexes, candidate_row_filter=delta_filter,
            )
            with _close_iterator_preserving_exception(candidates):
                for ordinal, transport_rowid, values in candidates:
                    if reference_error:
                        raise DeltaReaderError("DELTA_LOOKUP_TRANSPORT_REFERENCE_INVALID")
                    if not accept(values):
                        raise DeltaReaderError("DELTA_LOOKUP_CANDIDATE_INVALID")
                    reference = connection.execute(
                        f"SELECT \"source_rowid\", \"transport_rowid\", \"operation\", \"active\" "
                        f"FROM {_quote(map_table)} WHERE \"generation_index\"=? AND \"row_ordinal\"=?",
                        (generation_index, ordinal),
                    ).fetchone()
                    if (reference is None or reference[2] != "upsert" or reference[3] != 1
                            or type(transport_rowid) is not int
                            or reference[1] != transport_rowid
                            or type(reference[0]) is not int):
                        raise DeltaReaderError("DELTA_LOOKUP_TRANSPORT_REFERENCE_INVALID")
                    record(reference[0], values)
                if reference_error:
                    raise DeltaReaderError("DELTA_LOOKUP_TRANSPORT_REFERENCE_INVALID")
        return found

    def get_unique_row(self, table: str, criteria: dict[str, Any]) -> Optional[dict[str, Any]]:
        """Return one exact-typed row without restoring unrelated large values.

        The result preserves the original source rowid (or ``None`` for a
        table whose source identity is unavailable). It deliberately omits an
        archive ordinal, which is generation-local and is not a source key.
        Before returning, it checks the open package tokens, complete operation
        metadata, every requested-table operation-to-transport reference, and
        per-generation row counts. It restores full values only for matching
        candidates; this is not a second full source-versus-shards proof.
        """
        self._require_open()
        pending_error: Optional[BaseException] = None
        try:
            self._assert_published_inputs_unchanged()
            if type(table) is not str or table not in self._final_tables:
                raise DeltaReaderError("DELTA_LOOKUP_TABLE_UNKNOWN")
            if type(criteria) is not dict:
                raise DeltaReaderError("DELTA_LOOKUP_CRITERIA_INVALID")
            xinfo = self._final_metadata.get("source_table_columns", {}).get(table)
            if not isinstance(xinfo, list):
                raise DeltaReaderError("DELTA_LOOKUP_TABLE_UNKNOWN")
            columns = [item["name"] for item in xinfo if item["hidden"] != 1]
            criteria_indexes: dict[int, Any] = {}
            for name, value in criteria.items():
                if type(name) is not str:
                    raise DeltaReaderError("DELTA_LOOKUP_CRITERIA_INVALID")
                if name not in columns:
                    raise DeltaReaderError("DELTA_LOOKUP_COLUMN_UNKNOWN")
                if not _delta._type_ok(value):
                    raise DeltaReaderError("DELTA_LOOKUP_CRITERIA_INVALID")
                criteria_indexes[columns.index(name)] = value

            if not self._lookup_fast_eligible(table):
                return self._lookup_materialized(table, criteria_indexes)
            operations_cache = self._build_lookup_operations_cache()
            map_table = self._build_lookup_row_map(table, operations_cache)
            if map_table is None:
                return self._lookup_materialized(table, criteria_indexes)
            return self._lookup_fast_candidates(table, criteria_indexes, map_table)
        except BaseException as exc:
            pending_error = exc
            if not isinstance(exc, Exception):
                raise
            if isinstance(exc, DeltaReaderError):
                raise
            code = (str(exc) if isinstance(exc, ValueError)
                    and re.fullmatch(r"[A-Z0-9_]+", str(exc))
                    else "DELTA_LOOKUP_FAILED")
            raise DeltaReaderError(code) from None
        finally:
            try:
                self._assert_published_inputs_unchanged()
            except BaseException:
                if pending_error is None or isinstance(pending_error, Exception):
                    raise

    def iter_rows(self, table: str) -> Iterator[tuple[int, Optional[int], tuple[Any, ...]]]:
        """Yield current rows as ``(ordinal, original_source_rowid, values)``."""
        self._assert_public_read_state()
        if not isinstance(table, str) or table not in self._final_tables:
            raise KeyError("table is not in the selected generation")

        def rows() -> Generator[tuple[int, Optional[int], tuple[Any, ...]], None, None]:
            self._require_open()
            try:
                self._materialize(table)
                conn = self._require_open()
                cache = self._cache_names[table]
                columns = [item["name"] for item in self._final_metadata["source_table_columns"][table]
                           if item["hidden"] != 1]
                value_names = [f"v{index:06d}" for index in range(len(columns))]
                query = (
                    f"SELECT \"source_rowid\",{','.join(_quote(name) for name in value_names)} "
                    f"FROM {_quote(cache)} WHERE \"live\"=1 ORDER BY \"sequence\""
                )
                count = 0
                cursor = conn.execute(query)
                try:
                    for source_rowid, *values in cursor:
                        if source_rowid is not None and type(source_rowid) is not int:
                            raise ValueError("ROW_IDENTITY_INVALID")
                        if any(not _delta._type_ok(value) for value in values):
                            raise ValueError("ROW_VALUE_INVALID")
                        yield count, source_rowid, tuple(values)
                        count += 1
                finally:
                    cursor.close()
                if count != self._final_metadata["source_row_counts"][table]:
                    raise ValueError("DELTA_ROW_COUNT_MISMATCH")
            except Exception as exc:
                code = str(exc) if isinstance(exc, ValueError) and re.fullmatch(r"[A-Z0-9_]+", str(exc)) else "DELTA_ROW_READ_FAILED"
                raise DeltaReaderError(code) from None

        return self._track_iterator(rows)

    def iter_selected_matches(self, mode: str, rule_token: Optional[str] = None) -> Iterator[tuple[Any, ...]]:
        """Return current matches selected from the overlaid classification rows.

        This deliberately derives selectors from current full-table rows. It
        never reads a baseline selector database after deltas have been applied.
        """
        self._assert_public_read_state()
        if not isinstance(mode, str) or not _MODE.fullmatch(mode):
            raise ValueError("analysis mode is invalid")
        if rule_token is not None and (not isinstance(rule_token, str) or not _RULE.fullmatch(rule_token)):
            raise ValueError("rule token is invalid")
        if mode == UNCLASSIFIED and rule_token is not None:
            raise ValueError("unclassified selector cannot be rule-filtered")
        required = {"matches", "match_classification"}
        if not required <= set(self._final_tables):
            raise DeltaReaderError("SELECTOR_TABLES_MISSING")
        matches_columns = [item["name"] for item in self._final_metadata["source_table_columns"]["matches"]
                           if item["hidden"] != 1]
        class_columns = [item["name"] for item in self._final_metadata["source_table_columns"]["match_classification"]
                         if item["hidden"] != 1]
        key_names = ("account", "kind", "match_key")
        if (any(name not in matches_columns for name in key_names)
                or any(name not in class_columns for name in key_names)
                or (mode != UNCLASSIFIED and "analysis_set" not in class_columns)
                or (rule_token is not None and "rule_raw" not in class_columns)):
            raise DeltaReaderError("SELECTOR_SCHEMA_INVALID")

        def rows() -> Generator[tuple[Any, ...], None, None]:
            conn = self._require_open()
            self._materialize("match_classification")
            self._materialize("matches")
            self._selector_counter += 1
            selector = f"_selected_match_keys_{self._selector_counter:08d}"
            selector_created = False
            class_idx = {name: class_columns.index(name) for name in key_names}
            mode_idx = class_columns.index("analysis_set") if "analysis_set" in class_columns else None
            rule_idx = class_columns.index("rule_raw") if "rule_raw" in class_columns else None
            classification_rows = None
            matches_rows = None
            try:
                conn.execute(f"CREATE TABLE {_quote(selector)} (\"key\" TEXT PRIMARY KEY) WITHOUT ROWID")
                selector_created = True
                classification_rows = self.iter_rows("match_classification")
                try:
                    for _ordinal, _source_rowid, values in classification_rows:
                        key = _tuple_identity(tuple(values[class_idx[name]] for name in key_names))
                        if mode == UNCLASSIFIED:
                            conn.execute(f"INSERT OR IGNORE INTO {_quote(selector)} VALUES (?)", (key,))
                            continue
                        if values[mode_idx] != mode or type(values[mode_idx]) is not str:
                            continue
                        if rule_token is not None:
                            raw_rule = values[rule_idx]
                            if raw_rule is not None and type(raw_rule) is not str:
                                raise ValueError("SELECTOR_RULE_VALUE_INVALID")
                            if _rule_token(raw_rule) != rule_token:
                                continue
                        conn.execute(f"INSERT OR IGNORE INTO {_quote(selector)} VALUES (?)", (key,))
                finally:
                    if classification_rows is not None:
                        classification_rows.close()
                        classification_rows = None
                matches_idx = {name: matches_columns.index(name) for name in key_names}
                matches_rows = self.iter_rows("matches")
                try:
                    for _ordinal, _source_rowid, values in matches_rows:
                        key = _tuple_identity(tuple(values[matches_idx[name]] for name in key_names))
                        present = conn.execute(f"SELECT 1 FROM {_quote(selector)} WHERE \"key\"=?", (key,)).fetchone()
                        if mode == UNCLASSIFIED and present is not None:
                            continue
                        if mode != UNCLASSIFIED and present is None:
                            continue
                        yield tuple(values)
                finally:
                    if matches_rows is not None:
                        matches_rows.close()
                        matches_rows = None
            except DeltaReaderError:
                raise
            except Exception as exc:
                code = str(exc) if isinstance(exc, ValueError) and re.fullmatch(r"[A-Z0-9_]+", str(exc)) else "SELECTOR_READ_FAILED"
                raise DeltaReaderError(code) from None
            finally:
                for nested in (classification_rows, matches_rows):
                    if nested is not None:
                        try:
                            nested.close()
                        except BaseException:
                            pass
                if selector_created:
                    conn.execute(f"DELETE FROM {_quote(selector)}")

        return self._track_iterator(rows)


def _tuple_identity(values: Sequence[Any]) -> str:
    return _canonical([_typed_value(value) for value in values])


__all__ = ["DeltaChainReader", "DeltaReaderError"]
