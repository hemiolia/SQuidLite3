#!/usr/bin/env python3
"""Publish one prepared, lossless SQLite delta generation to the database remote.

The prepared generation is immutable input. Local files are validated before
uploads; each artifact is streamed back and checked after upload; the immutable
delta index is written last; and latest.json is replaced only after a final
readback comparison against the exact prior bytes. Rclone does not provide a
remote compare-and-swap, so the final comparison narrows but cannot eliminate
the race with another writer.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import sqlite3
import stat
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src/python"))
sys.path.insert(0, str(ROOT / "scripts"))

import nas_full_data_publish as publisher  # noqa: E402
from ikarchive import lossless_sqlite  # noqa: E402
from ikarchive.delta_transport import DeltaTransportError, iter_delta_records  # noqa: E402
from ikarchive.slice_selectors import data_files  # noqa: E402


_PLAN_KEYS = {
    "version", "status", "role", "generation_id", "baseline_generation_id",
    "baseline_source_sha256", "parent_generation_id", "expected_previous_generation_id",
    "replaces_delta_chain", "supersedes_generation_id", "kind", "captured_at",
    "after_event_id", "through_event_id", "source_schema_sha256", "source_row_counts",
    "source_table_columns", "source_foreign_keys", "schemas", "metadata", "transport",
    "transport_database", "value_verification", "xlsx_verification", "files",
    "max_part_bytes", "coverage",
}
_SCHEMA_KEYS = {"name", "type", "tbl_name", "sql"}
_XINFO_KEYS = {"cid", "name", "type", "notnull", "dflt_value", "pk", "hidden"}
_XLSX_COUNTS = {"exported_tables", "exported_rows", "exported_cells", "chunks", "pieces",
                "source_rowids_verified", "source_rowid_rows_checked", "source_bytes"}
_DELTA_INDEX_ROLE = "lossless_delta_generation"
_FULL_STATE_ROLE = "lossless_full_state"
_MAX_PART_BYTES = 20 * 1024 * 1024
_MAX_XLSX_PIECE_BYTES = 20 * 1024 * 1024


def _fail(category: str) -> None:
    raise publisher.PublishError(category)


def _verify_remote_directories(client) -> None:
    verifier = getattr(publisher, "verify_remote_directories", None)
    if not callable(verifier):
        verifier = getattr(client, "verify_remote_directories", None)
        if not callable(verifier):
            return
        args = ()
    else:
        args = (client,)
    try:
        verifier(*args)
    except publisher.PublishError:
        raise
    except Exception as exc:
        raise publisher.PublishError("REMOTE_DIRECTORY_VERIFICATION_FAILED") from exc


def _canonical_sha(value) -> str:
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _json_bytes(value) -> bytes:
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")


def _read_local_json(root: Path, relative: str):
    try:
        raw, size, digest = publisher.read_control_file(publisher.local_path_for(root, relative))
        return publisher._json_object(raw), raw, size, digest
    except publisher.PublishError:
        raise
    except Exception as exc:
        raise publisher.PublishError("LOCAL_METADATA_INVALID") from exc


def _same_schema_columns(columns) -> bool:
    if not isinstance(columns, dict):
        return False
    for table, rows in columns.items():
        if not isinstance(table, str) or not table or not isinstance(rows, list) or not rows:
            return False
        for expected_cid, row in enumerate(rows):
            if not isinstance(row, dict) or set(row) != _XINFO_KEYS:
                return False
            if (type(row.get("cid")) is not int or row["cid"] != expected_cid
                    or not isinstance(row.get("name"), str)
                    or not isinstance(row.get("type"), str)
                    or type(row.get("notnull")) is not int
                    or type(row.get("pk")) is not int
                    or type(row.get("hidden")) is not int
                    or (row.get("dflt_value") is not None and not isinstance(row["dflt_value"], str))):
                return False
    return True


def _validate_metadata(plan: dict) -> None:
    metadata = plan.get("metadata")
    if not isinstance(metadata, dict):
        _fail("PLAN_INVALID")
    stable_fields = (
        "after_event_id", "through_event_id", "schemas", "source_schema_sha256",
        "source_row_counts", "source_foreign_keys", "source_table_columns",
    )
    if any(metadata.get(key) != plan.get(key) for key in stable_fields):
        _fail("PLAN_METADATA_MISMATCH")
    for key in ("generation_id", "parent_generation_id", "baseline_generation_id",
                "baseline_source_sha256", "captured_at", "kind", "replaces_delta_chain",
                "supersedes_generation_id"):
        if metadata.get(key) != plan.get(key):
            _fail("PLAN_METADATA_MISMATCH")
    if (metadata.get("captured_at_kind") != "pinned_read_transaction"
            or metadata.get("schema_comparison_required") is not True):
        _fail("PLAN_METADATA_MISMATCH")

    schemas = plan.get("schemas")
    if (not isinstance(schemas, list)
            or any(not isinstance(item, dict) or set(item) != _SCHEMA_KEYS
                   or not isinstance(item.get("name"), str)
                   or not isinstance(item.get("type"), str)
                   or not isinstance(item.get("tbl_name"), str)
                   or (item.get("sql") is not None and not isinstance(item.get("sql"), str))
                   for item in schemas)
            or schemas != sorted(schemas, key=lambda item: (item["type"], item["name"]))
            or _canonical_sha(schemas) != plan.get("source_schema_sha256")):
        _fail("PLAN_SCHEMA_INVALID")
    table_names = [item["name"] for item in schemas if item["type"] == "table"]
    row_counts = plan.get("source_row_counts")
    foreign_keys = plan.get("source_foreign_keys")
    columns = plan.get("source_table_columns")
    if (not isinstance(row_counts, dict) or set(row_counts) != set(table_names)
            or any(type(count) is not int or count < 0 for count in row_counts.values())
            or not isinstance(foreign_keys, dict) or set(foreign_keys) != set(table_names)
            or any(not isinstance(rows, list) for rows in foreign_keys.values())
            or not _same_schema_columns(columns) or set(columns) != set(table_names)):
        _fail("PLAN_SCHEMA_INVALID")


def _validate_identity(plan: dict) -> str:
    if (set(plan) != _PLAN_KEYS or type(plan.get("version")) is not int or plan["version"] != 1
            or plan.get("status") != "prepared" or plan.get("role") != _DELTA_INDEX_ROLE):
        _fail("PLAN_INVALID")
    generation_id = plan.get("generation_id")
    baseline_id = plan.get("baseline_generation_id")
    expected_previous = plan.get("expected_previous_generation_id")
    for value in (generation_id, baseline_id, expected_previous):
        if not publisher.valid_generation_id(value):
            _fail("PLAN_ID_INVALID")
    for key in ("baseline_source_sha256", "source_schema_sha256"):
        if not publisher.valid_sha(plan.get(key)):
            _fail("PLAN_HASH_INVALID")
    if (not isinstance(plan.get("captured_at"), str)
            or plan.get("kind") not in ("change_feed", "baseline_reconciliation")
            or type(plan.get("after_event_id")) is not int or plan["after_event_id"] < 0
            or type(plan.get("through_event_id")) is not int
            or plan["through_event_id"] < plan["after_event_id"]
            or type(plan.get("replaces_delta_chain")) is not bool):
        _fail("PLAN_INVALID")
    try:
        publisher.parse_utc_timestamp(plan["captured_at"])
    except publisher.PublishError as exc:
        raise publisher.PublishError("PLAN_INVALID") from exc

    reset = plan["replaces_delta_chain"]
    if reset:
        if (plan["kind"] != "baseline_reconciliation"
                or plan.get("parent_generation_id") != baseline_id
                or (expected_previous == baseline_id and plan.get("supersedes_generation_id") is not None)
                or (expected_previous != baseline_id
                    and plan.get("supersedes_generation_id") != expected_previous)):
            _fail("PLAN_RESET_INVALID")
    else:
        if (plan["kind"] != "change_feed"
                or plan.get("parent_generation_id") != expected_previous
                or plan.get("supersedes_generation_id") is not None):
            _fail("PLAN_CHAIN_INVALID")
    if plan.get("coverage") != {
            "all_changed_values": True,
            "all_source_tables_metadata": True,
            "full_schema": True,
            "original_row_identities": True,
            "baseline_gap_reconciled": reset,
    }:
        _fail("PLAN_COVERAGE_INCOMPLETE")
    max_bytes = plan.get("max_part_bytes")
    if (type(max_bytes) is not int
            or not lossless_sqlite.MIN_MAX_BYTES <= max_bytes <= _MAX_PART_BYTES):
        _fail("PLAN_PART_LIMIT_INVALID")
    _validate_metadata(plan)
    return generation_id


def _validate_file_plan(root: Path, plan: dict, generation_id: str) -> dict[str, dict]:
    files = plan.get("files")
    if not isinstance(files, list) or not files:
        _fail("PLAN_FILES_INVALID")
    result = {}
    remotes = set()
    for item in files:
        if not isinstance(item, dict) or set(item) != {"local", "remote", "bytes", "sha256"}:
            _fail("PLAN_FILES_INVALID")
        try:
            local = publisher.safe_rel_path(item["local"])
            remote = publisher.safe_rel_path(item["remote"])
        except publisher.PublishError as exc:
            raise publisher.PublishError("PLAN_PATH_INVALID") from exc
        if (local in result or remote in remotes
                or remote != f"deltas/generations/{generation_id}/{local}"
                or type(item.get("bytes")) is not int or item["bytes"] <= 0
                or not publisher.valid_sha(item.get("sha256"))):
            _fail("PLAN_FILES_INVALID")
        result[local] = {"local": local, "remote": remote,
                         "bytes": item["bytes"], "sha256": item["sha256"]}
        remotes.add(remote)

    actual_files = set()
    actual_dirs = {"."}
    for current, dirs, names in os.walk(root, topdown=True, followlinks=False):
        directory = Path(current)
        rel_dir = directory.relative_to(root).as_posix() or "."
        actual_dirs.add(rel_dir)
        for name in list(dirs):
            child = directory / name
            try:
                mode = child.lstat().st_mode
            except OSError as exc:
                raise publisher.PublishError("PATH_SAFETY_ERROR") from exc
            if not stat.S_ISDIR(mode):
                _fail("PATH_SAFETY_ERROR")
            actual_dirs.add(child.relative_to(root).as_posix())
        for name in names:
            child = directory / name
            try:
                mode = child.lstat().st_mode
            except OSError as exc:
                raise publisher.PublishError("PATH_SAFETY_ERROR") from exc
            if not stat.S_ISREG(mode):
                _fail("PATH_SAFETY_ERROR")
            actual_files.add(child.relative_to(root).as_posix())

    transport = plan.get("transport")
    if not isinstance(transport, dict):
        _fail("PLAN_TRANSPORT_INVALID")
    kind = transport.get("kind")
    if kind == "native_sqlite":
        if set(transport) != {"kind", "path"} or transport.get("path") != "changes.sqlite3":
            _fail("PLAN_TRANSPORT_INVALID")
        expected = {"changes.sqlite3", "transport-document.json", "value-verification.json",
                    "xlsx/index.json", "xlsx/manifest.json", "xlsx/verification.json"}
        if any(name.startswith("transport/") for name in result):
            _fail("PLAN_TRANSPORT_INVALID")
    elif kind == "lossless_sqlite_shards":
        if (set(transport) != {"kind", "path", "verification"}
                or transport.get("path") != "transport/manifest.json"
                or not isinstance(transport.get("verification"), dict)):
            _fail("PLAN_TRANSPORT_INVALID")
        expected = {"transport/manifest.json", "transport/verification.json",
                    "transport-document.json", "value-verification.json",
                    "xlsx/index.json", "xlsx/manifest.json", "xlsx/verification.json"}
        try:
            shard_manifest, _raw, _size, _sha = _read_local_json(root, "transport/manifest.json")
            expected.update("transport/" + row["file"] for row in data_files(shard_manifest))
        except publisher.PublishError:
            raise
        except Exception as exc:
            raise publisher.PublishError("TRANSPORT_MANIFEST_INVALID") from exc
    else:
        _fail("PLAN_TRANSPORT_INVALID")

    try:
        xlsx_index, _raw, _size, _sha = _read_local_json(root, "xlsx/index.json")
        piece_rows = xlsx_index.get("pieces")
        if not isinstance(piece_rows, list):
            _fail("XLSX_INDEX_INVALID")
        piece_locals = set()
        for row in piece_rows:
            if (not isinstance(row, dict) or not isinstance(row.get("name"), str)
                    or not publisher.XLSX_FILE_RE.fullmatch(row["name"])):
                _fail("XLSX_INDEX_INVALID")
            piece_locals.add("xlsx/" + row["name"])
        expected.update(piece_locals)
    except publisher.PublishError:
        raise
    except Exception as exc:
        raise publisher.PublishError("XLSX_INDEX_INVALID") from exc

    # The sharded form intentionally retains this local cache for independent
    # typed-record verification, but does not put it on the remote.
    allowed_unpublished = {"changes.sqlite3"} if kind == "lossless_sqlite_shards" else set()
    required = expected | {"xlsx/index.json", "xlsx/manifest.json", "xlsx/verification.json"}
    if set(result) != required:
        _fail("PLAN_FILE_SET_MISMATCH")
    expected_disk = set(result) | {"delta-plan.json"} | allowed_unpublished
    if actual_files != expected_disk:
        _fail("GENERATION_FILE_SET_MISMATCH")
    expected_dirs = {"."}
    for relative in expected_disk:
        parts = relative.split("/")[:-1]
        for count in range(1, len(parts) + 1):
            expected_dirs.add("/".join(parts[:count]))
    if actual_dirs != expected_dirs:
        _fail("GENERATION_LAYOUT_INVALID")

    for local, item in result.items():
        try:
            size, digest = publisher.hash_regular_file(publisher.local_path_for(root, local))
        except publisher.PublishError:
            raise
        except Exception as exc:
            raise publisher.PublishError("LOCAL_FILE_INVALID") from exc
        if (size, digest) != (item["bytes"], item["sha256"]):
            _fail("LOCAL_FILE_MISMATCH")
    return result


def _delta_plan_file_entry(generation_id: str, size: int, digest: str) -> dict[str, object]:
    return {
        "local": "delta-plan.json",
        "remote": f"deltas/generations/{generation_id}/delta-plan.json",
        "bytes": size,
        "sha256": digest,
    }


def _normalized_plan_files(plan: dict, generation_id: str) -> list[dict[str, object]]:
    """Validate and normalize plan.files without adding a self-hash to the plan."""
    files = plan.get("files")
    if not isinstance(files, list) or not files:
        _fail("PLAN_FILES_INVALID")
    result = []
    seen_local = set()
    seen_remote = set()
    prefix = f"deltas/generations/{generation_id}/"
    for item in files:
        if not isinstance(item, dict) or set(item) != {"local", "remote", "bytes", "sha256"}:
            _fail("PLAN_FILES_INVALID")
        try:
            local = publisher.safe_rel_path(item["local"])
            remote = publisher.safe_rel_path(item["remote"])
        except publisher.PublishError as exc:
            raise publisher.PublishError("PLAN_PATH_INVALID") from exc
        if (local == "delta-plan.json" or local in seen_local or remote in seen_remote
                or remote != prefix + local
                or type(item.get("bytes")) is not int or item["bytes"] <= 0
                or not publisher.valid_sha(item.get("sha256"))):
            _fail("PLAN_FILES_INVALID")
        result.append({"local": local, "remote": remote,
                       "bytes": item["bytes"], "sha256": item["sha256"]})
        seen_local.add(local)
        seen_remote.add(remote)
    return result


def _open_transport_cache(path: Path) -> sqlite3.Connection:
    for suffix in ("-wal", "-shm", "-journal"):
        sidecar = Path(str(path) + suffix)
        try:
            info = sidecar.lstat()
        except FileNotFoundError:
            continue
        except OSError as exc:
            raise publisher.PublishError("TRANSPORT_CACHE_INVALID") from exc
        if not stat.S_ISREG(info.st_mode) or info.st_size:
            _fail("TRANSPORT_CACHE_HAS_SIDECAR")
    uri = path.as_uri() + "?mode=ro&immutable=1"
    try:
        conn = sqlite3.connect(uri, uri=True)
        conn.execute("PRAGMA query_only=ON")
        conn.execute("PRAGMA trusted_schema=OFF")
        return conn
    except sqlite3.Error as exc:
        raise publisher.PublishError("TRANSPORT_CACHE_INVALID") from exc


def _validate_transport_representation(plan: dict, cache_info: dict) -> None:
    representation = plan.get("transport")
    if not isinstance(representation, dict):
        _fail("TRANSPORT_REPRESENTATION_INVALID")
    kind = representation.get("kind")
    if kind == "native_sqlite":
        if (set(representation) != {"kind", "path"}
                or representation.get("path") != "changes.sqlite3"):
            _fail("TRANSPORT_REPRESENTATION_INVALID")
        if cache_info["bytes"] > plan["max_part_bytes"]:
            _fail("TRANSPORT_NATIVE_EXCEEDS_PART_LIMIT")
    elif kind == "lossless_sqlite_shards":
        if (set(representation) != {"kind", "path", "verification"}
                or representation.get("path") != "transport/manifest.json"
                or not isinstance(representation.get("verification"), dict)):
            _fail("TRANSPORT_REPRESENTATION_INVALID")
    else:
        _fail("TRANSPORT_REPRESENTATION_INVALID")


def _validate_shard_proof(root: Path, plan: dict, files: dict[str, dict], manifest: dict,
                          recorded_receipt: dict) -> None:
    if (manifest.get("version") != 2 or manifest.get("role") != "lossless_sqlite_shards"
            or manifest.get("snapshot_identifier") != plan["generation_id"]
            or manifest.get("max_bytes") != plan["max_part_bytes"]):
        _fail("TRANSPORT_MANIFEST_INVALID")
    if recorded_receipt != plan["transport"].get("verification"):
        _fail("TRANSPORT_PROOF_MISMATCH")
    if (recorded_receipt.get("status") != "verified"
            or recorded_receipt.get("snapshot_identifier") != plan["generation_id"]
            or not isinstance(recorded_receipt.get("source_schema_sha256"), str)
            or any(recorded_receipt.get("coverage", {}).get(key) is not True for key in
                   ("all_tables", "all_rows", "all_columns", "all_values", "external_values"))):
        _fail("TRANSPORT_PROOF_INCOMPLETE")
    cache = root / "changes.sqlite3"
    conn = _open_transport_cache(cache)
    try:
        # Prepared generations are immutable inputs. Compare all shard data
        # without replacing the producer's verification receipt.
        actual = lossless_sqlite.verify_sqlite_shards(
            conn, root / "transport", manifest, write_receipt=False)
    except publisher.PublishError:
        raise
    except Exception as exc:
        raise publisher.PublishError("TRANSPORT_SHARD_VERIFICATION_FAILED") from exc
    finally:
        conn.close()
    stable = (
        "version", "role", "status", "snapshot_identifier", "source_schema_sha256",
        "table_count", "row_counts", "row_count", "cell_count", "external_cell_count",
        "value_chunk_count", "file_count", "coverage", "known_source_foreign_key_violations",
    )
    for key in stable:
        if actual.get(key) != recorded_receipt.get(key):
            _fail("TRANSPORT_PROOF_MISMATCH")
    if actual.get("file_count") != len(data_files(manifest)):
        _fail("TRANSPORT_PROOF_MISMATCH")
    if set(actual.get("row_counts", {})) != {table["name"] for table in manifest.get("tables", [])}:
        _fail("TRANSPORT_PROOF_MISMATCH")
    expected_rel = {"transport/manifest.json", "transport/verification.json"}
    expected_rel.update("transport/" + row["file"] for row in data_files(manifest))
    if set(files) & {name for name in files if name.startswith("transport/")} != expected_rel:
        _fail("PLAN_TRANSPORT_FILE_SET_MISMATCH")


def _validate_xlsx(root: Path, plan: dict, files: dict[str, dict], cache_info: dict) -> dict:
    index, _raw, index_size, index_sha = _read_local_json(root, "xlsx/index.json")
    manifest, _mraw, manifest_size, manifest_sha = _read_local_json(root, "xlsx/manifest.json")
    verification, _vraw, verification_size, verification_sha = _read_local_json(root, "xlsx/verification.json")
    if plan.get("xlsx_verification") != verification:
        _fail("XLSX_PROOF_MISMATCH")
    if (index.get("status") != "verified" or index.get("verification_status") != "verified"
            or index.get("row_identity_format") != "source_rowid_column_v1"
            or index.get("snapshot_identifier") != plan["generation_id"]
            or index.get("snapshot_id") != plan["generation_id"]
            or index.get("snapshot_sha256") != cache_info["sha256"]
            or verification.get("status") != "verified"
            or verification.get("source_rowids_verified") is not True
            or verification.get("snapshot_identifier") != plan["generation_id"]
            or verification.get("snapshot_sha256") != cache_info["sha256"]
            or manifest.get("status") != "verified"
            or manifest.get("snapshot_identifier") != plan["generation_id"]
            or manifest.get("snapshot_sha256") != cache_info["sha256"]
            or index.get("verification_receipt") != "verification.json"
            or verification.get("manifest") != "manifest.json"
            or verification.get("index") != "index.json"):
        _fail("XLSX_PROOF_INCOMPLETE")
    source = {"path": str((root / "changes.sqlite3").absolute()),
              "bytes": cache_info["bytes"], "sha256": cache_info["sha256"]}
    if any(document.get("source") != source for document in (index, manifest, verification)):
        _fail("XLSX_SOURCE_MISMATCH")
    counts = verification.get("counts")
    if (not isinstance(counts, dict) or set(counts) != _XLSX_COUNTS
            or counts.get("source_rowids_verified") is not True
            or counts.get("source_bytes") != cache_info["bytes"]
            or any(type(counts.get(key)) is not int or counts[key] < 0 for key in
                   ("exported_tables", "exported_rows", "exported_cells", "chunks", "pieces",
                    "source_rowid_rows_checked"))
            or counts.get("pieces") != len(index.get("pieces", []))
            or index.get("counts") != counts or manifest.get("counts") != counts):
        _fail("XLSX_COUNTS_INVALID")
    pieces = index.get("pieces")
    if not isinstance(pieces, list):
        _fail("XLSX_INDEX_INVALID")
    mapped = {}
    for row in pieces:
        if (not isinstance(row, dict) or set(row) !=
                {"name", "bytes", "sha256", "table", "chunk_count"}
                or not isinstance(row.get("name"), str)
                or not publisher.XLSX_FILE_RE.fullmatch(row["name"])
                or row["name"] in mapped or type(row.get("bytes")) is not int
                or row["bytes"] <= 0 or not publisher.valid_sha(row.get("sha256"))
                or not isinstance(row.get("table"), str)
                or type(row.get("chunk_count")) is not int or row["chunk_count"] < 0):
            _fail("XLSX_INDEX_INVALID")
        if row["bytes"] > _MAX_XLSX_PIECE_BYTES:
            _fail("XLSX_PIECE_TOO_LARGE")
        mapped[row["name"]] = {"name": row["name"], "bytes": row["bytes"], "sha256": row["sha256"]}
    if index.get("piece_names") != list(mapped):
        _fail("XLSX_INDEX_INVALID")
    expected_outputs = {f"xlsx/{name}" for name in mapped}
    if {name for name in files if name.startswith("xlsx/")} != (
            expected_outputs | {"xlsx/index.json", "xlsx/manifest.json", "xlsx/verification.json"}):
        _fail("XLSX_FILE_SET_MISMATCH")
    for name, item in mapped.items():
        recorded = files.get(f"xlsx/{name}")
        if recorded is None or (recorded["bytes"], recorded["sha256"]) != (item["bytes"], item["sha256"]):
            _fail("XLSX_FILE_MISMATCH")
    xlsx_files = [*mapped.values(), {"name": "index.json", "bytes": index_size, "sha256": index_sha}]
    if manifest.get("files") != xlsx_files or verification.get("files") != xlsx_files:
        _fail("XLSX_MANIFEST_MISMATCH")
    for local, actual in (
        ("xlsx/index.json", (index_size, index_sha)),
        ("xlsx/manifest.json", (manifest_size, manifest_sha)),
        ("xlsx/verification.json", (verification_size, verification_sha)),
    ):
        recorded = files.get(local)
        if recorded is None or (recorded["bytes"], recorded["sha256"]) != actual:
            _fail("XLSX_FILE_MISMATCH")
    return {"index": index, "manifest": manifest, "verification": verification}


def _preflight(root: Path):
    plan, plan_raw, plan_size, plan_sha = _read_local_json(root, "delta-plan.json")
    generation_id = _validate_identity(plan)
    files = _validate_file_plan(root, plan, generation_id)
    if list(files.values()) != _normalized_plan_files(plan, generation_id):
        _fail("PLAN_FILES_INVALID")
    try:
        transport_doc, _draw, _dsize, _dsha = _read_local_json(root, "transport-document.json")
        value_receipt, _vraw, _vsize, _vsha = _read_local_json(root, "value-verification.json")
    except publisher.PublishError:
        raise
    if (transport_doc.get("status") != "built"
            or transport_doc.get("generation_id") != generation_id
            or transport_doc.get("parent_generation_id") != plan["parent_generation_id"]
            or transport_doc.get("baseline_generation_id") != plan["baseline_generation_id"]
            or transport_doc.get("kind") != plan["kind"]
            or transport_doc.get("captured_at") != plan["captured_at"]
            or transport_doc.get("after_event_id") != plan["after_event_id"]
            or transport_doc.get("through_event_id") != plan["through_event_id"]
            or transport_doc.get("source_schema_sha256") != plan["source_schema_sha256"]
            or transport_doc.get("source_row_counts") != plan["source_row_counts"]
            or transport_doc.get("source_table_columns") != plan["source_table_columns"]
            or transport_doc.get("schemas") != plan["schemas"]
            or transport_doc.get("metadata") != plan["metadata"]
            or transport_doc.get("database") != plan.get("transport_database")):
        _fail("TRANSPORT_DOCUMENT_MISMATCH")
    cache_path = publisher.local_path_for(root, "changes.sqlite3")
    try:
        cache_info = {"bytes": cache_path.stat().st_size,
                      "sha256": publisher.hash_regular_file(cache_path)[1]}
    except (OSError, publisher.PublishError) as exc:
        raise publisher.PublishError("TRANSPORT_CACHE_INVALID") from exc
    database = transport_doc.get("database")
    if (not isinstance(database, dict) or set(database) != {"path", "bytes", "sha256"}
            or database.get("path") != str(cache_path.absolute())
            or (database.get("bytes"), database.get("sha256")) !=
               (cache_info["bytes"], cache_info["sha256"])):
        _fail("TRANSPORT_CACHE_MISMATCH")
    if plan.get("transport_database") != database:
        _fail("TRANSPORT_CACHE_MISMATCH")
    _validate_transport_representation(plan, cache_info)
    value_stable = {
        "version": 1,
        "status": "verified",
        "generation_id": generation_id,
        "parent_generation_id": plan["parent_generation_id"],
        "baseline_generation_id": plan["baseline_generation_id"],
        "kind": plan["kind"],
        "captured_at": plan["captured_at"],
        "source_schema_sha256": plan["source_schema_sha256"],
        "source_row_counts": plan["source_row_counts"],
        "database": database,
        "operation_counts": transport_doc.get("operation_counts"),
        "upsert_counts_by_table": transport_doc.get("upsert_counts_by_table"),
    }
    if (plan.get("value_verification") != value_receipt
            or any(value_receipt.get(key) != expected for key, expected in value_stable.items())):
        _fail("VALUE_PROOF_MISMATCH")
    if (not isinstance(transport_doc.get("operation_counts"), dict)
            or set(transport_doc["operation_counts"]) != {"upsert", "delete", "clear_table"}
            or any(type(count) is not int or count < 0 for count in transport_doc["operation_counts"].values())):
        _fail("TRANSPORT_DOCUMENT_INVALID")
    try:
        for _record in iter_delta_records(cache_path, transport_doc):
            pass
    except DeltaTransportError as exc:
        raise publisher.PublishError("TRANSPORT_RECORDS_INVALID") from exc
    except Exception as exc:
        raise publisher.PublishError("TRANSPORT_RECORDS_INVALID") from exc

    if plan["transport"]["kind"] == "lossless_sqlite_shards":
        manifest, _mraw, _msize, _msha = _read_local_json(root, "transport/manifest.json")
        receipt, _rraw, _rsize, _rsha = _read_local_json(root, "transport/verification.json")
        if receipt != plan["transport"].get("verification"):
            _fail("TRANSPORT_PROOF_MISMATCH")
        _validate_shard_proof(root, plan, files, manifest, receipt)
    xlsx_docs = _validate_xlsx(root, plan, files, cache_info)

    # The serialized plan and all local controls are covered by the same bytes
    # used for per-generation resume state.
    if hashlib.sha256(plan_raw).hexdigest() != plan_sha or plan_size != len(plan_raw):
        _fail("PLAN_CHANGED")
    published_files = [*files.values(), _delta_plan_file_entry(generation_id, plan_size, plan_sha)]
    info = {
        "plan": plan, "plan_bytes": plan_size, "plan_sha256": plan_sha,
        "generation_id": generation_id, "files": published_files,
        "local_files": files, "transport_document": transport_doc,
        "value_verification": value_receipt, "xlsx": xlsx_docs,
        "transport_cache": cache_info,
    }
    return info


def _read_remote_object(client, remote_path):
    metadata = client.stat(remote_path)
    if metadata is None or type(metadata.get("Size")) is not int or metadata["Size"] <= 0:
        _fail("REMOTE_OBJECT_MISSING")
    size, digest = client.readback(remote_path)
    if size != metadata["Size"] or not publisher.valid_sha(digest):
        _fail("REMOTE_READBACK_MISMATCH")
    raw = publisher.read_remote_bytes(client, remote_path, size, digest)
    if len(raw) != size or hashlib.sha256(raw).hexdigest() != digest:
        _fail("REMOTE_READBACK_MISMATCH")
    return raw, size, digest


def _remote_json(client, remote_path):
    raw, size, digest = _read_remote_object(client, remote_path)
    try:
        document = publisher._json_object(raw)
    except publisher.PublishError as exc:
        raise publisher.PublishError("REMOTE_METADATA_INVALID") from exc
    return document, raw, size, digest


def _validate_index_file_inventory(document: dict, *, generation_id: str,
                                   delta: bool) -> None:
    files = document.get("files")
    verification = document.get("verification")
    if (not isinstance(files, list) or not isinstance(verification, dict)
            or verification.get("full_readback") is not True
            or verification.get("file_count") != len(files)
            or not files):
        _fail("INDEX_PROOF_INCOMPLETE")
    seen_local = set()
    seen_remote = set()
    prefix = f"deltas/generations/{generation_id}/" if delta else None
    for item in files:
        if not isinstance(item, dict) or set(item) != {"local", "remote", "bytes", "sha256"}:
            _fail("INDEX_FILE_INVENTORY_INVALID")
        try:
            local = publisher.safe_rel_path(item["local"])
            remote = publisher.safe_rel_path(item["remote"])
        except publisher.PublishError as exc:
            raise publisher.PublishError("INDEX_FILE_INVENTORY_INVALID") from exc
        if (local in seen_local or remote in seen_remote
                or type(item.get("bytes")) is not int or item["bytes"] <= 0
                or not publisher.valid_sha(item.get("sha256"))
                or (prefix is not None and remote != prefix + local)):
            _fail("INDEX_FILE_INVENTORY_INVALID")
        seen_local.add(local)
        seen_remote.add(remote)
    if delta:
        plan_entry = files[-1]
        if (plan_entry.get("local") != "delta-plan.json"
                or plan_entry.get("remote") != prefix + "delta-plan.json"
                or plan_entry.get("sha256") != document.get("plan_sha256")):
            _fail("DELTA_PLAN_INDEX_PROOF_INVALID")


def _validate_baseline_index(document: dict, baseline_id: str, baseline_sha: str,
                             index_sha: str) -> dict:
    if (document.get("version") != 1 or document.get("generation_id") != baseline_id
            or not publisher.valid_sha(index_sha)):
        _fail("BASELINE_INDEX_INVALID")
    source = document.get("source")
    if (not isinstance(source, dict) or type(source.get("bytes")) is not int
            or source["bytes"] <= 0 or source.get("sha256") != baseline_sha):
        _fail("BASELINE_SOURCE_MISMATCH")
    schema_sha = document.get("source_schema_sha256")
    if not publisher.valid_sha(schema_sha):
        _fail("BASELINE_SCHEMA_INVALID")
    _validate_index_file_inventory(document, generation_id=baseline_id, delta=False)
    for item in document["files"]:
        try:
            expected_remote = publisher._expected_remote(baseline_id, item["local"])
        except publisher.PublishError as exc:
            raise publisher.PublishError("BASELINE_FILE_INVENTORY_INVALID") from exc
        if item["remote"] != expected_remote:
            _fail("BASELINE_FILE_INVENTORY_INVALID")
    sqlite_proof = document.get("sqlite_verification")
    selector_proof = document.get("selector_verification")
    xlsx_proof = document.get("xlsx_verification")
    if (not isinstance(sqlite_proof, dict) or sqlite_proof.get("status") != "verified"
            or sqlite_proof.get("snapshot_identifier") != baseline_id
            or sqlite_proof.get("source_sha256") != baseline_sha
            or sqlite_proof.get("source_schema_sha256") != schema_sha
            or any(sqlite_proof.get("coverage", {}).get(key) is not True for key in
                   ("all_tables", "all_rows", "all_columns", "all_values", "external_values"))
            or not isinstance(selector_proof, dict)
            or selector_proof.get("source_sha256") != baseline_sha
            or selector_proof.get("all_shared_files_reachable") is not True
            or selector_proof.get("all_mode_matches") is not True
            or selector_proof.get("all_rule_matches") is not True
            or not isinstance(xlsx_proof, dict) or xlsx_proof.get("status") != "verified"
            or xlsx_proof.get("snapshot_identifier") != baseline_id
            or xlsx_proof.get("snapshot_sha256") != baseline_sha
            or xlsx_proof.get("source_rowids_verified") is not True):
        _fail("BASELINE_PROOF_INCOMPLETE")
    required = {
        "source.sqlite3", "source-manifest.json", "slices/manifest.json",
        "slices/verification.json", "slices/selectors-verification.json",
        "xlsx/index.json", "xlsx/manifest.json", "xlsx/verification.json",
    }
    actual = {row["local"] for row in document["files"]}
    if not required.issubset(actual):
        _fail("BASELINE_FILE_INVENTORY_INCOMPLETE")
    return {"generation_id": baseline_id, "source_sha256": baseline_sha,
            "index_sha256": index_sha, "source_schema_sha256": schema_sha,
            "index_files": len(document["files"])}


def _validate_delta_index(document: dict, *, generation_id: str, baseline: dict,
                          parent_generation_id: str | None = None,
                          source_schema_sha256: str | None = None) -> None:
    if (document.get("version") != 1 or document.get("role") != _DELTA_INDEX_ROLE
            or document.get("generation_id") != generation_id
            or document.get("parent_generation_id") == generation_id
            or document.get("baseline_generation_id") != baseline["generation_id"]
            or document.get("baseline_source_sha256") != baseline["source_sha256"]
            or not publisher.valid_sha(document.get("plan_sha256"))):
        _fail("DELTA_INDEX_INVALID")
    if parent_generation_id is not None and document.get("parent_generation_id") != parent_generation_id:
        _fail("DELTA_CHAIN_INVALID")
    if (source_schema_sha256 is not None
            and document.get("source_schema_sha256") != source_schema_sha256):
        _fail("DELTA_SCHEMA_CHAIN_INVALID")
    if (not publisher.valid_sha(document.get("source_schema_sha256"))
            or document.get("kind") not in ("change_feed", "baseline_reconciliation")
            or document.get("captured_at_kind") != "pinned_read_transaction"
            or not isinstance(document.get("captured_at"), str)
            or type(document.get("after_event_id")) is not int
            or type(document.get("through_event_id")) is not int
            or document["after_event_id"] < 0
            or document["through_event_id"] < document["after_event_id"]):
        _fail("DELTA_INDEX_INVALID")
    try:
        publisher.parse_utc_timestamp(document["captured_at"])
        publisher.parse_utc_timestamp(document["completed_at"])
    except (KeyError, publisher.PublishError) as exc:
        raise publisher.PublishError("DELTA_INDEX_INVALID") from exc
    schemas = document.get("schemas")
    if (not isinstance(schemas, list)
            or any(not isinstance(item, dict) or set(item) != _SCHEMA_KEYS
                   or item.get("type") not in ("table", "index", "view", "trigger")
                   or not isinstance(item.get("name"), str) or not item["name"]
                   or not isinstance(item.get("tbl_name"), str)
                   or (item.get("sql") is not None and not isinstance(item.get("sql"), str))
                   for item in schemas)
            or schemas != sorted(schemas, key=lambda item: (item["type"], item["name"]))
            or len({(item["type"], item["name"]) for item in schemas}) != len(schemas)
            or _canonical_sha(schemas) != document["source_schema_sha256"]
            or not isinstance(document.get("source_row_counts"), dict)
            or not isinstance(document.get("source_table_columns"), dict)
            or not isinstance(document.get("source_foreign_keys"), dict)):
        _fail("DELTA_INDEX_SCHEMA_INVALID")
    table_names = [item["name"] for item in schemas if item["type"] == "table"]
    if (len(table_names) != len(set(table_names))
            or set(document["source_row_counts"]) != set(table_names)
            or set(document["source_table_columns"]) != set(table_names)
            or set(document["source_foreign_keys"]) != set(table_names)
            or not _same_schema_columns(document["source_table_columns"])
            or any(type(value) is not int or value < 0
                   for value in document["source_row_counts"].values())):
        _fail("DELTA_INDEX_SCHEMA_INVALID")
    _validate_index_file_inventory(document, generation_id=generation_id, delta=True)
    verification = document.get("verification", {})
    if (document.get("coverage", {}).get("all_changed_values") is not True
            or document.get("coverage", {}).get("all_source_tables_metadata") is not True
            or document.get("coverage", {}).get("full_schema") is not True
            or document.get("coverage", {}).get("original_row_identities") is not True
            or verification.get("full_readback") is not True):
        _fail("DELTA_INDEX_PROOF_INCOMPLETE")


def _chain_entry(index: dict, index_size: int, index_sha: str) -> dict:
    return {
        "generation_id": index["generation_id"],
        "index_remote": f"deltas/generations/{index['generation_id']}/index.json",
        "index_bytes": index_size,
        "index_sha256": index_sha,
        "file_count": index["verification"]["file_count"],
        "parent_generation_id": index["parent_generation_id"],
        "kind": index["kind"],
        "captured_at": index["captured_at"],
        "source_schema_sha256": index["source_schema_sha256"],
        "after_event_id": index["after_event_id"],
        "through_event_id": index["through_event_id"],
    }


def _validate_remote_delta_plan(client, remote: str, index: dict) -> dict:
    plan_entry = index["files"][-1]
    plan_remote = publisher.join_remote(remote, plan_entry["remote"])
    try:
        plan, _raw, size, digest = _remote_json(client, plan_remote)
    except publisher.PublishError as exc:
        if exc.category == "REMOTE_OBJECT_MISSING":
            raise publisher.PublishError("DELTA_PLAN_REMOTE_MISSING") from exc
        raise
    if (size != plan_entry["bytes"] or digest != plan_entry["sha256"]
            or digest != index["plan_sha256"]):
        _fail("DELTA_PLAN_READBACK_MISMATCH")
    _validate_identity(plan)
    generation_id = index["generation_id"]
    if _normalized_plan_files(plan, generation_id) != index["files"][:-1]:
        _fail("DELTA_PLAN_INDEX_PROOF_INVALID")
    _validate_local_index(index, {"plan": plan, "plan_sha256": digest}, index["files"])
    return plan


def _validate_chain(client, remote: str, latest: dict, baseline: dict) -> list[dict]:
    chain = latest.get("delta_chain")
    if not isinstance(chain, list):
        _fail("LATEST_CHAIN_INVALID")
    if (latest.get("version") != 1 or latest.get("role") != _FULL_STATE_ROLE
            or not publisher.valid_generation_id(latest.get("generation_id"))
            or set(latest) != {
                "version", "role", "generation_id", "parent_generation_id", "baseline",
                "baseline_generation_id", "baseline_source_sha256", "baseline_index_sha256",
                "expected_previous_generation_id", "index_remote", "index_bytes", "index_sha256",
                "last_success", "captured_at", "captured_at_kind", "capture_lag_seconds",
                "after_event_id", "through_event_id", "replaces_delta_chain",
                "supersedes_generation_id", "source_schema_sha256", "source_row_counts",
                "source_table_columns", "source_foreign_keys", "schemas", "delta_chain", "verification",
            }
            or not isinstance(latest.get("last_success"), str)
            or latest.get("captured_at_kind") != "pinned_read_transaction"
            or not isinstance(latest.get("captured_at"), str)
            or not publisher.valid_generation_id(latest.get("parent_generation_id"))
            or not publisher.valid_generation_id(latest.get("expected_previous_generation_id"))
            or latest.get("index_remote") !=
               f"deltas/generations/{latest.get('generation_id')}/index.json"
            or type(latest.get("index_bytes")) is not int or latest["index_bytes"] <= 0
            or not publisher.valid_sha(latest.get("index_sha256"))
            or type(latest.get("capture_lag_seconds")) not in (int, float)
            or not math.isfinite(latest["capture_lag_seconds"])
            or latest["capture_lag_seconds"] < 0
            or type(latest.get("after_event_id")) is not int or latest["after_event_id"] < 0
            or type(latest.get("through_event_id")) is not int
            or latest["through_event_id"] < latest["after_event_id"]
            or type(latest.get("replaces_delta_chain")) is not bool):
        _fail("LATEST_ROLE_INVALID")
    try:
        publisher.parse_utc_timestamp(latest["last_success"])
        publisher.parse_utc_timestamp(latest["captured_at"])
    except publisher.PublishError as exc:
        raise publisher.PublishError("LATEST_INVALID") from exc
    base_ref = {
        "generation_id": baseline["generation_id"],
        "source_sha256": baseline["source_sha256"],
        "index_sha256": baseline["index_sha256"],
    }
    if latest.get("baseline") != base_ref:
        _fail("LATEST_BASELINE_MISMATCH")
    prior = baseline["generation_id"]
    previous_through = None
    previous_schema = None
    verified = []
    seen_generation_ids = set()
    for position, entry in enumerate(chain):
        required_keys = {"generation_id", "index_remote", "index_bytes", "index_sha256", "file_count",
                         "parent_generation_id", "kind", "captured_at", "source_schema_sha256",
                         "after_event_id", "through_event_id"}
        if (not isinstance(entry, dict) or set(entry) != required_keys
                or not publisher.valid_generation_id(entry.get("generation_id"))
                or entry.get("generation_id") in seen_generation_ids
                or entry.get("parent_generation_id") != prior
                or entry.get("index_remote") != f"deltas/generations/{entry.get('generation_id')}/index.json"
                or type(entry.get("index_bytes")) is not int or entry["index_bytes"] <= 0
                or type(entry.get("file_count")) is not int or entry["file_count"] <= 0
                or not publisher.valid_sha(entry.get("index_sha256"))):
            _fail("LATEST_CHAIN_INVALID")
        index_path = publisher.join_remote(remote, entry["index_remote"])
        index, raw, size, digest = _remote_json(client, index_path)
        if (size, digest) != (entry["index_bytes"], entry["index_sha256"]):
            _fail("DELTA_INDEX_READBACK_MISMATCH")
        _validate_delta_index(index, generation_id=entry["generation_id"], baseline=baseline,
                              parent_generation_id=prior, source_schema_sha256=previous_schema)
        _validate_remote_delta_plan(client, remote, index)
        if (index.get("kind") != entry["kind"] or index.get("captured_at") != entry["captured_at"]
                or index.get("source_schema_sha256") != entry["source_schema_sha256"]
                or index.get("after_event_id") != entry["after_event_id"]
                or index.get("through_event_id") != entry["through_event_id"]
                or index.get("verification", {}).get("file_count") != entry["file_count"]):
            _fail("DELTA_CHAIN_ENTRY_MISMATCH")
        if position == 0:
            if (index.get("kind") != "baseline_reconciliation"
                    or index.get("replaces_delta_chain") is not True
                    or index.get("parent_generation_id") != baseline["generation_id"]):
                _fail("DELTA_CHAIN_INVALID")
        else:
            if (index.get("kind") != "change_feed"
                    or index.get("replaces_delta_chain") is not False
                    or previous_through != index.get("after_event_id")):
                _fail("DELTA_CHAIN_INVALID")
        verified.append({"entry": entry, "index": index, "raw": raw})
        seen_generation_ids.add(entry["generation_id"])
        prior = entry["generation_id"]
        previous_through = index["through_event_id"]
        previous_schema = index["source_schema_sha256"]

    generation_id = latest.get("generation_id")
    if chain:
        last = chain[-1]
        if (generation_id != last["generation_id"]
                or latest.get("index_sha256") != last["index_sha256"]
                or latest.get("index_bytes") != last["index_bytes"]):
            _fail("LATEST_CHAIN_TAIL_MISMATCH")
        tail = verified[-1]["index"]
        if (latest.get("source_schema_sha256") != tail.get("source_schema_sha256")
                or latest.get("source_row_counts") != tail.get("source_row_counts")
                or latest.get("source_table_columns") != tail.get("source_table_columns")
                or latest.get("source_foreign_keys") != tail.get("source_foreign_keys")
                or latest.get("through_event_id") != tail.get("through_event_id")):
            _fail("LATEST_SOURCE_METADATA_MISMATCH")
        if (latest.get("index_remote") != f"deltas/generations/{tail['generation_id']}/index.json"
                or latest.get("captured_at") != tail.get("captured_at")
                or latest.get("after_event_id") != tail.get("after_event_id")
                or latest.get("parent_generation_id") != tail.get("parent_generation_id")
                or latest.get("expected_previous_generation_id") !=
                   tail.get("expected_previous_generation_id")
                or latest.get("replaces_delta_chain") != tail.get("replaces_delta_chain")
                or latest.get("supersedes_generation_id") != tail.get("supersedes_generation_id")):
            _fail("LATEST_CHAIN_TAIL_MISMATCH")
        latest_verification = latest.get("verification")
        if (not isinstance(latest_verification, dict)
                or latest_verification.get("full_readback") is not True
                or latest_verification.get("all_delta_indices_full_readback") is not True
                or latest_verification.get("delta_count") != len(chain)
                or latest_verification.get("baseline_index_full_readback") is not True
                or latest_verification.get("baseline_file_count") != baseline["index_files"]):
            _fail("LATEST_PROOF_INCOMPLETE")
    else:
        if generation_id != baseline["generation_id"] or latest.get("index_sha256") != baseline["index_sha256"]:
            _fail("LATEST_BASELINE_MISMATCH")
    return verified


def _remote_baseline(client, remote: str, plan: dict):
    latest_remote = publisher.join_remote(remote, "latest.json")
    latest, latest_raw, latest_size, latest_sha = _remote_json(client, latest_remote)
    baseline_id = plan["baseline_generation_id"]
    baseline_sha = plan["baseline_source_sha256"]
    base_index_remote = publisher.join_remote(remote, f"generations/{baseline_id}/index.json")
    try:
        base_index, _base_raw, _base_size, base_index_sha = _remote_json(client, base_index_remote)
    except publisher.PublishError as exc:
        if exc.category == "REMOTE_OBJECT_MISSING":
            raise publisher.PublishError("BASELINE_NOT_READY") from exc
        raise
    baseline = _validate_baseline_index(base_index, baseline_id, baseline_sha, base_index_sha)
    if (latest.get("generation_id") == baseline_id and latest.get("role") is None):
        if (set(latest) != {"version", "generation_id", "index_sha256", "captured_at",
                            "captured_at_kind", "last_success"}
                or latest.get("version") != 1
                or latest.get("index_sha256") != base_index_sha
                or latest.get("captured_at_kind") != "pinned_read_transaction"
                or not isinstance(latest.get("captured_at"), str)
                or not isinstance(latest.get("last_success"), str)):
            _fail("LATEST_BASELINE_MISMATCH")
        try:
            publisher.parse_utc_timestamp(latest["captured_at"])
            publisher.parse_utc_timestamp(latest["last_success"])
        except publisher.PublishError as exc:
            raise publisher.PublishError("LATEST_BASELINE_MISMATCH") from exc
        chain_state = {"chain": [], "generation_id": baseline_id, "through_event_id": None,
                       "source_schema_sha256": base_index["source_schema_sha256"]}
        return baseline, base_index, latest, latest_raw, latest_size, latest_sha, chain_state
    if latest.get("role") != _FULL_STATE_ROLE:
        _fail("LATEST_ROLE_INVALID")
    verified_chain = _validate_chain(client, remote, latest, baseline)
    chain_state = {
        "chain": [row["entry"] for row in verified_chain],
        "generation_id": latest.get("generation_id"),
        "through_event_id": latest.get("through_event_id"),
        "source_schema_sha256": latest.get("source_schema_sha256"),
    }
    return baseline, base_index, latest, latest_raw, latest_size, latest_sha, chain_state


def _build_delta_index(info: dict, files: list[dict], completed_at: str) -> dict:
    plan = info["plan"]
    return {
        "version": 1,
        "role": _DELTA_INDEX_ROLE,
        "generation_id": plan["generation_id"],
        "plan_sha256": info["plan_sha256"],
        "baseline_generation_id": plan["baseline_generation_id"],
        "baseline_source_sha256": plan["baseline_source_sha256"],
        "parent_generation_id": plan["parent_generation_id"],
        "expected_previous_generation_id": plan["expected_previous_generation_id"],
        "replaces_delta_chain": plan["replaces_delta_chain"],
        "supersedes_generation_id": plan["supersedes_generation_id"],
        "kind": plan["kind"],
        "captured_at": plan["captured_at"],
        "captured_at_kind": "pinned_read_transaction",
        "completed_at": completed_at,
        "after_event_id": plan["after_event_id"],
        "through_event_id": plan["through_event_id"],
        "source_schema_sha256": plan["source_schema_sha256"],
        "source_row_counts": plan["source_row_counts"],
        "source_table_columns": plan["source_table_columns"],
        "source_foreign_keys": plan["source_foreign_keys"],
        "schemas": plan["schemas"],
        "coverage": plan["coverage"],
        "transport": plan["transport"],
        "transport_database": plan["transport_database"],
        "value_verification": plan["value_verification"],
        "xlsx_verification": plan["xlsx_verification"],
        "files": files,
        "verification": {"full_readback": True, "file_count": len(files)},
    }


def _validate_local_index(index: dict, info: dict, files: list[dict]) -> None:
    plan = info["plan"]
    for key in ("generation_id", "plan_sha256", "baseline_generation_id", "baseline_source_sha256",
                "parent_generation_id", "expected_previous_generation_id", "replaces_delta_chain",
                "supersedes_generation_id", "kind", "captured_at", "after_event_id",
                "through_event_id", "source_schema_sha256", "source_row_counts", "source_table_columns",
                "source_foreign_keys", "schemas", "coverage", "transport", "transport_database",
                "value_verification", "xlsx_verification"):
        expected = info["plan_sha256"] if key == "plan_sha256" else plan.get(key)
        if index.get(key) != expected:
            _fail("DELTA_INDEX_PLAN_MISMATCH")
    if (index.get("role") != _DELTA_INDEX_ROLE
            or index.get("files") != files
            or index.get("captured_at_kind") != "pinned_read_transaction"
            or not isinstance(index.get("completed_at"), str)):
        _fail("DELTA_INDEX_PLAN_MISMATCH")
    try:
        publisher.parse_utc_timestamp(index["completed_at"])
    except publisher.PublishError as exc:
        raise publisher.PublishError("DELTA_INDEX_PLAN_MISMATCH") from exc
    _validate_delta_index(index, generation_id=plan["generation_id"],
                          baseline={"generation_id": plan["baseline_generation_id"],
                                    "source_sha256": plan["baseline_source_sha256"]},
                          parent_generation_id=plan["parent_generation_id"])


def _get_or_create_index(client, root: Path, state_dir: Path, remote: str,
                         info: dict, progress: dict):
    plan = info["plan"]
    index_rel = f"deltas/generations/{plan['generation_id']}/index.json"
    index_remote = publisher.join_remote(remote, index_rel)
    metadata = client.stat(index_remote)
    if metadata is not None:
        raw, size, digest = _read_remote_object(client, index_remote)
        try:
            existing = publisher._json_object(raw)
        except publisher.PublishError as exc:
            raise publisher.PublishError("DELTA_INDEX_INVALID") from exc
        _validate_local_index(existing, info, info["files"])
        progress["prepared_index"] = {"completed_at": existing["completed_at"], "sha256": digest}
        progress.setdefault("receipts", {})["generation_index"] = {
            "bytes": size, "sha256": digest, "verified": True,
        }
        publisher._save_progress(progress["path"], progress)
        return existing, raw, size, digest

    prepared = progress.get("prepared_index")
    if prepared is not None:
        completed_at = prepared["completed_at"]
    else:
        completed_at = publisher.now_utc()
    index = _build_delta_index(info, info["files"], completed_at)
    _validate_local_index(index, info, info["files"])
    raw = _json_bytes(index)
    digest = hashlib.sha256(raw).hexdigest()
    if prepared is not None and prepared.get("sha256") != digest:
        _fail("PROGRESS_STATE_MISMATCH")
    progress["prepared_index"] = {"completed_at": completed_at, "sha256": digest}
    publisher._save_progress(progress["path"], progress)
    publisher._ensure_immutable_bytes(client, state_dir, index_remote, raw, digest,
                                      progress=progress, receipt_key="generation_index")
    return index, raw, len(raw), digest


def _make_latest_payload(info: dict, baseline: dict, previous_chain_state: dict,
                         index: dict, index_size: int, index_sha: str,
                         progress: dict) -> dict:
    plan = info["plan"]
    current_entry = _chain_entry(index, index_size, index_sha)
    if plan["replaces_delta_chain"]:
        chain = [current_entry]
    else:
        chain = [*previous_chain_state["chain"], current_entry]
    prior_payload = progress.get("delta_latest_payload")
    if prior_payload is not None:
        required = {
            "version", "role", "generation_id", "parent_generation_id", "baseline",
            "baseline_generation_id", "baseline_source_sha256", "baseline_index_sha256",
            "index_remote", "index_bytes", "index_sha256", "last_success", "captured_at",
            "expected_previous_generation_id",
            "captured_at_kind", "capture_lag_seconds", "after_event_id", "through_event_id",
            "replaces_delta_chain", "supersedes_generation_id", "source_schema_sha256",
            "source_row_counts", "source_table_columns", "source_foreign_keys", "schemas",
            "delta_chain", "verification",
        }
        if (not isinstance(prior_payload, dict)
                or set(prior_payload) != required
                or prior_payload.get("generation_id") != plan["generation_id"]
                or prior_payload.get("baseline") != {
                    "generation_id": baseline["generation_id"],
                    "source_sha256": baseline["source_sha256"],
                    "index_sha256": baseline["index_sha256"],
                }
                or prior_payload.get("delta_chain") != chain
                or prior_payload.get("source_schema_sha256") != plan["source_schema_sha256"]
                or prior_payload.get("source_row_counts") != plan["source_row_counts"]
                or prior_payload.get("source_table_columns") != plan["source_table_columns"]
                or prior_payload.get("source_foreign_keys") != plan["source_foreign_keys"]
                or prior_payload.get("schemas") != plan["schemas"]
                or prior_payload.get("parent_generation_id") != plan["parent_generation_id"]
                or prior_payload.get("baseline_generation_id") != baseline["generation_id"]
                or prior_payload.get("baseline_source_sha256") != baseline["source_sha256"]
                or prior_payload.get("baseline_index_sha256") != baseline["index_sha256"]
                or prior_payload.get("index_remote") != current_entry["index_remote"]
                or prior_payload.get("index_bytes") != index_size
                or prior_payload.get("index_sha256") != index_sha
                or prior_payload.get("expected_previous_generation_id") !=
                   plan["expected_previous_generation_id"]
                or prior_payload.get("captured_at") != plan["captured_at"]
                or prior_payload.get("captured_at_kind") != "pinned_read_transaction"
                or prior_payload.get("after_event_id") != plan["after_event_id"]
                or prior_payload.get("through_event_id") != plan["through_event_id"]
                or prior_payload.get("replaces_delta_chain") != plan["replaces_delta_chain"]
                or prior_payload.get("supersedes_generation_id") != plan["supersedes_generation_id"]
                or prior_payload.get("verification") != {
                    "full_readback": True, "all_delta_indices_full_readback": True,
                    "baseline_index_full_readback": True,
                    "baseline_file_count": baseline["index_files"],
                    "delta_count": len(chain),
                }
                or not isinstance(prior_payload.get("last_success"), str)
                or type(prior_payload.get("capture_lag_seconds")) not in (int, float)
                or prior_payload["capture_lag_seconds"] < 0):
            _fail("PROGRESS_STATE_MISMATCH")
        try:
            publisher.parse_utc_timestamp(prior_payload["last_success"])
        except publisher.PublishError as exc:
            raise publisher.PublishError("PROGRESS_STATE_MISMATCH") from exc
        return prior_payload
    captured = datetime.fromisoformat(plan["captured_at"].replace("Z", "+00:00"))
    now = datetime.now(timezone.utc)
    lag_seconds = max(0.0, (now - captured).total_seconds())
    payload = {
        "version": 1,
        "role": _FULL_STATE_ROLE,
        "generation_id": plan["generation_id"],
        "parent_generation_id": plan["parent_generation_id"],
        "baseline": {
            "generation_id": baseline["generation_id"],
            "source_sha256": baseline["source_sha256"],
            "index_sha256": baseline["index_sha256"],
        },
        "baseline_generation_id": baseline["generation_id"],
        "baseline_source_sha256": baseline["source_sha256"],
        "baseline_index_sha256": baseline["index_sha256"],
        "expected_previous_generation_id": plan["expected_previous_generation_id"],
        "index_remote": f"deltas/generations/{plan['generation_id']}/index.json",
        "index_bytes": index_size,
        "index_sha256": index_sha,
        "last_success": publisher.now_utc(),
        "captured_at": plan["captured_at"],
        "captured_at_kind": "pinned_read_transaction",
        "capture_lag_seconds": lag_seconds,
        "after_event_id": plan["after_event_id"],
        "through_event_id": plan["through_event_id"],
        "replaces_delta_chain": plan["replaces_delta_chain"],
        "supersedes_generation_id": plan["supersedes_generation_id"],
        "source_schema_sha256": plan["source_schema_sha256"],
        "source_row_counts": plan["source_row_counts"],
        "source_table_columns": plan["source_table_columns"],
        "source_foreign_keys": plan["source_foreign_keys"],
        "schemas": plan["schemas"],
        "delta_chain": chain,
        "verification": {"full_readback": True, "all_delta_indices_full_readback": True,
                          "baseline_index_full_readback": True,
                          "baseline_file_count": baseline["index_files"],
                          "delta_count": len(chain)},
    }
    progress["delta_latest_payload"] = payload
    publisher._save_progress(progress["path"], progress)
    return payload


def _validate_current_generation_latest(client, remote: str, latest: dict,
                                        baseline: dict, info: dict, files: list[dict]):
    plan = info["plan"]
    if (latest.get("role") != _FULL_STATE_ROLE
            or latest.get("generation_id") != plan["generation_id"]
            or latest.get("baseline_generation_id") != baseline["generation_id"]
            or latest.get("baseline_source_sha256") != baseline["source_sha256"]):
        _fail("LATEST_GENERATION_MISMATCH")
    verified = _validate_chain(client, remote, latest, baseline)
    if not verified or verified[-1]["index"].get("plan_sha256") != info["plan_sha256"]:
        _fail("LATEST_GENERATION_MISMATCH")
    index_doc = verified[-1]["index"]
    _validate_local_index(index_doc, info, files)
    if (index_doc.get("files") != files
            or index_doc.get("source_schema_sha256") != plan["source_schema_sha256"]
            or latest.get("source_schema_sha256") != plan["source_schema_sha256"]
            or latest.get("source_row_counts") != plan["source_row_counts"]
            or latest.get("through_event_id") != plan["through_event_id"]
            or latest.get("after_event_id") != plan["after_event_id"]
            or latest.get("captured_at") != plan["captured_at"]
            or latest.get("index_remote") !=
               f"deltas/generations/{plan['generation_id']}/index.json"
            or latest.get("expected_previous_generation_id") !=
               plan["expected_previous_generation_id"]
            or latest.get("parent_generation_id") != plan["parent_generation_id"]
            or latest.get("replaces_delta_chain") != plan["replaces_delta_chain"]
            or latest.get("supersedes_generation_id") != plan["supersedes_generation_id"]):
        _fail("LATEST_GENERATION_MISMATCH")
    return index_doc, verified[-1]["entry"]["index_bytes"], verified[-1]["entry"]["index_sha256"]


def _publish_latest(client, state_dir: Path, remote: str, info: dict,
                    baseline: dict, previous_chain_state: dict,
                    prior_raw: bytes, prior_size: int, prior_sha: str,
                    index: dict, index_raw: bytes, index_size: int, index_sha: str,
                    progress: dict):
    plan = info["plan"]
    latest_payload = _make_latest_payload(info, baseline, previous_chain_state,
                                          index, index_size, index_sha, progress)
    latest_raw = _json_bytes(latest_payload)
    latest_sha = hashlib.sha256(latest_raw).hexdigest()
    if progress.get("latest_payload_sha256") not in (None, latest_sha):
        _fail("PROGRESS_STATE_MISMATCH")
    progress["latest_payload_sha256"] = latest_sha
    publisher._save_progress(progress["path"], progress)

    latest_remote = publisher.join_remote(remote, "latest.json")
    previous_remote = publisher.join_remote(remote,
                                            f"generations/{plan['generation_id']}/previous-latest.json")
    publisher._write_preserved_latest(state_dir, plan["generation_id"], prior_raw, prior_sha)
    publisher._ensure_immutable_bytes(client, state_dir, previous_remote, prior_raw, prior_sha,
                                      progress=progress, receipt_key="previous_latest")

    # rclone has no conditional PUT. Compare the complete prior object again
    # immediately before replacement and refuse a concurrent writer's update.
    current_meta = client.stat(latest_remote)
    if current_meta is None or current_meta.get("Size") != prior_size:
        _fail("LATEST_CHANGED")
    if client.readback(latest_remote) != (prior_size, prior_sha):
        _fail("LATEST_CHANGED")

    _verify_remote_directories(client)
    temp = publisher.write_temp_bytes(state_dir, latest_raw, ".tmp-delta-latest-")
    try:
        client.copyto(temp, latest_remote, immutable=False)
        publisher.verify_remote_bytes(client, latest_remote, len(latest_raw), latest_sha)
    finally:
        try:
            temp.unlink()
        except OSError:
            pass
    return latest_payload, latest_raw, latest_sha


def _write_success_state(state_dir: Path, info: dict, baseline: dict,
                         index_sha: str, latest_sha: str, latest_payload: dict) -> dict:
    plan = info["plan"]
    checkpoint = {
        "version": 1,
        "status": "complete",
        "scope": "immutable_lossless_delta_generation",
        "generation_id": plan["generation_id"],
        "plan_sha256": info["plan_sha256"],
        "baseline_generation_id": baseline["generation_id"],
        "baseline_source_sha256": baseline["source_sha256"],
        "parent_generation_id": plan["parent_generation_id"],
        "latest_generation_id": latest_payload["generation_id"],
        "index_sha256": index_sha,
        "latest_sha256": latest_sha,
        "through_event_id": plan["through_event_id"],
        "last_success": latest_payload["last_success"],
        "rclone_compare_and_swap": False,
    }
    publisher.atomic_json(publisher.state_file_path(state_dir, "delta-published-checkpoint.json"), checkpoint)
    state_path, current = publisher._current_state(state_dir)
    current.update({"last_success": latest_payload["last_success"],
                    "phase": "complete", "generation_id": plan["generation_id"]})
    atomic = getattr(publisher, "atomic_json")
    atomic(state_path, current)
    return checkpoint


def publish_delta(generation_dir, remote, state_dir, rclone_bin="rclone") -> dict:
    """Publish and verify one prepared delta; all errors preserve prior success state."""
    state_dir = publisher.ensure_private_state_dir(state_dir)
    current_path = publisher.state_file_path(state_dir, "current_state.json")
    started_at = publisher.now_utc()
    generation_id = None
    try:
        remote = publisher.validate_remote(remote)
        root = publisher.generation_root(generation_dir)
        with publisher.PublishLock(state_dir):
            current_path, current = publisher._current_state(state_dir)
            current = dict(current)
            current.update({"last_attempt": started_at, "phase": "preflight"})
            atomic_json = publisher.atomic_json
            atomic_json(current_path, current)

            info = _preflight(root)
            plan = info["plan"]
            generation_id = info["generation_id"]
            current.update({"phase": "validating_remote_baseline", "generation_id": generation_id})
            atomic_json(current_path, current)

            progress = publisher._load_progress_state(
                state_dir, generation_id, info["plan_sha256"], remote)
            publisher._save_progress(progress["path"], progress)
            client = publisher.Rclone(rclone_bin)
            try:
                baseline, base_index, latest, prior_raw, prior_size, prior_sha, chain_state = _remote_baseline(
                    client, remote, plan)
            except publisher.PublishError as exc:
                if exc.category in ("REMOTE_OBJECT_MISSING", "BASELINE_NOT_READY"):
                    current.update({"phase": "pending_baseline_publication",
                                    "generation_id": generation_id})
                    atomic_json(current_path, current)
                    return {
                        "status": "pending_baseline_publication",
                        "scope": "immutable_lossless_delta_generation",
                        "generation_id": generation_id,
                        "baseline_generation_id": plan["baseline_generation_id"],
                        "checkpoint_advanced": False,
                    }
                raise

            # A completed remote latest is an idempotent success. Still verify
            # the full local and remote file inventory before checkpointing.
            if latest.get("generation_id") == generation_id:
                current.update({"phase": "verifying_published_generation"})
                atomic_json(current_path, current)
                publisher._publish_files(client, root, state_dir, info["files"], progress)
                index, index_size, index_sha = _validate_current_generation_latest(
                    client, remote, latest, baseline, info, info["files"])
                latest_sha = hashlib.sha256(prior_raw).hexdigest()
                index_remote = publisher.join_remote(
                    remote, f"deltas/generations/{generation_id}/index.json")
                _index_raw, verified_index_size, verified_index_sha = _read_remote_object(
                    client, index_remote)
                if (verified_index_size, verified_index_sha) != (index_size, index_sha):
                    _fail("DELTA_INDEX_READBACK_MISMATCH")
                prepared_index = progress.get("prepared_index")
                if (prepared_index is not None
                        and (prepared_index.get("sha256") != index_sha
                             or prepared_index.get("completed_at") != index.get("completed_at"))):
                    _fail("PROGRESS_STATE_MISMATCH")
                old_latest_payload = progress.get("delta_latest_payload")
                if old_latest_payload is not None and old_latest_payload != latest:
                    _fail("PROGRESS_STATE_MISMATCH")
                if progress.get("latest_payload_sha256") not in (None, latest_sha):
                    _fail("PROGRESS_STATE_MISMATCH")
                progress["prepared_index"] = {
                    "completed_at": index["completed_at"], "sha256": index_sha,
                }
                progress.setdefault("receipts", {})["generation_index"] = {
                    "bytes": index_size, "sha256": index_sha, "verified": True,
                }
                progress["delta_latest_payload"] = latest
                progress["latest_payload_sha256"] = latest_sha
                publisher._save_progress(progress["path"], progress)
                _verify_remote_directories(client)
                checkpoint = _write_success_state(state_dir, info, baseline, index_sha,
                                                 latest_sha, latest)
                return {
                    "status": "complete", "scope": "immutable_lossless_delta_generation",
                    "generation_id": generation_id, "baseline_generation_id": baseline["generation_id"],
                    "index_sha256": index_sha, "latest_sha256": latest_sha,
                    "file_count": len(info["files"]), "through_event_id": plan["through_event_id"],
                    "checkpoint_advanced": True,
                    "checkpoint": checkpoint,
                }

            if latest.get("generation_id") != plan["expected_previous_generation_id"]:
                _fail("LATEST_GENERATION_MISMATCH")
            previous_id = chain_state["generation_id"]
            if plan["parent_generation_id"] != (
                    baseline["generation_id"] if plan["replaces_delta_chain"] else previous_id):
                _fail("DELTA_PARENT_MISMATCH")
            if plan["replaces_delta_chain"]:
                if (plan["supersedes_generation_id"] is not None
                        and plan["supersedes_generation_id"] != previous_id):
                    _fail("DELTA_RESET_SUPERSEDES_MISMATCH")
            elif (chain_state["through_event_id"] is not None
                  and plan["after_event_id"] != chain_state["through_event_id"]):
                _fail("DELTA_EVENT_RANGE_GAP")
            elif (chain_state["source_schema_sha256"] is not None
                  and plan["source_schema_sha256"] != chain_state["source_schema_sha256"]):
                _fail("DELTA_SCHEMA_DRIFT_WITHOUT_RESET")

            current.update({"phase": "publishing_delta_artifacts"})
            atomic_json(current_path, current)
            publisher._publish_files(client, root, state_dir, info["files"], progress)

            current.update({"phase": "publishing_delta_index"})
            atomic_json(current_path, current)
            index, index_raw, index_size, index_sha = _get_or_create_index(
                client, root, state_dir, remote, info, progress)

            current.update({"phase": "publishing_full_state_latest"})
            atomic_json(current_path, current)
            latest_payload, latest_raw, latest_sha = _publish_latest(
                client, state_dir, remote, info, baseline, chain_state,
                prior_raw, prior_size, prior_sha, index, index_raw, index_size, index_sha,
                progress)
            # Verify latest again through its complete byte representation.
            published_latest, verify_raw, verify_size, verify_sha = _remote_json(
                client, publisher.join_remote(remote, "latest.json"))
            if (verify_raw != latest_raw or verify_size != len(latest_raw)
                    or verify_sha != latest_sha or published_latest != latest_payload):
                _fail("LATEST_READBACK_MISMATCH")
            _verify_remote_directories(client)
            _write_success_state(state_dir, info, baseline, index_sha, latest_sha, latest_payload)
            return {
                "status": "complete", "scope": "immutable_lossless_delta_generation",
                "generation_id": generation_id, "baseline_generation_id": baseline["generation_id"],
                "index_sha256": index_sha, "latest_sha256": latest_sha,
                "file_count": len(info["files"]), "through_event_id": plan["through_event_id"],
                "checkpoint_advanced": True,
            }
    except Exception as exc:
        category = exc.category if isinstance(exc, publisher.PublishError) else "INTERNAL_ERROR"
        try:
            with publisher.PublishLock(state_dir):
                current_path, latest_current = publisher._current_state(state_dir)
                # A later invocation may have completed between releasing the
                # failed attempt's lock and reacquiring it here. Never replace
                # that newer state with a stale failure record.
                if latest_current.get("last_attempt") == started_at:
                    latest_current = dict(latest_current)
                    latest_current["last_attempt"] = started_at
                    latest_current["last_failure"] = {"at": started_at, "category": category}
                    latest_current["phase"] = "failed"
                    if generation_id is not None:
                        latest_current["generation_id"] = generation_id
                    publisher.atomic_json(current_path, latest_current)
        except Exception:
            pass
        raise


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--generation-dir", type=Path, required=True)
    parser.add_argument("--remote", required=True)
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument("--rclone-bin", default="rclone")
    args = parser.parse_args(argv)
    result = publish_delta(args.generation_dir, args.remote, args.state_dir, args.rclone_bin)
    # Keep stdout to identifiers and counts; never print archived values.
    print(json.dumps({key: result[key] for key in (
        "status", "scope", "generation_id", "baseline_generation_id", "index_sha256",
        "latest_sha256", "file_count", "through_event_id", "checkpoint_advanced",
    ) if key in result}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
