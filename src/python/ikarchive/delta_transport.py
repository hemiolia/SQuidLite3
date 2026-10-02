"""Lossless SQLite transport for bounded change-feed deltas.

The transport database keeps a no-affinity table for every source table and a
small operation index that maps source identities to the corresponding row in
those tables.  Full source DDL and ``table_xinfo`` metadata remain available in
the document and in the transport database's metadata table.
"""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import itertools
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import stat
from typing import Any, Iterable, Iterator, Optional

from .change_feed import CHANGE_TABLE, read_change_batch
from .lossless_sqlite import _bootstrap_sqlite_reserved_tables
from .reconciliation import read_reconciliation


_GENERATION_RE = re.compile(r"[0-9]{8}T[0-9]{6}Z-[0-9a-f]{8}\Z", re.ASCII)
_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z", re.ASCII)
_METADATA_FIELDS = {"type", "name", "tbl_name", "sql"}
_XINFO_FIELDS = {"cid", "name", "type", "notnull", "dflt_value", "pk", "hidden"}
_FOREIGN_KEY_FIELDS = {"id", "seq", "table", "from", "to", "on_update", "on_delete", "match"}
_OP_FIELDS = {"table_name", "columns", "source_rowid", "identity_json", "operation", "values"}
_MAX_INT64 = (1 << 63) - 1
_MIN_INT64 = -(1 << 63)
_OPS_COLUMNS = (
    ("operation_ordinal", "INTEGER", 0, 1),
    ("table_name", "TEXT", 1, 0),
    ("operation", "TEXT", 1, 0),
    ("source_rowid", "INTEGER", 0, 0),
    ("identity_json", "TEXT", 0, 0),
    ("row_ordinal", "INTEGER", 0, 0),
    ("transport_rowid", "INTEGER", 0, 0),
    ("identity_key", "TEXT", 0, 0),
)


class DeltaTransportError(ValueError):
    """A safe error code plus a receipt describing the failed artifact."""

    def __init__(self, code: str, receipt: dict[str, Any]):
        super().__init__(code)
        self.code = code
        self.receipt = receipt


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _sqlite_name_key(value: str) -> str:
    return "".join(chr(ord(char) + 32) if "A" <= char <= "Z" else char for char in value)


def _canonical_json(value: Any) -> str:
    try:
        return json.dumps(
            value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
        )
    except (TypeError, ValueError, UnicodeError) as exc:
        raise ValueError("METADATA_INVALID") from exc


def _json_equal(left: Any, right: Any) -> bool:
    try:
        return _canonical_json(left) == _canonical_json(right)
    except ValueError:
        return False


def _sha256_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _hash_file(path: Path) -> tuple[int, str]:
    _check_path_components(path, require_exists=True)
    try:
        info = path.lstat()
        if not stat.S_ISREG(info.st_mode):
            raise ValueError("DATABASE_PATH_INVALID")
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        fd = os.open(path, flags)
    except OSError as exc:
        raise ValueError("DATABASE_READ_FAILED") from exc
    total = 0
    digest = hashlib.sha256()
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise ValueError("DATABASE_PATH_INVALID")
        with os.fdopen(fd, "rb", closefd=False) as stream:
            for block in iter(lambda: stream.read(1024 * 1024), b""):
                total += len(block)
                digest.update(block)
    finally:
        os.close(fd)
    return total, digest.hexdigest()


def _check_path_components(path: Path, *, require_exists: bool = False) -> Path:
    raw = Path(path)
    if ".." in raw.parts:
        raise ValueError("OUTPUT_PATH_INVALID")
    absolute = Path(os.path.abspath(os.fspath(raw)))
    chain = [absolute, *absolute.parents]
    for item in chain:
        try:
            mode = item.lstat().st_mode
        except FileNotFoundError:
            continue
        except OSError as exc:
            raise ValueError("OUTPUT_PATH_INVALID") from exc
        if stat.S_ISLNK(mode):
            raise ValueError("OUTPUT_PATH_SYMLINK")
    if require_exists and not absolute.exists():
        raise ValueError("DATABASE_MISSING")
    return absolute


def _coerce_path(value: Any, code: str) -> Path:
    try:
        raw = os.fspath(value)
    except (TypeError, ValueError, OSError) as exc:
        raise ValueError(code) from exc
    if not isinstance(raw, str) or not raw:
        raise ValueError(code)
    return Path(raw)


def _timestamp(value: Any) -> bool:
    if not isinstance(value, str) or not value:
        return False
    candidate = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError:
        return False
    return parsed.tzinfo is not None and parsed.utcoffset() == timezone.utc.utcoffset(parsed)


def _int(value: Any, *, minimum: Optional[int] = None) -> bool:
    return type(value) is int and (minimum is None or value >= minimum)


def _type_ok(value: Any) -> bool:
    if value is None or type(value) in (str, bytes):
        return True
    if type(value) is int:
        return _MIN_INT64 <= value <= _MAX_INT64
    if type(value) is float:
        return not math.isnan(value)
    return False


def _same_value(left: Any, right: Any) -> bool:
    if type(left) is not type(right):
        return False
    if isinstance(left, float):
        return left.hex() == right.hex()
    return left == right


def _same_values(left: Iterable[Any], right: Iterable[Any]) -> bool:
    left_tuple, right_tuple = tuple(left), tuple(right)
    return len(left_tuple) == len(right_tuple) and all(
        _same_value(a, b) for a, b in zip(left_tuple, right_tuple)
    )


def _schema_hash(schemas: list[dict[str, Any]]) -> str:
    return _sha256_bytes(_canonical_json(schemas).encode("utf-8"))


def _table_specs(metadata: dict[str, Any]) -> tuple[list[str], dict[str, list[dict[str, Any]]], dict[str, list[str]]]:
    schemas = metadata.get("schemas")
    if not isinstance(schemas, list):
        raise ValueError("SCHEMA_METADATA_INVALID")
    objects = []
    table_names = []
    for item in schemas:
        if not isinstance(item, dict) or set(item) != _METADATA_FIELDS:
            raise ValueError("SCHEMA_METADATA_INVALID")
        if (item.get("type") not in ("table", "index", "view", "trigger")
                or not isinstance(item.get("name"), str) or not item["name"]
                or not isinstance(item.get("tbl_name"), str)
                or (item.get("sql") is not None and not isinstance(item["sql"], str))):
            raise ValueError("SCHEMA_METADATA_INVALID")
        objects.append((item["type"], item["name"]))
        if item["type"] == "table":
            table_names.append(item["name"])
            sql = item.get("sql") or ""
            if re.match(r"\s*CREATE\s+VIRTUAL\s+TABLE\b", sql, re.IGNORECASE):
                raise ValueError("UNSUPPORTED_VIRTUAL_TABLE")
    if objects != sorted(objects) or len(objects) != len(set(objects)):
        raise ValueError("SCHEMA_METADATA_INVALID")
    folded = [_sqlite_name_key(name) for name in table_names]
    if len(folded) != len(set(folded)):
        raise ValueError("SCHEMA_METADATA_INVALID")

    source_counts = metadata.get("source_row_counts")
    if (not isinstance(source_counts, dict) or set(source_counts) != set(table_names)
            or any(not _int(value, minimum=0) for value in source_counts.values())):
        raise ValueError("SOURCE_ROW_COUNTS_INVALID")

    source_columns = metadata.get("source_table_columns")
    if not isinstance(source_columns, dict) or set(source_columns) != set(table_names):
        raise ValueError("SOURCE_TABLE_COLUMNS_INVALID")
    source_foreign_keys = metadata.get("source_foreign_keys")
    if not isinstance(source_foreign_keys, dict) or set(source_foreign_keys) != set(table_names):
        raise ValueError("SOURCE_FOREIGN_KEYS_INVALID")
    for table, foreign_keys in source_foreign_keys.items():
        if not isinstance(foreign_keys, list):
            raise ValueError("SOURCE_FOREIGN_KEYS_INVALID")
        pairs = []
        for foreign_key in foreign_keys:
            if not isinstance(foreign_key, dict) or set(foreign_key) != _FOREIGN_KEY_FIELDS:
                raise ValueError("SOURCE_FOREIGN_KEYS_INVALID")
            if (not _int(foreign_key.get("id"), minimum=0)
                    or not _int(foreign_key.get("seq"), minimum=0)
                    or not isinstance(foreign_key.get("table"), str) or not foreign_key["table"]
                    or not isinstance(foreign_key.get("from"), str) or not foreign_key["from"]
                    or (foreign_key.get("to") is not None and not isinstance(foreign_key["to"], str))
                    or any(not isinstance(foreign_key.get(field), str)
                           for field in ("on_update", "on_delete", "match"))):
                raise ValueError("SOURCE_FOREIGN_KEYS_INVALID")
            pairs.append((foreign_key["id"], foreign_key["seq"]))
        if pairs != sorted(set(pairs)):
            raise ValueError("SOURCE_FOREIGN_KEYS_INVALID")
    xinfo_by_table: dict[str, list[dict[str, Any]]] = {}
    visible_by_table: dict[str, list[str]] = {}
    for table in table_names:
        columns = source_columns[table]
        if not isinstance(columns, list) or not columns:
            raise ValueError("SOURCE_TABLE_COLUMNS_INVALID")
        copied = []
        names = []
        for column in columns:
            if not isinstance(column, dict) or set(column) != _XINFO_FIELDS:
                raise ValueError("SOURCE_TABLE_COLUMNS_INVALID")
            if (not _int(column.get("cid"), minimum=0)
                    or not isinstance(column.get("name"), str) or not column["name"]
                    or not isinstance(column.get("type"), str)
                    or type(column.get("notnull")) is not int or column["notnull"] not in (0, 1)
                    or not _int(column.get("pk"), minimum=0)
                    or not _int(column.get("hidden"), minimum=0)
                    or (column.get("dflt_value") is not None
                        and type(column.get("dflt_value")) not in (str, int, float))):
                raise ValueError("SOURCE_TABLE_COLUMNS_INVALID")
            if type(column.get("dflt_value")) is float and not math.isfinite(column["dflt_value"]):
                raise ValueError("SOURCE_TABLE_COLUMNS_INVALID")
            copied.append(dict(column))
            if column["hidden"] != 1:
                names.append(column["name"])
        if [column["cid"] for column in copied] != list(range(len(copied))):
            raise ValueError("SOURCE_TABLE_COLUMNS_INVALID")
        if len({_sqlite_name_key(name) for name in names}) != len(names):
            raise ValueError("SOURCE_TABLE_COLUMNS_INVALID")
        xinfo_by_table[table] = copied
        visible_by_table[table] = names

    if not _int(metadata.get("after_event_id"), minimum=0) or not _int(metadata.get("through_event_id"), minimum=0):
        raise ValueError("EVENT_RANGE_INVALID")
    if metadata["after_event_id"] > metadata["through_event_id"]:
        raise ValueError("EVENT_RANGE_INVALID")
    if not _timestamp(metadata.get("captured_at")):
        raise ValueError("CAPTURE_TIME_INVALID")
    if metadata.get("kind") not in ("change_feed", "baseline_reconciliation"):
        raise ValueError("DELTA_KIND_INVALID")
    for key in ("generation_id", "parent_generation_id", "baseline_generation_id"):
        if key not in metadata:
            raise ValueError("GENERATION_ID_INVALID")
        value = metadata.get(key)
        if value is not None and (not isinstance(value, str) or not _GENERATION_RE.fullmatch(value)):
            raise ValueError("GENERATION_ID_INVALID")
    if not isinstance(metadata.get("generation_id"), str):
        raise ValueError("GENERATION_ID_INVALID")
    schema_sha = metadata.get("source_schema_sha256")
    if not isinstance(schema_sha, str) or not _SHA256_RE.fullmatch(schema_sha) or schema_sha != _schema_hash(schemas):
        raise ValueError("SOURCE_SCHEMA_HASH_MISMATCH")

    kind = metadata["kind"]
    if kind == "change_feed":
        if metadata.get("requires_baseline_reconciliation") is not False:
            raise ValueError("CHANGE_FEED_COVERAGE_INVALID")
        for key in ("untracked_tables", "mismatched_triggers", "event_tables_missing_from_schema"):
            if metadata.get(key) != []:
                raise ValueError("CHANGE_FEED_COVERAGE_INVALID")
        tracked = metadata.get("tracked_tables")
        replaced = metadata.get("replace_tables")
        expected_tracked = sorted(
            name for name in table_names
            if name != CHANGE_TABLE and not name.casefold().startswith("sqlite_")
        )
        expected_replaced = sorted(
            name for name in table_names
            if name != CHANGE_TABLE and name.casefold().startswith("sqlite_")
        )
        if tracked != expected_tracked or replaced != expected_replaced or CHANGE_TABLE not in table_names:
            raise ValueError("CHANGE_FEED_COVERAGE_INVALID")
    else:
        removed = metadata.get("removed_tables")
        baseline_counts = metadata.get("baseline_row_counts")
        if (not isinstance(removed, list) or any(not isinstance(name, str) or not name for name in removed)
                or removed != sorted(set(removed)) or set(removed) & set(table_names)
                or not isinstance(baseline_counts, dict)
                or set(baseline_counts) - (set(table_names) | set(removed))
                or not set(removed) <= set(baseline_counts)
                or any(not _int(value, minimum=0) for value in baseline_counts.values())):
            raise ValueError("RECONCILIATION_METADATA_INVALID")
        if ("baseline_source_sha256" in metadata
                and (not isinstance(metadata["baseline_source_sha256"], str)
                     or not _SHA256_RE.fullmatch(metadata["baseline_source_sha256"]))):
            raise ValueError("RECONCILIATION_METADATA_INVALID")
    _canonical_json(metadata)
    return table_names, xinfo_by_table, visible_by_table


def _unique_name(base: str, occupied: set[str]) -> str:
    candidate = base
    suffix = 0
    while candidate.casefold() in occupied:
        suffix += 1
        candidate = f"{base}_{suffix}"
    occupied.add(candidate.casefold())
    return candidate


def _internal_names(metadata: dict[str, Any], table_names: list[str]) -> dict[str, str]:
    occupied = {name.casefold() for name in table_names}
    occupied.update(item["name"].casefold() for item in metadata["schemas"])
    result = {
        "operations_table": _unique_name("__ikarchive_delta_operations__", occupied),
        "metadata_table": _unique_name("__ikarchive_delta_metadata__", occupied),
        "identity_unique_index": _unique_name("__ikarchive_delta_identity_uq__", occupied),
        "row_ordinal_unique_index": _unique_name("__ikarchive_delta_ordinal_uq__", occupied),
    }
    return result


def _metadata_json(metadata: dict[str, Any]) -> str:
    return _canonical_json(metadata)


def _parse_identity(identity_json: str) -> list[dict[str, Any]]:
    if not isinstance(identity_json, str) or not identity_json:
        raise ValueError("IDENTITY_INVALID")
    try:
        identity = json.loads(identity_json, object_pairs_hook=lambda pairs: _unique_object(pairs))
    except (ValueError, json.JSONDecodeError, TypeError) as exc:
        raise ValueError("IDENTITY_INVALID") from exc
    if not isinstance(identity, list) or not identity:
        raise ValueError("IDENTITY_INVALID")
    for part in identity:
        if not isinstance(part, dict) or set(part) != {"column", "sqlite_type", "value"}:
            raise ValueError("IDENTITY_INVALID")
        if not isinstance(part["column"], str) or not part["column"]:
            raise ValueError("IDENTITY_INVALID")
        kind, value = part["sqlite_type"], part["value"]
        if kind == "integer":
            if (not isinstance(value, str) or not re.fullmatch(r"-?(0|[1-9][0-9]*)", value, re.ASCII)
                    or str(int(value)) != value or not _MIN_INT64 <= int(value) <= _MAX_INT64):
                raise ValueError("IDENTITY_INVALID")
        elif kind == "real":
            if not isinstance(value, str):
                raise ValueError("IDENTITY_INVALID")
            try:
                number = float(value)
            except ValueError as exc:
                raise ValueError("IDENTITY_INVALID") from exc
            if math.isnan(number):
                raise ValueError("IDENTITY_INVALID")
        elif kind == "text":
            if not isinstance(value, str):
                raise ValueError("IDENTITY_INVALID")
        elif kind == "blob":
            if (not isinstance(value, str) or len(value) % 2
                    or not re.fullmatch(r"[0-9A-Fa-f]*", value, re.ASCII)):
                raise ValueError("IDENTITY_INVALID")
        else:
            raise ValueError("IDENTITY_INVALID")
    if _canonical_json(identity) != identity_json:
        raise ValueError("IDENTITY_INVALID")
    return identity


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate key")
        result[key] = value
    return result


def _identity_key(table: str, rowid: Optional[int], identity_json: Optional[str]) -> Optional[str]:
    if rowid is not None:
        return _canonical_json([table, "rowid", rowid])
    if identity_json is not None:
        identity = _parse_identity(identity_json)
        normalized = []
        for part in identity:
            kind, value = part["sqlite_type"], part["value"]
            if kind == "integer":
                normalized_value = str(int(value))
            elif kind == "real":
                normalized_value = float(value).hex()
            elif kind == "blob":
                normalized_value = bytes.fromhex(value).hex().upper()
            else:
                normalized_value = value
            normalized.append({
                "column": _sqlite_name_key(part["column"]),
                "sqlite_type": kind,
                "value": normalized_value,
            })
        normalized.sort(key=lambda part: part["column"])
        return _canonical_json([table, "primary_key", normalized])
    return None


def _validate_record(record: Any, table_names: set[str], visible_columns: dict[str, list[str]],
                     cleared: set[str], clear_closed: set[str], active_clear: Optional[str],
                     seen_table_ops: set[str]) -> tuple[dict[str, Any], Optional[str], Optional[str]]:
    if not isinstance(record, dict) or set(record) != _OP_FIELDS:
        raise ValueError("DELTA_RECORD_INVALID")
    table = record.get("table_name")
    op = record.get("operation")
    if not isinstance(table, str) or table not in table_names or op not in ("upsert", "delete", "clear_table"):
        raise ValueError("DELTA_RECORD_INVALID")
    if active_clear is not None and table != active_clear:
        clear_closed.add(active_clear)
        active_clear = None
    if table in clear_closed:
        raise ValueError("CLEAR_TABLE_ORDER_INVALID")
    columns = record.get("columns")
    if not isinstance(columns, list) or columns != visible_columns[table]:
        raise ValueError("DELTA_COLUMNS_MISMATCH")
    source_rowid = record.get("source_rowid")
    identity_json = record.get("identity_json")
    if source_rowid is not None and (not _int(source_rowid) or not _MIN_INT64 <= source_rowid <= _MAX_INT64):
        raise ValueError("DELTA_IDENTITY_INVALID")
    if identity_json is not None:
        _parse_identity(identity_json)
    if source_rowid is not None and identity_json is not None:
        raise ValueError("DELTA_IDENTITY_INVALID")
    if op == "clear_table":
        if (record.get("values") is not None or source_rowid is not None or identity_json is not None
                or table in cleared or table in seen_table_ops):
            raise ValueError("CLEAR_TABLE_INVALID")
        cleared.add(table)
        seen_table_ops.add(table)
        active_clear = table
        return dict(record), None, active_clear
    if op == "upsert":
        values = record.get("values")
        if not isinstance(values, tuple) or len(values) != len(columns) or any(not _type_ok(value) for value in values):
            raise ValueError("DELTA_VALUES_INVALID")
        if source_rowid is None and identity_json is None and table not in cleared:
            raise ValueError("DELTA_IDENTITY_REQUIRED")
        seen_table_ops.add(table)
        return dict(record), _identity_key(table, source_rowid, identity_json), active_clear
    if record.get("values") is not None or source_rowid is None and identity_json is None or table in cleared:
        raise ValueError("DELTA_DELETE_INVALID")
    seen_table_ops.add(table)
    return dict(record), _identity_key(table, source_rowid, identity_json), active_clear


def _bootstrap_reserved_tables(conn: sqlite3.Connection, table_names: list[str]) -> None:
    reserved = sorted(name for name in table_names if name.casefold().startswith("sqlite_"))
    for table in reserved:
        try:
            _bootstrap_sqlite_reserved_tables(conn, table)
        except ValueError as exc:
            raise ValueError("UNSUPPORTED_SQLITE_RESERVED_TABLE") from exc
    actual_reserved = {
        row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name GLOB 'sqlite_*'")
    }
    if actual_reserved != {name for name in reserved}:
        raise ValueError("UNSUPPORTED_SQLITE_RESERVED_TABLE")


def _create_source_tables(conn: sqlite3.Connection, table_names: list[str], xinfo: dict[str, list[dict[str, Any]]],
                          visible_columns: dict[str, list[str]]) -> None:
    # Bootstrap SQLite-owned table names before creating user tables: a source
    # table may itself use one of the bootstrap helper names.
    _bootstrap_reserved_tables(conn, table_names)
    reserved = {name.casefold() for name in table_names if name.casefold().startswith("sqlite_")}
    for table in table_names:
        columns = visible_columns[table]
        if not columns:
            raise ValueError("SOURCE_TABLE_HAS_NO_VISIBLE_COLUMNS")
        if table.casefold() in reserved:
            actual = [row[1] for row in conn.execute(f"PRAGMA table_xinfo({_quote(table)})")]
            if actual != [row["name"] for row in xinfo[table]]:
                raise ValueError("SQLITE_INTERNAL_COLUMNS_MISMATCH")
            continue
        definitions = ",".join(_quote(name) for name in columns)
        conn.execute(f"CREATE TABLE {_quote(table)} ({definitions})")


def _fsync_file_and_parent(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    parent_fd = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(parent_fd)
    finally:
        os.close(parent_fd)


def _failure_receipt(path: Optional[Path], code: str, *, created: bool) -> dict[str, Any]:
    receipt = {"status": "failed", "error": code, "created": created}
    if path is not None:
        receipt["database_path"] = str(path)
    if created and path is not None:
        receipt["created_path"] = str(path)
        try:
            if path.is_file():
                receipt["database"] = {"bytes": path.stat().st_size, "sha256": _hash_file(path)[1]}
        except (OSError, ValueError):
            pass
    return receipt


def _normalize_exception(exc: BaseException) -> str:
    if isinstance(exc, DeltaTransportError):
        return exc.code
    if isinstance(exc, (sqlite3.IntegrityError,)):
        return "DUPLICATE_OR_CONSTRAINT_FAILURE"
    if isinstance(exc, (sqlite3.Error,)):
        return "DATABASE_OPERATION_FAILED"
    if isinstance(exc, FileExistsError):
        return "OUTPUT_ALREADY_EXISTS"
    text = str(exc)
    allowed = {
        "OUTPUT_ALREADY_EXISTS", "OUTPUT_PATH_INVALID", "OUTPUT_PATH_SYMLINK",
        "OUTPUT_PARENT_MISSING", "METADATA_INVALID", "SCHEMA_METADATA_INVALID",
        "SOURCE_ROW_COUNTS_INVALID", "SOURCE_TABLE_COLUMNS_INVALID", "SOURCE_FOREIGN_KEYS_INVALID",
        "EVENT_RANGE_INVALID",
        "CAPTURE_TIME_INVALID", "DELTA_KIND_INVALID", "GENERATION_ID_INVALID",
        "SOURCE_SCHEMA_HASH_MISMATCH", "CHANGE_FEED_COVERAGE_INVALID",
        "RECONCILIATION_METADATA_INVALID", "UNSUPPORTED_VIRTUAL_TABLE",
        "IDENTITY_INVALID", "DELTA_RECORD_INVALID", "DELTA_COLUMNS_MISMATCH",
        "DELTA_IDENTITY_INVALID", "CLEAR_TABLE_INVALID", "DELTA_VALUES_INVALID",
        "DELTA_IDENTITY_REQUIRED", "DELTA_DELETE_INVALID", "CLEAR_TABLE_ORDER_INVALID",
        "UNSUPPORTED_SQLITE_RESERVED_TABLE", "SQLITE_INTERNAL_COLUMNS_MISMATCH",
        "SOURCE_TABLE_HAS_NO_VISIBLE_COLUMNS", "DATABASE_MISSING", "DATABASE_PATH_INVALID",
        "DATABASE_READ_FAILED", "DATABASE_SIDECAR_PRESENT", "SOURCE_CONNECTION_INVALID",
        "SOURCE_SCHEMA_MISMATCH", "SOURCE_ROW_COUNTS_MISMATCH", "SOURCE_TABLE_COLUMNS_MISMATCH",
        "TRANSPORT_DOCUMENT_INVALID", "TRANSPORT_DATABASE_MISMATCH", "TRANSPORT_INVENTORY_INVALID",
        "TRANSPORT_METADATA_MISMATCH", "TRANSPORT_RECORDS_INVALID", "DUPLICATE_IDENTITY",
        "DELTA_STREAM_FAILED", "SOURCE_LOOKUP_MISMATCH", "SOURCE_DELETE_STILL_EXISTS",
        "FULL_REPLACEMENT_MISMATCH", "CHANGE_FEED_MISMATCH", "CHANGE_FEED_COVERAGE_INVALID",
        "DELTA_DATABASE_INSERT_INVALID", "DATABASE_OPERATION_FAILED",
        "DUPLICATE_OR_CONSTRAINT_FAILURE",
        "BASELINE_CONNECTION_REQUIRED", "BASELINE_CONNECTION_INVALID",
        "RECONCILIATION_STREAM_MISMATCH", "RECONCILIATION_METADATA_MISMATCH",
        "SOURCE_FOREIGN_KEYS_MISMATCH",
    }
    return text if text in allowed else "INTERNAL_ERROR"


def build_delta_database(records_iterator: Iterable[dict[str, Any]], metadata: dict[str, Any],
                         output_path: str | os.PathLike[str]) -> dict[str, Any]:
    """Stream change records into a new no-affinity SQLite transport database."""
    path: Optional[Path] = None
    created = False
    conn: Optional[sqlite3.Connection] = None
    try:
        raw_path = _coerce_path(output_path, "OUTPUT_PATH_INVALID")
        path = Path(os.path.abspath(os.fspath(raw_path)))
        _check_path_components(raw_path)
        if not path.parent.is_dir():
            raise ValueError("OUTPUT_PARENT_MISSING")
        if path.exists():
            raise FileExistsError("OUTPUT_ALREADY_EXISTS")
        if not isinstance(metadata, dict):
            raise ValueError("METADATA_INVALID")
        table_names, xinfo_by_table, visible_columns = _table_specs(metadata)
        metadata_encoded = _metadata_json(metadata)
        # O_EXCL prevents a race from replacing a file created after the check.
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_NOFOLLOW", 0), 0o600)
        os.close(fd)
        created = True
        conn = sqlite3.connect(path)
        conn.execute("PRAGMA journal_mode=DELETE")
        conn.execute("PRAGMA synchronous=FULL")
        conn.execute("BEGIN IMMEDIATE")

        _create_source_tables(conn, table_names, xinfo_by_table, visible_columns)
        internal = _internal_names(metadata, table_names)
        ops_name = internal["operations_table"]
        meta_name = internal["metadata_table"]
        conn.execute(
            f"CREATE TABLE {_quote(ops_name)} ("
            '"operation_ordinal" INTEGER PRIMARY KEY, "table_name" TEXT NOT NULL, '
            '"operation" TEXT NOT NULL, "source_rowid" INTEGER, "identity_json" TEXT, '
            '"row_ordinal" INTEGER, "transport_rowid" INTEGER, "identity_key" TEXT)'
        )
        conn.execute(
            f'CREATE TABLE {_quote(meta_name)} ("name" TEXT NOT NULL, "value" TEXT NOT NULL)'
        )
        conn.execute(
            f"CREATE UNIQUE INDEX {_quote(internal['identity_unique_index'])} "
            f'ON {_quote(ops_name)} ("identity_key") WHERE "identity_key" IS NOT NULL'
        )
        conn.execute(
            f"CREATE UNIQUE INDEX {_quote(internal['row_ordinal_unique_index'])} "
            f'ON {_quote(ops_name)} ("table_name", "row_ordinal") WHERE "row_ordinal" IS NOT NULL'
        )
        conn.execute(f'INSERT INTO {_quote(meta_name)} ("name", "value") VALUES (?, ?)',
                     ("source_metadata", metadata_encoded))

        insert_sql = {
            table: f"INSERT INTO {_quote(table)} VALUES ({','.join('?' for _ in visible_columns[table])})"
            for table in table_names
        }
        op_sql = (
            f"INSERT INTO {_quote(ops_name)} "
            '("operation_ordinal","table_name","operation","source_rowid","identity_json",'
            '"row_ordinal","transport_rowid","identity_key") VALUES (?,?,?,?,?,?,?,?)'
        )
        table_set = set(table_names)
        cleared: set[str] = set()
        clear_closed: set[str] = set()
        seen_table_ops: set[str] = set()
        active_clear: Optional[str] = None
        row_ordinals = {table: 0 for table in table_names}
        upsert_counts = {table: 0 for table in table_names}
        op_counts = {"upsert": 0, "delete": 0, "clear_table": 0}
        operation_ordinal = 0
        iterator = iter(records_iterator)
        while True:
            try:
                record = next(iterator)
            except StopIteration:
                break
            except Exception as exc:
                raise ValueError("DELTA_STREAM_FAILED") from exc
            try:
                normalized, identity_key, active_clear = _validate_record(
                    record, table_set, visible_columns, cleared, clear_closed,
                    active_clear, seen_table_ops,
                )
                table = normalized["table_name"]
                op = normalized["operation"]
                row_ordinal = None
                transport_rowid = None
                if op == "upsert":
                    row_ordinal = row_ordinals[table]
                    try:
                        cursor = conn.execute(insert_sql[table], normalized["values"])
                    except sqlite3.Error as exc:
                        raise ValueError("DELTA_VALUES_INVALID") from exc
                    transport_rowid = cursor.lastrowid
                    if not _int(transport_rowid, minimum=1):
                        raise ValueError("DELTA_DATABASE_INSERT_INVALID")
                    row_ordinals[table] += 1
                    upsert_counts[table] += 1
                try:
                    conn.execute(op_sql, (
                        operation_ordinal, table, op, normalized["source_rowid"],
                        normalized["identity_json"], row_ordinal, transport_rowid, identity_key,
                    ))
                except sqlite3.IntegrityError as exc:
                    if identity_key is not None:
                        raise ValueError("DUPLICATE_IDENTITY") from exc
                    raise ValueError("DUPLICATE_OR_CONSTRAINT_FAILURE") from exc
                op_counts[op] += 1
                operation_ordinal += 1
            except DeltaTransportError:
                raise
            except ValueError:
                raise
            except sqlite3.Error as exc:
                raise ValueError("DATABASE_OPERATION_FAILED") from exc

        conn.commit()
        conn.close()
        conn = None
        _fsync_file_and_parent(path)
        database_bytes, database_sha = _hash_file(path)
        document = {
            "version": 1,
            "status": "built",
            "generation_id": metadata["generation_id"],
            "parent_generation_id": metadata.get("parent_generation_id"),
            "baseline_generation_id": metadata.get("baseline_generation_id"),
            "kind": metadata["kind"],
            "captured_at": metadata["captured_at"],
            "after_event_id": metadata["after_event_id"],
            "through_event_id": metadata["through_event_id"],
            "source_schema_sha256": metadata["source_schema_sha256"],
            "source_row_counts": dict(metadata["source_row_counts"]),
            "source_table_columns": metadata["source_table_columns"],
            "schemas": metadata["schemas"],
            "metadata": json.loads(metadata_encoded),
            "internal_names": internal,
            "table_names": table_names,
            "upsert_counts_by_table": {name: count for name, count in upsert_counts.items() if count},
            "operation_counts": op_counts,
            "database": {"path": str(path), "bytes": database_bytes, "sha256": database_sha},
        }
        return document
    except BaseException as exc:
        if conn is not None:
            try:
                conn.rollback()
            except sqlite3.Error:
                pass
            try:
                conn.close()
            except sqlite3.Error:
                pass
        if not isinstance(exc, Exception):
            raise
        code = _normalize_exception(exc)
        receipt = _failure_receipt(path, code, created=created)
        raise DeltaTransportError(code, receipt) from exc


def _open_database(db_path: str | os.PathLike[str]) -> tuple[Path, sqlite3.Connection]:
    path = _check_path_components(_coerce_path(db_path, "DATABASE_PATH_INVALID"), require_exists=True)
    for suffix in ("-wal", "-shm", "-journal"):
        sidecar = Path(str(path) + suffix)
        try:
            sidecar.lstat()
        except FileNotFoundError:
            continue
        except OSError as exc:
            raise ValueError("DATABASE_SIDECAR_PRESENT") from exc
        raise ValueError("DATABASE_SIDECAR_PRESENT")
    uri = path.as_uri() + "?mode=ro&immutable=1"
    try:
        conn = sqlite3.connect(uri, uri=True)
        conn.execute("PRAGMA query_only=ON")
        conn.execute("PRAGMA trusted_schema=OFF")
    except sqlite3.Error as exc:
        raise ValueError("DATABASE_READ_FAILED") from exc
    return path, conn


def _validate_document(doc: Any, db_path: str | os.PathLike[str]) -> tuple[dict[str, Any], dict[str, Any], list[str], dict[str, list[dict[str, Any]]], dict[str, list[str]], Path]:
    required = {
        "version", "status", "generation_id", "parent_generation_id", "baseline_generation_id",
        "kind", "captured_at", "after_event_id", "through_event_id", "source_schema_sha256",
        "source_row_counts", "source_table_columns", "schemas", "metadata", "internal_names",
        "table_names", "upsert_counts_by_table", "operation_counts", "database",
    }
    if (not isinstance(doc, dict) or set(doc) != required
            or type(doc.get("version")) is not int or doc.get("version") != 1
            or doc.get("status") != "built"):
        raise ValueError("TRANSPORT_DOCUMENT_INVALID")
    metadata = doc.get("metadata")
    if not isinstance(metadata, dict):
        raise ValueError("TRANSPORT_DOCUMENT_INVALID")
    table_names, xinfo, visible = _table_specs(metadata)
    if (doc.get("generation_id") != metadata.get("generation_id")
            or doc.get("parent_generation_id") != metadata.get("parent_generation_id")
            or doc.get("baseline_generation_id") != metadata.get("baseline_generation_id")
            or doc.get("kind") != metadata.get("kind")
            or doc.get("captured_at") != metadata.get("captured_at")
            or not _json_equal(doc.get("after_event_id"), metadata.get("after_event_id"))
            or not _json_equal(doc.get("through_event_id"), metadata.get("through_event_id"))
            or doc.get("source_schema_sha256") != metadata.get("source_schema_sha256")
            or not _json_equal(doc.get("source_row_counts"), metadata.get("source_row_counts"))
            or not _json_equal(doc.get("source_table_columns"), metadata.get("source_table_columns"))
            or not _json_equal(doc.get("schemas"), metadata.get("schemas"))
            or doc.get("table_names") != table_names):
        raise ValueError("TRANSPORT_DOCUMENT_INVALID")
    internal = doc.get("internal_names")
    expected_internal = _internal_names(metadata, table_names)
    if internal != expected_internal:
        raise ValueError("TRANSPORT_DOCUMENT_INVALID")
    database = doc.get("database")
    if (not isinstance(database, dict) or set(database) != {"path", "bytes", "sha256"}
            or not isinstance(database.get("path"), str)
            or not _int(database.get("bytes"), minimum=1)
            or not isinstance(database.get("sha256"), str) or not _SHA256_RE.fullmatch(database["sha256"])):
        raise ValueError("TRANSPORT_DOCUMENT_INVALID")
    upsert_counts = doc.get("upsert_counts_by_table")
    operation_counts = doc.get("operation_counts")
    if (not isinstance(upsert_counts, dict)
            or any(table not in table_names or not _int(count, minimum=1)
                   for table, count in upsert_counts.items())
            or not isinstance(operation_counts, dict)
            or set(operation_counts) != {"upsert", "delete", "clear_table"}
            or any(not _int(count, minimum=0) for count in operation_counts.values())):
        raise ValueError("TRANSPORT_DOCUMENT_INVALID")
    if not _json_equal(doc.get("metadata"), metadata):
        raise ValueError("TRANSPORT_DOCUMENT_INVALID")
    path = _check_path_components(_coerce_path(db_path, "DATABASE_PATH_INVALID"), require_exists=True)
    if str(path) != database["path"]:
        raise ValueError("TRANSPORT_DOCUMENT_INVALID")
    return metadata, internal, table_names, xinfo, visible, path


def _check_database_hash(path: Path, database: dict[str, Any]) -> None:
    size, digest = _hash_file(path)
    if (size, digest) != (database["bytes"], database["sha256"]):
        raise ValueError("TRANSPORT_DATABASE_MISMATCH")


def _validate_database_inventory(conn: sqlite3.Connection, metadata: dict[str, Any], internal: dict[str, str],
                                 table_names: list[str], xinfo_by_table: dict[str, list[dict[str, Any]]],
                                 visible_columns: dict[str, list[str]]) -> None:
    expected_tables = set(table_names) | {internal["operations_table"], internal["metadata_table"]}
    actual_tables = {
        row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }
    if actual_tables != expected_tables:
        raise ValueError("TRANSPORT_INVENTORY_INVALID")
    expected_indexes = {internal["identity_unique_index"], internal["row_ordinal_unique_index"]}
    actual_indexes = {
        row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='index'")
    }
    if actual_indexes != expected_indexes:
        raise ValueError("TRANSPORT_INVENTORY_INVALID")
    other_objects = conn.execute(
        "SELECT type,name FROM sqlite_master WHERE type NOT IN ('table','index')"
    ).fetchall()
    if other_objects:
        raise ValueError("TRANSPORT_INVENTORY_INVALID")

    for table in table_names:
        rows = conn.execute(f"PRAGMA table_xinfo({_quote(table)})").fetchall()
        if [row[1] for row in rows] != visible_columns[table]:
            raise ValueError("TRANSPORT_INVENTORY_INVALID")
        expected_rows = [
            (index, name, "", 0, None, 0, 0)
            for index, name in enumerate(visible_columns[table])
        ]
        if [tuple(row) for row in rows] != expected_rows:
            raise ValueError("TRANSPORT_INVENTORY_INVALID")
    ops = internal["operations_table"]
    ops_rows = conn.execute(f"PRAGMA table_xinfo({_quote(ops)})").fetchall()
    expected_ops_rows = [
        (index, name, declared_type, not_null, None, primary_key, 0)
        for index, (name, declared_type, not_null, primary_key) in enumerate(_OPS_COLUMNS)
    ]
    if [tuple(row) for row in ops_rows] != expected_ops_rows:
        raise ValueError("TRANSPORT_INVENTORY_INVALID")
    meta = internal["metadata_table"]
    meta_rows = conn.execute(f"PRAGMA table_xinfo({_quote(meta)})").fetchall()
    if [tuple(row) for row in meta_rows] != [
            (0, "name", "TEXT", 1, None, 0, 0),
            (1, "value", "TEXT", 1, None, 0, 0)]:
        raise ValueError("TRANSPORT_INVENTORY_INVALID")
    index_specs = {}
    for index_name in expected_indexes:
        rows = conn.execute(f"PRAGMA index_info({_quote(index_name)})").fetchall()
        index_specs[index_name] = [row[2] for row in rows]
        listing = conn.execute(f"PRAGMA index_list({_quote(ops)})").fetchall()
        info = next((row for row in listing if row[1] == index_name), None)
        if info is None or info[2] != 1 or info[4] != 1:
            raise ValueError("TRANSPORT_INVENTORY_INVALID")
    if index_specs[internal["identity_unique_index"]] != ["identity_key"]:
        raise ValueError("TRANSPORT_INVENTORY_INVALID")
    if index_specs[internal["row_ordinal_unique_index"]] != ["table_name", "row_ordinal"]:
        raise ValueError("TRANSPORT_INVENTORY_INVALID")

    stored = conn.execute(f"SELECT name,value FROM {_quote(meta)}").fetchall()
    if len(stored) != 1 or stored[0][0] != "source_metadata" or stored[0][1] != _metadata_json(metadata):
        raise ValueError("TRANSPORT_METADATA_MISMATCH")


def _transport_rowid_alias(columns: list[str]) -> Optional[str]:
    names = {column.casefold() for column in columns}
    return next((name for name in ("_rowid_", "rowid", "oid") if name.casefold() not in names), None)


def _iter_transport(conn: sqlite3.Connection, metadata: dict[str, Any], internal: dict[str, str],
                    table_names: list[str], visible_columns: dict[str, list[str]],
                    doc_counts: Optional[dict[str, int]] = None,
                    doc_operation_counts: Optional[dict[str, int]] = None) -> Iterator[dict[str, Any]]:
    ops = internal["operations_table"]
    cursor = conn.execute(
        f"SELECT operation_ordinal,table_name,operation,source_rowid,identity_json,row_ordinal,transport_rowid,identity_key "
        f"FROM {_quote(ops)} ORDER BY operation_ordinal"
    )
    table_cursors: dict[str, sqlite3.Cursor] = {}
    row_ordinals = {table: 0 for table in table_names}
    op_counts = {"upsert": 0, "delete": 0, "clear_table": 0}
    cleared: set[str] = set()
    clear_closed: set[str] = set()
    seen_table_ops: set[str] = set()
    active_clear: Optional[str] = None
    table_set = set(table_names)
    seen_ops = 0
    for row in cursor:
        ordinal, table, op, source_rowid, identity_json, row_ordinal, transport_rowid, identity_key = row
        if (not _int(ordinal, minimum=0) or ordinal != seen_ops or table not in visible_columns
                or op not in ("upsert", "delete", "clear_table")):
            raise ValueError("TRANSPORT_RECORDS_INVALID")
        seen_ops += 1
        columns = visible_columns[table]
        values = None
        if op == "upsert":
            if (not _int(row_ordinal, minimum=0) or row_ordinal != row_ordinals[table]
                    or not _int(transport_rowid, minimum=1)
                    or (source_rowid is not None and not _int(source_rowid))
                    or (identity_json is not None and not isinstance(identity_json, str))
                    or identity_key != _identity_key(table, source_rowid, identity_json)):
                raise ValueError("TRANSPORT_RECORDS_INVALID")
            row_ordinals[table] += 1
            alias = _transport_rowid_alias(columns)
            if alias is not None:
                fetched = conn.execute(
                    f"SELECT * FROM {_quote(table)} WHERE {_quote(alias)}=?", (transport_rowid,)
                ).fetchone()
            else:
                table_cursor = table_cursors.get(table)
                if table_cursor is None:
                    table_cursor = conn.execute(f"SELECT * FROM {_quote(table)} NOT INDEXED")
                    table_cursors[table] = table_cursor
                fetched = table_cursor.fetchone()
            if fetched is None or len(fetched) != len(columns):
                raise ValueError("TRANSPORT_RECORDS_INVALID")
            values = tuple(fetched)
            if any(not _type_ok(value) for value in values):
                raise ValueError("TRANSPORT_RECORDS_INVALID")
        else:
            if (row_ordinal is not None or transport_rowid is not None
                    or (source_rowid is not None and not _int(source_rowid))
                    or (identity_json is not None and not isinstance(identity_json, str))):
                raise ValueError("TRANSPORT_RECORDS_INVALID")
            if op == "clear_table":
                if (source_rowid is not None or identity_json is not None or identity_key is not None):
                    raise ValueError("TRANSPORT_RECORDS_INVALID")
            else:
                if ((source_rowid is None) == (identity_json is None)
                        or identity_key != _identity_key(table, source_rowid, identity_json)):
                    raise ValueError("TRANSPORT_RECORDS_INVALID")
        record = {
            "table_name": table,
            "columns": columns,
            "source_rowid": source_rowid,
            "identity_json": identity_json,
            "operation": op,
            "values": values,
        }
        try:
            record, computed_key, active_clear = _validate_record(
                record, table_set, visible_columns, cleared, clear_closed, active_clear, seen_table_ops,
            )
        except ValueError as exc:
            raise ValueError("TRANSPORT_RECORDS_INVALID") from exc
        if computed_key != identity_key:
            raise ValueError("TRANSPORT_RECORDS_INVALID")
        op_counts[op] += 1
        yield record
    if doc_counts is not None:
        expected = {table: doc_counts.get(table, 0) for table in table_names}
        if row_ordinals != expected:
            raise ValueError("TRANSPORT_RECORDS_INVALID")
    if doc_operation_counts is not None and op_counts != doc_operation_counts:
        raise ValueError("TRANSPORT_RECORDS_INVALID")
    if any(conn.execute(f"SELECT COUNT(*) FROM {_quote(table)}").fetchone()[0] != row_ordinals[table]
           for table in table_names):
        raise ValueError("TRANSPORT_RECORDS_INVALID")
    for table, table_cursor in table_cursors.items():
        if table_cursor.fetchone() is not None:
            raise ValueError("TRANSPORT_RECORDS_INVALID")


def iter_delta_records(db_path: str | os.PathLike[str], doc: dict[str, Any]) -> Iterator[dict[str, Any]]:
    """Yield records in original feed order, resolving values from no-affinity tables."""
    path: Optional[Path] = None
    try:
        metadata, internal, table_names, _xinfo, visible, path = _validate_document(doc, db_path)
        database = doc["database"]
        _check_database_hash(path, database)
        _opened_path, conn = _open_database(path)
        try:
            _validate_database_inventory(conn, metadata, internal, table_names,
                                         doc["source_table_columns"], visible)
            yield from _iter_transport(conn, metadata, internal, table_names, visible,
                                       doc["upsert_counts_by_table"], doc["operation_counts"])
        finally:
            conn.close()
    except Exception as exc:
        code = _normalize_exception(exc)
        raise DeltaTransportError(code, _failure_receipt(path, code, created=False)) from exc


def _source_snapshot(conn: sqlite3.Connection, metadata: dict[str, Any], table_names: list[str],
                     xinfo_expected: dict[str, list[dict[str, Any]]]) -> None:
    if not isinstance(conn, sqlite3.Connection) or conn.text_factory is not str or not conn.in_transaction:
        raise ValueError("SOURCE_CONNECTION_INVALID")
    attached = [row[1] for row in conn.execute("PRAGMA database_list") if row[1] not in ("main", "temp")]
    if attached or conn.execute("SELECT 1 FROM sqlite_temp_master LIMIT 1").fetchone() is not None:
        raise ValueError("SOURCE_CONNECTION_INVALID")
    schemas = [
        {"type": row[0], "name": row[1], "tbl_name": row[2], "sql": row[3]}
        for row in conn.execute("SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name")
    ]
    if schemas != metadata["schemas"] or _schema_hash(schemas) != metadata["source_schema_sha256"]:
        raise ValueError("SOURCE_SCHEMA_MISMATCH")
    actual_tables = [item["name"] for item in schemas if item["type"] == "table"]
    if actual_tables != table_names:
        raise ValueError("SOURCE_SCHEMA_MISMATCH")
    counts = {
        table: int(conn.execute(f"SELECT COUNT(*) FROM {_quote(table)}").fetchone()[0])
        for table in table_names
    }
    if counts != metadata["source_row_counts"]:
        raise ValueError("SOURCE_ROW_COUNTS_MISMATCH")
    actual_xinfo = {}
    actual_foreign_keys = {}
    for table in table_names:
        actual_xinfo[table] = [
            {"cid": row[0], "name": row[1], "type": row[2], "notnull": row[3],
             "dflt_value": row[4], "pk": row[5], "hidden": row[6]}
            for row in conn.execute(f"PRAGMA table_xinfo({_quote(table)})")
        ]
        actual_foreign_keys[table] = [
            {"id": row[0], "seq": row[1], "table": row[2], "from": row[3], "to": row[4],
             "on_update": row[5], "on_delete": row[6], "match": row[7]}
            for row in conn.execute(f"PRAGMA foreign_key_list({_quote(table)})")
        ]
    if actual_xinfo != xinfo_expected or actual_xinfo != metadata["source_table_columns"]:
        raise ValueError("SOURCE_TABLE_COLUMNS_MISMATCH")
    if actual_foreign_keys != metadata["source_foreign_keys"]:
        raise ValueError("SOURCE_FOREIGN_KEYS_MISMATCH")
    table_flags = {
        row[1]: {"type": row[2], "without_rowid": bool(row[4])}
        for row in conn.execute("PRAGMA table_list") if row[0] == "main"
    }
    if any(table_flags.get(name, {}).get("type") in ("virtual", "shadow") for name in table_names):
        raise ValueError("UNSUPPORTED_VIRTUAL_TABLE")


def _source_rowid_alias(conn: sqlite3.Connection, table: str, columns: list[str]) -> Optional[str]:
    flags = next((row for row in conn.execute("PRAGMA table_list")
                  if row[0] == "main" and row[1] == table), None)
    if flags is None or flags[2] != "table" or bool(flags[4]):
        return None
    folded = {name.casefold() for name in columns}
    return next((name for name in ("_rowid_", "rowid", "oid") if name.casefold() not in folded), None)


def _primary_key_columns(xinfo: list[dict[str, Any]]) -> list[str]:
    return [row["name"] for row in sorted((item for item in xinfo if item["pk"]), key=lambda item: item["pk"])]


def _decode_identity_value(part: dict[str, Any]) -> Any:
    kind, value = part["sqlite_type"], part["value"]
    if kind == "integer":
        return int(value)
    if kind == "real":
        return float(value)
    if kind == "blob":
        return bytes.fromhex(value)
    return value


def _source_lookup(conn: sqlite3.Connection, table: str, columns: list[str], xinfo: list[dict[str, Any]],
                   source_rowid: Optional[int], identity_json: Optional[str]) -> Optional[tuple[Any, ...]]:
    if source_rowid is not None:
        alias = _source_rowid_alias(conn, table, columns)
        if alias is None:
            raise ValueError("SOURCE_LOOKUP_MISMATCH")
        rows = conn.execute(
            f"SELECT * FROM {_quote(table)} WHERE {_quote(alias)}=? LIMIT 2", (source_rowid,)
        ).fetchall()
        if len(rows) > 1:
            raise ValueError("SOURCE_LOOKUP_MISMATCH")
        return tuple(rows[0]) if rows else None
    if identity_json is None:
        raise ValueError("SOURCE_LOOKUP_MISMATCH")
    identity = _parse_identity(identity_json)
    pk_columns = _primary_key_columns(xinfo)
    if [item["column"] for item in identity] != pk_columns or not pk_columns:
        raise ValueError("SOURCE_LOOKUP_MISMATCH")
    values = {column: identity[index] for index, column in enumerate(pk_columns)}
    where = " AND ".join(f"{_quote(column)}=?" for column in pk_columns)
    parameters = tuple(_decode_identity_value(values[column]) for column in pk_columns)
    rows = conn.execute(f"SELECT * FROM {_quote(table)} WHERE {where} LIMIT 2", parameters).fetchall()
    if len(rows) > 1:
        raise ValueError("SOURCE_LOOKUP_MISMATCH")
    if not rows:
        return None
    row = tuple(rows[0])
    # Check the encoded key against the named PK columns. Do not assume PK
    # columns occupy the first positions in SELECT *.
    by_column = dict(zip(columns, row))
    for column in pk_columns:
        expected = _decode_identity_value(values[column])
        actual = by_column[column]
        if not _same_value(expected, actual):
            raise ValueError("SOURCE_LOOKUP_MISMATCH")
    return row


def _source_rows_for_replacement(conn: sqlite3.Connection, table: str,
                                 columns: list[str]) -> Iterator[tuple[Optional[int], tuple[Any, ...]]]:
    fields = ",".join(_quote(column) for column in columns)
    alias = _source_rowid_alias(conn, table, columns)
    if alias is not None:
        query = f"SELECT {_quote(alias)},{fields} FROM {_quote(table)} ORDER BY {_quote(alias)}"
        for row in conn.execute(query):
            yield row[0], tuple(row[1:])
        return
    query = f"SELECT {fields} FROM {_quote(table)}"
    for row in conn.execute(query):
        yield None, tuple(row)


def _records_equal(left: dict[str, Any], right: dict[str, Any]) -> bool:
    return (left["table_name"] == right["table_name"]
            and left["columns"] == right["columns"]
            and left["source_rowid"] == right["source_rowid"]
            and left["identity_json"] == right["identity_json"]
            and left["operation"] == right["operation"]
            and (left["values"] is None and right["values"] is None
                 or left["values"] is not None and right["values"] is not None
                 and _same_values(left["values"], right["values"])))


def _verify_change_feed_source(conn: sqlite3.Connection, metadata: dict[str, Any],
                               records: Iterator[dict[str, Any]]) -> tuple[dict[str, int], dict[str, int]]:
    batch = read_change_batch(conn, metadata["after_event_id"])
    try:
        batch.__enter__()
        for key in (
            "after_event_id", "through_event_id", "schemas", "source_schema_sha256",
            "source_row_counts", "source_table_columns", "source_foreign_keys",
            "tracked_tables", "replace_tables",
            "untracked_tables", "mismatched_triggers", "event_tables_missing_from_schema",
            "requires_baseline_reconciliation",
        ):
            if batch.metadata.get(key) != metadata.get(key):
                raise ValueError("CHANGE_FEED_MISMATCH")
        sentinel = object()
        expected_iter = batch.iter_current_changes()
        counts = {"upsert": 0, "delete": 0, "clear_table": 0}
        by_table: dict[str, int] = {}
        for left, right in itertools.zip_longest(records, expected_iter, fillvalue=sentinel):
            if left is sentinel or right is sentinel or not _records_equal(left, right):
                raise ValueError("CHANGE_FEED_MISMATCH")
            counts[left["operation"]] += 1
            if left["operation"] == "upsert":
                table = left["table_name"]
                by_table[table] = by_table.get(table, 0) + 1
        return counts, by_table
    finally:
        batch.__exit__(None, None, None)


def _verify_reconciliation_source(conn: sqlite3.Connection, metadata: dict[str, Any],
                                  records: Iterator[dict[str, Any]],
                                  xinfo_by_table: dict[str, list[dict[str, Any]]],
                                  expected_records: Iterator[dict[str, Any]]) -> tuple[dict[str, int], dict[str, int]]:
    counts = {"upsert": 0, "delete": 0, "clear_table": 0}
    by_table: dict[str, int] = {}
    cleared: set[str] = set()
    clear_closed: set[str] = set()
    active_clear: Optional[str] = None
    replacement_iterators: dict[str, Iterator[tuple[Optional[int], tuple[Any, ...]]]] = {}
    replacement_counts: dict[str, int] = {}

    def finish_table(table: str) -> None:
        iterator = replacement_iterators[table]
        if next(iterator, None) is not None:
            raise ValueError("FULL_REPLACEMENT_MISMATCH")
        if replacement_counts[table] != metadata["source_row_counts"][table]:
            raise ValueError("FULL_REPLACEMENT_MISMATCH")

    sentinel = object()
    for record, expected_record in itertools.zip_longest(
            records, expected_records, fillvalue=sentinel):
        if (record is sentinel or expected_record is sentinel
                or not _records_equal(record, expected_record)):
            raise ValueError("RECONCILIATION_STREAM_MISMATCH")
        table = record["table_name"]
        op = record["operation"]
        if active_clear is not None and table != active_clear:
            finish_table(active_clear)
            clear_closed.add(active_clear)
            active_clear = None
        if table in clear_closed:
            raise ValueError("FULL_REPLACEMENT_MISMATCH")

        counts[op] += 1
        if op == "clear_table":
            if table in cleared:
                raise ValueError("FULL_REPLACEMENT_MISMATCH")
            cleared.add(table)
            active_clear = table
            replacement_iterators[table] = _source_rows_for_replacement(
                conn, table, record["columns"]
            )
            replacement_counts[table] = 0
            continue

        if op == "upsert" and active_clear == table:
            expected = next(replacement_iterators[table], None)
            if (expected is None or not _same_values(record["values"], expected[1])
                    or record["source_rowid"] != expected[0]
                    or record["identity_json"] is not None):
                raise ValueError("FULL_REPLACEMENT_MISMATCH")
            replacement_counts[table] += 1
            by_table[table] = by_table.get(table, 0) + 1
            continue

        if op == "upsert":
            if record["source_rowid"] is None and record["identity_json"] is None:
                raise ValueError("SOURCE_LOOKUP_MISMATCH")
            found = _source_lookup(conn, table, record["columns"], xinfo_by_table[table],
                                   record["source_rowid"], record["identity_json"])
            if found is None or not _same_values(found, record["values"]):
                raise ValueError("SOURCE_LOOKUP_MISMATCH")
            by_table[table] = by_table.get(table, 0) + 1
        elif op == "delete":
            found = _source_lookup(conn, table, record["columns"], xinfo_by_table[table],
                                   record["source_rowid"], record["identity_json"])
            if found is not None:
                raise ValueError("SOURCE_DELETE_STILL_EXISTS")

    if active_clear is not None:
        finish_table(active_clear)
    if cleared != clear_closed | ({active_clear} if active_clear is not None else set()):
        # Every clear table was finalized at its next table or at end-of-stream.
        raise ValueError("FULL_REPLACEMENT_MISMATCH")
    return counts, by_table


def _verify_reconciliation_metadata(actual: dict[str, Any], expected: dict[str, Any]) -> None:
    fields = (
        "kind", "after_event_id", "through_event_id", "schemas", "source_schema_sha256",
        "source_row_counts", "source_table_columns", "source_foreign_keys",
        "tracked_tables", "replace_tables",
        "untracked_tables", "mismatched_triggers", "event_tables_missing_from_schema",
        "requires_baseline_reconciliation", "removed_tables", "baseline_row_counts",
    )
    if any(actual.get(field) != expected.get(field) for field in fields):
        raise ValueError("RECONCILIATION_METADATA_MISMATCH")


def verify_delta_database(source_conn: sqlite3.Connection, db_path: str | os.PathLike[str],
                          doc: dict[str, Any], *,
                          baseline_conn: Optional[sqlite3.Connection] = None) -> dict[str, Any]:
    """Verify the immutable transport file and bind all operations to one source snapshot."""
    path: Optional[Path] = None
    try:
        metadata, internal, table_names, xinfo_by_table, visible_columns, path = _validate_document(doc, db_path)
        if metadata["kind"] == "baseline_reconciliation":
            if baseline_conn is None:
                raise ValueError("BASELINE_CONNECTION_REQUIRED")
            if not isinstance(baseline_conn, sqlite3.Connection) or baseline_conn is source_conn:
                raise ValueError("BASELINE_CONNECTION_INVALID")
            if baseline_conn.text_factory is not str:
                raise ValueError("BASELINE_CONNECTION_INVALID")
            attached = [row[1] for row in baseline_conn.execute("PRAGMA database_list")
                        if row[1] not in ("main", "temp")]
            if (attached or baseline_conn.execute(
                    "SELECT 1 FROM sqlite_temp_master LIMIT 1").fetchone() is not None):
                raise ValueError("BASELINE_CONNECTION_INVALID")
        elif baseline_conn is not None:
            raise ValueError("BASELINE_CONNECTION_INVALID")
        _source_snapshot(source_conn, metadata, table_names, xinfo_by_table)
        _check_database_hash(path, doc["database"])
        _opened_path, conn = _open_database(path)
        try:
            _validate_database_inventory(conn, metadata, internal, table_names,
                                         xinfo_by_table, visible_columns)
            records = _iter_transport(conn, metadata, internal, table_names, visible_columns,
                                      doc.get("upsert_counts_by_table"), doc.get("operation_counts"))
            if metadata["kind"] == "change_feed":
                actual_counts, actual_per_table = _verify_change_feed_source(
                    source_conn, metadata, records
                )
            else:
                with read_reconciliation(source_conn, baseline_conn) as batch:
                    _verify_reconciliation_metadata(batch.metadata, metadata)
                    actual_counts, actual_per_table = _verify_reconciliation_source(
                        source_conn, metadata, records, xinfo_by_table,
                        batch.iter_current_changes(),
                    )
            if actual_counts != doc.get("operation_counts") or {
                    name: count for name, count in actual_per_table.items() if count
            } != doc.get("upsert_counts_by_table"):
                raise ValueError("TRANSPORT_RECORDS_INVALID")
        finally:
            conn.close()
        return {
            "version": 1,
            "status": "verified",
            "generation_id": metadata["generation_id"],
            "parent_generation_id": metadata.get("parent_generation_id"),
            "baseline_generation_id": metadata.get("baseline_generation_id"),
            "kind": metadata["kind"],
            "captured_at": metadata["captured_at"],
            "source_schema_sha256": metadata["source_schema_sha256"],
            "source_row_counts": dict(metadata["source_row_counts"]),
            "database": dict(doc["database"]),
            "operation_counts": actual_counts,
            "upsert_counts_by_table": {name: count for name, count in actual_per_table.items() if count},
            "verified_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        }
    except Exception as exc:
        code = _normalize_exception(exc)
        raise DeltaTransportError(code, _failure_receipt(path, code, created=False)) from exc


__all__ = [
    "DeltaTransportError",
    "build_delta_database",
    "verify_delta_database",
    "iter_delta_records",
]
