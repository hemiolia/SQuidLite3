"""Lossless, size-bounded SQLite shards for immutable archive snapshots.

The source schema is recorded as metadata.  Each data shard deliberately uses
tables without declared column types so SQLite does not coerce dynamic values.
Large TEXT and BLOB cells are stored as ordered 64 KiB raw-byte chunks in
separate SQLite files and linked from the row shard.
"""

from __future__ import annotations

from collections import OrderedDict
from contextlib import closing, contextmanager
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path, PurePosixPath
import re
import sqlite3
import stat
import tempfile
from typing import Any, Generator, Iterable, Iterator, Optional, Sequence


MANIFEST_VERSION = 2
MANIFEST_ROLE = "lossless_sqlite_shards"
DEFAULT_MAX_BYTES = 20 * 1024 * 1024
MAX_ALLOWED_BYTES = 20 * 1024 * 1024
VALUE_CHUNK_BYTES = 64 * 1024
MIN_MAX_BYTES = 256 * 1024
_DEFAULT_EXTERNAL_THRESHOLD = 256 * 1024
MAX_SELECTOR_BYTES = 20 * 1024 * 1024
_INTERNAL_ROWS = "_archive_rows"
_INTERNAL_EXTERNAL = "_archive_external_cells"
_SELECTOR_MODE = re.compile(r"[a-z0-9_]{1,64}\Z", re.ASCII)
_SELECTOR_RULE = re.compile(r"[A-Za-z0-9_]{1,64}\Z", re.ASCII)


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _file_record(path: Path, root: Path, **metadata: Any) -> dict[str, Any]:
    result = {
        "file": path.relative_to(root).as_posix(),
        "bytes": path.stat().st_size,
        "sha256": _sha256_file(path),
    }
    result.update(metadata)
    return result


def _reject_symlink_components(
    path: Path, *, label: str, within: Optional[Path] = None
) -> Path:
    absolute = Path(os.path.abspath(os.fspath(path)))
    if within is None:
        candidates = [absolute]
    else:
        boundary = Path(os.path.abspath(os.fspath(within)))
        try:
            relative_parts = absolute.relative_to(boundary).parts
        except ValueError as exc:
            raise ValueError(f"{label} path escapes its root") from exc
        candidates = []
        cursor = boundary
        for component in relative_parts:
            cursor = cursor / component
            candidates.append(cursor)
    for cursor in candidates:
        try:
            mode = cursor.lstat().st_mode
        except FileNotFoundError:
            continue
        if stat.S_ISLNK(mode):
            raise ValueError(f"{label} path contains a symbolic link: {cursor}")
    return absolute


def _source_path(source: sqlite3.Connection) -> Optional[Path]:
    row = source.execute("PRAGMA database_list").fetchone()
    if not row or not row[2]:
        return None
    return _reject_symlink_components(Path(row[2]), label="source")


def _prepare_output(source: sqlite3.Connection, output: str | Path) -> Path:
    raw = Path(output)
    root = _reject_symlink_components(raw, label="output")
    if root.exists():
        if not root.is_dir():
            raise ValueError(f"output must be a directory: {root}")
        if any(root.iterdir()):
            raise ValueError(f"output directory must be empty: {root}")
    else:
        root.mkdir(parents=True, exist_ok=False, mode=0o700)
        root.chmod(0o700)

    _reject_source_output_overlap(source, root)
    return root


def _reject_source_output_overlap(source: sqlite3.Connection, root: Path) -> None:
    source_path = _source_path(source)
    if source_path is not None:
        source_resolved = source_path.resolve(strict=True)
        root_resolved = root.resolve(strict=True)
        if source_resolved == root_resolved or source_resolved.is_relative_to(root_resolved):
            raise ValueError("output directory overlaps the source SQLite file")
        if root_resolved == source_resolved or root_resolved.is_relative_to(source_resolved):
            raise ValueError("output directory overlaps the source SQLite file")


def _reject_nonmain_databases(source: sqlite3.Connection) -> None:
    databases = source.execute("PRAGMA database_list").fetchall()
    attached = [row[1] for row in databases if row[1] not in {"main", "temp"}]
    if attached:
        raise ValueError("attached SQLite databases are unsupported: " + ", ".join(attached))
    if source.execute("SELECT 1 FROM sqlite_temp_master LIMIT 1").fetchone() is not None:
        raise ValueError("temporary SQLite schema objects are unsupported")


def _require_standard_text_factory(source: sqlite3.Connection) -> None:
    if source.text_factory is not str:
        raise ValueError("source connection must use sqlite3's standard str text_factory")


@contextmanager
def _read_transaction(source: sqlite3.Connection) -> Iterator[None]:
    owned = not source.in_transaction
    if owned:
        source.execute("BEGIN")
    try:
        yield
    except BaseException:
        if owned and source.in_transaction:
            source.rollback()
        raise
    else:
        if owned and source.in_transaction:
            source.commit()


def _schema_objects(source: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = source.execute(
        "SELECT type, name, tbl_name, sql FROM sqlite_master ORDER BY type, name"
    )
    return [
        {"type": row[0], "name": row[1], "tbl_name": row[2], "sql": row[3]}
        for row in rows
    ]


def _schema_sha256(objects: list[dict[str, Any]]) -> str:
    return _sha256_bytes(_canonical_json_bytes(objects))


def _table_xinfo(source: sqlite3.Connection, table: str) -> list[dict[str, Any]]:
    rows = source.execute(f"PRAGMA table_xinfo({_quote(table)})").fetchall()
    return [
        {
            "cid": row[0],
            "name": row[1],
            "type": row[2],
            "notnull": row[3],
            "dflt_value": row[4],
            "pk": row[5],
            "hidden": row[6],
        }
        for row in rows
    ]


def _select_columns(xinfo: list[dict[str, Any]]) -> list[str]:
    # hidden=1 columns are omitted by SQLite SELECT *; generated columns (2/3)
    # remain visible and their computed values are copied as ordinary cells.
    return [column["name"] for column in xinfo if column["hidden"] != 1]


def _table_list_flags(source: sqlite3.Connection) -> dict[str, dict[str, Any]]:
    try:
        rows = source.execute("PRAGMA table_list").fetchall()
    except sqlite3.DatabaseError:
        return {}
    result: dict[str, dict[str, Any]] = {}
    for row in rows:
        if len(row) >= 6:
            result[row[1]] = {"type": row[2], "ncol": row[3], "wr": row[4], "strict": row[5]}
    return result


def _foreign_keys(source: sqlite3.Connection, table: str) -> list[dict[str, Any]]:
    rows = source.execute(f"PRAGMA foreign_key_list({_quote(table)})").fetchall()
    return [
        {
            "id": row[0],
            "seq": row[1],
            "table": row[2],
            "from": row[3],
            "to": row[4],
            "on_update": row[5],
            "on_delete": row[6],
            "match": row[7],
        }
        for row in rows
    ]


def _source_row_plan(
    source: sqlite3.Connection,
    table: str,
    xinfo: list[dict[str, Any]],
    table_flags: dict[str, dict[str, Any]],
) -> tuple[str, Optional[str], list[str]]:
    visible = _select_columns(xinfo)
    wr = table_flags.get(table, {}).get("wr")
    if wr:
        indexes = source.execute(f"PRAGMA index_list({_quote(table)})").fetchall()
        primary_index = next(
            (row[1] for row in indexes if len(row) > 3 and row[3] == "pk"), None
        )
        if primary_index is not None:
            key_parts = [
                row
                for row in source.execute(f"PRAGMA index_xinfo({_quote(primary_index)})")
                if row[5]
            ]
            key_parts.sort(key=lambda row: row[0])
            order: list[str] = []
            for part in key_parts:
                if part[2] is None:
                    raise ValueError(f"WITHOUT ROWID primary key has an unnamed term: {table}")
                expression = _quote(part[2])
                if part[4]:
                    expression += f" COLLATE {_quote(part[4])}"
                expression += " DESC" if part[3] else " ASC"
                order.append(expression)
        else:
            primary = sorted(
                (column for column in xinfo if column["pk"]),
                key=lambda column: column["pk"],
            )
            order = [_quote(column["name"]) for column in primary]
        if not order:
            raise ValueError(f"WITHOUT ROWID table has no declared primary key: {table}")
        return "without_rowid_primary_key", None, order

    visible_folded = {name.casefold() for name in visible}
    rowid_alias = next(
        (name for name in ("_rowid_", "rowid", "oid") if name.casefold() not in visible_folded),
        None,
    )
    if rowid_alias is not None:
        return "rowid", rowid_alias, [_quote(rowid_alias)]
    # If every built-in alias is shadowed, SQLite's table scan still walks the
    # rowid B-tree in rowid order. There is no legal SQL expression left with
    # which to project the original hidden rowid.
    return "shadowed_rowid_aliases", None, []


def _source_rows(
    source: sqlite3.Connection,
    table: str,
    columns: list[str],
    rowid_alias: Optional[str],
    order_by: list[str],
) -> sqlite3.Cursor:
    quoted_table = _quote(table)
    select = "SELECT *"
    rowid_output_name: Optional[str] = None
    if rowid_alias is not None:
        rowid_output_name = "__archive_source_rowid_value"
        suffix = 0
        while rowid_output_name.casefold() in {name.casefold() for name in columns}:
            suffix += 1
            rowid_output_name = f"__archive_source_rowid_value_{suffix}"
        select += f", {_quote(rowid_alias)} AS {_quote(rowid_output_name)}"
    query = f"{select} FROM {quoted_table}"
    if order_by:
        query += " ORDER BY " + ", ".join(order_by)
    elif rowid_alias is None:
        query += " NOT INDEXED"
    cursor = source.execute(query)
    actual = [column[0] for column in cursor.description or ()]
    expected = list(columns)
    if rowid_output_name is not None:
        expected.append(rowid_output_name)
    if actual != expected:
        raise ValueError(
            f"SELECT * column names mismatch for table {table!r}: "
            f"expected {expected!r}, got {actual!r}"
        )
    return cursor


def _value_payload(value: Any) -> tuple[str, bytes]:
    if value is None:
        return "null", b""
    if isinstance(value, int):
        return "integer", str(value).encode("ascii")
    if isinstance(value, float):
        return "real", value.hex().encode("ascii")
    if isinstance(value, str):
        return "text", value.encode("utf-8", "surrogatepass")
    if isinstance(value, (bytes, bytearray, memoryview)):
        return "blob", bytes(value)
    raise TypeError(f"unsupported SQLite value type: {type(value).__name__}")


def _needs_external(value: Any, byte_length: int, threshold: int) -> bool:
    if not isinstance(value, (str, bytes, bytearray, memoryview)):
        return False
    if byte_length > threshold:
        return True
    if isinstance(value, str):
        try:
            value.encode("utf-8", "strict")
        except UnicodeEncodeError:
            return True
    return False


def _update_stream_digest(
    digest: "hashlib._Hash", row_ordinal: int, source_rowid: Any, values: Sequence[Any]
) -> None:
    digest.update(row_ordinal.to_bytes(8, "big", signed=False))
    if source_rowid is None:
        digest.update(b"R0")
    else:
        kind, payload = _value_payload(source_rowid)
        digest.update(b"R1" + kind.encode("ascii") + b"\0")
        digest.update(len(payload).to_bytes(8, "big"))
        digest.update(payload)
    digest.update(len(values).to_bytes(8, "big"))
    for value in values:
        kind, payload = _value_payload(value)
        marker = kind.encode("ascii")
        digest.update(len(marker).to_bytes(2, "big"))
        digest.update(marker)
        digest.update(len(payload).to_bytes(8, "big"))
        digest.update(payload)


def _db_size(conn: sqlite3.Connection) -> tuple[int, int, int]:
    page_count = int(conn.execute("PRAGMA page_count").fetchone()[0])
    page_size = int(conn.execute("PRAGMA page_size").fetchone()[0])
    return page_count * page_size, page_count, page_size


def _new_unique_internal(base: str, table: str, other: Optional[str] = None) -> str:
    occupied = {table.casefold()}
    if other is not None:
        occupied.add(other.casefold())
    candidate = base
    suffix = 0
    while candidate.casefold() in occupied:
        suffix += 1
        candidate = f"{base}_{suffix}"
    return candidate


def _bootstrap_sqlite_reserved_tables(conn: sqlite3.Connection, source_table: str) -> None:
    lowered = source_table.casefold()
    if lowered == "sqlite_sequence":
        conn.execute(
            'CREATE TABLE "__archive_seq_bootstrap" '
            '(id INTEGER PRIMARY KEY AUTOINCREMENT)'
        )
        conn.execute('DROP TABLE "__archive_seq_bootstrap"')
        return
    if lowered in {"sqlite_stat1", "sqlite_stat4"}:
        conn.execute('CREATE TABLE "__archive_stat_bootstrap" (value)')
        conn.execute('ANALYZE "__archive_stat_bootstrap"')
        conn.execute('DROP TABLE "__archive_stat_bootstrap"')
        generated_stats = [
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name GLOB 'sqlite_stat*'"
            )
        ]
        for stat_table in generated_stats:
            conn.execute(f"DELETE FROM {_quote(stat_table)}")
        return
    if lowered.startswith("sqlite_"):
        raise ValueError(f"unsupported SQLite-reserved source table name: {source_table}")


def _create_shard_schema(
    conn: sqlite3.Connection,
    table: str,
    columns: list[str],
    row_meta_name: str,
    ext_meta_name: str,
) -> None:
    _bootstrap_sqlite_reserved_tables(conn, table)
    if table.casefold() != "sqlite_sequence" and table.casefold() not in {
        "sqlite_stat1",
        "sqlite_stat4",
    }:
        definitions = ", ".join(_quote(name) for name in columns)
        conn.execute(f"CREATE TABLE {_quote(table)} ({definitions})")
    if table.casefold() in {"sqlite_stat1", "sqlite_stat4"}:
        # ANALYZE created this reserved table using no-affinity declarations.
        actual = [row[1] for row in conn.execute(f"PRAGMA table_xinfo({_quote(table)})")]
        if actual != columns:
            raise ValueError(f"reserved stats table columns mismatch for {table}")
    conn.execute(
        f"CREATE TABLE {_quote(row_meta_name)} "
        '("row_ordinal" INTEGER PRIMARY KEY, "source_rowid")'
    )
    conn.execute(
        f"CREATE TABLE {_quote(ext_meta_name)} "
        '("table_id" TEXT NOT NULL, "row_ordinal" INTEGER NOT NULL, "column_ordinal" INTEGER NOT NULL, '
        '"sqlite_type" TEXT NOT NULL, "byte_length" INTEGER NOT NULL, '
        '"sha256" TEXT NOT NULL, "chunk_total" INTEGER NOT NULL, '
        '"value_files_json" TEXT NOT NULL, '
        'PRIMARY KEY ("row_ordinal", "column_ordinal"))'
    )


class _ValueWriter:
    def __init__(self, root: Path, table_dir: Path, table_id: str, table_name: str, limit: int):
        self.root = root
        self.table_dir = table_dir
        self.table_id = table_id
        self.table_name = table_name
        self.limit = limit
        self.conn: Optional[sqlite3.Connection] = None
        self.path: Optional[Path] = None
        self.part_number = 0
        self.part_chunk_count = 0
        self.part_cell_count = 0
        self.last_cell: Optional[tuple[int, int]] = None
        self.files: list[dict[str, Any]] = []

    def _open(self) -> None:
        if self.conn is not None:
            return
        value_dir = self.table_dir / "value"
        value_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        value_dir.chmod(0o700)
        self.part_number += 1
        self.path = value_dir / f"part{self.part_number:06d}.sqlite3"
        if self.path.exists():
            raise FileExistsError(self.path)
        self.conn = sqlite3.connect(self.path)
        self.conn.execute("PRAGMA page_size=4096")
        self.conn.execute("PRAGMA journal_mode=DELETE")
        self.conn.execute("PRAGMA synchronous=FULL")
        self.conn.execute(
            "CREATE TABLE value_chunks ("
            "row_ordinal INTEGER NOT NULL, column_ordinal INTEGER NOT NULL, "
            "chunk_index INTEGER NOT NULL, chunk_total INTEGER NOT NULL, "
            "sqlite_type TEXT NOT NULL, payload BLOB NOT NULL, "
            "PRIMARY KEY(row_ordinal, column_ordinal, chunk_index))"
        )
        self.conn.execute("BEGIN")
        size, _, _ = _db_size(self.conn)
        if size > self.limit:
            self.conn.rollback()
            self.conn.close()
            self.conn = None
            self.path.unlink(missing_ok=True)
            raise ValueError("max_bytes is too small for a value shard schema")
        self.part_chunk_count = 0
        self.part_cell_count = 0
        self.last_cell = None

    def _finish(self) -> None:
        if self.conn is None or self.path is None:
            return
        _, page_count, page_size = _db_size(self.conn)
        self.conn.commit()
        self.conn.close()
        self.conn = None
        size = self.path.stat().st_size
        if page_count * page_size > self.limit or size > self.limit:
            self.path.unlink(missing_ok=True)
            raise ValueError(f"value shard exceeds max_bytes: {self.path.name}")
        self.path.chmod(0o600)
        self.files.append(
            _file_record(
                self.path,
                self.root,
                table_id=self.table_id,
                table_name=self.table_name,
                chunk_count=self.part_chunk_count,
                cell_count=self.part_cell_count,
                page_count=page_count,
                page_size=page_size,
            )
        )

    def write_cell(
        self,
        row_ordinal: int,
        column_ordinal: int,
        sqlite_type: str,
        payload: bytes,
    ) -> tuple[int, list[str]]:
        if not payload:
            raise ValueError("zero-byte values must remain inline to distinguish them from NULL")
        chunk_total = math.ceil(len(payload) / VALUE_CHUNK_BYTES)
        used_files: list[str] = []
        key = (row_ordinal, column_ordinal)
        for chunk_index in range(chunk_total):
            chunk = payload[
                chunk_index * VALUE_CHUNK_BYTES : (chunk_index + 1) * VALUE_CHUNK_BYTES
            ]
            while True:
                self._open()
                assert self.conn is not None and self.path is not None
                self.conn.execute("SAVEPOINT value_chunk")
                self.conn.execute(
                    "INSERT INTO value_chunks "
                    "(row_ordinal,column_ordinal,chunk_index,chunk_total,sqlite_type,payload) "
                    "VALUES(?,?,?,?,?,?)",
                    (row_ordinal, column_ordinal, chunk_index, chunk_total, sqlite_type, chunk),
                )
                size, _, _ = _db_size(self.conn)
                if size <= self.limit:
                    self.conn.execute("RELEASE value_chunk")
                    self.part_chunk_count += 1
                    if self.last_cell != key:
                        self.part_cell_count += 1
                        self.last_cell = key
                    relative = self.path.relative_to(self.root).as_posix()
                    if not used_files or used_files[-1] != relative:
                        used_files.append(relative)
                    break
                self.conn.execute("ROLLBACK TO value_chunk")
                self.conn.execute("RELEASE value_chunk")
                if self.part_chunk_count == 0:
                    raise ValueError("max_bytes is too small for one 64 KiB value chunk")
                self._finish()
        return chunk_total, used_files

    def close(self) -> list[dict[str, Any]]:
        self._finish()
        return self.files


def _open_new_part(
    root: Path,
    table_dir: Path,
    table_id: str,
    part_number: int,
    table_name: str,
    columns: list[str],
    limit: int,
    row_meta_name: str,
    ext_meta_name: str,
) -> tuple[sqlite3.Connection, Path]:
    path = table_dir / f"part{part_number:06d}.sqlite3"
    if path.exists():
        raise FileExistsError(path)
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA page_size=4096")
    conn.execute("PRAGMA journal_mode=DELETE")
    conn.execute("PRAGMA synchronous=FULL")
    _create_shard_schema(conn, table_name, columns, row_meta_name, ext_meta_name)
    # ANALYZE is needed to materialize sqlite_statN tables and can leave a
    # transaction active. Commit the empty schema before row insertion starts.
    conn.commit()
    conn.execute("BEGIN")
    size, _, _ = _db_size(conn)
    if size > limit:
        conn.rollback()
        conn.close()
        path.unlink(missing_ok=True)
        raise ValueError(f"max_bytes is too small for table shard schema: {table_name}")
    return conn, path


def _finish_part(
    conn: sqlite3.Connection,
    path: Path,
    root: Path,
    limit: int,
    table_id: str,
    row_start: int,
    row_end: int,
) -> dict[str, Any]:
    _, page_count, page_size = _db_size(conn)
    conn.commit()
    conn.close()
    actual_bytes = path.stat().st_size
    if page_count * page_size > limit or actual_bytes > limit:
        path.unlink(missing_ok=True)
        raise ValueError(f"table shard exceeds max_bytes: {path.name}")
    path.chmod(0o600)
    return _file_record(
        path,
        root,
        table_id=table_id,
        row_start=row_start,
        row_end=row_end,
        row_count=row_end - row_start,
        page_count=page_count,
        page_size=page_size,
    )


def _metadata_names(table: str) -> tuple[str, str]:
    row_name = _new_unique_internal(_INTERNAL_ROWS, table)
    external_name = _new_unique_internal(_INTERNAL_EXTERNAL, table, row_name)
    return row_name, external_name


def _table_id(index: int, name: str) -> str:
    suffix = _sha256_bytes(name.encode("utf-8", "surrogatepass"))[:12]
    return f"t{index:04d}_{suffix}"


def _exception_receipt(root: Path, filename: str, exc: BaseException, *, snapshot_id: Any = None) -> None:
    if not root.exists() or not root.is_dir() or root.is_symlink():
        return
    target = root / filename
    if target.exists() or target.is_symlink():
        return
    payload = {
        "version": MANIFEST_VERSION,
        "role": MANIFEST_ROLE,
        "status": "failed",
        "snapshot_identifier": snapshot_id,
        "failed_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "error_type": type(exc).__name__,
        # Error strings produced here contain object names and ordinals only;
        # source cell values are never included in exceptions or receipts.
        "message": str(exc)[:500],
    }
    try:
        with target.open("x", encoding="utf-8") as stream:
            json.dump(payload, stream, ensure_ascii=False, sort_keys=True, indent=2)
            stream.write("\n")
        target.chmod(0o600)
    except OSError:
        pass


def export_sqlite_shards(
    sourceconn: sqlite3.Connection,
    output: str | Path,
    *,
    snapshot_id: Any,
    max_bytes: int = DEFAULT_MAX_BYTES,
) -> dict[str, Any]:
    """Export every main-schema table into typed SQLite shards.

    Rows are processed one at a time. No source cell value is printed or placed
    in a manifest. A failure leaves already-created diagnostic fragments and a
    failure receipt in the newly-created output directory.
    """
    if not isinstance(sourceconn, sqlite3.Connection):
        raise TypeError("sourceconn must be a sqlite3.Connection")
    if not isinstance(snapshot_id, str):
        raise TypeError("snapshot_id must be an opaque string")
    if not isinstance(max_bytes, int) or max_bytes < MIN_MAX_BYTES or max_bytes > MAX_ALLOWED_BYTES:
        raise ValueError(
            f"max_bytes must be between {MIN_MAX_BYTES} and {MAX_ALLOWED_BYTES}"
        )
    _require_standard_text_factory(sourceconn)

    root = _prepare_output(sourceconn, output)
    totals = {"rows": 0, "cells": 0, "chunks": 0, "pieces": 0, "external_cells": 0}
    try:
        with _read_transaction(sourceconn):
            _reject_nonmain_databases(sourceconn)
            objects = _schema_objects(sourceconn)
            schema_sha = _schema_sha256(objects)
            table_names = [item["name"] for item in objects if item["type"] == "table"]
            table_flags = _table_list_flags(sourceconn)
            unsupported = [
                name
                for name in table_names
                if table_flags.get(name, {}).get("type") in {"virtual", "shadow"}
            ]
            unsupported.extend(
                item["name"]
                for item in objects
                if item["type"] == "table"
                and item.get("sql")
                and re.match(r"\s*CREATE\s+VIRTUAL\s+TABLE\b", item["sql"], re.IGNORECASE)
                and item["name"] not in unsupported
            )
            if unsupported:
                raise ValueError(
                    "virtual/shadow tables are not supported by this shard format: "
                    + ", ".join(sorted(unsupported))
                )
            table_docs: list[dict[str, Any]] = []
            external_docs: list[dict[str, Any]] = []

            for table_index, table_name in enumerate(table_names, 1):
                xinfo = _table_xinfo(sourceconn, table_name)
                columns = _select_columns(xinfo)
                if not columns:
                    raise ValueError(f"source table has no SELECT * columns: {table_name}")
                table_id = _table_id(table_index, table_name)
                table_dir = root / "shared" / table_id
                table_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
                table_dir.chmod(0o700)
                row_meta_name, ext_meta_name = _metadata_names(table_name)
                rowid_kind, rowid_alias, order_by = _source_row_plan(
                    sourceconn, table_name, xinfo, table_flags
                )
                source_row_count = int(
                    sourceconn.execute(f"SELECT COUNT(*) FROM {_quote(table_name)}").fetchone()[0]
                )
                row_digest = hashlib.sha256()
                values_writer = _ValueWriter(root, table_dir, table_id, table_name, max_bytes)
                threshold = min(_DEFAULT_EXTERNAL_THRESHOLD, max_bytes // 16)

                part_number = 0
                part_conn: Optional[sqlite3.Connection] = None
                part_path: Optional[Path] = None
                part_start = 0
                part_rows = 0
                part_docs: list[dict[str, Any]] = []

                def open_part(start: int) -> None:
                    nonlocal part_number, part_conn, part_path, part_start, part_rows
                    part_number += 1
                    part_conn, part_path = _open_new_part(
                        root,
                        table_dir,
                        table_id,
                        part_number,
                        table_name,
                        columns,
                        max_bytes,
                        row_meta_name,
                        ext_meta_name,
                    )
                    part_start = start
                    part_rows = 0

                def finish_part() -> None:
                    nonlocal part_conn, part_path
                    if part_conn is None or part_path is None:
                        return
                    part_docs.append(
                        _finish_part(
                            part_conn,
                            part_path,
                            root,
                            max_bytes,
                            table_id,
                            part_start,
                            part_start + part_rows,
                        )
                    )
                    part_conn = None
                    part_path = None

                source_cursor = _source_rows(
                    sourceconn, table_name, columns, rowid_alias, order_by
                )
                read_rows = 0
                for source_row in source_cursor:
                    source_values = tuple(source_row[: len(columns)])
                    source_rowid = source_row[-1] if rowid_alias is not None else None
                    ordinal = read_rows
                    if len(source_values) != len(columns):
                        raise ValueError(f"source column count mismatch for {table_name}")
                    _update_stream_digest(row_digest, ordinal, source_rowid, source_values)

                    row_values = list(source_values)
                    external_cells: dict[int, dict[str, Any]] = {}

                    def externalize(column_index: int) -> None:
                        if column_index in external_cells:
                            return
                        value = row_values[column_index]
                        sqlite_type, payload = _value_payload(value)
                        if sqlite_type not in {"text", "blob"} or not payload:
                            raise ValueError(
                                f"cannot externalize source cell at {table_name} "
                                f"row {ordinal}, column {column_index}"
                            )
                        chunk_total, value_files = values_writer.write_cell(
                            ordinal, column_index, sqlite_type, payload
                        )
                        external_cells[column_index] = {
                            "sqlite_type": sqlite_type,
                            "byte_length": len(payload),
                            "sha256": _sha256_bytes(payload),
                            "chunk_total": chunk_total,
                            "value_files_json": json.dumps(
                                value_files, ensure_ascii=False, separators=(",", ":")
                            ),
                        }
                        row_values[column_index] = None

                    for column_index, value in enumerate(row_values):
                        if isinstance(value, (str, bytes, bytearray, memoryview)):
                            _, payload = _value_payload(value)
                            if _needs_external(value, len(payload), threshold):
                                externalize(column_index)

                    while True:
                        if part_conn is None:
                            open_part(ordinal)
                        assert part_conn is not None
                        part_conn.execute("SAVEPOINT append_row")
                        placeholders = ",".join("?" for _ in columns)
                        part_conn.execute(
                            f"INSERT INTO {_quote(table_name)} VALUES ({placeholders})",
                            row_values,
                        )
                        part_conn.execute(
                            f"INSERT INTO {_quote(row_meta_name)} "
                            '("row_ordinal","source_rowid") VALUES (?,?)',
                            (ordinal, source_rowid),
                        )
                        for column_index, metadata in external_cells.items():
                            part_conn.execute(
                                f"INSERT INTO {_quote(ext_meta_name)} "
                                '("table_id","row_ordinal","column_ordinal","sqlite_type",'
                                '"byte_length","sha256","chunk_total","value_files_json") '
                                "VALUES(?,?,?,?,?,?,?,?)",
                                (
                                    table_id,
                                    ordinal,
                                    column_index,
                                    metadata["sqlite_type"],
                                    metadata["byte_length"],
                                    metadata["sha256"],
                                    metadata["chunk_total"],
                                    metadata["value_files_json"],
                                ),
                            )
                        size, _, _ = _db_size(part_conn)
                        if size <= max_bytes:
                            part_conn.execute("RELEASE append_row")
                            part_rows += 1
                            break

                        part_conn.execute("ROLLBACK TO append_row")
                        part_conn.execute("RELEASE append_row")
                        if part_rows:
                            finish_part()
                            continue

                        candidates: list[tuple[int, int]] = []
                        for column_index, value in enumerate(row_values):
                            if column_index in external_cells:
                                continue
                            if isinstance(value, (str, bytes, bytearray, memoryview)):
                                _, payload = _value_payload(value)
                                if payload:
                                    candidates.append((len(payload), column_index))
                        if not candidates:
                            raise ValueError(
                                f"single row cannot fit max_bytes for table {table_name}, "
                                f"row {ordinal}"
                            )
                        _, chosen_column = max(candidates)
                        externalize(chosen_column)

                    read_rows += 1
                    totals["rows"] += 1
                    totals["cells"] += len(columns)
                    totals["external_cells"] += len(external_cells)

                finish_part()
                external_docs.extend(values_writer.close())
                if read_rows != source_row_count:
                    raise ValueError(
                        f"source row count changed for {table_name}: "
                        f"COUNT(*)={source_row_count}, read={read_rows}"
                    )
                if not part_docs:
                    # Empty source tables still have one inspectable, bounded part.
                    open_part(0)
                    finish_part()
                if part_docs[0]["row_start"] != 0 or part_docs[-1]["row_end"] != read_rows:
                    raise ValueError(f"generated row ranges do not cover {table_name}")
                for previous, current in zip(part_docs, part_docs[1:]):
                    if previous["row_end"] != current["row_start"]:
                        raise ValueError(f"generated row ranges overlap or have a gap for {table_name}")

                table_doc: dict[str, Any] = {
                    "name": table_name,
                    "table_id": table_id,
                    "columns": columns,
                    "column_schema": xinfo,
                    "row_count": read_rows,
                    "foreign_keys": _foreign_keys(sourceconn, table_name),
                    "row_stream_sha256": row_digest.hexdigest(),
                    "parts": part_docs,
                    "archive_metadata_tables": {
                        "rows": row_meta_name,
                        "external_cells": ext_meta_name,
                    },
                    "rowid_kind": rowid_kind,
                }
                if rowid_alias is not None:
                    table_doc["source_rowid_column"] = rowid_alias
                elif rowid_kind == "shadowed_rowid_aliases":
                    # No rowid-kind field is emitted for the all-aliases-shadowed case.
                    del table_doc["rowid_kind"]
                    table_doc["rowid_aliases_shadowed"] = True
                table_docs.append(table_doc)
                totals["chunks"] += len(part_docs) + sum(
                    item["chunk_count"] for item in external_docs if item["table_id"] == table_id
                )
                totals["pieces"] += len(part_docs)

            # A generated SQLite file must occur exactly once in the manifest.
            listed = [item["file"] for table in table_docs for item in table["parts"]]
            listed.extend(item["file"] for item in external_docs)
            if len(listed) != len(set(listed)):
                raise ValueError("generated SQLite shard paths overlap")

            manifest: dict[str, Any] = {
                "version": MANIFEST_VERSION,
                "role": MANIFEST_ROLE,
                "snapshot_identifier": snapshot_id,
                "source_schema_sha256": schema_sha,
                "schema_objects": objects,
                "tables": table_docs,
                "external_values": external_docs,
                "max_bytes": max_bytes,
                "coverage": {
                    "all_tables": True,
                    "all_rows": True,
                    "all_columns": True,
                    "all_values": True,
                    "external_values": True,
                },
                "counts": {
                    "exported_tables": len(table_docs),
                    "exported_rows": totals["rows"],
                    "exported_cells": totals["cells"],
                    "external_cells": totals["external_cells"],
                    "value_chunks": sum(item["chunk_count"] for item in external_docs),
                    "parts": len(listed) - len(external_docs),
                    "files": len(listed),
                },
            }
            manifest_path = root / "manifest.json"
            descriptor, temporary_name = tempfile.mkstemp(
                prefix=".manifest.json.", suffix=".tmp", dir=root
            )
            try:
                with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
                    json.dump(manifest, stream, ensure_ascii=False, sort_keys=True, indent=2)
                    stream.write("\n")
                    stream.flush()
                    os.fsync(stream.fileno())
                os.chmod(temporary_name, 0o600)
                # Link creation is exclusive: never replace a file that was
                # not created by this export, even if another process races us.
                os.link(temporary_name, manifest_path)
            finally:
                try:
                    os.unlink(temporary_name)
                except FileNotFoundError:
                    pass
            return manifest
    except BaseException as exc:
        _exception_receipt(root, "verification.failed.json", exc, snapshot_id=snapshot_id)
        raise


def _validate_relpath(value: Any) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError("manifest file path must be a non-empty string")
    path = PurePosixPath(value)
    if path.is_absolute() or ".." in path.parts or "" in path.parts:
        raise ValueError(f"unsafe manifest file path: {value!r}")
    return value


def _safe_file(root: Path, relative: str) -> Path:
    relative = _validate_relpath(relative)
    path = root.joinpath(*PurePosixPath(relative).parts)
    _reject_symlink_components(path, label="shard", within=root)
    if not path.is_file():
        raise ValueError(f"manifest file is missing: {relative}")
    return path


def _open_readonly(path: Path) -> sqlite3.Connection:
    uri = path.resolve(strict=True).as_uri() + "?mode=ro&immutable=1"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = None
    return conn


def _manifest_tables(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    if not isinstance(manifest, dict):
        raise ValueError("manifest must be an object")
    if manifest.get("version") != MANIFEST_VERSION or manifest.get("role") != MANIFEST_ROLE:
        raise ValueError("unsupported lossless SQLite shard manifest")
    tables = manifest.get("tables")
    if not isinstance(tables, list):
        raise ValueError("manifest tables must be a list")
    names = [item.get("name") for item in tables if isinstance(item, dict)]
    if (
        len(names) != len(tables)
        or not all(isinstance(name, str) for name in names)
        or len(names) != len(set(names))
    ):
        raise ValueError("manifest table names are missing or duplicated")
    return tables


def _check_manifest_disk(root: Path, manifest: dict[str, Any]) -> None:
    path = root / "manifest.json"
    if not path.is_file() or path.is_symlink():
        raise ValueError("manifest.json is missing or unsafe")
    try:
        on_disk = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError("manifest.json cannot be read") from exc
    if on_disk != manifest:
        raise ValueError("provided manifest differs from manifest.json")


def _selector_rule_token(raw: Any) -> str:
    if raw is None:
        return "rule_unknown"
    if not isinstance(raw, str):
        raise ValueError("selector rule_raw must be text or null")
    if _SELECTOR_RULE.fullmatch(raw) and raw != "rule_unknown" and not raw.startswith("rule_"):
        return raw
    return "rule_" + _sha256_bytes(raw.encode("utf-8"))[:48]


def _declared_selector_files(
    manifest: dict[str, Any], root: Path, *, verify_hashes: bool = True
) -> dict[str, dict[str, Any]]:
    has_modes = "by_mode" in manifest
    has_rules = "by_rule" in manifest
    if not has_modes and not has_rules:
        return {}
    if not has_modes or not has_rules:
        raise ValueError("selector manifest must declare both by_mode and by_rule")

    by_mode = manifest.get("by_mode")
    by_rule = manifest.get("by_rule")
    if not isinstance(by_mode, list) or not isinstance(by_rule, list):
        raise ValueError("selector manifest entries must be lists")

    declared: dict[str, dict[str, Any]] = {}
    for axis, rows in (("by-mode", by_mode), ("by-rule", by_rule)):
        for row in rows:
            if not isinstance(row, dict):
                raise ValueError("selector manifest entry must be an object")
            mode = row.get("analysis_set")
            if not isinstance(mode, str) or not _SELECTOR_MODE.fullmatch(mode):
                raise ValueError("selector analysis_set is invalid")
            if axis == "by-mode":
                expected = f"by-mode/{mode}.sqlite3"
            else:
                if "rule_raw" not in row:
                    raise ValueError("rule selector is missing rule_raw")
                rule_token = _selector_rule_token(row["rule_raw"])
                expected = f"by-rule/{mode}__{rule_token}.sqlite3"

            relative = _validate_relpath(row.get("file"))
            if relative != expected:
                raise ValueError("selector path is not canonical")
            if relative in declared:
                raise ValueError(f"duplicate selector path: {relative}")
            size = row.get("bytes")
            digest = row.get("sha256")
            if type(size) is not int or size <= 0 or size > MAX_SELECTOR_BYTES:
                raise ValueError(f"selector exceeds {MAX_SELECTOR_BYTES} bytes or has invalid size")
            if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest, re.ASCII):
                raise ValueError("selector SHA-256 metadata is invalid")
            _check_file_record(
                root, relative, row, MAX_SELECTOR_BYTES, verify_hash=verify_hashes
            )
            declared[relative] = row
    return declared


def _declared_files(
    manifest: dict[str, Any], root: Path, *, verify_hashes: bool = True
) -> tuple[
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
]:
    tables = _manifest_tables(manifest)
    part_files: dict[str, dict[str, Any]] = {}
    table_ids: dict[str, str] = {}
    schema_objects = manifest.get("schema_objects")
    required_schema_keys = {"name", "type", "tbl_name", "sql"}
    if not isinstance(schema_objects, list) or any(
        not isinstance(item, dict)
        or set(item) != required_schema_keys
        or not isinstance(item.get("name"), str)
        or not isinstance(item.get("type"), str)
        or not isinstance(item.get("tbl_name"), str)
        or (item.get("sql") is not None and not isinstance(item.get("sql"), str))
        for item in schema_objects
    ):
        raise ValueError("manifest schema_objects must be a list")
    if schema_objects != sorted(schema_objects, key=lambda item: (item["type"], item["name"])):
        raise ValueError("manifest schema_objects are not ordered by type,name")
    schema_tables = [item.get("name") for item in schema_objects if item.get("type") == "table"]
    if schema_tables != [table["name"] for table in tables]:
        raise ValueError("manifest tables do not cover schema_objects table entries")
    for table_index, table in enumerate(tables, 1):
        table_id = table.get("table_id")
        name = table["name"]
        if not isinstance(table_id, str) or not re.fullmatch(r"t\d{4,}_[0-9a-f]{12}", table_id):
            raise ValueError(f"invalid table_id for {name!r}")
        if table_id != _table_id(table_index, name) or table_id in table_ids.values():
            raise ValueError(f"table_id is duplicate or inconsistent for {name!r}")
        table_ids[name] = table_id
        parts = table.get("parts")
        if not isinstance(parts, list) or not parts:
            raise ValueError(f"table has no parts: {name}")
        previous_end = 0
        row_count = table.get("row_count")
        if not isinstance(row_count, int) or row_count < 0:
            raise ValueError(f"invalid row_count for table {name!r}")
        for index, part in enumerate(parts, 1):
            if not isinstance(part, dict):
                raise ValueError(f"invalid part metadata for table {name!r}")
            relative = _validate_relpath(part.get("file"))
            start, end = part.get("row_start"), part.get("row_end")
            if not isinstance(start, int) or not isinstance(end, int) or start != previous_end or end < start:
                raise ValueError(f"part row ranges are not contiguous for table {name!r}")
            if part.get("table_id") != table_id or end - start != part.get("row_count"):
                raise ValueError(f"part metadata mismatch for table {name!r}")
            if index == 1 and start != 0:
                raise ValueError(f"first part does not start at row zero for {name!r}")
            if relative in part_files:
                raise ValueError(f"duplicate shard path: {relative}")
            part_files[relative] = part
            previous_end = end
        if previous_end != row_count:
            raise ValueError(f"parts do not cover every row of table {name!r}")
        if row_count == 0 and (len(parts) != 1 or parts[0]["row_start"] != 0 or parts[0]["row_end"] != 0):
            raise ValueError(f"empty table must have exactly one empty part: {name!r}")

    external_files: dict[str, dict[str, Any]] = {}
    external = manifest.get("external_values")
    if not isinstance(external, list):
        raise ValueError("manifest external_values must be a list")
    for item in external:
        if not isinstance(item, dict):
            raise ValueError("invalid external value file metadata")
        relative = _validate_relpath(item.get("file"))
        if relative in part_files or relative in external_files:
            raise ValueError(f"duplicate shard path: {relative}")
        if item.get("table_id") not in table_ids.values():
            raise ValueError(f"external value file has unknown table_id: {relative}")
        if not isinstance(item.get("chunk_count"), int) or item["chunk_count"] <= 0:
            raise ValueError(f"external value file has invalid chunk_count: {relative}")
        if not isinstance(item.get("cell_count"), int) or item["cell_count"] <= 0:
            raise ValueError(f"external value file has invalid cell_count: {relative}")
        external_files[relative] = item

    all_paths = sorted(set(part_files) | set(external_files))
    for previous, current in zip(all_paths, all_paths[1:]):
        previous_parts = PurePosixPath(previous).parts
        current_parts = PurePosixPath(current).parts
        shorter, longer = (
            (previous_parts, current_parts)
            if len(previous_parts) <= len(current_parts)
            else (current_parts, previous_parts)
        )
        if longer[: len(shorter)] == shorter:
            raise ValueError("SQLite shard file paths overlap")

    for table in tables:
        table_id = table["table_id"]
        for part_index, part in enumerate(table["parts"], 1):
            expected = f"shared/{table_id}/part{part_index:06d}.sqlite3"
            if part["file"] != expected:
                raise ValueError(f"unexpected table shard path for {table['name']!r}")
        value_index = 0
        for path, record in external_files.items():
            if record["table_id"] != table_id:
                continue
            value_index += 1
            expected = f"shared/{table_id}/value/part{value_index:06d}.sqlite3"
            if path != expected:
                raise ValueError(f"unexpected external value path for {table['name']!r}")

    all_declared = set(part_files) | set(external_files)
    if len(all_declared) != len(part_files) + len(external_files):
        raise ValueError("shard file paths overlap")
    selector_files = _declared_selector_files(
        manifest, root, verify_hashes=verify_hashes
    )
    if set(selector_files) & all_declared:
        raise ValueError("selector paths overlap data shard paths")
    return part_files, external_files, selector_files


def _check_file_record(
    root: Path,
    relative: str,
    record: dict[str, Any],
    max_bytes: int,
    *,
    verify_hash: bool = True,
) -> Path:
    path = _safe_file(root, relative)
    size = path.stat().st_size
    expected_size = record.get("bytes")
    expected_sha = record.get("sha256")
    if type(expected_size) is not int or expected_size < 0 or size != expected_size:
        raise ValueError(f"shard byte count mismatch: {relative}")
    if size > max_bytes:
        raise ValueError(f"shard exceeds max_bytes: {relative}")
    if not isinstance(expected_sha, str) or not re.fullmatch(r"[0-9a-f]{64}", expected_sha):
        raise ValueError(f"shard SHA-256 metadata is invalid: {relative}")
    if verify_hash and _sha256_file(path) != expected_sha:
        raise ValueError(f"shard SHA-256 mismatch: {relative}")
    return path


def _check_unlisted_sqlite_files(root: Path, declared: set[str]) -> None:
    found: set[str] = set()
    for current, directories, files in os.walk(root, followlinks=False):
        current_path = Path(current)
        for directory in list(directories):
            candidate = current_path / directory
            if candidate.is_symlink():
                raise ValueError(f"shard tree contains a symbolic link: {candidate.relative_to(root)}")
        for filename in files:
            path = current_path / filename
            if path.is_symlink():
                raise ValueError(f"shard tree contains a symbolic link: {path.relative_to(root)}")
            if filename.endswith(("-wal", "-journal", "-shm")):
                raise ValueError(f"SQLite shard has a sidecar file: {path.relative_to(root)}")
            if path.suffix == ".sqlite3":
                _reject_symlink_components(path, label="shard")
                if path.is_file():
                    found.add(path.relative_to(root).as_posix())
    if found != declared:
        missing = sorted(declared - found)
        extra = sorted(found - declared)
        raise ValueError(
            f"SQLite shard file inventory mismatch (missing={len(missing)}, extra={len(extra)})"
        )


def _value_connection(
    relative: str,
    external_docs: dict[str, dict[str, Any]],
    root: Path,
    max_bytes: int,
    connections: OrderedDict[str, sqlite3.Connection],
    checked: set[str],
) -> sqlite3.Connection:
    record = external_docs.get(relative)
    if record is None:
        raise ValueError(f"external cell references an undeclared value file: {relative}")
    if relative not in checked:
        path = _check_file_record(root, relative, record, max_bytes)
        checked.add(relative)
    else:
        path = _safe_file(root, relative)
    conn = connections.pop(relative, None)
    if conn is None:
        conn = _open_readonly(path)
        size, page_count, page_size = _db_size(conn)
        if (
            size > max_bytes
            or page_count * page_size > max_bytes
            or page_count != record.get("page_count")
            or page_size != record.get("page_size")
        ):
            conn.close()
            raise ValueError(f"external value SQLite page metadata mismatch: {relative}")
        integrity = conn.execute("PRAGMA quick_check").fetchone()
        if not integrity or integrity[0] != "ok":
            conn.close()
            raise ValueError(f"external value SQLite integrity check failed: {relative}")
        actual_tables = {
            row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        if actual_tables != {"value_chunks"}:
            conn.close()
            raise ValueError(f"unexpected tables in external value shard: {relative}")
        info = conn.execute("PRAGMA table_xinfo(value_chunks)").fetchall()
        if [(row[1], row[2]) for row in info] != [
            ("row_ordinal", "INTEGER"),
            ("column_ordinal", "INTEGER"),
            ("chunk_index", "INTEGER"),
            ("chunk_total", "INTEGER"),
            ("sqlite_type", "TEXT"),
            ("payload", "BLOB"),
        ]:
            conn.close()
            raise ValueError(f"external value shard columns mismatch: {relative}")
    connections[relative] = conn
    while len(connections) > 8:
        _, old = connections.popitem(last=False)
        old.close()
    return conn


def _read_external_cell(
    root: Path,
    external_docs: dict[str, dict[str, Any]],
    table_id: str,
    row_ordinal: int,
    column_ordinal: int,
    sqlite_type: str,
    byte_length: int,
    expected_sha: str,
    chunk_total: int,
    file_refs: list[str],
    max_bytes: int,
    connections: OrderedDict[str, sqlite3.Connection],
    checked_files: set[str],
    file_usage: dict[str, list[int]],
) -> Any:
    if not file_refs or len(file_refs) != len(set(file_refs)):
        raise ValueError(f"external cell file references are empty or duplicated at row {row_ordinal}")
    payload = bytearray()
    expected_index = 0
    for relative in file_refs:
        record = external_docs.get(relative)
        if record is None or record.get("table_id") != table_id:
            raise ValueError(f"external cell references the wrong value file at row {row_ordinal}")
        conn = _value_connection(
            relative, external_docs, root, max_bytes, connections, checked_files
        )
        cursor = conn.execute(
            "SELECT chunk_index,chunk_total,sqlite_type,payload FROM value_chunks "
            "WHERE row_ordinal=? AND column_ordinal=? ORDER BY chunk_index",
            (row_ordinal, column_ordinal),
        )
        file_chunk_count = 0
        for row in cursor:
            chunk_index = int(row[0])
            if chunk_index != expected_index:
                raise ValueError(
                    f"external value chunks are missing, duplicated, or unordered at "
                    f"row {row_ordinal}, column {column_ordinal}"
                )
            if int(row[1]) != chunk_total or row[2] != sqlite_type or not isinstance(row[3], bytes):
                raise ValueError(
                    f"external value chunk metadata mismatch at row {row_ordinal}, column {column_ordinal}"
                )
            chunk_bytes = len(row[3])
            if (
                chunk_bytes <= 0
                or chunk_bytes > VALUE_CHUNK_BYTES
                or (chunk_index < chunk_total - 1 and chunk_bytes != VALUE_CHUNK_BYTES)
            ):
                raise ValueError(
                    f"external value chunk size mismatch at row {row_ordinal}, "
                    f"column {column_ordinal}, chunk {chunk_index}"
                )
            payload.extend(row[3])
            expected_index += 1
            file_chunk_count += 1
        if file_chunk_count == 0:
            raise ValueError(
                f"external cell references a value file without its chunks at row {row_ordinal}"
            )
        usage = file_usage.setdefault(relative, [0, 0])
        usage[0] += file_chunk_count
        usage[1] += 1
    if expected_index != chunk_total:
        raise ValueError(
            f"external value chunk count mismatch at row {row_ordinal}, column {column_ordinal}"
        )
    payload = bytes(payload)
    if len(payload) != byte_length or _sha256_bytes(payload) != expected_sha:
        raise ValueError(
            f"external value length or SHA-256 mismatch at row {row_ordinal}, column {column_ordinal}"
        )
    if sqlite_type == "text":
        try:
            return payload.decode("utf-8", "surrogatepass")
        except UnicodeDecodeError as exc:
            raise ValueError("external TEXT is not valid UTF-8 with surrogatepass") from exc
    if sqlite_type == "blob":
        return payload
    raise ValueError(f"unsupported external SQLite type: {sqlite_type}")


def _iter_table_entries(
    root: Path,
    manifest: dict[str, Any],
    tablename: str,
    *,
    stats: Optional[dict[str, int]] = None,
    candidate_criteria: Optional[dict[int, Any]] = None,
    verified_files: Any = None,
    verified_records: Optional[dict[str, dict[str, Any]]] = None,
) -> Generator[tuple[int, Any, tuple[Any, ...]], None, None]:
    candidate_mode = candidate_criteria is not None
    tables = _manifest_tables(manifest)
    table = next((item for item in tables if item["name"] == tablename), None)
    if table is None:
        raise KeyError(f"table is not in manifest: {tablename}")
    max_bytes = manifest.get("max_bytes")
    if not isinstance(max_bytes, int) or max_bytes < MIN_MAX_BYTES or max_bytes > MAX_ALLOWED_BYTES:
        raise ValueError("manifest max_bytes is invalid")
    if candidate_mode:
        from .verified_files import VerifiedFiles

        if type(verified_files) is not VerifiedFiles or verified_records is None:
            raise ValueError("candidate lookup requires a complete VerifiedFiles package token")
        VerifiedFiles._authenticate(verified_files)
        verified_files.assert_matches(root, verified_records)
    else:
        _check_manifest_disk(root, manifest)
    part_files, external_docs, selector_files = _declared_files(
        manifest, root, verify_hashes=not candidate_mode
    )
    if candidate_mode:
        required_record_paths = (
            set(part_files) | set(external_docs) | set(selector_files)
            | {"manifest.json", "verification.json", "selectors-verification.json"}
        )
        if set(verified_records) != required_record_paths:
            raise ValueError("candidate lookup token does not cover the complete package")
        for relative, record in {
            **part_files, **external_docs, **selector_files,
        }.items():
            token_record = verified_records.get(relative)
            if (not isinstance(token_record, dict)
                    or token_record.get("bytes") != record.get("bytes")
                    or token_record.get("sha256") != record.get("sha256")):
                raise ValueError("candidate lookup token does not match manifest file records")
        _check_manifest_disk(root, manifest)
    declared_files = set(part_files) | set(external_docs) | set(selector_files)
    _check_unlisted_sqlite_files(root, declared_files)
    for relative in declared_files:
        _safe_file(root, relative)

    table_id = table["table_id"]
    columns = table.get("columns")
    xinfo = table.get("column_schema")
    if not isinstance(columns, list) or not all(isinstance(item, str) for item in columns):
        raise ValueError(f"invalid column list for {tablename!r}")
    if not isinstance(xinfo, list):
        raise ValueError(f"invalid table_xinfo metadata for {tablename!r}")
    expected_columns = [item["name"] for item in xinfo if item["hidden"] != 1]
    if columns != expected_columns:
        raise ValueError(f"columns do not match table_xinfo for {tablename!r}")
    if _schema_sha256(manifest.get("schema_objects", [])) != manifest.get("source_schema_sha256"):
        raise ValueError("manifest source schema SHA-256 mismatch")

    ext_meta = table.get("archive_metadata_tables")
    if not isinstance(ext_meta, dict) or not isinstance(ext_meta.get("rows"), str) or not isinstance(ext_meta.get("external_cells"), str):
        raise ValueError(f"archive metadata table names missing for {tablename!r}")

    digest = hashlib.sha256()
    expected_ordinal = 0
    connections: OrderedDict[str, sqlite3.Connection] = OrderedDict()
    checked_files: set[str] = set(external_docs) if candidate_mode else set()
    file_usage: dict[str, list[int]] = {}
    referenced_external_files: set[str] = set()
    total_external_cells = 0
    table_external_order = [
        name for name, info in external_docs.items() if info.get("table_id") == table_id
    ]
    external_rank = {name: index for index, name in enumerate(table_external_order)}
    try:
        for part in table["parts"]:
            relative = part["file"]
            path = _check_file_record(
                root, relative, part, max_bytes, verify_hash=not candidate_mode
            )
            with closing(_open_readonly(path)) as conn:
                size, page_count, page_size = _db_size(conn)
                if size > max_bytes or page_count * page_size > max_bytes:
                    raise ValueError(f"part page budget exceeded: {relative}")
                if page_count != part.get("page_count") or page_size != part.get("page_size"):
                    raise ValueError(f"part SQLite page metadata mismatch: {relative}")
                integrity = conn.execute("PRAGMA quick_check").fetchone()
                if not integrity or integrity[0] != "ok":
                    raise ValueError(f"part SQLite integrity check failed: {relative}")
                if conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                    (tablename,),
                ).fetchone() is None:
                    raise ValueError(f"source data table missing from part: {relative}")
                shard_info = conn.execute(f"PRAGMA table_xinfo({_quote(tablename)})").fetchall()
                shard_columns = [row[1] for row in shard_info]
                if shard_columns != columns or any(row[2] not in (None, "") for row in shard_info):
                    raise ValueError(f"shard data table columns or affinity mismatch: {relative}")
                shard_rowid_alias: Optional[str] = None
                candidate_indexes: list[int] = []
                candidate_projection_start = 0
                candidate_null_flags_start = 0
                if candidate_mode:
                    # The shard table itself is an ordinary rowid table. Use
                    # its hidden rowid to fetch complete rows only after the
                    # narrow candidate probe matches. Source rowids remain in
                    # _archive_rows and are never inferred from this shard id.
                    shadowed = {column.casefold() for column in columns}
                    shard_rowid_alias = next(
                        (alias for alias in ("rowid", "_rowid_", "oid")
                         if alias.casefold() not in shadowed),
                        None,
                    )
                    candidate_indexes = sorted(candidate_criteria)
                    probe_fields: list[str] = []
                    if shard_rowid_alias is not None:
                        probe_fields.append(
                            f"{_quote(shard_rowid_alias)} AS {_quote('__archive_probe_rowid')}"
                        )
                    probe_fields.extend(_quote(columns[index]) for index in candidate_indexes)
                    candidate_projection_start = 1 if shard_rowid_alias is not None else 0
                    candidate_null_flags_start = candidate_projection_start + len(candidate_indexes)
                    probe_fields.extend(
                        f"({_quote(column)} IS NULL) AS {_quote(f'__archive_probe_null_{index:06d}')}"
                        for index, column in enumerate(columns)
                    )
                    query = f"SELECT {', '.join(probe_fields)} FROM {_quote(tablename)} NOT INDEXED"
                    if shard_rowid_alias is not None:
                        query += f" ORDER BY {_quote(shard_rowid_alias)}"
                    data_cursor = conn.execute(query)
                    if len(data_cursor.description or ()) != candidate_null_flags_start + len(columns):
                        raise ValueError(f"shard candidate probe columns mismatch: {relative}")
                else:
                    data_cursor = conn.execute(f"SELECT * FROM {_quote(tablename)} NOT INDEXED")
                    if [column[0] for column in data_cursor.description or ()] != columns:
                        raise ValueError(f"shard SELECT * columns mismatch: {relative}")
                row_table = ext_meta["rows"]
                ext_table = ext_meta["external_cells"]
                row_schema = conn.execute(f"PRAGMA table_xinfo({_quote(row_table)})").fetchall()
                ext_schema = conn.execute(f"PRAGMA table_xinfo({_quote(ext_table)})").fetchall()
                if [(row[1], row[2]) for row in row_schema] != [
                    ("row_ordinal", "INTEGER"),
                    ("source_rowid", ""),
                ] or row_schema[0][5] != 1:
                    raise ValueError(f"row ordinal metadata schema mismatch: {relative}")
                if [(row[1], row[2]) for row in ext_schema] != [
                    ("table_id", "TEXT"),
                    ("row_ordinal", "INTEGER"),
                    ("column_ordinal", "INTEGER"),
                    ("sqlite_type", "TEXT"),
                    ("byte_length", "INTEGER"),
                    ("sha256", "TEXT"),
                    ("chunk_total", "INTEGER"),
                    ("value_files_json", "TEXT"),
                ]:
                    raise ValueError(f"external cell metadata schema mismatch: {relative}")
                row_meta_cursor = conn.execute(
                    f"SELECT row_ordinal,source_rowid FROM {_quote(row_table)} ORDER BY row_ordinal"
                )
                actual_tables = {
                    row[0]
                    for row in conn.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    )
                }
                expected_tables = {tablename, row_table, ext_table}
                generated_empty_stats = actual_tables - expected_tables
                if any(name not in {"sqlite_stat1", "sqlite_stat4"} for name in generated_empty_stats):
                    raise ValueError(f"unexpected tables inside shard: {relative}")
                for stat_table in generated_empty_stats:
                    if int(conn.execute(f"SELECT COUNT(*) FROM {_quote(stat_table)}").fetchone()[0]) != 0:
                        raise ValueError(f"unexpected rows in SQLite stats helper table: {relative}")
                actual_start = expected_ordinal
                part_external_start = total_external_cells
                for row_meta in row_meta_cursor:
                    if type(row_meta[0]) is not int:
                        raise ValueError(f"row ordinal metadata is not an integer: {relative}")
                    ordinal = row_meta[0]
                    if ordinal != expected_ordinal:
                        raise ValueError(f"row ordinals are duplicated, missing, or unordered: {relative}")
                    source_rowid = row_meta[1]
                    if table.get("rowid_kind") == "rowid":
                        if type(source_rowid) is not int:
                            raise ValueError(f"source rowid metadata is not an integer at row {ordinal}")
                    elif source_rowid is not None:
                        raise ValueError(f"unavailable source rowid metadata is not NULL at row {ordinal}")
                    stored = data_cursor.fetchone()
                    if stored is None:
                        raise ValueError(f"row metadata and source data row count mismatch: {relative}")
                    if candidate_mode:
                        if len(stored) != candidate_null_flags_start + len(columns):
                            raise ValueError(f"shard candidate probe row width mismatch: {relative}")
                        shard_rowid = stored[0] if shard_rowid_alias is not None else None
                        if shard_rowid_alias is not None and type(shard_rowid) is not int:
                            raise ValueError(f"shard hidden rowid is invalid: {relative}")
                        criteria_values = {
                            index: stored[candidate_projection_start + offset]
                            for offset, index in enumerate(candidate_indexes)
                        }
                        null_flags = stored[candidate_null_flags_start:]
                        if any(type(flag) is not int or flag not in (0, 1) for flag in null_flags):
                            raise ValueError(f"shard candidate NULL probe is invalid: {relative}")
                        values: list[Any] = []
                    else:
                        if len(stored) != len(columns):
                            raise ValueError(f"row metadata and source data row count mismatch: {relative}")
                        values = list(stored)
                    ext_rows = conn.execute(
                        f"SELECT table_id,column_ordinal,sqlite_type,byte_length,sha256,chunk_total,value_files_json "
                        f"FROM {_quote(ext_table)} WHERE row_ordinal=? ORDER BY column_ordinal",
                        (ordinal,),
                    )
                    previous_column = -1
                    row_external_count = 0
                    external_metadata: list[tuple[int, str, int, str, int, list[str]]] = []
                    for ext_row in ext_rows:
                        if ext_row[0] != table_id:
                            raise ValueError(f"external cell table_id mismatch at row {ordinal}")
                        if candidate_mode and type(ext_row[1]) is not int:
                            raise ValueError(f"external cell column ordinal is invalid at row {ordinal}")
                        column_index = int(ext_row[1])
                        column_count = len(columns) if candidate_mode else len(values)
                        if column_index <= previous_column or column_index < 0 or column_index >= column_count:
                            raise ValueError(f"external cell column order or range is invalid at row {ordinal}")
                        previous_column = column_index
                        if candidate_mode:
                            if null_flags[column_index] != 1:
                                raise ValueError(
                                    f"externalized cell placeholder is not NULL at row {ordinal}"
                                )
                        elif values[column_index] is not None:
                            raise ValueError(f"externalized cell placeholder is not NULL at row {ordinal}")
                        if candidate_mode:
                            sqlite_type, byte_length, sha256, chunk_total = ext_row[2:6]
                            if not isinstance(sqlite_type, str) or sqlite_type not in ("text", "blob"):
                                raise ValueError(f"external cell SQLite type is invalid at row {ordinal}")
                            if type(byte_length) is not int or byte_length <= 0:
                                raise ValueError(f"external cell byte length is invalid at row {ordinal}")
                            if not isinstance(sha256, str) or not re.fullmatch(
                                r"[0-9a-f]{64}", sha256, re.ASCII
                            ):
                                raise ValueError(f"external cell SHA-256 is invalid at row {ordinal}")
                            if (type(chunk_total) is not int or chunk_total <= 0
                                    or chunk_total != math.ceil(byte_length / VALUE_CHUNK_BYTES)):
                                raise ValueError(f"external cell chunk count is invalid at row {ordinal}")
                        else:
                            sqlite_type = ext_row[2]
                            byte_length = int(ext_row[3])
                            sha256 = ext_row[4]
                            chunk_total = int(ext_row[5])
                        try:
                            refs = json.loads(ext_row[6])
                        except (TypeError, json.JSONDecodeError) as exc:
                            raise ValueError(f"external value file references are invalid at row {ordinal}") from exc
                        if not isinstance(refs, list) or not all(isinstance(ref, str) for ref in refs):
                            raise ValueError(f"external value file references are invalid at row {ordinal}")
                        if candidate_mode:
                            if (not refs or len(refs) != len(set(refs))
                                    or any(ref not in external_docs for ref in refs)
                                    or any(external_docs[ref].get("table_id") != table_id for ref in refs)):
                                raise ValueError(f"external value file references are invalid at row {ordinal}")
                            ranks = [external_rank[ref] for ref in refs]
                            if (ranks != sorted(ranks)
                                    or any(right != left + 1 for left, right in zip(ranks, ranks[1:]))):
                                raise ValueError(f"external value file references are unordered at row {ordinal}")
                            referenced_external_files.update(refs)
                            external_metadata.append(
                                (column_index, sqlite_type, byte_length, sha256, chunk_total, refs)
                            )
                        else:
                            values[column_index] = _read_external_cell(
                                root,
                                external_docs,
                                table_id,
                                ordinal,
                                column_index,
                                sqlite_type,
                                byte_length,
                                sha256,
                                chunk_total,
                                refs,
                                max_bytes,
                                connections,
                                checked_files,
                                file_usage,
                            )
                        total_external_cells += 1
                        row_external_count += 1
                    if candidate_mode:
                        # Test inline cells first. Rows rejected here never
                        # transfer unrelated inline TEXT/BLOB values into
                        # Python or decode any external value payload.
                        metadata_by_column = {item[0]: item for item in external_metadata}
                        match = all(
                            index in metadata_by_column
                            or _same_sqlite_value(criteria_values[index], expected)
                            for index, expected in candidate_criteria.items()
                        )
                        decoded_external: dict[int, Any] = {}
                        if match:
                            for index, expected in candidate_criteria.items():
                                item = metadata_by_column.get(index)
                                if item is None:
                                    continue
                                _, sqlite_type, byte_length, sha256, chunk_total, refs = item
                                value = _read_external_cell(
                                    root, external_docs, table_id, ordinal, index,
                                    sqlite_type, byte_length, sha256, chunk_total, refs,
                                    max_bytes, connections, checked_files, file_usage,
                                )
                                decoded_external[index] = value
                                if not _same_sqlite_value(value, expected):
                                    match = False
                                    break
                        if match:
                            if shard_rowid_alias is not None:
                                full_cursor = conn.execute(
                                    f"SELECT * FROM {_quote(tablename)} NOT INDEXED "
                                    f"WHERE {_quote(shard_rowid_alias)}=?",
                                    (shard_rowid,),
                                )
                            else:
                                # All three SQLite rowid aliases are shadowed
                                # by source columns. Keep the physical table
                                # scan for the probe and use a one-row offset
                                # fetch only for actual matches.
                                part_local_position = ordinal - actual_start
                                full_cursor = conn.execute(
                                    f"SELECT * FROM {_quote(tablename)} NOT INDEXED LIMIT 1 OFFSET ?",
                                    (part_local_position,),
                                )
                            if [column[0] for column in full_cursor.description or ()] != columns:
                                raise ValueError(f"shard SELECT * columns mismatch: {relative}")
                            full_row = full_cursor.fetchone()
                            if full_row is None or len(full_row) != len(columns):
                                raise ValueError(f"candidate full row lookup failed at row {ordinal}")
                            values = list(full_row)
                            if any(
                                not _same_sqlite_value(values[index], criteria_values[index])
                                for index in candidate_indexes
                            ):
                                raise ValueError(f"candidate row changed during lookup at row {ordinal}")
                            for index, sqlite_type, byte_length, sha256, chunk_total, refs in external_metadata:
                                if values[index] is not None:
                                    raise ValueError(
                                        f"externalized cell placeholder is not NULL at row {ordinal}"
                                    )
                                if index in decoded_external:
                                    values[index] = decoded_external[index]
                                else:
                                    values[index] = _read_external_cell(
                                        root, external_docs, table_id, ordinal, index,
                                        sqlite_type, byte_length, sha256, chunk_total, refs,
                                        max_bytes, connections, checked_files, file_usage,
                                    )
                            value_tuple = tuple(values)
                        else:
                            value_tuple = ()
                    else:
                        value_tuple = tuple(values)
                        _update_stream_digest(digest, ordinal, source_rowid, value_tuple)
                    if stats is not None:
                        stats["rows"] = stats.get("rows", 0) + 1
                        stats["cells"] = stats.get("cells", 0) + len(value_tuple)
                        stats["external_cells"] = stats.get("external_cells", 0) + row_external_count
                    if not candidate_mode or match:
                        yield ordinal, source_rowid, value_tuple
                    expected_ordinal += 1
                if data_cursor.fetchone() is not None:
                    raise ValueError(f"source data has rows without ordinal metadata: {relative}")
                if actual_start != part["row_start"] or expected_ordinal != part["row_end"]:
                    raise ValueError(f"part row range does not match stored ordinals: {relative}")
                row_meta_count = int(conn.execute(f"SELECT COUNT(*) FROM {_quote(row_table)}").fetchone()[0])
                ext_count = int(conn.execute(f"SELECT COUNT(*) FROM {_quote(ext_table)}").fetchone()[0])
                if row_meta_count != part["row_count"]:
                    raise ValueError(f"part row count mismatch: {relative}")
                if ext_count != total_external_cells - part_external_start:
                    raise ValueError(f"part external-cell metadata has orphan rows: {relative}")
        if expected_ordinal != table["row_count"]:
            raise ValueError(f"table row count mismatch for {tablename!r}")
        if not candidate_mode and digest.hexdigest() != table.get("row_stream_sha256"):
            raise ValueError(f"table row stream SHA-256 mismatch for {tablename!r}")
        if stats is not None:
            stats["chunks"] = stats.get("chunks", 0)
            stats["parts"] = len(table["parts"])
        # At exhaustion, every declared external file for this table must have
        # been reached through at least one explicit cell reference.
        table_external_files = {
            name for name, info in external_docs.items() if info.get("table_id") == table_id
        }
        reached_external_files = referenced_external_files if candidate_mode else checked_files
        if table_external_files != reached_external_files:
            missing = table_external_files - reached_external_files
            if missing:
                raise ValueError(f"unreferenced external value files for {tablename!r}")
        if not candidate_mode:
            for relative in table_external_files:
                record = external_docs[relative]
                conn = _value_connection(
                    relative, external_docs, root, max_bytes, connections, checked_files
                )
                chunk_count, cell_count = conn.execute(
                    "SELECT (SELECT COUNT(*) FROM value_chunks), (SELECT COUNT(*) FROM ("
                    "SELECT row_ordinal,column_ordinal FROM value_chunks GROUP BY row_ordinal,column_ordinal"
                    "))"
                ).fetchone()
                if int(chunk_count) != record.get("chunk_count") or int(cell_count) != record.get("cell_count"):
                    raise ValueError(f"external value file chunk inventory mismatch: {relative}")
                expected_chunk_count, expected_cell_count = file_usage.get(relative, [0, 0])
                if (
                    int(chunk_count) != expected_chunk_count
                    or int(cell_count) != expected_cell_count
                ):
                    raise ValueError(f"external value file contains unreferenced chunks: {relative}")
    finally:
        try:
            for connection in connections.values():
                connection.close()
        finally:
            if candidate_mode:
                verified_files.assert_matches(root, verified_records)


def iter_table_rows(
    root: str | Path, manifest: dict[str, Any], tablename: str
) -> Generator[tuple[int, tuple[Any, ...]], None, None]:
    """Yield ``(zero_based_row_ordinal, typed_values)`` for one complete table."""
    root_path = _reject_symlink_components(Path(root), label="shard root")
    if not root_path.is_dir():
        raise ValueError("shard root is not a directory")
    with closing(_iter_table_entries(root_path, manifest, tablename)) as rows:
        for ordinal, _, values in rows:
            yield ordinal, values


def iter_table_rows_with_identity(
    root: str | Path, manifest: dict[str, Any], tablename: str
) -> Generator[tuple[int, Any, tuple[Any, ...]], None, None]:
    """Yield ordinal, original hidden rowid (or None), and all typed values.

    The ordinal locates a row inside this immutable generation. It is never a
    replacement for the original SQLite identity when applying later changes.
    Exhausting this iterator performs the same full stream/inventory checks as
    ``iter_table_rows``.
    """
    root_path = _reject_symlink_components(Path(root), label="shard root")
    if not root_path.is_dir():
        raise ValueError("shard root is not a directory")
    yield from _iter_table_entries(root_path, manifest, tablename)


def _iter_table_candidates(
    root: str | Path,
    manifest: dict[str, Any],
    tablename: str,
    criteria_by_column: dict[int, Any],
    *,
    verified_files: Any,
    verified_records: dict[str, dict[str, Any]],
) -> Generator[tuple[int, Any, tuple[Any, ...]], None, None]:
    """Yield matching rows from an already full-hash-verified package.

    This path rechecks row, part, and external-cell metadata but deliberately
    does not recompute the complete row-stream digest or recount every
    value-chunk file. The caller's process-local token binds the complete
    declared package to its prior verification. Only candidate external cells
    are reconstructed and chunk/SHA checked here.
    """
    root_path = _reject_symlink_components(Path(root), label="shard root")
    if not root_path.is_dir():
        raise ValueError("shard root is not a directory")
    tables = _manifest_tables(manifest)
    table = next((item for item in tables if item["name"] == tablename), None)
    if table is None:
        raise KeyError(f"table is not in manifest: {tablename}")
    columns = table.get("columns")
    if not isinstance(columns, list) or not isinstance(criteria_by_column, dict) or any(
        type(index) is not int or index < 0 or index >= len(columns)
        for index in criteria_by_column
    ):
        raise ValueError("candidate column criteria are invalid")
    yield from _iter_table_entries(
        root_path,
        manifest,
        tablename,
        candidate_criteria=criteria_by_column,
        verified_files=verified_files,
        verified_records=verified_records,
    )


def _same_sqlite_value(source_value: Any, shard_value: Any) -> bool:
    if source_value is None or shard_value is None:
        return source_value is None and shard_value is None
    if isinstance(source_value, float) and isinstance(shard_value, float):
        return source_value.hex() == shard_value.hex()
    if type(source_value) is not type(shard_value):
        # sqlite3 returns BLOBs as bytes. A source configured with a custom
        # text_factory is outside the default SQLite scalar contract.
        return False
    if isinstance(source_value, memoryview):
        return bytes(source_value) == bytes(shard_value)
    if isinstance(source_value, bytearray):
        return bytes(source_value) == bytes(shard_value)
    return source_value == shard_value


def _owned_receipt_write(root: Path, filename: str, payload: dict[str, Any]) -> None:
    target = root / filename
    if target.exists() or target.is_symlink():
        try:
            existing = json.loads(target.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            raise FileExistsError(f"refusing to replace non-owned receipt: {filename}")
        if existing.get("generated_by") != "ikarchive.lossless_sqlite":
            raise FileExistsError(f"refusing to replace non-owned receipt: {filename}")
    descriptor, temporary = tempfile.mkstemp(prefix=f".{filename}.", suffix=".tmp", dir=root)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(payload, stream, ensure_ascii=False, sort_keys=True, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(temporary, 0o600)
        os.replace(temporary, target)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


def verify_sqlite_shards(
    sourceconn: sqlite3.Connection, root: str | Path, manifest: dict[str, Any],
    *, write_receipt: bool = True,
) -> dict[str, Any]:
    """Compare every source value; immutable readers may disable receipt writes."""
    if type(write_receipt) is not bool:
        raise TypeError("write_receipt must be a bool")
    if not isinstance(sourceconn, sqlite3.Connection):
        raise TypeError("sourceconn must be a sqlite3.Connection")
    root_path = _reject_symlink_components(Path(root), label="shard root")
    if not root_path.is_dir():
        raise ValueError("shard root is not a directory")
    _reject_source_output_overlap(sourceconn, root_path)
    snapshot_id = manifest.get("snapshot_identifier") if isinstance(manifest, dict) else None
    try:
        _require_standard_text_factory(sourceconn)
        _check_manifest_disk(root_path, manifest)
        tables = _manifest_tables(manifest)
        part_files, external_docs, selector_files = _declared_files(manifest, root_path)
        _check_unlisted_sqlite_files(
            root_path, set(part_files) | set(external_docs) | set(selector_files)
        )
        max_bytes = manifest.get("max_bytes")
        if not isinstance(max_bytes, int) or max_bytes < MIN_MAX_BYTES or max_bytes > MAX_ALLOWED_BYTES:
            raise ValueError("manifest max_bytes is invalid")

        verified_counts: dict[str, int] = {}
        total_rows = 0
        total_cells = 0
        total_external = 0
        total_chunks = 0
        with _read_transaction(sourceconn):
            _reject_nonmain_databases(sourceconn)
            source_objects = _schema_objects(sourceconn)
            source_schema_sha = _schema_sha256(source_objects)
            if source_objects != manifest.get("schema_objects"):
                raise ValueError("source schema_objects differ from manifest")
            if source_schema_sha != manifest.get("source_schema_sha256"):
                raise ValueError("source schema SHA-256 differs from manifest")
            source_tables = [item["name"] for item in source_objects if item["type"] == "table"]
            manifest_names = [table["name"] for table in tables]
            if source_tables != manifest_names:
                raise ValueError("source table coverage differs from manifest")
            flags = _table_list_flags(sourceconn)

            for table in tables:
                table_name = table["name"]
                xinfo = _table_xinfo(sourceconn, table_name)
                columns = _select_columns(xinfo)
                if xinfo != table.get("column_schema") or columns != table.get("columns"):
                    raise ValueError(f"source columns/table_xinfo differ for {table_name!r}")
                if _foreign_keys(sourceconn, table_name) != table.get("foreign_keys"):
                    raise ValueError(f"source foreign keys differ for {table_name!r}")
                row_plan, rowid_alias, order_by = _source_row_plan(
                    sourceconn, table_name, xinfo, flags
                )
                recorded_kind = table.get("rowid_kind")
                if row_plan == "shadowed_rowid_aliases":
                    if recorded_kind is not None or not table.get("rowid_aliases_shadowed"):
                        raise ValueError(f"shadowed rowid metadata mismatch for {table_name!r}")
                elif recorded_kind != row_plan:
                    raise ValueError(f"row ordering metadata mismatch for {table_name!r}")
                source_count = int(
                    sourceconn.execute(f"SELECT COUNT(*) FROM {_quote(table_name)}").fetchone()[0]
                )
                if source_count != table.get("row_count"):
                    raise ValueError(f"source row count differs for {table_name!r}")

                source_cursor = _source_rows(
                    sourceconn, table_name, columns, rowid_alias, order_by
                )
                read_stats: dict[str, int] = {}
                archived = _iter_table_entries(root_path, manifest, table_name, stats=read_stats)
                compared = 0
                for ordinal, source_row in enumerate(source_cursor):
                    source_values = tuple(source_row[: len(columns)])
                    source_rowid = source_row[-1] if rowid_alias is not None else None
                    try:
                        archive_ordinal, archive_rowid, archive_values = next(archived)
                    except StopIteration as exc:
                        raise ValueError(f"shard ended early for {table_name!r} at row {ordinal}") from exc
                    if archive_ordinal != ordinal or not _same_sqlite_value(source_rowid, archive_rowid):
                        raise ValueError(f"row ordinal/rowid mismatch for {table_name!r} at row {ordinal}")
                    if len(source_values) != len(archive_values):
                        raise ValueError(f"cell count mismatch for {table_name!r} at row {ordinal}")
                    for column_index, (source_value, archive_value) in enumerate(
                        zip(source_values, archive_values)
                    ):
                        if not _same_sqlite_value(source_value, archive_value):
                            raise ValueError(
                                f"SQLite typed value mismatch for {table_name!r} "
                                f"at row {ordinal}, column {column_index}"
                            )
                    compared += 1
                try:
                    next(archived)
                except StopIteration:
                    pass
                else:
                    raise ValueError(f"shard has extra rows for {table_name!r}")
                verified_counts[table_name] = compared
                total_rows += compared
                total_cells += compared * len(columns)
                total_external += read_stats.get("external_cells", 0)
                for external_file in external_docs.values():
                    if external_file.get("table_id") == table["table_id"]:
                        total_chunks += int(external_file.get("chunk_count", 0))

            fk_issues: dict[str, int] = {}
            try:
                for row in sourceconn.execute("PRAGMA foreign_key_check"):
                    fk_issues[row[0]] = fk_issues.get(row[0], 0) + 1
            except sqlite3.DatabaseError:
                # Unsupported virtual/schema objects do not invalidate exact
                # row-value preservation; they remain recorded in schema SQL.
                fk_issues = {}

        file_count = len(part_files) + len(external_docs)
        receipt = {
            "version": MANIFEST_VERSION,
            "role": MANIFEST_ROLE,
            "generated_by": "ikarchive.lossless_sqlite",
            "status": "verified",
            "snapshot_identifier": snapshot_id,
            "verified_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "source_schema_sha256": manifest["source_schema_sha256"],
            "table_count": len(tables),
            "row_counts": verified_counts,
            "row_count": total_rows,
            "cell_count": total_cells,
            "external_cell_count": total_external,
            "value_chunk_count": total_chunks,
            "file_count": file_count,
            "coverage": {
                "all_tables": True,
                "all_rows": True,
                "all_columns": True,
                "all_values": True,
                "external_values": True,
            },
            "known_source_foreign_key_violations": fk_issues,
        }
        if write_receipt:
            _owned_receipt_write(root_path, "verification.json", receipt)
        return receipt
    except BaseException as exc:
        if write_receipt:
            _exception_receipt(root_path, "verification.failed.json", exc, snapshot_id=snapshot_id)
        raise


__all__ = [
    "DEFAULT_MAX_BYTES",
    "MANIFEST_ROLE",
    "MANIFEST_VERSION",
    "VALUE_CHUNK_BYTES",
    "export_sqlite_shards",
    "iter_table_rows",
    "verify_sqlite_shards",
]
