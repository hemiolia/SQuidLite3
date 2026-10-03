#!/usr/bin/env python3
"""Run an offline published SQLite reader fixture with artificial data only.

The fixture uses Python 3.11's standard library and modules beneath
``--code-root``. It creates a private temporary baseline, publishes it and a
reset plus two incremental generations to an in-memory fake remote, then
checks the local published reader against pinned read-only SQLite snapshots.
It never opens a live database or starts a network client.
"""

from __future__ import annotations

import argparse
from contextlib import closing
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import stat
import subprocess
import sys
import tempfile
import time
from typing import Any
from unittest.mock import patch


REMOTE = "fixture:database"
BASELINE_ID = "20261003T080000Z-9a100001"
INITIAL_ID = "20261003T080100Z-9a100002"
RESET_ID = "20261003T080200Z-9a100003"
INCREMENTAL_ONE_ID = "20261003T080300Z-9a100004"
INCREMENTAL_TWO_ID = "20261003T080400Z-9a100005"
MODULES = (
    "archive.py",
    "scripts/data_root.py",
    "scripts/prepare_full_data_generation.py",
    "scripts/prepare_full_data_delta.py",
    "scripts/verified_backup_support.py",
    "scripts/export_full_xlsx.py",
    "scripts/published_sqlite_reader.py",
    "scripts/mirror_published_controls.py",
    "src/python/ikarchive/store.py",
    "src/python/ikarchive/locking.py",
    "src/python/ikarchive/planner.py",
    "src/python/ikarchive/classify.py",
    "src/python/ikarchive/rates.py",
    "src/python/ikarchive/catalog_binding.py",
    "src/python/ikarchive/collector.py",
    "src/python/ikarchive/storage.py",
    "src/python/ikarchive/ranking_policy.py",
    "src/python/ikarchive/gui.py",
    "src/python/ikarchive/display.py",
    "src/python/ikarchive/records.py",
    "src/python/ikarchive/slices.py",
    "src/python/ikarchive/slice_selectors.py",
    "src/python/ikarchive/change_feed.py",
    "src/python/ikarchive/writer_guards.py",
    "src/python/ikarchive/reconciliation.py",
    "src/python/ikarchive/delta_transport.py",
    "src/python/ikarchive/delta_reader.py",
    "src/python/ikarchive/shard_reader.py",
    "src/python/ikarchive/lossless_sqlite.py",
    "src/python/ikarchive/lossless_xlsx.py",
    "src/python/ikarchive/verified_files.py",
    "scripts/publish_full_data_delta.py",
    "scripts/nas_full_data_publish.py",
)


class FixtureFailure(RuntimeError):
    """A stable fixture failure code that never includes data or paths."""


def _require(condition: Any, code: str) -> None:
    if not condition:
        raise FixtureFailure(code)


def _sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _compact(value: Any) -> bytes:
    return (json.dumps(value, ensure_ascii=False, sort_keys=True,
                       separators=(",", ":"), allow_nan=False) + "\n").encode("utf-8")


def _source_code_sha(code_root: Path) -> str:
    digest = hashlib.sha256()
    for relative in MODULES:
        path = code_root / relative
        if path.is_symlink() or not path.is_file():
            raise FixtureFailure("CODE_MODULE_MISSING_OR_UNSAFE")
        raw = path.read_bytes()
        digest.update(relative.encode("ascii"))
        digest.update(b"\0")
        digest.update(len(raw).to_bytes(8, "big"))
        digest.update(raw)
    return digest.hexdigest()


def _validate_code_root(value: Path) -> Path:
    if value.is_symlink():
        raise FixtureFailure("CODE_ROOT_INVALID")
    root = value.resolve(strict=True)
    if not root.is_dir():
        raise FixtureFailure("CODE_ROOT_INVALID")
    for component in (root, *root.parents):
        if component.is_symlink():
            raise FixtureFailure("CODE_ROOT_SYMLINK")
    for relative in ("scripts", "src/python"):
        path = root / relative
        if path.is_symlink() or not path.is_dir():
            raise FixtureFailure("CODE_ROOT_LAYOUT_INVALID")
    return root


def _validate_scratch_root(value: Path, code_root: Path) -> Path:
    if value.is_symlink():
        raise FixtureFailure("TEMP_ROOT_SYMLINK")
    root = value.resolve(strict=True)
    if not root.is_dir() or root == code_root or code_root in root.parents:
        raise FixtureFailure("TEMP_ROOT_INVALID")
    for component in (root, *root.parents):
        if component.is_symlink():
            raise FixtureFailure("TEMP_ROOT_SYMLINK")
    return root


def _quote(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _write_source(path: Path) -> None:
    with closing(sqlite3.connect(path)) as connection:
        connection.execute("PRAGMA journal_mode=DELETE")
        connection.execute("PRAGMA foreign_keys=ON")
        connection.executescript(
            """
            CREATE TABLE parents (
                parent_id INTEGER PRIMARY KEY,
                label TEXT NOT NULL UNIQUE
            );
            CREATE TABLE typed_records (
                record_id INTEGER PRIMARY KEY,
                parent_id INTEGER NOT NULL REFERENCES parents(parent_id),
                integer_min INTEGER,
                integer_max INTEGER,
                real_value REAL,
                text_value TEXT,
                literal_json TEXT,
                blob_value BLOB,
                nullable_value,
                empty_text TEXT,
                empty_blob BLOB
            );
            CREATE TABLE rowid_values (
                label TEXT UNIQUE,
                payload BLOB,
                note TEXT
            );
            CREATE TABLE matches (
                account TEXT NOT NULL,
                kind TEXT NOT NULL,
                match_key TEXT NOT NULL,
                payload BLOB,
                PRIMARY KEY(account, kind, match_key)
            );
            CREATE TABLE match_classification (
                account TEXT NOT NULL,
                kind TEXT NOT NULL,
                match_key TEXT NOT NULL,
                analysis_set TEXT NOT NULL,
                rule_raw TEXT,
                raw_classification BLOB,
                PRIMARY KEY(account, kind, match_key),
                FOREIGN KEY(account, kind, match_key)
                    REFERENCES matches(account, kind, match_key)
            );
            CREATE TABLE deliberately_empty (label TEXT, payload BLOB);
            """
        )
        connection.execute("INSERT INTO parents VALUES(?,?)", (31, "parent-root"))
        connection.execute(
            "INSERT INTO typed_records VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (
                41,
                31,
                -(1 << 63),
                (1 << 63) - 1,
                1.25,
                "left\x00middle\r\n雪とastral𐀀",
                '{"literal":true,"large":9007199254740993}',
                b"\x00\xff\x80native-blob\x00",
                None,
                "",
                b"",
            ),
        )
        connection.execute(
            "INSERT INTO rowid_values(rowid,label,payload,note) VALUES(?,?,?,?)",
            (-401, "rowid-negative", b"\x00first\xff", "rowid source"),
        )
        connection.execute(
            "INSERT INTO rowid_values(rowid,label,payload,note) VALUES(?,?,?,?)",
            (9001, "rowid-to-delete", b"delete-me", "retained until final delta"),
        )
        connection.execute(
            "INSERT INTO matches VALUES(?,?,?,?)",
            ("fixture-account", "vs", "fixture-match", b"\x00match\xff"),
        )
        connection.execute(
            "INSERT INTO match_classification VALUES(?,?,?,?,?,?)",
            ("fixture-account", "vs", "fixture-match", "fixture_mode", "RULE_FIXTURE",
             b"\x00classification"),
        )
        connection.commit()
        _require(connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok",
                 "INITIAL_SQLITE_INTEGRITY_FAILED")
        _require(not connection.execute("PRAGMA foreign_key_check").fetchall(),
                 "INITIAL_FOREIGN_KEY_CHECK_FAILED")


class MemoryRclone:
    """In-memory-only client implementing the publisher's small read/write API."""

    def __init__(self):
        self.objects: dict[str, bytes] = {}
        self.directory_binding_checks = 0

    @staticmethod
    def _check_remote(remote_path: str) -> None:
        prefix = REMOTE + "/"
        if not isinstance(remote_path, str) or not remote_path.startswith(prefix):
            raise RuntimeError("FAKE_REMOTE_PATH_REJECTED")
        relative = remote_path[len(prefix):]
        if not relative or any(part in ("", ".", "..") for part in relative.split("/")):
            raise RuntimeError("FAKE_REMOTE_PATH_REJECTED")

    def stat(self, remote_path: str):
        self._check_remote(remote_path)
        raw = self.objects.get(remote_path)
        if raw is None:
            return None
        return {"IsDir": False, "Size": len(raw)}

    def readback(self, remote_path: str) -> tuple[int, str]:
        self._check_remote(remote_path)
        raw = self.objects.get(remote_path)
        if raw is None:
            raise RuntimeError("FAKE_REMOTE_OBJECT_MISSING")
        return len(raw), _sha256(raw)

    def readback_bytes(self, remote_path: str, expected_bytes: int, expected_sha: str) -> bytes:
        self._check_remote(remote_path)
        raw = self.objects.get(remote_path)
        if raw is None or (len(raw), _sha256(raw)) != (expected_bytes, expected_sha):
            raise RuntimeError("FAKE_REMOTE_READBACK_MISMATCH")
        return raw

    def copyto(self, source: str | os.PathLike[str], remote_path: str, *, immutable: bool) -> None:
        self._check_remote(remote_path)
        raw = Path(source).read_bytes()
        existing = self.objects.get(remote_path)
        if immutable and existing is not None:
            if existing != raw:
                raise RuntimeError("FAKE_REMOTE_IMMUTABLE_CONFLICT")
            return
        self.objects[remote_path] = raw

    def verify_directory_bindings(self) -> None:
        self.directory_binding_checks += 1
        for remote_path in self.objects:
            self._check_remote(remote_path)


def _readonly(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path.resolve(strict=True).as_uri() + "?mode=ro&immutable=1", uri=True)
    connection.execute("PRAGMA query_only=ON")
    return connection


def _typed(value: Any) -> tuple[str, Any]:
    if value is None:
        return "null", None
    if type(value) is int:
        return "integer", value
    if type(value) is float:
        return "real", value.hex()
    if type(value) is str:
        return "text", value
    if type(value) is bytes:
        return "blob", value.hex()
    raise FixtureFailure("UNSUPPORTED_NATIVE_SQLITE_VALUE")


def _column_metadata(connection: sqlite3.Connection, table: str) -> list[dict[str, Any]]:
    keys = ("cid", "name", "type", "notnull", "dflt_value", "pk", "hidden")
    return [dict(zip(keys, row)) for row in connection.execute(f"PRAGMA table_xinfo({_quote(table)})")]


def _foreign_key_metadata(connection: sqlite3.Connection, table: str) -> list[dict[str, Any]]:
    keys = ("id", "seq", "table", "from", "to", "on_update", "on_delete", "match")
    return [dict(zip(keys, row)) for row in connection.execute(f"PRAGMA foreign_key_list({_quote(table)})")]


def _capture_source(path: Path) -> dict[str, Any]:
    with closing(_readonly(path)) as connection:
        _require(connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok",
                 "SOURCE_INTEGRITY_CHECK_FAILED")
        _require(not connection.execute("PRAGMA foreign_key_check").fetchall(),
                 "SOURCE_FOREIGN_KEY_CHECK_FAILED")
        schema_objects = [
            {"type": row[0], "name": row[1], "tbl_name": row[2], "sql": row[3]}
            for row in connection.execute(
                "SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name"
            )
        ]
        table_rows: dict[str, dict[str, Any]] = {}
        table_flags = {row[1]: row for row in connection.execute("PRAGMA table_list")}
        table_names = [row[0] for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )]
        for table in table_names:
            xinfo = _column_metadata(connection, table)
            visible_columns = [row["name"] for row in xinfo if row["hidden"] != 1]
            column_names = {name.casefold() for name in visible_columns}
            flags = table_flags.get(table)
            _require(flags is not None, "SOURCE_TABLE_FLAGS_MISSING")
            without_rowid = bool(flags[4])
            rowid_alias = next(
                (name for name in ("_rowid_", "rowid", "oid") if name.casefold() not in column_names),
                None,
            )
            select_columns = ",".join(_quote(name) for name in visible_columns)
            order_by = ""
            if not without_rowid and rowid_alias is not None:
                rowid_expr = _quote(rowid_alias)
                query = f"SELECT {rowid_expr},{select_columns} FROM {_quote(table)} ORDER BY {rowid_expr}"
            else:
                rowid_expr = None
                pk_columns = [row["name"] for row in sorted(
                    (row for row in xinfo if row["pk"]), key=lambda row: row["pk"]
                )]
                _require(bool(pk_columns), "SOURCE_TABLE_IDENTITY_MISSING")
                order_by = " ORDER BY " + ",".join(_quote(name) for name in pk_columns)
                query = f"SELECT {select_columns} FROM {_quote(table)}{order_by}"
            rows = []
            for raw_row in connection.execute(query):
                if rowid_expr is None:
                    source_rowid, values = None, tuple(raw_row)
                else:
                    source_rowid, values = raw_row[0], tuple(raw_row[1:])
                rows.append({"rowid": source_rowid, "values": values})
            table_rows[table] = {
                "columns": visible_columns,
                "column_schema": xinfo,
                "foreign_keys": _foreign_key_metadata(connection, table),
                "row_count": len(rows),
                "rows": rows,
            }
        return {"schema_objects": schema_objects, "tables": table_rows}


def _signature_rows(rows: list[dict[str, Any]]) -> list[Any]:
    return [
        (_typed(row["rowid"]), tuple(_typed(value) for value in row["values"]))
        for row in rows
    ]


def _check_reader(reader: Any, expected: dict[str, Any]) -> tuple[int, int]:
    _require(reader.schema_objects() == expected["schema_objects"], "SCHEMA_OBJECTS_MISMATCH")
    metadata = reader.tables()
    expected_tables = expected["tables"]
    _require([row["name"] for row in metadata] == sorted(expected_tables), "TABLE_INVENTORY_MISMATCH")
    observed_rows = 0
    for table_meta in metadata:
        name = table_meta["name"]
        wanted = expected_tables[name]
        _require(table_meta["columns"] == wanted["columns"], "TABLE_COLUMNS_MISMATCH")
        _require(table_meta["column_schema"] == wanted["column_schema"], "TABLE_XINFO_MISMATCH")
        _require(table_meta["foreign_keys"] == wanted["foreign_keys"], "TABLE_FOREIGN_KEYS_MISMATCH")
        _require(table_meta["row_count"] == wanted["row_count"], "TABLE_COUNT_METADATA_MISMATCH")
        _require(reader.columns(name) == wanted["column_schema"], "COLUMNS_API_MISMATCH")
        _require(reader.foreign_keys(name) == wanted["foreign_keys"], "FOREIGN_KEYS_API_MISMATCH")
        _require(reader.row_count(name) == wanted["row_count"], "ROW_COUNT_API_MISMATCH")
        actual_rows = list(reader.iter_rows(name))
        _require(len(actual_rows) == wanted["row_count"], "ITERATED_ROW_COUNT_MISMATCH")
        actual_signature = []
        for ordinal, rowid, values in actual_rows:
            _require(ordinal == len(actual_signature), "ROW_ORDINAL_MISMATCH")
            actual_signature.append((_typed(rowid), tuple(_typed(value) for value in values)))
        _require(sorted(actual_signature) == sorted(_signature_rows(wanted["rows"])),
                 "NATIVE_ROWS_OR_ROWIDS_MISMATCH")
        observed_rows += len(actual_rows)
    return len(metadata), observed_rows


def _check_native_examples(snapshot: dict[str, Any]) -> None:
    table = snapshot["tables"].get("typed_records")
    _require(table is not None and table["row_count"] == 1, "TYPED_EXAMPLE_TABLE_MISSING")
    columns = table["columns"]
    row = table["rows"][0]["values"]
    values = dict(zip(columns, row))
    _require(type(values["integer_min"]) is int and values["integer_min"] == -(1 << 63),
             "INTEGER_MIN_NOT_PRESERVED")
    _require(type(values["integer_max"]) is int and values["integer_max"] == (1 << 63) - 1,
             "INTEGER_MAX_NOT_PRESERVED")
    _require(type(values["real_value"]) is float, "REAL_NOT_PRESERVED")
    _require(type(values["text_value"]) is str and "\x00" in values["text_value"]
             and "𐀀" in values["text_value"], "TEXT_NUL_OR_ASTRAL_NOT_PRESERVED")
    _require(values["literal_json"] == '{"generation":"final","n":2}',
             "LITERAL_JSON_TEXT_NOT_PRESERVED")
    _require(type(values["blob_value"]) is bytes and values["blob_value"].startswith(b"\x00\xff"),
             "BLOB_NOT_PRESERVED")
    _require(values["nullable_value"] is None and values["empty_text"] == ""
             and values["empty_blob"] == b"", "NULL_OR_EMPTY_VALUE_NOT_PRESERVED")
    rowids = {entry["rowid"] for entry in snapshot["tables"]["rowid_values"]["rows"]}
    _require(rowids == {-401, 902}, "HIDDEN_ROWID_NOT_PRESERVED")
    _require(snapshot["tables"]["deliberately_empty"]["row_count"] == 0,
             "EMPTY_TABLE_NOT_PRESERVED")


def _tree_snapshot(root: Path) -> dict[str, tuple[int, str]]:
    result = {}
    for path in sorted(root.rglob("*")):
        if path.is_symlink():
            raise FixtureFailure("FIXTURE_TREE_SYMLINK")
        if path.is_file():
            raw = path.read_bytes()
            result[path.relative_to(root).as_posix()] = (len(raw), _sha256(raw))
    return result


def _file_snapshot(path: Path) -> tuple[Any, ...]:
    if not path.exists():
        return (False,)
    if path.is_symlink() or not path.is_file():
        raise FixtureFailure("SOURCE_FILE_TYPE_CHANGED")
    info = path.stat()
    raw = path.read_bytes()
    return (True, info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns,
            info.st_ctime_ns, _sha256(raw))


def _all_source_file_snapshots(paths: tuple[Path, ...]) -> dict[str, tuple[Any, ...]]:
    result = {}
    for path in paths:
        result[path.name] = _file_snapshot(path)
        for suffix in ("-wal", "-shm", "-journal"):
            sidecar = Path(str(path) + suffix)
            result[path.name + suffix] = _file_snapshot(sidecar)
    return result


def _apply_update(path: Path, statements: tuple[tuple[str, tuple[Any, ...]], ...]) -> None:
    with closing(sqlite3.connect(path)) as connection:
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA recursive_triggers=ON")
        for statement, params in statements:
            connection.execute(statement, params)
        connection.commit()
        _require(not connection.execute("PRAGMA foreign_key_check").fetchall(),
                 "WRITER_FOREIGN_KEY_CHECK_FAILED")


def _copy_control_and_local_generation(
    *,
    delta_root: Path,
    remote: MemoryRclone,
    generation_ids: tuple[str, ...],
    generations: dict[str, Path],
) -> None:
    for generation_id in generation_ids:
        local_dir = delta_root / generation_id
        if not local_dir.exists():
            shutil.copytree(generations[generation_id], local_dir)
        remote_path = f"{REMOTE}/deltas/generations/{generation_id}/index.json"
        local_index = local_dir / "index.json"
        local_index.write_bytes(remote.objects[remote_path])


def _publish_delta_generation(delta_publisher: Any, remote: MemoryRclone,
                              generation_dir: Path, state_dir: Path) -> dict[str, Any]:
    with patch.object(delta_publisher.publisher, "Rclone", return_value=remote):
        result = delta_publisher.publish_delta(generation_dir, REMOTE, state_dir)
    _require(result.get("status") == "complete" and result.get("checkpoint_advanced") is True,
             "DELTA_PUBLISH_FIXTURE_FAILED")
    return result


def _run_archive_cli(code_root: Path, temp_root: Path, db_sentinel: Path,
                     arguments: tuple[str, ...]) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["IKARING_ARCHIVE_DATA_DIR"] = str(temp_root / "cli-data-must-stay-absent")
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    command = [sys.executable, str(code_root / "archive.py"),
               "--db", str(db_sentinel), *arguments]
    try:
        completed = subprocess.run(
            command, cwd=code_root, env=env, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, text=True, timeout=90, check=False,
        )
    except Exception:
        raise FixtureFailure("ARCHIVE_CLI_EXECUTION_FAILED") from None
    _require(not db_sentinel.exists()
             and not Path(str(db_sentinel) + ".lock").exists()
             and not (temp_root / "cli-data-must-stay-absent").exists(),
             "ARCHIVE_CLI_OPENED_OR_CREATED_STORE")
    if completed.stderr:
        _require(str(temp_root) not in completed.stderr
                 and str(db_sentinel) not in completed.stderr
                 and "Traceback" not in completed.stderr,
                 "ARCHIVE_CLI_LEAKED_PATH_OR_TRACEBACK")
    return completed


def _cli_json(raw: str) -> Any:
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        raise FixtureFailure("ARCHIVE_CLI_JSON_INVALID") from None


def _decode_cli_cell(cell: dict[str, Any]) -> Any:
    kind = cell.get("sqlite_type")
    encoding = cell.get("encoding")
    value = cell.get("value")
    if kind == "null":
        _require(encoding == "none" and value is None, "CLI_NULL_ENCODING_INVALID")
        return None
    if kind == "integer":
        _require(encoding == "decimal" and isinstance(value, str),
                 "CLI_INTEGER_ENCODING_INVALID")
        return int(value)
    if kind == "real":
        _require(encoding == "float.hex" and isinstance(value, str),
                 "CLI_REAL_ENCODING_INVALID")
        return float.fromhex(value)
    if kind == "text":
        _require(encoding == "unicode" and isinstance(value, str),
                 "CLI_TEXT_ENCODING_INVALID")
        return value
    if kind == "blob":
        _require(encoding == "hex" and isinstance(value, str),
                 "CLI_BLOB_ENCODING_INVALID")
        return bytes.fromhex(value)
    raise FixtureFailure("CLI_SQLITE_TYPE_INVALID")


def _run(code_root_arg: Path) -> dict[str, Any]:
    started = time.monotonic()
    code_root = _validate_code_root(code_root_arg)
    sys.dont_write_bytecode = True
    sys.path.insert(0, str(code_root / "scripts"))
    sys.path.insert(0, str(code_root / "src/python"))
    try:
        import mirror_published_controls as control_mirror
        import nas_full_data_publish as full_publisher
        import prepare_full_data_generation as full_generator
        import prepare_full_data_delta as delta_preparer
        import publish_full_data_delta as delta_publisher
        from published_sqlite_reader import PublishedSQLiteReader, PublishedSQLiteReaderError
        from ikarchive.change_feed import install_change_feed
        from ikarchive.writer_guards import install_writer_guards
    except Exception:
        raise FixtureFailure("PROJECT_MODULE_IMPORT_FAILED") from None

    code_sha_before = _source_code_sha(code_root)
    passed_checks: list[str] = []
    counts: dict[str, int] = {}
    previous_umask = os.umask(0o077)
    try:
        # Keep generated data outside the source checkout so --code-root may
        # be mounted read-only. Resolve the system scratch path before passing
        # it to readers that reject symlinked ancestors.
        with tempfile.TemporaryDirectory(prefix="nas-published-reader-") as raw_temp:
            root = _validate_scratch_root(Path(raw_temp), code_root)
            os.chmod(root, 0o700)
            _require(stat.S_IMODE(root.stat().st_mode) == 0o700, "TEMP_ROOT_NOT_PRIVATE")
            state_dir = root / "publisher-state"
            state_dir.mkdir(mode=0o700)
            source_dir = root / "source"
            source_dir.mkdir(mode=0o700)
            baseline_db = source_dir / "baseline.sqlite3"
            current_db = source_dir / "current.sqlite3"
            source_manifest_path = source_dir / "source-manifest.json"
            baseline_generation_dir = root / "baseline-generation"
            delta_root = root / "delta-generations"
            delta_root.mkdir(mode=0o700)
            control_root = root / "published-controls"
            control_root.mkdir(mode=0o700)
            _write_source(baseline_db)
            shutil.copyfile(baseline_db, current_db)
            baseline_raw = baseline_db.read_bytes()
            baseline_source = {"bytes": len(baseline_raw), "sha256": _sha256(baseline_raw)}
            source_manifest = {
                "storage": "plaintext",
                "encryption": None,
                "captured_at": "2026-10-03T08:00:00Z",
                "captured_at_kind": "pinned_read_transaction",
                "raw_snapshot": {
                    "basename": baseline_db.name,
                    **baseline_source,
                    "quick_check": "ok",
                },
                "verification": {"sha256_match": True, "quick_check": "ok"},
            }
            source_manifest_path.write_bytes(_compact(source_manifest))

            remote = MemoryRclone()
            quiet_run = subprocess.run

            def _quiet_subprocess_run(*args, **kwargs):
                kwargs.setdefault("stdout", subprocess.PIPE)
                kwargs.setdefault("stderr", subprocess.PIPE)
                return quiet_run(*args, **kwargs)

            with patch.object(full_generator.subprocess, "run", _quiet_subprocess_run):
                full_generator.prepare_generation(
                    baseline_db, source_manifest_path, baseline_generation_dir, BASELINE_ID,
                )
            _require(os.path.samefile(baseline_db, baseline_generation_dir / "source.sqlite3"),
                     "BASELINE_SNAPSHOT_BINDING_FAILED")
            with patch.object(full_publisher, "Rclone", return_value=remote):
                baseline_result = full_publisher.publish_generation(
                    baseline_generation_dir, BASELINE_ID, REMOTE, state_dir,
                )
            _require(baseline_result.get("status") == "complete", "BASELINE_PUBLISH_FIXTURE_FAILED")
            baseline_latest_raw = remote.objects[f"{REMOTE}/latest.json"]
            baseline_package = baseline_generation_dir / "slices"
            passed_checks.append("publisher_baseline_generation_complete")

            with closing(sqlite3.connect(current_db)) as connection:
                connection.execute("PRAGMA foreign_keys=ON")
                connection.execute("PRAGMA recursive_triggers=ON")
                install_change_feed(connection)
                install_writer_guards(connection)
                connection.commit()

            plans: dict[str, dict[str, Any]] = {}
            generations: dict[str, Path] = {}

            def prepare_and_publish(generation_id: str, previous: dict[str, Any] | None):
                generation_dir = root / "prepared-deltas" / generation_id
                generation_dir.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
                plan = delta_preparer.prepare_delta(
                    current_db, baseline_db, source_manifest_path, generation_dir,
                    generation_id, BASELINE_ID, previous=previous,
                    max_part_bytes=256 * 1024,
                )
                _publish_delta_generation(delta_publisher, remote, generation_dir, state_dir)
                plans[generation_id] = plan
                generations[generation_id] = generation_dir
                return plan, remote.objects[f"{REMOTE}/latest.json"]

            initial_plan, _initial_latest = prepare_and_publish(INITIAL_ID, None)
            _require(initial_plan["kind"] == "baseline_reconciliation"
                     and initial_plan["replaces_delta_chain"] is True,
                     "INITIAL_RESET_NOT_PUBLISHED")

            _apply_update(current_db, (
                ("UPDATE typed_records SET real_value=?,text_value=? WHERE record_id=?",
                 (2.5, "before schema reset\x00𐀀", 41)),
                ("UPDATE rowid_values SET payload=? WHERE rowid=?", (b"\x00before-reset", -401)),
            ))
            with closing(sqlite3.connect(current_db)) as connection:
                connection.execute("PRAGMA foreign_keys=ON")
                connection.execute("PRAGMA recursive_triggers=ON")
                connection.execute("ALTER TABLE typed_records ADD COLUMN post_reset TEXT")
                connection.execute("UPDATE typed_records SET post_reset=? WHERE record_id=?",
                                   ("created-by-schema-reset", 41))
                connection.commit()

            reset_plan, _reset_latest = prepare_and_publish(RESET_ID, initial_plan)
            _require(reset_plan["kind"] == "baseline_reconciliation"
                     and reset_plan["replaces_delta_chain"] is True,
                     "SCHEMA_RESET_NOT_PUBLISHED")

            _apply_update(current_db, (
                ("UPDATE typed_records SET real_value=?,text_value=?,blob_value=? WHERE record_id=?",
                 (3.5, "incremental-one\x00𐀀", b"\x00\xffincremental-one", 41)),
                ("UPDATE rowid_values SET payload=?,note=? WHERE rowid=?",
                 (b"\x00updated-on-delta-one", "updated exact rowid", -401)),
                ("INSERT INTO rowid_values(rowid,label,payload,note) VALUES(?,?,?,?)",
                 (902, "rowid-added", b"\xffnew\x00", "inserted on delta one")),
            ))
            incremental_one_plan, latest_one_raw = prepare_and_publish(INCREMENTAL_ONE_ID, reset_plan)
            _require(incremental_one_plan["kind"] == "change_feed"
                     and incremental_one_plan["replaces_delta_chain"] is False,
                     "FIRST_INCREMENTAL_NOT_PUBLISHED")
            state_one = _capture_source(current_db)

            final_json_text = '{"generation":"final","n":2}'
            final_blob = b"\x00\xff\x80final-native-blob\x00"
            _apply_update(current_db, (
                ("UPDATE typed_records SET literal_json=?,blob_value=?,nullable_value=?,empty_text=?,empty_blob=? WHERE record_id=?",
                 (final_json_text, final_blob, None, "", b"", 41)),
                ("DELETE FROM rowid_values WHERE rowid=?", (9001,)),
                ("UPDATE match_classification SET analysis_set=?,rule_raw=?,raw_classification=?",
                 ("fixture_mode", "RULE_FIXTURE", b"\x00final-classification")),
            ))
            incremental_two_plan, latest_two_raw = prepare_and_publish(INCREMENTAL_TWO_ID,
                                                                         incremental_one_plan)
            _require(incremental_two_plan["kind"] == "change_feed"
                     and incremental_two_plan["replaces_delta_chain"] is False,
                     "SECOND_INCREMENTAL_NOT_PUBLISHED")
            state_two = _capture_source(current_db)
            _check_native_examples(state_two)

            effective_chain = (RESET_ID, INCREMENTAL_ONE_ID, INCREMENTAL_TWO_ID)
            _require([row["generation_id"] for row in json.loads(latest_two_raw)["delta_chain"]]
                     == list(effective_chain), "PUBLISHED_RESET_CHAIN_INVALID")
            _require([row["generation_id"] for row in json.loads(latest_one_raw)["delta_chain"]]
                     == [RESET_ID, INCREMENTAL_ONE_ID], "FIRST_PINNED_CHAIN_INVALID")
            passed_checks.append("schema_reset_then_two_incrementals")
            passed_checks.append("baseline_and_delta_publisher_controls_created")

            _copy_control_and_local_generation(
                delta_root=delta_root,
                remote=remote,
                generation_ids=effective_chain,
                generations=generations,
            )

            # Run the real mirror implementation with the in-memory-only
            # client. Pin the earlier latest; a later fresh reader will see
            # the final generation after the normal latest advance below.
            remote_latest_path = f"{REMOTE}/latest.json"
            latest_two_remote_raw = remote.objects[remote_latest_path]
            remote.objects[remote_latest_path] = latest_one_raw
            checks_before_mirror = remote.directory_binding_checks
            try:
                mirror_receipt = control_mirror.mirror_published_controls(
                    remote, REMOTE, control_root,
                )
            finally:
                remote.objects[remote_latest_path] = latest_two_remote_raw
            expected_mirror = {
                "latest.json": latest_one_raw,
                f"generations/{BASELINE_ID}/index.json": remote.objects[
                    f"{REMOTE}/generations/{BASELINE_ID}/index.json"],
            }
            for generation_id in (RESET_ID, INCREMENTAL_ONE_ID):
                for name in ("index.json", "delta-plan.json"):
                    relative = f"deltas/generations/{generation_id}/{name}"
                    expected_mirror[relative] = remote.objects[f"{REMOTE}/{relative}"]
            mirrored_snapshot = _tree_snapshot(control_root)
            lock_snapshot = mirrored_snapshot.pop(".published-controls.lock", None)
            _require(lock_snapshot == (0, _sha256(b"")),
                     "CONTROL_MIRROR_LOCK_INVALID")
            _require(mirrored_snapshot == {
                name: (len(raw), _sha256(raw))
                for name, raw in expected_mirror.items()
            }, "CONTROL_MIRROR_BYTES_OR_INVENTORY_MISMATCH")
            _require(mirror_receipt.get("generation_id") == INCREMENTAL_ONE_ID
                     and mirror_receipt.get("baseline_generation_id") == BASELINE_ID
                     and mirror_receipt.get("controls_full_readback") is True
                     and mirror_receipt.get("all_remote_artifacts_verified") is False
                     and mirror_receipt.get("realtime_synchronized") is False
                     and remote.directory_binding_checks - checks_before_mirror >= 2,
                     "CONTROL_MIRROR_RECEIPT_OR_BINDING_CHECK_INVALID")

            latest_two_control_root = root / "published-controls-latest-two"
            latest_two_control_root.mkdir(mode=0o700)
            checks_before_latest_two_mirror = remote.directory_binding_checks
            latest_two_receipt = control_mirror.mirror_published_controls(
                remote, REMOTE, latest_two_control_root,
            )
            expected_latest_two_mirror = {
                "latest.json": latest_two_raw,
                f"generations/{BASELINE_ID}/index.json": remote.objects[
                    f"{REMOTE}/generations/{BASELINE_ID}/index.json"],
            }
            for generation_id in effective_chain:
                for name in ("index.json", "delta-plan.json"):
                    relative = f"deltas/generations/{generation_id}/{name}"
                    expected_latest_two_mirror[relative] = remote.objects[
                        f"{REMOTE}/{relative}"]
            latest_two_snapshot = _tree_snapshot(latest_two_control_root)
            latest_two_lock = latest_two_snapshot.pop(".published-controls.lock", None)
            _require(latest_two_lock == (0, _sha256(b""))
                     and latest_two_snapshot == {
                         name: (len(raw), _sha256(raw))
                         for name, raw in expected_latest_two_mirror.items()
                     }, "LATEST_TWO_CONTROL_MIRROR_BYTES_OR_INVENTORY_MISMATCH")
            _require(latest_two_receipt.get("generation_id") == INCREMENTAL_TWO_ID
                     and latest_two_receipt.get("baseline_generation_id") == BASELINE_ID
                     and latest_two_receipt.get("controls_full_readback") is True
                     and remote.directory_binding_checks - checks_before_latest_two_mirror >= 2,
                     "LATEST_TWO_CONTROL_MIRROR_RECEIPT_INVALID")
            for name in ("index.json", "delta-plan.json"):
                relative = f"deltas/generations/{INCREMENTAL_TWO_ID}/{name}"
                mirrored = latest_two_control_root / relative
                target = control_root / relative
                target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
                target.write_bytes(mirrored.read_bytes())
                _require(target.read_bytes() == remote.objects[f"{REMOTE}/{relative}"],
                         "LATEST_TWO_CHAIN_CONTROL_COPY_MISMATCH")
            passed_checks.append("remote_control_mirror_exact_bytes_and_receipt")

            control_root_fingerprint = _tree_snapshot(control_root)
            baseline_control_root = root / "published-baseline-controls"
            baseline_control_root.mkdir(mode=0o700)
            remote_final_latest = remote.objects[f"{REMOTE}/latest.json"]
            remote.objects[f"{REMOTE}/latest.json"] = baseline_latest_raw
            try:
                baseline_mirror_receipt = control_mirror.mirror_published_controls(
                    remote, REMOTE, baseline_control_root,
                )
            finally:
                remote.objects[f"{REMOTE}/latest.json"] = remote_final_latest
            baseline_control_snapshot = _tree_snapshot(baseline_control_root)
            baseline_lock = baseline_control_snapshot.pop(".published-controls.lock", None)
            _require(baseline_lock == (0, _sha256(b""))
                     and baseline_control_snapshot == {
                         "latest.json": (len(baseline_latest_raw), _sha256(baseline_latest_raw)),
                         f"generations/{BASELINE_ID}/index.json": (
                             len(remote.objects[f"{REMOTE}/generations/{BASELINE_ID}/index.json"]),
                             _sha256(remote.objects[f"{REMOTE}/generations/{BASELINE_ID}/index.json"])),
                     }
                     and baseline_mirror_receipt.get("generation_id") == BASELINE_ID
                     and baseline_mirror_receipt.get("controls_full_readback") is True,
                     "BASELINE_ONLY_CONTROL_MIRROR_INVALID")
            cli_db_sentinel = root / "database-must-not-be-created.sqlite3"

            baseline_list = _run_archive_cli(code_root, root, cli_db_sentinel, (
                "published-list", "--controls", str(baseline_control_root),
                "--root", str(baseline_package), "--delta-root", str(delta_root),
                "--generation", BASELINE_ID,
            ))
            _require(baseline_list.returncode == 0 and not baseline_list.stderr,
                     "BASELINE_PUBLISHED_LIST_FAILED")
            baseline_list_doc = _cli_json(baseline_list.stdout)
            baseline_tables = _capture_source(baseline_db)["tables"]
            _require(baseline_list_doc.get("baseline_generation") == BASELINE_ID
                     and baseline_list_doc.get("latest_generation") == BASELINE_ID
                     and baseline_list_doc.get("published_deltas_verified") is False
                     and baseline_list_doc.get("table_count") == len(baseline_tables)
                     and baseline_list_doc.get("schema_objects") == _capture_source(baseline_db)["schema_objects"],
                     "BASELINE_PUBLISHED_LIST_METADATA_MISMATCH")
            baseline_control_after_cli = _tree_snapshot(baseline_control_root)
            _require({key: value for key, value in baseline_control_after_cli.items()
                      if key != ".published-controls.lock"} == baseline_control_snapshot,
                     "BASELINE_CLI_MUTATED_CONTROLS")
            passed_checks.append("archive_cli_baseline_list_and_absent_delta_chain")

            package_fingerprints = {
                "baseline_generation": _tree_snapshot(baseline_generation_dir),
                **{generation_id: _tree_snapshot(generations[generation_id])
                   for generation_id in effective_chain},
            }
            local_delta_fingerprints = {
                generation_id: _tree_snapshot(delta_root / generation_id)
                for generation_id in effective_chain
            }
            source_files = (baseline_db, current_db, source_manifest_path)
            source_fingerprints = _all_source_file_snapshots(source_files)
            # A local prepared generation without the publisher's full index
            # is not treated as published, even when its transport is valid.
            reset_index_path = delta_root / RESET_ID / "index.json"
            reset_index_raw = reset_index_path.read_bytes()
            reset_index_path.unlink()
            try:
                with _expect_reader_rejection(
                    PublishedSQLiteReader, PublishedSQLiteReaderError,
                    control_root, baseline_package, delta_root,
                    "PUBLISHED_CONTROL_FILE_INVALID",
                ):
                    pass
            finally:
                reset_index_path.write_bytes(reset_index_raw)
            passed_checks.append("missing_published_index_rejected")

            # A local index whose SHA/content differs from its control mirror
            # must be rejected before rows are exposed.
            local_one_index = delta_root / INCREMENTAL_ONE_ID / "index.json"
            local_one_raw = local_one_index.read_bytes()
            local_one_index.write_bytes(local_one_raw + b" ")
            try:
                with _expect_reader_rejection(
                    PublishedSQLiteReader, PublishedSQLiteReaderError,
                    control_root, baseline_package, delta_root,
                    "PUBLISHED_LOCAL_DELTA_MISMATCH",
                ):
                    pass
            finally:
                local_one_index.write_bytes(local_one_raw)
            passed_checks.append("local_control_index_mismatch_rejected")

            # An immutable baseline slice binding mismatch between the publisher
            # control index and local baseline package must be rejected even if
            # the index/latest controls are structurally consistent.
            baseline_control_index = control_root / "generations" / BASELINE_ID / "index.json"
            baseline_control_raw = baseline_control_index.read_bytes()
            tampered_doc = json.loads(baseline_control_raw.decode("utf-8"))
            found_manifest = False
            for entry in tampered_doc.get("files", []):
                if entry.get("local") == "slices/manifest.json":
                    entry["sha256"] = "0" * 64
                    found_manifest = True
                    break
            _require(found_manifest, "GLOBAL_BASELINE_MANIFEST_ENTRY_MISSING")
            tampered_raw = _compact(tampered_doc)
            baseline_control_index.write_bytes(tampered_raw)
            latest_control_file = control_root / "latest.json"
            latest_control_raw = latest_control_file.read_bytes()
            latest_doc = json.loads(latest_control_raw.decode("utf-8"))
            if latest_doc.get("role") is None:
                latest_doc["index_sha256"] = _sha256(tampered_raw)
            else:
                baseline_ref = latest_doc.get("baseline")
                _require(isinstance(baseline_ref, dict), "LATEST_BASELINE_REFERENCE_MISSING")
                changed_index_sha = _sha256(tampered_raw)
                baseline_ref["index_sha256"] = changed_index_sha
                latest_doc["baseline_index_sha256"] = changed_index_sha
            latest_control_file.write_bytes(_compact(latest_doc))
            try:
                with _expect_reader_rejection(
                    PublishedSQLiteReader, PublishedSQLiteReaderError,
                    control_root, baseline_package, delta_root,
                    "PUBLISHED_BASELINE_BINDING_MISMATCH",
                ):
                    pass
            finally:
                baseline_control_index.write_bytes(baseline_control_raw)
                latest_control_file.write_bytes(latest_control_raw)
            passed_checks.append("immutable_baseline_binding_mismatch_rejected")

            control_index = control_root / "deltas" / "generations" / RESET_ID / "index.json"
            control_index_raw = control_index.read_bytes()
            reader = PublishedSQLiteReader(
                control_root, baseline_package, delta_root,
                expected_generation_id=INCREMENTAL_ONE_ID,
            )
            with reader as opened:
                _require(opened.generation_id == INCREMENTAL_ONE_ID,
                         "PINNED_GENERATION_ID_MISMATCH")
                _require(opened.baseline_generation_id == BASELINE_ID,
                         "PINNED_BASELINE_ID_MISMATCH")
                _require(opened.published_control_binding_verified is True
                         and opened.published_deltas_verified is True,
                         "PUBLISHED_BINDING_PROOF_MISSING")
                tables_one, rows_one = _check_reader(opened, state_one)
                # latest is mutable by design: advancing it must not change
                # this reader's pinned generation or typed snapshot.
                (control_root / "latest.json").write_bytes(latest_two_raw)
                _require(opened.generation_id == INCREMENTAL_ONE_ID,
                         "OPEN_READER_MOVED_WITH_LATEST")
                _require(_check_reader(opened, state_one) == (tables_one, rows_one),
                         "OPEN_READER_SNAPSHOT_CHANGED_AFTER_ADVANCE")
            passed_checks.append("all_tables_schema_xinfo_foreign_keys_and_native_rows")

            # A fresh context observes the advanced latest and the second
            # incremental delta, including the reset-era schema.
            with PublishedSQLiteReader(
                control_root, baseline_package, delta_root,
                expected_generation_id=INCREMENTAL_TWO_ID,
            ) as opened:
                _require(opened.generation_id == INCREMENTAL_TWO_ID,
                         "NEW_CONTEXT_DID_NOT_OBSERVE_LATEST")
                tables_final, rows_final = _check_reader(opened, state_two)
                _require(tables_final == len(state_two["tables"]), "FINAL_TABLE_COUNT_MISMATCH")
            _check_native_examples(state_two)
            passed_checks.append("source_rowids_and_sqlite_native_types")
            passed_checks.append("latest_pin_and_new_context_advance")

            final_list = _run_archive_cli(code_root, root, cli_db_sentinel, (
                "published-list", "--controls", str(control_root),
                "--root", str(baseline_package), "--delta-root", str(delta_root),
                "--generation", INCREMENTAL_TWO_ID,
            ))
            _require(final_list.returncode == 0 and not final_list.stderr,
                     "FINAL_PUBLISHED_LIST_FAILED")
            final_list_doc = _cli_json(final_list.stdout)
            final_table_metadata = final_list_doc.get("tables")
            _require(final_list_doc.get("baseline_generation") == BASELINE_ID
                     and final_list_doc.get("latest_generation") == INCREMENTAL_TWO_ID
                     and final_list_doc.get("published_control_binding_verified") is True
                     and final_list_doc.get("published_deltas_verified") is True
                     and final_list_doc.get("all_remote_artifacts_verified") is False
                     and final_list_doc.get("realtime_synchronized") is False
                     and final_list_doc.get("table_count") == len(state_two["tables"])
                     and final_list_doc.get("schema_objects") == state_two["schema_objects"]
                     and isinstance(final_table_metadata, list)
                     and [row.get("name") for row in final_table_metadata]
                     == sorted(state_two["tables"]),
                     "FINAL_PUBLISHED_LIST_METADATA_MISMATCH")
            for table_meta in final_table_metadata:
                expected_meta = state_two["tables"][table_meta["name"]]
                _require(table_meta.get("columns") == expected_meta["columns"]
                         and table_meta.get("column_schema") == expected_meta["column_schema"]
                         and table_meta.get("foreign_keys") == expected_meta["foreign_keys"]
                         and table_meta.get("row_count") == expected_meta["row_count"],
                         "FINAL_PUBLISHED_LIST_TABLE_METADATA_MISMATCH")
            passed_checks.append("archive_cli_final_list_and_full_chain_metadata")

            def run_published_read(*arguments: str):
                completed = _run_archive_cli(code_root, root, cli_db_sentinel, (
                    "published-read", "--controls", str(control_root),
                    "--root", str(baseline_package), "--delta-root", str(delta_root),
                    *arguments,
                ))
                _require(completed.returncode == 0 and not completed.stderr,
                         "PUBLISHED_READ_CLI_FAILED")
                try:
                    documents = [json.loads(line) for line in completed.stdout.splitlines()]
                except json.JSONDecodeError:
                    raise FixtureFailure("PUBLISHED_READ_CLI_JSON_INVALID") from None
                _require(len(documents) >= 2 and documents[0].get("type") == "header"
                         and documents[-1].get("type") == "footer",
                         "PUBLISHED_READ_CLI_STREAM_INVALID")
                return documents[0], documents[1:-1], documents[-1]

            typed_header, typed_rows, typed_footer = run_published_read(
                "--generation", INCREMENTAL_TWO_ID, "--table", "typed_records",
            )
            expected_typed = state_two["tables"]["typed_records"]["rows"]
            _require(len(typed_rows) == len(expected_typed) == 1,
                     "PUBLISHED_READ_TYPED_ROW_COUNT_INVALID")
            typed_row = typed_rows[0]
            _require(typed_row.get("source_rowid") == {
                "sqlite_type": "integer", "encoding": "decimal", "value": "41",
            }, "PUBLISHED_READ_SOURCE_ROWID_INVALID")
            decoded_values = tuple(_decode_cli_cell(cell) for cell in typed_row.get("values", []))
            _require(typed_header.get("baseline_generation") == BASELINE_ID
                     and typed_header.get("latest_generation") == INCREMENTAL_TWO_ID
                     and typed_header.get("published_deltas_verified") is True
                     and typed_header.get("source_rowid_projection") == "original_source"
                     and typed_header.get("columns") == state_two["tables"]["typed_records"]["columns"]
                     and decoded_values == tuple(expected_typed[0]["values"])
                     and _decode_cli_cell(typed_row["source_rowid"]) == expected_typed[0]["rowid"]
                     and typed_footer.get("generation") == INCREMENTAL_TWO_ID
                     and typed_footer.get("latest_generation") == INCREMENTAL_TWO_ID
                     and typed_footer.get("pinned_latest_sha256") ==
                     typed_header.get("pinned_latest_sha256"),
                     "PUBLISHED_READ_TYPED_VALUES_OR_PIN_MISMATCH")
            passed_checks.append("archive_cli_native_typed_values_rowid_and_same_pin_footer")

            full_match_header, full_match_rows, _full_match_footer = run_published_read(
                "--generation", INCREMENTAL_TWO_ID, "--table", "matches",
            )
            selector_header, selector_rows, _selector_footer = run_published_read(
                "--generation", INCREMENTAL_TWO_ID, "--table", "matches",
                "--mode", "fixture_mode", "--rule", "RULE_FIXTURE",
            )
            full_match_expected = state_two["tables"]["matches"]["rows"][0]
            _require(len(full_match_rows) == len(selector_rows) == 1
                     and "source_rowid" in full_match_rows[0]
                     and _decode_cli_cell(full_match_rows[0]["source_rowid"])
                     == full_match_expected["rowid"]
                     and "source_rowid" not in selector_rows[0]
                     and tuple(_decode_cli_cell(cell) for cell in full_match_rows[0]["values"])
                     == tuple(full_match_expected["values"])
                     and tuple(_decode_cli_cell(cell) for cell in selector_rows[0]["values"])
                     == tuple(full_match_expected["values"])
                     and full_match_header.get("source_rowid_projection") == "original_source"
                     and selector_header.get("source_rowid_projection") ==
                     "unavailable_for_derived_rows"
                     and selector_header.get("scope", {}).get("kind") == "mode_rule_selector"
                     and selector_header.get("scope", {}).get("analysis_set") == "fixture_mode"
                     and selector_header.get("scope", {}).get("rule_token") == "RULE_FIXTURE",
                     "PUBLISHED_SELECTOR_SOURCE_IDENTITY_CONTRACT_INVALID")
            passed_checks.append("archive_cli_selector_rows_do_not_forge_source_identity")

            limited_header, limited_rows, limited_footer = run_published_read(
                "--generation", INCREMENTAL_TWO_ID, "--table", "rowid_values", "--limit", "1",
            )
            _require(limited_header.get("table_full_row_count") == 2
                     and len(limited_rows) == 1
                     and limited_footer.get("returned_rows") == 1
                     and limited_footer.get("truncated") is True
                     and limited_footer.get("limit") == 1,
                     "PUBLISHED_READ_LIMIT_TRUNCATION_INVALID")
            passed_checks.append("archive_cli_limit_truncation_and_footer")

            reset_index_path = delta_root / RESET_ID / "index.json"
            reset_index_raw = reset_index_path.read_bytes()
            reset_index_path.unlink()
            try:
                missing_cli = _run_archive_cli(code_root, root, cli_db_sentinel, (
                    "published-read", "--controls", str(control_root),
                    "--root", str(baseline_package), "--delta-root", str(delta_root),
                    "--generation", INCREMENTAL_TWO_ID, "--table", "typed_records",
                ))
            finally:
                reset_index_path.write_bytes(reset_index_raw)
            missing_error = _cli_json(missing_cli.stderr)
            _require(missing_cli.returncode == 1 and missing_cli.stdout == ""
                     and missing_error == {"error": "PUBLISHED_CONTROL_FILE_INVALID"},
                     "PUBLISHED_READ_MISSING_INDEX_NOT_SANITIZED")
            passed_checks.append("archive_cli_missing_index_fails_without_partial_output")

            # Same bytes at a replacement inode are still a changed immutable
            # publisher control. Closing a partial iterator must also close
            # both readers and the private overlay database.
            partial_reader = PublishedSQLiteReader(
                control_root, baseline_package, delta_root,
                expected_generation_id=INCREMENTAL_TWO_ID,
            )
            caught = None
            owned = {}
            try:
                with partial_reader as opened:
                    owned["baseline"] = opened._baseline_reader
                    owned["delta"] = opened._delta_reader
                    owned["temp_root"] = opened._delta_reader._temp_root
                    iterator = opened.iter_rows("typed_records")
                    next(iterator)
                    replacement = control_index.with_suffix(".fixture-replacement")
                    replacement.write_bytes(control_index_raw)
                    os.replace(replacement, control_index)
                    try:
                        iterator.close()
                    except PublishedSQLiteReaderError as exc:
                        caught = exc
            except PublishedSQLiteReaderError as exc:
                caught = caught or exc
            _require(caught is not None and caught.category == "PUBLISHED_CONTROL_CHANGED",
                     "IMMUTABLE_CONTROL_INODE_EXCHANGE_NOT_REJECTED")
            _require(not partial_reader._active and owned["baseline"]._active is False,
                     "READER_RESOURCE_CLEANUP_FAILED")
            _require(owned["delta"]._connection is None and owned["delta"]._temp is None
                     and not owned["temp_root"].exists(), "OVERLAY_RESOURCE_CLEANUP_FAILED")
            _require(control_index.read_bytes() == control_index_raw,
                     "CONTROL_BYTES_CHANGED_DURING_INODE_CHECK")
            passed_checks.append("partial_iterator_inode_exchange_and_cleanup")

            # The preceding same-byte inode exchange is intentionally allowed
            # before a new context pins that same immutable content.
            with PublishedSQLiteReader(
                control_root, baseline_package, delta_root,
                expected_generation_id=INCREMENTAL_TWO_ID,
            ) as opened:
                _require(_check_reader(opened, state_two) == (tables_final, rows_final),
                         "POST_NEGATIVE_READER_REOPEN_FAILED")
            passed_checks.append("same_bytes_reopen_after_inode_replacement")

            _require(_tree_snapshot(baseline_generation_dir)
                     == package_fingerprints["baseline_generation"],
                     "BASELINE_GENERATION_PACKAGE_MUTATED")
            for generation_id in effective_chain:
                _require(_tree_snapshot(generations[generation_id]) == package_fingerprints[generation_id],
                         "DELTA_PACKAGE_MUTATED")
                _require(_tree_snapshot(delta_root / generation_id)
                         == local_delta_fingerprints[generation_id],
                         "LOCAL_DELTA_PACKAGE_MUTATED")
            _require(_all_source_file_snapshots(source_files) == source_fingerprints,
                     "SOURCE_OR_SIDECAR_BYTES_CHANGED")
            final_source = _capture_source(current_db)
            _require(final_source["schema_objects"] == state_two["schema_objects"]
                     and _snapshot_signature(final_source) == _snapshot_signature(state_two),
                     "SOURCE_SCHEMA_OR_TYPED_ROWS_CHANGED")
            # latest.json is the one intentionally mutable control. All pinned
            # immutable controls and their bytes remain as captured.
            after_controls = _tree_snapshot(control_root)
            _require({key: value for key, value in after_controls.items() if key != "latest.json"}
                     == {key: value for key, value in control_root_fingerprint.items()
                         if key != "latest.json"}, "IMMUTABLE_CONTROLS_MUTATED")
            passed_checks.append("source_and_package_snapshots_unchanged")
            counts = {
                "baseline_tables": len(_capture_source(baseline_db)["tables"]),
                "final_tables": len(state_two["tables"]),
                "final_rows": sum(row["row_count"] for row in state_two["tables"].values()),
                "delta_generations_prepared": 4,
                "effective_delta_generations": len(effective_chain),
                "fixture_checks": len(passed_checks),
            }
    finally:
        os.umask(previous_umask)

    code_sha_after = _source_code_sha(code_root)
    _require(code_sha_before == code_sha_after, "CODE_CHANGED_DURING_FIXTURE")
    _require(counts.get("fixture_checks") == len(passed_checks),
             "FIXTURE_CHECK_DENOMINATOR_MISMATCH")
    elapsed_ms = int((time.monotonic() - started) * 1000)
    return {
        "status": "ok",
        "checks": passed_checks,
        "counts": counts,
        "module_composite_sha256": code_sha_before,
        "elapsed_ms": elapsed_ms,
    }


class _expect_reader_rejection:
    def __init__(self, reader_type, error_type, control_root, baseline_package,
                 delta_root, expected_category):
        self.reader_type = reader_type
        self.error_type = error_type
        self.control_root = control_root
        self.baseline_package = baseline_package
        self.delta_root = delta_root
        self.expected_category = expected_category

    def __enter__(self):
        try:
            with self.reader_type(self.control_root, self.baseline_package, self.delta_root):
                pass
        except self.error_type as exc:
            if exc.category != self.expected_category:
                raise FixtureFailure("PUBLISHED_READER_REJECTION_CATEGORY_MISMATCH") from None
            return self
        except Exception:
            raise FixtureFailure("UNEXPECTED_PUBLISHED_READER_EXCEPTION") from None
        raise FixtureFailure("EXPECTED_PUBLISHED_READER_REJECTION_MISSING")

    def __exit__(self, *_args):
        return False


def _snapshot_signature(snapshot: dict[str, Any]) -> Any:
    return (
        snapshot["schema_objects"],
        tuple((name,
               tuple(table["columns"]),
               tuple(tuple(sorted(column.items())) for column in table["column_schema"]),
               tuple(tuple(sorted(foreign_key.items())) for foreign_key in table["foreign_keys"]),
               table["row_count"],
               tuple(_signature_rows(table["rows"])))
              for name, table in sorted(snapshot["tables"].items())),
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--code-root", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        result = _run(args.code_root)
    except FixtureFailure as exc:
        print(json.dumps({"status": "error", "failure": str(exc)},
                         sort_keys=True, separators=(",", ":")), file=sys.stderr)
        return 1
    except Exception:
        print('{"status":"error","failure":"UNEXPECTED_FIXTURE_FAILURE"}', file=sys.stderr)
        return 1
    print(json.dumps(result, ensure_ascii=True, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
