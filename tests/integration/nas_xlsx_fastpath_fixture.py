#!/usr/bin/env python3
"""Compare saved and current XLSX exporters on a private synthetic database.

Uses Python's standard library and the two supplied exporter modules only.
The source database and both export trees live in a temporary directory.
This fixture never contacts a network service or accesses real archive data.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
from importlib.machinery import SourceFileLoader
import json
import os
import pathlib
import random
import sqlite3
import sys
import tempfile
import time
from types import ModuleType
from typing import Any


SNAPSHOT_IDENTIFIER = "artificial-xlsx-fastpath-equivalence"
MAX_ROWS_PER_SHEET = 29
MAX_ZIP_BYTES = 262_144
CHUNK_SIZE = 32_767
LARGE_BLOB_BYTES = 1_100_003
BOUNDARY_TEXT_CHARS = 2 * 32_767 + 19
USER_TABLES = {
    "values_fixture",
    "empty_fixture",
    "fk_parent",
    "fk_child",
    "composite_fixture",
    "shadowed_fixture",
    "generated_fixture",
    "audit_log",
    "auto_fixture",
}


class FixtureFailure(RuntimeError):
    """A stable, sanitized fixture check failure."""


def _require(condition: bool, code: str) -> None:
    if not condition:
        raise FixtureFailure(code)


def _sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _load_module(path: pathlib.Path, name: str, failure_code: str) -> ModuleType:
    try:
        loader = SourceFileLoader(name, str(path))
        spec = importlib.util.spec_from_loader(name, loader, origin=str(path))
        if spec is None or spec.loader is None:
            raise ImportError
        module = importlib.util.module_from_spec(spec)
        module.__file__ = str(path)
        sys.modules[name] = module
        previous_bytecode_setting = sys.dont_write_bytecode
        sys.dont_write_bytecode = True
        try:
            spec.loader.exec_module(module)
        finally:
            sys.dont_write_bytecode = previous_bytecode_setting
        return module
    except Exception as exc:
        sys.modules.pop(name, None)
        raise FixtureFailure(failure_code) from exc


def _typed(value: Any) -> dict[str, Any]:
    if value is None:
        return {"sqlite_type": "null", "value": None}
    if type(value) is int:
        return {"sqlite_type": "integer", "value": str(value)}
    if type(value) is float:
        return {"sqlite_type": "real", "value": value.hex()}
    if type(value) is str:
        return {"sqlite_type": "text", "value": value}
    if type(value) is bytes:
        return {"sqlite_type": "blob", "value": value.hex()}
    raise FixtureFailure("UNSUPPORTED_FIXTURE_SQLITE_VALUE")


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _capture_database(connection: sqlite3.Connection) -> dict[str, Any]:
    schema_objects = [
        {"type": row[0], "name": row[1], "tbl_name": row[2], "sql": row[3]}
        for row in connection.execute(
            "SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name"
        )
    ]
    tables = []
    table_names = [
        row[0]
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
    ]
    table_flags = {
        row[1]: row
        for row in connection.execute("PRAGMA table_list")
        if len(row) >= 6 and row[0] == "main"
    }
    for table_name in table_names:
        quoted = _quote_identifier(table_name)
        xinfo = [
            list(row)
            for row in connection.execute(f"PRAGMA table_xinfo({quoted})")
        ]
        flag_row = table_flags.get(table_name)
        if flag_row is None:
            raise FixtureFailure("SOURCE_TABLE_FLAGS_MISSING")
        columns = [row[1] for row in xinfo if row[6] != 1]
        names = {name.casefold() for name in columns}
        if bool(flag_row[4]):
            identity_kind, identity_alias = "without_rowid", None
        else:
            identity_alias = next(
                (
                    alias
                    for alias in ("_rowid_", "rowid", "oid")
                    if alias.casefold() not in names
                ),
                None,
            )
            identity_kind = "rowid" if identity_alias is not None else "shadowed"
        foreign_keys = [
            list(row)
            for row in connection.execute(f"PRAGMA foreign_key_list({quoted})")
        ]
        select = f"SELECT * FROM {quoted}"
        if identity_kind == "rowid":
            rowid_sql = _quote_identifier(identity_alias)
            select = f"SELECT *, {rowid_sql} FROM {quoted} ORDER BY {rowid_sql}"
        else:
            primary_key = [
                item[1]
                for item in sorted(
                    (item for item in xinfo if item[5]), key=lambda item: item[5]
                )
            ]
            order_by = primary_key or columns
            if order_by:
                select += " ORDER BY " + ", ".join(
                    _quote_identifier(name) for name in order_by
                )
        rows = []
        for row in connection.execute(select):
            values = row[: len(columns)]
            source_rowid = row[-1] if identity_kind == "rowid" else None
            if len(values) != len(columns):
                raise FixtureFailure("SOURCE_ROW_WIDTH_INVALID")
            rows.append({
                "source_rowid": None if source_rowid is None else str(source_rowid),
                "values": [_typed(value) for value in values],
            })
        tables.append({
            "name": table_name,
            "xinfo": xinfo,
            "table_flags": list(flag_row[2:]),
            "foreign_keys": foreign_keys,
            "source_rowid_kind": identity_kind,
            "source_rowid_alias": identity_alias,
            "rows": rows,
        })
    views = []
    for (view_name,) in connection.execute(
        "SELECT name FROM sqlite_master WHERE type='view' ORDER BY name"
    ):
        quoted_view = _quote_identifier(view_name)
        cursor = connection.execute(f"SELECT * FROM {quoted_view}")
        view_columns = [column[0] for column in cursor.description or ()]
        view_rows = [[_typed(value) for value in row] for row in cursor]
        views.append({"name": view_name, "columns": view_columns, "rows": view_rows})
    foreign_key_violations = [
        list(row) for row in connection.execute("PRAGMA foreign_key_check")
    ]
    return {
        "schema_objects": schema_objects,
        "tables": tables,
        "views": views,
        "foreign_key_violations": foreign_key_violations,
    }


def _read_output_tree(directory: pathlib.Path) -> dict[str, bytes]:
    result: dict[str, bytes] = {}
    for path in sorted(directory.iterdir(), key=lambda item: item.name):
        if path.is_symlink() or not path.is_file():
            raise FixtureFailure("UNEXPECTED_EXPORT_TREE_ENTRY")
        result[path.name] = path.read_bytes()
    return result


def _make_source(path: pathlib.Path) -> tuple[sqlite3.Connection, bytes, dict[str, Any]]:
    random_source = random.Random(921)
    large_blob = random_source.randbytes(LARGE_BLOB_BYTES)
    ascii_json = (
        '{"unknown":true,"int64":9223372036854775807,'
        '"decimal":1.2300e+04}' * 600
    )
    boundary_text = "A" * BOUNDARY_TEXT_CHARS
    rows = [
        (5, "ascii", ascii_json, large_blob),
        (23, "literal-tokens", "_x0000_ _x005F_ _x0041_ _X0041_\r\n", b""),
        (30, "xml-controls", "".join(chr(code) for code in range(128)), None),
        (71, "unicode", "日本語\U00010000\U00020BB7\ufffe\uffff\r\n", b"\x00\xff"),
        (100, "int64-min", -(1 << 63), b""),
        (501, "int64-max", (1 << 63) - 1, None),
        (811, "real", 0.125, None),
        (923, "negative-zero", -0.0, None),
        (2003, "ascii-boundary", boundary_text, b"boundary"),
    ]
    writable = sqlite3.connect(str(path), isolation_level=None)
    try:
        writable.execute("PRAGMA foreign_keys=ON")
        writable.execute(
            "CREATE TABLE values_fixture(label TEXT NOT NULL, value, payload BLOB)"
        )
        writable.execute(
            "CREATE INDEX values_fixture_label_idx ON values_fixture(label)"
        )
        writable.execute(
            "CREATE VIEW values_fixture_view AS "
            "SELECT label,value FROM values_fixture ORDER BY label"
        )
        writable.execute("CREATE TABLE empty_fixture(name TEXT, content BLOB)")
        writable.execute(
            "CREATE TABLE fk_parent(id INTEGER PRIMARY KEY, label TEXT NOT NULL UNIQUE)"
        )
        writable.execute(
            "CREATE TABLE fk_child(parent_id INTEGER NOT NULL REFERENCES fk_parent(id), "
            "note TEXT, payload BLOB)"
        )
        writable.execute("CREATE INDEX fk_child_parent_idx ON fk_child(parent_id)")
        writable.execute(
            "CREATE TABLE composite_fixture(tenant TEXT NOT NULL,item INTEGER NOT NULL,body, "
            "PRIMARY KEY(tenant,item)) WITHOUT ROWID"
        )
        writable.execute(
            'CREATE TABLE shadowed_fixture("_rowid_" TEXT,"rowid" INTEGER,"oid" BLOB,payload TEXT)'
        )
        writable.execute(
            "CREATE TABLE generated_fixture(base INTEGER NOT NULL,label TEXT NOT NULL,"
            "stored_twice INTEGER GENERATED ALWAYS AS (base*2) STORED,"
            "virtual_lower TEXT GENERATED ALWAYS AS (lower(label)) VIRTUAL)"
        )
        writable.execute(
            "CREATE TABLE audit_log(event TEXT NOT NULL,parent_id INTEGER NOT NULL,detail TEXT)"
        )
        writable.execute(
            "CREATE TRIGGER fk_parent_audit_insert AFTER INSERT ON fk_parent BEGIN "
            "INSERT INTO audit_log(event,parent_id,detail) "
            "VALUES('parent_insert',NEW.id,NEW.label); END"
        )
        writable.execute(
            "CREATE TABLE auto_fixture(id INTEGER PRIMARY KEY AUTOINCREMENT,payload TEXT NOT NULL)"
        )
        writable.executemany(
            "INSERT INTO values_fixture(rowid,label,value,payload) VALUES(?,?,?,?)",
            rows,
        )
        int64_min = -(1 << 63)
        int64_max = (1 << 63) - 1
        writable.executemany(
            "INSERT INTO fk_parent(id,label) VALUES(?,?)",
            [
                (int64_min, "parent-min"),
                (4, "parent-gap"),
                (int64_max, "parent-max"),
            ],
        )
        writable.executemany(
            "INSERT INTO fk_child(rowid,parent_id,note,payload) VALUES(?,?,?,?)",
            [
                (int64_min, int64_min, "child-min", b"\x00\xff"),
                (19, 4, "child-gap", "astral \U00020BB7"),
                (int64_max, int64_max, "child-max", None),
            ],
        )
        writable.executemany(
            "INSERT INTO composite_fixture(tenant,item,body) VALUES(?,?,?)",
            [
                ("z", -3, b"\x00\x80"),
                ("a", int64_max, "composite \U00020BB7"),
                ("m", int64_min, 0.125),
            ],
        )
        writable.executemany(
            'INSERT INTO shadowed_fixture("_rowid_","rowid","oid",payload) VALUES(?,?,?,?)',
            [
                ("shadow-a", 0, b"\x00", "first"),
                ("shadow-b", int64_max, b"\xff", "second"),
            ],
        )
        writable.executemany(
            "INSERT INTO generated_fixture(rowid,base,label) VALUES(?,?,?)",
            [(-11, 21, "MiXeD"), (33, -4, "LOWER")],
        )
        writable.executemany(
            "INSERT INTO auto_fixture(id,payload) VALUES(?,?)",
            [(3, "first-auto"), (40, "second-auto")],
        )
        writable.execute("UPDATE sqlite_sequence SET seq=123 WHERE name='auto_fixture'")
        writable.commit()
    finally:
        writable.close()

    source_bytes = path.read_bytes()
    uri = path.resolve(strict=True).as_uri() + "?mode=ro"
    readonly = sqlite3.connect(uri, uri=True)
    try:
        readonly.execute("PRAGMA query_only=ON")
        state = _capture_database(readonly)

        expected_tables = USER_TABLES | {"sqlite_sequence"}
        actual_tables = {table["name"] for table in state["tables"]}
        _require(actual_tables == expected_tables, "FIXTURE_TABLE_SET_INVALID")
        _require(len(state["tables"]) == 10, "FIXTURE_PHYSICAL_TABLE_COUNT_INVALID")
        table = next(item for item in state["tables"] if item["name"] == "values_fixture")
        _require(len(table["rows"]) == 9, "FIXTURE_ROW_COUNT_INVALID")
        _require(
            [int(row["source_rowid"]) for row in table["rows"]]
            == [5, 23, 30, 71, 100, 501, 811, 923, 2003],
            "FIXTURE_SOURCE_ROWIDS_INVALID",
        )
        value_by_label = {
            row["values"][0]["value"]: row["values"][1]
            for row in table["rows"]
        }
        type_set = {
            cell["sqlite_type"]
            for row in table["rows"]
            for cell in row["values"]
        }
        _require(
            type_set == {"null", "integer", "real", "text", "blob"},
            "FIXTURE_NATIVE_TYPES_INCOMPLETE",
        )
        _require(
            value_by_label["int64-min"]["value"] == str(-(1 << 63))
            and value_by_label["int64-max"]["value"] == str((1 << 63) - 1),
            "FIXTURE_INT64_BOUNDARIES_INVALID",
        )
        _require(
            value_by_label["negative-zero"] == {
                "sqlite_type": "real",
                "value": "-0x0.0p+0",
            },
            "FIXTURE_NEGATIVE_ZERO_NOT_RETAINED",
        )
        _require(
            len(value_by_label["ascii"]["value"]) > 30_000
            and "1.2300e+04" in value_by_label["ascii"]["value"],
            "FIXTURE_ASCII_JSON_INVALID",
        )
        _require(
            len(next(row["values"][2]["value"] for row in table["rows"]
                     if row["values"][0]["value"] == "ascii").encode("ascii"))
            == LARGE_BLOB_BYTES * 2,
            "FIXTURE_LARGE_BLOB_INVALID",
        )
        _require(
            "\U00010000" in value_by_label["unicode"]["value"]
            and "\U00020BB7" in value_by_label["unicode"]["value"]
            and "\ufffe" in value_by_label["unicode"]["value"]
            and "\uffff" in value_by_label["unicode"]["value"],
            "FIXTURE_UNICODE_CASES_INVALID",
        )
        _require(
            value_by_label["ascii-boundary"]["value"] == boundary_text
            and len(boundary_text) > 2 * 32_767,
            "FIXTURE_ASCII_BOUNDARY_CASE_INVALID",
        )
        _require(
            value_by_label["xml-controls"]["value"] == "".join(chr(code) for code in range(128)),
            "FIXTURE_XML_CONTROL_CASES_INVALID",
        )
        _require(
            "_x0000_" in value_by_label["literal-tokens"]["value"]
            and "_x0041_" in value_by_label["literal-tokens"]["value"]
            and "_x005F_" in value_by_label["literal-tokens"]["value"],
            "FIXTURE_LITERAL_ESCAPE_CASES_INVALID",
        )
        _require(
            state["foreign_key_violations"] == []
            and any(item["foreign_keys"] for item in state["tables"]),
            "FIXTURE_FOREIGN_KEY_CASES_INVALID",
        )
        child_fks = next(
            item["foreign_keys"]
            for item in state["tables"]
            if item["name"] == "fk_child"
        )
        _require(
            any(
                foreign_key[2:5] == ["fk_parent", "parent_id", "id"]
                for foreign_key in child_fks
            ),
            "FIXTURE_FOREIGN_KEY_DEFINITION_INVALID",
        )
        _require(
            next(item for item in state["tables"] if item["name"] == "empty_fixture")["rows"] == [],
            "FIXTURE_EMPTY_TABLE_MISSING",
        )
        shadowed = next(item for item in state["tables"] if item["name"] == "shadowed_fixture")
        _require(
            shadowed["source_rowid_kind"] == "shadowed"
            and all(row["source_rowid"] is None for row in shadowed["rows"]),
            "FIXTURE_SHADOWED_ROWID_CASE_INVALID",
        )
        generated = next(item for item in state["tables"] if item["name"] == "generated_fixture")
        _require(
            [row[6] for row in generated["xinfo"]] == [0, 0, 3, 2],
            "FIXTURE_GENERATED_COLUMN_CASE_INVALID",
        )
        composite = next(item for item in state["tables"] if item["name"] == "composite_fixture")
        _require(
            composite["table_flags"][2] == 1
            and [row[5] for row in composite["xinfo"]] == [1, 2, 0],
            "FIXTURE_WITHOUT_ROWID_COMPOSITE_KEY_INVALID",
        )
        sequence_rows = next(item for item in state["tables"] if item["name"] == "sqlite_sequence")["rows"]
        _require(
            any(
                row["values"] == [
                    {"sqlite_type": "text", "value": "auto_fixture"},
                    {"sqlite_type": "integer", "value": "123"},
                ]
                for row in sequence_rows
            ),
            "FIXTURE_SQLITE_SEQUENCE_CASE_INVALID",
        )
        child = next(item for item in state["tables"] if item["name"] == "fk_child")
        _require(
            [row["source_rowid"] for row in child["rows"]]
            == [str(-(1 << 63)), "19", str((1 << 63) - 1)],
            "FIXTURE_HIDDEN_ROWID_BOUNDARIES_INVALID",
        )
        _require(len(state["views"]) == 1, "FIXTURE_VIEW_MISSING")
        _require(
            len(next(item for item in state["tables"] if item["name"] == "audit_log")["rows"]) == 3,
            "FIXTURE_TRIGGER_ROWS_MISSING",
        )
        schema_types = {item["type"] for item in state["schema_objects"]}
        _require(
            {"index", "view", "trigger"}.issubset(schema_types)
            and any(
                item["type"] == "index" and item["sql"] is None
                for item in state["schema_objects"]
            )
            and any(
                item["type"] == "trigger" and item["tbl_name"] == "fk_parent"
                for item in state["schema_objects"]
            ),
            "FIXTURE_SCHEMA_OBJECT_CASES_INVALID",
        )
        return readonly, source_bytes, state
    except BaseException:
        readonly.close()
        raise


def _close(connection: sqlite3.Connection) -> None:
    try:
        connection.close()
    except sqlite3.Error as exc:
        raise FixtureFailure("SQLITE_CONNECTION_CLOSE_FAILED") from exc


def _load_independent_verifier(code_root: pathlib.Path) -> ModuleType:
    verifier_path = code_root / "scripts/export_full_xlsx.py"
    if verifier_path.is_symlink() or not verifier_path.is_file():
        raise FixtureFailure("INDEPENDENT_VERIFIER_MISSING_OR_UNREADABLE")
    return _load_module(
        verifier_path,
        "ikaring_independent_full_xlsx_verifier",
        "INDEPENDENT_VERIFIER_IMPORT_FAILED",
    )


def _xinfo_records(xinfo: list[list[Any]]) -> list[dict[str, Any]]:
    keys = ("cid", "name", "type", "notnull", "dflt_value", "pk", "hidden")
    if any(len(row) != len(keys) for row in xinfo):
        raise FixtureFailure("SOURCE_XINFO_WIDTH_INVALID")
    return [dict(zip(keys, row)) for row in xinfo]


def _artifact_inventory_sha256(files: dict[str, bytes]) -> str:
    digest = hashlib.sha256()
    for name, raw in sorted(files.items()):
        name_bytes = name.encode("utf-8")
        digest.update(len(name_bytes).to_bytes(8, "big"))
        digest.update(name_bytes)
        digest.update(len(raw).to_bytes(8, "big"))
        digest.update(hashlib.sha256(raw).digest())
    return digest.hexdigest()


def _run(code_root: pathlib.Path, reference_path: pathlib.Path) -> dict[str, Any]:
    try:
        if code_root.is_symlink():
            raise FixtureFailure("CODE_ROOT_INVALID")
        code_root = code_root.resolve(strict=True)
        current_path = code_root / "src/python/ikarchive/lossless_xlsx.py"
        if current_path.is_symlink() or not current_path.is_file():
            raise FixtureFailure("CURRENT_MODULE_MISSING_OR_UNREADABLE")
        current_bytes = current_path.read_bytes()
        verifier_path = code_root / "scripts/export_full_xlsx.py"
        if verifier_path.is_symlink() or not verifier_path.is_file():
            raise FixtureFailure("INDEPENDENT_VERIFIER_MISSING_OR_UNREADABLE")
        verifier_bytes = verifier_path.read_bytes()
    except FixtureFailure:
        raise
    except Exception as exc:
        raise FixtureFailure("CODE_ROOT_INVALID") from exc

    try:
        if reference_path.is_symlink() or not reference_path.is_file():
            raise FixtureFailure("REFERENCE_MODULE_MISSING_OR_UNREADABLE")
        reference_path = reference_path.resolve(strict=True)
        reference_bytes = reference_path.read_bytes()
    except FixtureFailure:
        raise
    except Exception as exc:
        raise FixtureFailure("REFERENCE_MODULE_MISSING_OR_UNREADABLE") from exc
    _require(reference_path != current_path.resolve(), "REFERENCE_MODULE_IS_CURRENT_MODULE")

    old_module = _load_module(
        reference_path, "ikaring_reference_lossless_xlsx", "REFERENCE_MODULE_IMPORT_FAILED",
    )
    current_module = _load_module(
        current_path, "ikaring_current_lossless_xlsx", "CURRENT_MODULE_IMPORT_FAILED",
    )
    independent_verifier = _load_independent_verifier(code_root)
    _require(
        callable(getattr(old_module, "export_sqlite_tables", None))
        and callable(getattr(current_module, "export_sqlite_tables", None))
        and callable(getattr(old_module, "reconstruct_sqlite_tables", None))
        and callable(getattr(current_module, "reconstruct_sqlite_tables", None))
        and callable(getattr(independent_verifier, "verify_export", None)),
        "EXPORTER_API_MISMATCH",
    )

    previous_umask = os.umask(0o077)
    started = time.monotonic()
    try:
        with tempfile.TemporaryDirectory(prefix="nas-xlsx-fastpath-") as temp_name:
            root = pathlib.Path(temp_name)
            root.chmod(0o700)
            source_path = root / "synthetic-source.sqlite"
            source, source_bytes_before, source_state_before = _make_source(source_path)
            source_sha_before = _sha256(source_bytes_before)
            try:
                source_tables = {
                    table["name"]: table for table in source_state_before["tables"]
                }
                expected_table_names = set(source_tables)
                expected_rows = sum(len(table["rows"]) for table in source_tables.values())
                expected_cells = sum(
                    len(table["rows"])
                    * sum(column[6] != 1 for column in table["xinfo"])
                    for table in source_tables.values()
                )
                expected_rowid_rows = sum(
                    len(table["rows"])
                    for table in source_tables.values()
                    if table["source_rowid_kind"] == "rowid"
                )
                output_trees: dict[str, dict[str, bytes]] = {}
                indexes: dict[str, dict[str, Any]] = {}
                for label, module in (("old", old_module), ("new", current_module)):
                    output_dir = root / f"output-{label}"
                    output_dir.mkdir(mode=0o700)
                    index = module.export_sqlite_tables(
                        source,
                        output_dir,
                        max_rows_per_sheet=MAX_ROWS_PER_SHEET,
                        max_zip_bytes=MAX_ZIP_BYTES,
                        chunk_size=CHUNK_SIZE,
                        snapshot_identifier=SNAPSHOT_IDENTIFIER,
                    )
                    files = _read_output_tree(output_dir)
                    _require("index.json" in files, "EXPORT_INDEX_MISSING")
                    disk_index = json.loads(files["index.json"].decode("utf-8"))
                    _require(index == disk_index, "RETURNED_INDEX_DIFFERS_FROM_DISK")
                    output_trees[label] = files
                    indexes[label] = disk_index

                _require(
                    output_trees["old"] == output_trees["new"],
                    "OLD_NEW_EXPORT_BYTES_DIFFER",
                )
                for label in ("old", "new"):
                    index = indexes[label]
                    _require(
                        index.get("snapshot_identifier") == SNAPSHOT_IDENTIFIER,
                        "SNAPSHOT_IDENTIFIER_NOT_FIXED",
                    )
                    _require(
                        index.get("schema_objects") == source_state_before["schema_objects"],
                        "INDEX_SCHEMA_OBJECTS_DIFFER_FROM_SOURCE",
                    )
                    index_tables = index.get("tables")
                    _require(
                        isinstance(index_tables, dict)
                        and set(index_tables) == expected_table_names,
                        "INDEX_TABLE_SET_DIFFERS_FROM_SOURCE",
                    )
                    declared_piece_names = ["index.json"]
                    for table_name, source_table in source_tables.items():
                        table_info = index_tables.get(table_name)
                        _require(isinstance(table_info, dict), "TABLE_INDEX_ENTRY_MISSING")
                        columns = [
                            column[1]
                            for column in source_table["xinfo"]
                            if column[6] != 1
                        ]
                        row_count = len(source_table["rows"])
                        cell_count = row_count * len(columns)
                        _require(
                            table_info.get("columns") == columns
                            and table_info.get("column_names") == columns
                            and table_info.get("column_schema")
                            == _xinfo_records(source_table["xinfo"])
                            and table_info.get("source_rowid_kind")
                            == source_table["source_rowid_kind"]
                            and table_info.get("source_rowid_alias")
                            == source_table["source_rowid_alias"],
                            "INDEX_TABLE_SCHEMA_OR_IDENTITY_INVALID",
                        )
                        _require(
                            table_info.get("source_row_count") == row_count
                            and table_info.get("read_row_count") == row_count
                            and table_info.get("source_cell_count") == cell_count
                            and table_info.get("read_cell_count") == cell_count,
                            "INDEX_SOURCE_COUNTS_INVALID",
                        )
                        pieces = table_info.get("pieces")
                        _require(
                            isinstance(pieces, list) and bool(pieces),
                            "XLSX_PIECES_MISSING",
                        )
                        declared_piece_names.extend(piece["name"] for piece in pieces)
                        for piece in pieces:
                            piece_bytes = output_trees[label].get(piece.get("name"))
                            _require(isinstance(piece_bytes, bytes), "DECLARED_XLSX_PIECE_MISSING")
                            _require(
                                len(piece_bytes) == piece.get("bytes")
                                and _sha256(piece_bytes) == piece.get("sha256"),
                                "XLSX_PIECE_BYTES_OR_SHA_INVALID",
                            )
                    _require(
                        set(output_trees[label])
                        == set(declared_piece_names),
                        "OUTPUT_INVENTORY_DIFFERS_FROM_INDEX",
                    )

                    try:
                        verified_counts = independent_verifier.verify_export(
                            source, root / f"output-{label}"
                        )
                    except Exception:
                        raise FixtureFailure("INDEPENDENT_VERIFIER_REJECTED_OUTPUT") from None
                    _require(
                        verified_counts.get("exported_tables") == len(expected_table_names)
                        and verified_counts.get("exported_rows") == expected_rows
                        and verified_counts.get("exported_cells") == expected_cells
                        and verified_counts.get("source_rowids_verified") is True
                        and verified_counts.get("source_rowid_rows_checked")
                        == expected_rowid_rows
                        and verified_counts.get("pieces")
                        == len(declared_piece_names) - 1
                        and verified_counts.get("source_bytes") == len(source_bytes_before),
                        "INDEPENDENT_VERIFIER_COUNTS_INVALID",
                    )
                    boundary_row_number = next(
                        ordinal
                        for ordinal, row in enumerate(
                            source_tables["values_fixture"]["rows"], 1
                        )
                        if row["values"][0]["value"] == "ascii-boundary"
                    )
                    boundary_records = []
                    for piece in index_tables["values_fixture"]["pieces"]:
                        boundary_records.extend(
                            record
                            for record in independent_verifier._iter_piece_rows(
                                root / f"output-{label}" / piece["name"], piece
                            )
                            if record["table"] == "values_fixture"
                            and record["row_number"] == boundary_row_number
                            and record["column_name"] == "value"
                        )
                    boundary_records.sort(key=lambda record: record["chunk_number"])
                    _require(
                        len(boundary_records) == 3
                        and [record["chunk_number"] for record in boundary_records]
                        == [1, 2, 3]
                        and all(
                            record["total_chunks"] == 3
                            and record["sqlite_type"] == "text"
                            and record["payload_encoding"] == "plain"
                            for record in boundary_records
                        )
                        and "".join(record["value_chunk"] for record in boundary_records)
                        == "A" * BOUNDARY_TEXT_CHARS,
                        "ASCII_CELL_LIMIT_BOUNDARY_CHUNKS_INVALID",
                    )

                    target = sqlite3.connect(":memory:")
                    try:
                        target.execute("PRAGMA foreign_keys=ON")
                        reconstructed_counts = module.reconstruct_sqlite_tables(
                            root / f"output-{label}", target
                        )
                        _require(
                            reconstructed_counts
                            == {
                                table_name: len(table["rows"])
                                for table_name, table in source_tables.items()
                            },
                            "RECONSTRUCTED_ROW_COUNTS_DIFFER_FROM_SOURCE",
                        )
                        _require(
                            target.execute("PRAGMA quick_check").fetchall() == [("ok",)],
                            "RECONSTRUCTED_DATABASE_QUICK_CHECK_FAILED",
                        )
                        _require(
                            target.execute("PRAGMA foreign_key_check").fetchall() == [],
                            "RECONSTRUCTED_DATABASE_FOREIGN_KEY_CHECK_FAILED",
                        )
                        _require(
                            _capture_database(target) == source_state_before,
                            "RECONSTRUCTED_DATABASE_DIFFERS_FROM_SOURCE",
                        )
                    finally:
                        target.close()

                source_state_after = _capture_database(source)
                source_bytes_after = source_path.read_bytes()
                _require(source_state_after == source_state_before, "SOURCE_SCHEMA_OR_ROWS_CHANGED")
                _require(source_bytes_after == source_bytes_before, "SOURCE_DATABASE_BYTES_CHANGED")
                _require(
                    _sha256(source_bytes_after) == source_sha_before,
                    "SOURCE_DATABASE_SHA_CHANGED",
                )

                total_elapsed = time.monotonic() - started
                shared_index = indexes["new"]
                new_files = output_trees["new"]
                old_inventory_sha = _artifact_inventory_sha256(output_trees["old"])
                new_inventory_sha = _artifact_inventory_sha256(new_files)
                _require(
                    old_inventory_sha == new_inventory_sha,
                    "OLD_NEW_ARTIFACT_INVENTORIES_DIFFER",
                )
                xlsx_files = sum(name.endswith(".xlsx") for name in new_files)
                return {
                    "status": "ok",
                    "checks": [
                        "old_new_xlsx_and_index_bytes_identical",
                        "both_exports_restore_exact_native_values_schema_and_rowids",
                        "source_database_schema_rows_and_bytes_unchanged",
                        "all_parts_match_index_sizes_and_sha256",
                        "required_native_and_xstring_cases_present",
                    ],
                    "counts": {
                        "source_rows": expected_rows,
                        "source_tables": len(expected_table_names),
                        "source_cell_count": expected_cells,
                        "source_rowid_rows": expected_rowid_rows,
                        "source_schema_objects": len(source_state_before["schema_objects"]),
                        "foreign_key_tables": sum(
                            bool(table["foreign_keys"]) for table in source_tables.values()
                        ),
                        "xlsx_files": xlsx_files,
                        "index_files": 1,
                        "output_files": len(new_files),
                        "piece_chunks": sum(
                            piece["chunk_count"]
                            for table_info in shared_index["tables"].values()
                            for piece in table_info["pieces"]
                        ),
                        "deterministic_blob_bytes": LARGE_BLOB_BYTES,
                    },
                    "artifact_inventory_sha256": new_inventory_sha,
                    "old_artifact_inventory_sha256": old_inventory_sha,
                    "new_artifact_inventory_sha256": new_inventory_sha,
                    "synthetic_source_sha256": source_sha_before,
                    "old_code_sha256": _sha256(reference_bytes),
                    "new_code_sha256": _sha256(current_bytes),
                    "independent_verifier_sha256": _sha256(verifier_bytes),
                    "elapsed_seconds": round(total_elapsed, 6),
                }
            finally:
                _close(source)
    finally:
        os.umask(previous_umask)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--code-root", type=pathlib.Path, required=True)
    parser.add_argument("--reference-module", type=pathlib.Path, required=True)
    args = parser.parse_args(argv)
    try:
        result = _run(args.code_root, args.reference_module)
    except FixtureFailure as exc:
        print(json.dumps({"status": "error", "failure": str(exc)}, sort_keys=True), file=sys.stderr)
        return 1
    except Exception:
        print(json.dumps({"status": "error", "failure": "UNEXPECTED_FIXTURE_FAILURE"}, sort_keys=True), file=sys.stderr)
        return 1
    print(json.dumps(result, ensure_ascii=True, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
