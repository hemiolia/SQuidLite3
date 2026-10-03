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
CHUNK_SIZE = 4096
LARGE_BLOB_BYTES = 1_100_003


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
    for table_name in table_names:
        quoted = _quote_identifier(table_name)
        xinfo = [
            list(row)
            for row in connection.execute(f"PRAGMA table_xinfo({quoted})")
        ]
        rows = []
        for row in connection.execute(f"SELECT rowid,* FROM {quoted} ORDER BY rowid"):
            rows.append({
                "source_rowid": str(row[0]),
                "values": [_typed(value) for value in row[1:]],
            })
        tables.append({"name": table_name, "xinfo": xinfo, "rows": rows})
    return {"schema_objects": schema_objects, "tables": tables}


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
    rows = [
        (5, "ascii", ascii_json, large_blob),
        (23, "literal-tokens", "_x0000_ _x005F_ _x0041_ _X0041_\r\n", b""),
        (30, "xml-controls", "".join(chr(code) for code in range(128)), None),
        (71, "unicode", "日本語\U00010000\ufffe\uffff\r\n", b"\x00\xff"),
        (100, "int64-min", -(1 << 63), b""),
        (501, "int64-max", (1 << 63) - 1, None),
        (811, "real", 0.125, None),
        (923, "negative-zero", -0.0, None),
    ]
    writable = sqlite3.connect(str(path), isolation_level=None)
    try:
        writable.execute(
            "CREATE TABLE values_fixture(label TEXT NOT NULL, value, payload BLOB)"
        )
        writable.execute(
            "CREATE INDEX values_fixture_label_idx ON values_fixture(label)"
        )
        writable.executemany(
            "INSERT INTO values_fixture(rowid,label,value,payload) VALUES(?,?,?,?)",
            rows,
        )
        writable.commit()
    finally:
        writable.close()

    source_bytes = path.read_bytes()
    uri = path.resolve(strict=True).as_uri() + "?mode=ro"
    readonly = sqlite3.connect(uri, uri=True)
    try:
        readonly.execute("PRAGMA query_only=ON")
        state = _capture_database(readonly)

        _require(len(state["tables"]) == 1, "FIXTURE_TABLE_COUNT_INVALID")
        table = state["tables"][0]
        _require(table["name"] == "values_fixture", "FIXTURE_TABLE_NAME_INVALID")
        _require(len(table["rows"]) == 8, "FIXTURE_ROW_COUNT_INVALID")
        _require(
            [int(row["source_rowid"]) for row in table["rows"]]
            == [5, 23, 30, 71, 100, 501, 811, 923],
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
            and "\ufffe" in value_by_label["unicode"]["value"]
            and "\uffff" in value_by_label["unicode"]["value"],
            "FIXTURE_UNICODE_CASES_INVALID",
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
        return readonly, source_bytes, state
    except BaseException:
        readonly.close()
        raise


def _close(connection: sqlite3.Connection) -> None:
    try:
        connection.close()
    except sqlite3.Error as exc:
        raise FixtureFailure("SQLITE_CONNECTION_CLOSE_FAILED") from exc


def _run(code_root: pathlib.Path, reference_path: pathlib.Path) -> dict[str, Any]:
    try:
        if code_root.is_symlink():
            raise FixtureFailure("CODE_ROOT_INVALID")
        code_root = code_root.resolve(strict=True)
        current_path = code_root / "src/python/ikarchive/lossless_xlsx.py"
        if current_path.is_symlink() or not current_path.is_file():
            raise FixtureFailure("CURRENT_MODULE_MISSING_OR_UNREADABLE")
        current_bytes = current_path.read_bytes()
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
    _require(
        callable(getattr(old_module, "export_sqlite_tables", None))
        and callable(getattr(current_module, "export_sqlite_tables", None))
        and callable(getattr(old_module, "reconstruct_sqlite_tables", None))
        and callable(getattr(current_module, "reconstruct_sqlite_tables", None)),
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
            try:
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
                    table_info = index.get("tables", {}).get("values_fixture")
                    _require(isinstance(table_info, dict), "TABLE_INDEX_ENTRY_MISSING")
                    _require(
                        table_info.get("source_row_count") == 8
                        and table_info.get("read_row_count") == 8
                        and table_info.get("source_cell_count") == table_info.get("read_cell_count") == 24,
                        "INDEX_SOURCE_COUNTS_INVALID",
                    )
                    pieces = table_info.get("pieces")
                    _require(isinstance(pieces, list) and bool(pieces), "XLSX_PIECES_MISSING")
                    _require(
                        set(output_trees[label])
                        == {"index.json", *(piece["name"] for piece in pieces)},
                        "OUTPUT_INVENTORY_DIFFERS_FROM_INDEX",
                    )
                    for piece in pieces:
                        piece_bytes = output_trees[label].get(piece.get("name"))
                        _require(isinstance(piece_bytes, bytes), "DECLARED_XLSX_PIECE_MISSING")
                        _require(
                            len(piece_bytes) == piece.get("bytes")
                            and _sha256(piece_bytes) == piece.get("sha256"),
                            "XLSX_PIECE_BYTES_OR_SHA_INVALID",
                        )

                    target = sqlite3.connect(":memory:")
                    try:
                        module.reconstruct_sqlite_tables(root / f"output-{label}", target)
                        _require(
                            target.execute("PRAGMA quick_check").fetchall() == [("ok",)],
                            "RECONSTRUCTED_DATABASE_QUICK_CHECK_FAILED",
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
                    hashlib.sha256(source_bytes_after).digest()
                    == hashlib.sha256(source_bytes_before).digest(),
                    "SOURCE_DATABASE_SHA_CHANGED",
                )

                total_elapsed = time.monotonic() - started
                shared_index = indexes["new"]
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
                        "source_rows": 8,
                        "source_tables": 1,
                        "xlsx_files": sum(name.endswith(".xlsx") for name in output_trees["new"]),
                        "index_files": 1,
                        "output_files": len(output_trees["new"]),
                        "piece_chunks": sum(
                            piece["chunk_count"] for piece in shared_index["tables"]["values_fixture"]["pieces"]
                        ),
                        "deterministic_blob_bytes": LARGE_BLOB_BYTES,
                    },
                    "old_code_sha256": _sha256(reference_bytes),
                    "new_code_sha256": _sha256(current_bytes),
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
