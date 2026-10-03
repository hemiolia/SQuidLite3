"""人工 transport fixture tests for typed native candidate reads."""

from __future__ import annotations

import copy
import hashlib
import math
import shutil
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src/python"))

from ikarchive.change_feed import install_change_feed, read_change_batch
from ikarchive.delta_transport import build_delta_database, iter_delta_records
from ikarchive.native_candidates import NativeTransportCandidates
from ikarchive.verified_files import VerifiedFiles, verify_files
from ikarchive.writer_guards import install_writer_guards
import ikarchive.native_candidates as native_candidates_module


def _fixture_connection(
    *, shadow_all_rowids: bool = False, infinities: bool = False,
) -> sqlite3.Connection:
    connection = sqlite3.connect(":memory:")
    if shadow_all_rowids:
        connection.execute(
            'CREATE TABLE shadowed("rowid", "_rowid_", "oid", value, '
            'PRIMARY KEY(value)) WITHOUT ROWID'
        )
    connection.execute(
        "CREATE TABLE typed_values (int_value, real_value, text_value, blob_value, "
        "null_value, large_payload, special_float)"
    )
    connection.execute("CREATE TABLE empty_native (value)")
    connection.execute("CREATE TABLE int64_edges (value)")
    connection.commit()
    install_change_feed(connection)
    connection.execute("PRAGMA recursive_triggers=ON")
    install_writer_guards(connection)
    large_payload = b"large candidate proof payload\0" * 8192
    connection.executemany(
        "INSERT INTO typed_values VALUES (?,?,?,?,?,?,?)",
        [
            (1, 2.5, "nul\0text\U00020000", b"\x00\xffblob", None,
             large_payload, -0.0),
            (1.0, 1, "1", b"second", "present", large_payload, 3.125),
            ("1", 4.0, "third", b"third", None, large_payload,
             -math.inf if infinities else 5.0),
        ],
    )
    if infinities:
        connection.execute(
            'UPDATE "typed_values" SET "special_float"=? WHERE "_rowid_"=2',
            (math.inf,),
        )
    connection.executemany(
        "INSERT INTO int64_edges VALUES (?)",
        [(-(1 << 63),), ((1 << 63) - 1,)],
    )
    if shadow_all_rowids:
        connection.execute("INSERT INTO shadowed VALUES(1,2,3,'hidden identity')")
    connection.commit()
    return connection


def _generation(suffix: str) -> str:
    return f"20261003T000000Z-{suffix}"


def _build_package(
    root: Path, *, shadow_all_rowids: bool = False, infinities: bool = False,
):
    root.mkdir(parents=True, exist_ok=True)
    database_relative = "deltas/generation/transport.sqlite3"
    database_path = root / database_relative
    database_path.parent.mkdir(parents=True, exist_ok=True)
    connection = _fixture_connection(
        shadow_all_rowids=shadow_all_rowids, infinities=infinities,
    )
    try:
        with read_change_batch(connection, 0) as batch:
            metadata = dict(batch.metadata)
            metadata.update(
                kind="change_feed",
                generation_id=_generation("0123abcd"),
                parent_generation_id=None,
                baseline_generation_id=None,
                captured_at="2026-10-03T00:00:00Z",
            )
            document = build_delta_database(
                batch.iter_current_changes(), metadata, database_path,
            )
    finally:
        connection.close()

    control_path = root / "transport-control.json"
    control_path.write_text('{"fixture":true}\n', encoding="utf-8")
    records = {}
    for path in (database_path, control_path):
        raw = path.read_bytes()
        records[path.relative_to(root).as_posix()] = {
            "bytes": len(raw),
            "sha256": hashlib.sha256(raw).hexdigest(),
            "role": "transport" if path == database_path else "control",
        }
    token = verify_files(root, records)
    adapter = NativeTransportCandidates(
        root,
        database_relative,
        document,
        verified_files=token,
        verified_records=records,
    )
    return adapter, document, token, records, database_path, control_path


class _CursorProxy:
    def __init__(self, cursor, tracker):
        self._cursor = cursor
        self._tracker = tracker

    @property
    def description(self):
        return self._cursor.description

    def fetchone(self):
        return self._cursor.fetchone()

    def fetchall(self):
        return self._cursor.fetchall()

    def __iter__(self):
        return self

    def __next__(self):
        return next(self._cursor)

    def close(self):
        self._tracker.cursor_close_calls += 1
        return self._cursor.close()


class _ConnectionProxy:
    def __init__(self, connection, tracker, *, execute_error=None, close_error=None):
        self._connection = connection
        self._tracker = tracker
        self._execute_error = execute_error
        self._close_error = close_error

    def execute(self, sql, parameters=()):
        self._tracker.sql.append((sql, tuple(parameters)))
        if self._execute_error is not None:
            error = self._execute_error
            self._execute_error = None
            raise error
        return _CursorProxy(self._connection.execute(sql, parameters), self._tracker)

    def close(self):
        self._tracker.connection_close_calls += 1
        self._connection.close()
        if self._close_error is not None:
            raise self._close_error


class _ConnectionTracker:
    def __init__(self):
        self.sql = []
        self.cursor_close_calls = 0
        self.connection_close_calls = 0


class NativeTransportCandidatesTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name).resolve()
        (self.adapter, self.document, self.token, self.records,
         self.database_path, self.control_path) = _build_package(self.root)

    def tearDown(self):
        self.temporary.cleanup()

    def _typed_column_indexes(self):
        columns = self.adapter.visible["typed_values"]
        return {name: columns.index(name) for name in columns}

    def _adapter_for_mutated_database(self, mutate):
        temporary = tempfile.TemporaryDirectory()
        root = Path(temporary.name).resolve()
        database_relative = "deltas/generation/transport.sqlite3"
        database_path = root / database_relative
        database_path.parent.mkdir(parents=True)
        shutil.copyfile(self.database_path, database_path)
        control_path = root / "transport-control.json"
        shutil.copyfile(self.control_path, control_path)
        connection = sqlite3.connect(database_path)
        try:
            mutate(connection, copy.deepcopy(self.document))
            connection.commit()
        finally:
            connection.close()

        document = copy.deepcopy(self.document)
        raw = database_path.read_bytes()
        document["database"].update({
            "path": str(database_path),
            "bytes": len(raw),
            "sha256": hashlib.sha256(raw).hexdigest(),
        })
        records = {}
        for path in (database_path, control_path):
            content = path.read_bytes()
            records[path.relative_to(root).as_posix()] = {
                "bytes": len(content),
                "sha256": hashlib.sha256(content).hexdigest(),
            }
        token = verify_files(root, records)
        return temporary, root, database_relative, document, records, token

    def test_typed_native_matches_preserve_sqlite_types_null_and_float_bits(self):
        indexes = self._typed_column_indexes()
        self.assertEqual(
            list(self.adapter.iter_table_candidates(
                "typed_values", {indexes["int_value"]: 1},
            )),
            [
                (0, 1, (1, 2.5, "nul\0text\U00020000", b"\x00\xffblob", None,
                       b"large candidate proof payload\0" * 8192, -0.0)),
            ],
        )
        self.assertEqual(
            list(self.adapter.iter_table_candidates(
                "typed_values", {indexes["int_value"]: 1.0},
            )),
            [(1, 2, (1.0, 1, "1", b"second", "present",
                     b"large candidate proof payload\0" * 8192, 3.125))],
        )
        self.assertEqual(
            list(self.adapter.iter_table_candidates(
                "typed_values", {indexes["int_value"]: "1"},
            )),
            [(2, 3, ("1", 4.0, "third", b"third", None,
                     b"large candidate proof payload\0" * 8192, 5.0))],
        )
        self.assertEqual(
            list(self.adapter.iter_table_candidates(
                "typed_values", {indexes["null_value"]: None},
            )),
            [
                (0, 1, (1, 2.5, "nul\0text\U00020000", b"\x00\xffblob", None,
                       b"large candidate proof payload\0" * 8192, -0.0)),
                (2, 3, ("1", 4.0, "third", b"third", None,
                       b"large candidate proof payload\0" * 8192, 5.0)),
            ],
        )
        zero = next(self.adapter.iter_table_candidates(
            "typed_values", {indexes["special_float"]: -0.0},
        ))
        self.assertEqual(zero[2][indexes["special_float"]].hex(), "-0x0.0p+0")

    def test_exact_python_types_and_sqlite_real_validation(self):
        indexes = self._typed_column_indexes()
        with self.assertRaisesRegex(ValueError, "CRITERIA_VALUE_INVALID"):
            list(self.adapter.iter_table_candidates(
                "typed_values", {indexes["int_value"]: True},
            ))
        self.assertEqual(list(self.adapter.iter_table_candidates(
            "typed_values", {indexes["real_value"]: 1.0},
        )), [])
        self.assertEqual(list(self.adapter.iter_table_candidates(
            "typed_values", {indexes["int_value"]: "1"},
        )), [(2, 3, ("1", 4.0, "third", b"third", None,
                     b"large candidate proof payload\0" * 8192, 5.0))])
        with self.assertRaisesRegex(ValueError, "CRITERIA_VALUE_INVALID"):
            list(self.adapter.iter_table_candidates(
                "typed_values", {indexes["int_value"]: math.nan},
            ))
        with self.assertRaisesRegex(ValueError, "CRITERIA_INVALID"):
            list(self.adapter.iter_table_candidates("typed_values", {True: 1}))

        blob_index = indexes["blob_value"]
        self.assertEqual(list(self.adapter.iter_table_candidates(
            "typed_values", {blob_index: b"\x00\xffblob"},
        ))[0][0:2], (0, 1))
        with self.assertRaisesRegex(ValueError, "CRITERIA_VALUE_INVALID"):
            list(self.adapter.iter_table_candidates(
                "typed_values", {blob_index: bytearray(b"\x00\xffblob")},
            ))
        self.assertEqual(
            [(ordinal, rowid, values[0]) for ordinal, rowid, values in
             self.adapter.iter_table_candidates("int64_edges", {0: -(1 << 63)})],
            [(0, 1, -(1 << 63))],
        )
        self.assertEqual(
            [(ordinal, rowid, values[0]) for ordinal, rowid, values in
             self.adapter.iter_table_candidates("int64_edges", {0: (1 << 63) - 1})],
            [(1, 2, (1 << 63) - 1)],
        )
        for out_of_range in (-(1 << 63) - 1, 1 << 63):
            with self.assertRaisesRegex(ValueError, "CRITERIA_VALUE_INVALID"):
                list(self.adapter.iter_table_candidates("int64_edges", {0: out_of_range}))

    def test_positive_and_negative_infinity_are_native_real_values(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            adapter, _doc, _token, _records, _db, _control = _build_package(
                root, infinities=True,
            )
            index = adapter.visible["typed_values"].index("special_float")
            positive = list(adapter.iter_table_candidates(
                "typed_values", {index: math.inf},
            ))
            negative = list(adapter.iter_table_candidates(
                "typed_values", {index: -math.inf},
            ))
            self.assertEqual(len(positive), 1)
            self.assertEqual(len(negative), 1)
            self.assertEqual(positive[0][2][index].hex(), math.inf.hex())
            self.assertEqual(negative[0][2][index].hex(), (-math.inf).hex())

    def test_filter_false_scans_rowids_without_selecting_unrequested_large_payload(self):
        tracker = _ConnectionTracker()
        real_connect = sqlite3.connect

        def tracked_connect(*args, **kwargs):
            return _ConnectionProxy(real_connect(*args, **kwargs), tracker)

        indexes = self._typed_column_indexes()
        adapter = self.adapter
        with patch.object(native_candidates_module.sqlite3, "connect", side_effect=tracked_connect):
            candidates = adapter.iter_table_candidates(
                "typed_values",
                {indexes["null_value"]: None},
                candidate_row_filter=lambda _ordinal, _rowid: False,
            )
            self.assertEqual(list(candidates), [])

        scan_sql = [sql for sql, _parameters in tracker.sql
                    if 'JOIN "typed_values"' in sql]
        self.assertEqual(len(scan_sql), 1)
        self.assertIn('"_rowid_"', scan_sql[0])
        self.assertIn('"null_value"', scan_sql[0])
        self.assertNotIn('"large_payload"', scan_sql[0])
        self.assertNotIn("SELECT *", scan_sql[0])
        self.assertEqual(tracker.connection_close_calls, 1)
        self.assertGreaterEqual(tracker.cursor_close_calls, 3)

    def test_connection_uses_immutable_readonly_uri_and_readonly_pragmas(self):
        tracker = _ConnectionTracker()
        real_connect = sqlite3.connect
        connect_calls = []

        def tracked_connect(*args, **kwargs):
            connect_calls.append((args, dict(kwargs)))
            return _ConnectionProxy(real_connect(*args, **kwargs), tracker)

        with patch.object(native_candidates_module.sqlite3, "connect", side_effect=tracked_connect):
            self.assertEqual(list(self.adapter.iter_table_candidates(
                "typed_values", {}, candidate_row_filter=lambda _ordinal, _rowid: False,
            )), [])

        self.assertEqual(len(connect_calls), 1)
        args, kwargs = connect_calls[0]
        self.assertIn("?mode=ro&immutable=1", args[0])
        self.assertIs(kwargs.get("uri"), True)
        sql = [statement for statement, _parameters in tracker.sql]
        self.assertIn("PRAGMA query_only=ON", sql)
        self.assertIn("PRAGMA trusted_schema=OFF", sql)

    def test_empty_criteria_filter_sweep_validates_counts_without_full_rows(self):
        tracker = _ConnectionTracker()
        real_connect = sqlite3.connect

        def tracked_connect(*args, **kwargs):
            return _ConnectionProxy(real_connect(*args, **kwargs), tracker)

        with patch.object(native_candidates_module.sqlite3, "connect", side_effect=tracked_connect):
            self.assertEqual(list(self.adapter.iter_table_candidates(
                "typed_values", {}, candidate_row_filter=lambda _ordinal, _rowid: False,
            )), [])
            self.assertEqual(list(self.adapter.iter_table_candidates(
                self.adapter.metadata_table, {}, candidate_row_filter=lambda _ordinal, _rowid: False,
            )), [])
            operation_rows = self.adapter.table_documents[self.adapter.operations_table]["row_count"]
            operation_ids = []

            def reject_operation(ordinal, rowid):
                operation_ids.append((ordinal, rowid))
                return False

            self.assertEqual(list(self.adapter.iter_table_candidates(
                self.adapter.operations_table, {},
                candidate_row_filter=reject_operation,
            )), [])

        selects = [sql for sql, _parameters in tracker.sql if sql.startswith("SELECT")]
        self.assertTrue(selects)
        self.assertFalse(any("SELECT *" in sql for sql in selects))
        self.assertGreater(operation_rows, 0)
        self.assertEqual(operation_ids, [(ordinal, ordinal) for ordinal in range(operation_rows)])
        self.assertEqual(tracker.connection_close_calls, 3)
        self.assertGreaterEqual(tracker.cursor_close_calls, 9)

    def test_full_candidates_include_synthetic_table_columns_and_properties_are_copies(self):
        documents = self.adapter.table_documents
        self.assertEqual(documents[self.adapter.operations_table]["columns"], [
            "operation_ordinal", "table_name", "operation", "source_rowid",
            "identity_json", "row_ordinal", "transport_rowid", "identity_key",
        ])
        self.assertEqual(documents[self.adapter.metadata_table]["columns"], ["name", "value"])
        self.assertEqual(documents[self.adapter.metadata_table]["row_count"], 1)
        docs_before = copy.deepcopy(documents)
        documents[self.adapter.metadata_table]["columns"].append("forged")
        visible = self.adapter.visible
        visible["typed_values"].append("forged")
        table_names = self.adapter.table_names
        table_names.append("forged")
        self.assertEqual(self.adapter.table_documents, docs_before)
        self.assertNotIn("forged", self.adapter.visible["typed_values"])
        self.assertNotIn("forged", self.adapter.table_names)

        all_rows = list(self.adapter.iter_table_candidates("empty_native", {}))
        self.assertEqual(all_rows, [])
        metadata_row = list(self.adapter.iter_table_candidates(self.adapter.metadata_table, {}))
        self.assertEqual(len(metadata_row), 1)
        self.assertEqual(metadata_row[0][0:2], (0, 1))
        self.assertEqual(metadata_row[0][2][0], "source_metadata")

    def test_fully_shadowed_rowid_alias_fails_only_for_requested_table(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            adapter, _doc, _token, _records, _db, _control = _build_package(
                root, shadow_all_rowids=True,
            )
            self.assertEqual(
                list(adapter.iter_table_candidates("typed_values", {}))[0][0:2],
                (0, 1),
            )
            with self.assertRaisesRegex(ValueError, "ROWID_UNAVAILABLE"):
                list(adapter.iter_table_candidates("shadowed", {}))

    def test_document_counts_inventory_metadata_and_token_bindings_are_enforced(self):
        altered_count = copy.deepcopy(self.document)
        altered_count["upsert_counts_by_table"]["empty_native"] = 1
        with self.assertRaisesRegex(ValueError, "TABLE_COUNT_MISMATCH"):
            NativeTransportCandidates(
                self.root, "deltas/generation/transport.sqlite3", altered_count,
                verified_files=self.token, verified_records=self.records,
            )

        altered_operation_count = copy.deepcopy(self.document)
        altered_operation_count["operation_counts"]["upsert"] += 1
        with self.assertRaisesRegex(ValueError, "TABLE_COUNT_MISMATCH"):
            NativeTransportCandidates(
                self.root, "deltas/generation/transport.sqlite3", altered_operation_count,
                verified_files=self.token, verified_records=self.records,
            )

        altered_document = copy.deepcopy(self.document)
        altered_document["metadata"]["kind"] = "baseline_reconciliation"
        with self.assertRaisesRegex(ValueError, "RECONCILIATION_METADATA_INVALID"):
            NativeTransportCandidates(
                self.root, "deltas/generation/transport.sqlite3", altered_document,
                verified_files=self.token, verified_records=self.records,
            )

        subset_records = {"deltas/generation/transport.sqlite3": self.records[
            "deltas/generation/transport.sqlite3"
        ]}
        subset_token = verify_files(self.root, subset_records)
        with self.assertRaisesRegex(ValueError, "VERIFIED_FILES_BINDING_MISMATCH"):
            NativeTransportCandidates(
                self.root, "deltas/generation/transport.sqlite3", self.document,
                verified_files=subset_token, verified_records=self.records,
            )
        with self.assertRaisesRegex(ValueError, "NATIVE_TRANSPORT_VERIFIED_FILES_INVALID"):
            NativeTransportCandidates(
                self.root, "deltas/generation/transport.sqlite3", self.document,
                verified_files=object(), verified_records=self.records,
            )

    def test_database_document_is_relocated_on_a_copy_and_bound_to_registered_hash(self):
        document = copy.deepcopy(self.document)
        document["database"]["path"] = "/synthetic/original/transport.sqlite3"
        before = copy.deepcopy(document)
        adapter = NativeTransportCandidates(
            self.root, "deltas/generation/transport.sqlite3", document,
            verified_files=self.token, verified_records=self.records,
        )
        self.assertEqual(document, before)
        self.assertEqual(adapter.table_names, self.adapter.table_names)

        wrong_hash_binding = copy.deepcopy(self.document)
        wrong_hash_binding["database"]["bytes"] += 1
        with self.assertRaisesRegex(ValueError, "VERIFIED_FILES_RECORD_MISMATCH"):
            NativeTransportCandidates(
                self.root, "deltas/generation/transport.sqlite3", wrong_hash_binding,
                verified_files=self.token, verified_records=self.records,
            )

    def test_registered_database_symlink_and_missing_file_are_rejected(self):
        for replacement in ("symlink", "missing"):
            with self.subTest(replacement=replacement), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary).resolve()
                adapter, _document, _token, _records, database_path, _control = (
                    _build_package(root)
                )
                original = database_path.read_bytes()
                database_path.unlink()
                if replacement == "symlink":
                    target = root / "outside.sqlite3"
                    target.write_bytes(original)
                    database_path.symlink_to(target)
                with self.assertRaisesRegex(ValueError, "VERIFIED_FILES_"):
                    list(adapter.iter_table_candidates("typed_values", {}))

    def test_unknown_sqlite_header_has_a_stable_constructor_error(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            database_relative = "deltas/generation/transport.sqlite3"
            database_path = root / database_relative
            database_path.parent.mkdir(parents=True)
            database_path.write_bytes(b"synthetic non-SQLite transport")
            control_path = root / "transport-control.json"
            control_path.write_bytes(b"{}\n")
            document = copy.deepcopy(self.document)
            raw = database_path.read_bytes()
            document["database"].update({
                "path": str(database_path),
                "bytes": len(raw),
                "sha256": hashlib.sha256(raw).hexdigest(),
            })
            records = {}
            for path in (database_path, control_path):
                content = path.read_bytes()
                records[path.relative_to(root).as_posix()] = {
                    "bytes": len(content),
                    "sha256": hashlib.sha256(content).hexdigest(),
                }
            token = verify_files(root, records)
            with self.assertRaisesRegex(ValueError, "DATABASE_INVALID"):
                NativeTransportCandidates(
                    root, database_relative, document,
                    verified_files=token, verified_records=records,
                )

    def test_physical_inventory_metadata_and_unrelated_table_count_are_checked_on_open(self):
        cases = [
            (
                lambda connection, _document: connection.execute(
                    'CREATE TABLE "unlisted_extra" ("value")'
                ),
                "TRANSPORT_INVENTORY_INVALID",
            ),
            (
                lambda connection, document: connection.execute(
                    f'UPDATE "{document["internal_names"]["metadata_table"]}" '
                    'SET "value"=? WHERE "name"=?', ("{}", "source_metadata"),
                ),
                "TRANSPORT_METADATA_MISMATCH",
            ),
            (
                lambda connection, _document: connection.execute(
                    'DELETE FROM "typed_values" WHERE "_rowid_"=1'
                ),
                "NATIVE_TRANSPORT_TABLE_COUNT_MISMATCH",
            ),
        ]
        for mutate, expected_code in cases:
            with self.subTest(expected_code=expected_code):
                temporary, root, database_relative, document, records, token = (
                    self._adapter_for_mutated_database(mutate)
                )
                try:
                    with self.assertRaisesRegex(ValueError, expected_code):
                        NativeTransportCandidates(
                            root, database_relative, document,
                            verified_files=token, verified_records=records,
                        )
                finally:
                    temporary.cleanup()

    def test_source_upsert_reference_inventory_is_checked_before_candidate_scans(self):
        temporary, root, database_relative, document, records, token = (
            self._adapter_for_mutated_database(
                lambda connection, doc: connection.execute(
                    f'UPDATE "{doc["internal_names"]["operations_table"]}" '
                    'SET "transport_rowid"=0 '
                    'WHERE "table_name"=? AND "operation"=? AND "row_ordinal"=?',
                    ("typed_values", "upsert", 0),
                )
            )
        )
        try:
            with self.assertRaisesRegex(ValueError, "SOURCE_REFERENCES_INVALID"):
                NativeTransportCandidates(
                    root, database_relative, document,
                    verified_files=token, verified_records=records,
                )
        finally:
            temporary.cleanup()

    def test_missing_upsert_row_reference_is_detected_during_joined_scan(self):
        temporary, root, database_relative, document, records, token = (
            self._adapter_for_mutated_database(
                lambda connection, _doc: connection.execute(
                    'UPDATE "typed_values" SET "_rowid_"=9 WHERE "_rowid_"=3'
                )
            )
        )
        try:
            adapter = NativeTransportCandidates(
                root, database_relative, document,
                verified_files=token, verified_records=records,
            )
            with self.assertRaisesRegex(ValueError, "ROWID_INVALID"):
                list(adapter.iter_table_candidates("typed_values", {}))
        finally:
            temporary.cleanup()

    def test_source_transport_hidden_rowid_gaps_are_preserved_without_inventing_ordinals(self):
        temporary, root, database_relative, document, records, token = (
            self._adapter_for_mutated_database(
                lambda connection, doc: (
                    connection.execute(
                        'UPDATE "typed_values" SET "_rowid_"=9 WHERE "_rowid_"=1'
                    ),
                    connection.execute(
                        f'UPDATE "{doc["internal_names"]["operations_table"]}" '
                        'SET "transport_rowid"=9 '
                        'WHERE "table_name"=? AND "operation"=? AND "row_ordinal"=?',
                        ("typed_values", "upsert", 0),
                    ),
                )
            )
        )
        try:
            adapter = NativeTransportCandidates(
                root, database_relative, document,
                verified_files=token, verified_records=records,
            )
            rows = list(adapter.iter_table_candidates("typed_values", {}))
            self.assertEqual([(ordinal, rowid) for ordinal, rowid, _ in rows],
                             [(0, 9), (1, 2), (2, 3)])
            full_upserts = [
                record for record in iter_delta_records(
                    root / database_relative, document,
                ) if record["table_name"] == "typed_values"
            ]
            self.assertEqual(len(full_upserts), 3)
            self.assertEqual(
                [values for _ordinal, _rowid, values in rows],
                [record["values"] for record in full_upserts],
            )
        finally:
            temporary.cleanup()

    def test_metadata_table_accepts_any_actual_integer_rowid(self):
        for metadata_rowid in (0, -7):
            with self.subTest(metadata_rowid=metadata_rowid):
                temporary, root, database_relative, document, records, token = (
                    self._adapter_for_mutated_database(
                        lambda connection, doc: connection.execute(
                            f'UPDATE "{doc["internal_names"]["metadata_table"]}" '
                            'SET "_rowid_"=?', (metadata_rowid,),
                        )
                    )
                )
                try:
                    adapter = NativeTransportCandidates(
                        root, database_relative, document,
                        verified_files=token, verified_records=records,
                    )
                    self.assertEqual(
                        [(ordinal, rowid) for ordinal, rowid, _ in
                         adapter.iter_table_candidates(adapter.metadata_table, {})],
                        [(0, metadata_rowid)],
                    )
                finally:
                    temporary.cleanup()

    def test_sidecar_is_rejected_even_when_not_in_verified_inventory(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            adapter, document, _token, _records, database_path, _control = _build_package(root)
            Path(str(database_path) + "-wal").write_bytes(b"")
            raw = database_path.read_bytes()
            control = root / "transport-control.json"
            records = {
                "deltas/generation/transport.sqlite3": {
                    "bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest(),
                },
                "transport-control.json": {
                    "bytes": control.stat().st_size,
                    "sha256": hashlib.sha256(control.read_bytes()).hexdigest(),
                },
            }
            token = verify_files(root, records)
            with self.assertRaisesRegex(ValueError, "SIDECAR_PRESENT"):
                NativeTransportCandidates(
                    root, "deltas/generation/transport.sqlite3", document,
                    verified_files=token, verified_records=records,
                )

    def test_parent_file_mutation_at_iterator_boundaries_is_detected_without_rehash(self):
        import ikarchive.verified_files as verified_files_module

        real_hash = verified_files_module._hash_descriptor
        with patch.object(verified_files_module, "_hash_descriptor", wraps=real_hash) as hasher:
            candidates = self.adapter.iter_table_candidates("typed_values", {})
            first = next(candidates)
            self.assertEqual(first[0:2], (0, 1))
            self.assertEqual(hasher.call_count, 0)
            candidates.close()
            self.assertEqual(hasher.call_count, 0)

        stale = self.adapter.iter_table_candidates("typed_values", {})
        self.control_path.write_text('{"fixture":false}\n', encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "VERIFIED_FILES_CHANGED"):
            next(stale)

    def test_mutation_during_yield_is_reported_by_close_and_connections_release(self):
        tracker = _ConnectionTracker()
        real_connect = sqlite3.connect

        def tracked_connect(*args, **kwargs):
            return _ConnectionProxy(real_connect(*args, **kwargs), tracker)

        # Restore the verified sibling after this test so tearDown remains simple.
        original_control = self.control_path.read_bytes()
        try:
            with patch.object(native_candidates_module.sqlite3, "connect", side_effect=tracked_connect):
                candidates = self.adapter.iter_table_candidates("typed_values", {})
                self.assertEqual(next(candidates)[0:2], (0, 1))
                self.control_path.write_bytes(original_control + b" ")
                with self.assertRaisesRegex(ValueError, "VERIFIED_FILES_CHANGED"):
                    candidates.close()
            self.assertEqual(tracker.connection_close_calls, 1)
            self.assertGreaterEqual(tracker.cursor_close_calls, 3)
        finally:
            self.control_path.write_bytes(original_control)

    def test_iterator_preserves_interrupt_when_cursor_or_connection_cleanup_fails(self):
        original = KeyboardInterrupt("native scan interrupted")
        close_error = RuntimeError("cursor close collision")
        tracker = _ConnectionTracker()
        real_connect = sqlite3.connect
        def faulting_connect(*args, **kwargs):
            connection = real_connect(*args, **kwargs)
            return _ConnectionProxy(
                connection, tracker, execute_error=original, close_error=close_error,
            )

        with patch.object(native_candidates_module.sqlite3, "connect", side_effect=faulting_connect):
            candidates = self.adapter.iter_table_candidates("typed_values", {})
            with self.assertRaises(KeyboardInterrupt) as caught:
                next(candidates)
        self.assertIs(caught.exception, original)
        self.assertEqual(tracker.connection_close_calls, 1)

    def test_callback_exception_categories_preserve_baseexception_over_cleanup_and_guard_errors(self):
        class CallbackAbort(BaseException):
            pass

        original = CallbackAbort("synthetic callback interruption")
        close_error = RuntimeError("synthetic close collision")
        tracker = _ConnectionTracker()
        real_connect = sqlite3.connect
        original_control = self.control_path.read_bytes()

        def faulting_connect(*args, **kwargs):
            connection = real_connect(*args, **kwargs)
            return _ConnectionProxy(connection, tracker, close_error=close_error)

        def ordinary_failure(_ordinal, _rowid):
            raise RuntimeError("synthetic private callback detail")

        def interrupt_after_file_drift(_ordinal, _rowid):
            self.control_path.write_bytes(original_control + b"changed")
            raise original

        try:
            with patch.object(
                native_candidates_module.sqlite3, "connect", side_effect=faulting_connect,
            ):
                with self.assertRaisesRegex(
                    ValueError, "CANDIDATE_FILTER_FAILED",
                ) as ordinary:
                    list(self.adapter.iter_table_candidates(
                        "typed_values", {}, candidate_row_filter=ordinary_failure,
                    ))
                self.assertEqual(str(ordinary.exception),
                                 "NATIVE_TRANSPORT_CANDIDATE_FILTER_FAILED")

                candidates = self.adapter.iter_table_candidates(
                    "typed_values", {}, candidate_row_filter=interrupt_after_file_drift,
                )
                with self.assertRaises(CallbackAbort) as caught:
                    next(candidates)
            self.assertIs(caught.exception, original)
            self.assertEqual(tracker.connection_close_calls, 2)
        finally:
            self.control_path.write_bytes(original_control)

    def test_constructor_setup_failure_closes_and_preserves_baseexception(self):
        original = SystemExit("native PRAGMA setup interrupted")

        class FailingConnection:
            def __init__(self):
                self.close_calls = 0

            def execute(self, _sql):
                raise original

            def close(self):
                self.close_calls += 1
                raise RuntimeError("close collision")

        connection = FailingConnection()
        with patch.object(native_candidates_module.sqlite3, "connect", return_value=connection):
            with self.assertRaises(SystemExit) as caught:
                NativeTransportCandidates(
                    self.root, "deltas/generation/transport.sqlite3", self.document,
                    verified_files=self.token, verified_records=self.records,
                )
        self.assertIs(caught.exception, original)
        self.assertEqual(connection.close_calls, 1)

    def test_invalid_inputs_close_connections_and_preserve_data_files(self):
        before = {
            path.relative_to(self.root).as_posix(): path.read_bytes()
            for path in (self.database_path, self.control_path)
        }
        before_sha = {name: hashlib.sha256(raw).hexdigest() for name, raw in before.items()}
        with self.assertRaisesRegex(ValueError, "TABLE_UNKNOWN"):
            list(self.adapter.iter_table_candidates("unknown", {}))
        with self.assertRaisesRegex(ValueError, "TABLE_UNKNOWN"):
            list(self.adapter.iter_table_candidates([], {}))
        with self.assertRaisesRegex(ValueError, "CANDIDATE_FILTER_RESULT_INVALID"):
            list(self.adapter.iter_table_candidates(
                "typed_values", {}, candidate_row_filter=lambda _ordinal, _rowid: 1,
            ))
        after = {
            path.relative_to(self.root).as_posix(): path.read_bytes()
            for path in (self.database_path, self.control_path)
        }
        self.assertEqual(after, before)
        self.assertEqual(
            {name: hashlib.sha256(raw).hexdigest() for name, raw in after.items()},
            before_sha,
        )


if __name__ == "__main__":
    unittest.main()
