import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src/python"))

from ikarchive.writer_guards import install_writer_guards
from ikarchive.change_feed import install_change_feed, read_change_batch
from ikarchive.delta_transport import (
    DeltaTransportError,
    build_delta_database,
    iter_delta_records,
    verify_delta_database,
)
from ikarchive.reconciliation import read_reconciliation


def _generation(suffix="0123abcd"):
    return f"20261002T121212Z-{suffix}"


def _change_connection():
    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE records (payload, optional_value, key_blob, key_integer, "
        "largest_integer, ratio, PRIMARY KEY(key_blob,key_integer)) WITHOUT ROWID"
    )
    conn.execute("CREATE TABLE empty_table (value)")
    conn.execute("CREATE TABLE parents (id PRIMARY KEY)")
    conn.execute("CREATE TABLE children (parent_id REFERENCES parents(id))")
    conn.execute(
        "INSERT INTO records VALUES (?,?,?,?,?,?)",
        ("before\x00text", None, b"\x00\xff", 1, (1 << 63) - 1, -0.0),
    )
    conn.execute(
        "INSERT INTO records VALUES (?,?,?,?,?,?)",
        (b"\xffpayload", "remove", b"delete", -9, -(1 << 63), float("inf")),
    )
    conn.commit()
    install_change_feed(conn)
    conn.execute("PRAGMA recursive_triggers=ON")
    install_writer_guards(conn)
    conn.execute(
        "UPDATE records SET payload=?,optional_value=?,key_blob=?,key_integer=? "
        "WHERE key_blob=? AND key_integer=?",
        (b"\x00\xffafter", "new\x00value", b"new\xff", 2, b"\x00\xff", 1),
    )
    conn.execute("DELETE FROM records WHERE key_blob=? AND key_integer=?", (b"delete", -9))
    return conn


def _feed_metadata(batch, suffix="0123abcd"):
    metadata = dict(batch.metadata)
    metadata.update(
        kind="change_feed",
        generation_id=_generation(suffix),
        parent_generation_id=None,
        baseline_generation_id=None,
        captured_at="2026-10-02T12:00:00Z",
    )
    return metadata


def _same_records(test, actual, expected):
    test.assertEqual(len(actual), len(expected))
    for actual_record, expected_record in zip(actual, expected):
        test.assertEqual(
            {key: value for key, value in actual_record.items() if key != "values"},
            {key: value for key, value in expected_record.items() if key != "values"},
        )
        actual_values, expected_values = actual_record["values"], expected_record["values"]
        if actual_values is None or expected_values is None:
            test.assertIs(actual_values, expected_values)
            continue
        test.assertEqual(len(actual_values), len(expected_values))
        for left, right in zip(actual_values, expected_values):
            test.assertIs(type(left), type(right))
            if type(left) is float:
                test.assertEqual(left.hex(), right.hex())
            else:
                test.assertEqual(left, right)


class DeltaTransportTests(unittest.TestCase):
    def test_change_feed_delta_roundtrips_typed_rows_and_nonleading_composite_key(self):
        conn = _change_connection()
        try:
            with tempfile.TemporaryDirectory() as temporary:
                database = Path(temporary).resolve() / "delta.sqlite3"
                with read_change_batch(conn, 0) as batch:
                    metadata = _feed_metadata(batch)
                    expected = list(batch.iter_current_changes())
                    document = build_delta_database(iter(expected), metadata, database)
                    receipt = verify_delta_database(conn, database, document)
                    actual = list(iter_delta_records(database, document))

                    self.assertEqual(receipt["status"], "verified")
                    self.assertEqual(receipt["database"], document["database"])
                    self.assertEqual(document["source_table_columns"], metadata["source_table_columns"])
                    self.assertEqual(document["source_row_counts"], metadata["source_row_counts"])
                    self.assertEqual(metadata["source_foreign_keys"]["children"], [{
                        "id": 0, "seq": 0, "table": "parents", "from": "parent_id", "to": "id",
                        "on_update": "NO ACTION", "on_delete": "NO ACTION", "match": "NONE",
                    }])
                    self.assertEqual(document["operation_counts"], {
                        "upsert": 3, "delete": 2, "clear_table": 0,
                    })
                    self.assertEqual(stat_mode(database), 0o600)
                    self.assertFalse(Path(str(database) + "-journal").exists())
                    self.assertFalse(Path(str(database) + "-wal").exists())
                    _same_records(self, actual, expected)

                    row_changes = [record for record in actual if record["table_name"] == "records"]
                    self.assertCountEqual([record["operation"] for record in row_changes], ["upsert", "delete", "delete"])
                    current = next(record for record in row_changes if record["operation"] == "upsert")
                    self.assertEqual(current["values"][0], b"\x00\xffafter")
                    self.assertEqual(current["values"][1], "new\x00value")
                    self.assertEqual(current["values"][4], (1 << 63) - 1)
                    self.assertEqual(current["values"][5].hex(), "-0x0.0p+0")
                    identity = json.loads(current["identity_json"])
                    self.assertEqual([item["column"] for item in identity], ["key_blob", "key_integer"])
                    self.assertEqual([item["value"] for item in identity], ["6E6577FF", "2"])
        finally:
            conn.close()

    def test_sqlite_sequence_and_statistics_tables_are_retained(self):
        conn = sqlite3.connect(":memory:")
        try:
            conn.execute("CREATE TABLE auto_rows(id INTEGER PRIMARY KEY AUTOINCREMENT, value)")
            conn.execute("INSERT INTO auto_rows(value) VALUES('before')")
            conn.execute("ANALYZE")
            conn.commit()
            install_change_feed(conn)
            conn.execute("PRAGMA recursive_triggers=ON")
            install_writer_guards(conn)
            conn.execute("INSERT INTO auto_rows(value) VALUES('after')")
            with tempfile.TemporaryDirectory() as temporary:
                database = Path(temporary).resolve() / "delta.sqlite3"
                with read_change_batch(conn, 0) as batch:
                    metadata = _feed_metadata(batch, "0123abce")
                    expected = list(batch.iter_current_changes())
                    document = build_delta_database(iter(expected), metadata, database)
                    receipt = verify_delta_database(conn, database, document)
                    actual = list(iter_delta_records(database, document))
                    self.assertEqual(receipt["status"], "verified")
                    self.assertTrue({"sqlite_sequence", "sqlite_stat1", "sqlite_stat4"} <= set(document["table_names"]))
                    self.assertEqual(document["source_row_counts"]["sqlite_sequence"], 1)
                    sequence = [record for record in actual if record["table_name"] == "sqlite_sequence"]
                    self.assertEqual([record["operation"] for record in sequence], ["clear_table", "upsert"])
                    self.assertEqual(sequence[1]["values"], ("auto_rows", 2))
                    _same_records(self, actual, expected)
        finally:
            conn.close()

    def test_reconciliation_requires_baseline_and_detects_an_omitted_change(self):
        current = sqlite3.connect(":memory:")
        baseline = sqlite3.connect(":memory:")
        try:
            current.executescript(
                "CREATE TABLE rows(id INTEGER PRIMARY KEY, value);"
                "INSERT INTO rows VALUES(1,'old'),(2,'deleted');"
                "CREATE TABLE key_only(payload, key TEXT PRIMARY KEY) WITHOUT ROWID;"
                "INSERT INTO key_only VALUES(X'00FF','key-a');"
            )
            current.commit()
            current.backup(baseline)
            current.execute("UPDATE rows SET value='new' WHERE id=1")
            current.execute("DELETE FROM rows WHERE id=2")
            current.execute("UPDATE key_only SET payload=X'FF00'")
            current.commit()
            install_change_feed(current)

            with tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary).resolve()
                incomplete_path = root / "incomplete.sqlite3"
                complete_path = root / "complete.sqlite3"
                with read_reconciliation(current, baseline) as batch:
                    metadata = dict(batch.metadata)
                    metadata.update(
                        generation_id=_generation("0123abcf"),
                        parent_generation_id=None,
                        baseline_generation_id=None,
                    )
                    complete_records = list(batch.iter_current_changes())
                    removed = False
                    incomplete_records = []
                    for record in complete_records:
                        if not removed and record["table_name"] == "rows" and record["operation"] == "upsert":
                            removed = True
                            continue
                        incomplete_records.append(record)
                    self.assertTrue(removed)
                    incomplete_doc = build_delta_database(iter(incomplete_records), metadata, incomplete_path)
                    complete_metadata = dict(metadata)
                    complete_metadata["generation_id"] = _generation("0123abd0")
                    complete_doc = build_delta_database(iter(complete_records), complete_metadata, complete_path)

                with self.assertRaises(DeltaTransportError) as missing_baseline:
                    verify_delta_database(current, complete_path, complete_doc)
                self.assertEqual(missing_baseline.exception.code, "BASELINE_CONNECTION_REQUIRED")
                self.assertEqual(missing_baseline.exception.receipt["created"], False)

                current.execute("BEGIN")
                baseline.execute("BEGIN")
                with self.assertRaises(DeltaTransportError) as missing_change:
                    verify_delta_database(current, incomplete_path, incomplete_doc, baseline_conn=baseline)
                self.assertEqual(missing_change.exception.code, "RECONCILIATION_STREAM_MISMATCH")
                self.assertTrue(current.in_transaction)
                self.assertTrue(baseline.in_transaction)

                receipt = verify_delta_database(current, complete_path, complete_doc, baseline_conn=baseline)
                self.assertEqual(receipt["status"], "verified")
                self.assertTrue(current.in_transaction)
                self.assertTrue(baseline.in_transaction)
                current.rollback()
                baseline.rollback()
        finally:
            current.close()
            baseline.close()

    def test_failed_build_retains_receipt_and_never_overwrites_existing_target(self):
        conn = _change_connection()
        try:
            with tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary).resolve()
                with read_change_batch(conn, 0) as batch:
                    metadata = _feed_metadata(batch, "0123abd1")
                    records = list(batch.iter_current_changes())
                    original = next(record for record in records
                                    if record["table_name"] == "records" and record["operation"] == "upsert")
                    duplicate_path = root / "duplicate.sqlite3"
                    with self.assertRaises(DeltaTransportError) as duplicate:
                        build_delta_database(iter([original, original]), metadata, duplicate_path)
                    self.assertEqual(duplicate.exception.code, "DUPLICATE_IDENTITY")
                    self.assertEqual(duplicate.exception.receipt["status"], "failed")
                    self.assertEqual(duplicate.exception.receipt["created_path"], str(duplicate_path))
                    self.assertTrue(duplicate_path.is_file())

                    alternate = dict(original)
                    alternate_identity = json.loads(original["identity_json"])
                    alternate_identity[0]["value"] = alternate_identity[0]["value"].lower()
                    alternate["identity_json"] = json.dumps(
                        alternate_identity, ensure_ascii=False, sort_keys=True, separators=(",", ":")
                    )
                    equivalent_path = root / "equivalent-identity.sqlite3"
                    with self.assertRaises(DeltaTransportError) as equivalent:
                        build_delta_database(iter([original, alternate]), metadata, equivalent_path)
                    self.assertEqual(equivalent.exception.code, "DUPLICATE_IDENTITY")

                    existing_path = root / "existing.sqlite3"
                    existing_path.write_bytes(b"retain these bytes")
                    with self.assertRaises(DeltaTransportError) as existing:
                        build_delta_database(iter(records), metadata, existing_path)
                    self.assertEqual(existing.exception.code, "OUTPUT_ALREADY_EXISTS")
                    self.assertFalse(existing.exception.receipt["created"])
                    self.assertNotIn("created_path", existing.exception.receipt)
                    self.assertEqual(existing_path.read_bytes(), b"retain these bytes")
        finally:
            conn.close()

    def test_malformed_records_fail_with_created_artifact_receipts(self):
        conn = _change_connection()
        try:
            with tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary).resolve()
                with read_change_batch(conn, 0) as batch:
                    metadata = _feed_metadata(batch, "0123abd5")
                    original = next(record for record in batch.iter_current_changes()
                                    if record["table_name"] == "records" and record["operation"] == "upsert")
                    cases = []
                    missing = dict(original)
                    missing.pop("columns")
                    cases.append(("missing", missing, "DELTA_RECORD_INVALID"))
                    unknown = dict(original)
                    unknown["table_name"] = "unlisted_table"
                    cases.append(("unknown", unknown, "DELTA_RECORD_INVALID"))
                    columns = dict(original)
                    columns["columns"] = ["wrong"]
                    cases.append(("columns", columns, "DELTA_COLUMNS_MISMATCH"))
                    values = dict(original)
                    values["values"] = ["list is not a typed row tuple"]
                    cases.append(("values", values, "DELTA_VALUES_INVALID"))
                    for name, record, expected_code in cases:
                        target = root / f"{name}.sqlite3"
                        with self.assertRaises(DeltaTransportError) as failure:
                            build_delta_database(iter([record]), metadata, target)
                        self.assertEqual(failure.exception.code, expected_code)
                        self.assertEqual(failure.exception.receipt["status"], "failed")
                        self.assertEqual(failure.exception.receipt["created_path"], str(target))
                        self.assertTrue(target.is_file())
        finally:
            conn.close()

    def test_bad_paths_and_boolean_event_ids_are_rejected_before_creation(self):
        conn = _change_connection()
        try:
            with tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary).resolve()
                real_parent = root / "real"
                real_parent.mkdir()
                link_parent = root / "linked"
                link_parent.symlink_to(real_parent, target_is_directory=True)
                with self.assertRaises(DeltaTransportError) as symlink:
                    build_delta_database([], {}, link_parent / "new.sqlite3")
                self.assertEqual(symlink.exception.code, "OUTPUT_PATH_SYMLINK")

                with self.assertRaises(DeltaTransportError) as traversal:
                    build_delta_database([], {}, root / "child" / ".." / "outside.sqlite3")
                self.assertEqual(traversal.exception.code, "OUTPUT_PATH_INVALID")

                with read_change_batch(conn, 0) as batch:
                    metadata = _feed_metadata(batch, "0123abd2")
                    metadata["after_event_id"] = True
                    target = root / "boolean.sqlite3"
                    with self.assertRaises(DeltaTransportError) as bad_id:
                        build_delta_database([], metadata, target)
                    self.assertEqual(bad_id.exception.code, "EVENT_RANGE_INVALID")
                    self.assertFalse(target.exists())
        finally:
            conn.close()

    def test_transport_hash_mismatch_is_rejected(self):
        conn = _change_connection()
        try:
            with tempfile.TemporaryDirectory() as temporary:
                database = Path(temporary).resolve() / "delta.sqlite3"
                with read_change_batch(conn, 0) as batch:
                    metadata = _feed_metadata(batch, "0123abd3")
                    expected = list(batch.iter_current_changes())
                    document = build_delta_database(iter(expected), metadata, database)
                    writer = sqlite3.connect(database)
                    try:
                        writer.execute('CREATE TABLE "unexpected" (value)')
                        writer.commit()
                    finally:
                        writer.close()
                    with self.assertRaises(DeltaTransportError) as mismatch:
                        verify_delta_database(conn, database, document)
                    self.assertEqual(mismatch.exception.code, "TRANSPORT_DATABASE_MISMATCH")
                    self.assertFalse(mismatch.exception.receipt["created"])
        finally:
            conn.close()

    def test_new_source_events_after_build_cannot_verify_against_old_delta(self):
        conn = _change_connection()
        try:
            with tempfile.TemporaryDirectory() as temporary:
                database = Path(temporary).resolve() / "delta.sqlite3"
                with read_change_batch(conn, 0) as batch:
                    metadata = _feed_metadata(batch, "0123abd6")
                    records = list(batch.iter_current_changes())
                    document = build_delta_database(iter(records), metadata, database)
                conn.execute("UPDATE records SET payload='later' WHERE key_blob=? AND key_integer=?",
                             (b"new\xff", 2))
                conn.commit()
                with read_change_batch(conn, 0):
                    with self.assertRaises(DeltaTransportError) as mismatch:
                        verify_delta_database(conn, database, document)
                    self.assertEqual(mismatch.exception.code, "SOURCE_ROW_COUNTS_MISMATCH")
                    self.assertTrue(conn.in_transaction)
        finally:
            conn.close()

    def test_change_feed_verification_rejects_an_omitted_record(self):
        conn = _change_connection()
        try:
            with tempfile.TemporaryDirectory() as temporary:
                database = Path(temporary).resolve() / "delta.sqlite3"
                with read_change_batch(conn, 0) as batch:
                    metadata = _feed_metadata(batch, "0123abd7")
                    records = list(batch.iter_current_changes())
                self.assertGreater(len(records), 1)
                document = build_delta_database(iter(records[:-1]), metadata, database)
                with self.assertRaises(DeltaTransportError) as mismatch:
                    verify_delta_database(conn, database, document)
                self.assertEqual(mismatch.exception.code, "CHANGE_FEED_MISMATCH")
        finally:
            conn.close()

    def test_foreign_key_snapshot_is_required_and_bound_to_source(self):
        conn = _change_connection()
        try:
            with tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary).resolve()
                with read_change_batch(conn, 0) as batch:
                    metadata = _feed_metadata(batch, "0123abd4")
                    records = list(batch.iter_current_changes())

                    invalid_metadata = dict(metadata)
                    invalid_metadata["source_foreign_keys"] = dict(metadata["source_foreign_keys"])
                    invalid_metadata["source_foreign_keys"]["children"] = [{
                        "id": True, "seq": 0, "table": "parents", "from": "parent_id", "to": "id",
                        "on_update": "NO ACTION", "on_delete": "NO ACTION", "match": "NONE",
                    }]
                    with self.assertRaises(DeltaTransportError) as invalid:
                        build_delta_database(iter(records), invalid_metadata, root / "invalid.sqlite3")
                    self.assertEqual(invalid.exception.code, "SOURCE_FOREIGN_KEYS_INVALID")
                    self.assertFalse((root / "invalid.sqlite3").exists())

                    forged_metadata = dict(metadata)
                    forged_metadata["source_foreign_keys"] = dict(metadata["source_foreign_keys"])
                    forged_metadata["source_foreign_keys"]["children"] = []
                    database = root / "forged.sqlite3"
                    document = build_delta_database(iter(records), forged_metadata, database)
                    with self.assertRaises(DeltaTransportError) as mismatch:
                        verify_delta_database(conn, database, document)
                    self.assertEqual(mismatch.exception.code, "SOURCE_FOREIGN_KEYS_MISMATCH")
        finally:
            conn.close()


def stat_mode(path: Path) -> int:
    return path.stat().st_mode & 0o777


if __name__ == "__main__":
    unittest.main()
