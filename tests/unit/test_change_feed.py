import contextlib
import hashlib
import io
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src/python"))

from ikarchive.writer_guards import install_writer_guards

from ikarchive.change_feed import (
    CHANGE_TABLE,
    install_change_feed,
    read_change_batch,
)


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _trigger_name(table: str, operation: str) -> str:
    digest = hashlib.sha256((table + "\0" + operation).encode("utf-8", "surrogatepass")).hexdigest()
    return "ia_change_" + digest


class ChangeFeedTests(unittest.TestCase):
    def test_writer_contract_missing_or_removed_forces_full_reconciliation(self):
        conn = sqlite3.connect(":memory:")
        try:
            conn.execute("CREATE TABLE precious(value)")
            conn.commit()
            installed = install_change_feed(conn)
            self.assertFalse(installed["all_writers_contract_enforced"])
            with read_change_batch(conn, 0) as batch:
                self.assertTrue(batch.metadata["requires_baseline_reconciliation"])
                self.assertEqual(batch.metadata["writer_guard_status"]["status"], "missing")
            guards = install_writer_guards(conn)
            with read_change_batch(conn, 0) as batch:
                self.assertFalse(batch.metadata["requires_baseline_reconciliation"])
                self.assertTrue(batch.metadata["all_writers_contract_enforced"])
                # This read-only connection is OFF; database guards enforce
                # the contract separately on each actual writer connection.
                self.assertFalse(batch.metadata["writer_guard_status"]["connection_recursive_triggers"])
            conn.execute(f'DROP TRIGGER {_quote(guards["trigger_names"][0])}')
            with read_change_batch(conn, 0) as batch:
                self.assertTrue(batch.metadata["requires_baseline_reconciliation"])
                self.assertFalse(batch.metadata["all_writers_contract_enforced"])
        finally:
            conn.close()

    def test_idempotent_install_preserves_user_trigger_and_streams_all_operations(self):
        conn = sqlite3.connect(":memory:")
        try:
            table = 'quoted " records'
            conn.execute(
                f"CREATE TABLE {_quote(table)} ("
                f"{_quote('record " id')} INTEGER PRIMARY KEY, "
                f"{_quote('secret " column')} TEXT, "
                "unrecognized BLOB, "
                f"{_quote('derived " value')} TEXT GENERATED ALWAYS AS "
                f"({_quote('secret " column')} || ':' || {_quote('record " id')}) STORED)"
            )
            conn.execute('CREATE TABLE audit_seen(id)')
            user_trigger = (
                f'CREATE TRIGGER "user_keep" AFTER INSERT ON {_quote(table)} '
                f'BEGIN INSERT INTO audit_seen VALUES(NEW.{_quote("record \" id")}); END'
            )
            conn.execute(user_trigger)
            conn.execute(
                f"INSERT INTO {_quote(table)}({_quote('record " id')},{_quote('secret " column')},unrecognized) "
                "VALUES(?,?,?)",
                (1, "baseline-secret", b"baseline\x00blob"),
            )
            conn.commit()
            before_user_trigger = conn.execute(
                "SELECT sql FROM sqlite_master WHERE type='trigger' AND name='user_keep'"
            ).fetchone()[0]

            with contextlib.redirect_stdout(io.StringIO()) as output:
                first = install_change_feed(conn, include_row_counts=True)
                second = install_change_feed(conn, include_row_counts=True)
            self.assertEqual(output.getvalue(), "")
            self.assertTrue(first["initial_baseline_required"])
            self.assertTrue(first["requires_baseline_reconciliation"])
            self.assertTrue(first["schema_comparison_required"])
            self.assertTrue(first["source_schema_changed_by_installation"])
            self.assertTrue(first["feed_guard_schema_changed_by_installation"])
            self.assertEqual(
                first["feed_integrity_guard_status_before_installation"]["status"],
                "missing",
            )
            self.assertEqual(first["feed_integrity_guard_status"]["status"], "verified")
            self.assertEqual(
                first["feed_integrity_guard_status"]["expected_triggers"],
                ["ia_feed_guard_before_delete", "ia_feed_guard_before_update"],
            )
            self.assertEqual(
                first["writer_requirements"], {"recursive_triggers": True}
            )
            self.assertFalse(first["writer_requirements_verified"])
            self.assertIn("INSERT OR REPLACE", first["writer_requirements_note"])
            self.assertIn("do not set or verify", first["writer_requirements_note"])
            self.assertEqual(
                conn.execute("PRAGMA recursive_triggers").fetchone()[0], 0,
                "installer must preserve the caller's writer pragma",
            )
            self.assertEqual(
                set(first["source_table_columns"]),
                set(first["source_row_counts"]),
            )
            self.assertIn(CHANGE_TABLE, first["source_table_columns"])
            self.assertEqual(
                first["source_table_columns"][table][0],
                {
                    "cid": 0,
                    "name": 'record " id',
                    "type": "INTEGER",
                    "notnull": 0,
                    "dflt_value": None,
                    "pk": 1,
                    "hidden": 0,
                },
            )
            self.assertFalse(second["initial_baseline_required"])
            self.assertTrue(second["requires_baseline_reconciliation"])
            self.assertFalse(second["source_schema_changed_by_installation"])
            self.assertFalse(second["feed_guard_schema_changed_by_installation"])
            self.assertEqual(second["feed_integrity_guard_status"]["status"], "verified")
            self.assertEqual(second["source_schema_sha256"], first["source_schema_sha256"])
            self.assertFalse(first["all_writers_contract_enforced"])
            conn.execute("PRAGMA recursive_triggers=ON")
            install_writer_guards(conn)
            self.assertEqual(
                conn.execute(
                    "SELECT sql FROM sqlite_master WHERE type='trigger' AND name='user_keep'"
                ).fetchone()[0],
                before_user_trigger,
            )
            self.assertEqual(len(first["trigger_names"]), 6)
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type='trigger' AND name LIKE 'ia_change_%'"
                ).fetchone()[0],
                6,
            )
            guard_objects = [
                item for item in first["schemas"]
                if item["type"] == "trigger" and item["name"].startswith("ia_feed_guard_")
            ]
            self.assertEqual({item["name"] for item in guard_objects}, {
                "ia_feed_guard_before_update", "ia_feed_guard_before_delete"
            })
            self.assertTrue(all("RAISE(ABORT,'ARCHIVE_CHANGE_FEED_APPEND_ONLY')" in item["sql"] for item in guard_objects))
            self.assertTrue(set(item["name"] for item in guard_objects).isdisjoint(first["trigger_names"]))

            conn.execute(
                f"INSERT INTO {_quote(table)}({_quote('record " id')},{_quote('secret " column')},unrecognized) "
                "VALUES(?,?,?)",
                (2, "private-after-install", b"\x00\xff"),
            )
            conn.execute(f"UPDATE {_quote(table)} SET {_quote('secret " column')}=? WHERE {_quote('record " id')}=2", ("private-after-install",))
            conn.execute(
                f"UPDATE {_quote(table)} SET {_quote('record " id')}=3 WHERE {_quote('record " id')}=2"
            )
            conn.execute(f"DELETE FROM {_quote(table)} WHERE {_quote('record " id')}=1")

            with read_change_batch(conn, 0) as batch:
                self.assertTrue(conn.in_transaction)
                self.assertTrue(batch.metadata["schema_comparison_required"])
                self.assertFalse(batch.metadata["requires_baseline_reconciliation"])
                self.assertEqual(batch.metadata["feed_integrity_guard_status"]["status"], "verified")
                self.assertEqual(
                    batch.metadata["writer_requirements"], {"recursive_triggers": True}
                )
                self.assertFalse(batch.metadata["writer_requirements_verified"])
                self.assertIn("read-only batch", batch.metadata["writer_requirements_note"])
                self.assertEqual(
                    set(batch.metadata["source_table_columns"]),
                    set(batch.metadata["source_row_counts"]),
                )
                self.assertIn(CHANGE_TABLE, batch.metadata["source_table_columns"])
                events = list(batch.events())
                table_events = [item for item in events if item["table_name"] == table]
                self.assertEqual([item["operation"] for item in table_events], ["INSERT", "UPDATE", "UPDATE", "DELETE"])
                changes = list(batch.iter_current_changes())
                table_changes = [item for item in changes if item["table_name"] == table]
                by_identity = {item["source_rowid"]: item for item in table_changes}
                self.assertEqual(by_identity[1]["operation"], "delete")
                self.assertEqual(by_identity[2]["operation"], "delete")
                current = by_identity[3]
                self.assertEqual(current["operation"], "upsert")
                self.assertEqual(current["columns"], [
                    'record " id', 'secret " column', "unrecognized", 'derived " value'
                ])
                self.assertEqual(current["values"], (3, "private-after-install", b"\x00\xff", "private-after-install:3"))
                journal = [item for item in changes if item["table_name"] == CHANGE_TABLE]
                self.assertEqual(len(journal), len(events))
                self.assertEqual(batch.metadata["source_row_counts"][CHANGE_TABLE], len(events))
            self.assertEqual(
                conn.execute("PRAGMA recursive_triggers").fetchone()[0], 1,
                "read batch must not modify or claim knowledge of writer configuration",
            )
            self.assertTrue(conn.in_transaction)
        finally:
            conn.close()

    def test_feed_is_append_only_and_source_update_delete_still_record(self):
        conn = sqlite3.connect(":memory:")
        try:
            conn.execute("CREATE TABLE rows(id INTEGER PRIMARY KEY, value TEXT)")
            conn.executemany(
                "INSERT INTO rows VALUES(?,?)",
                [(1, "before-update"), (2, "before-delete")],
            )
            conn.commit()
            install_change_feed(conn)
            conn.execute("PRAGMA recursive_triggers=ON")
            install_writer_guards(conn)

            conn.execute("INSERT INTO rows VALUES(3,'feed-guard-probe')")
            with self.assertRaisesRegex(sqlite3.IntegrityError, "ARCHIVE_CHANGE_FEED_APPEND_ONLY"):
                conn.execute(
                    f"UPDATE {_quote(CHANGE_TABLE)} SET operation='DELETE' WHERE event_id=1"
                )
            with self.assertRaisesRegex(sqlite3.IntegrityError, "ARCHIVE_CHANGE_FEED_APPEND_ONLY"):
                conn.execute(f"DELETE FROM {_quote(CHANGE_TABLE)} WHERE event_id=1")
            self.assertEqual(
                conn.execute(f"SELECT operation FROM {_quote(CHANGE_TABLE)} WHERE event_id=1").fetchone()[0],
                "INSERT",
            )

            conn.execute("UPDATE rows SET value='after-update' WHERE id=1")
            conn.execute("DELETE FROM rows WHERE id=2")
            with read_change_batch(conn, 0) as batch:
                self.assertFalse(batch.metadata["requires_baseline_reconciliation"])
                self.assertEqual(batch.metadata["feed_integrity_guard_status"]["status"], "verified")
                events = [event for event in batch.events() if event["table_name"] == "rows"]
                self.assertEqual(
                    [(event["operation"], event["old_rowid"], event["new_rowid"]) for event in events],
                    [("INSERT", None, 3), ("UPDATE", 1, 1), ("DELETE", 2, None)],
                )
                changes = [
                    change for change in batch.iter_current_changes()
                    if change["table_name"] == "rows"
                ]
                self.assertEqual(
                    {change["source_rowid"]: change["operation"] for change in changes},
                    {1: "upsert", 2: "delete", 3: "upsert"},
                )
                self.assertEqual(
                    next(change["values"] for change in changes if change["source_rowid"] == 1),
                    (1, "after-update"),
                )
        finally:
            conn.close()

    def test_removed_or_mismatched_feed_guards_require_baseline_reconciliation(self):
        missing_conn = sqlite3.connect(":memory:")
        try:
            missing_conn.execute("CREATE TABLE rows(id INTEGER PRIMARY KEY, value)")
            install_change_feed(missing_conn)
            missing_conn.execute("DROP TRIGGER ia_feed_guard_before_update")
            with read_change_batch(missing_conn, 0) as batch:
                status = batch.metadata["feed_integrity_guard_status"]
                self.assertEqual(status["status"], "missing")
                self.assertEqual(status["missing_triggers"], ["ia_feed_guard_before_update"])
                self.assertEqual(status["mismatched_triggers"], [])
                self.assertTrue(batch.metadata["requires_baseline_reconciliation"])

            repaired = install_change_feed(missing_conn)
            self.assertEqual(repaired["feed_integrity_guard_status"]["status"], "verified")
            self.assertTrue(repaired["feed_guard_schema_changed_by_installation"])
            self.assertTrue(repaired["requires_baseline_reconciliation"])
        finally:
            missing_conn.close()

        mismatched_conn = sqlite3.connect(":memory:")
        try:
            mismatched_conn.execute("CREATE TABLE rows(id INTEGER PRIMARY KEY, value)")
            install_change_feed(mismatched_conn)
            mismatched_conn.execute("DROP TRIGGER ia_feed_guard_before_delete")
            wrong_sql = (
                "CREATE TRIGGER ia_feed_guard_before_delete BEFORE DELETE ON archive_change_feed "
                "BEGIN SELECT RAISE(ABORT,'WRONG_GUARD'); END"
            )
            mismatched_conn.execute(wrong_sql)
            with read_change_batch(mismatched_conn, 0) as batch:
                status = batch.metadata["feed_integrity_guard_status"]
                self.assertEqual(status["status"], "mismatched")
                self.assertEqual(status["missing_triggers"], [])
                self.assertEqual(status["mismatched_triggers"], ["ia_feed_guard_before_delete"])
                self.assertTrue(batch.metadata["requires_baseline_reconciliation"])

            with self.assertRaisesRegex(ValueError, "different identity SQL"):
                install_change_feed(mismatched_conn)
            preserved_sql = mismatched_conn.execute(
                "SELECT sql FROM sqlite_master WHERE type='trigger' AND name='ia_feed_guard_before_delete'"
            ).fetchone()[0]
            self.assertEqual(preserved_sql, wrong_sql)
        finally:
            mismatched_conn.close()

    def test_replace_tracks_old_delete_and_new_upsert_when_recursive_triggers_enabled(self):
        conn = sqlite3.connect(":memory:")
        try:
            conn.execute("PRAGMA recursive_triggers=ON")
            conn.execute(
                "CREATE TABLE rows(id INTEGER PRIMARY KEY, unique_value TEXT UNIQUE, payload TEXT)"
            )
            conn.executemany(
                "INSERT INTO rows VALUES(?,?,?)",
                [(1, "claimed", "old-row"), (2, "other", "untouched-row")],
            )
            conn.commit()
            installed = install_change_feed(conn)
            self.assertEqual(conn.execute("PRAGMA recursive_triggers").fetchone()[0], 1)
            self.assertEqual(installed["writer_requirements"], {"recursive_triggers": True})
            self.assertFalse(installed["writer_requirements_verified"])

            conn.execute("INSERT OR REPLACE INTO rows VALUES(3,'claimed','replacement-row')")
            with read_change_batch(conn, 0) as batch:
                self.assertEqual(batch.metadata["writer_requirements"], {"recursive_triggers": True})
                self.assertFalse(batch.metadata["writer_requirements_verified"])
                events = [event for event in batch.events() if event["table_name"] == "rows"]
                self.assertEqual(
                    [(event["operation"], event["old_rowid"], event["new_rowid"]) for event in events],
                    [("DELETE", 1, None), ("INSERT", None, 3)],
                )
                changes = [
                    change for change in batch.iter_current_changes()
                    if change["table_name"] == "rows"
                ]
                self.assertEqual(
                    {change["source_rowid"]: change["operation"] for change in changes},
                    {1: "delete", 3: "upsert"},
                )
                self.assertEqual(
                    next(change["values"] for change in changes if change["source_rowid"] == 3),
                    (3, "claimed", "replacement-row"),
                )
            self.assertEqual(conn.execute("PRAGMA recursive_triggers").fetchone()[0], 1)
            self.assertEqual(
                conn.execute("SELECT id,unique_value,payload FROM rows ORDER BY id").fetchall(),
                [(2, "other", "untouched-row"), (3, "claimed", "replacement-row")],
            )
        finally:
            conn.close()

    def test_typed_composite_and_shadowed_rowid_keys_roundtrip(self):
        conn = sqlite3.connect(":memory:")
        try:
            conn.execute(
                "CREATE TABLE composite_keys (payload, text_key, blob_key, value, "
                "PRIMARY KEY(text_key,blob_key)) WITHOUT ROWID"
            )
            conn.execute(
                "CREATE TABLE real_keys (payload, key REAL PRIMARY KEY) WITHOUT ROWID"
            )
            conn.execute(
                "CREATE TABLE shadowed (rowid, _rowid_, oid, key_value, payload, "
                "PRIMARY KEY(key_value))"
            )
            conn.execute(
                "INSERT INTO composite_keys VALUES(?,?,?,?)",
                ("payload-before", 1, b"\x00\xff", "before"),
            )
            real_key = float.fromhex("0x1.fffffffffffffp+100")
            conn.execute("INSERT INTO real_keys VALUES(?,?)", ("real-payload", real_key))
            conn.execute(
                "INSERT INTO shadowed VALUES(?,?,?,?,?)",
                ("row", "_rowid_", "oid", "shadowed-key", "payload"),
            )
            conn.commit()
            install_change_feed(conn)
            conn.execute(
                "UPDATE composite_keys SET text_key=?,value=? WHERE text_key IS ? AND blob_key IS ?",
                ("one\x00'quoted", "after", 1, b"\x00\xff"),
            )
            conn.execute(
                "UPDATE real_keys SET key=? WHERE key IS ?",
                (real_key / 3.0, real_key),
            )
            conn.execute(
                "UPDATE shadowed SET key_value=? WHERE key_value IS ?",
                ("changed", "shadowed-key"),
            )

            with read_change_batch(conn, 0) as batch:
                events = list(batch.events())
                old_key = next(event["old_key_json"] for event in events if event["table_name"] == "composite_keys")
                new_key = next(event["new_key_json"] for event in events if event["table_name"] == "composite_keys")
                decoded_old = json.loads(old_key)
                decoded_new = json.loads(new_key)
                self.assertEqual(decoded_old[0], {"column": "text_key", "sqlite_type": "integer", "value": "1"})
                self.assertEqual(decoded_old[1], {"column": "blob_key", "sqlite_type": "blob", "value": "00FF"})
                self.assertEqual(decoded_new[0], {"column": "text_key", "sqlite_type": "text", "value": "one\u0000'quoted"})
                rows = list(batch.iter_current_changes())
                composite = [item for item in rows if item["table_name"] == "composite_keys"]
                self.assertEqual([item["operation"] for item in composite], ["delete", "upsert"])
                self.assertEqual(composite[0]["values"], None)
                self.assertEqual(composite[1]["values"], (
                    "payload-before", "one\x00'quoted", b"\x00\xff", "after"
                ))
                shadowed = [item for item in rows if item["table_name"] == "shadowed"]
                self.assertEqual(sorted(item["operation"] for item in shadowed), ["delete", "upsert"])
                deleted_shadowed = next(item for item in shadowed if item["operation"] == "delete")
                inserted_shadowed = next(item for item in shadowed if item["operation"] == "upsert")
                self.assertEqual(json.loads(deleted_shadowed["identity_json"])[0]["value"], "shadowed-key")
                self.assertEqual(json.loads(inserted_shadowed["identity_json"])[0]["value"], "changed")
                real_changes = [item for item in rows if item["table_name"] == "real_keys"]
                self.assertEqual(sorted(item["operation"] for item in real_changes), ["delete", "upsert"])
                real_upsert = next(item for item in real_changes if item["operation"] == "upsert")
                self.assertEqual(real_upsert["values"], ("real-payload", real_key / 3.0))
        finally:
            conn.close()

    def test_system_tables_are_streamed_as_clear_and_replace(self):
        conn = sqlite3.connect(":memory:")
        try:
            conn.execute("CREATE TABLE auto_rows(id INTEGER PRIMARY KEY AUTOINCREMENT, content)")
            conn.execute("INSERT INTO auto_rows(content) VALUES('before')")
            conn.execute("ANALYZE")
            conn.commit()
            installed = install_change_feed(conn)
            self.assertIn("sqlite_sequence", installed["replace_tables"])
            self.assertIn("sqlite_stat1", installed["replace_tables"])

            conn.execute("INSERT INTO auto_rows(content) VALUES('after')")
            with read_change_batch(conn, 0) as batch:
                self.assertIn("sqlite_sequence", batch.metadata["replace_tables"])
                self.assertIn("sqlite_stat1", batch.metadata["replace_tables"])
                changes = list(batch.iter_current_changes())
                sequence = [item for item in changes if item["table_name"] == "sqlite_sequence"]
                self.assertEqual(sequence[0]["operation"], "clear_table")
                self.assertEqual(sequence[0]["values"], None)
                self.assertEqual(sequence[1]["operation"], "upsert")
                self.assertEqual(sequence[1]["values"], ("auto_rows", 2))
                self.assertEqual(batch.metadata["source_row_counts"]["sqlite_sequence"], 1)
                journal = [item for item in changes if item["table_name"] == CHANGE_TABLE]
                self.assertEqual(len(journal), batch.metadata["through_event_id"])
        finally:
            conn.close()

    def test_rollback_and_outer_transaction_atomicity(self):
        conn = sqlite3.connect(":memory:")
        try:
            conn.execute("CREATE TABLE rows(id INTEGER PRIMARY KEY, value)")
            conn.commit()
            conn.execute("BEGIN")
            metadata = install_change_feed(conn)
            self.assertTrue(conn.in_transaction)
            self.assertTrue(metadata["initial_baseline_required"])
            conn.rollback()
            self.assertIsNone(conn.execute(
                "SELECT 1 FROM sqlite_master WHERE name=?", (CHANGE_TABLE,)
            ).fetchone())

            install_change_feed(conn)
            conn.commit()
            conn.execute("BEGIN")
            conn.execute("INSERT INTO rows VALUES(1,'rolled back')")
            self.assertEqual(conn.execute(f"SELECT COUNT(*) FROM {_quote(CHANGE_TABLE)}").fetchone()[0], 1)
            conn.rollback()
            self.assertEqual(conn.execute(f"SELECT COUNT(*) FROM {_quote(CHANGE_TABLE)}").fetchone()[0], 0)
            with read_change_batch(conn, 0) as batch:
                self.assertEqual(list(batch.events()), [])
                self.assertEqual(list(batch.iter_current_changes()), [])
        finally:
            conn.close()

    def test_atomic_failure_and_identityless_table_fail_without_partial_install(self):
        conn = sqlite3.connect(":memory:")
        try:
            conn.execute("CREATE TABLE rows(id INTEGER PRIMARY KEY, value)")
            collision_name = _trigger_name("rows", "INSERT")
            conn.execute(
                f'CREATE TRIGGER {_quote(collision_name)} AFTER INSERT ON rows BEGIN SELECT 1; END'
            )
            conn.commit()
            with self.assertRaisesRegex(ValueError, "identity SQL"):
                install_change_feed(conn)
            self.assertIsNone(conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (CHANGE_TABLE,)
            ).fetchone())
            self.assertIsNotNone(conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='trigger' AND name=?", (collision_name,)
            ).fetchone())

            conn.execute("DROP TRIGGER " + _quote(collision_name))
            conn.execute("CREATE TABLE identityless(rowid,_rowid_,oid,value)")
            conn.commit()
            with self.assertRaisesRegex(ValueError, "no usable rowid or primary key"):
                install_change_feed(conn)
            self.assertIsNone(conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (CHANGE_TABLE,)
            ).fetchone())
        finally:
            conn.close()

    def test_reserved_name_collision_and_virtual_tables_are_explicit_failures(self):
        conn = sqlite3.connect(":memory:")
        try:
            conn.execute("CREATE TABLE archive_change_feed(private_data)")
            conn.execute("INSERT INTO archive_change_feed VALUES('keep')")
            conn.commit()
            with self.assertRaisesRegex(ValueError, "different identity SQL"):
                install_change_feed(conn)
            self.assertEqual(conn.execute("SELECT * FROM archive_change_feed").fetchall(), [("keep",)])
        finally:
            conn.close()

        virtual = sqlite3.connect(":memory:")
        try:
            virtual.execute("CREATE VIRTUAL TABLE docs USING fts5(body)")
            with self.assertRaisesRegex(ValueError, "virtual/shadow"):
                install_change_feed(virtual)
            self.assertIsNone(virtual.execute(
                "SELECT 1 FROM sqlite_master WHERE name=?", (CHANGE_TABLE,)
            ).fetchone())
        finally:
            virtual.close()

    def test_snapshot_highwater_excludes_concurrent_writer_until_next_batch(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "snapshot.sqlite3"
            writer = sqlite3.connect(path)
            writer.execute("PRAGMA journal_mode=WAL")
            writer.execute("CREATE TABLE rows(id INTEGER PRIMARY KEY, value)")
            install_change_feed(writer)
            writer.execute("INSERT INTO rows VALUES(1,'before')")
            writer.commit()

            reader = sqlite3.connect(path)
            try:
                with read_change_batch(reader, 0) as batch:
                    pinned_highwater = batch.metadata["through_event_id"]
                    self.assertEqual(pinned_highwater, 1)
                    writer.execute("INSERT INTO rows VALUES(2,'private-later-value')")
                    writer.commit()
                    self.assertEqual([event["event_id"] for event in batch.events()], [1])
                    changes = list(batch.iter_current_changes())
                    row_changes = [item for item in changes if item["table_name"] == "rows"]
                    self.assertEqual([item["values"] for item in row_changes], [(1, "before")])
                    self.assertNotIn("private-later-value", repr(changes))
                with read_change_batch(reader, pinned_highwater) as next_batch:
                    self.assertEqual(next_batch.metadata["through_event_id"], 2)
                    self.assertEqual([event["event_id"] for event in next_batch.events()], [2])
                    rows = [item for item in next_batch.iter_current_changes() if item["table_name"] == "rows"]
                    self.assertEqual(rows[0]["values"], (2, "private-later-value"))
            finally:
                reader.close()
                writer.close()

    def test_new_table_and_schema_hash_signal_reconciliation(self):
        conn = sqlite3.connect(":memory:")
        try:
            conn.execute("CREATE TABLE original(id INTEGER PRIMARY KEY, value)")
            conn.commit()
            installed = install_change_feed(conn)
            initial_schema_sha = installed["source_schema_sha256"]
            conn.execute("ALTER TABLE original ADD COLUMN later")
            conn.execute("CREATE TABLE added_without_feed(id INTEGER PRIMARY KEY, value)")
            conn.commit()
            with read_change_batch(conn, 0) as batch:
                self.assertTrue(batch.metadata["schema_comparison_required"])
                self.assertNotEqual(batch.metadata["source_schema_sha256"], initial_schema_sha)
                self.assertTrue(batch.metadata["requires_baseline_reconciliation"])
                self.assertIn("added_without_feed", batch.metadata["untracked_tables"])
                self.assertIn("added_without_feed", batch.metadata["source_row_counts"])
        finally:
            conn.close()

    def test_sqlite_row_factory_is_supported_and_iterators_require_context(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("CREATE TABLE rows(id INTEGER PRIMARY KEY, value)")
            install_change_feed(conn)
            conn.execute("INSERT INTO rows VALUES(1,'value')")
            batch = read_change_batch(conn, 0)
            with batch:
                self.assertEqual(len(list(batch.events())), 1)
                rows = list(batch.iter_current_changes())
                self.assertIn((1, "value"), [row["values"] for row in rows])
            with self.assertRaisesRegex(RuntimeError, "only inside"):
                list(batch.events())
        finally:
            conn.close()

    def test_replacement_primary_key_identity_uses_actual_column_positions(self):
        conn = sqlite3.connect(":memory:")
        try:
            conn.execute(
                "CREATE TABLE rows(payload TEXT, key INTEGER, PRIMARY KEY(key)) WITHOUT ROWID"
            )
            conn.execute("INSERT INTO rows VALUES('body', 47)")
            conn.commit()
            install_change_feed(conn)
            with read_change_batch(conn, 0) as batch:
                # Exercise the same clear-and-replace path used for SQLite
                # system tables against a PK that is not the first column.
                batch.metadata["replace_tables"] = ["rows"]
                changes = list(batch.iter_current_changes())
                self.assertEqual(changes[0]["operation"], "clear_table")
                self.assertEqual(changes[1]["columns"], ["payload", "key"])
                self.assertEqual(changes[1]["values"], ("body", 47))
                self.assertEqual(
                    json.loads(changes[1]["identity_json"]),
                    [{"column": "key", "sqlite_type": "integer", "value": "47"}],
                )
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
