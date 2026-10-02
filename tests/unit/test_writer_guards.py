import hashlib
from pathlib import Path
import sqlite3
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src/python"))

from ikarchive.change_feed import CHANGE_TABLE, install_change_feed
from ikarchive.writer_guards import install_writer_guards, inspect_writer_guards


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _guard_name(table: str, operation: str) -> str:
    digest = hashlib.sha256((table + "\0" + operation).encode("utf-8", "surrogatepass")).hexdigest()
    return "ia_writer_guard_" + digest


class WriterGuardTests(unittest.TestCase):
    def test_recursive_off_writes_and_replace_are_rejected_without_source_or_feed_changes(self):
        conn = sqlite3.connect(":memory:")
        try:
            conn.execute("CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT, value TEXT)")
            conn.execute("INSERT INTO records VALUES(1,'original')")
            conn.commit()
            install_change_feed(conn)
            receipt = install_writer_guards(conn)
            self.assertTrue(receipt["all_writers_contract_enforced"])
            self.assertFalse(receipt["connection_recursive_triggers"])
            self.assertEqual(receipt["guarded_tables"], ["records"])
            self.assertNotIn(CHANGE_TABLE, receipt["guarded_tables"])
            self.assertNotIn("sqlite_sequence", receipt["guarded_tables"])

            original_events = conn.execute(
                f"SELECT COUNT(*) FROM {_quote(CHANGE_TABLE)}"
            ).fetchone()[0]
            with self.assertRaisesRegex(
                sqlite3.IntegrityError,
                "ARCHIVE_CHANGE_FEED_RECURSIVE_TRIGGERS_REQUIRED",
            ):
                conn.execute("UPDATE records SET value='must-not-commit' WHERE id=1")
            with self.assertRaisesRegex(
                sqlite3.IntegrityError,
                "ARCHIVE_CHANGE_FEED_RECURSIVE_TRIGGERS_REQUIRED",
            ):
                conn.execute("INSERT OR REPLACE INTO records VALUES(1,'replacement')")

            self.assertEqual(conn.execute("SELECT id,value FROM records").fetchall(), [(1, "original")])
            self.assertEqual(
                conn.execute(f"SELECT COUNT(*) FROM {_quote(CHANGE_TABLE)}").fetchone()[0],
                original_events,
            )

            conn.execute("PRAGMA recursive_triggers=ON")
            conn.execute("INSERT INTO records VALUES(2,'inserted')")
            conn.execute("UPDATE records SET value='updated' WHERE id=2")
            conn.execute("DELETE FROM records WHERE id=2")
            conn.execute("INSERT OR REPLACE INTO records VALUES(1,'replaced')")
            events = conn.execute(
                f"SELECT operation,old_rowid,new_rowid FROM {_quote(CHANGE_TABLE)} "
                "WHERE table_name='records' ORDER BY event_id"
            ).fetchall()
            self.assertEqual(
                events,
                [
                    ("INSERT", None, 2),
                    ("UPDATE", 2, 2),
                    ("DELETE", 2, None),
                    ("DELETE", 1, None),
                    ("INSERT", None, 1),
                ],
            )
            self.assertTrue(inspect_writer_guards(conn)["connection_recursive_triggers"])
        finally:
            conn.close()

    def test_canonical_sorted_contract_for_22_tables_and_idempotent_install(self):
        conn = sqlite3.connect(":memory:")
        try:
            for index in range(22):
                conn.execute(f"CREATE TABLE source_{index:02d}(id INTEGER PRIMARY KEY, value TEXT)")
            conn.commit()

            first = install_writer_guards(conn)
            self.assertEqual(first["status"], "verified")
            self.assertTrue(first["all_writers_contract_enforced"])
            self.assertEqual(first["guarded_tables"], [f"source_{i:02d}" for i in range(22)])
            self.assertEqual(len(first["expected"]), 66)
            names = [item["name"] for item in first["expected"]]
            self.assertEqual(names, sorted(names))
            self.assertEqual(first["trigger_names"], names)
            self.assertEqual(first["install_sql"], [item["sql"] for item in first["expected"]])
            self.assertEqual(len(first["installed_trigger_names"]), 66)
            self.assertTrue(
                all(
                    "WHEN (SELECT recursive_triggers FROM pragma_recursive_triggers)=0" in item["sql"]
                    and "ARCHIVE_CHANGE_FEED_RECURSIVE_TRIGGERS_REQUIRED" in item["sql"]
                    for item in first["expected"]
                )
            )
            self.assertNotEqual(
                first["source_schema_sha256_before_installation"],
                first["source_schema_sha256_after_installation"],
            )

            second = install_writer_guards(conn)
            self.assertEqual(second["status"], "verified")
            self.assertEqual(second["installed_trigger_names"], [])
            self.assertFalse(second["source_schema_changed_by_installation"])
            self.assertEqual(
                second["source_schema_sha256_before_installation"],
                second["source_schema_sha256_after_installation"],
            )
        finally:
            conn.close()

    def test_existing_user_trigger_is_preserved(self):
        conn = sqlite3.connect(":memory:")
        try:
            conn.executescript(
                "CREATE TABLE records(id INTEGER PRIMARY KEY, value TEXT);"
                "CREATE TABLE audit(record_id INTEGER);"
                "CREATE TRIGGER user_keep AFTER INSERT ON records "
                "BEGIN INSERT INTO audit VALUES(NEW.id); END;"
            )
            conn.commit()
            before = conn.execute(
                "SELECT sql FROM sqlite_master WHERE type='trigger' AND name='user_keep'"
            ).fetchone()[0]

            receipt = install_writer_guards(conn)
            after = conn.execute(
                "SELECT sql FROM sqlite_master WHERE type='trigger' AND name='user_keep'"
            ).fetchone()[0]
            self.assertEqual(after, before)
            self.assertEqual(receipt["guarded_tables"], ["audit", "records"])

            conn.execute("PRAGMA recursive_triggers=ON")
            conn.execute("INSERT INTO records VALUES(7,'ok')")
            self.assertEqual(conn.execute("SELECT * FROM audit").fetchall(), [(7,)])
        finally:
            conn.close()

    def test_install_uses_savepoint_and_preserves_callers_outer_transaction(self):
        conn = sqlite3.connect(":memory:")
        try:
            conn.execute("CREATE TABLE records(id INTEGER PRIMARY KEY, value TEXT)")
            conn.execute("INSERT INTO records VALUES(1,'unchanged')")
            conn.commit()
            conn.execute("BEGIN")
            receipt = install_writer_guards(conn)
            self.assertTrue(conn.in_transaction)
            self.assertEqual(receipt["status"], "verified")
            self.assertEqual(conn.execute("SELECT value FROM records").fetchone()[0], "unchanged")
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type='trigger' AND name LIKE 'ia_writer_guard_%'"
                ).fetchone()[0],
                3,
            )

            conn.rollback()
            self.assertEqual(conn.execute("SELECT value FROM records").fetchone()[0], "unchanged")
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type='trigger' AND name LIKE 'ia_writer_guard_%'"
                ).fetchone()[0],
                0,
            )
        finally:
            conn.close()

    def test_mismatched_existing_guard_is_not_overwritten_and_install_is_atomic(self):
        conn = sqlite3.connect(":memory:")
        try:
            conn.execute("CREATE TABLE alpha(id INTEGER PRIMARY KEY)")
            conn.execute("CREATE TABLE beta(id INTEGER PRIMARY KEY)")
            conn.commit()
            target = inspect_writer_guards(conn)["expected"][0]
            bad_sql = (
                f"CREATE TRIGGER {_quote(target['name'])} BEFORE {target['operation']} "
                f"ON {_quote(target['table'])} BEGIN SELECT RAISE(ABORT,'WRONG_GUARD'); END"
            )
            conn.execute(bad_sql)
            conn.commit()

            before = conn.execute(
                "SELECT name,sql FROM sqlite_master WHERE type='trigger' ORDER BY name"
            ).fetchall()
            status = inspect_writer_guards(conn)
            self.assertEqual(status["status"], "missing_and_mismatched")
            self.assertEqual(status["mismatched"], [target["name"]])
            with self.assertRaisesRegex(ValueError, "different identity SQL"):
                install_writer_guards(conn)
            after = conn.execute(
                "SELECT name,sql FROM sqlite_master WHERE type='trigger' ORDER BY name"
            ).fetchall()
            self.assertEqual(after, before)
        finally:
            conn.close()

    def test_new_table_is_reported_as_missing_writer_contract(self):
        conn = sqlite3.connect(":memory:")
        try:
            conn.execute("CREATE TABLE original(id INTEGER PRIMARY KEY)")
            conn.commit()
            install_writer_guards(conn)
            conn.execute("CREATE TABLE added_later(id INTEGER PRIMARY KEY)")
            conn.commit()

            status = inspect_writer_guards(conn)
            self.assertEqual(status["status"], "missing")
            self.assertFalse(status["all_writers_contract_enforced"])
            self.assertEqual(len(status["missing"]), 3)
            self.assertEqual(
                {row["table"] for row in status["expected"] if row["name"] in status["missing"]},
                {"added_later"},
            )

            repaired = install_writer_guards(conn)
            self.assertEqual(repaired["status"], "verified")
            self.assertEqual(len(repaired["installed_trigger_names"]), 3)
        finally:
            conn.close()

    def test_virtual_tables_fail_before_any_guard_is_installed(self):
        conn = sqlite3.connect(":memory:")
        try:
            conn.execute("CREATE TABLE ordinary(id INTEGER PRIMARY KEY)")
            try:
                conn.execute("CREATE VIRTUAL TABLE search USING fts5(body)")
            except sqlite3.OperationalError as exc:
                self.skipTest(f"SQLite build lacks FTS5 for the virtual-table probe: {exc}")
            conn.commit()

            with self.assertRaisesRegex(ValueError, "virtual/shadow source tables are unsupported"):
                install_writer_guards(conn)
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type='trigger' AND name LIKE 'ia_writer_guard_%'"
                ).fetchone()[0],
                0,
            )
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
