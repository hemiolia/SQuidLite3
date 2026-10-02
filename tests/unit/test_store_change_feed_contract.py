"""Store の既設 change-feed 契約を人工SQLite DBで検証する。"""

import hashlib
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'src/python'))

from ikarchive.change_feed import install_change_feed
from ikarchive.store import Store
from ikarchive.writer_guards import inspect_writer_guards, install_writer_guards


def _writer_guard_name(table, operation):
    digest = hashlib.sha256((table + '\0' + operation).encode('utf-8', 'surrogatepass')).hexdigest()
    return 'ia_writer_guard_' + digest


class StoreChangeFeedContractTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / 'database' / 'archive.sqlite3'
        self.path.parent.mkdir(parents=True)
        initial = Store(self.path)
        initial.close()

    def tearDown(self):
        self.tmp.cleanup()

    def _install_feed_then_add_untracked_table(self, *, broken_guard=False):
        conn = sqlite3.connect(self.path)
        conn.execute('PRAGMA recursive_triggers=ON')
        install_change_feed(conn)
        install_writer_guards(conn)
        conn.commit()
        conn.execute('CREATE TABLE extra(value TEXT UNIQUE)')
        conn.execute(
            'CREATE TRIGGER extra_user_unchanged AFTER UPDATE ON extra '
            'BEGIN SELECT 1; END'
        )
        if broken_guard:
            name = _writer_guard_name('extra', 'INSERT')
            conn.execute(
                f'CREATE TRIGGER "{name}" BEFORE INSERT ON extra BEGIN SELECT 1; END'
            )
        conn.commit()
        user_sql = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='trigger' AND name='extra_user_unchanged'"
        ).fetchone()[0]
        conn.close()
        return user_sql

    def test_existing_feed_reopen_installs_missing_table_tracking_and_writer_guards(self):
        user_sql = self._install_feed_then_add_untracked_table()

        store = Store(self.path)
        self.assertEqual(store.db.execute('PRAGMA recursive_triggers').fetchone()[0], 1)
        guard_receipt = inspect_writer_guards(store.db)
        self.assertEqual(guard_receipt['status'], 'verified')
        self.assertIn('extra', guard_receipt['guarded_tables'])
        self.assertEqual(
            store.db.execute(
                "SELECT sql FROM sqlite_master WHERE type='trigger' AND name='extra_user_unchanged'"
            ).fetchone()[0],
            user_sql,
        )

        store.db.execute("INSERT INTO extra(rowid,value) VALUES(-8,'original')")
        store.db.commit()
        store.db.execute("UPDATE extra SET value='replacement' WHERE rowid=-8")
        store.db.commit()
        store.db.execute("INSERT OR REPLACE INTO extra(rowid,value) VALUES(99,'replacement')")
        store.db.commit()
        events = [tuple(row) for row in store.db.execute(
            "SELECT operation,old_rowid,new_rowid FROM archive_change_feed "
            "WHERE table_name='extra' ORDER BY event_id"
        )]
        self.assertEqual(events, [
            ('INSERT', None, -8),
            ('UPDATE', -8, -8),
            ('DELETE', -8, None),
            ('INSERT', None, 99),
        ])
        store.close()

        # 再オープンは冪等で、ユーザーtriggerのSQLを一切置換しない。
        reopened = Store(self.path)
        self.assertEqual(inspect_writer_guards(reopened.db)['status'], 'verified')
        self.assertEqual(
            reopened.db.execute(
                "SELECT sql FROM sqlite_master WHERE type='trigger' AND name='extra_user_unchanged'"
            ).fetchone()[0],
            user_sql,
        )
        reopened.close()

    def test_new_database_and_readonly_store_do_not_create_a_feed_or_schema(self):
        conn = sqlite3.connect(self.path)
        before = conn.execute(
            "SELECT type,name,sql FROM sqlite_master ORDER BY type,name"
        ).fetchall()
        self.assertIsNone(conn.execute(
            "SELECT 1 FROM sqlite_master WHERE name='archive_change_feed'"
        ).fetchone())
        conn.close()

        readonly = Store(self.path, readonly=True)
        self.assertIsNone(readonly.db.execute(
            "SELECT 1 FROM sqlite_master WHERE name='archive_change_feed'"
        ).fetchone())
        after = [tuple(row) for row in readonly.db.execute(
            "SELECT type,name,sql FROM sqlite_master ORDER BY type,name"
        )]
        self.assertEqual(after, before)
        self.assertEqual(readonly.db.execute('PRAGMA query_only').fetchone()[0], 1)
        readonly.close()

    def test_mismatched_guard_fails_without_replacement_or_connection_lock_leak(self):
        self._install_feed_then_add_untracked_table(broken_guard=True)
        conn = sqlite3.connect(self.path)
        bad_name = _writer_guard_name('extra', 'INSERT')
        bad_sql = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='trigger' AND name=?", (bad_name,)
        ).fetchone()[0]
        established_name = _writer_guard_name('bodies', 'INSERT')
        established_sql = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='trigger' AND name=?",
            (established_name,),
        ).fetchone()[0]
        conn.close()

        with self.assertRaisesRegex(ValueError, 'different identity SQL'):
            Store(self.path)

        check = sqlite3.connect(self.path)
        self.assertEqual(check.execute(
            "SELECT sql FROM sqlite_master WHERE type='trigger' AND name=?", (bad_name,)
        ).fetchone()[0], bad_sql)
        self.assertEqual(check.execute(
            "SELECT sql FROM sqlite_master WHERE type='trigger' AND name=?",
            (established_name,),
        ).fetchone()[0], established_sql)
        # The outer installer savepoint rolls back newly added extra-table feed
        # triggers, while leaving the existing feed and all user objects intact.
        self.assertIsNone(check.execute(
            "SELECT 1 FROM sqlite_master WHERE type='trigger' AND tbl_name='extra' "
            "AND name LIKE 'ia_change_%'"
        ).fetchone())
        check.close()

        writer = sqlite3.connect(self.path, timeout=0.2)
        writer.execute('PRAGMA recursive_triggers=ON')
        writer.execute("INSERT INTO extra(value) VALUES('still-writable')")
        writer.commit()
        writer.close()


if __name__ == '__main__':
    unittest.main()
