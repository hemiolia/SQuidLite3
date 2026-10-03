"""Store の既設 change-feed 契約を人工SQLite DBで検証する。"""

import hashlib
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'src/python'))

from ikarchive.change_feed import install_change_feed
import ikarchive.store as store_module
from ikarchive.store import Store
from ikarchive.writer_guards import inspect_writer_guards, install_writer_guards


def _writer_guard_name(table, operation):
    digest = hashlib.sha256((table + '\0' + operation).encode('utf-8', 'surrogatepass')).hexdigest()
    return 'ia_writer_guard_' + digest


class _FakeCursor:
    def fetchone(self):
        return None


class _FakeConnection:
    def __init__(self, *, failures=None, close_error=None, rollback_error=None, script_error=None):
        self.failures = failures or {}
        self.close_error = close_error
        self.rollback_error = rollback_error
        self.script_error = script_error
        self.executed = []
        self.close_calls = 0
        self.rollback_calls = 0
        self.executescript_calls = 0
        self.closed = False
        self.row_factory = None

    @property
    def in_transaction(self):
        return True

    def execute(self, sql, *_args):
        self.executed.append(sql)
        failure = self.failures.get(sql)
        if failure is not None:
            raise failure
        return _FakeCursor()

    def executescript(self, _script):
        self.executescript_calls += 1
        if self.script_error is not None:
            raise self.script_error

    def rollback(self):
        self.rollback_calls += 1
        if self.rollback_error is not None:
            raise self.rollback_error

    def close(self):
        self.close_calls += 1
        if self.close_error is not None:
            raise self.close_error
        self.closed = True


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

    def test_database_is_slice_preserves_query_error_when_probe_close_fails(self):
        query_error = KeyboardInterrupt('synthetic slice-probe interrupt')
        close_error = RuntimeError('synthetic probe close failure')
        connection = _FakeConnection(
            failures={
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='slice_meta'": query_error,
            },
            close_error=close_error,
        )
        with patch.object(store_module.sqlite3, 'connect', return_value=connection):
            with self.assertRaises(KeyboardInterrupt) as caught:
                store_module.database_is_slice(self.path)
        self.assertIs(caught.exception, query_error)
        self.assertEqual(connection.close_calls, 1)
        self.assertFalse(connection.closed)

    def test_readonly_constructor_closes_each_failed_connection_phase(self):
        probe_cases = (
            (
                'probe_interrupt',
                KeyboardInterrupt('synthetic probe interrupt'),
                'PRAGMA query_only=ON',
            ),
            (
                'probe_query_error',
                OSError('synthetic probe query error'),
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='slice_meta'",
            ),
        )
        main_cases = (
            ('main_interrupt', KeyboardInterrupt('synthetic main interrupt')),
            ('main_query_error', OSError('synthetic main query error')),
        )

        for name, original, sql in probe_cases:
            with self.subTest(phase=name):
                close_error = RuntimeError('synthetic close collision')
                probe = _FakeConnection(failures={sql: original}, close_error=close_error)
                instance = Store.__new__(Store)
                with patch.object(store_module.sqlite3, 'connect', return_value=probe):
                    with self.assertRaises(type(original)) as caught:
                        Store.__init__(instance, self.path, readonly=True)
                self.assertIs(caught.exception, original)
                self.assertEqual(probe.close_calls, 1)
                self.assertFalse(probe.closed)
                self.assertIsNone(instance.db)

        for name, original in main_cases:
            with self.subTest(phase=name):
                close_error = RuntimeError('synthetic main close collision')
                probe = _FakeConnection()
                main = _FakeConnection(
                    failures={'PRAGMA query_only=ON': original},
                    close_error=close_error,
                )
                instance = Store.__new__(Store)
                with patch.object(store_module.sqlite3, 'connect', side_effect=[probe, main]):
                    with self.assertRaises(type(original)) as caught:
                        Store.__init__(instance, self.path, readonly=True)
                self.assertIs(caught.exception, original)
                self.assertEqual(probe.close_calls, 1)
                self.assertTrue(probe.closed)
                self.assertEqual(main.close_calls, 1)
                self.assertFalse(main.closed)
                self.assertIsNone(instance.db)

    def test_successful_probe_close_failure_propagates_and_writable_failure_keeps_original(self):
        probe_close_error = RuntimeError('normal probe close failure')
        probe = _FakeConnection(close_error=probe_close_error)
        with patch.object(store_module.sqlite3, 'connect', return_value=probe):
            with self.assertRaises(RuntimeError) as caught_probe:
                Store(self.path, readonly=True)
        self.assertIs(caught_probe.exception, probe_close_error)
        self.assertEqual(probe.close_calls, 1)

        original = KeyboardInterrupt('synthetic writer setup interrupt')
        rollback_error = OSError('synthetic rollback collision')
        close_error = RuntimeError('synthetic writer close collision')
        writer_connection = _FakeConnection(
            script_error=original,
            rollback_error=rollback_error,
            close_error=close_error,
        )
        instance = Store.__new__(Store)
        with patch.object(store_module, 'database_is_slice', return_value=False):
            with patch.object(store_module.sqlite3, 'connect', return_value=writer_connection):
                with self.assertRaises(KeyboardInterrupt) as caught_writer:
                    Store.__init__(instance, self.path, readonly=False)
        self.assertIs(caught_writer.exception, original)
        self.assertEqual(writer_connection.rollback_calls, 1)
        self.assertEqual(writer_connection.close_calls, 1)
        self.assertFalse(writer_connection.closed)
        self.assertIsNone(instance.db)

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
