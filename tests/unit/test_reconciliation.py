import sqlite3
from pathlib import Path
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'src/python'))
from ikarchive.change_feed import install_change_feed
from ikarchive.reconciliation import read_reconciliation


class ReconciliationTests(unittest.TestCase):
    def setUp(self):
        self.current = sqlite3.connect(':memory:')
        self.current.executescript('''
            CREATE TABLE data (key TEXT PRIMARY KEY, value, future);
            INSERT INTO data(rowid,key,value,future) VALUES
                (2,'unchanged',X'00FF',NULL),(10,'changed','before',1),
                (21,'deleted','gone',NULL);
            CREATE TABLE keyed (value, key TEXT PRIMARY KEY) WITHOUT ROWID;
            INSERT INTO keyed VALUES(X'0000','a');
            CREATE TABLE empty_table (future);
        ''')
        self.baseline = sqlite3.connect(':memory:')
        self.current.backup(self.baseline)

    def tearDown(self):
        self.current.close()
        self.baseline.close()

    def test_initial_untracked_gap_and_every_type_are_reconciled(self):
        # These mutations deliberately happen before feed installation.
        self.current.execute("UPDATE data SET value=?,future=? WHERE rowid=10",
                             (b'\x00\xffafter', 9223372036854775807))
        self.current.execute('DELETE FROM data WHERE rowid=21')
        self.current.execute('INSERT INTO data(rowid,key,value,future) VALUES(99,?,?,?)',
                             ('new', 'NUL\x00text', -0.0))
        self.current.commit()
        install_change_feed(self.current)
        with read_reconciliation(self.current, self.baseline) as batch:
            records = list(batch.iter_current_changes())
            self.assertEqual(batch.metadata['through_event_id'], 0)
            self.assertEqual(set(batch.metadata['source_table_columns']),
                             {'data', 'keyed', 'empty_table', 'archive_change_feed'})
        changes = [row for row in records if row['table_name'] == 'data']
        self.assertEqual([(row['source_rowid'], row['operation']) for row in changes],
                         [(10, 'upsert'), (21, 'delete'), (99, 'upsert')])
        self.assertEqual(changes[0]['values'], ('changed', b'\x00\xffafter', 9223372036854775807))
        self.assertEqual(changes[-1]['values'][1], 'NUL\x00text')
        self.assertEqual(changes[-1]['values'][2].hex(), '-0x0.0p+0')
        with self.assertRaisesRegex(RuntimeError, 'context'):
            list(batch.iter_current_changes())
        self.assertFalse(self.current.in_transaction)
        self.assertFalse(self.baseline.in_transaction)

    def test_key_only_and_new_empty_table_are_explicitly_replaced(self):
        self.current.execute('CREATE TABLE new_empty (all_unknown_columns BLOB)')
        self.current.commit()
        install_change_feed(self.current)
        with read_reconciliation(self.current, self.baseline) as batch:
            rows = list(batch.iter_current_changes())
        keyed = [row for row in rows if row['table_name'] == 'keyed']
        self.assertEqual([row['operation'] for row in keyed], ['clear_table', 'upsert'])
        self.assertEqual(keyed[1]['values'], (b'\x00\x00', 'a'))
        empty = [row for row in rows if row['table_name'] == 'new_empty']
        self.assertEqual(len(empty), 1)
        self.assertEqual(empty[0]['operation'], 'clear_table')
        self.assertEqual(empty[0]['columns'], ['all_unknown_columns'])

    def test_added_column_and_removed_table_change_inventory(self):
        self.current.execute('ALTER TABLE data ADD COLUMN added TEXT')
        self.current.execute('DROP TABLE empty_table')
        self.current.commit()
        install_change_feed(self.current)
        with read_reconciliation(self.current, self.baseline) as batch:
            rows = list(batch.iter_current_changes())
            self.assertEqual(batch.metadata['removed_tables'], ['empty_table'])
        data = [row for row in rows if row['table_name'] == 'data']
        self.assertEqual(data[0]['operation'], 'clear_table')
        self.assertEqual(data[0]['columns'][-1], 'added')
        self.assertEqual(len(data), 4)

    def test_caller_transactions_survive_exceptions(self):
        install_change_feed(self.current)
        self.current.execute('BEGIN')
        self.baseline.execute('BEGIN')
        with self.assertRaisesRegex(RuntimeError, 'test failure'):
            with read_reconciliation(self.current, self.baseline):
                raise RuntimeError('test failure')
        self.assertTrue(self.current.in_transaction)
        self.assertTrue(self.baseline.in_transaction)

    def test_concurrent_commit_waits_for_next_snapshot(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary).resolve() / 'current.sqlite3'
            current = sqlite3.connect(path)
            writer = sqlite3.connect(path)
            baseline = sqlite3.connect(':memory:')
            try:
                current.execute('PRAGMA journal_mode=WAL')
                current.executescript('CREATE TABLE rows(value); INSERT INTO rows VALUES(1);')
                current.backup(baseline)
                install_change_feed(current)
                with read_reconciliation(current, baseline) as first:
                    writer.execute('UPDATE rows SET value=2')
                    writer.commit()
                    self.assertEqual([row for row in first.iter_current_changes()
                                      if row['table_name'] == 'rows'], [])
                    self.assertEqual(first.metadata['through_event_id'], 0)
                with read_reconciliation(current, baseline) as second:
                    changes = [row for row in second.iter_current_changes()
                               if row['table_name'] == 'rows']
                    self.assertEqual(changes[0]['values'], (2,))
                    self.assertEqual(second.metadata['through_event_id'], 1)
            finally:
                current.close()
                writer.close()
                baseline.close()


if __name__ == '__main__':
    unittest.main()
