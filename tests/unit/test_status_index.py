"""Exact status counts should use a compact index on the large entities table."""

import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'src/python'))

from ikarchive.store import Store


class StatusIndexTests(unittest.TestCase):
    def test_entities_count_is_exact_with_rowid_holes_and_uses_compact_index(self):
        with tempfile.TemporaryDirectory() as directory:
            db_path = Path(directory) / 'archive.sqlite3'
            store = Store(db_path)
            try:
                store.db.execute("INSERT INTO bodies VALUES('test-sha', X'7B7D', 2)")
                store.db.execute('''INSERT INTO responses(
                    event_id,account,fetched_at,operation,variables_json,headers_json,body_sha256
                ) VALUES('test-event','account','2026-09-26T00:00:00+00:00','TestQuery','{}','{}','test-sha')''')
                store.db.executemany(
                    'INSERT INTO entities(account,typename,entity_id,response_id,json_text) VALUES(?,?,?,?,?)',
                    [('account', 'Weapon', str(i), 1, '{"payload":"value"}') for i in range(100)],
                )
                store.db.execute("DELETE FROM entities WHERE entity_id IN ('3','7','51')")
                store.db.commit()

                self.assertGreater(
                    store.db.execute('SELECT max(rowid) FROM entities').fetchone()[0], 97
                )
                self.assertEqual(store.status()['entities'], 97)
                plan = [row[3] for row in store.db.execute(
                    'EXPLAIN QUERY PLAN SELECT count(*) FROM entities'
                )]
                self.assertTrue(
                    any('USING COVERING INDEX entities_response_id' in step for step in plan),
                    plan,
                )
            finally:
                store.close()


if __name__ == '__main__':
    unittest.main()
