"""Store の再オープンが、値の変わらない書込みを行わないことを人工データで検証する。"""

import base64
from pathlib import Path
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'src/python'))
sys.path.insert(0, str(ROOT))

from ikarchive.change_feed import CHANGE_TABLE, install_change_feed
from ikarchive.planner import Planner
from ikarchive.store import Store, js, now
import json

from test_analysis import MANIFEST, coop_id, response, vs_detail

TABLES = ('matches', 'match_classification', 'rate_points', 'analysis_genre')


def dump(db, table):
    return [tuple(r) for r in db.execute(f'SELECT rowid,* FROM {table} ORDER BY rowid')]


class NoopWriteTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name).resolve() / 'a.sqlite'
        self.planner = Planner(MANIFEST)
        self.store = Store(self.path)
        self.ingest('VsHistoryDetailQuery', {'data': {'vsHistoryDetail': self._bankara()}})
        job = {'id': coop_id('noop'), 'rule': 'REGULAR', 'playedTime': '2026-09-22T02:00:00Z', 'jobRate': 120, 'jobScore': 140,
               'myResult': {'goldenDeliverCount': 40}, 'memberResults': [], 'waveResults': [{'teamDeliverCount': 20}]}
        self.ingest('CoopHistoryDetailQuery', {'data': {'coopHistoryDetail': job}})
        self.store.close()
        self.store = None

    def tearDown(self):
        if self.store is not None:
            self.store.close()
        self.tmp.cleanup()

    @staticmethod
    def _bankara():
        d = vs_detail('BANKARA', 'noop', (4, 4), bankara='CHALLENGE', rule='AREA')
        d['bankaraMatch']['bankaraPower'] = {'power': 2100, 'weaponPower': 1980}
        return d

    def ingest(self, op, body):
        rid = self.store.record(response(op, body))
        self.store.project(rid, self.planner)
        return rid

    def enable_feed(self):
        s = Store(self.path)
        install_change_feed(s.db)
        s.db.commit()
        s.close()

    def feed_count(self, store):
        return store.db.execute(f'SELECT count(*) FROM {CHANGE_TABLE}').fetchone()[0]

    def test_reopen_leaves_feed_and_rows_untouched(self):
        s = Store(self.path)
        self.assertGreater(s.db.execute('SELECT count(*) FROM rate_points').fetchone()[0], 0)
        self.assertGreater(s.db.execute('SELECT count(*) FROM match_classification').fetchone()[0], 0)
        s.close()
        self.enable_feed()
        s = Store(self.path)
        before = {t: dump(s.db, t) for t in TABLES}
        count = self.feed_count(s)
        s.close()
        for _ in range(2):
            s = Store(self.path)
            self.assertEqual(self.feed_count(s), count)
            self.assertEqual({t: dump(s.db, t) for t in TABLES}, before)
            s.close()

    def test_changed_documents_are_still_reclassified(self):
        s = Store(self.path)
        old_class = dump(s.db, 'match_classification')
        old_rates = dump(s.db, 'rate_points')
        key = s.db.execute("SELECT match_key FROM matches WHERE kind='vs'").fetchone()[0]
        doc = json.loads(s.db.execute("SELECT json_text FROM documents WHERE kind='vs'").fetchone()[0])
        doc['vsRule'] = {'rule': 'LOFT', 'name': 'LOFT'}
        doc['bankaraMatch']['bankaraPower'] = {'power': 2222, 'weaponPower': 1980}
        doc['playedTime'] = '2026-09-23T05:05:05Z'
        s.db.execute("UPDATE documents SET json_text=? WHERE kind='vs' AND match_key=?", (js(doc), key))
        s.db.commit()
        s.close()
        s = Store(self.path)
        new_class = dump(s.db, 'match_classification')
        new_rates = dump(s.db, 'rate_points')
        s.close()
        self.assertNotEqual(old_class, new_class)
        self.assertNotEqual(old_rates, new_rates)
        self.assertIn('LOFT', [r[1:] for r in new_class if r[2] == 'vs'][0])
        self.assertIn(2222, [r[8] for r in new_rates])

    def test_removed_series_are_deleted(self):
        s = Store(self.path)
        key = s.db.execute("SELECT match_key FROM matches WHERE kind='vs'").fetchone()[0]
        doc = json.loads(s.db.execute("SELECT json_text FROM documents WHERE kind='vs'").fetchone()[0])
        del doc['bankaraMatch']['bankaraPower']
        s.db.execute("UPDATE documents SET json_text=? WHERE kind='vs' AND match_key=?", (js(doc), key))
        s.db.commit()
        s.close()
        s = Store(self.path)
        labels = {r[0] for r in s.db.execute("SELECT label FROM rate_points WHERE match_key=?", (key,))}
        s.close()
        self.assertNotIn('バンカラパワー', labels)

    def test_schema_is_skipped_when_hash_matches_and_reapplied_when_broken(self):
        s = Store(self.path)
        views = s.db.execute("SELECT name,rootpage FROM sqlite_master WHERE type='view' ORDER BY name").fetchall()
        s.db.execute("DROP VIEW battle_awards")  # 痕跡: 再適用されなければ欠けたまま残る
        s.db.commit()
        s.close()
        s = Store(self.path)  # ハッシュ一致: 流さない
        names = {r[0] for r in s.db.execute("SELECT name FROM sqlite_master WHERE type='view'")}
        self.assertNotIn('battle_awards', names)
        s.db.execute("UPDATE control SET value='broken' WHERE key='schema_sql_sha256'")
        s.db.commit()
        s.close()
        s = Store(self.path)  # ハッシュ不一致: 再適用
        names = {r[0] for r in s.db.execute("SELECT name FROM sqlite_master WHERE type='view'")}
        stored = s.db.execute("SELECT value FROM control WHERE key='schema_sql_sha256'").fetchone()[0]
        s.close()
        self.assertIn('battle_awards', names)
        self.assertNotEqual(stored, 'broken')
        self.assertEqual(len(stored), 64)


if __name__ == '__main__':
    unittest.main()
