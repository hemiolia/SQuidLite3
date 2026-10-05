"""同じ内容の再観測で matches / entities を書き換えないことを、人工データと変更追跡で検証する。

収集は同じ応答を毎周期取り直す。変更前は、取り直しのたびに応答に載る全試合の matches.last_seen を
書き換えており、変更追跡 archive_change_feed に matches の UPDATE が毎周期並んだ（部品の作り直しの原因）。
ここでは次を確かめる。

- 同じ応答の取り直し（再観測）では matches の行が1件も変わらず、変更追跡にも出ない。取り直しを含む
  観測時刻は、ビュー match_observations が response_fetches から与える。
- 新しい内容の応答でも、first_seen/last_seen の値が変わらないときは UPDATE が出ない。
- 変更前の matches.last_seen（取り直しで進めていた値）と、変更後の match_observations.last_observed_at が、
  同じ取得の列で同じ値になる。
- entities は JSON が変わったときだけ書き換わる。
- 測定器の対照: 変更前の文を同じ値で流すと、変更追跡に UPDATE が出る（出なければ「出ない」は何も測っていない）。
"""

import base64
import sys
import tempfile
import unittest
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'src/python'))
sys.path.insert(0, str(ROOT))

from ikarchive.change_feed import CHANGE_TABLE, install_change_feed
from ikarchive.planner import Planner, identity, walk
from ikarchive.store import Store, js

from test_analysis import MANIFEST, vs_detail, vs_id

ACCOUNT = 'account-a'
LIST_OP = 'LatestBattleHistoriesQuery'
DETAIL_OP = 'VsHistoryDetailQuery'
ENTITY_OP = 'EntityProbeQuery'

# 変更前の文（作業履歴 .history/opus-20261005/store.py.before-reobserve と同じ。.history は git の管理外）。測定器の対照に使う。
OLD_MATCHES_UPSERT = (
    'INSERT INTO matches VALUES(?,?,?,?,?,NULL) ON CONFLICT(account,kind,match_key) DO UPDATE SET '
    'last_seen=MAX(last_seen,excluded.last_seen),first_seen=MIN(first_seen,excluded.first_seen)'
)
OLD_ENTITIES_UPSERT = (
    'INSERT INTO entities VALUES(?,?,?,?,?) ON CONFLICT(account,typename,entity_id) DO UPDATE SET '
    'response_id=excluded.response_id,json_text=excluded.json_text'
)

# 実カタログの操作に頼らず、型付きの実体（Weapon / Badge）だけを返す最小の操作定義。
ENTITY_MANIFEST = {'queries': {ENTITY_OP: {
    'params': {'operationKind': 'query', 'id': 'entity-probe'},
    'operation': {'argumentDefinitions': [], 'selections': [
        {'kind': 'LinkedField', 'name': 'weapon', 'alias': None, 'concreteType': 'Weapon', 'selections': [
            {'kind': 'ScalarField', 'name': 'id', 'alias': None},
            {'kind': 'ScalarField', 'name': 'name', 'alias': None}]},
        {'kind': 'LinkedField', 'name': 'badge', 'alias': None, 'concreteType': 'Badge', 'selections': [
            {'kind': 'ScalarField', 'name': 'id', 'alias': None},
            {'kind': 'ScalarField', 'name': 'name', 'alias': None}]},
    ]}}}}


def T(hour):
    """時刻の文字列。取得時刻は書式をそろえ、文字列比較と julianday が同じ順序になるようにする。"""
    return f'2026-10-05T{hour:02d}:00:00+00:00'


def event(op, body, fetched_at, variables=None):
    return {
        'event_id': str(uuid.uuid4()),
        'account': ACCOUNT,
        'fetched_at': fetched_at,
        'operation': op,
        'variables': variables or {},
        'status': 200,
        'body_base64': base64.b64encode(js(body).encode()).decode(),
    }


def history_list(*ids):
    nodes = [{'id': i} for i in ids]
    return {'data': {'latestBattleHistories': {'historyGroups': {'nodes': [{'historyDetails': {'nodes': nodes}}]}}}}


def detail_body(suffix, **extra):
    detail = vs_detail('REGULAR', suffix, (4, 4), rule='TURF_WAR')
    detail.update(extra)
    return {'data': {'vsHistoryDetail': detail}}


def entity_body(weapon_name, badge_name):
    return {'data': {'weapon': {'id': 'W1', 'name': weapon_name}, 'badge': {'id': 'B1', 'name': badge_name}}}


def legacy_matches(self, r, data, okay, reobserve=False):
    """変更前の Store._matches（reobserve を持たない版）。引数 reobserve は無視する。

    変更前の store.py を写したもの。再観測の呼び出し（reobserve=True）も、新しい内容の応答の投影も、
    matches を無条件に UPSERT し、詳細応答が同じでも detail_response_id を書き直していた。
    """
    for path, v in walk(data):
        if not isinstance(v, dict) or not isinstance(v.get('id'), str):
            continue
        ident = identity(v['id'])
        if not ident:
            continue
        kind, key = ident
        a = r['account']
        t = r['fetched_at']
        self.db.execute(OLD_MATCHES_UPSERT, (a, kind, key, t, t))
        self.db.execute('INSERT OR IGNORE INTO match_refs VALUES(?,?,?,?)', (a, kind, v['id'], key))
        self.queue(a, 'VsHistoryDetailQuery' if kind == 'vs' else 'CoopHistoryDetailQuery',
                   {'vsResultId' if kind == 'vs' else 'coopHistoryDetailId': v['id']}, kind, key)
        self.db.execute('INSERT OR IGNORE INTO sightings VALUES(?,?,?,?,?,?)', (r['id'], a, kind, key, js(path), js(v)))
        full = ((r['operation'] == 'VsHistoryDetailQuery' and kind == 'vs' and len(path) == 1)
                or (r['operation'] == 'CoopHistoryDetailQuery' and kind == 'coop' and len(path) == 1))
        if full:
            self.db.execute('INSERT OR IGNORE INTO documents VALUES(?,?,?,?,?)', (r['id'], a, kind, key, js(v)))
            if okay:
                self.db.execute('''UPDATE matches SET detail_response_id=? WHERE account=? AND kind=? AND match_key=? AND
                    (detail_response_id IS NULL OR COALESCE((SELECT MAX(julianday(fetched_at)) FROM response_fetches WHERE response_id=detail_response_id),
                    (SELECT julianday(fetched_at) FROM responses WHERE id=detail_response_id))<=julianday(?))''',
                    (r['id'], a, kind, key, r['fetched_at']))
            if kind in ('vs', 'coop'):
                self._write_classification(a, kind, key)
                self._write_rates(a, kind, key)


class LegacyStore(Store):
    """_matches だけを変更前の挙動にした Store。ほかの処理は現行のまま。"""

    _matches = legacy_matches


def feed_mark(store):
    return store.db.execute(f'SELECT COALESCE(MAX(event_id),0) FROM {CHANGE_TABLE}').fetchone()[0]


def feed_since(store, mark):
    """mark より後の変更追跡を {(表, 操作): 件数} で返す。"""
    return {(t, o): n for t, o, n in store.db.execute(
        f'SELECT table_name,operation,count(*) FROM {CHANGE_TABLE} WHERE event_id>? GROUP BY 1,2', (mark,))}


def dump(store, table):
    return [tuple(r) for r in store.db.execute(f'SELECT rowid,* FROM {table} ORDER BY rowid')]


def matches_by_key(store):
    return {r['match_key']: dict(r) for r in store.db.execute('SELECT * FROM matches')}


def observations_by_key(store):
    return {r['match_key']: dict(r) for r in store.db.execute('SELECT * FROM match_observations')}


class ReobserveTestBase(unittest.TestCase):
    store_class = Store
    manifest = MANIFEST

    @classmethod
    def setUpClass(cls):
        cls.planner = Planner(cls.manifest)

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dirs = Path(self.tmp.name).resolve()
        self.store = self.open_store('main.sqlite')

    def tearDown(self):
        self.store.close()
        for store in getattr(self, 'extra_stores', []):
            store.close()
        self.tmp.cleanup()

    def open_store(self, name, store_class=None):
        """空の DB を作り、変更追跡を入れてから開き直す（以後の書込みがすべて変更追跡に残る）。"""
        path = self.dirs / name
        first = Store(path)
        install_change_feed(first.db)
        first.db.commit()
        first.close()
        store = (store_class or self.store_class)(path)
        if name != 'main.sqlite':
            self.extra_stores = getattr(self, 'extra_stores', []) + [store]
        return store

    def ingest(self, op, body, fetched_at, variables=None, store=None, planner=None):
        store = store or self.store
        rid = store.record(event(op, body, fetched_at, variables))
        store.project(rid, planner or self.planner)
        return rid

    def step(self, op, body, fetched_at, variables=None, store=None, planner=None):
        """1回の取得を取り込み、(応答 id, その取り込みで増えた変更追跡) を返す。"""
        store = store or self.store
        mark = feed_mark(store)
        rid = self.ingest(op, body, fetched_at, variables, store, planner)
        return rid, feed_since(store, mark)


class ReobserveMatchesTests(ReobserveTestBase):
    def test_identical_list_refetch_leaves_matches_untouched(self):
        a, b = vs_id('REGULAR', 'a'), vs_id('REGULAR', 'b')
        body = history_list(a, b)
        first, created = self.step(LIST_OP, body, T(1))
        self.assertEqual(created.get(('matches', 'INSERT')), 2)
        before = dump(self.store, 'matches')
        self.assertEqual(len(before), 2)
        other = {t: dump(self.store, t) for t in ('sightings', 'match_refs', 'entities')}

        for hour in (2, 3):
            again, changes = self.step(LIST_OP, body, T(hour))
            self.assertEqual(again, first, '同じ本文の取り直しは同じ応答')
            self.assertEqual(dump(self.store, 'matches'), before, 'matches の行が rowid と全列で変わらない')
            for operation in ('INSERT', 'UPDATE', 'DELETE'):
                self.assertNotIn(('matches', operation), changes)
            for table in ('sightings', 'match_refs', 'entities'):
                self.assertEqual(dump(self.store, table), other[table])
                for operation in ('INSERT', 'UPDATE', 'DELETE'):
                    self.assertNotIn((table, operation), changes)
            self.assertEqual(changes.get(('response_fetches', 'INSERT')), 1, '取り直しの記録は残る')

        for row in matches_by_key(self.store).values():
            self.assertEqual((row['first_seen'], row['last_seen']), (T(1), T(1)))
        observed = observations_by_key(self.store)
        self.assertEqual(len(observed), 2)
        for row in observed.values():
            self.assertEqual(row['first_observed_at'], T(1))
            self.assertEqual(row['last_observed_at'], T(3), '観測時刻は最後の取り直しへ進む')
            self.assertEqual(row['observing_responses'], 1)

    def test_new_content_list_updates_only_when_the_values_change(self):
        a, b, c = (vs_id('REGULAR', s) for s in 'abc')
        _, changes = self.step(LIST_OP, history_list(a, b), T(1))
        self.assertEqual(changes.get(('matches', 'INSERT')), 2)

        # 新しい内容（c が加わる）を、より新しい時刻で保存 -> a,b の last_seen が進む。c は新規。
        _, changes = self.step(LIST_OP, history_list(a, b, c), T(3))
        self.assertEqual(changes.get(('matches', 'UPDATE')), 2)
        self.assertEqual(changes.get(('matches', 'INSERT')), 1)
        self.assertNotIn(('entities', 'UPDATE'), changes, '一覧の節点の JSON が同じなら entities も動かない')
        rows = matches_by_key(self.store)
        self.assertEqual({k.rsplit('_', 1)[1]: (v['first_seen'], v['last_seen']) for k, v in rows.items()},
                         {'a': (T(1), T(3)), 'b': (T(1), T(3)), 'c': (T(3), T(3))})

        # 新しい内容だが、時刻が既存の範囲の内側（first_seen <= t <= last_seen）-> 値が変わらないので UPDATE が出ない。
        snapshot = dump(self.store, 'matches')
        _, changes = self.step(LIST_OP, history_list(b, a), T(2))
        self.assertEqual(dump(self.store, 'matches'), snapshot)
        for operation in ('INSERT', 'UPDATE', 'DELETE'):
            self.assertNotIn(('matches', operation), changes)
        self.assertEqual(changes.get(('responses', 'INSERT')), 1, '新しい内容の応答は保存されている')

        # 既存の first_seen より古い時刻 -> first_seen が下がる（値が変わるので UPDATE が出る）。
        _, changes = self.step(LIST_OP, history_list(a), T(0))
        self.assertEqual(changes.get(('matches', 'UPDATE')), 1)
        rows = matches_by_key(self.store)
        self.assertEqual({k.rsplit('_', 1)[1]: (v['first_seen'], v['last_seen']) for k, v in rows.items()},
                         {'a': (T(0), T(3)), 'b': (T(1), T(3)), 'c': (T(3), T(3))})

    def test_old_statement_would_have_written_in_the_same_steps(self):
        """対照: 変更前の _matches では、同じ取得の列で上の「UPDATE が出ない」取得が UPDATE を出す。"""
        legacy = self.open_store('legacy.sqlite', LegacyStore)
        a, b = vs_id('REGULAR', 'a'), vs_id('REGULAR', 'b')
        body = history_list(a, b)
        self.step(LIST_OP, body, T(1), store=legacy)
        _, refetch = self.step(LIST_OP, body, T(2), store=legacy)
        self.assertEqual(refetch.get(('matches', 'UPDATE')), 2, '取り直しで全試合の行が書き換わる')
        self.step(LIST_OP, history_list(a, b, vs_id('REGULAR', 'c')), T(3), store=legacy)
        _, inside = self.step(LIST_OP, history_list(b, a), T(2), store=legacy)
        self.assertEqual(inside.get(('matches', 'UPDATE')), 2, '範囲の内側の新しい内容でも値の同じ UPDATE が出る')

    def test_last_seen_semantics_after_a_b_a(self):
        """A -> B -> A の詳細: detail_response_id は A に戻り、last_seen は B のまま、観測時刻は A の取り直しへ進む。"""
        ida = vs_id('REGULAR', 'x')
        va = {'vsResultId': ida}
        rid_a, _ = self.step(DETAIL_OP, detail_body('x', label='A'), T(1), va)
        rid_b, _ = self.step(DETAIL_OP, detail_body('x', label='B'), T(2), va)
        self.assertNotEqual(rid_a, rid_b)
        def row():
            return dict(self.store.db.execute('SELECT * FROM matches').fetchone())

        self.assertEqual(row()['detail_response_id'], rid_b)

        # 同じ詳細（B）の取り直し: 何も書き換わらない。
        again, changes = self.step(DETAIL_OP, detail_body('x', label='B'), T(3), va)
        self.assertEqual(again, rid_b)
        for operation in ('INSERT', 'UPDATE', 'DELETE'):
            self.assertNotIn(('matches', operation), changes, '同じ詳細応答の取り直しで detail_response_id を書き直さない')

        # A が戻ってくる（古い本文の新しい観測）: 現行の詳細が A に戻る。書き換わるのは matches の1行1回。
        back, changes = self.step(DETAIL_OP, detail_body('x', label='A'), T(4), va)
        self.assertEqual(back, rid_a)
        self.assertEqual(changes.get(('matches', 'UPDATE')), 1)
        self.assertEqual(row()['detail_response_id'], rid_a)
        self.assertEqual(row()['last_seen'], T(2), 'last_seen は新しい内容の応答（B）を保存した時刻のまま')
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM matches').fetchone()[0], 1)
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM documents').fetchone()[0], 2)

        # A をもう一度取り直す: すでに A が現行なので何も書き換わらない。
        _, changes = self.step(DETAIL_OP, detail_body('x', label='A'), T(5), va)
        for operation in ('INSERT', 'UPDATE', 'DELETE'):
            self.assertNotIn(('matches', operation), changes)
        observed = observations_by_key(self.store)
        self.assertEqual(len(observed), 1)
        only = next(iter(observed.values()))
        self.assertEqual((only['first_observed_at'], only['last_observed_at'], only['observing_responses']), (T(1), T(5), 2))

    def test_observations_view_shape(self):
        self.assertEqual(
            [d[0] for d in self.store.db.execute('SELECT * FROM match_observations').description],
            ['account', 'kind', 'match_key', 'first_observed_at', 'last_observed_at', 'observing_responses'])
        self.assertEqual(
            self.store.db.execute("SELECT type FROM sqlite_master WHERE name='match_observations'").fetchone()[0], 'view')

    def test_observing_responses_counts_each_response_once(self):
        """同じ試合が1つの応答に2か所で載っても（目撃記録は2行）、観測した応答は1つと数える。"""
        a = vs_id('REGULAR', 'a')
        self.step(LIST_OP, history_list(a, a), T(1))
        self.step(LIST_OP, history_list(a, a), T(2))
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM sightings').fetchone()[0], 2)
        (row,) = observations_by_key(self.store).values()
        self.assertEqual((row['first_observed_at'], row['last_observed_at'], row['observing_responses']), (T(1), T(2), 1))

    def test_view_is_created_in_an_existing_database(self):
        """ビューの無い既存 DB（schema.sql のハッシュが古い）を開くと、ビューが作られる。"""
        self.store.db.execute('DROP VIEW match_observations')
        self.store.db.execute("UPDATE control SET value='before-reobserve' WHERE key='schema_sql_sha256'")
        self.store.db.commit()
        self.store.close()
        self.store = Store(self.dirs / 'main.sqlite')
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM match_observations').fetchone()[0], 0)

    def test_reobservation_recreates_a_missing_matches_row(self):
        """再観測の経路でも、試合の行が無ければ作る（あれば書き換えない）。"""
        a = vs_id('REGULAR', 'a')
        self.step(LIST_OP, history_list(a), T(1))
        key = self.store.db.execute('SELECT match_key FROM matches').fetchone()[0]
        self.store.db.execute('PRAGMA foreign_keys=OFF')
        self.store.db.execute('DELETE FROM match_refs')
        self.store.db.execute('DELETE FROM matches')
        self.store.db.commit()
        self.store.db.execute('PRAGMA foreign_keys=ON')
        self.step(LIST_OP, history_list(a), T(2))
        rows = matches_by_key(self.store)
        self.assertEqual(list(rows), [key])
        self.assertEqual((rows[key]['first_seen'], rows[key]['last_seen']), (T(2), T(2)))


class OldAndNewObservationTimeTests(ReobserveTestBase):
    """変更前の matches.last_seen（取り直しで進めていた値）= 変更後の match_observations.last_observed_at。"""

    @staticmethod
    def suffix(key):
        return key.rsplit('_', 1)[1]

    @staticmethod
    def scenario():
        a, b, c = (vs_id('REGULAR', s) for s in 'abc')
        l1, l2 = history_list(a, b), history_list(a, b, c)
        da, db = detail_body('a'), detail_body('b')
        va, vb = {'vsResultId': a}, {'vsResultId': b}
        # (名前, 操作, 本文, 時刻, variables, 取り直しか)。時刻は昇順（同じ取得の順序で両方へ流す）。
        return [
            ('L1 新規', LIST_OP, l1, T(1), None, False),
            ('L1 取り直し', LIST_OP, l1, T(2), None, True),
            ('L2 新規（c が加わる）', LIST_OP, l2, T(3), None, False),
            ('L2 取り直し', LIST_OP, l2, T(4), None, True),
            ('L1 取り直し（A->B->A）', LIST_OP, l1, T(5), None, True),
            ('a の詳細 新規', DETAIL_OP, da, T(6), va, False),
            ('a の詳細 取り直し', DETAIL_OP, da, T(7), va, True),
            ('b の詳細 新規', DETAIL_OP, db, T(8), vb, False),
            ('L2 取り直し', LIST_OP, l2, T(9), None, True),
            ('a の詳細 取り直し', DETAIL_OP, da, T(10), va, True),
        ]

    def run_scenario(self, store):
        per_step = []
        for name, op, body, at, variables, refetch in self.scenario():
            _, changes = self.step(op, body, at, variables, store=store)
            per_step.append((name, refetch, changes.get(('matches', 'INSERT'), 0) + changes.get(('matches', 'UPDATE'), 0)))
        return per_step

    def test_old_last_seen_equals_new_last_observed_at(self):
        old_store = self.open_store('old.sqlite', LegacyStore)
        old_steps = self.run_scenario(old_store)
        new_steps = self.run_scenario(self.store)

        old_rows = matches_by_key(old_store)
        new_rows = matches_by_key(self.store)
        observed = observations_by_key(self.store)
        self.assertEqual(sorted(map(self.suffix, old_rows)), ['a', 'b', 'c'], '分母: 試合は3件')
        self.assertEqual(set(old_rows), set(new_rows))
        self.assertEqual(set(old_rows), set(observed))

        # 変更前の last_seen は取り直しで進んでいる。
        self.assertEqual({self.suffix(k): v['last_seen'] for k, v in old_rows.items()},
                         {'a': T(10), 'b': T(9), 'c': T(9)})
        for key, old in old_rows.items():
            with self.subTest(match=self.suffix(key)):
                # 同じ取得の列で: 旧 matches.last_seen == 新 match_observations.last_observed_at。
                self.assertEqual(old['last_seen'], observed[key]['last_observed_at'])
                self.assertEqual(old['first_seen'], observed[key]['first_observed_at'])
                # first_seen は新旧の matches でも一致する。
                self.assertEqual(old['first_seen'], new_rows[key]['first_seen'])

        # 変更後の matches.last_seen は「新しい内容の応答を最後に保存した時刻」で、取り直しでは進まない。
        self.assertEqual({self.suffix(k): v['last_seen'] for k, v in new_rows.items()},
                         {'a': T(6), 'b': T(8), 'c': T(3)})
        for key, row in new_rows.items():
            newest_response = self.store.db.execute(
                'SELECT MAX(r.fetched_at) FROM sightings s JOIN responses r ON r.id=s.response_id '
                'WHERE s.account=? AND s.kind=? AND s.match_key=?',
                (row['account'], row['kind'], key)).fetchone()[0]
            self.assertEqual(row['last_seen'], newest_response)
            self.assertLess(row['last_seen'], observed[key]['last_observed_at'])
        self.assertEqual({self.suffix(k): v['observing_responses'] for k, v in observed.items()},
                         {'a': 3, 'b': 3, 'c': 1})

        # 取り直しの取得では matches に何も書かない。新しい内容の取得だけが書く。変更前は取り直しでも毎回書いていた。
        self.assertEqual([name for name, *_ in old_steps], [name for name, *_ in new_steps])
        for (name, refetch, new_count), (_, _, old_count) in zip(new_steps, old_steps):
            with self.subTest(step=name):
                if refetch:
                    self.assertEqual(new_count, 0)
                    self.assertGreater(old_count, 0)
                else:
                    self.assertGreater(new_count, 0)
        refetch_total_old = sum(n for _, refetch, n in old_steps if refetch)
        refetch_total_new = sum(n for _, refetch, n in new_steps if refetch)
        self.assertEqual((refetch_total_new, refetch_total_old > 0), (0, True))

        # 取り直し以外の中身（応答・目撃記録・文書・取得記録）は新旧で同じ。
        for table in ('responses', 'sightings', 'documents', 'response_fetches', 'match_refs'):
            self.assertEqual(self.store.db.execute(f'SELECT count(*) FROM {table}').fetchone()[0],
                             old_store.db.execute(f'SELECT count(*) FROM {table}').fetchone()[0], table)
        self.assertEqual(
            [r[0] for r in self.store.db.execute('SELECT detail_response_id FROM matches ORDER BY match_key')],
            [r[0] for r in old_store.db.execute('SELECT detail_response_id FROM matches ORDER BY match_key')])


class EntityReobserveTests(ReobserveTestBase):
    manifest = ENTITY_MANIFEST

    def entity(self, entity_id):
        return dict(self.store.db.execute('SELECT * FROM entities WHERE entity_id=?', (entity_id,)).fetchone())

    def test_entities_change_only_when_the_json_changes(self):
        first, changes = self.step(ENTITY_OP, entity_body('splattershot', 'one'), T(1))
        self.assertEqual(changes.get(('entities', 'INSERT')), 2)
        weapon = self.entity('W1')
        self.assertEqual(weapon['response_id'], first)

        # 別の応答（Badge の中身が変わった）に、同じ JSON の Weapon が載る -> Weapon は動かない。Badge だけ更新される。
        second, changes = self.step(ENTITY_OP, entity_body('splattershot', 'two'), T(2))
        self.assertNotEqual(second, first)
        self.assertEqual(changes.get(('entities', 'UPDATE')), 1)
        self.assertEqual(self.entity('W1'), weapon, 'JSON が同じなら response_id も動かない')
        badge = self.entity('B1')
        self.assertEqual(badge['response_id'], second)
        self.assertEqual(badge['json_text'], js({'id': 'B1', 'name': 'two'}))

        # 本文は同じで variables だけ違う応答（別の応答として保存される）-> 実体の JSON はすべて同じ -> 何も動かない。
        third, changes = self.step(ENTITY_OP, entity_body('splattershot', 'two'), T(3), {'page': 2})
        self.assertNotIn(third, (first, second))
        self.assertNotIn(('entities', 'UPDATE'), changes)
        self.assertNotIn(('entities', 'INSERT'), changes)
        self.assertEqual(self.entity('W1'), weapon)
        self.assertEqual(self.entity('B1'), badge)

        # Weapon の JSON が変わる -> 更新され、response_id もその応答へ移る。
        fourth, changes = self.step(ENTITY_OP, entity_body('splatroller', 'two'), T(4))
        self.assertEqual(changes.get(('entities', 'UPDATE')), 1)
        weapon = self.entity('W1')
        self.assertEqual(weapon['response_id'], fourth)
        self.assertEqual(weapon['json_text'], js({'id': 'W1', 'name': 'splatroller'}))
        self.assertEqual(self.entity('B1'), badge)
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM entities').fetchone()[0], 2)

        # 同じ本文の取り直しは、そもそも実体を書かない。
        again, changes = self.step(ENTITY_OP, entity_body('splatroller', 'two'), T(5))
        self.assertEqual(again, fourth)
        for operation in ('INSERT', 'UPDATE', 'DELETE'):
            self.assertNotIn(('entities', operation), changes)


class InstrumentControlTests(ReobserveTestBase):
    def test_feed_records_old_statements_that_rewrite_the_same_values(self):
        """測定器の対照: 変更前の文を同じ値で流すと、変更追跡に UPDATE が出る。"""
        self.step(LIST_OP, history_list(vs_id('REGULAR', 'a')), T(1))
        match = self.store.db.execute('SELECT account,kind,match_key,first_seen,last_seen FROM matches').fetchone()
        entity = self.store.db.execute('SELECT account,typename,entity_id,response_id,json_text FROM entities').fetchone()
        self.assertIsNotNone(match)
        self.assertIsNotNone(entity)
        mark = feed_mark(self.store)
        before = (dump(self.store, 'matches'), dump(self.store, 'entities'))
        self.store.db.execute(OLD_MATCHES_UPSERT, (match['account'], match['kind'], match['match_key'], match['first_seen'], match['last_seen']))
        self.store.db.execute(OLD_ENTITIES_UPSERT, tuple(entity))
        self.store.db.commit()
        changes = feed_since(self.store, mark)
        self.assertEqual((dump(self.store, 'matches'), dump(self.store, 'entities')), before, '値は何も変わっていない')
        self.assertEqual(changes.get(('matches', 'UPDATE')), 1)
        self.assertEqual(changes.get(('entities', 'UPDATE')), 1)


if __name__ == '__main__':
    unittest.main()
