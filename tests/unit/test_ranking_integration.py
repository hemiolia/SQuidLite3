"""collector.sync と ranking_policy の統合検証テスト。

人工 Store と FakeBridge を使用して、以下の確定検証項目および負例を検証する:
(1) 既存の EventMatchRankingPeriodQuery ジョブで参加証拠なし -> Bridge.query に一度も送られない。
    (負例: 参加証拠があるジョブは送信される)
(2) 最新履歴 / 完全試合詳細が scope 処理より先に実行でき、期間 metadata 応答から新規に queue された
    不参加 period も次の Bridge 要求前に out_of_scope となる。
    (負例: 期間 metadata から新規に queue された参加 period は送信される)
(3) XRankingDetailRefetchQuery は一度も送られない (superseded)。
    (負例: 終了済みシーズンの XRankingDetailQuery は送信される)
(4) 終了後に取得済みの done_final ジョブが翌日 sync でも再要求されない。
    (負例: 未取得 (pending) の詳細の仕事は送信される。成功済み (done) の詳細の仕事は、期限が過ぎても
     pending に戻らず再要求されない。2026-10-06 前田さんの判断: 取得に成功した試合詳細を取り直すこと自体が誤り)
(5) BankaraBattleHistoriesQuery と VsHistoryDetailQuery は対象外 (unrestricted) として送信可能。
    (負例: 同一実行内にある不適格なスコープ対象操作は遮断される)
"""

import base64
import json
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'src/python'))
from ikarchive.store import Store, js, now
from ikarchive.collector import sync

ROOT = Path(__file__).resolve().parents[2]
MANIFEST = json.loads((ROOT / 'config/query-catalog.snapshot.json').read_text())


def encoded(s: str) -> str:
    return base64.b64encode(s.encode()).decode()


def response(op, body, account='acc-test', variables=None, status=200):
    raw = body if isinstance(body, bytes) else js(body).encode()
    return {
        'event_id': str(uuid.uuid4()),
        'account': account,
        'fetched_at': now(),
        'operation': op,
        'variables': variables or {},
        'status': status,
        'body_base64': base64.b64encode(raw).decode(),
    }


def make_manifest(queries):
    q_dict = {k: MANIFEST['queries'][k] for k in queries if k in MANIFEST['queries']}
    return {
        **MANIFEST,
        'queries': q_dict,
        'expected': len(q_dict),
    }


class RankingIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmp.name) / 'test.sqlite'
        self.store = Store(self.db_path)
        self.store.db.execute("INSERT OR IGNORE INTO runs(id, started_at, status) VALUES(1, ?, 'completed')", (now(),))
        self.store.db.commit()

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def _insert_period_entity(self, account, period_id, start_time, end_time, event_id):
        sha = f'sha-period-{period_id}'
        self.store.db.execute('INSERT OR IGNORE INTO bodies VALUES(?,?,?)', (sha, b'{}', 2))
        self.store.db.execute(
            '''INSERT INTO responses(event_id, run_id, account, fetched_at, operation, variables_json, headers_json, body_sha256, json_text, projected)
               VALUES(?,1,?,'2026-09-01T00:00:00Z','EventMatchRankingQuery','{}','{}',?,?,'1')''',
            (str(uuid.uuid4()), account, sha, js({'data': {}})),
        )
        rid = self.store.db.execute('SELECT last_insert_rowid()').fetchone()[0]
        data = {
            'id': period_id,
            'startTime': start_time,
            'endTime': end_time,
            'leagueMatchSetting': {
                'leagueMatchEvent': {'id': event_id}
            }
        }
        self.store.db.execute(
            'INSERT INTO entities VALUES (?, ?, ?, ?, ?)',
            (account, 'LeagueMatchRankingTimePeriod', period_id, rid, js(data)),
        )
        self.store.db.commit()
        return rid

    def _insert_canonical_event_detail(self, account, match_key, event_id, played_time):
        sha = f'sha-event-detail-{account}-{match_key}'
        detail_obj = {
            'id': f'vs-event-{match_key}',
            'playedTime': played_time,
            'leagueMatch': {
                'leagueMatchEvent': {'id': event_id}
            }
        }
        self.store.db.execute('INSERT OR IGNORE INTO bodies VALUES(?,?,?)', (sha, b'{}', 2))
        self.store.db.execute(
            '''INSERT INTO responses(event_id, run_id, account, fetched_at, operation, variables_json, headers_json, body_sha256, json_text, projected)
               VALUES(?,1,?,?,'VsHistoryDetailQuery','{}','{}',?,?,'1')''',
            (str(uuid.uuid4()), account, played_time, sha, js({'data': detail_obj})),
        )
        rid = self.store.db.execute('SELECT last_insert_rowid()').fetchone()[0]
        self.store.db.execute(
            '''INSERT INTO matches(account, kind, match_key, first_seen, last_seen, detail_response_id)
               VALUES(?,?,?,?,?,?)''',
            (account, 'vs', match_key, played_time, played_time, rid),
        )
        self.store.db.execute(
            '''INSERT INTO documents(response_id, account, kind, match_key, json_text)
               VALUES(?,?,?,?,?)''',
            (rid, account, 'vs', match_key, js(detail_obj)),
        )
        self.store.db.execute(
            '''INSERT INTO match_classification(
                   account, kind, match_key, genre, roster_class, analysis_set,
                   detail_response_id, classified_at
               ) VALUES(?,?,?,'event','normal','regular',?,?)''',
            (account, 'vs', match_key, rid, played_time),
        )
        self.store.db.commit()
        return rid

    def _insert_x_season_entity(self, account, season_id, is_current, end_time):
        sha = f'sha-season-{season_id}'
        self.store.db.execute('INSERT OR IGNORE INTO bodies VALUES(?,?,?)', (sha, b'{}', 2))
        self.store.db.execute(
            '''INSERT INTO responses(event_id, run_id, account, fetched_at, operation, variables_json, headers_json, body_sha256, json_text, projected)
               VALUES(?,1,?,'2026-08-01T00:00:00Z','XRankingQuery','{}','{}',?,?,'1')''',
            (str(uuid.uuid4()), account, sha, js({'data': {}})),
        )
        rid = self.store.db.execute('SELECT last_insert_rowid()').fetchone()[0]
        data = {
            'id': season_id,
            'isCurrent': is_current,
            'endTime': end_time,
        }
        self.store.db.execute(
            'INSERT INTO entities VALUES (?, ?, ?, ?, ?)',
            (account, 'XRankingSeason', season_id, rid, js(data)),
        )
        self.store.db.commit()
        return rid

    def test_1_unparticipated_event_ranking_period_never_sent_to_bridge(self):
        """(1) 既存のEventMatchRankingPeriodQueryジョブで参加証拠なし→Bridge.queryに一度も送られない。
        負例: 参加証拠が存在するジョブはBridge.queryに送られdone_finalとなる。
        """
        account = 'acc-synthetic-1'
        manifest = make_manifest(['EventMatchRankingPeriodQuery'])

        # 未参加 period: 証拠なし
        self._insert_period_entity(
            account=account,
            period_id='period-unparticipated',
            start_time='2026-09-01T00:00:00Z',
            end_time='2026-09-01T02:00:00Z',
            event_id='event-unparticipated',
        )
        self.store.queue(account, 'EventMatchRankingPeriodQuery', {'eventMatchRankingPeriodId': 'period-unparticipated'})

        # 参加済み period (負例): 証拠あり
        self._insert_period_entity(
            account=account,
            period_id='period-participated',
            start_time='2026-09-01T02:00:00Z',
            end_time='2026-09-01T04:00:00Z',
            event_id='event-participated',
        )
        self._insert_canonical_event_detail(
            account=account,
            match_key='match-part-1',
            event_id='event-participated',
            played_time='2026-09-01T03:00:00Z',
        )
        self.store.queue(account, 'EventMatchRankingPeriodQuery', {'eventMatchRankingPeriodId': 'period-participated'})
        self.store.db.commit()

        seen_queries = []
        store = self.store

        class Bridge:
            def call(self, command, **kw):
                if command == 'init':
                    return {'account': account, 'country': 'JP'}
                op = kw['operation']
                v = kw.get('variables', {})
                seen_queries.append((op, v))
                body = {'data': {'eventMatchRankingPeriod': {'id': v.get('eventMatchRankingPeriodId')}}}
                e = response(op, body, account=account, variables=v)
                p = store.output_root / 'spool' / (e['event_id'] + '.json')
                p.parent.mkdir(parents=True, exist_ok=True)
                p.write_text(json.dumps(e))
                return {'spool_file': str(p)}

            def close(self):
                pass

        with patch('ikarchive.collector.catalog', return_value=manifest), \
             patch('ikarchive.collector.Bridge', Bridge), \
             patch('ikarchive.collector.fetch_assets', return_value=0), \
             patch('ikarchive.collector.storage_health', return_value={'state': 'normal'}), \
             patch('ikarchive.publish.publish_outputs', return_value={}):
            result = sync(self.store, account=account, budget=10, delay=0)

        # 検証 (1): 未参加 period は一度も Bridge.query に送られない
        unpart_sent = [v for op, v in seen_queries if v.get('eventMatchRankingPeriodId') == 'period-unparticipated']
        self.assertEqual(unpart_sent, [], '未参加のEventMatchRankingPeriodQueryがBridge.queryに送信されてはならない')

        # 負例: 参加済み period は Bridge.query に送信される
        part_sent = [v for op, v in seen_queries if v.get('eventMatchRankingPeriodId') == 'period-participated']
        self.assertEqual(len(part_sent), 1, '参加済みのEventMatchRankingPeriodQueryはBridge.queryに送信されなければならない')

        # DB内のジョブ状態の検証
        jobs = {
            json.loads(j['variables_json'])['eventMatchRankingPeriodId']: j['state']
            for j in self.store.db.execute("SELECT variables_json, state FROM jobs WHERE account=? AND operation='EventMatchRankingPeriodQuery'", (account,)).fetchall()
        }
        self.assertEqual(jobs['period-unparticipated'], 'out_of_scope')
        self.assertEqual(jobs['period-participated'], 'done_final')

    def test_2_history_and_detail_precede_scope_and_newly_queued_unparticipated_period_out_of_scope(self):
        """(2) 最新履歴/完全試合詳細がscope処理より先に実行でき、
        期間metadata応答から新規にqueueされた不参加periodも次のBridge要求前にout_of_scopeとなる。
        負例: 期間metadata応答から新規にqueueされた参加periodはBridge要求に送られる。
        """
        account = 'acc-synthetic-2'
        manifest = make_manifest([
            'LatestBattleHistoriesQuery',
            'VsHistoryDetailQuery',
            'EventMatchRankingQuery',
            'EventMatchRankingPeriodQuery',
        ])

        match_key = '20260902T010000_11111111-1111-1111-1111-111111111111'
        match_id = encoded(f'VsHistoryDetail-u-test:EVENT:{match_key}')

        # 試合詳細ジョブを先行して queue (priority 1)
        self.store.queue(account, 'VsHistoryDetailQuery', {'vsResultId': match_id})
        self.store.db.commit()

        seen_queries = []
        store = self.store

        class Bridge:
            def call(self, command, **kw):
                if command == 'init':
                    return {'account': account, 'country': 'JP'}
                op = kw['operation']
                v = kw.get('variables', {})
                seen_queries.append((op, v))

                if op == 'LatestBattleHistoriesQuery':
                    body = {'data': {'latestBattleHistories': {'historyGroups': {'nodes': []}}}}
                elif op == 'VsHistoryDetailQuery':
                    # 試合詳細の返信: event-part への参加証拠を含む
                    body = {
                        'data': {
                            'vsHistoryDetail': {
                                'id': match_id,
                                'vsMode': {'mode': 'LEAGUE'},
                                'playedTime': '2026-09-02T01:30:00Z',
                                'leagueMatch': {
                                    'leagueMatchEvent': {'id': 'event-part'}
                                },
                                'myTeam': {'players': []},
                                'otherTeams': [],
                            }
                        }
                    }
                elif op == 'EventMatchRankingQuery':
                    # 期間 metadata の返信: 不参加 period と参加 period の両方を返す
                    body = {
                        'data': {
                            'leagueMatchRankingSeasons': {
                                'edges': [{
                                    'node': {
                                        'id': 'season-metadata-1',
                                        'leagueMatchRankingTimePeriodGroups': {
                                            'edges': [
                                                {
                                                    'node': {
                                                        '__typename': 'LeagueMatchRankingTimePeriodGroup',
                                                        'id': 'group-metadata-unpart',
                                                        'leagueMatchSetting': {
                                                            'leagueMatchEvent': {'id': 'event-unpart'}
                                                        },
                                                        'timePeriods': [{
                                                            '__typename': 'LeagueMatchRankingTimePeriod',
                                                            'id': 'period-meta-unpart',
                                                            'startTime': '2026-09-01T00:00:00Z',
                                                            'endTime': '2026-09-01T02:00:00Z',
                                                        }]
                                                    }
                                                },
                                                {
                                                    'node': {
                                                        '__typename': 'LeagueMatchRankingTimePeriodGroup',
                                                        'id': 'group-metadata-part',
                                                        'leagueMatchSetting': {
                                                            'leagueMatchEvent': {'id': 'event-part'}
                                                        },
                                                        'timePeriods': [{
                                                            '__typename': 'LeagueMatchRankingTimePeriod',
                                                            'id': 'period-meta-part',
                                                            'startTime': '2026-09-02T00:00:00Z',
                                                            'endTime': '2026-09-02T02:00:00Z',
                                                        }]
                                                    }
                                                }
                                            ]
                                        }
                                    }
                                }]
                            }
                        }
                    }
                elif op == 'EventMatchRankingPeriodQuery':
                    body = {'data': {'eventMatchRankingPeriod': {'id': v.get('eventMatchRankingPeriodId')}}}
                else:
                    body = {'data': {}}

                e = response(op, body, account=account, variables=v)
                p = store.output_root / 'spool' / (e['event_id'] + '.json')
                p.parent.mkdir(parents=True, exist_ok=True)
                p.write_text(json.dumps(e))
                return {'spool_file': str(p)}

            def close(self):
                pass

        with patch('ikarchive.collector.catalog', return_value=manifest), \
             patch('ikarchive.collector.Bridge', Bridge), \
             patch('ikarchive.collector.fetch_assets', return_value=0), \
             patch('ikarchive.collector.storage_health', return_value={'state': 'normal'}), \
             patch('ikarchive.publish.publish_outputs', return_value={}):
            result = sync(self.store, account=account, budget=10, delay=0)

        ops_sequence = [op for op, v in seen_queries]

        # 検証 (2a): 最新履歴 (priority 0) と試合詳細 (priority 1) がランキングクエリより先に実行される
        self.assertIn('LatestBattleHistoriesQuery', ops_sequence)
        self.assertIn('VsHistoryDetailQuery', ops_sequence)
        latest_idx = ops_sequence.index('LatestBattleHistoriesQuery')
        vs_idx = ops_sequence.index('VsHistoryDetailQuery')
        ranking_meta_idx = ops_sequence.index('EventMatchRankingQuery')
        self.assertLess(latest_idx, ranking_meta_idx, 'LatestBattleHistoriesQueryはランキングメタデータより先に実行される')
        self.assertLess(vs_idx, ranking_meta_idx, 'VsHistoryDetailQueryはランキングメタデータより先に実行される')

        # 検証 (2b): EventMatchRankingQuery から新規に queue された不参加 period は一度も Bridge.query に送られない
        unpart_ranking_sent = [v for op, v in seen_queries if op == 'EventMatchRankingPeriodQuery' and v.get('eventMatchRankingPeriodId') == 'period-meta-unpart']
        self.assertEqual(unpart_ranking_sent, [], 'メタデータ応答から新規queueされた不参加periodはBridge.queryに送られてはならない')

        # 負例: 試合詳細で証拠が取得できた参加 period は Bridge.query に送信される
        part_ranking_sent = [v for op, v in seen_queries if op == 'EventMatchRankingPeriodQuery' and v.get('eventMatchRankingPeriodId') == 'period-meta-part']
        self.assertEqual(len(part_ranking_sent), 1, 'メタデータ応答から新規queueされた参加periodはBridge.queryに送信されなければならない')

        # ジョブ状態の確認
        jobs = {
            json.loads(j['variables_json']).get('eventMatchRankingPeriodId'): j['state']
            for j in self.store.db.execute("SELECT variables_json, state FROM jobs WHERE account=? AND operation='EventMatchRankingPeriodQuery'", (account,)).fetchall()
        }
        self.assertEqual(jobs.get('period-meta-unpart'), 'out_of_scope')
        self.assertEqual(jobs.get('period-meta-part'), 'done_final')

    def test_3_x_ranking_detail_refetch_never_sent(self):
        """(3) XRankingDetailRefetchQueryは一度も送られない。
        負例: 終了済みシーズンのXRankingDetailQueryは適格としてBridge.queryに送られる。
        """
        account = 'acc-synthetic-3'
        manifest = make_manifest(['XRankingDetailRefetchQuery', 'XRankingDetailQuery'])

        # 終了済みシーズンエンティティ
        self._insert_x_season_entity(
            account=account,
            season_id='season-ended-2026',
            is_current=False,
            end_time='2026-08-01T00:00:00Z',
        )

        # XRankingDetailRefetchQuery と XRankingDetailQuery を両方 queue
        self.store.queue(account, 'XRankingDetailRefetchQuery', {'id': 'season-ended-2026'})
        self.store.queue(account, 'XRankingDetailQuery', {'id': 'season-ended-2026'})
        self.store.db.commit()

        seen_queries = []
        store = self.store

        class Bridge:
            def call(self, command, **kw):
                if command == 'init':
                    return {'account': account, 'country': 'JP'}
                op = kw['operation']
                v = kw.get('variables', {})
                seen_queries.append((op, v))
                body = {'data': {'xRanking': {'id': v.get('id')}}}
                e = response(op, body, account=account, variables=v)
                p = store.output_root / 'spool' / (e['event_id'] + '.json')
                p.parent.mkdir(parents=True, exist_ok=True)
                p.write_text(json.dumps(e))
                return {'spool_file': str(p)}

            def close(self):
                pass

        with patch('ikarchive.collector.catalog', return_value=manifest), \
             patch('ikarchive.collector.Bridge', Bridge), \
             patch('ikarchive.collector.fetch_assets', return_value=0), \
             patch('ikarchive.collector.storage_health', return_value={'state': 'normal'}), \
             patch('ikarchive.publish.publish_outputs', return_value={}):
            result = sync(self.store, account=account, budget=10, delay=0)

        seen_ops = [op for op, v in seen_queries]

        # 検証 (3): XRankingDetailRefetchQuery は一度も送られない
        self.assertNotIn('XRankingDetailRefetchQuery', seen_ops, 'XRankingDetailRefetchQueryがBridge.queryに送信されてはならない')

        # 負例: XRankingDetailQuery は送信される
        self.assertIn('XRankingDetailQuery', seen_ops, '終了シーズンのXRankingDetailQueryはBridge.queryに送信されなければならない')

        # ジョブ状態の確認
        refetch_job = self.store.db.execute(
            "SELECT state FROM jobs WHERE account=? AND operation='XRankingDetailRefetchQuery'",
            (account,),
        ).fetchone()
        detail_job = self.store.db.execute(
            "SELECT state FROM jobs WHERE account=? AND operation='XRankingDetailQuery'",
            (account,),
        ).fetchone()

        self.assertEqual(refetch_job['state'], 'superseded')
        self.assertEqual(detail_job['state'], 'done_final')

    def test_4_done_final_job_not_re_requested_on_next_day_sync(self):
        """(4) 終了後に取得済みのdone_finalジョブが翌日syncでも再要求されない。
        負例: 未取得 (pending) の詳細の仕事は要求される。成功済み (done) の詳細の仕事は、next_attempt が
        過ぎても翌日syncでpendingに戻らず再要求されない (取得に成功した試合詳細を取り直さない)。
        """
        account = 'acc-synthetic-4'
        manifest = make_manifest(['XRankingDetailQuery', 'VsHistoryDetailQuery'])

        # 過去に終了したシーズンエンティティ
        rid_season = self._insert_x_season_entity(
            account=account,
            season_id='season-final-completed',
            is_current=False,
            end_time='2026-08-01T00:00:00Z',
        )

        # 終了日時以降に取得済みの詳細応答レコード (final receipt)
        sha_detail = 'sha-detail-receipt-done-final'
        self.store.db.execute('INSERT OR IGNORE INTO bodies VALUES(?,?,?)', (sha_detail, b'{}', 2))
        self.store.db.execute(
            '''INSERT INTO responses(event_id, run_id, account, fetched_at, operation, variables_json, headers_json, body_sha256, json_text, projected)
               VALUES(?,1,?,'2026-08-02T12:00:00Z','XRankingDetailQuery',?,'{}',?,?,'1')''',
            (str(uuid.uuid4()), account, js({'id': 'season-final-completed'}), sha_detail, js({'data': {'xRanking': {'id': 'season-final-completed'}}})),
        )
        rid_detail = self.store.db.execute('SELECT last_insert_rowid()').fetchone()[0]
        self.store.db.execute(
            '''INSERT INTO response_fetches(event_id, response_id, run_id, fetched_at, headers_json, acknowledged)
               VALUES(?,?,?,'2026-08-02T12:00:00Z','{}',1)''',
            (str(uuid.uuid4()), rid_detail, 1),
        )

        # 対象ジョブを done_final 状態で配置 (next_attempt=0 で翌日再訪期限切れを模擬)
        self.store.queue(account, 'XRankingDetailQuery', {'id': 'season-final-completed'})
        self.store.db.execute(
            "UPDATE jobs SET state='done_final', next_attempt=0, last_response_id=? WHERE account=? AND operation='XRankingDetailQuery'",
            (rid_detail, account),
        )

        # 負例: 成功済みの done ジョブ (VsHistoryDetailQuery) を配置 (next_attempt=0)。再要求されない。
        self.store.queue(account, 'VsHistoryDetailQuery', {'vsResultId': 'vs-test-normal'})
        self.store.db.execute(
            "UPDATE jobs SET state='done', next_attempt=0 WHERE account=? AND operation='VsHistoryDetailQuery'",
            (account,),
        )
        # 負例 (空振りの防止): 未取得の pending ジョブ (VsHistoryDetailQuery) を配置。こちらは要求される。
        self.store.queue(account, 'VsHistoryDetailQuery', {'vsResultId': 'vs-test-pending'})
        self.store.db.commit()

        seen_queries = []
        store = self.store

        class Bridge:
            def call(self, command, **kw):
                if command == 'init':
                    return {'account': account, 'country': 'JP'}
                op = kw['operation']
                v = kw.get('variables', {})
                seen_queries.append((op, v))
                body = {'data': {'vsHistoryDetail': {'id': v.get('vsResultId')}}}
                e = response(op, body, account=account, variables=v)
                p = store.output_root / 'spool' / (e['event_id'] + '.json')
                p.parent.mkdir(parents=True, exist_ok=True)
                p.write_text(json.dumps(e))
                return {'spool_file': str(p)}

            def close(self):
                pass

        with patch('ikarchive.collector.catalog', return_value=manifest), \
             patch('ikarchive.collector.Bridge', Bridge), \
             patch('ikarchive.collector.fetch_assets', return_value=0), \
             patch('ikarchive.collector.storage_health', return_value={'state': 'normal'}), \
             patch('ikarchive.publish.publish_outputs', return_value={}):
            result = sync(self.store, account=account, budget=10, delay=0)

        seen_ops = [op for op, v in seen_queries]

        # 検証 (4): 終了後取得済みの done_final ジョブは翌日 sync でも再要求されない
        self.assertNotIn('XRankingDetailQuery', seen_ops, 'done_finalジョブが翌日syncで再要求されてはならない')

        # 負例: 未取得 (pending) の詳細の仕事だけが要求される。成功済み (done) の詳細の仕事は pending に戻らず再要求されない
        self.assertIn('VsHistoryDetailQuery', seen_ops, '未取得のpendingジョブはsyncで要求されなければならない')
        requested = [v for op, v in seen_queries if op == 'VsHistoryDetailQuery']
        self.assertEqual(requested, [{'vsResultId': 'vs-test-pending'}], '成功済みのdoneジョブは翌日syncで再要求されてはならない')
        normal_job = self.store.db.execute(
            "SELECT state FROM jobs WHERE account=? AND operation='VsHistoryDetailQuery' AND variables_json=?",
            (account, js({'vsResultId': 'vs-test-normal'})),
        ).fetchone()
        self.assertEqual(normal_job['state'], 'done')

        # ジョブ状態の確認
        final_job = self.store.db.execute(
            "SELECT state FROM jobs WHERE account=? AND operation='XRankingDetailQuery'",
            (account,),
        ).fetchone()
        self.assertEqual(final_job['state'], 'done_final')

    def test_5_bankara_and_vs_detail_unrestricted_can_be_sent(self):
        """(5) BankaraBattleHistoriesQueryとVsHistoryDetailQueryは対象外として送信可能。
        負例: 同一セッション内のスコープ対象かつ不適格な操作は遮断される。
        """
        account = 'acc-synthetic-5'
        manifest = make_manifest([
            'BankaraBattleHistoriesQuery',
            'VsHistoryDetailQuery',
            'EventMatchRankingPeriodQuery',
        ])

        match_id = encoded('VsHistoryDetail-u-test:BANKARA:20260902T010000_11111111-1111-1111-1111-111111111111')

        # 不適格なスコープ対象ジョブ (未参加 period) を配置
        self._insert_period_entity(
            account=account,
            period_id='period-unrestricted-test-unpart',
            start_time='2026-09-01T00:00:00Z',
            end_time='2026-09-01T02:00:00Z',
            event_id='event-unrestricted-test',
        )
        self.store.queue(account, 'EventMatchRankingPeriodQuery', {'eventMatchRankingPeriodId': 'period-unrestricted-test-unpart'})

        # 対象外の操作: BankaraBattleHistoriesQuery (root) と VsHistoryDetailQuery
        self.store.queue(account, 'VsHistoryDetailQuery', {'vsResultId': match_id})
        self.store.db.commit()

        seen_queries = []
        store = self.store

        class Bridge:
            def call(self, command, **kw):
                if command == 'init':
                    return {'account': account, 'country': 'JP'}
                op = kw['operation']
                v = kw.get('variables', {})
                seen_queries.append((op, v))

                if op == 'BankaraBattleHistoriesQuery':
                    body = {'data': {'bankaraBattleHistories': {'historyGroups': {'nodes': []}}}}
                elif op == 'VsHistoryDetailQuery':
                    body = {
                        'data': {
                            'vsHistoryDetail': {
                                'id': match_id,
                                'vsMode': {'mode': 'BANKARA'},
                                'bankaraMatch': {'mode': 'OPEN'},
                                'myTeam': {'players': []},
                                'otherTeams': [],
                            }
                        }
                    }
                else:
                    body = {'data': {}}

                e = response(op, body, account=account, variables=v)
                p = store.output_root / 'spool' / (e['event_id'] + '.json')
                p.parent.mkdir(parents=True, exist_ok=True)
                p.write_text(json.dumps(e))
                return {'spool_file': str(p)}

            def close(self):
                pass

        with patch('ikarchive.collector.catalog', return_value=manifest), \
             patch('ikarchive.collector.Bridge', Bridge), \
             patch('ikarchive.collector.fetch_assets', return_value=0), \
             patch('ikarchive.collector.storage_health', return_value={'state': 'normal'}), \
             patch('ikarchive.publish.publish_outputs', return_value={}):
            result = sync(self.store, account=account, budget=10, delay=0)

        seen_ops = [op for op, v in seen_queries]

        # 検証 (5): BankaraBattleHistoriesQuery と VsHistoryDetailQuery は対象外として送信可能
        self.assertIn('BankaraBattleHistoriesQuery', seen_ops, 'BankaraBattleHistoriesQueryが送信可能でなければならない')
        self.assertIn('VsHistoryDetailQuery', seen_ops, 'VsHistoryDetailQueryが送信可能でなければならない')

        # 負例: 同一セッション内の不適格なスコープ対象操作 (EventMatchRankingPeriodQuery) は送信されない
        self.assertNotIn('EventMatchRankingPeriodQuery', seen_ops, '不適格なスコープ対象操作は送信されてはならない')

        # ジョブ状態の確認
        jobs = {
            j['operation']: j['state']
            for j in self.store.db.execute("SELECT operation, state FROM jobs WHERE account=?", (account,)).fetchall()
        }
        self.assertEqual(jobs['BankaraBattleHistoriesQuery'], 'done')
        self.assertEqual(jobs['VsHistoryDetailQuery'], 'done')
        self.assertEqual(jobs['EventMatchRankingPeriodQuery'], 'out_of_scope')


if __name__ == '__main__':
    unittest.main()
