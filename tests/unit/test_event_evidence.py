"""イベント記録があるのに分類試合がない状態を、無参加と誤判定しない。"""

import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / 'src/python'))
from archive import audit
from ikarchive.event_evidence import event_evidence
from ikarchive.store import Store


class EventEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(':memory:')
        self.db.executescript('''
            CREATE TABLE responses(account TEXT, operation TEXT, http_status INTEGER,
                                   projected INTEGER, json_text TEXT);
            CREATE TABLE match_classification(analysis_set TEXT);
        ''')

    def tearDown(self):
        self.db.close()

    def put(self, operation, data, status=200, projected=1, account='a'):
        self.db.execute(
            'INSERT INTO responses VALUES (?,?,?,?,?)',
            (account, operation, status, projected, json.dumps({'data': data})),
        )

    def test_event_results_are_separate_from_classified_matches(self):
        self.put('DetailFestRecordDetailQuery', {'fest': {'id': 'event-a', 'playerResult': {'grade': {}}}})
        self.put('DetailFestRefethQuery', {'fest': {'id': 'event-a', 'playerResult': {'grade': {}}}})
        self.put('DetailFestRecordDetailQuery', {'fest': {'id': 'event-b', 'playerResult': None}})
        def record(day, score):
            return {'node': {'startTime': day, 'endTime': day + 'Z',
                             'coopStage': {'id': 'stage-a'}, 'highestJobScore': score}}
        first = record('a', 175)
        zero = record('b', 0)
        last = record('c', 156)
        self.put('CoopRecordQuery', {'coopRecord': {
            'bigRunRecord': {'records': {'edges': [first, zero]}},
            'teamContestRecord': {'attend': 6},
        }})
        self.put('CoopRecordRefetchQuery', {'coopRecord': {
            'bigRunRecord': {'records': {'edges': [first]}},
            'teamContestRecord': {'attend': 5},
        }})
        self.put('CoopRecordBigRunRecordContainerPaginationQuery', {'coopRecord': {
            'bigRunRecord': {'records': {'edges': [last]}},
        }})
        # 失敗・未投影の応答は参加証拠ではない。
        self.put('DetailFestRecordDetailQuery', {'fest': {'id': 'event-c', 'playerResult': {}}},
                 status=503)
        self.put('DetailFestRecordDetailQuery', {'fest': {'id': 'event-d', 'playerResult': {}}},
                 projected=0)
        result = event_evidence(self.db)
        self.assertEqual(result['fest_event_details'], 2)
        self.assertEqual(result['fest_events_with_player_result'], 1)
        self.assertEqual(result['big_run_event_records'], 3)
        self.assertEqual(result['big_run_events_with_positive_highest_job_score'], 2)
        self.assertEqual(result['team_contest_max_observed_attend'], 6)
        self.assertEqual(result['classified_match_counts'],
                         {'fest': 0, 'big_run': 0, 'team_contest': 0})
        self.assertEqual(result['unusable_event_responses_or_rows'], 0)

    def test_nulls_and_unkeyed_rows_are_not_play_evidence(self):
        self.put('DetailFestRecordDetailQuery', {'fest': None})
        self.put('CoopRecordQuery', {'coopRecord': {
            'teamContestRecord': {'attend': True},
            'bigRunRecord': {'records': {'edges': [{'node': {'highestJobScore': 999}}]}},
        }})
        result = event_evidence(self.db)
        self.assertEqual(result['fest_events_with_player_result'], 0)
        self.assertEqual(result['big_run_events_with_positive_highest_job_score'], 0)
        self.assertIsNone(result['team_contest_max_observed_attend'])
        self.assertEqual(result['unusable_event_responses_or_rows'], 2)

    def test_audit_exposes_event_evidence_without_claiming_complete_coverage(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = Store(Path(temporary) / 'artificial.sqlite3')
            try:
                manifest = (ROOT / 'config/query-catalog.snapshot.json').read_text(encoding='utf-8')
                store.db.execute(
                    "INSERT INTO manifests(sha256,fetched_at,json_text) VALUES('artificial','2026-09-22',?)",
                    (manifest,),
                )
                # 正本のバイト列や識別子は audit の出力に混ぜない。
                store.db.execute(
                    "INSERT INTO bodies(sha256,body,byte_length) VALUES('artificial',x'7b7d',2)"
                )
                store.db.execute(
                    """INSERT INTO responses(
                        event_id,account,fetched_at,operation,variables_json,headers_json,
                        http_status,body_sha256,json_text,projected)
                        VALUES('artificial-event','artificial-account','2026-09-22',
                               'DetailFestRecordDetailQuery','{}','{}',200,'artificial',
                               '{"data":{"fest":{"id":"private-event-id","playerResult":{}}}}',1)"""
                )
                result = audit(store)
                self.assertEqual(result['event_evidence']['fest_events_with_player_result'], 1)
                self.assertEqual(result['event_evidence']['classified_match_counts']['fest'], 0)
                self.assertFalse(result['all_server_records_verified'])
                self.assertNotIn('private-event-id', json.dumps(result))
                self.assertNotIn('artificial-account', json.dumps(result['event_evidence']))
            finally:
                store.close()


if __name__ == '__main__':
    unittest.main()
