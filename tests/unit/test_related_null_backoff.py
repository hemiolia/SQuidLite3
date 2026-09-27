import copy
import sqlite3
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from test_archive import MANIFEST, Planner, Store, response


ROOTS = {
    'SaleGearDetailQuery': 'saleGear',
    'DownloadSearchReplayQuery': 'replay',
}
DELAYS = [300, 600, 1200, 2400, 4800, 9600, 19200, 38400, 76800, 86400, 86400]


class RelatedNullBackoffTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.tmp.name) / 'archive.sqlite3')
        self.planner = Planner(MANIFEST)

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def ingest(self, operation, body, *, status=200, query_id=None):
        event = response(operation, body, status=status)
        event['query_id'] = query_id
        rid = self.store.record(event)
        self.store.project(rid, self.planner)
        return rid, event

    def job(self, operation):
        return self.store.db.execute('SELECT * FROM jobs WHERE operation=?', (operation,)).fetchone()

    def issue_codes(self, operation):
        return [row[0] for row in self.store.db.execute(
            'SELECT i.code FROM issues i JOIN responses r ON r.id=i.response_id '
            'WHERE r.operation=? ORDER BY i.id', (operation,))]

    def test_identical_null_refetches_advance_independent_counter_to_cap(self):
        for operation, root in ROOTS.items():
            with self.subTest(operation=operation), patch('ikarchive.store.time.time', return_value=10000):
                self.store.queue('account-a', operation, {})
                self.store.db.execute('UPDATE jobs SET attempts=596 WHERE operation=?', (operation,))
                self.store.db.commit()
                response_ids = []
                for count, delay in enumerate(DELAYS, 1):
                    rid, event = self.ingest(operation, {'data': {root: None}},
                        query_id=MANIFEST['queries'][operation]['params']['id'])
                    response_ids.append(rid)
                    job = self.job(operation)
                    self.assertEqual((job['state'], job['empty_retries'], job['attempts']),
                        ('retry', count, 596 + count))
                    self.assertEqual(job['next_attempt'], 10000 + delay)
                    self.assertEqual(job['last_response_id'], rid)
                self.assertEqual(len(set(response_ids)), 1)
                self.assertEqual(self.store.db.execute(
                    'SELECT count(*) FROM response_fetches WHERE response_id=?',
                    (response_ids[0],)).fetchone()[0], len(DELAYS))
                self.assertEqual(self.issue_codes(operation), ['RELATED_DETAIL_EMPTY'])
                # Replaying the same event is idempotent; a new event with the same
                # body is a real new observation and was counted above.
                before = dict(self.job(operation))
                self.store.record(event)
                self.store.project(response_ids[-1], self.planner)
                self.assertEqual(dict(self.job(operation)), before)

    def test_other_outcomes_reset_streak_and_preserve_existing_delay(self):
        for operation, root in ROOTS.items():
            for case, body, status, query_id, expected_state, expected_issue in (
                ('success', {'data': {root: {'id': 'other'}}}, 200, None, 'done', None),
                ('graphql', {'data': {root: None}, 'errors': [{'message': 'error'}]}, 200, None, 'retry', 'INCOMPLETE_RESPONSE'),
                ('http', {'data': {root: None}}, 503, None, 'retry', 'INCOMPLETE_RESPONSE'),
                ('missing_root', {'data': {}}, 200, MANIFEST['queries'][operation]['params']['id'], 'retry', 'INCOMPLETE_RESPONSE'),
            ):
                with self.subTest(operation=operation, case=case), patch('ikarchive.store.time.time', return_value=20000):
                    self.store.queue('account-a', operation, {})
                    self.store.db.execute('UPDATE jobs SET empty_retries=3 WHERE operation=?', (operation,))
                    self.store.db.commit()
                    self.ingest(operation, body, status=status, query_id=query_id)
                    job = self.job(operation)
                    self.assertEqual((job['state'], job['empty_retries']), (expected_state, 0))
                    self.assertEqual(job['next_attempt'], 20000 + (86400 if expected_state == 'done' else 300))
                    if expected_issue:
                        self.assertIn(expected_issue, self.issue_codes(operation))
                    else:
                        self.assertNotIn('RELATED_DETAIL_EMPTY', self.issue_codes(operation))
                    self.store.db.execute('DELETE FROM jobs WHERE operation=?', (operation,))
                    self.store.db.commit()

    def test_v_s_and_coop_null_boundaries_remain_unavailable(self):
        for operation, root in (
            ('VsHistoryDetailQuery', 'vsHistoryDetail'),
            ('CoopHistoryDetailQuery', 'coopHistoryDetail'),
        ):
            with self.subTest(operation=operation):
                self.store.queue('account-a', operation, {})
                self.ingest(operation, {'data': {root: None}})
                self.assertEqual(self.job(operation)['state'], 'unavailable')
                self.assertEqual(self.job(operation)['empty_retries'], 0)
                self.assertEqual(self.issue_codes(operation), ['DETAIL_UNAVAILABLE'])

    def test_present_null_with_selected_field_omission_resets_counter(self):
        for operation, root in ROOTS.items():
            with self.subTest(operation=operation), patch('ikarchive.store.time.time', return_value=30000):
                manifest = copy.deepcopy(MANIFEST)
                manifest['queries'][operation]['operation']['selections'].append({
                    'kind': 'ScalarField', 'name': 'requiredSibling', 'alias': None,
                })
                self.planner = Planner(manifest)
                self.store.queue('account-a', operation, {})
                self.store.db.execute('UPDATE jobs SET empty_retries=4 WHERE operation=?', (operation,))
                self.store.db.commit()
                self.ingest(operation, {'data': {root: None}},
                    query_id=manifest['queries'][operation]['params']['id'])
                job = self.job(operation)
                self.assertEqual((job['state'], job['empty_retries'], job['next_attempt']),
                    ('retry', 0, 30300))
                self.assertEqual(self.issue_codes(operation),
                    ['SELECTED_FIELD_MISSING', 'INCOMPLETE_RESPONSE'])

    def test_first_null_after_reset_begins_new_audit_streak(self):
        operation, root = 'SaleGearDetailQuery', 'saleGear'
        self.store.queue('account-a', operation, {})
        self.ingest(operation, {'data': {root: None}})
        self.ingest(operation, {'data': {root: None}, 'errors': [{'message': 'error'}]})
        self.assertEqual(self.job(operation)['empty_retries'], 0)
        self.ingest(operation, {'data': {root: None}})
        self.assertEqual(self.job(operation)['empty_retries'], 1)
        self.assertEqual(self.issue_codes(operation).count('RELATED_DETAIL_EMPTY'), 2)

    def test_existing_jobs_table_migrates_without_resetting_old_job(self):
        self.store.close()
        path = Path(self.tmp.name) / 'legacy.sqlite3'
        conn = sqlite3.connect(path)
        conn.execute('CREATE TABLE jobs(account TEXT NOT NULL,operation TEXT NOT NULL,variables_json TEXT NOT NULL,'
            'kind TEXT,match_key TEXT,state TEXT NOT NULL DEFAULT \'pending\',attempts INTEGER NOT NULL DEFAULT 0,'
            'next_attempt REAL NOT NULL DEFAULT 0,last_response_id INTEGER,PRIMARY KEY(account,operation,variables_json))')
        conn.execute("INSERT INTO jobs(account,operation,variables_json,state,attempts,next_attempt) "
            "VALUES('account-a','SaleGearDetailQuery','{}','retry',596,12345)")
        conn.commit()
        conn.close()
        migrated = Store(path)
        try:
            row = migrated.db.execute('SELECT state,attempts,empty_retries,next_attempt FROM jobs').fetchone()
            self.assertEqual(tuple(row), ('retry', 596, 0, 12345))
            migrated.close()
            migrated = Store(path)
            row = migrated.db.execute('SELECT state,attempts,empty_retries,next_attempt FROM jobs').fetchone()
            self.assertEqual(tuple(row), ('retry', 596, 0, 12345))
        finally:
            migrated.close()


if __name__ == '__main__':
    unittest.main()
