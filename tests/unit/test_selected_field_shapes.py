import base64
import json
import sys
import tempfile
import unittest
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'src/python'))

from ikarchive.planner import Planner
from ikarchive.store import Store, js


def scalar(name, alias=None):
    return {'kind': 'ScalarField', 'name': name, 'alias': alias}


def linked(name, plural, selections=(), alias=None, concrete='Object'):
    return {
        'kind': 'LinkedField', 'name': name, 'alias': alias,
        'plural': plural, 'concreteType': concrete,
        'selections': list(selections),
    }


def query(selections, query_id='shape-qid'):
    return {
        'params': {'operationKind': 'query', 'id': query_id},
        'operation': {'argumentDefinitions': [], 'selections': list(selections)},
    }


def manifest():
    return {'queries': {
        'ShapeQuery': query([
            scalar('scalarValue'),
            linked('one', False, [scalar('value')], alias='oneAlias'),
            linked('many', True, [scalar('value'), linked('child', False, [])]),
            {'kind': 'Condition', 'condition': 'includeGuard', 'passingValue': True,
             'selections': [linked('guarded', False, [], alias='guardAlias')]},
            {'kind': 'InlineFragment', 'type': 'User', 'abstractKey': '__isNode',
             'selections': [
                 {'kind': 'TypeDiscriminator', 'abstractKey': '__isNode'},
                 linked('friends', True, [], alias='friendAlias', concrete='User'),
             ]},
        ]),
        'LatestBattleHistoriesQuery': query([
            linked('latestBattleHistories', False, [], concrete='HistoryCollection'),
        ], 'history-qid'),
        'VsHistoryDetailQuery': query([
            linked('vsHistoryDetail', False, [
                scalar('id'),
                linked('myTeam', False, [
                    linked('players', True, [], concrete='Player'),
                ], concrete='Team'),
            ], concrete='VsHistoryDetail'),
        ], 'detail-qid'),
    }}


def event(operation, raw, query_id, fetched_at, variables=None):
    return {
        'event_id': str(uuid.uuid4()),
        'account': 'shape-test',
        'fetched_at': fetched_at,
        'operation': operation,
        'variables': variables or {},
        'query_id': query_id,
        'status': 200,
        'headers': {},
        'body_base64': base64.b64encode(raw).decode('ascii'),
    }


class SelectedFieldShapeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.planner = Planner(manifest())

    def tearDown(self):
        self.tmp.cleanup()

    def test_singular_and_plural_shape_errors_use_selected_paths(self):
        cases = [
            ({'oneAlias': []}, [('oneAlias',)]),
            ({'oneAlias': 'object expected'}, [('oneAlias',)]),
            ({'many': {}}, [('many',)]),
            ({'many': 7}, [('many',)]),
            ({'many': [{'value': 'ok'}, 'bad']}, [('many', 1)]),
            ({'many': [{'child': []}]}, [('many', 0, 'child')]),
        ]
        for data, expected in cases:
            with self.subTest(data=data):
                self.assertEqual(
                    self.planner.invalid_shapes('ShapeQuery', data, {'includeGuard': False}),
                    expected,
                )

    def test_null_empty_and_null_elements_are_legal_and_scalar_types_are_not_guessed(self):
        data = {
            'scalarValue': {'arbitrary': 'scalar schema has no declared type'},
            'oneAlias': None,
            'many': [None, {'value': 'ok'}],
            'guardAlias': None,
            'friendAlias': [],
            '__typename': 'OtherType',
        }
        self.assertEqual(
            self.planner.invalid_shapes('ShapeQuery', data, {'includeGuard': True}),
            [],
        )
        data['many'] = []
        self.assertEqual(self.planner.invalid_shapes('ShapeQuery', data, {'includeGuard': True}), [])

    def test_shape_walk_respects_conditions_fragments_aliases_and_missing_keys(self):
        data = {
            '__typename': 'User',
            'scalarValue': None,
            'oneAlias': {'value': 1},
            'many': [],
            'guardAlias': [],
            'friendAlias': {},
        }
        self.assertEqual(
            self.planner.invalid_shapes('ShapeQuery', data, {'includeGuard': True}),
            [('guardAlias',), ('friendAlias',)],
        )
        self.assertEqual(
            self.planner.invalid_shapes('ShapeQuery', data, {'includeGuard': False}),
            [('friendAlias',)],
        )
        del data['oneAlias']
        self.assertEqual(
            self.planner.invalid_shapes('ShapeQuery', data, {'includeGuard': False}),
            [('friendAlias',)],
        )
        self.assertEqual(
            self.planner.missing_fields('ShapeQuery', data, {'includeGuard': False}),
            [('oneAlias',)],
        )
        self.assertEqual(self.planner.invalid_shapes('ShapeQuery', ['root is not an object'], {}), [])

    def test_invalid_history_shape_keeps_raw_body_retries_job_and_preserves_last_head(self):
        db = Path(self.tmp.name) / 'store.sqlite'
        store = Store(db)
        try:
            store.queue('shape-test', 'LatestBattleHistoriesQuery', {})
            good_raw = b'{"data":{"latestBattleHistories":{}}}'
            good = store.record(event(
                'LatestBattleHistoriesQuery', good_raw, 'history-qid', '2026-10-03T01:00:00+00:00',
            ))
            store.project(good, self.planner)
            self.assertEqual(
                store.db.execute("SELECT response_id FROM endpoint_heads WHERE operation='LatestBattleHistoriesQuery'").fetchone()[0],
                good,
            )

            bad_raw = b'{ "data" : { "latestBattleHistories" : [], "future" : {"x": 9007199254740993} } }\n'
            bad_event = event(
                'LatestBattleHistoriesQuery', bad_raw, 'history-qid', '2026-10-03T02:00:00+00:00',
            )
            bad = store.record(bad_event)
            store.project(bad, self.planner)

            self.assertEqual(store.db.execute('SELECT body FROM bodies WHERE sha256=(SELECT body_sha256 FROM responses WHERE id=?)', (bad,)).fetchone()[0], bad_raw)
            self.assertEqual(
                store.db.execute("SELECT response_id FROM endpoint_heads WHERE operation='LatestBattleHistoriesQuery'").fetchone()[0],
                good,
            )
            self.assertEqual(
                store.db.execute("SELECT state FROM jobs WHERE operation='LatestBattleHistoriesQuery'").fetchone()[0],
                'retry',
            )
            issues = store.db.execute(
                "SELECT code,context FROM issues WHERE response_id=? ORDER BY code", (bad,),
            ).fetchall()
            self.assertEqual([row['code'] for row in issues], ['INCOMPLETE_RESPONSE', 'SELECTED_FIELD_SHAPE_INVALID'])
            shape_context = json.loads(next(row['context'] for row in issues if row['code'] == 'SELECTED_FIELD_SHAPE_INVALID'))
            self.assertEqual(shape_context, {'operation': 'LatestBattleHistoriesQuery', 'path': ['latestBattleHistories']})

            clock = next(c for c in store.sync_health()['histories'] if c['operation'] == 'LatestBattleHistoriesQuery')
            self.assertEqual(clock['last_success_at'], '2026-10-03T01:00:00+00:00')
        finally:
            store.close()

    def test_shape_validation_keeps_existing_query_id_and_known_route_gate(self):
        store = Store(Path(self.tmp.name) / 'store.sqlite')
        try:
            store.queue('shape-test', 'LatestBattleHistoriesQuery', {})
            raw = b'{"data":{"latestBattleHistories":[]}}'
            item = event(
                'LatestBattleHistoriesQuery', raw, None, '2026-10-03T01:00:00+00:00',
            )
            rid = store.record(item)
            store.project(rid, self.planner)
            self.assertEqual(store.db.execute(
                "SELECT count(*) FROM issues WHERE response_id=? AND code='SELECTED_FIELD_SHAPE_INVALID'", (rid,),
            ).fetchone()[0], 0)
            self.assertEqual(store.db.execute(
                "SELECT state FROM jobs WHERE operation='LatestBattleHistoriesQuery'",
            ).fetchone()[0], 'done')

            store.queue('shape-test', 'UnknownFutureQuery', {})
            unknown = store.record(event(
                'UnknownFutureQuery', b'{"data":{"future":[]}}', 'future-qid',
                '2026-10-03T02:00:00+00:00',
            ))
            store.project(unknown, self.planner)
            self.assertEqual(store.db.execute(
                "SELECT count(*) FROM issues WHERE response_id=? AND code='SELECTED_FIELD_SHAPE_INVALID'", (unknown,),
            ).fetchone()[0], 0)
        finally:
            store.close()

    def test_invalid_detail_deduplicated_reack_cannot_replace_canonical_complete_detail(self):
        store = Store(Path(self.tmp.name) / 'store.sqlite')
        try:
            remote_id = base64.b64encode(
                b'VsHistoryDetail-u-shape:REGULAR:20261003T010101_result'
            ).decode('ascii')
            variables = {'vsResultId': remote_id}
            store.queue('shape-test', 'VsHistoryDetailQuery', variables, 'vs', 'u-shape:20261003T010101_result')

            original_raw = js({'data': {'vsHistoryDetail': {
                'id': remote_id,
                'myTeam': {'players': []},
                'futureField': {'raw': 'kept'},
            }}}).encode('utf-8')
            original = store.record(event(
                'VsHistoryDetailQuery', original_raw, 'detail-qid', '2026-10-03T01:00:00+00:00', variables,
            ))
            store.project(original, self.planner)
            canonical_before = store.db.execute(
                "SELECT detail_response_id FROM matches WHERE account='shape-test' AND match_key='u-shape:20261003T010101_result'",
            ).fetchone()[0]
            self.assertEqual(canonical_before, original)

            # The selected singular `myTeam` is a list here. Keep the JSON's exact
            # spacing and an unknown field so this also checks raw-byte preservation.
            bad_raw = (
                b'{ "data" : { "vsHistoryDetail" : { "id" : "' + remote_id.encode('ascii') +
                b'", "myTeam" : [], "futureField" : { "unknown" : [null, 9007199254740995] } } } }\n'
            )
            first_fetch = event(
                'VsHistoryDetailQuery', bad_raw, 'detail-qid', '2026-10-03T02:00:00+00:00', variables,
            )
            bad = store.record(first_fetch)
            store.project(bad, self.planner)
            self.assertEqual(
                store.db.execute('SELECT body FROM bodies WHERE sha256=(SELECT body_sha256 FROM responses WHERE id=?)', (bad,)).fetchone()[0],
                bad_raw,
            )
            self.assertEqual(
                store.db.execute("SELECT detail_response_id FROM matches WHERE account='shape-test' AND match_key='u-shape:20261003T010101_result'").fetchone()[0],
                original,
            )
            canonical_text = store.db.execute(
                "SELECT json_text FROM match_details WHERE account='shape-test' AND match_key='u-shape:20261003T010101_result'",
            ).fetchone()[0]
            self.assertIn('kept', canonical_text)
            job = store.db.execute(
                "SELECT state,attempts,last_response_id FROM jobs WHERE operation='VsHistoryDetailQuery' AND variables_json=?",
                (js(variables),),
            ).fetchone()
            self.assertEqual((job['state'], job['last_response_id']), ('retry', bad))
            first_attempts = job['attempts']

            duplicate_receipt = event(
                'VsHistoryDetailQuery', bad_raw, 'detail-qid', '2026-10-03T03:00:00+00:00', variables,
            )
            same_response = store.record(duplicate_receipt)
            self.assertEqual(same_response, bad)
            store.project(same_response, self.planner)

            self.assertEqual(
                store.db.execute("SELECT detail_response_id FROM matches WHERE account='shape-test' AND match_key='u-shape:20261003T010101_result'").fetchone()[0],
                original,
            )
            job = store.db.execute(
                "SELECT state,attempts,last_response_id FROM jobs WHERE operation='VsHistoryDetailQuery' AND variables_json=?",
                (js(variables),),
            ).fetchone()
            self.assertEqual(job['state'], 'retry')
            self.assertEqual(job['last_response_id'], bad)
            self.assertEqual(job['attempts'], first_attempts + 1)
            self.assertEqual(store.db.execute(
                "SELECT count(*) FROM issues WHERE response_id=? AND code='SELECTED_FIELD_SHAPE_INVALID'", (bad,),
            ).fetchone()[0], 1)
            shape_context = json.loads(store.db.execute(
                "SELECT context FROM issues WHERE response_id=? AND code='SELECTED_FIELD_SHAPE_INVALID'", (bad,),
            ).fetchone()[0])
            self.assertEqual(shape_context, {'operation': 'VsHistoryDetailQuery', 'path': ['vsHistoryDetail', 'myTeam']})
        finally:
            store.close()


if __name__ == '__main__':
    unittest.main()
