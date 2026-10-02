import base64
import contextlib
import hashlib
import io
import json
import sqlite3
import sys
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / 'src/python'))

import archive
from ikarchive.planner import Planner
from ikarchive.store import Store, js


def scalar(name):
    return {'kind': 'ScalarField', 'name': name, 'alias': None}


def linked(name, selections=(), plural=False, concrete='Object', args=None):
    field = {
        'kind': 'LinkedField', 'name': name, 'alias': None,
        'plural': plural, 'concreteType': concrete,
        'selections': list(selections),
    }
    if args is not None:
        field['args'] = args
    return field


def operation(query_id, selections, argument_definitions=()):
    return {
        'params': {'operationKind': 'query', 'id': query_id},
        'operation': {
            'argumentDefinitions': list(argument_definitions),
            'selections': list(selections),
        },
    }


def manifest(version, queries):
    return {'version': version, 'queries': queries}


def add_manifest(store, value, fetched_at):
    text = js(value)
    sha = hashlib.sha256(text.encode('utf-8')).hexdigest()
    store.db.execute(
        'INSERT OR IGNORE INTO manifests(sha256,fetched_at,json_text) VALUES(?,?,?)',
        (sha, fetched_at, text),
    )
    store.db.commit()
    return sha


def event(operation_name, raw, query_id, app_version, fetched_at, variables=None):
    return {
        'event_id': str(uuid.uuid4()),
        'account': 'catalog-test',
        'fetched_at': fetched_at,
        'operation': operation_name,
        'variables': variables or {},
        'query_id': query_id,
        'app_version': app_version,
        'status': 200,
        'headers': {'x-test': 'retained'},
        'body_base64': base64.b64encode(raw).decode('ascii'),
    }


def response_body(store, response_id):
    return store.db.execute(
        'SELECT b.body FROM responses r JOIN bodies b ON b.sha256=r.body_sha256 WHERE r.id=?',
        (response_id,),
    ).fetchone()[0]


class ResponseCatalogBindingTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.stores = []

    def tearDown(self):
        for store in self.stores:
            store.close()
        self.tmp.cleanup()

    def new_store(self, name='archive.sqlite'):
        store = Store(self.root / name)
        self.stores.append(store)
        return store

    def test_old_duplicate_fetch_after_catalog_change_retries_without_replacing_head_or_detail(self):
        store = self.new_store()
        remote_id = base64.b64encode(
            b'VsHistoryDetail-u-catalog:REGULAR:20261003T010101_old'
        ).decode('ascii')
        variables = {'vsResultId': remote_id}
        old_manifest = manifest('app-old', {
            'VsHistoryDetailQuery': operation('qid-old', [linked(
                'vsHistoryDetail', [scalar('id'), scalar('oldValue')], concrete='VsHistoryDetail',
            )]),
        })
        new_manifest = manifest('app-new', {
            'VsHistoryDetailQuery': operation('qid-new', [linked(
                'vsHistoryDetail', [scalar('id'), scalar('oldValue'), scalar('newRequired')],
                concrete='VsHistoryDetail',
            )]),
        })
        add_manifest(store, old_manifest, '2026-10-03T01:00:00Z')
        add_manifest(store, new_manifest, '2026-10-03T02:00:00Z')

        old_raw = (
            b'{ "data" : { "vsHistoryDetail" : { "id" : "' + remote_id.encode('ascii') +
            b'", "oldValue" : "old-complete", "future" : 9007199254740997 } } }\n'
        )
        old_id = store.record(event(
            'VsHistoryDetailQuery', old_raw, 'qid-old', 'app-old',
            '2026-10-03T01:10:00Z', variables,
        ))
        store.project(old_id, Planner(old_manifest))
        self.assertEqual(store.db.execute(
            'SELECT state FROM jobs WHERE operation=? AND variables_json=?',
            ('VsHistoryDetailQuery', js(variables)),
        ).fetchone()[0], 'done')

        new_raw = js({'data': {'vsHistoryDetail': {
            'id': remote_id, 'oldValue': 'new-current', 'newRequired': 'present',
        }}}).encode('utf-8')
        new_id = store.record(event(
            'VsHistoryDetailQuery', new_raw, 'qid-new', 'app-new',
            '2026-10-03T02:10:00Z', variables,
        ))
        current_planner = Planner(new_manifest)
        store.project(new_id, current_planner)
        self.assertEqual(store.db.execute(
            'SELECT detail_response_id FROM matches WHERE account=?', ('catalog-test',),
        ).fetchone()[0], new_id)

        sentinel_context = js({'retain': 'older audit issue'})
        store.db.execute(
            'INSERT INTO issues(response_id,code,context,created_at) VALUES(?,?,?,?)',
            (old_id, 'PREEXISTING_AUDIT', sentinel_context, '2026-10-03T00:00:00Z'),
        )
        store.db.commit()
        duplicate_receipt = event(
            'VsHistoryDetailQuery', old_raw, 'qid-old', 'app-old',
            '2026-10-03T03:10:00Z', variables,
        )
        self.assertEqual(store.record(duplicate_receipt), old_id)
        store.project(old_id, current_planner)

        self.assertEqual(response_body(store, old_id), old_raw)
        self.assertEqual(store.db.execute(
            'SELECT detail_response_id FROM matches WHERE account=?', ('catalog-test',),
        ).fetchone()[0], new_id)
        self.assertEqual(store.db.execute(
            'SELECT response_id FROM endpoint_heads WHERE account=? AND operation=?',
            ('catalog-test', 'VsHistoryDetailQuery'),
        ).fetchone()[0], new_id)
        self.assertEqual(store.db.execute(
            'SELECT state FROM jobs WHERE operation=? AND variables_json=?',
            ('VsHistoryDetailQuery', js(variables)),
        ).fetchone()[0], 'retry')
        self.assertEqual(store.db.execute(
            "SELECT count(*) FROM issues WHERE response_id=? AND code='SELECTED_FIELD_MISSING'",
            (old_id,),
        ).fetchone()[0], 0)
        issue = store.db.execute(
            "SELECT context FROM issues WHERE response_id=? AND code='RESPONSE_CATALOG_MISMATCH'",
            (old_id,),
        ).fetchone()
        self.assertEqual(json.loads(issue['context']), {
            'binding_status': 'known_saved', 'operation': 'VsHistoryDetailQuery',
        })
        self.assertNotIn('qid-old', issue['context'])
        self.assertNotIn('old-complete', issue['context'])
        prior_issue = store.db.execute(
            "SELECT context,created_at FROM issues WHERE response_id=? AND code='PREEXISTING_AUDIT'",
            (old_id,),
        ).fetchone()
        self.assertEqual(tuple(prior_issue), (sentinel_context, '2026-10-03T00:00:00Z'))
        self.assertEqual(store.db.execute(
            'SELECT acknowledged FROM response_fetches WHERE event_id=?',
            (duplicate_receipt['event_id'],),
        ).fetchone()[0], 1)

    def test_spool_recovery_of_saved_old_definition_retries_and_skips_related_and_page_visits(self):
        store = self.new_store('spool.sqlite')
        after_arg = [{'kind': 'Variable', 'name': 'after', 'variableName': 'after'}]
        user_id_arg = [{'kind': 'Variable', 'name': 'id', 'variableName': 'userId'}]
        user_detail = operation('user-qid', [linked(
            'userById', [{
                'kind': 'InlineFragment', 'type': 'User', 'abstractKey': None,
                'selections': [scalar('id')],
            }], concrete='User', args=user_id_arg,
        )], [{'name': 'userId', 'defaultValue': None}])

        def root_operation(query_id, include_current_field):
            selections = [
                linked('users', [scalar('id'), scalar('name')], plural=True, concrete='User'),
                linked('pageResults', [
                    linked('pageInfo', [scalar('hasNextPage'), scalar('endCursor')]),
                    linked('nodes', [scalar('id')], plural=True),
                ], concrete='Page', args=after_arg),
            ]
            if include_current_field:
                selections.append(scalar('newRequired'))
            return operation(query_id, selections, [{'name': 'after', 'defaultValue': None}])

        old_manifest = manifest('app-old', {
            'RootQuery': root_operation('root-old', False),
            'UserDetailQuery': user_detail,
        })
        new_manifest = manifest('app-new', {
            'RootQuery': root_operation('root-new', True),
            'UserDetailQuery': user_detail,
        })
        add_manifest(store, old_manifest, '2026-10-03T01:00:00Z')
        add_manifest(store, new_manifest, '2026-10-03T02:00:00Z')
        variables = {'after': None}
        store.queue('catalog-test', 'RootQuery', variables)
        raw = js({'data': {
            'users': [{'__typename': 'User', 'id': 'user-1', 'name': 'Alice'}],
            'pageResults': {
                'pageInfo': {'hasNextPage': True, 'endCursor': 'cursor-next'},
                'nodes': [],
            },
        }}).encode('utf-8')
        spool = self.root / 'spool'
        spool.mkdir()
        source_event = event(
            'RootQuery', raw, 'root-old', 'app-old', '2026-10-03T03:00:00Z', variables,
        )
        source_path = spool / 'old-response.json'
        source_path.write_text(json.dumps(source_event), encoding='utf-8')

        store.recover(Planner(new_manifest), spool)

        rid = store.db.execute(
            'SELECT id FROM responses WHERE operation=?', ('RootQuery',),
        ).fetchone()[0]
        self.assertEqual(response_body(store, rid), raw)
        self.assertFalse(source_path.exists())
        self.assertEqual(store.db.execute(
            'SELECT state FROM jobs WHERE operation=? AND variables_json=?',
            ('RootQuery', js(variables)),
        ).fetchone()[0], 'retry')
        self.assertEqual(store.db.execute(
            "SELECT count(*) FROM jobs WHERE operation='UserDetailQuery'",
        ).fetchone()[0], 0)
        self.assertEqual(store.db.execute(
            "SELECT count(*) FROM jobs WHERE operation='RootQuery' AND variables_json=?",
            (js({'after': 'cursor-next'}),),
        ).fetchone()[0], 0)
        self.assertEqual(store.db.execute(
            'SELECT count(*) FROM entities WHERE account=?', ('catalog-test',),
        ).fetchone()[0], 0)
        self.assertEqual(store.db.execute(
            "SELECT count(*) FROM issues WHERE response_id=? AND code='SELECTED_FIELD_MISSING'",
            (rid,),
        ).fetchone()[0], 0)
        self.assertEqual(json.loads(store.db.execute(
            "SELECT context FROM issues WHERE response_id=? AND code='RESPONSE_CATALOG_MISMATCH'",
            (rid,),
        ).fetchone()[0]), {
            'binding_status': 'known_saved', 'operation': 'RootQuery',
        })

    def test_ambiguous_corrupt_duplicate_and_unknown_catalogs_are_never_projected_as_success(self):
        current_query = operation('stable-qid', [linked(
            'root', [scalar('id')], concrete='User',
        )])
        current_manifest = manifest('app-1', {'RootQuery': current_query})
        cases = ('ambiguous', 'corrupt', 'duplicate', 'unknown')

        for case in cases:
            with self.subTest(case=case):
                store = self.new_store(f'{case}.sqlite')
                add_manifest(store, current_manifest, '2026-10-03T01:00:00Z')
                operation_name = 'RootQuery'
                query_id = 'stable-qid'
                app_version = 'app-1'
                if case == 'ambiguous':
                    competing = manifest('app-1', {'RootQuery': operation(
                        'stable-qid', [linked('root', [scalar('different')], concrete='User')],
                    )})
                    add_manifest(store, competing, '2026-10-03T00:00:00Z')
                elif case == 'corrupt':
                    corrupt = manifest('app-1', {'RootQuery': current_query})
                    corrupt_text = js(corrupt)
                    store.db.execute(
                        'INSERT INTO manifests(sha256,fetched_at,json_text) VALUES(?,?,?)',
                        ('0' * 64, '2026-10-03T00:30:00Z', corrupt_text),
                    )
                    store.db.commit()
                elif case == 'duplicate':
                    duplicate_text = (
                        '{"version":"app-1","queries":{"RootQuery":{"params":'
                        '{"operationKind":"query","id":"wrong","id":"stable-qid"},'
                        '"operation":{"argumentDefinitions":[],"selections":'
                        '[{"kind":"LinkedField","name":"root","alias":null,'
                        '"plural":false,"concreteType":"User","selections":'
                        '[{"kind":"ScalarField","name":"id","alias":null}]}]}}}}'
                    )
                    duplicate_sha = hashlib.sha256(duplicate_text.encode('utf-8')).hexdigest()
                    store.db.execute(
                        'INSERT INTO manifests(sha256,fetched_at,json_text) VALUES(?,?,?)',
                        (duplicate_sha, '2026-10-03T00:30:00Z', duplicate_text),
                    )
                    store.db.commit()
                else:
                    operation_name = 'FutureQuery'
                    query_id = 'future-qid'

                variables = {}
                store.queue('catalog-test', operation_name, variables)
                raw = (
                    b'{ "data" : { "root" : { "__typename" : "User", "id" : "user-1", '
                    b'"future" : 9007199254740993 } }, "unknown" : "retained" }\n'
                )
                rid = store.record(event(
                    operation_name, raw, query_id, app_version,
                    '2026-10-03T03:00:00Z', variables,
                ))
                store.project(rid, Planner(current_manifest))

                self.assertEqual(response_body(store, rid), raw)
                self.assertEqual(store.db.execute(
                    'SELECT state FROM jobs WHERE operation=? AND variables_json=?',
                    (operation_name, js(variables)),
                ).fetchone()[0], 'retry')
                self.assertEqual(store.db.execute(
                    'SELECT count(*) FROM entities WHERE account=?', ('catalog-test',),
                ).fetchone()[0], 0)
                self.assertEqual(store.db.execute(
                    'SELECT count(*) FROM endpoint_heads WHERE account=?', ('catalog-test',),
                ).fetchone()[0], 0)
                binding_status = {
                    'ambiguous': 'ambiguous',
                    'corrupt': 'catalog_corrupt',
                    'duplicate': 'catalog_invalid',
                    'unknown': 'unresolved',
                }[case]
                issue = store.db.execute(
                    "SELECT context FROM issues WHERE response_id=? AND code='RESPONSE_CATALOG_UNRESOLVED'",
                    (rid,),
                ).fetchone()
                self.assertEqual(json.loads(issue['context']), {
                    'binding_status': binding_status, 'operation': operation_name,
                })

    def test_repair_of_saved_old_null_result_downgrades_job_without_rewriting_prior_issues(self):
        store = self.new_store('null-repair.sqlite')
        old_manifest = manifest('app-old', {'useCurrentFestQuery': operation(
            'fest-old', [scalar('currentFest')],
        )})
        new_manifest = manifest('app-new', {'useCurrentFestQuery': operation(
            'fest-new', [scalar('currentFest'), scalar('newRequired')],
        )})
        add_manifest(store, old_manifest, '2026-10-03T01:00:00Z')
        add_manifest(store, new_manifest, '2026-10-03T02:00:00Z')
        raw = b'{ "data" : { "currentFest" : null, "future" : "kept" } }\n'
        rid = store.record(event(
            'useCurrentFestQuery', raw, 'fest-old', 'app-old',
            '2026-10-03T03:00:00Z', {},
        ))
        store.db.execute(
            'INSERT INTO jobs(account,operation,variables_json,state,attempts,next_attempt,last_response_id) '
            'VALUES(?,?,?,?,?,?,?)',
            ('catalog-test', 'useCurrentFestQuery', '{}', 'done', 1, 999.0, rid),
        )
        prior_context = js({'reason': 'previously classified', 'keep': True})
        store.db.execute(
            'INSERT INTO issues(response_id,code,context,created_at) VALUES(?,?,?,?)',
            (rid, 'DETAIL_UNAVAILABLE', prior_context, '2026-10-03T00:00:00Z'),
        )
        store.db.commit()

        store._repair_job_outcomes()

        job = store.db.execute(
            "SELECT state FROM jobs WHERE operation='useCurrentFestQuery'",
        ).fetchone()
        self.assertEqual(job['state'], 'retry')
        self.assertEqual(response_body(store, rid), raw)
        prior = store.db.execute(
            "SELECT code,context,created_at FROM issues WHERE response_id=? AND code='DETAIL_UNAVAILABLE'",
            (rid,),
        ).fetchone()
        self.assertEqual(tuple(prior), ('DETAIL_UNAVAILABLE', prior_context, '2026-10-03T00:00:00Z'))
        self.assertEqual(json.loads(store.db.execute(
            "SELECT context FROM issues WHERE response_id=? AND code='RESPONSE_CATALOG_MISMATCH'",
            (rid,),
        ).fetchone()[0]), {
            'binding_status': 'known_saved', 'operation': 'useCurrentFestQuery',
        })

    def test_import_command_uses_catalog_gate_and_keeps_original_body_bytes(self):
        db_path = self.root / 'import.sqlite'
        store = Store(db_path)
        old_manifest = manifest('app-old', {'ImportQuery': operation(
            'import-old', [scalar('oldField')],
        )})
        current_manifest = manifest('app-new', {'ImportQuery': operation(
            'import-new', [scalar('oldField'), scalar('newRequired')],
        )})
        add_manifest(store, old_manifest, '2026-10-03T01:00:00Z')
        add_manifest(store, current_manifest, '2026-10-03T02:00:00Z')
        store.queue('catalog-test', 'ImportQuery', {})
        store.db.commit()
        store.close()

        raw = b'{ "data" : { "oldField" : "saved", "unknown" : 9007199254740993 } }\n'
        imported = event(
            'ImportQuery', raw, 'import-old', 'app-old', '2026-10-03T03:00:00Z', {},
        )
        import_dir = self.root / 'import-input'
        import_dir.mkdir()
        (import_dir / 'response.json').write_text(
            json.dumps(imported), encoding='utf-8',
        )
        stdout = io.StringIO()
        with patch.object(sys, 'argv', [
            'archive.py', '--db', str(db_path), 'import', str(import_dir),
            '--account', 'catalog-test',
        ]), patch.object(archive, 'catalog', return_value=current_manifest), \
                contextlib.redirect_stdout(stdout):
            exit_code = archive.main()

        self.assertEqual(exit_code, 0)
        self.assertEqual(json.loads(stdout.getvalue())['imported'], 1)
        connection = sqlite3.connect(f'{db_path.resolve().as_uri()}?mode=ro', uri=True)
        connection.row_factory = sqlite3.Row
        try:
            response_row = connection.execute(
                'SELECT id FROM responses WHERE operation=?', ('ImportQuery',),
            ).fetchone()
            rid = response_row['id']
            body = connection.execute(
                'SELECT body FROM bodies JOIN responses ON bodies.sha256=responses.body_sha256 WHERE responses.id=?',
                (rid,),
            ).fetchone()[0]
            state = connection.execute(
                "SELECT state FROM jobs WHERE operation='ImportQuery'",
            ).fetchone()[0]
            issue = connection.execute(
                "SELECT context FROM issues WHERE response_id=? AND code='RESPONSE_CATALOG_MISMATCH'",
                (rid,),
            ).fetchone()
        finally:
            connection.close()
        self.assertEqual(body, raw)
        self.assertEqual(state, 'retry')
        self.assertEqual(json.loads(issue['context']), {
            'binding_status': 'known_saved', 'operation': 'ImportQuery',
        })

    def test_catalog_mismatch_is_not_a_success_clock_and_legacy_remains_unchecked(self):
        store = self.new_store('health.sqlite')
        old_manifest = manifest('app-old', {'LatestBattleHistoriesQuery': operation(
            'history-old', [linked('latestBattleHistories')],
        )})
        current_manifest = manifest('app-new', {'LatestBattleHistoriesQuery': operation(
            'history-new', [linked('latestBattleHistories'), scalar('newRequired')],
        )})
        add_manifest(store, old_manifest, '2026-10-03T01:00:00Z')
        add_manifest(store, current_manifest, '2026-10-03T02:00:00Z')
        store.queue('catalog-test', 'LatestBattleHistoriesQuery', {})

        good_raw = b'{"data":{"latestBattleHistories":{},"newRequired":"present"}}'
        good_id = store.record(event(
            'LatestBattleHistoriesQuery', good_raw, 'history-new', 'app-new',
            '2026-10-03T02:10:00Z', {},
        ))
        current_planner = Planner(current_manifest)
        store.project(good_id, current_planner)
        old_raw = b'{"data":{"latestBattleHistories":{},"future":true}}'
        old_id = store.record(event(
            'LatestBattleHistoriesQuery', old_raw, 'history-old', 'app-old',
            '2026-10-03T03:10:00Z', {},
        ))
        store.project(old_id, current_planner)
        clock = next(
            item for item in store.sync_health()['histories']
            if item['operation'] == 'LatestBattleHistoriesQuery'
        )
        self.assertEqual(clock['last_success_at'], '2026-10-03T02:10:00Z')
        self.assertEqual(response_body(store, old_id), old_raw)

        legacy_raw = b'{"data":{"latestBattleHistories":[]}}'
        legacy_id = store.record(event(
            'LatestBattleHistoriesQuery', legacy_raw, None, None,
            '2026-10-03T04:10:00Z', {},
        ))
        store.project(legacy_id, current_planner)
        self.assertEqual(store.db.execute(
            "SELECT state FROM jobs WHERE operation='LatestBattleHistoriesQuery'",
        ).fetchone()[0], 'done')
        self.assertEqual(store.db.execute(
            "SELECT count(*) FROM issues WHERE response_id=? AND code LIKE 'RESPONSE_CATALOG_%'",
            (legacy_id,),
        ).fetchone()[0], 0)


if __name__ == '__main__':
    unittest.main()
