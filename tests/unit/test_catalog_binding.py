import hashlib
import json
import sqlite3
import tempfile
import unittest
from dataclasses import FrozenInstanceError
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'src/python'))

from ikarchive.catalog_binding import resolve_response_planner
from ikarchive.planner import Planner


def manifest(query_id, version, selected_field=None, marker=None):
    selections = []
    if selected_field is not None:
        selections.append({
            'kind': 'ScalarField', 'name': selected_field, 'alias': None,
        })
    result = {
        'fetched_at': f'{version}-catalog-time',
        'version': version,
        'queries': {
            'Q': {
                'params': {'operationKind': 'query', 'id': query_id},
                'operation': {
                    'argumentDefinitions': [],
                    'selections': selections,
                },
            },
        },
    }
    if marker is not None:
        result['marker'] = marker
    return result


def manifest_text(value):
    return json.dumps(
        value, ensure_ascii=False, allow_nan=False,
        separators=(',', ':'), sort_keys=True,
    )


def manifest_sha(value):
    return hashlib.sha256(manifest_text(value).encode('utf-8')).hexdigest()


def create_db(path, rows):
    connection = sqlite3.connect(path)
    try:
        connection.execute(
            'CREATE TABLE manifests(sha256 TEXT PRIMARY KEY, fetched_at TEXT NOT NULL, json_text TEXT NOT NULL)'
        )
        connection.executemany(
            'INSERT INTO manifests(sha256,fetched_at,json_text) VALUES(?,?,?)',
            rows,
        )
        connection.commit()
    finally:
        connection.close()


def stored_row(value):
    text = manifest_text(value)
    return manifest_sha(value), value['fetched_at'], text


def open_readonly(path):
    connection = sqlite3.connect(f'{path.resolve().as_uri()}?mode=ro', uri=True)
    connection.row_factory = sqlite3.Row
    return connection


def file_sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


class CatalogBindingTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def test_old_spool_binds_to_saved_old_catalog_not_new_current_catalog(self):
        old = manifest('old-qid', 'app-1', 'oldField')
        current = manifest('new-qid', 'app-2', 'newField')
        db_path = self.root / 'catalogs.sqlite'
        create_db(db_path, [stored_row(old)])
        before = file_sha(db_path)
        connection = open_readonly(db_path)
        try:
            binding = resolve_response_planner(
                connection,
                {'operation': 'Q', 'query_id': 'old-qid', 'app_version': 'app-1'},
                Planner(current),
            )
            self.assertEqual(binding.status, 'known_saved')
            self.assertEqual(binding.manifest_sha, manifest_sha(old))
            self.assertEqual(
                binding.planner.queries['Q']['operation']['selections'][0]['name'],
                'oldField',
            )
            self.assertEqual(connection.total_changes, 0)
        finally:
            connection.close()
        self.assertEqual(file_sha(db_path), before)
        with self.assertRaises(FrozenInstanceError):
            binding.status = 'unresolved'

    def test_current_candidate_requires_exact_query_id_and_present_version(self):
        current = manifest('current-qid', 'app-2', 'field')
        connection = sqlite3.connect(':memory:')
        try:
            cases = [
                ({'operation': 'Q', 'query_id': 'old-qid', 'app_version': 'app-2'}, 'unresolved'),
                ({'operation': 'Q', 'query_id': 'current-qid', 'app_version': 'app-1'}, 'unresolved'),
            ]
            for response, expected in cases:
                with self.subTest(response=response):
                    result = resolve_response_planner(connection, response, Planner(current))
                    self.assertEqual(result.status, expected)
                    self.assertIsNone(result.planner)
                    self.assertIsNone(result.manifest_sha)
        finally:
            connection.close()

    def test_legacy_without_query_id_returns_current_with_unverified_status(self):
        current = Planner(manifest('current-qid', 'app-2', 'field'))
        connection = sqlite3.connect(':memory:')
        try:
            result = resolve_response_planner(
                connection,
                {'operation': 'Q', 'query_id': None, 'app_version': 'app-2'},
                current,
            )
            self.assertEqual(result.status, 'legacy_unchecked')
            self.assertIs(result.planner, current)
            self.assertIsNone(result.manifest_sha)
        finally:
            connection.close()

    def test_explicit_current_schema_can_resolve_without_manifests_table(self):
        current = Planner(manifest('current-qid', 'app-2', 'field'))
        connection = sqlite3.connect(':memory:')
        try:
            result = resolve_response_planner(
                connection,
                {'operation': 'Q', 'query_id': 'current-qid', 'app_version': 'app-2'},
                current,
            )
            self.assertEqual(result.status, 'known_current')
            self.assertIs(result.planner, current)
            self.assertIsNone(result.manifest_sha)
        finally:
            connection.close()

    def test_saved_catalog_resolution_supports_default_tuple_rows(self):
        saved = manifest('saved-qid', 'app-1', 'savedField')
        connection = sqlite3.connect(':memory:')
        try:
            connection.execute(
                'CREATE TABLE manifests(sha256 TEXT, fetched_at TEXT, json_text TEXT)'
            )
            connection.execute(
                'INSERT INTO manifests VALUES(?,?,?)', stored_row(saved),
            )
            connection.commit()
            result = resolve_response_planner(
                connection,
                {'operation': 'Q', 'query_id': 'saved-qid', 'app_version': 'app-1'},
                Planner(manifest('new-qid', 'app-2', 'newField')),
            )
        finally:
            connection.close()
        self.assertEqual(result.status, 'known_saved')
        self.assertEqual(result.manifest_sha, manifest_sha(saved))
        self.assertEqual(
            result.planner.queries['Q']['operation']['selections'][0]['name'],
            'savedField',
        )

    def test_current_manifest_sha_is_returned_when_full_saved_manifest_matches(self):
        current_manifest = manifest('same-qid', 'app-1', 'sameField')
        raw_saved_text = json.dumps(current_manifest, ensure_ascii=False, indent=2)
        saved_sha = hashlib.sha256(raw_saved_text.encode('utf-8')).hexdigest()
        connection = sqlite3.connect(':memory:')
        try:
            connection.execute(
                'CREATE TABLE manifests(sha256 TEXT, fetched_at TEXT, json_text TEXT)'
            )
            connection.execute(
                'INSERT INTO manifests VALUES(?,?,?)',
                (saved_sha, current_manifest['fetched_at'], raw_saved_text),
            )
            connection.commit()
            result = resolve_response_planner(
                connection,
                {'operation': 'Q', 'query_id': 'same-qid', 'app_version': 'app-1'},
                Planner(current_manifest),
            )
        finally:
            connection.close()
        self.assertEqual(result.status, 'known_current')
        self.assertEqual(result.manifest_sha, saved_sha)

    def test_present_empty_query_id_is_not_treated_as_legacy(self):
        current = Planner(manifest('current-qid', 'app-2', 'field'))
        connection = sqlite3.connect(':memory:')
        try:
            result = resolve_response_planner(
                connection,
                {'operation': 'Q', 'query_id': '', 'app_version': 'app-2'},
                current,
            )
        finally:
            connection.close()
        self.assertEqual(result.status, 'unresolved')
        self.assertIsNone(result.planner)

    def test_same_definition_duplicates_choose_lexical_sha_independent_of_row_order(self):
        first = manifest('shared-qid', 'app-1', 'sameField', marker='first')
        second = manifest('shared-qid', 'app-1', 'sameField', marker='second')
        rows = [stored_row(first), stored_row(second)]
        current = Planner(manifest('other-qid', 'app-2', 'otherField'))
        expected_sha = min(manifest_sha(first), manifest_sha(second))
        expected_marker = first['marker'] if manifest_sha(first) == expected_sha else second['marker']
        bindings = []

        for index, ordered_rows in enumerate((rows, list(reversed(rows)))):
            db_path = self.root / f'order-{index}.sqlite'
            create_db(db_path, ordered_rows)
            connection = open_readonly(db_path)
            try:
                result = resolve_response_planner(
                    connection,
                    {'operation': 'Q', 'query_id': 'shared-qid', 'app_version': 'app-1'},
                    current,
                )
            finally:
                connection.close()
            self.assertEqual(result.status, 'known_saved')
            self.assertEqual(result.manifest_sha, expected_sha)
            self.assertEqual(result.planner.manifest['marker'], expected_marker)
            bindings.append((result.status, result.manifest_sha, result.planner.manifest['marker']))

        self.assertEqual(bindings[0], bindings[1])

    def test_different_selected_definitions_with_same_id_and_version_are_ambiguous(self):
        saved = manifest('shared-qid', 'app-1', 'savedSelection')
        current = Planner(manifest('shared-qid', 'app-1', 'currentSelection'))
        db_path = self.root / 'ambiguous.sqlite'
        create_db(db_path, [stored_row(saved)])
        connection = open_readonly(db_path)
        try:
            result = resolve_response_planner(
                connection,
                {'operation': 'Q', 'query_id': 'shared-qid', 'app_version': 'app-1'},
                current,
            )
        finally:
            connection.close()
        self.assertEqual(result.status, 'ambiguous')
        self.assertIsNone(result.planner)
        self.assertIsNone(result.manifest_sha)

    def test_matching_corrupt_saved_candidate_blocks_even_matching_current(self):
        current = manifest('shared-qid', 'app-1', 'currentSelection')
        corrupted_original = manifest('shared-qid', 'app-1', 'originalSelection')
        corrupted_text = manifest_text(manifest('shared-qid', 'app-1', 'tamperedSelection'))
        db_path = self.root / 'corrupt.sqlite'
        create_db(db_path, [(manifest_sha(corrupted_original), 'app-1-catalog-time', corrupted_text)])
        before = file_sha(db_path)
        connection = open_readonly(db_path)
        try:
            result = resolve_response_planner(
                connection,
                {'operation': 'Q', 'query_id': 'shared-qid', 'app_version': 'app-1'},
                Planner(current),
            )
            self.assertEqual(connection.total_changes, 0)
        finally:
            connection.close()
        self.assertEqual(file_sha(db_path), before)
        self.assertEqual(result.status, 'catalog_corrupt')
        self.assertIsNone(result.planner)
        self.assertIsNone(result.manifest_sha)

    def test_matching_saved_metadata_with_invalid_planner_shape_is_stable_invalid(self):
        invalid = manifest('bad-qid', 'app-1')
        invalid['queries']['Q']['operation']['selections'] = 'not-a-list'
        db_path = self.root / 'invalid.sqlite'
        create_db(db_path, [stored_row(invalid)])
        connection = open_readonly(db_path)
        try:
            result = resolve_response_planner(
                connection,
                {'operation': 'Q', 'query_id': 'bad-qid', 'app_version': 'app-1'},
                Planner(manifest('other-qid', 'app-2')),
            )
        finally:
            connection.close()
        self.assertEqual(result.status, 'catalog_invalid')
        self.assertIsNone(result.planner)
        self.assertIsNone(result.manifest_sha)

    def test_duplicate_json_keys_in_saved_manifest_are_invalid_and_read_only(self):
        raw_text = (
            '{"version":"app-1","queries":{"Q":{"params":'
            '{"operationKind":"query","id":"first","id":"duplicate"},'
            '"operation":{"argumentDefinitions":[],"selections":[]}}}}'
        )
        raw_bytes = raw_text.encode('utf-8')
        stored_sha = hashlib.sha256(raw_bytes).hexdigest()
        db_path = self.root / 'duplicate-key.sqlite'
        create_db(db_path, [(stored_sha, 'app-1-time', raw_text)])
        before = file_sha(db_path)
        connection = open_readonly(db_path)
        try:
            result = resolve_response_planner(
                connection,
                {'operation': 'Q', 'query_id': 'duplicate', 'app_version': 'app-1'},
                Planner(manifest('duplicate', 'app-1', 'currentField')),
            )
            self.assertEqual(connection.total_changes, 0)
        finally:
            connection.close()
        self.assertEqual(file_sha(db_path), before)
        self.assertEqual(result.status, 'catalog_invalid')
        self.assertIsNone(result.planner)
        self.assertIsNone(result.manifest_sha)

    def test_same_sha_same_length_raw_mutation_invalidates_cached_hash_result(self):
        original = manifest('saved-qid', 'app-1', 'field_a')
        replacement = manifest('saved-qid', 'app-1', 'field_b')
        original_text = manifest_text(original)
        replacement_text = manifest_text(replacement)
        self.assertEqual(len(original_text.encode('utf-8')), len(replacement_text.encode('utf-8')))
        stored_sha = hashlib.sha256(original_text.encode('utf-8')).hexdigest()
        db_path = self.root / 'raw-mutation.sqlite'
        create_db(db_path, [(stored_sha, 'app-1-time', original_text)])
        current = Planner(manifest('current-qid', 'app-2', 'currentField'))

        connection = open_readonly(db_path)
        try:
            first = resolve_response_planner(
                connection,
                {'operation': 'Q', 'query_id': 'saved-qid', 'app_version': 'app-1'},
                current,
            )
        finally:
            connection.close()
        self.assertEqual(first.status, 'known_saved')

        writer = sqlite3.connect(db_path)
        try:
            writer.execute('UPDATE manifests SET json_text=? WHERE sha256=?', (replacement_text, stored_sha))
            writer.commit()
        finally:
            writer.close()
        before = file_sha(db_path)
        connection = open_readonly(db_path)
        try:
            second = resolve_response_planner(
                connection,
                {'operation': 'Q', 'query_id': 'saved-qid', 'app_version': 'app-1'},
                current,
            )
            self.assertEqual(connection.total_changes, 0)
        finally:
            connection.close()
        self.assertEqual(file_sha(db_path), before)
        self.assertEqual(second.status, 'catalog_corrupt')
        self.assertIsNone(second.planner)

    def test_cached_parsed_manifest_is_not_shared_through_returned_planner(self):
        saved = manifest('saved-qid', 'app-1', 'originalField')
        db_path = self.root / 'cache-isolation.sqlite'
        create_db(db_path, [stored_row(saved)])
        response = {'operation': 'Q', 'query_id': 'saved-qid', 'app_version': 'app-1'}
        current = Planner(manifest('current-qid', 'app-2', 'currentField'))

        connection = open_readonly(db_path)
        try:
            first = resolve_response_planner(connection, response, current)
            first.planner.manifest['queries']['Q']['operation']['selections'][0]['name'] = 'callerMutation'
            second = resolve_response_planner(connection, response, current)
        finally:
            connection.close()
        self.assertEqual(first.status, 'known_saved')
        self.assertEqual(second.status, 'known_saved')
        self.assertEqual(
            second.planner.queries['Q']['operation']['selections'][0]['name'],
            'originalField',
        )

    def test_current_manifest_mutation_is_revalidated_against_saved_candidate(self):
        saved = manifest('shared-qid', 'app-1', 'savedField')
        db_path = self.root / 'current-mutation.sqlite'
        create_db(db_path, [stored_row(saved)])
        current_manifest = manifest('shared-qid', 'app-1', 'savedField')
        current = Planner(current_manifest)
        response = {'operation': 'Q', 'query_id': 'shared-qid', 'app_version': 'app-1'}
        connection = open_readonly(db_path)
        try:
            first = resolve_response_planner(connection, response, current)
            current.manifest['queries']['Q']['operation']['selections'][0]['name'] = 'mutatedField'
            second = resolve_response_planner(connection, response, current)
        finally:
            connection.close()
        self.assertEqual(first.status, 'known_current')
        self.assertEqual(second.status, 'ambiguous')
        self.assertIsNone(second.planner)


if __name__ == '__main__':
    unittest.main()
