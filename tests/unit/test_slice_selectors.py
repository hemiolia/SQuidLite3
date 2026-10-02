import copy
from contextlib import closing
import hashlib
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'src/python'))
from ikarchive.slice_selectors import export_selectors, verify_selectors, digest, rule_token


class SliceSelectorTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name).resolve()
        self.source = sqlite3.connect(':memory:')
        self.source.executescript('''
            CREATE TABLE bodies(sha256 TEXT PRIMARY KEY,body BLOB);
            CREATE TABLE responses(id INTEGER PRIMARY KEY,body_sha256 TEXT REFERENCES bodies(sha256));
            CREATE TABLE matches(account,kind,match_key,detail_response_id REFERENCES responses(id),
                original_extra, PRIMARY KEY(account,kind,match_key));
            CREATE TABLE match_classification(account,kind,match_key,analysis_set,rule_raw,
                FOREIGN KEY(account,kind,match_key) REFERENCES matches(account,kind,match_key));
            CREATE TABLE jobs(operation,state);
            CREATE TABLE empty_table(unknown_column);
            INSERT INTO bodies VALUES('body',X'0001ff');
            INSERT INTO responses VALUES(1,'body');
            INSERT INTO jobs VALUES('event','retry');
            INSERT INTO matches VALUES('account','vs','a',1,'unknown extra');
            INSERT INTO matches VALUES('account','vs','b',1,9007199254740993);
            INSERT INTO matches VALUES('account','coop','u',NULL,NULL);
            INSERT INTO match_classification VALUES('account','vs','a','bankara_open','AREA');
            INSERT INTO match_classification VALUES('account','vs','b','xmatch','LOFT');
        ''')
        self.source.commit()
        common = self.root / 'shared' / 'complete.sqlite3'
        common.parent.mkdir()
        target = sqlite3.connect(common)
        self.source.backup(target)
        target.close()
        self.source_sha = digest(common)['sha256']
        objects = [dict(zip(('type', 'name', 'tbl_name', 'sql'), row)) for row in self.source.execute(
            'SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name')]
        columns = ('cid', 'name', 'type', 'notnull', 'dflt_value', 'pk', 'hidden')
        self.manifest = {
            'version': 2, 'role': 'lossless_sqlite_shards', 'snapshot_identifier': 'fixture',
            'source_schema_sha256': hashlib.sha256(json.dumps(objects, ensure_ascii=False,
                sort_keys=True, separators=(',', ':')).encode()).hexdigest(),
            'schema_objects': objects, 'external_values': [],
            'tables': [{'name': name, 'column_schema': [dict(zip(columns, row)) for row in
                self.source.execute('PRAGMA table_xinfo("' + name + '")')],
                'parts': []} for (name,) in self.source.execute('SELECT name FROM sqlite_master WHERE type="table" ORDER BY name')],
        }
        # Shared file listing has one reference per artifact, not one per table.
        self.manifest['tables'][0]['parts'] = [{'file': 'shared/complete.sqlite3', **digest(common)}]
        export_selectors(self.source, self.root, self.manifest, self.source_sha)

    def tearDown(self):
        self.source.close()
        self.temp.cleanup()

    def test_all_information_reachable_and_scopes_exact(self):
        receipt = verify_selectors(self.source, self.root, self.manifest, self.source_sha)
        self.assertTrue(receipt['all_shared_files_reachable'])
        self.assertEqual(receipt['rule_files'], 4)
        self.assertEqual(sorted(row['matches'] for row in self.manifest['by_rule']), [0, 0, 1, 1])
        unclassified = next(row for row in self.manifest['by_mode'] if row['analysis_set'] == 'unclassified')
        self.assertEqual(unclassified['matches'], 1)
        for row in self.manifest['by_mode'] + self.manifest['by_rule']:
            with closing(sqlite3.connect(self.root / row['file'])) as conn, conn:
                file = conn.execute('SELECT file FROM shared_files').fetchone()[0]
            with closing(sqlite3.connect(self.root / file)) as conn, conn:
                self.assertEqual(conn.execute('SELECT body FROM bodies').fetchone()[0], b'\x00\x01\xff')
                self.assertEqual(conn.execute('SELECT state FROM jobs').fetchone()[0], 'retry')
                self.assertEqual(conn.execute('SELECT count(*) FROM empty_table').fetchone()[0], 0)

    def _alter(self, sql):
        row = next(row for row in self.manifest['by_mode'] if row['analysis_set'] == 'bankara_open')
        with closing(sqlite3.connect(self.root / row['file'])) as conn, conn:
            conn.execute(sql)
        row.update(digest(self.root / row['file']))

    def test_rejects_dependency_removed_even_with_updated_selector_hash(self):
        self._alter('DELETE FROM shared_files')
        with self.assertRaisesRegex(ValueError, 'DEPENDENCY_COVERAGE'):
            verify_selectors(self.source, self.root, self.manifest, self.source_sha)

    def test_rejects_original_extra_changed_even_with_same_count_and_new_hash(self):
        self._alter("UPDATE matches SET original_extra='lost original'")
        with self.assertRaisesRegex(ValueError, 'SEED_VALUES'):
            verify_selectors(self.source, self.root, self.manifest, self.source_sha)

    def test_rejects_foreign_key_description_removed(self):
        self._alter('DELETE FROM archive_foreign_keys')
        with self.assertRaisesRegex(ValueError, 'REFERENCE_METADATA'):
            verify_selectors(self.source, self.root, self.manifest, self.source_sha)

    def test_rejects_missing_empty_rule_combination(self):
        self.manifest['by_rule'].pop()
        with self.assertRaisesRegex(ValueError, 'RULE_COVERAGE'):
            verify_selectors(self.source, self.root, self.manifest, self.source_sha)

    def test_rejects_cross_generation_binding(self):
        self._alter("UPDATE slice_meta SET value='other' WHERE key='snapshot_identifier'")
        with self.assertRaisesRegex(ValueError, 'GENERATION'):
            verify_selectors(self.source, self.root, self.manifest, self.source_sha)

    def test_rejects_shared_file_corruption(self):
        with (self.root / 'shared/complete.sqlite3').open('ab') as handle:
            handle.write(b'corrupt')
        with self.assertRaisesRegex(ValueError, 'DEPENDENCY_HASH'):
            verify_selectors(self.source, self.root, self.manifest, self.source_sha)

    def test_null_named_and_arbitrary_rules_have_distinct_tokens(self):
        tokens = [rule_token(raw) for raw in (None, 'rule_unknown', 'AREA', '../AREA', 'rule_AREA')]
        self.assertEqual(len(set(tokens)), 5)
        self.assertTrue(all(len(token) <= 64 for token in tokens))


if __name__ == '__main__':
    unittest.main()
