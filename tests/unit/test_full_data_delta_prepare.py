import importlib.util
import json
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'src/python'))
sys.path.insert(0, str(ROOT / 'scripts'))
import prepare_full_data_delta as preparer
from ikarchive.writer_guards import install_writer_guards
from ikarchive.change_feed import install_change_feed
from ikarchive.delta_transport import iter_delta_records

BASE = '20261002T111654Z-b0ac6787'
FIRST = '20261003T000001Z-11111111'
SECOND = '20261003T000002Z-22222222'
THIRD = '20261003T000003Z-33333333'
FOURTH = '20261003T000004Z-44444444'

class DeltaPreparationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(dir=ROOT)
        self.root = Path(self.tmp.name)
        self.source = self.root / 'source.sqlite3'
        self.baseline = self.root / 'baseline.sqlite3'
        self.manifest = self.root / 'baseline.json'
        self.db = sqlite3.connect(self.source)
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.executescript('CREATE TABLE precious(value); CREATE TABLE empty_table(key TEXT PRIMARY KEY) WITHOUT ROWID;')
        self.db.execute("INSERT INTO precious(rowid,value) VALUES(-8,'original')")
        self.db.execute("INSERT INTO precious(rowid,value) VALUES(99,'retained')")
        self.db.commit()
        target = sqlite3.connect(self.baseline)
        self.db.backup(target)
        target.close()
        self.manifest.write_text(json.dumps({'storage':'plaintext','encryption':None,
            'raw_snapshot': {'basename': self.baseline.name, 'quick_check':'ok',
                             **preparer.file_digest(self.baseline)}}))
        # A change before feed installation is intentionally untracked.
        self.db.execute("UPDATE precious SET value=? WHERE rowid=-8", (b'\x00raw',))
        self.db.commit()
        install_change_feed(self.db)
        self.db.execute("PRAGMA recursive_triggers=ON")
        install_writer_guards(self.db)
        self.db.commit()

    def tearDown(self):
        self.db.close()
        self.tmp.cleanup()

    def prepare(self, identifier, previous=None, **kwargs):
        return preparer.prepare_delta(self.source, self.baseline, self.manifest,
            self.root / identifier, identifier, BASE, previous=previous, **kwargs)

    def records(self, plan):
        directory = self.root / plan['generation_id']
        doc = json.loads((directory / 'transport-document.json').read_text())
        return list(iter_delta_records(directory / 'changes.sqlite3', doc))

    def test_gap_full_values_normal_cycle_and_ddl_reset_rebase(self):
        original_hash = preparer.file_digest(self.baseline)
        first = self.prepare(FIRST)
        self.assertEqual(first['kind'], 'baseline_reconciliation')
        self.assertEqual(first['parent_generation_id'], BASE)
        self.assertTrue(first['replaces_delta_chain'])
        changed = [r for r in self.records(first) if r['table_name']=='precious']
        self.assertEqual(changed[0]['source_rowid'], -8)
        self.assertEqual(changed[0]['values'], (b'\x00raw',))
        self.assertTrue(first['xlsx_verification']['source_rowids_verified'])
        self.assertEqual(first['source_row_counts']['empty_table'], 0)
        self.db.execute('DELETE FROM precious WHERE rowid=99')
        self.db.execute('UPDATE precious SET value=? WHERE rowid=-8', (9223372036854775807,))
        self.db.commit()
        second = self.prepare(SECOND, first)
        self.assertEqual(second['kind'], 'change_feed')
        self.assertEqual(second['parent_generation_id'], FIRST)
        changes = [r for r in self.records(second) if r['table_name']=='precious']
        self.assertEqual({r['operation'] for r in changes}, {'delete','upsert'})
        self.assertEqual(second['source_row_counts']['precious'], 1)
        # A schema change rebases against the original baseline. Restoring the
        # baseline value must not append an empty patch onto the previous int64.
        self.db.execute('CREATE TABLE new_empty(value)')
        self.db.execute("UPDATE precious SET value='original' WHERE rowid=-8")
        self.db.commit()
        third = self.prepare(THIRD, second)
        self.assertEqual(third['kind'], 'baseline_reconciliation')
        self.assertEqual(third['parent_generation_id'], BASE)
        self.assertEqual(third['expected_previous_generation_id'], SECOND)
        self.assertEqual(third['supersedes_generation_id'], SECOND)
        self.assertTrue(third['replaces_delta_chain'])
        self.assertIn('new_empty', third['source_row_counts'])
        self.assertEqual(preparer.file_digest(self.baseline), original_hash)
        self.assertEqual(self.db.execute('SELECT value FROM precious').fetchone()[0], 'original')

    def test_large_native_transport_is_split_and_not_in_remote_inventory(self):
        self.db.execute('INSERT INTO precious VALUES(?)', (b'a' * 400000,))
        self.db.commit()
        plan = self.prepare(FIRST, max_part_bytes=256*1024)
        self.assertEqual(plan['transport']['kind'], 'lossless_sqlite_shards')
        self.assertTrue((self.root / FIRST / 'changes.sqlite3').is_file())
        self.assertNotIn('changes.sqlite3', [item['local'] for item in plan['files']])
        self.assertTrue(plan['transport']['verification']['coverage']['all_values'])
        for item in plan['files']:
            if item['local'].startswith('transport/shared/'):
                self.assertLessEqual(item['bytes'], 256*1024)

    def test_concurrent_writer_after_pin_is_excluded_and_caught_next_cycle(self):
        real_builder = preparer.build_delta_database
        def concurrent(records, metadata, output):
            self.db.execute("UPDATE precious SET value='later' WHERE rowid=-8")
            self.db.commit()
            return real_builder(records, metadata, output)
        with patch.object(preparer, 'build_delta_database', concurrent):
            first = self.prepare(FIRST)
        self.assertEqual([r['values'] for r in self.records(first)
                          if r['table_name']=='precious'], [(b'\x00raw',)])
        second = self.prepare(SECOND, first)
        self.assertEqual([r['values'] for r in self.records(second)
                          if r['table_name']=='precious'], [('later',)])

    def test_failure_preserves_artifacts_and_does_not_write_success_plan(self):
        with patch.object(preparer, 'export_full_xlsx', side_effect=RuntimeError('failure')):
            with self.assertRaises(RuntimeError):
                self.prepare(FIRST)
        directory = self.root / FIRST
        self.assertTrue((directory / 'changes.sqlite3').is_file())
        self.assertEqual(json.loads((directory / 'failed.json').read_text())['phase'], 'lossless_xlsx')
        self.assertFalse((directory / 'delta-plan.json').exists())
        self.db.execute("UPDATE precious SET value='writer still works' WHERE rowid=-8")
        self.db.commit()

    def test_invalid_previous_or_baseline_refused_before_generation_creation(self):
        with self.assertRaisesRegex(ValueError, 'PREVIOUS_GENERATION_INVALID'):
            self.prepare(FIRST, {'status':'prepared'})
        self.assertFalse((self.root / FIRST).exists())
        manifest = json.loads(self.manifest.read_text())
        manifest['raw_snapshot']['sha256'] = '0'*64
        self.manifest.write_text(json.dumps(manifest))
        with self.assertRaisesRegex(ValueError, 'BASELINE_DIGEST_MISMATCH'):
            self.prepare(FIRST)
        self.assertFalse((self.root / FIRST).exists())

    def test_process_local_token_reuses_one_full_hash_across_reconciliation_and_cycles(self):
        original = self.baseline.read_bytes()
        real_hash = preparer._hash_fd
        with patch.object(preparer, '_hash_fd', wraps=real_hash) as hash_spy:
            token = preparer.verify_baseline_once(self.baseline, self.manifest)
            self.assertEqual(hash_spy.call_count, 1)
            first = self.prepare(FIRST, baseline_verification=token)
            self.assertEqual(first['kind'], 'baseline_reconciliation')
            self.db.execute("UPDATE precious SET value='cycle two' WHERE rowid=-8")
            self.db.commit()
            second = self.prepare(SECOND, first, baseline_verification=token)
            self.assertEqual(second['kind'], 'change_feed')
            self.db.execute("UPDATE precious SET value='cycle three' WHERE rowid=-8")
            self.db.commit()
            third = self.prepare(THIRD, second, baseline_verification=token)
            self.assertEqual(third['kind'], 'change_feed')
            self.assertEqual(hash_spy.call_count, 1)
        changes = [record for record in self.records(third)
                   if record['table_name'] == 'precious']
        self.assertEqual(changes[-1]['values'], ('cycle three',))
        self.assertEqual(self.baseline.read_bytes(), original)

    def test_default_path_hashes_baseline_for_each_cycle(self):
        real_hash = preparer._hash_fd
        with patch.object(preparer, '_hash_fd', wraps=real_hash) as hash_spy:
            first = self.prepare(FIRST)
            self.db.execute("UPDATE precious SET value='next' WHERE rowid=-8")
            self.db.commit()
            self.prepare(SECOND, first)
        self.assertEqual(hash_spy.call_count, 2)

    def test_token_cannot_be_directly_constructed_or_serialized(self):
        import pickle
        token = preparer.verify_baseline_once(self.baseline, self.manifest)
        with self.assertRaisesRegex(TypeError, 'verify_baseline_once'):
            preparer.VerifiedBaseline()
        with self.assertRaisesRegex(TypeError, 'cannot be serialized'):
            pickle.dumps(token)

    def test_token_rejects_same_size_rewrite_even_when_mtime_is_restored(self):
        token = preparer.verify_baseline_once(self.baseline, self.manifest)
        before = self.baseline.stat()
        with self.baseline.open('r+b') as handle:
            first = handle.read(1)
            handle.seek(0)
            handle.write(bytes([first[0] ^ 1]))
            handle.flush()
            os.fsync(handle.fileno())
        os.utime(self.baseline, ns=(before.st_atime_ns, before.st_mtime_ns))
        after = self.baseline.stat()
        self.assertEqual(after.st_size, before.st_size)
        self.assertEqual(after.st_mtime_ns, before.st_mtime_ns)
        self.assertNotEqual(after.st_ctime_ns, before.st_ctime_ns)
        with self.assertRaisesRegex(ValueError, 'BASELINE_VERIFICATION_STALE'):
            token.validate(self.baseline, self.manifest)

    def test_token_rejects_inode_replacement_manifest_change_and_sidecar(self):
        token = preparer.verify_baseline_once(self.baseline, self.manifest)
        replacement = self.root / 'replacement.sqlite3'
        replacement.write_bytes(self.baseline.read_bytes())
        os.replace(replacement, self.baseline)
        with self.assertRaisesRegex(ValueError, 'BASELINE_VERIFICATION_STALE'):
            token.validate(self.baseline, self.manifest)

        # A newly verified token is bound to the new inode; changing manifest
        # bytes or introducing a live SQLite sidecar invalidates it as well.
        token = preparer.verify_baseline_once(self.baseline, self.manifest)
        self.manifest.write_bytes(self.manifest.read_bytes() + b' ')
        with self.assertRaisesRegex(ValueError, 'BASELINE_VERIFICATION_STALE'):
            token.validate(self.baseline, self.manifest)

        self.manifest.write_text(json.dumps({'storage':'plaintext','encryption':None,
            'raw_snapshot': {'basename': self.baseline.name, 'quick_check':'ok',
                             **preparer.file_digest(self.baseline)}}))
        token = preparer.verify_baseline_once(self.baseline, self.manifest)
        Path(str(self.baseline) + '-wal').write_bytes(b'wal')
        with self.assertRaisesRegex(ValueError, 'BASELINE_VERIFICATION_STALE'):
            token.validate(self.baseline, self.manifest)

    def test_baseline_symlink_is_rejected(self):
        token = preparer.verify_baseline_once(self.baseline, self.manifest)
        original = self.root / 'original.sqlite3'
        self.baseline.rename(original)
        self.baseline.symlink_to(original)
        with self.assertRaisesRegex(ValueError, 'BASELINE_VERIFICATION_STALE'):
            token.validate(self.baseline, self.manifest)

    def test_baseline_mutation_during_xlsx_export_prevents_success_plan(self):
        original_export = preparer.export_full_xlsx
        baseline_stat = self.baseline.stat()

        def export_then_mutate(*args, **kwargs):
            result = original_export(*args, **kwargs)
            with self.baseline.open('r+b') as handle:
                first = handle.read(1)
                handle.seek(0)
                handle.write(bytes([first[0] ^ 1]))
                handle.flush()
                os.fsync(handle.fileno())
            os.utime(self.baseline, ns=(baseline_stat.st_atime_ns, baseline_stat.st_mtime_ns))
            return result

        token = preparer.verify_baseline_once(self.baseline, self.manifest)
        with patch.object(preparer, 'export_full_xlsx', side_effect=export_then_mutate):
            with self.assertRaisesRegex(ValueError, 'BASELINE_VERIFICATION_STALE'):
                self.prepare(FIRST, baseline_verification=token)
        directory = self.root / FIRST
        self.assertFalse((directory / 'delta-plan.json').exists())
        failure = json.loads((directory / 'failed.json').read_text())
        self.assertEqual(failure['error_code'], 'BASELINE_VERIFICATION_STALE')
        self.assertFalse(failure['published_checkpoint_advanced'])

if __name__ == '__main__':
    unittest.main()
