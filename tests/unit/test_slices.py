"""全情報スライスの要求・公開境界を人工DBで検査する。実アカウントは使わない。"""
import argparse
from contextlib import redirect_stdout
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import stat
import sys
import tempfile
import unittest
from unittest.mock import patch
from io import StringIO

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'src/python'))
sys.path.insert(0, str(ROOT))

import archive
from ikarchive.slices import (
    _ensure_dir,
    export_slices,
    list_datasets,
    refresh_slices,
    sync_tags_to_slices,
)
from ikarchive.store import Store


class SliceControlTests(unittest.TestCase):
    RAW_SENTINEL = 'PRIVATE_RAW_BODY_MUST_NOT_LEAK'

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        # macOS /var may itself be a symlink; use the physical temporary root
        # so path-guard tests distinguish deliberate fixture symlinks.
        self.root = Path(self.tmp.name).resolve()
        self.db_path = self.root / 'database' / 'archive.sqlite3'
        self.db_path.parent.mkdir(mode=0o700)
        store = Store(self.db_path)
        try:
            raw = self.RAW_SENTINEL.encode('ascii')
            sha = hashlib.sha256(raw).hexdigest()
            store.db.execute(
                'INSERT INTO bodies(sha256,body,byte_length) VALUES(?,?,?)',
                (sha, raw, len(raw)),
            )
            store.db.execute(
                'INSERT INTO matches(account,kind,match_key,first_seen,last_seen) VALUES(?,?,?,?,?)',
                ('account-a', 'vs', 'match-1', 'seen-1', 'seen-1'),
            )
            store.db.execute(
                'INSERT INTO jobs(account,operation,variables_json,state) VALUES(?,?,?,?)',
                ('account-a', 'RouteQuery', '{}', 'pending'),
            )
            store.db.commit()
        finally:
            store.close()
        self.dest = self.db_path.parent / 'slices'

    def tearDown(self):
        self.tmp.cleanup()

    @staticmethod
    def _tree_bytes(root):
        return {
            path.relative_to(root).as_posix(): path.read_bytes()
            for path in root.rglob('*')
            if path.is_file() and not path.is_symlink()
        }

    def _legacy_partial_store(self):
        self.dest.mkdir(mode=0o700, parents=True)
        (self.dest / 'by-mode').mkdir(mode=0o700)
        (self.dest / 'by-rule').mkdir(mode=0o700)
        (self.dest / 'manifest.json').write_text(
            json.dumps({
                'version': 1,
                'by_mode': [{'file': 'by-mode/xmatch.sqlite3'}],
                'by_rule': [{'file': 'by-rule/xmatch__AREA.sqlite3'}],
                'source_bytes': 123,
            }),
            encoding='utf-8',
        )
        (self.dest / 'by-mode' / 'xmatch.sqlite3').write_bytes(b'old-partial-mode-bytes')
        (self.dest / 'by-rule' / 'xmatch__AREA.sqlite3').write_bytes(b'old-partial-rule-bytes')

    def _tag_args(self):
        return argparse.Namespace(
            tag_action='add',
            account='account-a',
            match_key='match-1',
            tag='練習',
            note=None,
        )

    def _sync_args(self):
        return argparse.Namespace(
            command='sync',
            budget=1,
            delay=0,
            account='account-a',
            nxapi_data=None,
        )

    @staticmethod
    def _request(path):
        return json.loads(path.read_text(encoding='utf-8'))

    def test_ensure_dir_changes_new_directories_but_not_existing_parent(self):
        parent = self.root / 'existing-parent'
        parent.mkdir()
        os.chmod(parent, 0o755)
        target = parent / 'created' / 'leaf'
        real_mkdir = Path.mkdir

        def create_too_open(self_path, mode=0o777, parents=False, exist_ok=False):
            return real_mkdir(self_path, mode=0o777, parents=parents, exist_ok=exist_ok)

        with patch.object(Path, 'mkdir', create_too_open):
            _ensure_dir(target)

        self.assertEqual(stat.S_IMODE(parent.stat().st_mode), 0o755)
        self.assertEqual(stat.S_IMODE((parent / 'created').stat().st_mode), 0o700)
        self.assertEqual(stat.S_IMODE(target.stat().st_mode), 0o700)

    def test_full_export_uses_readonly_snapshot_without_changing_source_or_wal(self):
        wal_root = self.root / 'wal-source'
        wal_root.mkdir()
        source = wal_root / 'archive.sqlite3'
        writer = sqlite3.connect(source)
        try:
            writer.execute('PRAGMA journal_mode=WAL')
            writer.execute('PRAGMA wal_autocheckpoint=0')
            writer.execute('CREATE TABLE source_fixture(payload TEXT)')
            writer.execute('INSERT INTO source_fixture VALUES(?)', ('snapshot-visible',))
            writer.commit()
            wal = Path(str(source) + '-wal')
            self.assertTrue(wal.is_file())
            before_db = source.read_bytes()
            before_wal = wal.read_bytes()
            self.assertGreater(len(before_wal), 0)

            def fake_export(snapshot, store_dir, *, snapshot_id):
                self.assertEqual(
                    snapshot.execute('SELECT payload FROM source_fixture').fetchone()[0],
                    'snapshot-visible',
                )
                with self.assertRaises(sqlite3.OperationalError):
                    snapshot.execute('CREATE TABLE forbidden_write(value TEXT)')
                return {
                    'role': 'lossless_sqlite_shards',
                    'version': 2,
                    'snapshot_identifier': snapshot_id,
                    'counts': {'tables': 1, 'rows': 1},
                }

            with patch(
                'ikarchive.lossless_sqlite.export_sqlite_shards', side_effect=fake_export
            ) as export, patch(
                'ikarchive.lossless_sqlite.verify_sqlite_shards',
                return_value={'status': 'verified'},
            ) as verify, patch(
                'ikarchive.slices.export_selectors'
            ) as selectors, patch(
                'ikarchive.slices.verify_selectors',
                return_value={'status': 'verified'},
            ) as verify_selectors:
                result = export_slices(source, source.parent / 'slices')

            self.assertEqual(result['status'], 'verified')
            export.assert_called_once()
            verify.assert_called_once()
            selectors.assert_called_once()
            verify_selectors.assert_called_once()
            self.assertEqual(source.read_bytes(), before_db)
            self.assertEqual(wal.read_bytes(), before_wal)
            self.assertNotIn(self.RAW_SENTINEL, json.dumps(result))

            listed = list_datasets(source)
            self.assertEqual(listed['partition_status'], 'verified_store_reader_pending')
            self.assertEqual(listed['items'], [{'token': 'unified', 'axis': 'unified'}])
        finally:
            writer.close()

    def test_export_rejects_source_and_destination_symlinks(self):
        source_link = self.root / 'source-link.sqlite3'
        source_link.symlink_to(self.db_path)
        with self.assertRaisesRegex(ValueError, 'SLICE_PATH_SYMLINK'):
            export_slices(source_link, self.root / 'out-from-source-link')

        actual_destination = self.root / 'actual-destination'
        actual_destination.mkdir()
        destination_link = self.root / 'destination-link'
        destination_link.symlink_to(actual_destination, target_is_directory=True)
        with self.assertRaisesRegex(ValueError, 'SLICE_PATH_SYMLINK'):
            export_slices(self.db_path, destination_link)
        self.assertEqual(list(actual_destination.iterdir()), [])

    def test_refresh_rejects_symlink_source_and_absent_destination_stays_absent(self):
        absent = refresh_slices(self.db_path, self.dest)
        self.assertEqual(absent['slice_refresh'], 'absent')
        self.assertFalse(self.dest.exists())
        self.assertEqual(
            sync_tags_to_slices(self.db_path, 'account-a', 'match-1', [])['slice_sync'],
            'absent',
        )
        self.assertFalse(self.dest.exists())

        self.dest.mkdir()
        source_link = self.root / 'database' / 'source-link.sqlite3'
        source_link.symlink_to(self.db_path)
        with self.assertRaisesRegex(ValueError, 'SYMLINK'):
            refresh_slices(source_link, self.dest)
        self.assertFalse((self.dest / 'update-request.json').exists())

    def test_raw_body_and_job_only_changes_each_queue_full_scope_request(self):
        self.dest.mkdir()
        previous_request_id = None
        updates = (
            ('UPDATE bodies SET body=? WHERE sha256=?',
             (b'PRIVATE_RAW_BODY_REPLACED', hashlib.sha256(self.RAW_SENTINEL.encode()).hexdigest())),
            ('UPDATE jobs SET state=?,next_attempt=? WHERE operation=?',
             ('retry', 123456.0, 'RouteQuery')),
        )
        for statement, parameters in updates:
            store = Store(self.db_path)
            try:
                store.db.execute(statement, parameters)
                store.db.commit()
            finally:
                store.close()

            result = refresh_slices(self.db_path, self.dest)
            self.assertEqual(result['slice_refresh'], 'pending_full_generation')
            request_path = self.dest / 'update-request.json'
            request = self._request(request_path)
            self.assertEqual(request['information_scope'], 'all_tables_all_values')
            self.assertEqual(request['reason'], 'committed_collection_or_tag_update')
            self.assertNotEqual(request['request_id'], previous_request_id)
            self.assertNotIn(self.RAW_SENTINEL, request_path.read_text(encoding='utf-8'))
            self.assertNotIn(str(self.db_path), request_path.read_text(encoding='utf-8'))
            self.assertEqual(
                set(request['source_state'][0]), {'suffix', 'bytes', 'mtime_ns'}
            )
            previous_request_id = request['request_id']

        repeated = refresh_slices(self.db_path, self.dest)
        self.assertEqual(repeated['slice_refresh'], 'pending_full_generation')
        self.assertNotEqual(repeated['request_id'], previous_request_id)

    def test_tag_update_only_queues_and_keeps_existing_partial_files_byte_identical(self):
        self._legacy_partial_store()
        before = self._tree_bytes(self.dest)

        store = Store(self.db_path)
        try:
            result = archive.apply_tag(store, self._tag_args())
            stored_tag = store.db.execute(
                'SELECT tag FROM match_tags WHERE account=? AND match_key=?',
                ('account-a', 'match-1'),
            ).fetchone()[0]
        finally:
            store.close()

        self.assertEqual(stored_tag, '練習')
        self.assertEqual(result['slice_sync'], 'pending_full_generation')
        after = self._tree_bytes(self.dest)
        for relative, contents in before.items():
            self.assertEqual(after[relative], contents, relative)
        self.assertEqual(set(after), set(before) | {'update-request.json'})
        request = self._request(self.dest / 'update-request.json')
        self.assertEqual(request['information_scope'], 'all_tables_all_values')
        self.assertNotIn(self.RAW_SENTINEL, json.dumps(request))

    def test_legacy_partial_datasets_are_listed_as_incomplete_without_file_tokens(self):
        self._legacy_partial_store()
        invalid_database = self.db_path.parent / 'not-sqlite.sqlite3'
        invalid_database.write_bytes(b'not a database')
        before = invalid_database.read_bytes()

        with patch('ikarchive.slices.sqlite3.connect', side_effect=AssertionError('database opened')):
            listed = list_datasets(invalid_database)

        self.assertEqual(listed['partition_status'], 'legacy_incomplete')
        self.assertEqual(listed['items'], [{'token': 'unified', 'axis': 'unified'}])
        encoded = json.dumps(listed)
        self.assertNotIn('xmatch', encoded)
        self.assertNotIn('by-mode', encoded)
        self.assertNotIn('by-rule', encoded)
        self.assertEqual(invalid_database.read_bytes(), before)

    def test_dataset_listing_rejects_traversal_and_symlink_paths(self):
        self.dest.mkdir()
        pointer = self.dest / 'current-generation.json'
        pointer.write_text(json.dumps({
            'version': 2,
            'status': 'verified',
            'generation_id': 'generation-1',
            'root': '../outside',
            'source': {'sha256': 'a' * 64},
        }), encoding='utf-8')
        with self.assertRaisesRegex(ValueError, 'SELECTOR_UNSAFE_PATH'):
            list_datasets(self.db_path)

        pointer.unlink()
        external_pointer = self.root / 'external-pointer.json'
        external_pointer.write_text('{}', encoding='utf-8')
        pointer.symlink_to(external_pointer)
        with self.assertRaisesRegex(ValueError, 'SELECTOR_SYMLINK'):
            list_datasets(self.db_path)

    def test_failed_sync_still_queues_full_refresh(self):
        self._legacy_partial_store()
        store = Store(self.db_path)
        try:
            with patch('archive.OUTPUT', self.root / 'client-output'), patch(
                'archive.sync', side_effect=RuntimeError('SYNC_ROOT_CAUSE')
            ), patch(
                'ikarchive.publish.publish_outputs', return_value={'status': 'mocked'}
            ), patch(
                'ikarchive.slices.refresh_slices', wraps=refresh_slices
            ) as refresh:
                with self.assertRaisesRegex(RuntimeError, 'SYNC_ROOT_CAUSE'):
                    archive.dispatch(store, self._sync_args())
        finally:
            store.close()

        refresh.assert_called_once_with(self.db_path, self.dest)
        request = self._request(self.dest / 'update-request.json')
        self.assertEqual(request['information_scope'], 'all_tables_all_values')

    def test_publish_failure_still_queues_refresh(self):
        self._legacy_partial_store()
        store = Store(self.db_path)
        output = StringIO()
        try:
            with patch('archive.OUTPUT', self.root / 'client-output'), patch(
                'archive.sync', return_value={'state': 'mock_sync_ok'}
            ), patch(
                'ikarchive.publish.publish_outputs', side_effect=RuntimeError('PUBLISH_FAILED')
            ), patch(
                'ikarchive.slices.refresh_slices', wraps=refresh_slices
            ) as refresh, redirect_stdout(output):
                code = archive.dispatch(store, self._sync_args())
        finally:
            store.close()

        self.assertEqual(code, 0)
        refresh.assert_called_once_with(self.db_path, self.dest)
        payload = json.loads(output.getvalue())
        self.assertEqual(payload['exports'], {'error': 'RuntimeError'})
        self.assertEqual(payload['slices']['slice_refresh'], 'pending_full_generation')
        self.assertEqual(
            self._request(self.dest / 'update-request.json')['information_scope'],
            'all_tables_all_values',
        )

    def test_sync_and_refresh_failures_keep_sync_error_as_primary_cause(self):
        self.dest.mkdir()
        store = Store(self.db_path)
        refresh_error = OSError('REFRESH_ROOT_CAUSE')
        try:
            with patch('archive.OUTPUT', self.root / 'client-output'), patch(
                'archive.sync', side_effect=RuntimeError('SYNC_ROOT_CAUSE')
            ), patch(
                'ikarchive.publish.publish_outputs', return_value={'status': 'mocked'}
            ), patch(
                'ikarchive.slices.refresh_slices', side_effect=refresh_error
            ) as refresh:
                with self.assertRaises(RuntimeError) as caught:
                    archive.dispatch(store, self._sync_args())
        finally:
            store.close()

        refresh.assert_called_once_with(self.db_path, self.dest)
        self.assertEqual(str(caught.exception), 'SYNC_ROOT_CAUSE')
        self.assertIs(caught.exception.__cause__, refresh_error)

    def test_successful_sync_propagates_refresh_failure(self):
        self.dest.mkdir()
        store = Store(self.db_path)
        refresh_error = OSError('REFRESH_ROOT_CAUSE')
        try:
            with patch('archive.OUTPUT', self.root / 'client-output'), patch(
                'archive.sync', return_value={'state': 'mock_sync_ok'}
            ), patch(
                'ikarchive.publish.publish_outputs', return_value={'status': 'mocked'}
            ), patch(
                'ikarchive.slices.refresh_slices', side_effect=refresh_error
            ) as refresh:
                with self.assertRaises(OSError) as caught:
                    archive.dispatch(store, self._sync_args())
        finally:
            store.close()

        refresh.assert_called_once_with(self.db_path, self.dest)
        self.assertIs(caught.exception, refresh_error)
        self.assertIsNone(caught.exception.__cause__)


if __name__ == '__main__':
    unittest.main()
