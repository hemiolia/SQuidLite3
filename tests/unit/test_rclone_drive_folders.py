"""rclone Drive folder ID cache tests; callback behavior is synthetic."""

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import sys
import time
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'scripts'))

from rclone_drive_folders import DriveFolderCache


def _error_factory(category):
    return ValueError(category)


def _folder(folder_id, is_dir=True):
    return {'ID': folder_id, 'IsDir': is_dir}


class RelativeDrive:
    """Emulate rclone --drive-root-folder-id path resolution.

    Without a parent ID, the callback must receive the complete top-level
    remote path. With an ID, it must receive exactly one child basename; the
    fake resolves that basename below the folder bound to the ID.
    """

    def __init__(self, directories):
        self.directories = directories
        self.calls = []
        self.metadata_overrides = {}
        self.ids = {folder_id: path for path, folder_id in directories.items()}

    def stat(self, path, parent_folder_id=None):
        self.calls.append((path, parent_folder_id))
        if path.count(':') != 1:
            raise AssertionError('callback path must have exactly one remote separator')
        remote, relative = path.split(':', 1)
        if not relative:
            raise AssertionError('the configured remote root has no cacheable folder ID')
        if '/' in relative:
            if parent_folder_id is not None:
                raise AssertionError('a parent-ID lookup must use one basename only')
            full_path = path
        elif parent_folder_id is None:
            full_path = path
        else:
            parent = self.ids.get(parent_folder_id)
            if parent is None or parent.split(':', 1)[0] != remote:
                raise AssertionError('parent ID is not bound to this remote')
            full_path = parent + '/' + relative

        folder_id = self.directories.get(full_path)
        if full_path in self.metadata_overrides:
            return self.metadata_overrides[full_path]
        return None if folder_id is None else _folder(folder_id)


class DriveFolderCacheTests(unittest.TestCase):
    def test_reuses_directory_lookups_and_passes_child_basenames(self):
        drive = RelativeDrive({
            'drive:archive': 'archive-1',
            'drive:archive/generations': 'generations-1',
        })
        cache = DriveFolderCache(drive.stat, _error_factory)
        self.assertEqual(
            cache.route('drive:archive/generations/a.json'),
            ('drive:a.json', 'generations-1'),
        )
        self.assertEqual(
            cache.route('drive:archive/generations/b.sqlite3'),
            ('drive:b.sqlite3', 'generations-1'),
        )
        self.assertEqual(drive.calls, [
            ('drive:archive', None),
            ('drive:generations', 'archive-1'),
        ])

        cache.verify_bindings()
        self.assertEqual(drive.calls[2:], [
            ('drive:archive', None),
            ('drive:generations', 'archive-1'),
        ])

    def test_different_remotes_do_not_share_cached_directory_ids(self):
        drive = RelativeDrive({
            'first:shared': 'first-shared',
            'second:shared': 'second-shared',
        })
        cache = DriveFolderCache(drive.stat, _error_factory)
        self.assertEqual(cache.route('first:shared/one.bin'), ('first:one.bin', 'first-shared'))
        self.assertEqual(cache.route('second:shared/two.bin'), ('second:two.bin', 'second-shared'))
        self.assertEqual(drive.calls, [
            ('first:shared', None),
            ('second:shared', None),
        ])

    def test_missing_parent_is_not_cached_and_full_path_is_returned(self):
        drive = RelativeDrive({})
        cache = DriveFolderCache(drive.stat, _error_factory)
        original = 'drive:late/subdir/file.bin'
        self.assertEqual(cache.route(original), (original, None))
        self.assertEqual(drive.calls, [('drive:late', None)])

        drive.directories['drive:late'] = 'late-id'
        drive.directories['drive:late/subdir'] = 'subdir-id'
        drive.ids.update({'late-id': 'drive:late', 'subdir-id': 'drive:late/subdir'})
        self.assertEqual(cache.route(original), ('drive:file.bin', 'subdir-id'))
        self.assertEqual(drive.calls[1:], [
            ('drive:late', None),
            ('drive:subdir', 'late-id'),
        ])

    def test_root_level_file_is_unchanged_and_never_stats_remote_alias(self):
        drive = RelativeDrive({})
        cache = DriveFolderCache(drive.stat, _error_factory)
        self.assertEqual(cache.route('drive:file.bin'), ('drive:file.bin', None))
        cache.verify_bindings()
        self.assertEqual(drive.calls, [])

    def test_concurrent_routes_share_each_directory_discovery_once(self):
        class SlowDrive(RelativeDrive):
            def stat(self, path, parent_folder_id=None):
                time.sleep(0.01)
                return super().stat(path, parent_folder_id)

        drive = SlowDrive({
            'drive:archive': 'archive-1',
            'drive:archive/generations': 'generations-1',
        })
        cache = DriveFolderCache(drive.stat, _error_factory)
        paths = [f'drive:archive/generations/item-{index}.json' for index in range(12)]
        with ThreadPoolExecutor(max_workers=8) as executor:
            results = list(executor.map(cache.route, paths))
        self.assertEqual(results, [
            (f'drive:item-{index}.json', 'generations-1') for index in range(12)
        ])
        self.assertEqual(drive.calls, [
            ('drive:archive', None),
            ('drive:generations', 'archive-1'),
        ])
        cache.verify_bindings()
        self.assertEqual(drive.calls[2:], [
            ('drive:archive', None),
            ('drive:generations', 'archive-1'),
        ])

    def test_file_at_expected_directory_path_and_unsafe_ids_are_rejected(self):
        cases = (
            ({'drive:child': _folder('a-file', is_dir=False)}, 'drive:child'),
            ({'drive:child': _folder('bad/id')}, 'drive:child'),
            ({'drive:child': _folder('unsafe id')}, 'drive:child'),
            ({'drive:child': _folder('é')}, 'drive:child'),
            ({'drive:child': _folder('x' * 257)}, 'drive:child'),
            ({'drive:child': {'ID': 'missing-isdir'}}, 'drive:child'),
            ({'drive:child': {'IsDir': True}}, 'drive:child'),
        )
        for raw, child_path in cases:
            with self.subTest(raw=raw):
                class MetadataDrive:
                    def __init__(self):
                        self.calls = []

                    def stat(self, path, parent_folder_id=None):
                        self.calls.append((path, parent_folder_id))
                        if path == child_path:
                            return raw[child_path]
                        return None

                drive = MetadataDrive()
                cache = DriveFolderCache(drive.stat, _error_factory)
                with self.assertRaisesRegex(ValueError, 'REMOTE_DIRECTORY_INVALID'):
                    cache.route('drive:child/file.bin')

    def test_top_level_and_child_id_changes_fail_final_verification(self):
        for changed_path in ('drive:database', 'drive:database/shared'):
            with self.subTest(changed_path=changed_path):
                drive = RelativeDrive({
                    'drive:database': 'database-before',
                    'drive:database/shared': 'shared-before',
                })
                cache = DriveFolderCache(drive.stat, _error_factory)
                self.assertEqual(
                    cache.route('drive:database/shared/file.bin'),
                    ('drive:file.bin', 'shared-before'),
                )
                drive.directories[changed_path] = 'replacement-id'
                drive.ids['replacement-id'] = changed_path
                with self.assertRaisesRegex(ValueError, 'REMOTE_DIRECTORY_BINDING_CHANGED'):
                    cache.verify_bindings()

    def test_missing_or_invalid_binding_during_final_check_fails_closed(self):
        replacements = (None, _folder('shared-before', is_dir=False), _folder('unsafe/id'))
        for replacement in replacements:
            with self.subTest(replacement=replacement):
                drive = RelativeDrive({
                    'drive:database': 'database-id',
                    'drive:database/shared': 'shared-before',
                })
                cache = DriveFolderCache(drive.stat, _error_factory)
                cache.route('drive:database/shared/file.bin')

                if replacement is None:
                    del drive.directories['drive:database/shared']
                else:
                    drive.directories['drive:database/shared'] = replacement['ID']
                    drive.ids[replacement['ID']] = 'drive:database/shared'
                    if replacement['IsDir'] is not True:
                        # Return a file-shaped object from the relative lookup.
                        drive.metadata_overrides['drive:database/shared'] = replacement
                with self.assertRaisesRegex(ValueError, 'REMOTE_DIRECTORY_BINDING_CHANGED'):
                    cache.verify_bindings()

    def test_callback_exception_propagates_unchanged(self):
        expected = ConnectionError('temporary transport failure')

        def stat(_path, _parent_folder_id=None):
            raise expected

        cache = DriveFolderCache(stat, _error_factory)
        with self.assertRaises(ConnectionError) as captured:
            cache.route('drive:folder/file.bin')
        self.assertIs(captured.exception, expected)

    def test_unsafe_file_remote_paths_are_rejected_before_callback(self):
        calls = []

        def stat(path, parent_folder_id=None):
            calls.append((path, parent_folder_id))
            return _folder('id')

        cache = DriveFolderCache(stat, _error_factory)
        invalid = (
            '', 'drive', ':file', 'bad name:file', 'drive:',
            'drive:/file', 'drive:file/', 'drive:a//file',
            'drive:./file', 'drive:../file', 'drive:a\\file',
            'drive:a:b', 'drivé:file', 'drive:雪/file',
        )
        for value in invalid:
            with self.subTest(value=value):
                with self.assertRaisesRegex(ValueError, 'REMOTE_PATH_INVALID'):
                    cache.route(value)
        self.assertEqual(calls, [])


if __name__ == '__main__':
    unittest.main()
