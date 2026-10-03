"""Drive ID routing must preserve publication bytes and final path bindings."""
import hashlib
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'scripts'))
import nas_full_data_publish as publisher


class DriveServer:
    def __init__(self):
        self.directories = {'': 'rootid', 'database': 'databaseid',
                            'database/shared': 'sharedid', 'database/shared/table': 'tableid'}
        self.objects = {}
        self.commands = []
        self.directory_listing_overrides = {}

    def path(self, command, remote):
        relative = remote.split(':', 1)[1]
        if '--drive-root-folder-id' in command:
            identity = command[command.index('--drive-root-folder-id') + 1]
            parents = [path for path, value in self.directories.items() if value == identity]
            if len(parents) != 1:
                raise AssertionError('unknown or ambiguous parent ID')
            relative = '/'.join(value for value in (parents[0], relative) if value)
        return relative

    def stat(self, command):
        self.commands.append(command)
        if '--dirs-only' in command:
            parent = self.path(command, command[5])
            if parent in self.directory_listing_overrides:
                entries = self.directory_listing_overrides[parent]
                return subprocess.CompletedProcess(command, 0, json.dumps(entries).encode(), b'')
            entries = []
            for directory, identity in self.directories.items():
                base, _, name = directory.rpartition('/')
                if directory and base == parent:
                    entries.append({'Name': name, 'IsDir': True, 'Size': -1, 'ID': identity})
            return subprocess.CompletedProcess(command, 0, json.dumps(entries).encode(), b'')
        path = self.path(command, command[4])
        if path in self.directories:
            # Actual NAS rclone --stat directory has no ID.
            value = {'IsDir': True, 'Size': -1}
        elif path in self.objects:
            value = {'IsDir': False, 'Size': len(self.objects[path]), 'ID': 'fileid'}
        else:
            return subprocess.CompletedProcess(command, 3, b'', b'object not found')
        return subprocess.CompletedProcess(command, 0, json.dumps(value).encode(), b'')

    def cat(self, command, *, capture_limit=None):
        self.commands.append(command)
        raw = self.objects[self.path(command, command[2])]
        return len(raw), hashlib.sha256(raw).hexdigest(), raw if capture_limit is not None else None

    def copy(self, command, **kwargs):
        self.commands.append(command)
        offset = 3 if '--immutable' in command else 2
        source, remote = command[offset:offset + 2]
        path = self.path(command, remote)
        if '--immutable' in command and path in self.objects:
            if self.objects[path] != Path(source).read_bytes():
                raise AssertionError('immutable conflict')
        self.objects[path] = Path(source).read_bytes()
        return type('Process', (), {'stderr': io.BytesIO(), 'wait': lambda self: 0})()


class MissingParentDriveServer:
    """Fake Drive tree whose first path-based copy creates its missing parent."""

    def __init__(self, *, race_path_copies=False, create_parent=True,
                 duplicate_parent_after_copy=False):
        self.directories = [('', 'rootid'), ('database', 'databaseid')]
        self.objects = {}
        self.commands = []
        self.path_copies = []
        self.folder_id_copies = []
        self.lock = threading.Lock()
        self.race_barrier = threading.Barrier(2) if race_path_copies else None
        self.create_parent = create_parent
        self.duplicate_parent_after_copy = duplicate_parent_after_copy

    @staticmethod
    def _remote(command):
        return next(value for value in command if isinstance(value, str)
                    and value.startswith('drive:'))

    def _path(self, command):
        relative = self._remote(command).split(':', 1)[1].strip('/')
        if '--drive-root-folder-id' not in command:
            return relative
        folder_id = command[command.index('--drive-root-folder-id') + 1]
        parent = next(path for path, identity in self.directories
                      if identity == folder_id)
        return '/'.join(value for value in (parent, relative) if value)

    def stat(self, command):
        with self.lock:
            self.commands.append(command)
            if '--dirs-only' in command:
                if '--drive-root-folder-id' in command:
                    parent = next(
                        path for path, identity in self.directories
                        if identity == command[command.index('--drive-root-folder-id') + 1]
                    )
                else:
                    parent = self._remote(command).split(':', 1)[1].strip('/')
                entries = []
                for path, identity in self.directories:
                    base, separator, name = path.rpartition('/')
                    if (base if separator else '') == parent and path:
                        entries.append({'Name': name, 'IsDir': True, 'Size': -1,
                                        'ID': identity})
                return subprocess.CompletedProcess(command, 0, json.dumps(entries).encode(), b'')

            path = self._path(command)
            raw = self.objects.get(path)
            if raw is None:
                return subprocess.CompletedProcess(command, 3, b'', b'object not found')
            return subprocess.CompletedProcess(
                command, 0,
                json.dumps({'IsDir': False, 'Size': len(raw), 'ID': 'fileid'}).encode(), b'',
            )

    def cat(self, command, *, capture_limit=None):
        with self.lock:
            self.commands.append(command)
            raw = self.objects[self._path(command)]
        return len(raw), hashlib.sha256(raw).hexdigest(), raw if capture_limit is not None else None

    def copy(self, command, **_kwargs):
        with self.lock:
            self.commands.append(command)
            offset = command.index('copyto') + 1
            if command[offset] == '--immutable':
                offset += 1
            source, _remote = command[offset:offset + 2]
            remote = self._remote(command)
            parent_missing = False
            directory = None
            if '--drive-root-folder-id' in command:
                target = self._path(command)
                self.folder_id_copies.append(target)
            else:
                target = remote.split(':', 1)[1].strip('/')
                directory = target.rpartition('/')[0]
                parent_missing = not any(path == directory for path, _identity in self.directories)
                self.path_copies.append(target)

        # Both unguarded workers observe the absent directory before either
        # creates it. The production serialization lock makes only one worker
        # reach this barrier in the fixed implementation, so this mode is used
        # by the explicit pre-fix comparison test only.
        if self.race_barrier is not None and parent_missing:
            self.race_barrier.wait(timeout=5)

        with self.lock:
            if parent_missing and self.create_parent:
                if self.duplicate_parent_after_copy:
                    for suffix in ('a', 'b'):
                        identity = f'created-{suffix}-{len(self.directories)}'
                        self.directories.append((directory, identity))
                elif (self.race_barrier is not None
                      or not any(path == directory for path, _identity in self.directories)):
                    identity = f'created-{len(self.directories)}'
                    self.directories.append((directory, identity))
            raw = Path(source).read_bytes()
            if '--immutable' in command and target in self.objects and self.objects[target] != raw:
                raise AssertionError('immutable conflict')
            self.objects[target] = raw

        class Process:
            stderr = io.BytesIO()
            def wait(self):
                return 0

        return Process()


class RcloneDrivePublicationTests(unittest.TestCase):
    def _publish_missing_parent(self, server, *, client_class=publisher.Rclone,
                                cache_enabled=True, workers=3, item_count=4):
        # The publisher rejects symlinked ancestors; macOS's default /var
        # temporary path is commonly an alias, so keep this fixture below the
        # resolved repository root instead.
        with tempfile.TemporaryDirectory(
                dir=Path(__file__).resolve().parents[2]) as temporary, \
                patch.object(publisher, 'run_stat_process', side_effect=server.stat), \
                patch.object(publisher, 'stream_remote_hash', side_effect=server.cat), \
                patch.object(publisher.subprocess, 'Popen', side_effect=server.copy), \
                patch.dict('os.environ', {
                    'IKARING_ARCHIVE_PUBLISH_FILE_WORKERS': str(workers),
                }, clear=False):
            temporary_path = Path(temporary)
            root = temporary_path / 'root'
            state = temporary_path / 'state'
            root.mkdir()
            state.mkdir()
            items = []
            expected_objects = {}
            for index in range(item_count):
                local = f'part-{index}.bin'
                raw = f'payload-{index}-with-exact-bytes'.encode()
                (root / local).write_bytes(raw)
                remote = f'generation/{local}'
                items.append({'local': local, 'remote': remote,
                              'bytes': len(raw),
                              'sha256': hashlib.sha256(raw).hexdigest()})
                expected_objects[f'database/{remote}'] = raw

            progress = {'path': state / 'progress.json', 'remote': 'drive:database',
                        'receipts': {}}
            client = client_class(drive_folder_cache=cache_enabled)
            category = None
            verified = 0
            try:
                verified = publisher._publish_files(client, root, state, items, progress)
                publisher.verify_remote_directories(client)
            except publisher.PublishError as exc:
                category = exc.category
            all_sources_retained = all((root / item['local']).is_file() for item in items)

        return {
            'category': category,
            'verified': verified,
            'progress': progress,
            'expected_objects': expected_objects,
            'all_sources_retained': all_sources_retained,
        }

    def test_default_cache_disabled_preserves_full_path_rclone_behavior(self):
        server = DriveServer()
        server.objects['database/shared/payload.bin'] = b'payload'
        with patch.dict('os.environ', {}, clear=True), \
                patch.object(publisher, 'run_stat_process', side_effect=server.stat):
            client = publisher.Rclone()
            result = client.stat('drive:database/shared/payload.bin')
            client.verify_directory_bindings()

        self.assertEqual(result['Size'], len(b'payload'))
        self.assertEqual(len(server.commands), 1)
        command = server.commands[0]
        self.assertEqual(command[1], 'lsjson')
        self.assertIn('--stat', command)
        self.assertEqual(command[4], 'drive:database/shared/payload.bin')
        self.assertNotIn('--dirs-only', command)
        self.assertNotIn('--drive-root-folder-id', command)

    def test_directory_listing_file_metadata_is_rejected(self):
        server = DriveServer()
        server.directory_listing_overrides['database/shared'] = [
            {'Name': 'table', 'IsDir': False, 'Size': 12, 'ID': 'file-id'},
        ]
        with patch.object(publisher, 'run_stat_process', side_effect=server.stat):
            client = publisher.Rclone(drive_folder_cache=True)
            with self.assertRaises(publisher.PublishError) as raised:
                client.stat('drive:database/shared/table/payload.bin')
        self.assertEqual(raised.exception.category, 'REMOTE_DIRECTORY_INVALID')
        self.assertFalse(any('--stat' in command for command in server.commands))

    def test_duplicate_folder_names_are_rejected_as_ambiguous(self):
        server = DriveServer()
        server.directory_listing_overrides['database/shared'] = [
            {'Name': 'table', 'IsDir': True, 'Size': -1, 'ID': 'tableid'},
            {'Name': 'table', 'IsDir': True, 'Size': -1, 'ID': 'other-table-id'},
        ]
        with patch.object(publisher, 'run_stat_process', side_effect=server.stat):
            client = publisher.Rclone(drive_folder_cache=True)
            with self.assertRaises(publisher.PublishError) as raised:
                client.stat('drive:database/shared/table/payload.bin')
        self.assertEqual(raised.exception.category, 'REMOTE_DIRECTORY_AMBIGUOUS')
        self.assertFalse(any('--stat' in command for command in server.commands))

    def test_real_rclone_adapter_uses_relative_folder_ids_and_reads_every_byte(self):
        server = DriveServer()
        raw = b'original\x00bytes\xff'
        digest = hashlib.sha256(raw).hexdigest()
        with tempfile.TemporaryDirectory() as temporary, \
                patch.object(publisher, 'run_stat_process', side_effect=server.stat), \
                patch.object(publisher, 'stream_remote_hash', side_effect=server.cat), \
                patch.object(publisher.subprocess, 'Popen', side_effect=server.copy):
            path = Path(temporary) / 'source'
            path.write_bytes(raw)
            client = publisher.Rclone(drive_folder_cache=True)
            remote = 'drive:database/shared/table/part.sqlite3'
            self.assertIsNone(client.stat(remote))
            client.copyto(path, remote, immutable=True)
            self.assertEqual(client.stat(remote)['Size'], len(raw))
            self.assertEqual(client.readback(remote), (len(raw), digest))
            self.assertEqual(client.readback_bytes(remote, len(raw), digest), raw)
            client.verify_directory_bindings()
        self.assertEqual(server.objects, {'database/shared/table/part.sqlite3': raw})
        file_commands = [command for command in server.commands
                         if 'drive:part.sqlite3' in command]
        self.assertEqual(len(file_commands), 5)
        for command in file_commands:
            self.assertEqual(command[command.index('--drive-root-folder-id') + 1], 'tableid')
        # One cold discovery and one final binding check, regardless of file operations.
        roots = [command for command in server.commands
                 if command[1] == 'lsjson' and '--dirs-only' in command
                 and command[5] == 'drive:' and '--drive-root-folder-id' not in command]
        self.assertEqual(len(roots), 2)

    def test_changed_folder_binding_prevents_index_upload(self):
        server = DriveServer()
        with tempfile.TemporaryDirectory() as temporary, \
                patch.object(publisher, 'run_stat_process', side_effect=server.stat), \
                patch.object(publisher, 'stream_remote_hash', side_effect=server.cat), \
                patch.object(publisher.subprocess, 'Popen', side_effect=server.copy):
            client = publisher.Rclone(drive_folder_cache=True)
            client.stat('drive:database/shared/table/part.sqlite3')
            server.directories['database/shared/table'] = 'replacementid'
            state = Path(temporary)
            progress = {'path': state / 'progress.json', 'receipts': {}}
            with self.assertRaises(publisher.PublishError) as raised:
                publisher._ensure_immutable_bytes(
                    client, state, 'drive:database/index.json', b'{}',
                    hashlib.sha256(b'{}').hexdigest(), progress=progress, receipt_key='index')
            self.assertEqual(raised.exception.category, 'REMOTE_DIRECTORY_BINDING_CHANGED')
            self.assertEqual(server.objects, {})
            self.assertFalse((state / 'progress.json').exists())

    def test_simultaneous_parent_and_child_changes_fail_directory_verification(self):
        server = DriveServer()
        with patch.object(publisher, 'run_stat_process', side_effect=server.stat):
            client = publisher.Rclone(drive_folder_cache=True)
            client.stat('drive:database/shared/table/part.sqlite3')
            # Both a parent binding and a descendant change after discovery.
            server.directories['database/shared'] = 'replacement-shared-id'
            server.directories['database/shared/table'] = 'replacement-table-id'
            with self.assertRaises(publisher.PublishError) as raised:
                client.verify_directory_bindings()
        self.assertEqual(raised.exception.category, 'REMOTE_DIRECTORY_BINDING_CHANGED')

    def test_transient_directory_listing_error_is_not_treated_as_missing(self):
        server = DriveServer()

        def transient_listing(command):
            if '--dirs-only' in command:
                server.commands.append(command)
                return subprocess.CompletedProcess(command, 5, b'', b'connection timed out')
            return server.stat(command)

        with patch.object(publisher, 'run_stat_process', side_effect=transient_listing), \
                patch.object(publisher.time, 'sleep') as sleep:
            client = publisher.Rclone(drive_folder_cache=True)
            with self.assertRaises(publisher.PublishError) as raised:
                client.stat('drive:database/shared/table/part.sqlite3')
        self.assertEqual(raised.exception.category, 'REMOTE_RETRY_EXHAUSTED')
        self.assertEqual(len(server.commands), publisher.RETRY_LIMIT)
        self.assertEqual([call.args[0] for call in sleep.call_args_list], [30, 60, 90, 120])
        self.assertEqual(server.objects, {})

    def test_pre_fix_parallel_missing_parent_creation_is_rejected_as_ambiguous(self):
        server = MissingParentDriveServer(race_path_copies=True)

        class UnserializedRclone(publisher.Rclone):
            # This is the prior route-then-copy behavior, retained here as a
            # negative comparison for the non-atomic missing-directory fake.
            def copyto(self, source, remote_path, *, immutable):
                routed, folder_id = self._route(remote_path)
                return self._copyto_routed(source, routed, folder_id, immutable=immutable)

        result = self._publish_missing_parent(
            server, client_class=UnserializedRclone, workers=2, item_count=2,
        )

        self.assertEqual(result['category'], 'REMOTE_DIRECTORY_AMBIGUOUS')
        # Per-object byte receipts may be recorded before the final directory
        # binding check; the ambiguity must still prevent publication success.
        self.assertEqual(result['verified'], 2)
        self.assertEqual(set(result['progress']['receipts']),
                         {'part-0.bin', 'part-1.bin'})
        self.assertEqual(len(server.path_copies), 2)
        self.assertEqual(len(server.folder_id_copies), 0)
        self.assertEqual(sum(path == 'database/generation'
                             for path, _identity in server.directories), 2)
        self.assertEqual(server.objects, result['expected_objects'])
        self.assertTrue(result['all_sources_retained'])

    def test_parallel_missing_parent_creation_is_serialized_and_fully_verified(self):
        server = MissingParentDriveServer()
        result = self._publish_missing_parent(server, workers=4, item_count=4)

        self.assertIsNone(result['category'])
        self.assertEqual(result['verified'], 4)
        self.assertEqual(len(server.path_copies), 1)
        self.assertEqual(len(server.folder_id_copies), 3)
        self.assertEqual(sum(path == 'database/generation'
                             for path, _identity in server.directories), 1)
        self.assertEqual(server.objects, result['expected_objects'])
        self.assertEqual(set(result['progress']['receipts']),
                         {f'part-{index}.bin' for index in range(4)})
        for item in result['progress']['receipts'].values():
            self.assertTrue(item['verified'])
        self.assertTrue(result['all_sources_retained'])

    def test_missing_parent_still_missing_after_path_copy_is_not_receipted(self):
        server = MissingParentDriveServer(create_parent=False)
        result = self._publish_missing_parent(server, workers=1, item_count=1)

        self.assertEqual(result['category'], 'REMOTE_DIRECTORY_MISSING')
        self.assertEqual(result['progress']['receipts'], {})
        self.assertEqual(len(server.path_copies), 1)
        self.assertEqual(server.objects, result['expected_objects'])
        self.assertTrue(result['all_sources_retained'])

    def test_duplicate_parent_discovered_after_path_copy_remains_ambiguous(self):
        server = MissingParentDriveServer(duplicate_parent_after_copy=True)
        result = self._publish_missing_parent(server, workers=1, item_count=1)

        self.assertEqual(result['category'], 'REMOTE_DIRECTORY_AMBIGUOUS')
        self.assertEqual(result['progress']['receipts'], {})
        self.assertEqual(len(server.path_copies), 1)
        self.assertEqual(sum(path == 'database/generation'
                             for path, _identity in server.directories), 2)
        self.assertEqual(server.objects, result['expected_objects'])
        self.assertTrue(result['all_sources_retained'])

    def test_cache_disabled_copy_keeps_full_path_flow(self):
        server = MissingParentDriveServer()
        result = self._publish_missing_parent(
            server, cache_enabled=False, workers=1, item_count=1,
        )

        self.assertIsNone(result['category'])
        self.assertEqual(result['verified'], 1)
        self.assertEqual(server.path_copies, ['database/generation/part-0.bin'])
        self.assertEqual(server.folder_id_copies, [])
        self.assertEqual(server.objects, result['expected_objects'])
        self.assertFalse(any('--dirs-only' in command for command in server.commands))

    def test_cached_remote_root_file_does_not_require_a_parent_folder_id(self):
        server = MissingParentDriveServer()
        raw = b'root-level exact payload'
        digest = hashlib.sha256(raw).hexdigest()
        with tempfile.TemporaryDirectory() as temporary, \
                patch.object(publisher, 'run_stat_process', side_effect=server.stat), \
                patch.object(publisher, 'stream_remote_hash', side_effect=server.cat), \
                patch.object(publisher.subprocess, 'Popen', side_effect=server.copy):
            source = Path(temporary) / 'source.bin'
            source.write_bytes(raw)
            client = publisher.Rclone(drive_folder_cache=True)
            client.copyto(source, 'drive:root-level.bin', immutable=True)
            self.assertEqual(client.stat('drive:root-level.bin')['Size'], len(raw))
            self.assertEqual(client.readback('drive:root-level.bin'), (len(raw), digest))

        self.assertEqual(server.path_copies, ['root-level.bin'])
        self.assertEqual(server.folder_id_copies, [])
        self.assertFalse(any('--dirs-only' in command for command in server.commands))
        self.assertEqual(server.objects, {'root-level.bin': raw})


if __name__ == '__main__':
    unittest.main()
