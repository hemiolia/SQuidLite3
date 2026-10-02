"""Drive ID routing must preserve publication bytes and final path bindings."""
import hashlib
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
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


class RcloneDrivePublicationTests(unittest.TestCase):
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


if __name__ == '__main__':
    unittest.main()
