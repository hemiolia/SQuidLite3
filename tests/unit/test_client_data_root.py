import io
import json
import os
import subprocess
import sys
import tarfile
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import archive
from scripts import data_root, install_file_sync_service, nas_archive, run_file_sync, start, sync_gdrive, sync_nas


MARKER = {
    'schema_version': 1, 'backend': 'nas', 'ssh_host': 'nas',
    'container': 'ikaring-archive', 'database': '/data/database/archive.sqlite3',
}


class ClientDataRootTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.home = Path(self.tmp.name)
        self.home_patch = patch.object(Path, 'home', return_value=self.home)
        self.env_patch = patch.dict(os.environ, {}, clear=True)
        self.platform_patch = patch.object(sys, 'platform', 'darwin')
        self.home_patch.start()
        self.env_patch.start()
        self.platform_patch.start()
        self.client = data_root.client_root()
        self.marker = data_root.marker_path(self.client)

    def tearDown(self):
        self.platform_patch.stop()
        self.env_patch.stop()
        self.home_patch.stop()
        self.tmp.cleanup()

    def put_marker(self, content=None):
        self.marker.parent.mkdir(parents=True, exist_ok=True)
        self.marker.write_text(json.dumps(MARKER) if content is None else content)

    def test_resolver_uses_client_only_when_marker_is_present_and_override_wins(self):
        legacy = self.home / 'Documents' / 'イカリング3アーカイブ'
        self.assertEqual(data_root.data_root(), legacy)
        self.put_marker()
        self.assertEqual(data_root.data_root(), self.client)
        self.assertTrue(data_root.client_mode())
        explicit = self.home / 'chosen'
        with patch.dict(os.environ, {'IKARING_ARCHIVE_DATA_DIR': str(explicit)}):
            self.assertEqual(data_root.data_root(), explicit)
            self.assertFalse(data_root.client_mode())
        with patch.dict(os.environ, {'IKARING_ARCHIVE_DATA_DIR': ''}):
            with self.assertRaisesRegex(ValueError, 'must not be empty'):
                data_root.data_root()
        self.assertFalse(legacy.exists())

    def test_symlink_or_malformed_client_marker_never_falls_back_to_local_start(self):
        self.marker.parent.mkdir(parents=True, exist_ok=True)
        target = self.home / 'target.json'
        target.write_text(json.dumps(MARKER))
        self.marker.symlink_to(target)
        self.assertEqual(data_root.data_root(), self.client)
        with patch.object(start.subprocess, 'run') as run:
            with self.assertRaisesRegex(ValueError, 'NAS_STORAGE_MARKER_MISSING_OR_INVALID'):
                start.main([])
            run.assert_not_called()
        self.marker.unlink()
        self.put_marker('{bad')
        with patch.object(start.subprocess, 'run') as run:
            with self.assertRaisesRegex(ValueError, 'NAS_STORAGE_MARKER_MISSING_OR_INVALID'):
                start.main([])
            run.assert_not_called()
        self.assertFalse((self.client / 'database').exists())
        self.assertFalse((self.home / 'Documents' / 'イカリング3アーカイブ').exists())

    def test_gui_download_uses_cache_and_retired_xlsx_preserves_existing_files(self):
        self.put_marker()

        def fake_run(command, **kwargs):
            if 'stdout' in kwargs:
                kwargs['stdout'].write(b'<html>artificial</html>')
            return type('Result', (), {'returncode': 0})()

        with patch.object(nas_archive.subprocess, 'run', side_effect=fake_run):
            self.assertEqual(nas_archive.main(['gui', '--no-open']), 0)
        cached = self.home / 'Library/Caches/ikaring-archive/exports/gui/index.html'
        self.assertEqual(cached.read_bytes(), b'<html>artificial</html>')
        self.assertFalse((self.home / 'Documents' / 'イカリング3アーカイブ').exists())

        cached_xlsx = self.home / 'Library/Caches/ikaring-archive/exports/分析.xlsx'
        client_xlsx = self.client / 'exports/分析.xlsx'
        cached_xlsx.parent.mkdir(parents=True, exist_ok=True)
        client_xlsx.parent.mkdir(parents=True, exist_ok=True)
        cached_xlsx.write_bytes(b'old cache workbook bytes')
        client_xlsx.write_bytes(b'old client export bytes')
        for path in (cached_xlsx, client_xlsx):
            path.chmod(0o640)
        before = {
            path: (path.read_bytes(), path.stat().st_mode & 0o777, path.stat().st_mtime_ns)
            for path in (cached_xlsx, client_xlsx)
        }
        stdout, stderr = io.StringIO(), io.StringIO()
        with patch.object(nas_archive.subprocess, 'run') as run, \
             patch('sys.stdout', stdout), patch('sys.stderr', stderr):
            code = nas_archive.main(['export-xlsx'])
            run.assert_not_called()
        self.assertEqual(code, 4)
        self.assertEqual(stdout.getvalue(), '')
        self.assertIn('LEGACY_ANALYSIS_XLSX_RETIRED', stderr.getvalue())
        after = {
            path: (path.read_bytes(), path.stat().st_mode & 0o777, path.stat().st_mtime_ns)
            for path in (cached_xlsx, client_xlsx)
        }
        self.assertEqual(after, before)

    def test_normal_start_and_archive_default_never_create_documents_root(self):
        self.put_marker()
        with patch.object(start.subprocess, 'run') as run:
            run.return_value.returncode = 0
            self.assertEqual(start.main([]), 0)
            run.assert_called_once_with([sys.executable, str(Path(start.ROOT) / 'scripts/nas_archive.py'), 'gui'])
        with patch.object(archive, 'OUTPUT', self.client), \
             patch.object(archive, 'DEFAULT', self.client / 'database/archive.sqlite3'), \
             patch.object(sys, 'argv', ['archive.py', 'sync']):
            with self.assertRaisesRegex(ValueError, 'NAS_STORAGE_ACTIVE'):
                archive.main()
        self.assertFalse((self.client / 'database').exists())
        self.assertFalse((self.home / 'Documents' / 'イカリング3アーカイブ').exists())

    def test_file_sync_entrypoints_reject_client_mode_before_side_effects(self):
        self.put_marker()
        with patch.object(run_file_sync.subprocess, 'run') as run:
            with self.assertRaisesRegex(RuntimeError, 'NAS_CLIENT_MODE_FILE_SYNC_DISABLED'):
                run_file_sync.run_sync([])
            run.assert_not_called()
        for module in (sync_nas, sync_gdrive):
            with self.subTest(module=module.__name__), patch.object(sys, 'argv', [module.__file__]), \
                 patch.object(module, '_run_sync') as run:
                with self.assertRaisesRegex(ValueError, 'NAS_CLIENT_MODE_FILE_SYNC_DISABLED'):
                    module.main()
                run.assert_not_called()
        self.assertFalse((self.client / '.sync-state.lock').exists())
        self.assertFalse((self.client / '.sync-state-nas.json').exists())
        self.assertFalse((self.client / '.sync-state-gdrive.json').exists())

    def test_installer_install_rejected_before_files_and_print_remains_available(self):
        self.put_marker()
        plist = self.home / 'Library/LaunchAgents/local.ikaring3.file-sync.plist'
        logs = self.home / 'Library/Logs/file-sync'
        with patch('sys.stderr', new_callable=io.StringIO) as err:
            result = install_file_sync_service.main([
                '--install', '--plist-path', str(plist), '--logs-dir', str(logs), '--skip-launchctl',
            ])
        self.assertEqual(result, 1)
        self.assertIn('NAS_CLIENT_MODE_FILE_SYNC_DISABLED', err.getvalue())
        self.assertFalse(plist.exists())
        self.assertFalse(logs.exists())
        with patch('sys.stdout', new_callable=io.StringIO) as out:
            self.assertEqual(install_file_sync_service.main(['--print']), 0)
        self.assertIn('local.ikaring3.file-sync', out.getvalue())

    def test_legacy_nas_marker_does_not_disable_distributed_file_sync(self):
        legacy = data_root.legacy_root()
        legacy_marker = data_root.marker_path(legacy)
        legacy_marker.parent.mkdir(parents=True)
        legacy_marker.write_text(json.dumps(MARKER))
        self.assertFalse(data_root.client_mode(legacy))
        with patch.dict(os.environ, {'IKARING_ARCHIVE_DATA_DIR': str(legacy)}), \
             patch.object(run_file_sync.subprocess, 'run') as run:
            run.return_value.returncode = 0
            self.assertEqual(run_file_sync.run_sync([]), 0)
            self.assertEqual(run.call_count, 2)

    def test_explicit_local_override_is_honored_even_with_client_marker(self):
        self.put_marker()
        alternate = self.home / 'alternate-local'
        with patch.object(run_file_sync.subprocess, 'run') as run:
            run.return_value.returncode = 0
            self.assertEqual(run_file_sync.run_sync(['--local', str(alternate)]), 0)
            self.assertEqual(run.call_count, 2)
            for call in run.call_args_list:
                self.assertEqual(call.args[0][-2:], ['--local', str(alternate)])

    def test_nas_source_transfer_contains_resolver_without_credentials(self):
        repo = Path(__file__).resolve().parents[2]
        fake_bin = self.home / 'fake-bin'
        fake_bin.mkdir()
        fake_ssh = fake_bin / 'ssh'
        fake_ssh.write_text('#!/bin/sh\ncat > "$IKARING_TEST_CAPTURE_TAR"\n')
        fake_ssh.chmod(0o700)
        captured = self.home / 'collector-source.tar'
        env = {**os.environ, 'PATH': str(fake_bin) + os.pathsep + '/usr/bin:/bin',
               'IKARING_TEST_CAPTURE_TAR': str(captured)}
        result = subprocess.run(['bash', str(repo / 'scripts/deploy_nas_container.sh')],
                                cwd=repo, env=env, capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        with tarfile.open(captured) as package:
            members = set(package.getnames())
            self.assertIn('archive.py', members)
            self.assertIn('scripts/data_root.py', members)
            self.assertNotIn('secrets', members)
            self.assertFalse(any(name.startswith(('database/', 'secrets/', 'spool/')) for name in members))
        stage = self.home / 'staged-source'
        stage.mkdir()
        subprocess.run(['tar', '-C', str(stage), '-xf', str(captured)], check=True, env=env)
        preflight = subprocess.run([sys.executable, str(stage / 'archive.py'), '--help'],
                                  cwd=stage, env=env, capture_output=True, text=True)
        self.assertEqual(preflight.returncode, 0, preflight.stderr)


if __name__ == '__main__':
    unittest.main()
