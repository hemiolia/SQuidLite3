#!/usr/bin/env python3
"""
Unit tests for macOS file sync LaunchAgent installer (scripts/install_file_sync_service.py).
Tests plist generation, atomic write, permission 0600, backup preservation,
CLI argument routing, and mocked launchctl command sequence.
"""
import io
import os
from pathlib import Path
import plistlib
import stat
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import scripts.install_file_sync_service as installer
import scripts.run_file_sync as run_file_sync


class TestFileSyncService(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.base_path = Path(self.temp_dir.name).resolve()
        self.repo_dir = self.base_path / "repo"
        self.scripts_dir = self.repo_dir / "scripts"
        self.scripts_dir.mkdir(parents=True, exist_ok=True)
        self.run_file_sync_py = self.scripts_dir / "run_file_sync.py"
        self.run_file_sync_py.write_text("#!/usr/bin/env python3\n", encoding="utf-8")

        self.launch_agents_dir = self.base_path / "Library" / "LaunchAgents"
        self.launch_agents_dir.mkdir(parents=True, exist_ok=True)
        self.plist_path = self.launch_agents_dir / f"{installer.SERVICE_LABEL}.plist"

        self.logs_dir = self.base_path / "Library" / "Logs" / "ikaring-archive-file-sync"

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_generate_plist_payload(self):
        payload = installer.generate_plist_payload(
            repo_root=self.repo_dir,
            logs_dir=self.logs_dir,
            python_bin="/custom/bin/python3",
            interval=300,
        )

        self.assertEqual(payload['Label'], 'local.ikaring3.file-sync')
        self.assertEqual(payload['RunAtLoad'], True)
        self.assertEqual(payload['StartInterval'], 300)
        self.assertEqual(
            payload['ProgramArguments'],
            ['/custom/bin/python3', str((self.repo_dir / 'scripts' / 'run_file_sync.py').resolve())],
        )
        self.assertEqual(payload['WorkingDirectory'], str(self.repo_dir.resolve()))

        env = payload['EnvironmentVariables']
        self.assertEqual(
            env['PATH'],
            '/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin',
        )
        self.assertEqual(env['PYTHON'], '/custom/bin/python3')
        self.assertEqual(env['PYTHONUNBUFFERED'], '1')

        self.assertEqual(
            payload['StandardOutPath'],
            str((self.logs_dir / 'stdout.log').resolve()),
        )
        self.assertEqual(
            payload['StandardErrorPath'],
            str((self.logs_dir / 'stderr.log').resolve()),
        )

        # XML round-trip
        data = plistlib.dumps(payload)
        loaded = plistlib.loads(data)
        self.assertEqual(loaded, payload)

    def test_cli_help_when_no_arguments(self):
        stdout_buf = io.StringIO()
        with patch('sys.stdout', stdout_buf):
            ret = installer.main([])
        self.assertEqual(ret, 0)
        help_text = stdout_buf.getvalue()
        self.assertIn("local.ikaring3.file-sync", help_text)
        self.assertIn("--install", help_text)
        self.assertIn("--print", help_text)

    def test_cli_print_outputs_plist_xml(self):
        stdout_buf = io.StringIO()
        with patch('sys.stdout', stdout_buf):
            ret = installer.main([
                '--print',
                '--repo-dir', str(self.repo_dir),
                '--logs-dir', str(self.logs_dir),
            ])
        self.assertEqual(ret, 0)
        output_xml = stdout_buf.getvalue()
        loaded = plistlib.loads(output_xml.encode('utf-8'))
        self.assertEqual(loaded['Label'], 'local.ikaring3.file-sync')
        self.assertEqual(loaded['StartInterval'], 300)
        self.assertEqual(
            loaded['ProgramArguments'],
            [sys.executable, str((self.repo_dir / 'scripts' / 'run_file_sync.py').resolve())],
        )

    def test_install_rejected_on_non_darwin_platform(self):
        stderr_buf = io.StringIO()
        with patch('sys.platform', 'linux'), patch('sys.stderr', stderr_buf):
            ret = installer.main([
                '--install',
                '--repo-dir', str(self.repo_dir),
                '--plist-path', str(self.plist_path),
                '--logs-dir', str(self.logs_dir),
                '--skip-launchctl',
            ])
        self.assertNotEqual(ret, 0)
        self.assertIn("darwin", stderr_buf.getvalue())
        self.assertFalse(self.plist_path.exists())

    def test_write_plist_creates_new_file_with_0600_mode(self):
        payload = installer.generate_plist_payload(
            repo_root=self.repo_dir,
            logs_dir=self.logs_dir,
        )
        payload_bytes = plistlib.dumps(payload)

        written, backup = installer.write_plist_file(self.plist_path, payload_bytes)
        self.assertTrue(written)
        self.assertIsNone(backup)
        self.assertTrue(self.plist_path.exists())
        self.assertEqual(self.plist_path.read_bytes(), payload_bytes)

        # Check permissions (0600: user rw, group/others none)
        file_mode = stat.S_IMODE(self.plist_path.stat().st_mode)
        self.assertEqual(file_mode, 0o600)

    def test_write_plist_identical_content_skips_write_and_backup(self):
        payload = installer.generate_plist_payload(
            repo_root=self.repo_dir,
            logs_dir=self.logs_dir,
        )
        payload_bytes = plistlib.dumps(payload)

        # First write
        installer.write_plist_file(self.plist_path, payload_bytes)
        first_mtime_ns = self.plist_path.stat().st_mtime_ns

        # Second write with identical content
        written, backup = installer.write_plist_file(self.plist_path, payload_bytes)
        self.assertFalse(written)
        self.assertIsNone(backup)
        self.assertEqual(self.plist_path.stat().st_mtime_ns, first_mtime_ns)

        # Confirm no backup files were created in directory
        other_files = [f for f in self.launch_agents_dir.iterdir() if f != self.plist_path]
        self.assertEqual(len(other_files), 0)

    def test_write_plist_different_content_preserves_old_version(self):
        old_payload = {'Label': 'local.ikaring3.file-sync', 'StartInterval': 600}
        old_bytes = plistlib.dumps(old_payload)
        self.plist_path.write_bytes(old_bytes)
        os.chmod(self.plist_path, 0o600)

        new_payload = installer.generate_plist_payload(
            repo_root=self.repo_dir,
            logs_dir=self.logs_dir,
            interval=300,
        )
        new_bytes = plistlib.dumps(new_payload)

        written, backup = installer.write_plist_file(self.plist_path, new_bytes)
        self.assertTrue(written)
        self.assertIsNotNone(backup)
        self.assertTrue(backup.exists())

        # Check backup content and filename pattern
        self.assertEqual(backup.read_bytes(), old_bytes)
        self.assertEqual(backup.parent, self.plist_path.parent)
        self.assertTrue(backup.name.startswith(f"{self.plist_path.name}."))
        self.assertTrue(backup.name.endswith(".bak"))

        # Check new file content and permission
        self.assertEqual(self.plist_path.read_bytes(), new_bytes)
        self.assertEqual(stat.S_IMODE(self.plist_path.stat().st_mode), 0o600)

    @patch('subprocess.run')
    def test_register_launchctl_mocked_sequence(self, mock_run):
        mock_run.return_value = MagicMock(returncode=0, stdout=b"", stderr=b"")

        installer.register_launchctl(
            label=installer.SERVICE_LABEL,
            plist_path=self.plist_path,
            uid=501,
        )

        self.assertEqual(mock_run.call_count, 3)

        # 1: bootout gui/501/local.ikaring3.file-sync
        call1 = mock_run.call_args_list[0][0][0]
        self.assertEqual(call1, ['launchctl', 'bootout', 'gui/501/local.ikaring3.file-sync'])

        # 2: enable gui/501/local.ikaring3.file-sync
        call2 = mock_run.call_args_list[1][0][0]
        self.assertEqual(call2, ['launchctl', 'enable', 'gui/501/local.ikaring3.file-sync'])

        # 3: bootstrap gui/501 <plist_path>
        call3 = mock_run.call_args_list[2][0][0]
        self.assertEqual(call3, ['launchctl', 'bootstrap', 'gui/501', str(self.plist_path.resolve())])

        # Verify collector is never touched
        for c in mock_run.call_args_list:
            cmd = c[0][0]
            for arg in cmd:
                self.assertNotIn("archive", arg)

    @patch('subprocess.run')
    def test_register_launchctl_bootout_failure_is_tolerated(self, mock_run):
        # bootout returns 1 (no service currently running), enable and bootstrap return 0
        mock_run.side_effect = [
            MagicMock(returncode=1, stdout=b"", stderr=b"Not found"),
            MagicMock(returncode=0, stdout=b"", stderr=b""),
            MagicMock(returncode=0, stdout=b"", stderr=b""),
        ]

        # Should not raise exception
        installer.register_launchctl(
            label=installer.SERVICE_LABEL,
            plist_path=self.plist_path,
            uid=501,
        )
        self.assertEqual(mock_run.call_count, 3)

    @patch('subprocess.run')
    def test_register_launchctl_enable_failure_raises(self, mock_run):
        mock_run.side_effect = [
            MagicMock(returncode=0, stdout=b"", stderr=b""),
            MagicMock(returncode=1, stdout=b"", stderr=b"Permission denied"),
        ]

        with self.assertRaises(RuntimeError) as ctx:
            installer.register_launchctl(
                label=installer.SERVICE_LABEL,
                plist_path=self.plist_path,
                uid=501,
            )
        self.assertIn("enable に失敗しました", str(ctx.exception))
        self.assertEqual(mock_run.call_count, 2)

    @patch('subprocess.run')
    def test_register_launchctl_bootstrap_failure_raises(self, mock_run):
        mock_run.side_effect = [
            MagicMock(returncode=0, stdout=b"", stderr=b""),
            MagicMock(returncode=0, stdout=b"", stderr=b""),
            MagicMock(returncode=1, stdout=b"", stderr=b"Bootstrap error"),
        ]

        with self.assertRaises(RuntimeError) as ctx:
            installer.register_launchctl(
                label=installer.SERVICE_LABEL,
                plist_path=self.plist_path,
                uid=501,
            )
        self.assertIn("bootstrap に失敗しました", str(ctx.exception))
        self.assertEqual(mock_run.call_count, 3)

    @patch('subprocess.run')
    def test_cli_install_flow_mocked(self, mock_run):
        mock_run.return_value = MagicMock(returncode=0, stdout=b"", stderr=b"")

        with patch('sys.platform', 'darwin'):
            ret = installer.main([
                '--install',
                '--repo-dir', str(self.repo_dir),
                '--plist-path', str(self.plist_path),
                '--logs-dir', str(self.logs_dir),
            ])

        self.assertEqual(ret, 0)
        self.assertTrue(self.plist_path.exists())
        self.assertTrue(self.logs_dir.exists())
        self.assertEqual(stat.S_IMODE(self.plist_path.stat().st_mode), 0o600)
        self.assertEqual(mock_run.call_count, 3)

    @patch('subprocess.run')
    def test_cli_install_failure_returns_nonzero(self, mock_run):
        # bootstrap fails
        mock_run.side_effect = [
            MagicMock(returncode=0, stdout=b"", stderr=b""),
            MagicMock(returncode=0, stdout=b"", stderr=b""),
            MagicMock(returncode=1, stdout=b"", stderr=b"bootstrap failure"),
        ]

        stderr_buf = io.StringIO()
        with patch('sys.platform', 'darwin'), patch('sys.stderr', stderr_buf):
            ret = installer.main([
                '--install',
                '--repo-dir', str(self.repo_dir),
                '--plist-path', str(self.plist_path),
                '--logs-dir', str(self.logs_dir),
            ])

        self.assertEqual(ret, 1)
        self.assertIn("インストールに失敗しました", stderr_buf.getvalue())


class TestRunFileSync(unittest.TestCase):

    @patch('subprocess.run')
    def test_run_sync_success_order_and_args(self, mock_run):
        mock_run.return_value = MagicMock(returncode=0)

        args = ['--dry-run', '--verbose']
        ret = run_file_sync.run_sync(args)

        self.assertEqual(ret, 0)
        self.assertEqual(mock_run.call_count, 2)

        # 1st call: sync_nas.py
        first_call = mock_run.call_args_list[0][0][0]
        self.assertEqual(first_call[0], sys.executable)
        self.assertTrue(first_call[1].endswith('sync_nas.py'))
        self.assertEqual(first_call[2:], args)

        # 2nd call: sync_gdrive.py
        second_call = mock_run.call_args_list[1][0][0]
        self.assertEqual(second_call[0], sys.executable)
        self.assertTrue(second_call[1].endswith('sync_gdrive.py'))
        self.assertEqual(second_call[2:], args)

    @patch('subprocess.run')
    def test_run_sync_nas_failure_stops_early(self, mock_run):
        mock_run.side_effect = [
            MagicMock(returncode=42),
        ]

        ret = run_file_sync.run_sync(['--dry-run'])

        self.assertEqual(ret, 42)
        self.assertEqual(mock_run.call_count, 1)
        first_call = mock_run.call_args_list[0][0][0]
        self.assertTrue(first_call[1].endswith('sync_nas.py'))

    @patch('subprocess.run')
    def test_run_sync_gdrive_failure_propagates(self, mock_run):
        mock_run.side_effect = [
            MagicMock(returncode=0),
            MagicMock(returncode=7),
        ]

        ret = run_file_sync.run_sync()

        self.assertEqual(ret, 7)
        self.assertEqual(mock_run.call_count, 2)

    @patch('scripts.run_file_sync.run_sync')
    @patch('sys.argv', ['run_file_sync.py', '--sample-arg'])
    def test_main_calls_run_sync(self, mock_run_sync):
        mock_run_sync.return_value = 0
        ret = run_file_sync.main()
        self.assertEqual(ret, 0)
        mock_run_sync.assert_called_once_with(['--sample-arg'])


if __name__ == '__main__':
    unittest.main()
