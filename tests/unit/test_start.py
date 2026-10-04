import io
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, call, patch

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import nas_archive, start

MARKER = {
    'schema_version': 1,
    'backend': 'nas',
    'ssh_host': 'nas',
    'container': 'ikaring-archive',
    'database': '/data/database/archive.sqlite3',
}


class StartScriptTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.env = patch.dict(os.environ, {'IKARING_ARCHIVE_DATA_DIR': str(self.root)})
        self.env.start()

    def tearDown(self):
        self.env.stop()
        self.tmp.cleanup()

    def create_marker(self, content=None):
        config_dir = self.root / 'config'
        config_dir.mkdir(parents=True, exist_ok=True)
        marker_file = config_dir / 'storage-location.json'
        if content is None:
            marker_file.write_text(json.dumps(MARKER), encoding='utf-8')
        else:
            marker_file.write_text(content, encoding='utf-8')
        return marker_file

    def test_nas_normal_start_runs_gui_and_propagates_exit_code(self):
        self.create_marker()
        calls = []

        def fake_run(cmd, *args, **kwargs):
            calls.append(cmd)
            mock_res = MagicMock()
            mock_res.returncode = 42
            return mock_res

        with patch.object(start.subprocess, 'run', side_effect=fake_run), \
             patch.object(start.shutil, 'which') as mock_which:
            exit_code = start.main([])
            self.assertEqual(exit_code, 42)
            self.assertEqual(len(calls), 1)
            expected_cmd = [sys.executable, str(ROOT / 'scripts' / 'desktop_app.py')]
            self.assertEqual(calls[0], expected_cmd)
            mock_which.assert_not_called()

    def test_nas_no_login_runs_gui_only(self):
        self.create_marker()
        calls = []

        def fake_run(cmd, *args, **kwargs):
            calls.append(cmd)
            mock_res = MagicMock()
            mock_res.returncode = 0
            return mock_res

        with patch.object(start.subprocess, 'run', side_effect=fake_run), \
             patch.object(start.shutil, 'which') as mock_which:
            exit_code = start.main(['--no-login'])
            self.assertEqual(exit_code, 0)
            self.assertEqual(len(calls), 1)
            expected_cmd = [sys.executable, str(ROOT / 'scripts' / 'desktop_app.py')]
            self.assertEqual(calls[0], expected_cmd)
            mock_which.assert_not_called()

    def test_nas_static_runs_nas_archive_gui(self):
        self.create_marker()
        calls = []

        def fake_run(cmd, *args, **kwargs):
            calls.append(cmd)
            mock_res = MagicMock()
            mock_res.returncode = 0
            return mock_res

        with patch.object(start.subprocess, 'run', side_effect=fake_run), \
             patch.object(start.shutil, 'which') as mock_which:
            exit_code = start.main(['--static'])
            self.assertEqual(exit_code, 0)
            self.assertEqual(len(calls), 1)
            expected_cmd = [sys.executable, str(ROOT / 'scripts/nas_archive.py'), 'gui']
            self.assertEqual(calls[0], expected_cmd)
            mock_which.assert_not_called()

    def test_nas_check_verifies_marker_and_ssh_without_network(self):
        self.create_marker()
        with patch.object(start.shutil, 'which', return_value='/usr/bin/ssh') as mock_which, \
             patch.object(start.subprocess, 'run') as mock_run, \
             patch('sys.stdout', new_callable=io.StringIO) as mock_stdout:
            exit_code = start.main(['--check'])
            self.assertEqual(exit_code, 0)
            mock_which.assert_called_once_with('ssh')
            mock_run.assert_not_called()
            output = mock_stdout.getvalue()
            self.assertIn('NAS起動条件を満たしています。', output)

    def test_nas_check_fails_if_ssh_missing(self):
        self.create_marker()
        with patch.object(start.shutil, 'which', return_value=None), \
             patch.object(start.subprocess, 'run') as mock_run:
            with self.assertRaisesRegex(RuntimeError, 'sshコマンドが見つかりません'):
                start.main(['--check'])
            mock_run.assert_not_called()

    def test_invalid_marker_symlink_rejected_without_local_fallback(self):
        config_dir = self.root / 'config'
        config_dir.mkdir(parents=True, exist_ok=True)
        target = self.root / 'target.json'
        target.write_text(json.dumps(MARKER), encoding='utf-8')
        marker = config_dir / 'storage-location.json'
        marker.symlink_to(target)

        with patch.object(start.shutil, 'which') as mock_which, \
             patch.object(start.subprocess, 'run') as mock_run:
            with self.assertRaisesRegex(ValueError, 'NAS_STORAGE_MARKER_MISSING_OR_INVALID'):
                start.main([])
            mock_which.assert_not_called()
            mock_run.assert_not_called()

    def test_invalid_marker_corrupt_json_rejected_without_local_fallback(self):
        self.create_marker('{corrupted_json')
        with patch.object(start.shutil, 'which') as mock_which, \
             patch.object(start.subprocess, 'run') as mock_run:
            with self.assertRaisesRegex(ValueError, 'NAS_STORAGE_MARKER_MISSING_OR_INVALID'):
                start.main([])
            mock_which.assert_not_called()
            mock_run.assert_not_called()

    def test_invalid_marker_bad_schema_rejected_without_local_fallback(self):
        bad_markers = [
            {**MARKER, 'backend': 'local'},
            {**MARKER, 'schema_version': 2},
            {**MARKER, 'ssh_host': '-invalid_host'},
            {**MARKER, 'container': ''},
            {**MARKER, 'database': '/var/wrong.sqlite3'},
        ]
        for bad in bad_markers:
            with self.subTest(bad=bad):
                self.create_marker(json.dumps(bad))
                with patch.object(start.shutil, 'which') as mock_which, \
                     patch.object(start.subprocess, 'run') as mock_run:
                    with self.assertRaisesRegex(ValueError, 'NAS_STORAGE_MARKER_MISSING_OR_INVALID'):
                        start.main([])
                    mock_which.assert_not_called()
                    mock_run.assert_not_called()

    def test_local_check_succeeds_when_prerequisites_met(self):
        # Marker does not exist
        def fake_which(cmd):
            if cmd in ('node', 'npm'):
                return f'/usr/local/bin/{cmd}'
            return None

        with patch.object(start.shutil, 'which', side_effect=fake_which), \
             patch.object(start.subprocess, 'check_output', return_value='22\n'), \
             patch('sys.stdout', new_callable=io.StringIO) as mock_stdout:
            exit_code = start.main(['--check'])
            self.assertEqual(exit_code, 0)
            self.assertIn('Python/Node.js/npmの起動条件を満たしています。', mock_stdout.getvalue())

    def test_local_check_fails_when_node_or_npm_missing(self):
        with patch.object(start.shutil, 'which', return_value=None):
            with self.assertRaisesRegex(RuntimeError, 'Node.js 22以上をインストールしてください'):
                start.main(['--check'])

    def test_local_check_fails_when_node_version_too_old(self):
        def fake_which(cmd):
            return f'/usr/local/bin/{cmd}'

        with patch.object(start.shutil, 'which', side_effect=fake_which), \
             patch.object(start.subprocess, 'check_output', return_value='20\n'):
            with self.assertRaisesRegex(RuntimeError, 'Node.js 22以上に更新してください'):
                start.main(['--check'])

    def test_local_normal_runs_full_setup(self):
        def fake_which(cmd):
            return f'/usr/local/bin/{cmd}'

        calls = []

        def fake_run(cmd, *args, **kwargs):
            calls.append(cmd)
            mock_res = MagicMock()
            mock_res.returncode = 0
            return mock_res

        with patch.object(start.shutil, 'which', side_effect=fake_which), \
             patch.object(start.subprocess, 'check_output', return_value='22\n'), \
             patch.object(start.subprocess, 'run', side_effect=fake_run), \
             patch.object(start.os, 'chdir'), \
             patch('sys.platform', 'darwin'):
            exit_code = start.main([])
            self.assertEqual(exit_code, 0)

            archive_py = str(ROOT / 'archive.py')
            self.assertIn(['/usr/local/bin/npm', 'ci', '--ignore-scripts', '--no-audit', '--no-fund'], calls)
            self.assertIn([sys.executable, archive_py, 'login'], calls)
            self.assertIn([sys.executable, archive_py, 'refresh-catalog'], calls)
            self.assertIn([sys.executable, archive_py, 'sync'], calls)
            self.assertIn([sys.executable, archive_py, 'install-service'], calls)

    def test_local_no_login_skips_login(self):
        def fake_which(cmd):
            return f'/usr/local/bin/{cmd}'

        calls = []

        def fake_run(cmd, *args, **kwargs):
            calls.append(cmd)
            mock_res = MagicMock()
            mock_res.returncode = 0
            return mock_res

        with patch.object(start.shutil, 'which', side_effect=fake_which), \
             patch.object(start.subprocess, 'check_output', return_value='22\n'), \
             patch.object(start.subprocess, 'run', side_effect=fake_run), \
             patch.object(start.os, 'chdir'), \
             patch('sys.platform', 'darwin'):
            exit_code = start.main(['--no-login'])
            self.assertEqual(exit_code, 0)

            archive_py = str(ROOT / 'archive.py')
            self.assertNotIn([sys.executable, archive_py, 'login'], calls)
            self.assertIn([sys.executable, archive_py, 'refresh-catalog'], calls)
            self.assertIn([sys.executable, archive_py, 'sync'], calls)
            self.assertIn([sys.executable, archive_py, 'install-service'], calls)

    def test_cli_execution_with_clean_subprocesses(self):
        # 1. 正常NAS markerでのCLI実行 (--check)
        self.create_marker()
        env = {**os.environ, 'IKARING_ARCHIVE_DATA_DIR': str(self.root)}
        res = subprocess.run([sys.executable, str(ROOT / 'scripts/start.py'), '--check'],
                             capture_output=True, text=True, env=env)
        self.assertEqual(res.returncode, 0)
        self.assertIn('NAS起動条件を満たしています。', res.stdout)

        # 2. 不正markerでのCLI実行 (exit code 1)
        self.create_marker('{invalid_json')
        res_bad = subprocess.run([sys.executable, str(ROOT / 'scripts/start.py'), '--check'],
                                 capture_output=True, text=True, env=env)
        self.assertEqual(res_bad.returncode, 1)
        self.assertIn('NAS_STORAGE_MARKER_MISSING_OR_INVALID', res_bad.stderr)


if __name__ == '__main__':
    unittest.main()
