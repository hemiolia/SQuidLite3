"""Three-way sync decisions and durable baseline behavior without live services."""

import contextlib
import hashlib
import io
import json
import os
import shlex
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import scripts.sync_baseline as baseline
import scripts.sync_gdrive as gdrive
import scripts.sync_nas as nas


def entry(content, mtime):
    return {'size': len(content), 'mtime': mtime,
            'sha256': hashlib.sha256(content.encode()).hexdigest()}


class SyncBaselineTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.local = self.root / 'local'
        self.remote = self.root / 'remote'
        self.local.mkdir()
        self.remote.mkdir()

    def tearDown(self):
        self.temp.cleanup()

    def test_one_sided_update_is_normal_and_two_sided_edit_is_conflict(self):
        key = 'exports/index.html'
        old, local_new, remote_new = entry('old', 100), entry('local', 80), entry('remote', 200)
        prior = {key: old['sha256']}
        for module in (nas, gdrive):
            with self.subTest(module=module.__name__):
                push, pull = module.plan_sync({key: local_new}, {key: old}, baseline=prior)
                self.assertEqual([action[2] for action in push], [False])
                self.assertEqual(pull, [])
                push, pull = module.plan_sync({key: old}, {key: remote_new}, baseline=prior)
                self.assertEqual(push, [])
                self.assertEqual([action[2] for action in pull], [False])
                push, pull = module.plan_sync({key: local_new}, {key: remote_new}, baseline=prior)
                self.assertEqual(push, [])
                self.assertEqual([action[2] for action in pull], [True])
                # Explicit direction still preserves an independently edited destination.
                push, _ = module.plan_sync({key: old}, {key: remote_new}, mode='push', baseline=prior)
                self.assertTrue(push[0][2])
                _, pull = module.plan_sync({key: local_new}, {key: old}, mode='pull', baseline=prior)
                self.assertTrue(pull[0][2])
                push, _ = module.plan_sync({key: local_new}, {key: old}, mode='push', baseline=prior)
                self.assertFalse(push[0][2])
                # No state file is the former conservative behavior.
                push, pull = module.plan_sync({key: local_new}, {key: old})
                self.assertEqual(len(push) + len(pull), 1)
                self.assertTrue((push + pull)[0][2])

    def test_state_is_excluded_everywhere_and_malformed_state_fails_closed(self):
        for name in ('.sync-state-nas.json', '.sync-state-gdrive.json'):
            (self.local / name).write_text('{broken', encoding='utf-8')
            (self.remote / name).write_text('{broken', encoding='utf-8')
            self.assertTrue(nas.should_ignore(name))
            self.assertTrue(gdrive.should_ignore(name))
            with self.assertRaises(json.JSONDecodeError):
                baseline.load_baseline(self.local, name)
        (self.local / 'exports').mkdir()
        (self.local / 'exports' / 'receipt.json').write_text('{}')
        self.assertNotIn('.sync-state-nas.json', nas.scan_local(self.local))
        self.assertNotIn('.sync-state-gdrive.json', gdrive.scan_dir(self.local))
        self.assertIn('exports/receipt.json', nas.scan_local(self.local))
        self.assertIn('exports/receipt.json', gdrive.scan_dir(self.local))

        real_run = subprocess.run
        def fake_ssh(cmd, *args, **kwargs):
            return real_run([sys.executable, '-'] if cmd[0] == 'ssh' else cmd, *args, **kwargs)
        with patch('subprocess.run', side_effect=fake_ssh):
            remote_files = nas.scan_remote('fake-nas', str(self.remote))
        self.assertNotIn('.sync-state-nas.json', remote_files)
        self.assertNotIn('.sync-state-gdrive.json', remote_files)

        bad = self.local / '.sync-state-nas.json'
        bad.write_text(json.dumps({'schema_version': 1, 'hashes': {'exports/x': 'not-sha256'}}))
        with self.assertRaises(ValueError):
            baseline.load_baseline(self.local, bad.name)

    def test_successful_gdrive_run_advances_baseline_without_conflict_copy(self):
        rel = 'exports/index.html'
        (self.local / 'exports').mkdir()
        (self.remote / 'exports').mkdir()
        local_file = self.local / rel
        remote_file = self.remote / rel
        local_file.write_text('new generated GUI')
        remote_file.write_text('old GUI')
        old_hash = gdrive.compute_sha256(remote_file)
        baseline.save_baseline(self.local, '.sync-state-gdrive.json', {rel: old_hash})
        argv = ['sync_gdrive.py', '--local', str(self.local), '--gdrive', str(self.remote)]
        with patch.object(sys, 'argv', argv), contextlib.redirect_stdout(io.StringIO()):
            gdrive.main()
        self.assertEqual(remote_file.read_text(), 'new generated GUI')
        self.assertFalse((self.remote / '.sync-conflicts').exists())
        self.assertEqual(baseline.load_baseline(self.local, '.sync-state-gdrive.json')[rel],
                         gdrive.compute_sha256(local_file))
        with patch.object(sys, 'argv', argv), contextlib.redirect_stdout(io.StringIO()):
            gdrive.main()
        self.assertFalse((self.remote / '.sync-conflicts').exists())

    def test_failed_transfer_and_dry_run_keep_old_baseline(self):
        rel = 'exports/report.txt'
        (self.local / 'exports').mkdir()
        (self.remote / 'exports').mkdir()
        (self.local / rel).write_text('new')
        (self.remote / rel).write_text('old')
        old_hash = gdrive.compute_sha256(self.remote / rel)
        baseline.save_baseline(self.local, '.sync-state-gdrive.json', {rel: old_hash})
        state_path = self.local / '.sync-state-gdrive.json'
        old_bytes = state_path.read_bytes()
        args = ['sync_gdrive.py', '--local', str(self.local), '--gdrive', str(self.remote)]
        with patch.object(sys, 'argv', args + ['--dry-run']), contextlib.redirect_stdout(io.StringIO()):
            gdrive.main()
        self.assertEqual(state_path.read_bytes(), old_bytes)
        with patch.object(sys, 'argv', args), patch.object(gdrive, 'copy_file', side_effect=OSError('interrupted')):
            with contextlib.redirect_stdout(io.StringIO()), self.assertRaises(OSError):
                gdrive.main()
        self.assertEqual(state_path.read_bytes(), old_bytes)
        self.assertEqual((self.remote / rel).read_text(), 'old')

    def test_normal_update_rejects_destination_changed_after_scan(self):
        src = self.local / 'report.txt'
        dst = self.remote / 'report.txt'
        src.write_text('new')
        dst.write_text('old')
        expected = gdrive.compute_sha256(dst)
        dst.write_text('concurrent edit')
        with self.assertRaises(RuntimeError):
            gdrive.copy_file(src, dst, root_dir=self.remote, rel_path='report.txt',
                             src_root=self.local, expected_dest_sha=expected)
        self.assertEqual(dst.read_text(), 'concurrent edit')
        self.assertFalse((self.remote / '.sync-conflicts').exists())

    def test_nas_generated_commit_runs_for_none_and_baseline_precondition(self):
        rel = 'exports/report.txt'
        (self.local / 'exports').mkdir()
        (self.remote / 'exports').mkdir()
        src = self.local / rel
        dst = self.remote / rel
        src.write_text('new')
        dst.write_text('old')
        old_hash = nas.compute_sha256(dst)

        real_popen, real_run = subprocess.Popen, subprocess.run
        def local_popen(cmd, *args, **kwargs):
            if cmd[0] == 'ssh':
                remote_cmd = shlex.split(cmd[2])
                self.assertEqual(remote_cmd[:2], ['python3', '-c'])
                return real_popen([sys.executable, '-c', remote_cmd[2]], *args, **kwargs)
            return real_popen(cmd, *args, **kwargs)
        def local_run(cmd, *args, **kwargs):
            if cmd[0] == 'ssh':
                return real_run([sys.executable, '-'], *args, **kwargs)
            return real_run(cmd, *args, **kwargs)

        with patch('subprocess.Popen', side_effect=local_popen), patch('subprocess.run', side_effect=local_run):
            nas.push_file(src, rel, 'fake-nas', str(self.remote), local_dir=self.local,
                          expected_dest_sha=old_hash)
            self.assertEqual(dst.read_text(), 'new')
            self.assertFalse((self.remote / '.sync-conflicts').exists())

            dst.write_text('independently edited')
            with self.assertRaises(RuntimeError):
                nas.push_file(src, rel, 'fake-nas', str(self.remote), local_dir=self.local,
                              expected_dest_sha=old_hash)
            self.assertEqual(dst.read_text(), 'independently edited')

            src.write_text('newer')
            nas.push_file(src, rel, 'fake-nas', str(self.remote), local_dir=self.local,
                          is_conflict=True)
            self.assertEqual(dst.read_text(), 'newer')
            preserved = list((self.remote / '.sync-conflicts').glob('*/exports/report.txt'))
            self.assertEqual(len(preserved), 1)
            self.assertEqual(preserved[0].read_text(), 'independently edited')

    def test_interrupted_nas_transfer_does_not_advance_state(self):
        rel = 'exports/report.txt'
        (self.local / 'exports').mkdir()
        (self.remote / 'exports').mkdir()
        (self.local / rel).write_text('new')
        (self.remote / rel).write_text('old')
        old_hash = nas.compute_sha256(self.remote / rel)
        baseline.save_baseline(self.local, '.sync-state-nas.json', {rel: old_hash})
        state_path = self.local / '.sync-state-nas.json'
        old_bytes = state_path.read_bytes()
        argv = ['sync_nas.py', '--local', str(self.local), '--remote-host', 'fake-nas',
                '--remote-dir', str(self.remote)]
        with patch.object(sys, 'argv', argv), patch.object(nas, 'scan_remote',
             side_effect=lambda *_: nas.scan_local(self.remote)), patch.object(nas, 'push_file',
             side_effect=RuntimeError('interrupted')):
            with contextlib.redirect_stdout(io.StringIO()), self.assertRaises(RuntimeError):
                nas.main()
        self.assertEqual(state_path.read_bytes(), old_bytes)
        self.assertEqual((self.remote / rel).read_text(), 'old')

    def test_nas_and_gdrive_share_nonblocking_lock(self):
        gdrive_args = ['sync_gdrive.py', '--local', str(self.local), '--gdrive', str(self.remote)]
        nas_args = ['sync_nas.py', '--local', str(self.local), '--remote-host', 'fake-nas']
        with baseline.sync_lock(self.local):
            with patch.object(sys, 'argv', gdrive_args), patch.object(gdrive, 'scan_dir') as scan:
                with self.assertRaises(RuntimeError):
                    gdrive.main()
                scan.assert_not_called()
            with patch.object(sys, 'argv', nas_args), patch.object(nas, 'scan_remote') as scan:
                with self.assertRaises(RuntimeError):
                    nas.main()
                scan.assert_not_called()
        self.assertTrue(nas.should_ignore('.sync-state.lock'))
        self.assertTrue(gdrive.should_ignore('.sync-state.lock'))


if __name__ == '__main__':
    unittest.main()
