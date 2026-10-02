#!/usr/bin/env python3
"""Unit tests for NAS backup cycle coordinator (scripts/nas_backup_cycle.py).

Tests safety invariants and edge cases using temporary directories and subprocess mocks:
- Successful full backup cycle, roundtrip verification, and secret-free cloud receipt
- GDrive remote listing failure aborts before upload
- Rejection of existing destination filenames on remote
- Manifest integrity validation and tampering rejection
- Encrypted file hash mismatch rejection
- Roundtrip SHA256 mismatch triggers cleanup of only the uploading temporary object
- Non-blocking flock concurrency rejection
"""

import fcntl
from contextlib import closing
import hashlib
import io
import json
import os
import pathlib
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from scripts import nas_backup_cycle


class TestNasBackupCycle(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.temp_dir.name)

        self.db_path = self.root / "archive.sqlite3"
        self.db_path.write_bytes(b"SQLITE3_DUMMY_DATABASE_CONTENT")

        self.passphrase_file = self.root / "passphrase.txt"
        self.passphrase_file.write_text("SUPER_SECRET_GPG_PASSPHRASE\n", encoding="utf-8")

        self.backup_dir = self.root / "backups"
        self.backup_dir.mkdir(parents=True, exist_ok=True)

        self.remote_dir = "gdrive_origin:ikaring-backups"

        # Prepare synthetic backup artifacts
        self.raw_path = self.backup_dir / "archive_20260925_120000_uuid123.sqlite3"
        self.raw_data = b"SQLite format 3\x00" + b"SYNTHETIC_RAW_SQLITE_CONTENT"
        self.raw_path.write_bytes(self.raw_data)
        self.raw_sha256 = hashlib.sha256(self.raw_data).hexdigest()
        self.raw_md5 = hashlib.md5(self.raw_data).hexdigest()

        self.manifest_path = self.backup_dir / "archive_20260925_120000_uuid123.sqlite3.manifest.json"
        self.manifest_dict = {
            "timestamp": "2026-09-25T12:00:00Z",
            "storage": "plaintext",
            "encryption": None,
            "raw_snapshot": {
                "basename": self.raw_path.name,
                "bytes": len(self.raw_data),
                "sha256": self.raw_sha256,
                "quick_check": "ok",
            },
            "verification": {
                "sha256_match": True,
                "quick_check": "ok",
                "plaintext": True,
            },
        }
        self.manifest_path.write_text(json.dumps(self.manifest_dict, indent=2), encoding="utf-8")
        self.manifest_bytes = self.manifest_path.stat().st_size
        self.manifest_md5 = hashlib.md5(self.manifest_path.read_bytes()).hexdigest()

    def tearDown(self):
        self.temp_dir.cleanup()

    def _default_subprocess_run(self, cmd, *args, **kwargs):
        """Mock dispatcher for subprocess.run handling backup script and rclone commands."""
        cmd_str = [str(c) for c in cmd]
        first = cmd_str[0]

        if "plaintext-snapshot" in cmd_str:
            payload = {
                "status": "ok",
                "raw_path": str(self.raw_path),
                "manifest_path": str(self.manifest_path),
            }
            return type("Result", (), {"returncode": 0, "stdout": json.dumps(payload) + "\n", "stderr": ""})()

        # rclone commands
        if first == "rclone":
            subcmd = cmd_str[1]
            if subcmd == "lsf":
                return type("Result", (), {"returncode": 0, "stdout": "existing_old_backup.sqlite3\n", "stderr": ""})()
            elif subcmd == "copyto":
                return type("Result", (), {"returncode": 0, "stdout": "", "stderr": ""})()
            elif subcmd == "size":
                target = cmd_str[3]
                if self.manifest_path.name in target:
                    return type("Result", (), {"returncode": 0, "stdout": json.dumps({"count": 1, "bytes": self.manifest_bytes}), "stderr": ""})()
                if self.raw_path.name in target:
                    return type("Result", (), {"returncode": 0, "stdout": json.dumps({"count": 1, "bytes": len(self.raw_data)}), "stderr": ""})()
                return type("Result", (), {"returncode": 0, "stdout": json.dumps({"count": 1, "bytes": 0}), "stderr": ""})()
            elif subcmd == "md5sum":
                target = cmd_str[2]
                if self.manifest_path.name in target:
                    return type("Result", (), {"returncode": 0, "stdout": f"{self.manifest_md5}  {target}\n", "stderr": ""})()
                if self.raw_path.name in target:
                    return type("Result", (), {"returncode": 0, "stdout": f"{self.raw_md5}  {target}\n", "stderr": ""})()
                return type("Result", (), {"returncode": 0, "stdout": f"dummy_md5  {target}\n", "stderr": ""})()
            elif subcmd == "moveto":
                return type("Result", (), {"returncode": 0, "stdout": "", "stderr": ""})()
            elif subcmd == "deletefile":
                return type("Result", (), {"returncode": 0, "stdout": "", "stderr": ""})()

        raise NotImplementedError(f"Unhandled mock command: {cmd_str}")

    def _default_subprocess_popen(self, cmd, *args, **kwargs):
        """Mock dispatcher for subprocess.Popen handling rclone cat."""
        cmd_str = [str(c) for c in cmd]
        if cmd_str[:2] == ["rclone", "cat"]:
            mock_proc = MagicMock()
            payload = (self.manifest_path.read_bytes()
                       if self.manifest_path.name in cmd_str[2] else self.raw_data)
            mock_proc.stdout = io.BytesIO(payload)
            mock_proc.wait.return_value = 0
            return mock_proc
        raise NotImplementedError(f"Unhandled mock popen command: {cmd_str}")

    def test_manifest_readback_corruption_prevents_finalization_and_receipt(self):
        commands = []
        cats = []

        def run(command, *args, **kwargs):
            commands.append([str(item) for item in command])
            return self._default_subprocess_run(command, *args, **kwargs)

        def popen(command, *args, **kwargs):
            cats.append([str(item) for item in command])
            result = self._default_subprocess_popen(command, *args, **kwargs)
            if self.manifest_path.name in command[2]:
                raw = self.manifest_path.read_bytes()
                result.stdout = io.BytesIO(bytes([raw[0] ^ 1]) + raw[1:])
            return result

        original = {path: path.read_bytes() for path in (self.raw_path, self.manifest_path)}
        with patch.object(nas_backup_cycle.subprocess, "run", side_effect=run), \
             patch.object(nas_backup_cycle.subprocess, "Popen", side_effect=popen):
            result = nas_backup_cycle.main([
                "--db", str(self.db_path), "--backup-dir", str(self.backup_dir),
                "--remote", self.remote_dir,
            ])
        self.assertNotEqual(result, 0)
        self.assertEqual(len(cats), 2)
        finalized = [item for item in commands if item[:2] == ["rclone", "moveto"]]
        self.assertEqual(len(finalized), 1)
        self.assertIn(self.raw_path.name, finalized[0][-1])
        self.assertFalse((self.backup_dir / "cloud-receipts").exists())
        for path, raw in original.items():
            self.assertEqual(path.read_bytes(), raw)

    def test_backup_cycle_success(self):
        """Standard success path: full backup, upload, verification and receipt creation."""
        executed_cmds = []

        def spy_run(cmd, *args, **kwargs):
            executed_cmds.append([str(c) for c in cmd])
            return self._default_subprocess_run(cmd, *args, **kwargs)

        with patch.object(nas_backup_cycle.subprocess, "run", side_effect=spy_run), \
             patch.object(nas_backup_cycle.subprocess, "Popen", side_effect=self._default_subprocess_popen):

            ret = nas_backup_cycle.main([
                "--db", str(self.db_path),
                "--backup-dir", str(self.backup_dir),
                "--passphrase-file", str(self.passphrase_file),
                "--remote", self.remote_dir,
            ])
            self.assertEqual(ret, 0)

        # Check receipt existence and content
        receipts_dir = self.backup_dir / "cloud-receipts"
        receipt_file = receipts_dir / self.manifest_path.name
        self.assertTrue(receipt_file.is_file())

        receipt_data = json.loads(receipt_file.read_text(encoding="utf-8"))
        self.assertEqual(receipt_data["status"], "ok")
        self.assertEqual(receipt_data["plaintext_snapshot"]["bytes"], len(self.raw_data))
        self.assertEqual(receipt_data["plaintext_snapshot"]["sha256"], self.raw_sha256)
        self.assertEqual(receipt_data["plaintext_snapshot"]["md5"], self.raw_md5)
        self.assertEqual(receipt_data["plaintext_snapshot"]["remote"], f"{self.remote_dir}/{self.raw_path.name}")
        self.assertNotIn(".gpg", receipt_data["plaintext_snapshot"]["remote"])
        self.assertEqual(receipt_data["manifest"]["remote"], f"{self.remote_dir}/{self.manifest_path.name}")
        self.assertTrue(receipt_data["verification"]["roundtrip_sha256_verified"])

        # Invariant: No secrets or passphrase in receipt or arguments
        receipt_text = receipt_file.read_text(encoding="utf-8")
        self.assertNotIn("SUPER_SECRET_GPG_PASSPHRASE", receipt_text)
        self.assertNotIn("account", receipt_text.lower())

        # Verify command sequence
        copy_cmds = [cmd for cmd in executed_cmds if cmd[:2] == ["rclone", "copyto"]]
        moveto_cmds = [cmd for cmd in executed_cmds if cmd[:2] == ["rclone", "moveto"]]
        self.assertEqual(len(copy_cmds), 2)
        self.assertEqual(len(moveto_cmds), 2)
        self.assertIn("--immutable", copy_cmds[0])
        self.assertIn("--immutable", moveto_cmds[0])
        uploaded = " ".join(" ".join(cmd) for cmd in copy_cmds + moveto_cmds)
        self.assertIn(self.raw_path.name, uploaded)
        self.assertNotIn(".gpg", uploaded)
        self.assertNotIn(".zst", uploaded)

    def test_listing_failure_aborts(self):
        """If rclone lsf fails, the cycle must abort immediately without attempting uploads."""
        def failed_lsf(cmd, *args, **kwargs):
            if cmd[:2] == ["rclone", "lsf"]:
                return type("Result", (), {"returncode": 1, "stdout": "", "stderr": "network error"})()
            return self._default_subprocess_run(cmd, *args, **kwargs)

        with patch.object(nas_backup_cycle.subprocess, "run", side_effect=failed_lsf), \
             patch.object(nas_backup_cycle.subprocess, "Popen", side_effect=self._default_subprocess_popen):

            ret = nas_backup_cycle.main([
                "--db", str(self.db_path),
                "--backup-dir", str(self.backup_dir),
                "--passphrase-file", str(self.passphrase_file),
                "--remote", self.remote_dir,
            ])
            self.assertEqual(ret, 1)

        # Ensure no cloud receipt was created
        receipt_file = self.backup_dir / "cloud-receipts" / self.manifest_path.name
        self.assertFalse(receipt_file.exists())

    def test_duplicate_remote_name_rejected(self):
        """If remote listing already contains the target filename, abort immediately."""
        def duplicate_lsf(cmd, *args, **kwargs):
            if cmd[:2] == ["rclone", "lsf"]:
                return type("Result", (), {"returncode": 0, "stdout": f"{self.raw_path.name}\n", "stderr": ""})()
            return self._default_subprocess_run(cmd, *args, **kwargs)

        with patch.object(nas_backup_cycle.subprocess, "run", side_effect=duplicate_lsf), \
             patch.object(nas_backup_cycle.subprocess, "Popen", side_effect=self._default_subprocess_popen):

            ret = nas_backup_cycle.main([
                "--db", str(self.db_path),
                "--backup-dir", str(self.backup_dir),
                "--passphrase-file", str(self.passphrase_file),
                "--remote", self.remote_dir,
            ])
            self.assertEqual(ret, 1)

    def test_manifest_tampering_rejected(self):
        """If manifest verification fields or checks are invalid, abort before uploading."""
        invalid_manifests = [
            # sha256_match not True
            {**self.manifest_dict, "verification": {"sha256_match": False, "quick_check": "ok", "plaintext": True}},
            # verification quick_check not ok
            {**self.manifest_dict, "verification": {"sha256_match": True, "quick_check": "corrupt", "plaintext": True}},
            # raw_snapshot quick_check not ok
            {**self.manifest_dict, "raw_snapshot": {**self.manifest_dict["raw_snapshot"], "quick_check": "corrupt"}},
            {**self.manifest_dict, "raw_snapshot": {**self.manifest_dict["raw_snapshot"], "basename": "wrong.sqlite3"}},
            {**self.manifest_dict, "storage": "ciphertext", "encryption": "AES256"},
        ]

        for bad_manifest in invalid_manifests:
            with self.subTest(bad_manifest=bad_manifest):
                self.manifest_path.write_text(json.dumps(bad_manifest), encoding="utf-8")
                with patch.object(nas_backup_cycle.subprocess, "run", side_effect=self._default_subprocess_run), \
                     patch.object(nas_backup_cycle.subprocess, "Popen", side_effect=self._default_subprocess_popen):

                    ret = nas_backup_cycle.main([
                        "--db", str(self.db_path),
                        "--backup-dir", str(self.backup_dir),
                        "--passphrase-file", str(self.passphrase_file),
                        "--remote", self.remote_dir,
                    ])
                    self.assertEqual(ret, 1)

    def test_plaintext_hash_mismatch_with_manifest_rejected(self):
        """If the local plaintext file does not match the manifest SHA256, abort before upload."""
        bad_manifest = {
            **self.manifest_dict,
            "raw_snapshot": {
                **self.manifest_dict["raw_snapshot"],
                "sha256": "0000000000000000000000000000000000000000000000000000000000000000",
            },
        }
        self.manifest_path.write_text(json.dumps(bad_manifest), encoding="utf-8")

        with patch.object(nas_backup_cycle.subprocess, "run", side_effect=self._default_subprocess_run), \
             patch.object(nas_backup_cycle.subprocess, "Popen", side_effect=self._default_subprocess_popen):

            ret = nas_backup_cycle.main([
                "--db", str(self.db_path),
                "--backup-dir", str(self.backup_dir),
                "--passphrase-file", str(self.passphrase_file),
                "--remote", self.remote_dir,
            ])
            self.assertEqual(ret, 1)

    def test_roundtrip_sha256_mismatch_cleans_only_uploading(self):
        """If rclone cat stream does not match source SHA256, delete uploading temp and preserve local raw/enc."""
        deleted_files = []

        def tracking_run(cmd, *args, **kwargs):
            if cmd[:2] == ["rclone", "deletefile"]:
                deleted_files.append(str(cmd[2]))
            return self._default_subprocess_run(cmd, *args, **kwargs)

        def corrupted_cat_popen(cmd, *args, **kwargs):
            cmd_str = [str(c) for c in cmd]
            if cmd_str[:2] == ["rclone", "cat"]:
                mock_proc = MagicMock()
                # Return corrupted content during stream
                mock_proc.stdout = io.BytesIO(b"CORRUPTED_STREAM_DATA")
                mock_proc.wait.return_value = 0
                return mock_proc
            return self._default_subprocess_popen(cmd, *args, **kwargs)

        with patch.object(nas_backup_cycle.subprocess, "run", side_effect=tracking_run), \
             patch.object(nas_backup_cycle.subprocess, "Popen", side_effect=corrupted_cat_popen):

            ret = nas_backup_cycle.main([
                "--db", str(self.db_path),
                "--backup-dir", str(self.backup_dir),
                "--passphrase-file", str(self.passphrase_file),
                "--remote", self.remote_dir,
            ])
            self.assertEqual(ret, 1)

        # Invariant: uploading temporary was cleaned up
        self.assertEqual(len(deleted_files), 1)
        self.assertTrue(".uploading-" in deleted_files[0])

        self.assertTrue(self.raw_path.is_file())
        self.assertTrue(self.manifest_path.is_file())

    def test_flock_concurrency_rejection(self):
        """If another instance holds the non-blocking flock, duplicate invocation is rejected."""
        lock_file = self.backup_dir / ".nas_backup_cycle.lock"
        lock_fd = os.open(str(lock_file), os.O_CREAT | os.O_RDWR, 0o600)
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)

        try:
            with patch.object(nas_backup_cycle.subprocess, "run", side_effect=self._default_subprocess_run), \
                 patch.object(nas_backup_cycle.subprocess, "Popen", side_effect=self._default_subprocess_popen):

                ret = nas_backup_cycle.main([
                    "--db", str(self.db_path),
                    "--backup-dir", str(self.backup_dir),
                    "--passphrase-file", str(self.passphrase_file),
                    "--remote", self.remote_dir,
                ])
                self.assertEqual(ret, 1)
        finally:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
            os.close(lock_fd)

    def test_missing_db_fails_and_missing_passphrase_is_ignored(self):
        """A missing database fails. A missing passphrase file does not, and nothing is encrypted."""
        nonexistent = self.root / "missing.file"
        ret_db = nas_backup_cycle.main([
            "--db", str(nonexistent),
            "--backup-dir", str(self.backup_dir),
            "--passphrase-file", str(self.passphrase_file),
            "--remote", self.remote_dir,
        ])
        self.assertEqual(ret_db, 1)

        with patch.object(nas_backup_cycle.subprocess, "run", side_effect=self._default_subprocess_run), \
             patch.object(nas_backup_cycle.subprocess, "Popen", side_effect=self._default_subprocess_popen):
            ret_pass = nas_backup_cycle.main([
                "--db", str(self.db_path),
                "--backup-dir", str(self.backup_dir),
                "--passphrase-file", str(nonexistent),
                "--remote", self.remote_dir,
            ])
        self.assertEqual(ret_pass, 0)

    def test_plaintext_snapshot_allows_wal_writer_and_keeps_original_rows(self):
        import sqlite3
        from scripts import verified_backup_support

        source = self.root / "concurrent-source.sqlite3"
        writer = sqlite3.connect(source, timeout=0.05)
        writer.execute("PRAGMA journal_mode=WAL")
        writer.execute("CREATE TABLE rows(id INTEGER PRIMARY KEY, body BLOB)")
        writer.executemany("INSERT INTO rows VALUES(?,?)", [(i, b"a" * 4096) for i in range(1, 101)])
        writer.commit()
        original_connect = sqlite3.connect
        committed = []

        class ObservedConnection(sqlite3.Connection):
            def backup(self, target, **kwargs):
                existing_progress = kwargs.get("progress")

                def progress(status, remaining, total):
                    if not committed:
                        writer.execute("INSERT INTO rows VALUES(101,?)", (b"new",))
                        writer.execute("UPDATE rows SET body=? WHERE id=1", (b"changed",))
                        writer.commit()
                        committed.append(True)
                    if existing_progress:
                        existing_progress(status, remaining, total)

                return super().backup(target, pages=1, progress=progress, sleep=0)

        def connect(*args, **kwargs):
            return original_connect(*args, factory=ObservedConnection, **kwargs)

        out = self.root / "concurrent-snapshots"
        try:
            with patch.object(verified_backup_support.sqlite3, "connect", side_effect=connect):
                rc = verified_backup_support.cmd_plaintext_snapshot(
                    type("A", (), {"db_path": str(source), "backup_dir": str(out)})()
                )
            self.assertEqual(rc, 0)
            self.assertEqual(committed, [True])
            snapshot = next(out.glob("archive_*.sqlite3"))
            with closing(original_connect(snapshot)) as copied, copied:
                self.assertEqual(copied.execute("SELECT count(*) FROM rows").fetchone()[0], 100)
                self.assertEqual(copied.execute("SELECT body FROM rows WHERE id=1").fetchone()[0], b"a" * 4096)
            self.assertEqual(writer.execute("SELECT count(*) FROM rows").fetchone()[0], 101)
            self.assertEqual(writer.execute("SELECT body FROM rows WHERE id=1").fetchone()[0], b"changed")
        finally:
            writer.close()

    def test_plaintext_snapshot_matches_source_bytes(self):
        """A real snapshot is a complete plaintext SQLite file with the same row, not a ciphertext."""
        import sqlite3
        from scripts import verified_backup_support

        source = self.root / "source.sqlite3"
        conn = sqlite3.connect(source)
        conn.execute("create table rows(id integer primary key, body text)")
        conn.execute("insert into rows(body) values (?)", ("全数",))
        conn.commit()
        conn.close()
        out = self.root / "snap"
        out.mkdir()
        rc = verified_backup_support.cmd_plaintext_snapshot(
            type("A", (), {"db_path": str(source), "backup_dir": str(out)})()
        )
        self.assertEqual(rc, 0)
        snaps = list(out.glob("archive_*.sqlite3"))
        self.assertEqual(len(snaps), 1)
        self.assertFalse(snaps[0].name.endswith(".gpg"))
        copied = sqlite3.connect(f"file:{snaps[0]}?mode=ro", uri=True)
        self.assertEqual(copied.execute("select body from rows").fetchone()[0], "全数")
        copied.close()
        manifest = json.loads(snaps[0].with_name(snaps[0].name + ".manifest.json").read_text())
        self.assertIsNone(manifest["encryption"])
        self.assertEqual(manifest["raw_snapshot"]["bytes"], snaps[0].stat().st_size)
        self.assertEqual(manifest["raw_snapshot"]["sha256"], hashlib.sha256(snaps[0].read_bytes()).hexdigest())


if __name__ == "__main__":
    unittest.main()
