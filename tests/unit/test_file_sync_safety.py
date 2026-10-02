#!/usr/bin/env python3
"""
Unit tests for hardened sync scripts (sync_nas.py and sync_gdrive.py).
Tests all safety invariants using only mocks and temporary directories:
- Symlink exclusion (including secrets)
- Special character path safety
- Same mtime & same size but different content detection (SHA256 comparison)
- Preservation of old versions in .sync-conflicts/<uuid>/<relative> on conflict
- Upstream failure prevents replacement (atomic replace guarantee)
- Max size evaluation (large remote vs small local)
- Scan errors are raised, not swallowed
- No deletion propagation
"""

import hashlib
import io
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import scripts.sync_gdrive as sync_gdrive
import scripts.sync_nas as sync_nas

class TestFileSyncSafety(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.base_path = Path(self.temp_dir.name)
        self.local_dir = self.base_path / "local"
        self.remote_dir = self.base_path / "remote"
        self.local_dir.mkdir(parents=True, exist_ok=True)
        self.remote_dir.mkdir(parents=True, exist_ok=True)

    def tearDown(self):
        self.temp_dir.cleanup()

    # 1. symlink秘密
    def test_symlink_secret(self):
        """Symlinks (both file and dir) pointing to secrets must be ignored completely."""
        # Create a secret file and directory
        secret_dir = self.local_dir / "secrets"
        secret_dir.mkdir(parents=True, exist_ok=True)
        secret_file = secret_dir / "token.txt"
        secret_file.write_text("SUPER_SECRET_TOKEN")

        # Create normal files
        normal_dir = self.local_dir / "exports"
        normal_dir.mkdir(parents=True, exist_ok=True)
        normal_file = normal_dir / "report.txt"
        normal_file.write_text("public data")

        # Create symlinks inside normal_dir pointing to the secret file and dir
        symlink_file = normal_dir / "leak_secret.txt"
        symlink_file.symlink_to(secret_file)

        symlink_dir = normal_dir / "leak_secret_dir"
        symlink_dir.symlink_to(secret_dir)

        # Scan local
        gdrive_scanned = sync_gdrive.scan_dir(self.local_dir)
        nas_scanned = sync_nas.scan_local(self.local_dir)

        # Neither the secret itself nor any symlink pointing to it should be scanned
        for scanned in (gdrive_scanned, nas_scanned):
            self.assertIn("exports/report.txt", scanned)
            self.assertNotIn("secrets/token.txt", scanned)
            self.assertNotIn("exports/leak_secret.txt", scanned)
            self.assertNotIn("exports/leak_secret_dir", scanned)
            self.assertNotIn("exports/leak_secret_dir/token.txt", scanned)

        # Verify should_ignore rules
        self.assertTrue(sync_gdrive.should_ignore("secrets/token.txt"))
        self.assertTrue(sync_gdrive.should_ignore("auth/token.json"))
        self.assertTrue(sync_gdrive.should_ignore("runtime/app.pid"))
        self.assertTrue(sync_gdrive.should_ignore("spool/item.bin"))
        self.assertTrue(sync_gdrive.should_ignore(".history/prev.py"))
        self.assertTrue(sync_gdrive.should_ignore(".git/config"))
        self.assertTrue(sync_gdrive.should_ignore("backups/old.sqlite3"))
        self.assertTrue(sync_gdrive.should_ignore(".sync-conflicts/uuid/file.txt"))
        self.assertTrue(sync_gdrive.should_ignore("logs/development/rclone-write.conf"))
        self.assertTrue(sync_gdrive.should_ignore("rclone.conf"))
        self.assertTrue(sync_nas.should_ignore("logs/development/rclone-write.conf"))
        self.assertTrue(sync_nas.should_ignore("rclone.conf"))

    # 2. 特殊文字path
    def test_special_character_paths(self):
        """Paths with spaces, quotes, symbols, semicolons, and UTF-8 characters must sync safely."""
        complex_rel = "exports/sub dir/テスト 'single' \"double\" ; $VAR & file.txt"
        src_file = self.local_dir / complex_rel
        src_file.parent.mkdir(parents=True, exist_ok=True)
        content = "safe content with special chars: 烏賊"
        src_file.write_text(content, encoding="utf-8")

        # Test scan_dir
        scanned = sync_gdrive.scan_dir(self.local_dir)
        self.assertIn(complex_rel, scanned)
        self.assertEqual(scanned[complex_rel]["size"], len(content.encode("utf-8")))

        # Test copy_file
        dst_file = self.remote_dir / complex_rel
        sync_gdrive.copy_file(src_file, dst_file, root_dir=self.remote_dir, rel_path=complex_rel)
        self.assertTrue(dst_file.exists())
        self.assertEqual(dst_file.read_text(encoding="utf-8"), content)

    # 3. 同mtime同size異内容
    def test_same_mtime_same_size_different_content(self):
        """Files with exact same size and mtime but different content (SHA256) must not be ignored."""
        rel = "exports/data.bin"
        file_loc = self.local_dir / rel
        file_loc.parent.mkdir(parents=True, exist_ok=True)
        file_rem = self.remote_dir / rel
        file_rem.parent.mkdir(parents=True, exist_ok=True)

        # Exactly 16 bytes each, but different contents
        content_loc = b"AAAA_1234_5678_X"
        content_rem = b"BBBB_1234_5678_Y"
        self.assertEqual(len(content_loc), len(content_rem))

        file_loc.write_bytes(content_loc)
        file_rem.write_bytes(content_rem)

        # Fix mtime to be exactly identical
        fixed_time = 1700000000
        os.utime(file_loc, (fixed_time, fixed_time))
        os.utime(file_rem, (fixed_time, fixed_time))

        loc_scanned = sync_gdrive.scan_dir(self.local_dir)
        rem_scanned = sync_gdrive.scan_dir(self.remote_dir)

        # Check metadata
        self.assertEqual(loc_scanned[rel]["size"], rem_scanned[rel]["size"])
        self.assertEqual(loc_scanned[rel]["mtime"], rem_scanned[rel]["mtime"])
        self.assertNotEqual(loc_scanned[rel]["sha256"], rem_scanned[rel]["sha256"])

        # Test plan_sync: must not be ignored (must be listed for sync)
        to_push, to_pull = sync_gdrive.plan_sync(loc_scanned, rem_scanned, mode="both")
        total_actions = len(to_push) + len(to_pull)
        self.assertEqual(total_actions, 1, "Differing content must not be ignored even if size and mtime match")

        # Also test sync_nas plan_sync
        to_push_nas, to_pull_nas = sync_nas.plan_sync(loc_scanned, rem_scanned, mode="both")
        self.assertEqual(len(to_push_nas) + len(to_pull_nas), 1)

    # 4. 両側編集時旧版保持
    def test_conflict_preserves_old_version(self):
        """When conflicting files exist on both sides, the destination old version must be preserved under .sync-conflicts/<uuid>/<relative>."""
        rel = "exports/report.txt"
        loc_file = self.local_dir / rel
        gdr_file = self.remote_dir / rel
        loc_file.parent.mkdir(parents=True, exist_ok=True)
        gdr_file.parent.mkdir(parents=True, exist_ok=True)

        old_gdrive_content = "OLD_GDRIVE_CONTENT"
        new_local_content = "NEW_LOCAL_CONTENT"
        gdr_file.write_text(old_gdrive_content)
        loc_file.write_text(new_local_content)

        # Copy local -> gdrive with is_conflict=True
        sync_gdrive.copy_file(
            src=loc_file,
            dst=gdr_file,
            root_dir=self.remote_dir,
            rel_path=rel,
            is_conflict=True
        )

        # Destination must now contain the new local content
        self.assertEqual(gdr_file.read_text(), new_local_content)

        # Old version must be preserved under .sync-conflicts
        conflicts_dir = self.remote_dir / ".sync-conflicts"
        self.assertTrue(conflicts_dir.exists())

        conflict_files = list(conflicts_dir.glob(f"*/{rel}"))
        self.assertEqual(len(conflict_files), 1, "Expected exactly one preserved conflict file")
        self.assertEqual(conflict_files[0].read_text(), old_gdrive_content)

        # Verify .sync-conflicts is ignored by scanners
        scanned = sync_gdrive.scan_dir(self.remote_dir)
        self.assertNotIn(f".sync-conflicts/{conflict_files[0].parent.name}/{rel}", scanned)
        self.assertIn(rel, scanned)

    # 5. 上流失敗時未置換
    def test_upstream_failure_does_not_replace(self):
        """If upstream process or transfer fails, destination file must remain intact."""
        rel = "exports/vital.txt"
        src_file = self.local_dir / rel
        dst_file = self.remote_dir / rel
        src_file.parent.mkdir(parents=True, exist_ok=True)
        dst_file.parent.mkdir(parents=True, exist_ok=True)

        original_content = "DO_NOT_DESTROY"
        new_content = "CORRUPT_OR_FAILING"
        dst_file.write_text(original_content)
        src_file.write_text(new_content)

        # Test sync_gdrive: simulate failure during transfer
        with patch("shutil.copyfileobj", side_effect=IOError("Disk full or transfer interrupted")):
            with self.assertRaises(IOError):
                sync_gdrive.copy_file(
                    src=src_file,
                    dst=dst_file,
                    root_dir=self.remote_dir,
                    rel_path=rel,
                    is_conflict=True
                )

        # Destination must remain untouched!
        self.assertEqual(dst_file.read_text(), original_content)

        # Test sync_nas push_file: simulate upstream tar process failure (returncode != 0)
        mock_p1 = MagicMock()
        mock_p1.communicate.return_value = (b"", b"tar read error")
        mock_p1.wait.return_value = 1  # Upstream failure
        mock_p1.stdout = MagicMock()

        mock_p2 = MagicMock()
        mock_p2.communicate.return_value = (b"", b"")
        mock_p2.returncode = 0

        with patch("subprocess.Popen", side_effect=[mock_p1, mock_p2]):
            with self.assertRaises(RuntimeError) as ctx:
                sync_nas.push_file(
                    local_path=src_file,
                    rel_path=rel,
                    remote_host="mock-nas",
                    remote_dir=str(self.remote_dir),
                    is_conflict=True
                )
            self.assertIn("tar=1", str(ctx.exception))

        # Test sync_nas pull_file: simulate upstream ssh process failure (returncode != 0)
        mock_p1_pull = MagicMock()
        mock_p1_pull.communicate.return_value = (b"", b"ssh connection dropped")
        mock_p1_pull.wait.return_value = 255  # Upstream failure
        mock_p1_pull.stdout = MagicMock()

        mock_p2_pull = MagicMock()
        mock_p2_pull.communicate.return_value = (b"", b"")
        mock_p2_pull.returncode = 0

        with patch("subprocess.Popen", side_effect=[mock_p1_pull, mock_p2_pull]):
            with self.assertRaises(RuntimeError) as ctx:
                sync_nas.pull_file(
                    remote_host="mock-nas",
                    remote_dir=str(self.remote_dir),
                    rel_path=rel,
                    local_path=dst_file,
                    local_dir=self.remote_dir,
                    is_conflict=True
                )
            self.assertIn("ssh=255", str(ctx.exception))

        # Destination still remains completely intact!
        self.assertEqual(dst_file.read_text(), original_content)

    # 6. remote巨大local小のサイズ判定
    def test_remote_large_local_small_size_limit(self):
        """File must be skipped if max(local_size, remote_size) exceeds 1GiB."""
        rel = "database/huge_backup.bin"  # or any file > 1GiB
        limit = 1024 * 1024 * 1024  # 1GiB

        # Case A: remote is >1GiB, local is small (100 bytes)
        local_files_a = {rel: {"size": 100, "mtime": 1000, "sha256": "loc_hash"}}
        remote_files_a = {rel: {"size": limit + 1, "mtime": 2000, "sha256": "rem_hash"}}

        to_push_a, to_pull_a = sync_gdrive.plan_sync(local_files_a, remote_files_a, mode="both")
        self.assertEqual(len(to_push_a), 0)
        self.assertEqual(len(to_pull_a), 0)

        to_push_nas_a, to_pull_nas_a = sync_nas.plan_sync(local_files_a, remote_files_a, mode="both")
        self.assertEqual(len(to_push_nas_a), 0)
        self.assertEqual(len(to_pull_nas_a), 0)

        # Case B: local is >1GiB, remote is small (100 bytes)
        local_files_b = {rel: {"size": limit + 1, "mtime": 2000, "sha256": "loc_hash"}}
        remote_files_b = {rel: {"size": 100, "mtime": 1000, "sha256": "rem_hash"}}

        to_push_b, to_pull_b = sync_gdrive.plan_sync(local_files_b, remote_files_b, mode="both")
        self.assertEqual(len(to_push_b), 0)
        self.assertEqual(len(to_pull_b), 0)

        to_push_nas_b, to_pull_nas_b = sync_nas.plan_sync(local_files_b, remote_files_b, mode="both")
        self.assertEqual(len(to_push_nas_b), 0)
        self.assertEqual(len(to_pull_nas_b), 0)

        # Case C: exactly <= 1GiB should be included if different
        local_files_c = {rel: {"size": limit, "mtime": 1000, "sha256": "loc_hash"}}
        remote_files_c = {rel: {"size": limit, "mtime": 2000, "sha256": "rem_hash"}}

        to_push_c, to_pull_c = sync_gdrive.plan_sync(local_files_c, remote_files_c, mode="both")
        self.assertEqual(len(to_push_c) + len(to_pull_c), 1)

    # 7. scanエラーを黙殺せず失敗を返す
    def test_scan_error_is_raised(self):
        """Errors during scanning (e.g. PermissionError) must not be silently swallowed."""
        test_file = self.local_dir / "unreadable.txt"
        test_file.write_text("data")

        with patch("pathlib.Path.stat", side_effect=PermissionError("Permission denied")):
            with self.assertRaises(PermissionError):
                sync_gdrive.scan_dir(self.local_dir)

            with self.assertRaises(PermissionError):
                sync_nas.scan_local(self.local_dir)

    # 8. 削除伝播なし
    def test_no_deletion_propagation(self):
        """Files present in one side but absent in the other must not trigger deletion."""
        local_files = {"local_only.txt": {"size": 50, "mtime": 1000, "sha256": "loc1"}}
        remote_files = {"remote_only.txt": {"size": 60, "mtime": 2000, "sha256": "rem1"}}

        to_push, to_pull = sync_gdrive.plan_sync(local_files, remote_files, mode="both")

        # Only additions/pulls, no deletions
        self.assertEqual(len(to_push), 1)
        self.assertEqual(to_push[0][0], "local_only.txt")
        self.assertFalse(to_push[0][2])  # is_conflict is False for new files

        self.assertEqual(len(to_pull), 1)
        self.assertEqual(to_pull[0][0], "remote_only.txt")
        self.assertFalse(to_pull[0][2])

    # 9. os.walkレベルPermissionError再raise
    def test_os_walk_level_permission_error_reraised(self):
        """os.walk must reraise PermissionError via onerror handler without swallowing it."""
        def mock_walk_error(top, followlinks=False, onerror=None):
            if onerror:
                onerror(PermissionError(f"Permission denied while walking {top}"))
            return iter([])

        with patch("os.walk", side_effect=mock_walk_error):
            with self.assertRaises(PermissionError):
                sync_gdrive.scan_dir(self.local_dir)

            with self.assertRaises(PermissionError):
                sync_nas.scan_local(self.local_dir)

    # 10. oversized file (1GiB超) のSHA計算が呼ばれずsha256=Noneを返す
    def test_oversized_file_hash_not_called(self):
        """Files exceeding 1GiB must not compute SHA256 during scan and must return sha256=None."""
        oversized_file = self.local_dir / "large_file.iso"
        oversized_file.write_text("dummy")

        oversized_size = sync_gdrive.MAX_FILE_BYTES + 1024

        orig_os_stat = os.stat
        def fake_os_stat(path_obj, *args, **kwargs):
            if Path(path_obj) == oversized_file:
                st = orig_os_stat(path_obj, *args, **kwargs)
                return os.stat_result((st.st_mode, st.st_ino, st.st_dev, st.st_nlink,
                                       st.st_uid, st.st_gid, oversized_size,
                                       st.st_atime, st.st_mtime, st.st_ctime))
            return orig_os_stat(path_obj, *args, **kwargs)

        with patch("os.stat", side_effect=fake_os_stat):
            with patch("scripts.sync_gdrive.compute_sha256") as mock_sha_gdrive:
                result_gdrive = sync_gdrive.scan_dir(self.local_dir)
                mock_sha_gdrive.assert_not_called()
                self.assertIn("large_file.iso", result_gdrive)
                self.assertIsNone(result_gdrive["large_file.iso"]["sha256"])
                self.assertEqual(result_gdrive["large_file.iso"]["size"], oversized_size)

            with patch("scripts.sync_nas.compute_sha256") as mock_sha_nas:
                result_nas = sync_nas.scan_local(self.local_dir)
                mock_sha_nas.assert_not_called()
                self.assertIn("large_file.iso", result_nas)
                self.assertIsNone(result_nas["large_file.iso"]["sha256"])
                self.assertEqual(result_nas["large_file.iso"]["size"], oversized_size)

            # Confirm plan_sync skips oversized files without error even with sha256=None
            to_push, to_pull = sync_gdrive.plan_sync(result_gdrive, {}, mode="both")
            self.assertEqual(len(to_push), 0)
            self.assertEqual(len(to_pull), 0)

            to_push_nas, to_pull_nas = sync_nas.plan_sync(result_nas, {}, mode="both")
            self.assertEqual(len(to_push_nas), 0)
            self.assertEqual(len(to_pull_nas), 0)

    # 11. dst親ディレクトリsymlink escape拒否
    def test_dst_parent_symlink_escape(self):
        """Sync must reject transfer when dst or src parent directory is a symlink or escapes root."""
        outside_dir = self.base_path / "outside_jail"
        outside_dir.mkdir(parents=True, exist_ok=True)
        outside_file = outside_dir / "target.txt"

        bad_dst_parent = self.remote_dir / "sym_parent"
        bad_dst_parent.symlink_to(outside_dir)

        src_file = self.local_dir / "report.txt"
        src_file.write_text("sensitive data")

        # 1. sync_gdrive copy_file with dst parent symlink
        bad_dst_file = bad_dst_parent / "report.txt"
        with self.assertRaises(ValueError) as ctx:
            sync_gdrive.copy_file(
                src=src_file,
                dst=bad_dst_file,
                root_dir=self.remote_dir,
                rel_path="sym_parent/report.txt"
            )
        self.assertIn("Symlink rejected", str(ctx.exception))
        self.assertFalse(outside_file.exists())

        # 2. sync_nas pull_file with local_path parent symlink
        with self.assertRaises(ValueError) as ctx:
            sync_nas.pull_file(
                remote_host="mock-nas",
                remote_dir=str(self.remote_dir),
                rel_path="sym_parent/report.txt",
                local_path=bad_dst_file,
                local_dir=self.remote_dir
            )
        self.assertIn("Symlink rejected", str(ctx.exception))

        # 3. sync_nas push_file with local_path parent symlink (src parent symlink)
        bad_src_parent = self.local_dir / "bad_src"
        bad_src_parent.symlink_to(outside_dir)
        bad_src_file = bad_src_parent / "file.txt"
        bad_src_file.write_text("evil")

        with self.assertRaises(ValueError) as ctx:
            sync_nas.push_file(
                local_path=bad_src_file,
                rel_path="bad_src/file.txt",
                remote_host="mock-nas",
                remote_dir=str(self.remote_dir),
                local_dir=self.local_dir
            )
        self.assertIn("Symlink rejected", str(ctx.exception))

    # 12. 上流tar非zeroだが有効tarを出した場合は既存dst保持
    def test_upstream_tar_nonzero_preserves_existing_dst(self):
        """
        If upstream tar exits with non-zero status even after outputting a valid tar stream,
        the remote staging must NOT commit, and the existing destination file must remain intact.
        """
        rel = "exports/existing_dst.txt"
        src_file = self.local_dir / rel
        dst_file = self.remote_dir / rel
        src_file.parent.mkdir(parents=True, exist_ok=True)
        dst_file.parent.mkdir(parents=True, exist_ok=True)

        original_content = "ORIGINAL_REMOTE_CONTENT_PRESERVED"
        new_content = "NEW_UNTRUSTED_CONTENT"
        dst_file.write_text(original_content)
        src_file.write_text(new_content)

        real_popen = subprocess.Popen

        def popen_hook(cmd, *args, **kwargs):
            if cmd[0] == "ssh":
                # Replace 'ssh', host with local python3 directly to simulate remote host
                local_cmd = ["python3"] + cmd[2:]
                return real_popen(local_cmd, *args, **kwargs)
            elif cmd[0] == "tar":
                proc = real_popen(cmd, *args, **kwargs)
                orig_wait = proc.wait
                orig_communicate = proc.communicate
                def fake_wait(timeout=None):
                    orig_wait(timeout=timeout)
                    proc.returncode = 1
                    return 1
                def fake_communicate(input=None, timeout=None):
                    out, err = orig_communicate(input=input, timeout=timeout)
                    proc.returncode = 1
                    return out, err
                proc.wait = fake_wait
                proc.communicate = fake_communicate
                return proc
            return real_popen(cmd, *args, **kwargs)

        def run_hook(cmd, *args, **kwargs):
            if cmd[0] == "ssh":
                local_cmd = ["python3"] + cmd[2:]
                return subprocess.run(local_cmd, *args, **kwargs)
            return subprocess.run(cmd, *args, **kwargs)

        with patch("subprocess.Popen", side_effect=popen_hook), patch("subprocess.run", side_effect=run_hook):
            with self.assertRaises(RuntimeError) as ctx:
                sync_nas.push_file(
                    local_path=src_file,
                    rel_path=rel,
                    remote_host="mock-nas",
                    remote_dir=str(self.remote_dir),
                    is_conflict=True,
                    local_dir=self.local_dir
                )
            self.assertIn("tar=1", str(ctx.exception))

        # The existing destination file MUST remain untouched!
        self.assertEqual(dst_file.read_text(), original_content)

    # 13. rclone設定除外（大文字小文字非依存・ネスト）、退役XLSX、通常exports対照保持
    def test_rclone_conf_exclusion_and_receipt_inclusion_all_paths(self):
        """
        Verify that:
        - rclone.conf and rclone-*.conf (case-insensitive) are excluded across all paths:
          * GDrive scan (sync_gdrive.scan_dir)
          * NAS local scan (sync_nas.scan_local)
          * NAS remote scan code (sync_nas.scan_remote)
          * should_ignore functions
        - Nested logs/development/rclone-write.conf is excluded across all paths
        - Normal receipt JSON files and full-data generation artifacts remain eligible
        - Only the two exact retired legacy workbook paths are excluded
        - Existing exclusions (database, secrets, auth, tokens, credentials, .history, etc.) remain intact
        """
        setup_dirs = [
            self.local_dir / "logs" / "development",
            self.local_dir / "logs" / "backup",
            self.local_dir / "exports",
            self.local_dir / "secrets",
            self.local_dir / "database",
            self.local_dir / "auth",
        ]
        for d in setup_dirs:
            d.mkdir(parents=True, exist_ok=True)

        exclude_files = [
            "logs/development/rclone-write.conf",
            "logs/development/RCLONE-WRITE.CONF",
            "logs/development/rclone.conf",
            "logs/development/Rclone.Conf",
            "logs/rclone-backup.conf",
            "rclone.conf",
            "rclone-custom.conf",
            "secrets/token.txt",
            "database/archive.sqlite3",
            "auth/token.json",
        ]
        for rel in exclude_files:
            p = self.local_dir / rel
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text("secret_or_config_content")

        include_files = [
            "logs/development/upload-receipt.json",
            "logs/backup/migration-cloud-upload.json",
            "exports/report.txt",
            "exports/gui.html",
            "exports/full-data/generations/test-generation/index.json",
            "exports/full-data/generations/test-generation/manifest.json",
            "exports/full-data/generations/test-generation/xlsx/piece-000001.xlsx",
        ]
        retired_files = ["exports/分析.xlsx", "exports/analysis.xlsx"]
        for rel in include_files:
            p = self.local_dir / rel
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text("public_or_receipt_data")
        for rel in retired_files:
            p = self.local_dir / rel
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_bytes(("legacy-local:" + rel).encode("utf-8"))

        for mod in (sync_gdrive, sync_nas):
            for rel in exclude_files:
                self.assertTrue(mod.should_ignore(rel), f"Expected {rel} to be ignored by {mod.__name__}")
            for rel in retired_files:
                self.assertTrue(mod.should_ignore(rel), f"Expected retired path {rel} to be ignored by {mod.__name__}")
            for rel in include_files:
                self.assertFalse(mod.should_ignore(rel), f"Expected {rel} NOT to be ignored by {mod.__name__}")
            # Do not infer additional retired aliases from similar names or case variants.
            for rel in ("exports/Analysis.xlsx", "exports/分析.XLSX", "exports/other.xlsx"):
                self.assertFalse(mod.should_ignore(rel), f"Unexpected path expansion: {rel}")

        # Path A: GDrive scan
        gdrive_scanned = sync_gdrive.scan_dir(self.local_dir)
        for rel in exclude_files:
            self.assertNotIn(rel, gdrive_scanned, f"Expected {rel} to be excluded from GDrive scan")
        for rel in retired_files:
            self.assertNotIn(rel, gdrive_scanned, f"Expected {rel} to be excluded from GDrive scan")
        for rel in include_files:
            self.assertIn(rel, gdrive_scanned, f"Expected {rel} to be included in GDrive scan")

        # Path B: NAS local scan
        nas_scanned = sync_nas.scan_local(self.local_dir)
        for rel in exclude_files:
            self.assertNotIn(rel, nas_scanned, f"Expected {rel} to be excluded from NAS local scan")
        for rel in retired_files:
            self.assertNotIn(rel, nas_scanned, f"Expected {rel} to be excluded from NAS local scan")
        for rel in include_files:
            self.assertIn(rel, nas_scanned, f"Expected {rel} to be included in NAS local scan")

        # Path C: NAS remote scan generated code execution
        for rel in exclude_files:
            p = self.remote_dir / rel
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text("remote_secret_or_config")
        for rel in retired_files:
            p = self.remote_dir / rel
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_bytes(("legacy-remote:" + rel).encode("utf-8"))
        for rel in include_files:
            p = self.remote_dir / rel
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text("remote_receipt_or_export")

        real_run = subprocess.run
        def fake_ssh_run(cmd, *args, **kwargs):
            if cmd[0] == "ssh":
                local_cmd = ["python3", "-"]
                return real_run(local_cmd, *args, **kwargs)
            return real_run(cmd, *args, **kwargs)

        with patch("subprocess.run", side_effect=fake_ssh_run):
            remote_scanned = sync_nas.scan_remote("mock-nas", str(self.remote_dir))

        for rel in exclude_files:
            self.assertNotIn(rel, remote_scanned, f"Expected {rel} to be excluded from NAS remote scan")
        for rel in retired_files:
            self.assertNotIn(rel, remote_scanned, f"Expected {rel} to be excluded from NAS remote scan")
        for rel in include_files:
            self.assertIn(rel, remote_scanned, f"Expected {rel} to be included in NAS remote scan")

    def test_retired_legacy_workbooks_are_never_planned_or_transferred(self):
        retired = ("exports/分析.xlsx", "exports/analysis.xlsx")
        for module in (sync_nas, sync_gdrive):
            for mode in ("both", "push", "pull"):
                scenarios = (
                    ({retired[0]: {"size": 10, "mtime": 1, "sha256": "a" * 64}}, {}),
                    ({}, {retired[1]: {"size": 11, "mtime": 1, "sha256": "b" * 64}}),
                    (
                        {retired[0]: {"size": 10, "mtime": 2, "sha256": "a" * 64}},
                        {retired[0]: {"size": 12, "mtime": 1, "sha256": "b" * 64}},
                    ),
                )
                for left, right in scenarios:
                    to_push, to_pull = module.plan_sync(left, right, mode=mode)
                    planned = [entry[0] for entry in to_push + to_pull]
                    self.assertFalse(set(retired) & set(planned), (module.__name__, mode, planned))

        local_legacy = {
            "exports/分析.xlsx": b"preserve-local-japanese",
            "exports/analysis.xlsx": b"preserve-local-english",
        }
        remote_legacy = {
            "exports/分析.xlsx": b"preserve-remote-japanese",
            "exports/analysis.xlsx": b"preserve-remote-english",
        }
        full_artifacts = {
            "exports/full-data/generations/test-generation/index.json": b'{"verified":true}',
            "exports/full-data/generations/test-generation/manifest.json": b'{"version":1}',
            "exports/full-data/generations/test-generation/xlsx/piece-000001.xlsx": b"full-data-xlsx",
        }
        for rel, content in {**local_legacy, **full_artifacts}.items():
            path = self.local_dir / rel
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(content)
        for rel, content in {**remote_legacy, **full_artifacts}.items():
            path = self.remote_dir / rel
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(content)

        before_local = {rel: (self.local_dir / rel).read_bytes() for rel in local_legacy}
        before_remote = {rel: (self.remote_dir / rel).read_bytes() for rel in remote_legacy}

        class Args:
            mode = "both"
            dry_run = False
            remote_host = "mock-nas"
            remote_dir = str(self.remote_dir)

        with patch.object(sync_gdrive, "copy_file") as gdrive_copy:
            sync_gdrive._run_sync(Args(), self.local_dir, self.remote_dir)
            gdrive_copy.assert_not_called()

        with patch.object(sync_nas, "scan_remote", return_value=sync_nas.scan_local(self.remote_dir)), \
             patch.object(sync_nas, "push_file") as nas_push, \
             patch.object(sync_nas, "pull_file") as nas_pull:
            sync_nas._run_sync(Args(), self.local_dir)
            nas_push.assert_not_called()
            nas_pull.assert_not_called()

        self.assertEqual(before_local, {rel: (self.local_dir / rel).read_bytes() for rel in local_legacy})
        self.assertEqual(before_remote, {rel: (self.remote_dir / rel).read_bytes() for rel in remote_legacy})
        for rel in full_artifacts:
            self.assertEqual((self.local_dir / rel).read_bytes(), full_artifacts[rel])
            self.assertEqual((self.remote_dir / rel).read_bytes(), full_artifacts[rel])

        # Direct transfer APIs also reject these two exact paths before invoking subprocesses or writing.
        for rel in retired:
            with patch("subprocess.Popen") as popen, patch("subprocess.run") as run:
                with self.assertRaisesRegex(ValueError, "Legacy analysis workbook sync is retired"):
                    sync_nas.push_file(self.local_dir / rel, rel, "mock-nas", str(self.remote_dir), local_dir=self.local_dir)
                with self.assertRaisesRegex(ValueError, "Legacy analysis workbook sync is retired"):
                    sync_nas.pull_file("mock-nas", str(self.remote_dir), rel, self.local_dir / rel, local_dir=self.local_dir)
                with self.assertRaisesRegex(ValueError, "Legacy analysis workbook sync is retired"):
                    sync_gdrive.copy_file(self.local_dir / rel, self.remote_dir / rel, root_dir=self.remote_dir, rel_path=rel)
                with self.assertRaisesRegex(ValueError, "Legacy analysis workbook sync is retired"):
                    sync_gdrive.copy_file(self.local_dir / rel, self.remote_dir / rel, root_dir=self.remote_dir)
                popen.assert_not_called()
                run.assert_not_called()

        self.assertEqual(before_local, {rel: (self.local_dir / rel).read_bytes() for rel in local_legacy})
        self.assertEqual(before_remote, {rel: (self.remote_dir / rel).read_bytes() for rel in remote_legacy})

if __name__ == '__main__':
    unittest.main()
