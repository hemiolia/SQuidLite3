#!/usr/bin/env python3
"""
Unit tests for nas_publish_exports.py using synthetic directories and fake executable rclone.
"""

import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
PUBLISH_SCRIPT = ROOT / "scripts" / "nas_publish_exports.py"


FAKE_RCLONE_SCRIPT = f"""#!{sys.executable}
import sys
import os
import json
import hashlib
import shutil
from pathlib import Path

remote_root_str = os.environ.get("FAKE_REMOTE_ROOT")
if not remote_root_str:
    sys.stderr.write("FAKE_REMOTE_ROOT not set\\n")
    sys.exit(1)
remote_root = Path(remote_root_str)

if os.environ.get("FAKE_RCLONE_LEAK_SECRET"):
    sys.stderr.write("DUMMY_BEARER_SECRET_TOKEN_987654\\n")

if os.environ.get("FAKE_RCLONE_FAIL_STAT") and "lsjson" in sys.argv:
    sys.stderr.write("lsjson simulated fatal error\\n")
    sys.exit(1)

def resolve_target(arg):
    if ":" in arg:
        _, rel = arg.split(":", 1)
        return remote_root / rel.lstrip("/")
    return Path(arg)

args = sys.argv[1:]
if not args:
    sys.exit(1)

subcmd = args[0]

if subcmd == "lsjson":
    target_arg = args[-1]
    target_path = resolve_target(target_arg)

    if "index.html" in target_arg:
        metadata = os.environ.get("FAKE_RCLONE_LSJSON_METADATA")
        if metadata is not None:
            print(metadata)
            sys.exit(0)
        if os.environ.get("FAKE_RCLONE_LSJSON_HUGE"):
            print(" " * 65537)
            sys.exit(0)

    counter_file = os.environ.get("FAKE_RCLONE_STAT_COUNTER_FILE")
    if counter_file and "index.html" in target_arg:
        c = 0
        if os.path.exists(counter_file):
            try:
                c = int(Path(counter_file).read_text().strip())
            except Exception:
                c = 0
        c += 1
        Path(counter_file).write_text(str(c))
        if c == 2 and os.environ.get("FAKE_RCLONE_RACE_DETECT"):
            if target_path.exists():
                target_path.write_bytes(b"CONCURRENT_MODIFICATION_DETECTED_BY_TEST")

    if not target_path.parent.exists():
        sys.exit(3)
    if not target_path.exists():
        sys.exit(4)

    if target_path.is_dir():
        res = {{
            "Path": target_path.name,
            "Name": target_path.name,
            "Size": -1,
            "MimeType": "inode/directory",
            "ModTime": "2026-09-27T00:00:00Z",
            "IsDir": True
        }}
    else:
        data = target_path.read_bytes()
        res = {{
            "Path": target_path.name,
            "Name": target_path.name,
            "Size": len(data),
            "MimeType": "application/octet-stream",
            "ModTime": "2026-09-27T00:00:00Z",
            "IsDir": False,
            "Hashes": {{"SHA-256": hashlib.sha256(data).hexdigest()}}
        }}
    print(json.dumps(res))
    sys.exit(0)

elif subcmd == "cat":
    target_arg = args[-1]
    target_path = resolve_target(target_arg)
    if not target_path.exists() or target_path.is_dir():
        sys.stderr.write("File not found\\n")
        sys.exit(1)

    if os.environ.get("FAKE_RCLONE_CORRUPT_CAT"):
        sys.stdout.buffer.write(b"CORRUPTED_READBACK_BYTES_FOR_TEST")
        sys.exit(0)

    data = target_path.read_bytes()
    sys.stdout.buffer.write(data)
    sys.exit(0)

elif subcmd == "copyto":
    positional = [a for a in args[1:] if not a.startswith("-")]
    if len(positional) != 2:
        sys.exit(1)
    src_arg, dst_arg = positional
    if os.environ.get("FAKE_RCLONE_FAIL_COPY_XLSX") and "分析.xlsx" in dst_arg:
        sys.exit(1)
    src_path = resolve_target(src_arg)
    dst_path = resolve_target(dst_arg)

    dst_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src_path, dst_path)
    sys.exit(0)

else:
    sys.stderr.write(f"Unknown subcommand {{subcmd}}\\n")
    sys.exit(1)
"""


class TestNasPublishExports(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name).resolve()

        self.exports_dir = self.root / "exports"
        self.state_dir = self.root / "state"
        self.fake_remote_dir = self.root / "fake_remote"

        self.exports_dir.mkdir(parents=True, exist_ok=True)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.fake_remote_dir.mkdir(parents=True, exist_ok=True)

        # Create fake rclone script
        self.fake_rclone = self.root / "fake_rclone"
        self.fake_rclone.write_text(FAKE_RCLONE_SCRIPT, encoding="utf-8")
        self.fake_rclone.chmod(self.fake_rclone.stat().st_mode | stat.S_IXUSR | stat.S_IRUSR)

        # Set up standard source files
        (self.exports_dir / "gui").mkdir(parents=True, exist_ok=True)
        self.initial_html_bytes = b"<!DOCTYPE html><html><body>Archive GUI</body></html>"
        self.initial_xlsx_bytes = b"PK\x03\x04DummyExcelSpreadsheetBytes"

        self.html_file = self.exports_dir / "gui" / "index.html"
        self.xlsx_file = self.exports_dir / "分析.xlsx"

        self.html_file.write_bytes(self.initial_html_bytes)
        self.xlsx_file.write_bytes(self.initial_xlsx_bytes)

        self.remote_prefix = "mockremote:ikaring-exports"

    def tearDown(self):
        self.temp_dir.cleanup()

    def run_publish(
        self,
        exports_dir=None,
        state_dir=None,
        remote=None,
        rclone_bin=None,
        env_extra=None,
    ) -> subprocess.CompletedProcess:
        env = dict(os.environ)
        env["FAKE_REMOTE_ROOT"] = str(self.fake_remote_dir)
        if env_extra:
            env.update(env_extra)

        cmd = [
            sys.executable,
            str(PUBLISH_SCRIPT),
            "--exports-dir",
            str(exports_dir or self.exports_dir),
            "--state-dir",
            str(state_dir or self.state_dir),
            "--remote",
            str(remote or self.remote_prefix),
            "--rclone-bin",
            str(rclone_bin or self.fake_rclone),
        ]
        return subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env,
        )

    def test_initial_upload_success_and_receipt(self):
        """Initial run with nonexistent remote uploads both files and writes state."""
        proc = self.run_publish()
        self.assertEqual(proc.returncode, 0, f"Failed with stderr: {proc.stderr}")

        receipt = json.loads(proc.stdout)
        self.assertEqual(receipt["status"], "success")
        self.assertEqual(receipt["counts"]["uploaded"], 2)
        self.assertEqual(receipt["counts"]["skipped"], 0)
        self.assertEqual(receipt["counts"]["conflicts"], 0)
        self.assertEqual(receipt["files"]["gui/index.html"]["status"], "uploaded")
        self.assertEqual(receipt["files"]["分析.xlsx"]["status"], "uploaded")
        self.assertNotIn(self.remote_prefix, proc.stdout)

        # Verify remote artifacts
        remote_html = self.fake_remote_dir / "ikaring-exports" / "gui" / "index.html"
        remote_xlsx = self.fake_remote_dir / "ikaring-exports" / "分析.xlsx"
        self.assertTrue(remote_html.exists())
        self.assertTrue(remote_xlsx.exists())
        self.assertEqual(remote_html.read_bytes(), self.initial_html_bytes)
        self.assertEqual(remote_xlsx.read_bytes(), self.initial_xlsx_bytes)

        # Verify state file
        state_file = self.state_dir / ".nas-publish-state.json"
        self.assertTrue(state_file.exists())
        state = json.loads(state_file.read_text(encoding="utf-8"))
        self.assertEqual(state["version"], 1)
        self.assertEqual(state["remote"], self.remote_prefix)
        self.assertEqual(
            state["files"]["gui/index.html"],
            hashlib.sha256(self.initial_html_bytes).hexdigest(),
        )
        self.assertEqual(
            state["files"]["分析.xlsx"],
            hashlib.sha256(self.initial_xlsx_bytes).hexdigest(),
        )

    def test_second_publish_skips_when_identical(self):
        """Second run with identical content skips transfers and preserves state."""
        proc1 = self.run_publish()
        self.assertEqual(proc1.returncode, 0)

        proc2 = self.run_publish()
        self.assertEqual(proc2.returncode, 0)
        receipt = json.loads(proc2.stdout)
        self.assertEqual(receipt["status"], "success")
        self.assertEqual(receipt["counts"]["uploaded"], 0)
        self.assertEqual(receipt["counts"]["skipped"], 2)
        self.assertEqual(receipt["counts"]["conflicts"], 0)
        self.assertEqual(receipt["files"]["gui/index.html"]["status"], "skipped")
        self.assertEqual(receipt["files"]["分析.xlsx"]["status"], "skipped")

    def test_source_only_normal_update_without_conflict(self):
        """Updating only source updates remote without generating conflict artifacts."""
        proc1 = self.run_publish()
        self.assertEqual(proc1.returncode, 0)

        # Update source index.html
        updated_html = b"<!DOCTYPE html><html><body>Archive GUI v2</body></html>"
        self.html_file.write_bytes(updated_html)

        proc2 = self.run_publish()
        self.assertEqual(proc2.returncode, 0)
        receipt = json.loads(proc2.stdout)
        self.assertEqual(receipt["counts"]["uploaded"], 1)
        self.assertEqual(receipt["counts"]["skipped"], 1)
        self.assertEqual(receipt["counts"]["conflicts"], 0)
        self.assertEqual(receipt["files"]["gui/index.html"]["status"], "uploaded")
        self.assertEqual(receipt["files"]["分析.xlsx"]["status"], "skipped")

        # Ensure no conflict directory was created
        conflicts_dir = self.state_dir / "conflicts"
        self.assertFalse(conflicts_dir.exists())

        remote_conflicts_dir = self.fake_remote_dir / "ikaring-exports" / ".sync-conflicts"
        self.assertFalse(remote_conflicts_dir.exists())

        # Verify remote received updated content
        remote_html = self.fake_remote_dir / "ikaring-exports" / "gui" / "index.html"
        self.assertEqual(remote_html.read_bytes(), updated_html)

    def test_remote_independent_update_saves_conflict_then_publishes(self):
        """
        When remote is modified independently (remote SHA != state SHA),
        the remote content is saved locally and to remote .sync-conflicts before publishing.
        """
        proc1 = self.run_publish()
        self.assertEqual(proc1.returncode, 0)

        # Independently modify remote file
        remote_html = self.fake_remote_dir / "ikaring-exports" / "gui" / "index.html"
        divergent_remote_bytes = b"<html>Divergent Remote Edit</html>"
        remote_html.write_bytes(divergent_remote_bytes)

        proc2 = self.run_publish()
        self.assertEqual(proc2.returncode, 0)
        receipt = json.loads(proc2.stdout)
        self.assertEqual(receipt["counts"]["conflicts"], 1)
        self.assertEqual(receipt["files"]["gui/index.html"]["status"], "conflict_uploaded")
        self.assertEqual(receipt["files"]["分析.xlsx"]["status"], "skipped")

        # Verify local conflict backup
        conflicts_dir = self.state_dir / "conflicts"
        self.assertTrue(conflicts_dir.exists())
        conflict_uuids = [p.name for p in conflicts_dir.iterdir() if p.is_dir()]
        self.assertEqual(len(conflict_uuids), 1)
        saved_conflict = conflicts_dir / conflict_uuids[0] / "gui" / "index.html"
        self.assertTrue(saved_conflict.exists())
        self.assertEqual(saved_conflict.read_bytes(), divergent_remote_bytes)
        # Verify 0600 permissions
        self.assertEqual(stat.S_IMODE(saved_conflict.stat().st_mode), 0o600)

        # Verify remote .sync-conflicts backup
        remote_conflicts = (
            self.fake_remote_dir / "ikaring-exports" / ".sync-conflicts" / conflict_uuids[0] / "gui" / "index.html"
        )
        self.assertTrue(remote_conflicts.exists())
        self.assertEqual(remote_conflicts.read_bytes(), divergent_remote_bytes)

        # Remote file itself should now be updated to source content
        self.assertEqual(remote_html.read_bytes(), self.initial_html_bytes)

    def test_first_run_existing_remote_difference_saves_conflict(self):
        """Initial run with differing pre-existing remote file preserves remote content as conflict."""
        pre_existing_bytes = b"<html>Pre-existing Remote HTML</html>"
        remote_html = self.fake_remote_dir / "ikaring-exports" / "gui" / "index.html"
        remote_html.parent.mkdir(parents=True, exist_ok=True)
        remote_html.write_bytes(pre_existing_bytes)

        proc = self.run_publish()
        self.assertEqual(proc.returncode, 0)
        receipt = json.loads(proc.stdout)
        self.assertEqual(receipt["counts"]["conflicts"], 1)
        self.assertEqual(receipt["files"]["gui/index.html"]["status"], "conflict_uploaded")

        # Verify conflict preserved
        conflicts_dir = self.state_dir / "conflicts"
        self.assertTrue(conflicts_dir.exists())
        conflict_uuids = [p.name for p in conflicts_dir.iterdir() if p.is_dir()]
        self.assertEqual(len(conflict_uuids), 1)
        saved_conflict = conflicts_dir / conflict_uuids[0] / "gui" / "index.html"
        self.assertEqual(saved_conflict.read_bytes(), pre_existing_bytes)

    def test_cat_readback_failure_preserves_old_state(self):
        """If cat readback verification fails, transaction aborts and old state remains unmodified."""
        proc1 = self.run_publish()
        self.assertEqual(proc1.returncode, 0)

        state_file = self.state_dir / ".nas-publish-state.json"
        old_state_content = state_file.read_text(encoding="utf-8")

        # Update source
        self.html_file.write_bytes(b"<html>New HTML</html>")

        # Trigger readback corruption
        proc2 = self.run_publish(env_extra={"FAKE_RCLONE_CORRUPT_CAT": "1"})
        self.assertEqual(proc2.returncode, 1)
        self.assertIn("VERIFY_ERROR", proc2.stderr)

        # State must remain identical to prior successful cycle
        self.assertEqual(state_file.read_text(encoding="utf-8"), old_state_content)

    def test_stat_non_notfound_failure_rejected(self):
        """lsjson stat failures with exit codes other than 3 or 4 are rejected fail-closed."""
        proc = self.run_publish(env_extra={"FAKE_RCLONE_FAIL_STAT": "1"})
        self.assertEqual(proc.returncode, 1)
        self.assertIn("REMOTE_ERROR", proc.stderr)
        state_file = self.state_dir / ".nas-publish-state.json"
        self.assertFalse(state_file.exists())

    def test_successful_stat_requires_one_bounded_file_object(self):
        """Exit zero is never evidence of absence or valid file metadata by itself."""
        invalid_metadata = [
            "[]",
            "null",
            '{"IsDir": true, "Size": 1}',
            '{"Size": 1}',
            '{"IsDir": false, "Size": true}',
            '{"IsDir": false, "Size": "1"}',
            '{"IsDir": false, "Size": -1}',
            '{"IsDir": false, "Size": 67108865}',
        ]
        for metadata in invalid_metadata:
            with self.subTest(metadata=metadata):
                proc = self.run_publish(env_extra={"FAKE_RCLONE_LSJSON_METADATA": metadata})
                self.assertEqual(proc.returncode, 1)
                self.assertIn(proc.stderr.strip(), {"REMOTE_ERROR", "SOURCE_OVERSIZE"})
                self.assertFalse((self.state_dir / ".nas-publish-state.json").exists())
                self.assertFalse((self.fake_remote_dir / "ikaring-exports").exists())

        proc = self.run_publish(env_extra={"FAKE_RCLONE_LSJSON_HUGE": "1"})
        self.assertEqual(proc.returncode, 1)
        self.assertEqual(proc.stderr.strip(), "REMOTE_ERROR")

    def test_failure_on_second_transfer_keeps_old_state_and_may_update_first(self):
        """Remote transfers are sequential, while the baseline commits after both verify."""
        self.assertEqual(self.run_publish().returncode, 0)
        state_file = self.state_dir / ".nas-publish-state.json"
        old_state = state_file.read_bytes()
        html_update = b"<html>new HTML</html>"
        xlsx_update = b"PK new XLSX"
        self.html_file.write_bytes(html_update)
        self.xlsx_file.write_bytes(xlsx_update)

        proc = self.run_publish(env_extra={"FAKE_RCLONE_FAIL_COPY_XLSX": "1"})
        self.assertEqual(proc.returncode, 1)
        self.assertEqual(proc.stderr.strip(), "REMOTE_ERROR")
        self.assertEqual(state_file.read_bytes(), old_state)
        remote_root = self.fake_remote_dir / "ikaring-exports"
        self.assertEqual((remote_root / "gui" / "index.html").read_bytes(), html_update)
        self.assertEqual((remote_root / "分析.xlsx").read_bytes(), self.initial_xlsx_bytes)

    def test_remote_conflict_detected_before_upload(self):
        """Detects concurrent modification in pre-upload verification and rejects."""
        proc1 = self.run_publish()
        self.assertEqual(proc1.returncode, 0)

        # Update source
        self.html_file.write_bytes(b"<html>Local Change</html>")

        counter_file = self.root / "stat_counter.txt"
        env = {
            "FAKE_RCLONE_STAT_COUNTER_FILE": str(counter_file),
            "FAKE_RCLONE_RACE_DETECT": "1",
        }
        proc2 = self.run_publish(env_extra=env)
        self.assertEqual(proc2.returncode, 1)
        self.assertIn("PRECONDITION_FAILED", proc2.stderr)

    def test_rejection_symlinks_oversize_missing(self):
        """Rejects symlinks, missing files, and oversized files."""
        # 1. Missing file
        self.xlsx_file.unlink()
        proc_missing = self.run_publish()
        self.assertEqual(proc_missing.returncode, 1)
        self.assertIn("SOURCE_MISSING", proc_missing.stderr)
        self.xlsx_file.write_bytes(self.initial_xlsx_bytes)

        # 2. Oversize file (> 64MiB)
        oversize_bytes = 64 * 1024 * 1024 + 1
        with open(self.html_file, "wb") as f:
            f.seek(oversize_bytes - 1)
            f.write(b"\0")
        proc_oversize = self.run_publish()
        self.assertEqual(proc_oversize.returncode, 1)
        self.assertIn("SOURCE_OVERSIZE", proc_oversize.stderr)
        self.html_file.write_bytes(self.initial_html_bytes)

        # 3. File symlink
        self.html_file.unlink()
        target_outside = self.root / "outside.html"
        target_outside.write_bytes(b"Outside")
        self.html_file.symlink_to(target_outside)
        proc_symlink_file = self.run_publish()
        self.assertEqual(proc_symlink_file.returncode, 1)
        self.assertIn("PATH_SAFETY_ERROR", proc_symlink_file.stderr)
        self.html_file.unlink()
        self.html_file.write_bytes(self.initial_html_bytes)

        # 4. Directory symlink
        gui_dir = self.exports_dir / "gui"
        shutil.rmtree(gui_dir)
        real_gui_dir = self.root / "real_gui"
        real_gui_dir.mkdir()
        (real_gui_dir / "index.html").write_bytes(self.initial_html_bytes)
        gui_dir.symlink_to(real_gui_dir)
        proc_symlink_dir = self.run_publish()
        self.assertEqual(proc_symlink_dir.returncode, 1)
        self.assertIn("PATH_SAFETY_ERROR", proc_symlink_dir.stderr)
        gui_dir.unlink()
        gui_dir.mkdir()
        self.html_file.write_bytes(self.initial_html_bytes)

        # 5. State directory is symlink
        sym_state = self.root / "sym_state"
        sym_state.symlink_to(self.state_dir)
        proc_symlink_state = self.run_publish(state_dir=sym_state)
        self.assertEqual(proc_symlink_state.returncode, 1)
        self.assertIn("PATH_SAFETY_ERROR", proc_symlink_state.stderr)

    def test_rejects_ancestor_and_conflicts_symlinks(self):
        """Path checks cover root ancestors and the lazily created conflict tree."""
        aliased_exports = self.root / "aliased_exports"
        aliased_exports.symlink_to(self.exports_dir, target_is_directory=True)
        source_proc = self.run_publish(exports_dir=aliased_exports)
        self.assertEqual(source_proc.returncode, 1)
        self.assertEqual(source_proc.stderr.strip(), "PATH_SAFETY_ERROR")

        aliased_parent = self.root / "aliased_parent"
        aliased_parent.symlink_to(self.root, target_is_directory=True)
        state_proc = self.run_publish(state_dir=aliased_parent / "state")
        self.assertEqual(state_proc.returncode, 1)
        self.assertEqual(state_proc.stderr.strip(), "PATH_SAFETY_ERROR")

        self.assertEqual(self.run_publish().returncode, 0)
        state_file = self.state_dir / ".nas-publish-state.json"
        old_state = state_file.read_bytes()
        remote_html = self.fake_remote_dir / "ikaring-exports" / "gui" / "index.html"
        remote_html.write_bytes(b"independent remote edit")
        outside = self.root / "outside_conflicts"
        outside.mkdir()
        (self.state_dir / "conflicts").symlink_to(outside, target_is_directory=True)

        conflict_proc = self.run_publish()
        self.assertEqual(conflict_proc.returncode, 1)
        self.assertEqual(conflict_proc.stderr.strip(), "PATH_SAFETY_ERROR")
        self.assertEqual(remote_html.read_bytes(), b"independent remote edit")
        self.assertEqual(state_file.read_bytes(), old_state)
        self.assertEqual(list(outside.iterdir()), [])

    def test_nonregular_source_and_growth_after_fstat_rejected(self):
        import scripts.nas_publish_exports as publisher
        from unittest.mock import patch

        self.html_file.unlink()
        os.mkfifo(self.html_file)
        fifo_proc = self.run_publish()
        self.assertEqual(fifo_proc.returncode, 1)
        self.assertEqual(fifo_proc.stderr.strip(), "PATH_SAFETY_ERROR")
        self.html_file.unlink()
        self.html_file.write_bytes(self.initial_html_bytes)

        real_fstat = os.fstat
        grown = False

        def fstat_then_grow(fd):
            nonlocal grown
            st = real_fstat(fd)
            if not grown:
                with self.html_file.open("ab") as output:
                    output.write(b"x" * 128)
                grown = True
            return st

        with patch.object(publisher, "MAX_FILE_BYTES", 128), patch.object(os, "fstat", side_effect=fstat_then_grow):
            with self.assertRaises(publisher.PublishError) as caught:
                publisher.snapshot_source_files(self.exports_dir)
        self.assertTrue(grown)
        self.assertEqual(caught.exception.category, "SOURCE_OVERSIZE")

    def test_no_secrets_leaked_to_stderr(self):
        """Ensures secrets printed by subprocess are not forwarded to stderr."""
        proc = self.run_publish(
            env_extra={
                "FAKE_RCLONE_FAIL_STAT": "1",
                "FAKE_RCLONE_LEAK_SECRET": "1",
            }
        )
        self.assertEqual(proc.returncode, 1)
        self.assertNotIn("DUMMY_BEARER_SECRET_TOKEN_987654", proc.stderr)
        # stderr must only contain the safe category
        self.assertEqual(proc.stderr.strip(), "REMOTE_ERROR")

    def test_flock_concurrency_rejection(self):
        """Confirms non-blocking flock rejects concurrent publish cycles."""
        lock_file = self.state_dir / ".nas-publish.lock"
        lock_fd = os.open(lock_file, os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            proc = self.run_publish()
            self.assertEqual(proc.returncode, 1)
            self.assertIn("LOCK_BUSY", proc.stderr)
        finally:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
            os.close(lock_fd)

    def test_state_validation_fail_closed(self):
        """Corrupt state or remote mismatch in state JSON fails closed."""
        state_file = self.state_dir / ".nas-publish-state.json"

        # 1. Corrupt JSON
        state_file.write_text("{corrupt json", encoding="utf-8")
        proc1 = self.run_publish()
        self.assertEqual(proc1.returncode, 1)
        self.assertIn("STATE_ERROR", proc1.stderr)

        # 2. Remote mismatch
        mismatched_state = {
            "version": 1,
            "remote": "other_remote:path",
            "files": {"gui/index.html": "a" * 64, "分析.xlsx": "b" * 64},
        }
        state_file.write_text(json.dumps(mismatched_state), encoding="utf-8")
        proc2 = self.run_publish()
        self.assertEqual(proc2.returncode, 1)
        self.assertIn("STATE_ERROR", proc2.stderr)

        # 3. Invalid version
        invalid_ver_state = {
            "version": 2,
            "remote": self.remote_prefix,
            "files": {"gui/index.html": "a" * 64, "分析.xlsx": "b" * 64},
        }
        state_file.write_text(json.dumps(invalid_ver_state), encoding="utf-8")
        proc3 = self.run_publish()
        self.assertEqual(proc3.returncode, 1)
        self.assertIn("STATE_ERROR", proc3.stderr)

        # JSON booleans must not pass the integer schema-version check.
        invalid_ver_state["version"] = True
        state_file.write_text(json.dumps(invalid_ver_state), encoding="utf-8")
        proc4 = self.run_publish()
        self.assertEqual(proc4.returncode, 1)
        self.assertIn("STATE_ERROR", proc4.stderr)

    def test_source_snapshot_persists_even_if_source_modified_mid_cycle(self):
        """
        Contract: The in-memory snapshot taken at cycle start is published even if
        source files on disk are modified during the cycle. Source files are never modified/overwritten.
        """
        import scripts.nas_publish_exports as publisher

        # Initial upload
        proc1 = self.run_publish()
        self.assertEqual(proc1.returncode, 0)

        # Simulate mid-cycle change by hooking run_rclone_cmd:
        # After initial snapshot, disk source is modified to something else.
        original_run_rclone = publisher.run_rclone_cmd
        first_call = True

        def hook_rclone(cmd):
            nonlocal first_call
            if first_call:
                # Modify source on disk to simulate concurrent modification during cycle
                self.html_file.write_bytes(b"<html>Mid-Cycle Disk Modification</html>")
                first_call = False
            return original_run_rclone(cmd)

        # Update source before starting second cycle
        snapshot_expected_html = b"<html>Snapshot Version 2</html>"
        self.html_file.write_bytes(snapshot_expected_html)

        from unittest.mock import patch

        with (
            patch.dict(os.environ, {"FAKE_REMOTE_ROOT": str(self.fake_remote_dir)}),
            patch("scripts.nas_publish_exports.run_rclone_cmd", side_effect=hook_rclone),
        ):
            receipt = publisher.publish_cycle(
                exports_dir=self.exports_dir,
                state_dir=self.state_dir,
                remote_prefix=self.remote_prefix,
                rclone_bin=str(self.fake_rclone),
            )

        self.assertEqual(receipt["status"], "success")
        self.assertEqual(receipt["counts"]["uploaded"], 1)

        # Remote file must contain the snapshot bytes (Snapshot Version 2), NOT the mid-cycle disk change
        remote_html = self.fake_remote_dir / "ikaring-exports" / "gui" / "index.html"
        self.assertEqual(remote_html.read_bytes(), snapshot_expected_html)

        # Disk source file must retain its modified content, never overwritten/reverted
        self.assertEqual(self.html_file.read_bytes(), b"<html>Mid-Cycle Disk Modification</html>")

    def test_remote_oversize_rejected(self):
        """Pre-check rejects existing remote files exceeding 64MiB."""
        oversize_bytes = 64 * 1024 * 1024 + 10
        remote_html = self.fake_remote_dir / "ikaring-exports" / "gui" / "index.html"
        remote_html.parent.mkdir(parents=True, exist_ok=True)
        with open(remote_html, "wb") as f:
            f.seek(oversize_bytes - 1)
            f.write(b"\0")

        proc = self.run_publish()
        self.assertEqual(proc.returncode, 1)
        self.assertIn("SOURCE_OVERSIZE", proc.stderr)

    def test_argument_validation(self):
        """Invalid CLI arguments or invalid remote format are rejected fail-closed."""
        import scripts.nas_publish_exports as publisher

        # Missing colon in remote prefix
        proc1 = self.run_publish(remote="invalid_remote_no_colon")
        self.assertEqual(proc1.returncode, 1)
        self.assertIn("CONFIG_ERROR", proc1.stderr)

        for invalid_remote in ("-x:path", "--config:path", ":path", "name:", "name:../path", "name:/absolute"):
            with self.subTest(remote=invalid_remote):
                with self.assertRaises(publisher.PublishError) as caught:
                    publisher.join_remote(invalid_remote, "gui/index.html")
                self.assertEqual(caught.exception.category, "CONFIG_ERROR")
                proc = self.run_publish(remote=invalid_remote)
                self.assertNotEqual(proc.returncode, 0)
                self.assertFalse((self.fake_remote_dir / "ikaring-exports").exists())

        # Null byte in arguments
        with self.assertRaises(publisher.PublishError) as ctx:
            publisher.check_command_args(["rclone", "lsjson\0bad"])
        self.assertEqual(ctx.exception.category, "CONFIG_ERROR")


if __name__ == "__main__":
    unittest.main()
