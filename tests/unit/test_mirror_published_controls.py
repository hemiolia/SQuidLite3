import contextlib
import fcntl
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "src/python"))
sys.path.insert(0, str(ROOT / "tests/unit"))

import nas_full_data_publish as publisher  # noqa: E402
import mirror_published_controls as mirror_module  # noqa: E402
import test_full_data_delta_publish as delta_fixtures  # noqa: E402
from mirror_published_controls import (  # noqa: E402
    _RemoteControlCache,
    main,
    mirror_published_controls,
)
from published_sqlite_reader import PublishedSQLiteReader  # noqa: E402


REMOTE = delta_fixtures.REMOTE
BASELINE_ID = delta_fixtures.BASELINE_ID
FIRST_ID = "20261003T041001Z-efef0001"
SECOND_ID = "20261003T041002Z-efef0002"


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def _compact(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":")).encode("utf-8")


def _tree_snapshot(root):
    if not root.exists():
        return {}
    result = {}
    for path in sorted(root.rglob("*")):
        relative = path.relative_to(root).as_posix()
        if relative == ".published-controls.lock":
            continue
        info = path.lstat()
        if path.is_dir():
            result[relative] = ("directory", info.st_mode & 0o777, info.st_mtime_ns)
        else:
            result[relative] = (path.read_bytes(), info.st_mode & 0o777, info.st_mtime_ns)
    return result


class ReadOnlyFakeClient:
    """A small remote that exposes only the publisher's read-only API."""

    def __init__(self, objects):
        self.objects = dict(objects)
        self.stat_calls = []
        self.readback_calls = []
        self.readback_bytes_calls = []
        self.verify_calls = 0
        self.copy_calls = []
        self.bad_bytes_path = None
        self.advance_after_bytes_path = None
        self.advance_to = None
        self.fail_verify = False

    def stat(self, remote_path):
        self.stat_calls.append(remote_path)
        raw = self.objects.get(remote_path)
        if raw is None:
            return None
        return {"IsDir": False, "Size": len(raw)}

    def readback(self, remote_path):
        self.readback_calls.append(remote_path)
        raw = self.objects.get(remote_path)
        if raw is None:
            raise publisher.PublishError("REMOTE_OBJECT_MISSING")
        return len(raw), _sha(raw)

    def readback_bytes(self, remote_path, expected_bytes, expected_sha):
        self.readback_bytes_calls.append(remote_path)
        raw = self.objects.get(remote_path)
        if raw is None:
            raise publisher.PublishError("REMOTE_OBJECT_MISSING")
        if self.bad_bytes_path == remote_path:
            return raw + b" "
        if (len(raw), _sha(raw)) != (expected_bytes, expected_sha):
            raise publisher.PublishError("REMOTE_READBACK_MISMATCH")
        captured = raw
        if self.advance_after_bytes_path == remote_path:
            self.objects[remote_path] = self.advance_to
        return captured

    def verify_directory_bindings(self):
        self.verify_calls += 1
        if self.fail_verify:
            raise publisher.PublishError("REMOTE_DIRECTORY_BINDING_CHANGED")

    def copyto(self, *_args, **_kwargs):
        self.copy_calls.append((_args, _kwargs))
        raise AssertionError("control mirror must never upload")


class MirrorPublishedControlsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.case = delta_fixtures.FullDataDeltaPublishTests(
            "test_initial_and_incremental_delta_append_chain_and_reread_every_file"
        )
        cls.case.setUp()
        try:
            cls.baseline_objects = dict(cls.case.remote.objects)
            first_dir, first_plan = cls.case.prepare(FIRST_ID)
            cls.case.publish(first_dir)
            cls.first_objects = dict(cls.case.remote.objects)

            conn = cls.case._open_current_writer()
            try:
                conn.execute("UPDATE records SET payload=?, ratio=? WHERE id=1",
                             ("second generation value", 2.75))
                conn.commit()
            finally:
                conn.close()
            second_dir, _second_plan = cls.case.prepare(SECOND_ID, previous=first_plan)
            cls.case.publish(second_dir)
            cls.second_objects = dict(cls.case.remote.objects)
        except BaseException:
            cls.case.tearDown()
            raise

    @classmethod
    def tearDownClass(cls):
        cls.case.tearDown()

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name).resolve()

    def tearDown(self):
        self.temporary.cleanup()

    def _path(self, objects, relative):
        return f"{REMOTE}/{relative}"

    def _mirror(self, objects=None, control_dir=None, client=None):
        client = client or ReadOnlyFakeClient(objects or self.second_objects)
        control_dir = control_dir or self.root / "controls"
        return mirror_published_controls(client, REMOTE, control_dir), client, Path(control_dir)

    def test_full_chain_controls_are_readback_verified_and_idempotent(self):
        receipt, client, control_dir = self._mirror()
        latest_raw = self.second_objects[f"{REMOTE}/latest.json"]
        self.assertEqual(set(receipt), {
            "generation_id", "baseline_generation_id", "control_count", "control_bytes",
            "pinned_latest_sha256", "controls_full_readback",
            "all_remote_artifacts_verified", "realtime_synchronized",
        })
        self.assertEqual(receipt["generation_id"], SECOND_ID)
        self.assertEqual(receipt["baseline_generation_id"], BASELINE_ID)
        self.assertEqual(receipt["control_count"], 6)
        self.assertEqual(receipt["control_bytes"], sum(
            len(self.second_objects[path]) for path in client.stat_calls
        ))
        self.assertEqual(receipt["pinned_latest_sha256"], _sha(latest_raw))
        self.assertIs(receipt["controls_full_readback"], True)
        self.assertIs(receipt["all_remote_artifacts_verified"], False)
        self.assertIs(receipt["realtime_synchronized"], False)
        self.assertEqual(client.verify_calls, 2)
        self.assertEqual(client.copy_calls, [])
        self.assertEqual(set(client.stat_calls), set(client.readback_calls))
        self.assertEqual(set(client.stat_calls), set(client.readback_bytes_calls))
        self.assertTrue(all(
            path.endswith(("latest.json", "index.json", "delta-plan.json"))
            for path in client.stat_calls
        ))
        self.assertFalse(any(path.endswith((".sqlite3", ".xlsx")) for path in client.stat_calls))
        self.assertEqual((control_dir / "latest.json").read_bytes(), latest_raw)
        self.assertEqual(os.stat(control_dir).st_mode & 0o777, 0o700)
        self.assertEqual(os.stat(control_dir / "latest.json").st_mode & 0o777, 0o600)
        snapshot = _tree_snapshot(control_dir)

        repeated, repeated_client, _ = self._mirror(control_dir=control_dir)
        self.assertEqual(repeated, receipt)
        self.assertEqual(_tree_snapshot(control_dir), snapshot)
        self.assertEqual(repeated_client.copy_calls, [])
        self.assertFalse((control_dir / "history").exists())

    def test_baseline_only_mirror_and_capture_binding(self):
        client = ReadOnlyFakeClient(self.baseline_objects)
        receipt = mirror_published_controls(client, REMOTE, self.root / "baseline-controls")
        self.assertEqual(receipt["generation_id"], BASELINE_ID)
        self.assertEqual(receipt["baseline_generation_id"], BASELINE_ID)
        self.assertEqual(receipt["control_count"], 2)

        changed = dict(self.baseline_objects)
        latest_path = f"{REMOTE}/latest.json"
        latest = json.loads(changed[latest_path])
        latest["captured_at"] = "2026-10-03T00:00:00Z"
        changed[latest_path] = _compact(latest)
        destination = self.root / "mismatched-capture"
        old_latest = b"previous local latest"
        destination.mkdir(mode=0o700)
        (destination / "latest.json").write_bytes(old_latest)
        (destination / "latest.json").chmod(0o600)
        with self.assertRaises(publisher.PublishError) as caught:
            mirror_published_controls(ReadOnlyFakeClient(changed), REMOTE, destination)
        self.assertEqual(caught.exception.category, "LATEST_BASELINE_MISMATCH")
        self.assertEqual((destination / "latest.json").read_bytes(), old_latest)
        self.assertFalse((destination / "history").exists())

    def test_latest_is_pinned_once_while_remote_advances(self):
        client = ReadOnlyFakeClient(self.second_objects)
        latest_path = f"{REMOTE}/latest.json"
        first_latest = self.first_objects[latest_path]
        second_latest = self.second_objects[latest_path]
        client.advance_after_bytes_path = latest_path
        client.advance_to = first_latest
        receipt = mirror_published_controls(client, REMOTE, self.root / "pinned-controls")
        self.assertEqual(receipt["generation_id"], SECOND_ID)
        self.assertEqual(receipt["pinned_latest_sha256"], _sha(second_latest))
        self.assertEqual((self.root / "pinned-controls" / "latest.json").read_bytes(), second_latest)
        self.assertEqual(client.objects[latest_path], first_latest)
        self.assertEqual(client.stat_calls.count(latest_path), 1)
        self.assertEqual(client.readback_calls.count(latest_path), 1)
        self.assertEqual(client.readback_bytes_calls.count(latest_path), 1)

    def test_replacing_latest_keeps_exact_previous_bytes_in_history(self):
        control_dir = self.root / "advancing-controls"
        first_client = ReadOnlyFakeClient(self.first_objects)
        mirror_published_controls(first_client, REMOTE, control_dir)
        first_latest = self.first_objects[f"{REMOTE}/latest.json"]
        second_latest = self.second_objects[f"{REMOTE}/latest.json"]

        mirror_published_controls(ReadOnlyFakeClient(self.second_objects), REMOTE, control_dir)

        self.assertEqual((control_dir / "latest.json").read_bytes(), second_latest)
        history_files = list((control_dir / "history").glob("*.json"))
        self.assertEqual(len(history_files), 1)
        self.assertEqual(history_files[0].read_bytes(), first_latest)
        self.assertEqual(history_files[0].stat().st_mode & 0o777, 0o600)

    def test_remote_readback_change_preserves_old_latest_and_history(self):
        control_dir = self.root / "failed-controls"
        control_dir.mkdir(mode=0o700)
        old_latest = b"old-latest-must-remain"
        (control_dir / "latest.json").write_bytes(old_latest)
        (control_dir / "latest.json").chmod(0o600)
        history = control_dir / "history"
        history.mkdir(mode=0o700)
        history_sentinel = history / "prior.json"
        history_sentinel.write_bytes(b"prior-history-must-remain")
        history_sentinel.chmod(0o600)
        before = _tree_snapshot(control_dir)
        client = ReadOnlyFakeClient(self.second_objects)
        client.bad_bytes_path = f"{REMOTE}/latest.json"

        with self.assertRaises(publisher.PublishError) as caught:
            mirror_published_controls(client, REMOTE, control_dir)

        self.assertEqual(caught.exception.category, "REMOTE_CONTROL_READBACK_MISMATCH")
        self.assertEqual(_tree_snapshot(control_dir), before)
        self.assertEqual(client.copy_calls, [])

    def test_duplicate_nonstandard_or_noninteger_version_json_is_rejected(self):
        cases = (
            (b'{"version":1,"version":1}', "REMOTE_CONTROL_JSON_INVALID"),
            (b'{"version":1,"invalid":NaN}', "REMOTE_CONTROL_JSON_INVALID"),
            (b'{"version":true}', "REMOTE_CONTROL_VERSION_INVALID"),
        )
        for raw, category in cases:
            with self.subTest(category=category, raw=raw[:20]):
                objects = dict(self.second_objects)
                objects[f"{REMOTE}/latest.json"] = raw
                client = ReadOnlyFakeClient(objects)
                destination = self.root / f"strict-{len(list(self.root.iterdir()))}"
                with self.assertRaises(publisher.PublishError) as caught:
                    mirror_published_controls(client, REMOTE, destination)
                self.assertEqual(caught.exception.category, category)
                self.assertFalse((destination / "latest.json").exists())

    def test_overlimit_control_count_and_aggregate_fail_before_install(self):
        latest_size = len(self.second_objects[f"{REMOTE}/latest.json"])
        with patch("mirror_published_controls.MAX_CONTROL_BYTES", latest_size - 1):
            client = ReadOnlyFakeClient(self.second_objects)
            with self.assertRaises(publisher.PublishError) as too_large:
                mirror_published_controls(client, REMOTE, self.root / "single-overlimit")
            self.assertEqual(too_large.exception.category, "REMOTE_CONTROL_METADATA_INVALID")
            self.assertEqual(client.readback_calls, [])

        with patch("mirror_published_controls.MAX_CONTROL_OBJECTS", 1):
            client = ReadOnlyFakeClient(self.second_objects)
            with self.assertRaises(publisher.PublishError) as too_many:
                mirror_published_controls(client, REMOTE, self.root / "count-overlimit")
            self.assertEqual(too_many.exception.category, "CONTROL_OBJECT_LIMIT")
            self.assertFalse((self.root / "count-overlimit" / "latest.json").exists())

        with patch("mirror_published_controls.MAX_TOTAL_CONTROL_BYTES", latest_size + 1):
            client = ReadOnlyFakeClient(self.second_objects)
            with self.assertRaises(publisher.PublishError) as aggregate:
                mirror_published_controls(client, REMOTE, self.root / "total-overlimit")
            self.assertEqual(aggregate.exception.category, "CONTROL_TOTAL_BYTE_LIMIT")
            self.assertEqual(client.readback_calls, [f"{REMOTE}/latest.json"])

    def test_allowlisted_path_adapter_refuses_data_and_traversal_paths(self):
        cache = _RemoteControlCache(ReadOnlyFakeClient(self.second_objects), REMOTE)
        invalid_paths = (
            f"{REMOTE}/source.sqlite3",
            f"{REMOTE}/xlsx-full/generations/{BASELINE_ID}/index.json",
            f"{REMOTE}/generations/{BASELINE_ID}/../index.json",
            f"{REMOTE}/deltas/generations/../../latest.json",
            f"{REMOTE}/deltas/generations/{SECOND_ID}/changes.sqlite3",
        )
        for remote_path in invalid_paths:
            with self.subTest(remote_path=remote_path.rsplit("/", 1)[-1]):
                with self.assertRaises(publisher.PublishError) as caught:
                    cache.relative(remote_path)
                self.assertEqual(caught.exception.category, "CONTROL_PATH_INVALID")

    def test_chain_order_and_index_binding_mismatches_are_rejected(self):
        reversed_objects = dict(self.second_objects)
        latest_path = f"{REMOTE}/latest.json"
        latest = json.loads(reversed_objects[latest_path])
        latest["delta_chain"] = list(reversed(latest["delta_chain"]))
        reversed_objects[latest_path] = _compact(latest)
        with self.assertRaises(publisher.PublishError):
            mirror_published_controls(ReadOnlyFakeClient(reversed_objects), REMOTE,
                                      self.root / "reversed-chain")
        self.assertFalse((self.root / "reversed-chain" / "latest.json").exists())

        changed_index_objects = dict(self.second_objects)
        index_path = f"{REMOTE}/deltas/generations/{FIRST_ID}/index.json"
        index = json.loads(changed_index_objects[index_path])
        index["source_row_counts"]["records"] += 1
        changed_index_objects[index_path] = _compact(index)
        with self.assertRaises(publisher.PublishError) as caught:
            mirror_published_controls(ReadOnlyFakeClient(changed_index_objects), REMOTE,
                                      self.root / "index-mismatch")
        self.assertIn(caught.exception.category,
                      {"DELTA_INDEX_READBACK_MISMATCH", "DELTA_INDEX_SCHEMA_INVALID"})
        self.assertFalse((self.root / "index-mismatch" / "latest.json").exists())

    def test_delta_plan_mismatch_is_rejected_without_install(self):
        objects = dict(self.second_objects)
        plan_path = f"{REMOTE}/deltas/generations/{FIRST_ID}/delta-plan.json"
        plan = json.loads(objects[plan_path])
        plan["metadata"]["kind"] = "tampered"
        objects[plan_path] = _compact(plan)
        with self.assertRaises(publisher.PublishError):
            mirror_published_controls(ReadOnlyFakeClient(objects), REMOTE,
                                      self.root / "plan-mismatch")
        self.assertFalse((self.root / "plan-mismatch" / "latest.json").exists())

    def test_conflicting_local_immutable_control_preserves_latest_and_history(self):
        control_dir = self.root / "conflicting-controls"
        control_dir.mkdir(mode=0o700)
        gen_dir = control_dir / "generations"
        gen_dir.mkdir(mode=0o700)
        base_dir = gen_dir / BASELINE_ID
        base_dir.mkdir(mode=0o700)
        wrong_index = base_dir / "index.json"
        wrong_index.write_bytes(b"conflicting immutable control")
        wrong_index.chmod(0o600)
        old_latest = b"previous-latest"
        latest_path = control_dir / "latest.json"
        latest_path.write_bytes(old_latest)
        latest_path.chmod(0o600)
        history = control_dir / "history"
        history.mkdir(mode=0o700)
        history_path = history / "retained.json"
        history_path.write_bytes(b"previous-history")
        history_path.chmod(0o600)
        before = _tree_snapshot(control_dir)

        with self.assertRaises(publisher.PublishError) as caught:
            mirror_published_controls(ReadOnlyFakeClient(self.second_objects), REMOTE, control_dir)

        self.assertEqual(caught.exception.category, "LOCAL_IMMUTABLE_CONTROL_CONFLICT")
        self.assertEqual(_tree_snapshot(control_dir), before)

    def test_new_nested_control_root_components_are_private_and_existing_modes_unchanged(self):
        existing_parent = self.root / "existing-parent"
        existing_parent.mkdir(mode=0o700)
        existing_parent.chmod(0o755)
        existing_inner = existing_parent / "existing-inner"
        existing_inner.mkdir(mode=0o700)
        existing_inner.chmod(0o751)
        before_modes = {
            self.root: self.root.stat().st_mode & 0o777,
            existing_parent: existing_parent.stat().st_mode & 0o777,
            existing_inner: existing_inner.stat().st_mode & 0o777,
        }
        control_dir = existing_inner / "created-one" / "created-two" / "controls"

        receipt = mirror_published_controls(
            ReadOnlyFakeClient(self.baseline_objects), REMOTE, control_dir
        )

        self.assertEqual(receipt["generation_id"], BASELINE_ID)
        self.assertEqual(
            {path: path.stat().st_mode & 0o777 for path in before_modes}, before_modes
        )
        for path in (existing_inner / "created-one",
                     existing_inner / "created-one" / "created-two", control_dir):
            self.assertEqual(path.stat().st_mode & 0o777, 0o700)

    def _assert_later_install_fault_preserves_latest_and_history(self, action):
        control_dir = self.root / f"fault-{action}-controls"
        control_dir.mkdir(mode=0o700)
        old_latest = b"prior-latest-remains-byte-exact"
        latest_path = control_dir / "latest.json"
        latest_path.write_bytes(old_latest)
        latest_path.chmod(0o600)
        history = control_dir / "history"
        history.mkdir(mode=0o700)
        history_path = history / "prior.json"
        history_path.write_bytes(b"prior-history-remains-byte-exact")
        history_path.chmod(0o600)
        before_history = {path.name: path.read_bytes() for path in history.iterdir()}

        real_install = mirror_module._write_immutable_control
        installed = []
        fault_injected = False

        def install_then_fault(path, raw):
            nonlocal fault_injected
            real_install(path, raw)
            installed.append(path)
            if len(installed) == 2 and not fault_injected:
                first = installed[0]
                if action == "modify":
                    first.write_bytes(b"changed-after-install")
                    first.chmod(0o600)
                else:
                    first.unlink()
                fault_injected = True

        with patch.object(mirror_module, "_write_immutable_control",
                          side_effect=install_then_fault):
            with self.assertRaises(publisher.PublishError) as caught:
                mirror_published_controls(
                    ReadOnlyFakeClient(self.second_objects), REMOTE, control_dir
                )

        self.assertEqual(caught.exception.category, "LOCAL_IMMUTABLE_CONTROL_CONFLICT")
        self.assertEqual(latest_path.read_bytes(), old_latest)
        self.assertEqual(
            {path.name: path.read_bytes() for path in history.iterdir()}, before_history
        )
        self.assertTrue(fault_injected)
        self.assertGreaterEqual(len(installed), 2)

    def test_mutated_earlier_immutable_control_is_rechecked_before_latest(self):
        self._assert_later_install_fault_preserves_latest_and_history("modify")

    def test_deleted_earlier_immutable_control_is_rechecked_before_latest(self):
        self._assert_later_install_fault_preserves_latest_and_history("delete")

    def test_control_root_symlink_and_directory_binding_failure_refuse(self):
        target = self.root / "target"
        target.mkdir(mode=0o700)
        link = self.root / "control-link"
        link.symlink_to(target, target_is_directory=True)
        with self.assertRaises(publisher.PublishError):
            mirror_published_controls(ReadOnlyFakeClient(self.second_objects), REMOTE, link)

        client = ReadOnlyFakeClient(self.second_objects)
        client.fail_verify = True
        destination = self.root / "binding-failure"
        with self.assertRaises(publisher.PublishError) as caught:
            mirror_published_controls(client, REMOTE, destination)
        self.assertEqual(caught.exception.category, "REMOTE_DIRECTORY_BINDING_CHANGED")
        self.assertFalse((destination / "latest.json").exists())

    def test_baseline_index_capture_fields_match_and_cli_emits_only_receipt(self):
        client = ReadOnlyFakeClient(self.baseline_objects)
        output = io.StringIO()
        with patch.object(publisher, "Rclone", return_value=client), contextlib.redirect_stdout(output):
            code = main([
                "--remote", REMOTE,
                "--control-dir", str(self.root / "cli-controls"),
                "--rclone-bin", "fake-rclone",
            ])
        self.assertEqual(code, 0)
        receipt = json.loads(output.getvalue())
        self.assertEqual(receipt["generation_id"], BASELINE_ID)
        self.assertEqual(receipt["baseline_generation_id"], BASELINE_ID)
        self.assertEqual(receipt["control_count"], 2)
        self.assertFalse(receipt["all_remote_artifacts_verified"])
        self.assertFalse(receipt["realtime_synchronized"])

    def test_cli_error_sanitization_and_failure_exit_codes(self):
        client = ReadOnlyFakeClient({})
        stdout_buf = io.StringIO()
        stderr_buf = io.StringIO()
        with patch.object(publisher, "Rclone", return_value=client), \
             contextlib.redirect_stdout(stdout_buf), \
             contextlib.redirect_stderr(stderr_buf):
            code = main([
                "--remote", REMOTE,
                "--control-dir", str(self.root / "cli-error-controls"),
                "--rclone-bin", "fake-rclone",
            ])
        self.assertEqual(code, 1)
        self.assertEqual(stdout_buf.getvalue(), "")
        stderr_val = stderr_buf.getvalue().strip()
        error_json = json.loads(stderr_val)
        self.assertEqual(error_json, {"category": "REMOTE_OBJECT_MISSING", "status": "error"})
        self.assertNotIn("fake-rclone", stderr_val)
        self.assertNotIn(REMOTE, stderr_val)
        self.assertNotIn(str(self.root), stderr_val)
        self.assertNotIn("Traceback", stderr_val)

    def test_cli_relative_control_dir_succeeds(self):
        client = ReadOnlyFakeClient(self.baseline_objects)
        output = io.StringIO()
        rel_dir_name = "test-cli-rel-controls"
        rel_path = self.root / rel_dir_name
        with patch.object(publisher, "Rclone", return_value=client), \
             patch("pathlib.Path.cwd", return_value=self.root), \
             contextlib.redirect_stdout(output):
            code = main([
                "--remote", REMOTE,
                "--control-dir", rel_dir_name,
                "--rclone-bin", "fake-rclone",
            ])
        self.assertEqual(code, 0)
        self.assertTrue((rel_path / "latest.json").exists())
        self.assertEqual(os.stat(rel_path).st_mode & 0o777, 0o700)

    def test_lock_busy_rejection(self):
        control_dir = self.root / "locked-controls"
        control_dir.mkdir(mode=0o700)
        lock_path = control_dir / ".published-controls.lock"
        fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            client = ReadOnlyFakeClient(self.baseline_objects)
            with self.assertRaises(publisher.PublishError) as caught:
                mirror_published_controls(client, REMOTE, control_dir)
            self.assertEqual(caught.exception.category, "CONTROL_LOCK_BUSY")
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    def test_remote_readback_change_in_delta_chain_preserves_old_latest(self):
        control_dir = self.root / "delta-corrupted-controls"
        control_dir.mkdir(mode=0o700)
        old_latest = b"old-latest-must-remain"
        (control_dir / "latest.json").write_bytes(old_latest)
        (control_dir / "latest.json").chmod(0o600)
        before = _tree_snapshot(control_dir)

        client = ReadOnlyFakeClient(self.second_objects)
        client.bad_bytes_path = f"{REMOTE}/deltas/generations/{FIRST_ID}/index.json"

        with self.assertRaises(publisher.PublishError) as caught:
            mirror_published_controls(client, REMOTE, control_dir)

        self.assertEqual(caught.exception.category, "REMOTE_CONTROL_READBACK_MISMATCH")
        self.assertEqual(_tree_snapshot(control_dir), before)
        self.assertEqual(client.copy_calls, [])

    def test_published_sqlite_reader_coexistence_with_mirrored_controls(self):
        control_dir = self.root / "reader-coexist-controls"
        client = ReadOnlyFakeClient(self.second_objects)
        receipt, _, _ = self._mirror(control_dir=control_dir, client=client)
        self.assertEqual(receipt["generation_id"], SECOND_ID)
        self.assertEqual(receipt["baseline_generation_id"], BASELINE_ID)

        dummy_shards = self.root / "dummy-shards"
        dummy_shards.mkdir(mode=0o700)
        dummy_deltas = self.root / "dummy-deltas"
        dummy_deltas.mkdir(mode=0o700)

        reader = PublishedSQLiteReader(control_dir, dummy_shards, dummy_deltas)
        reader._pin_controls()
        reader._validate_publisher_controls()
        self.assertEqual(reader._generation_id, SECOND_ID)
        self.assertEqual(reader._baseline_generation_id, BASELINE_ID)
        self.assertEqual(reader._pinned_latest_sha256, receipt["pinned_latest_sha256"])


if __name__ == "__main__":
    unittest.main()
