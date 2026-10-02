import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from scripts import full_data_baseline_worker as worker


GENERATION_ID = "20261002T120000Z-a1b2c3d4"
OLD_GENERATION_ID = "20261001T120000Z-a1b2c3d4"
REMOTE = "archive:database"


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True) + "\n", encoding="utf-8")


class BaselineWorkerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir=Path.cwd())
        self.root = Path(self.temp.name)
        self.work = self.root / "work"
        self.work.mkdir()
        self.generation = self.work / GENERATION_ID
        self.snapshot = self.root / "snapshot.sqlite3"
        self.snapshot_bytes = b"immutable synthetic sqlite bytes"
        self.snapshot.write_bytes(self.snapshot_bytes)
        self.source_sha = _sha(self.snapshot_bytes)
        self.source_manifest = self.root / "source-manifest.json"
        _write_json(
            self.source_manifest,
            {
                "storage": "plaintext",
                "encryption": None,
                "raw_snapshot": {
                    "basename": self.snapshot.name,
                    "bytes": len(self.snapshot_bytes),
                    "sha256": self.source_sha,
                    "quick_check": "ok",
                },
                "verification": {"sha256_match": True, "quick_check": "ok"},
            },
        )
        self.state = self.root / "private-state"
        self._make_ready_artifacts()

    def tearDown(self):
        self.temp.cleanup()

    def _make_ready_artifacts(self):
        self.generation.mkdir(parents=True, exist_ok=True)
        _write_json(
            self.generation / "slices/manifest.json",
            {
                "version": 2,
                "role": "lossless_sqlite_shards",
                "snapshot_identifier": GENERATION_ID,
                "source_sha256": self.source_sha,
                "source_schema_sha256": "b" * 64,
            },
        )
        _write_json(
            self.generation / "slices/verification.json",
            {
                "status": "verified",
                "snapshot_identifier": GENERATION_ID,
                "source_sha256": self.source_sha,
                "source_schema_sha256": "b" * 64,
                "table_count": 1,
                "row_counts": {"records": 1},
                "coverage": {
                    "all_tables": True,
                    "all_rows": True,
                    "all_columns": True,
                    "all_values": True,
                    "external_values": True,
                },
            },
        )
        _write_json(
            self.generation / "slices/selectors-verification.json",
            {
                "status": "verified",
                "snapshot_identifier": GENERATION_ID,
                "source_sha256": self.source_sha,
                "all_shared_files_reachable": True,
                "all_mode_matches": True,
                "all_rule_matches": True,
            },
        )
        _write_json(
            self.generation / "xlsx/manifest.json",
            {
                "status": "verified",
                "snapshot_identifier": GENERATION_ID,
                "snapshot_sha256": self.source_sha,
                "source": {
                    "path": str(self.generation / "source.sqlite3"),
                    "bytes": len(self.snapshot_bytes),
                    "sha256": self.source_sha,
                },
            },
        )
        _write_json(
            self.generation / "xlsx/index.json",
            {
                "status": "verified",
                "verification_status": "verified",
                "snapshot_identifier": GENERATION_ID,
                "snapshot_sha256": self.source_sha,
                "row_identity_format": "source_rowid_column_v1",
                "source": {
                    "path": str(self.generation / "source.sqlite3"),
                    "bytes": len(self.snapshot_bytes),
                    "sha256": self.source_sha,
                },
                "pieces": [{"name": "records.xlsx", "bytes": 1, "sha256": "f" * 64}],
                "counts": {
                    "exported_tables": 1,
                    "exported_rows": 1,
                    "exported_cells": 2,
                    "chunks": 0,
                    "pieces": 1,
                },
            },
        )
        _write_json(
            self.generation / "xlsx/verification.json",
            {
                "status": "verified",
                "snapshot_identifier": GENERATION_ID,
                "snapshot_sha256": self.source_sha,
                "source_rowids_verified": True,
                "source": {
                    "path": str(self.generation / "source.sqlite3"),
                    "bytes": len(self.snapshot_bytes),
                    "sha256": self.source_sha,
                },
            },
        )

    def _details(self):
        files = [
            {
                "local": "slices/verification.json",
                "remote": "slices/generations/test/verification.json",
                "bytes": 20,
                "sha256": "c" * 64,
            }
        ]
        return {
            "source": {"bytes": len(self.snapshot_bytes), "sha256": self.source_sha},
            "plan_sha256": "d" * 64,
            "plan_bytes": 123,
            "source_schema_sha256": "b" * 64,
            "files": files,
        }

    def _plan(self):
        return {
            "generation_id": GENERATION_ID,
            "source": {"bytes": len(self.snapshot_bytes), "sha256": self.source_sha},
            "sqlite_verification": {"coverage": {"all_values": True}},
            "xlsx_verification": {"source_rowids_verified": True},
        }

    def _fake_prepare(self, snapshot, source_manifest, generation_root, _generation_id):
        linked = generation_root / "source.sqlite3"
        if not linked.exists():
            os.link(snapshot, linked)
        copied_manifest = generation_root / "source-manifest.json"
        if not copied_manifest.exists():
            copied_manifest.write_bytes(Path(source_manifest).read_bytes())
        plan_path = generation_root / "generation-plan.json"
        if not plan_path.exists():
            _write_json(plan_path, self._plan())

    def _cycle(self):
        return worker.baseline_cycle(
            self.snapshot,
            self.source_manifest,
            self.work,
            GENERATION_ID,
            REMOTE,
            self.state,
            "fake-rclone",
        )

    def test_partial_artifacts_wait_without_mutating_generation_or_publishing(self):
        (self.generation / "xlsx/verification.json").unlink()
        before = sorted(str(path.relative_to(self.generation)) for path in self.generation.rglob("*"))
        with mock.patch.object(worker.preparer, "prepare_generation") as prepare, mock.patch.object(
            worker, "_validated_plan", return_value=(self._plan(), self._details())
        ), mock.patch.object(worker.publisher, "publish_generation", return_value={"status": "complete"}) as publish:
            result = self._cycle()
        after = sorted(str(path.relative_to(self.generation)) for path in self.generation.rglob("*"))
        self.assertEqual(result["status"], "pending_baseline_artifacts")
        self.assertEqual(result["category"], "ARTIFACTS_PENDING")
        self.assertEqual(before, after)
        self.assertFalse((self.state / 'baseline-worker-last-failure.json').exists())
        prepare.assert_not_called()
        publish.assert_not_called()

    def test_mixed_generation_artifact_is_rejected_without_prepare_or_publish(self):
        index_path = self.generation / "xlsx/index.json"
        index = json.loads(index_path.read_text())
        index["snapshot_identifier"] = OLD_GENERATION_ID
        _write_json(index_path, index)
        with mock.patch.object(worker.preparer, "prepare_generation") as prepare, mock.patch.object(
            worker.publisher, "publish_generation"
        ) as publish:
            result = self._cycle()
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["category"], "ARTIFACT_BINDING_MISMATCH")
        prepare.assert_not_called()
        publish.assert_not_called()

    def test_unverified_rowids_wait_without_prepare_or_publish(self):
        receipt_path = self.generation / "xlsx/verification.json"
        receipt = json.loads(receipt_path.read_text())
        receipt["source_rowids_verified"] = False
        _write_json(receipt_path, receipt)
        with mock.patch.object(worker.preparer, "prepare_generation") as prepare, mock.patch.object(
            worker.publisher, "publish_generation"
        ) as publish:
            result = self._cycle()
        self.assertEqual(result["status"], "pending_baseline_artifacts")
        self.assertEqual(result["category"], "ROWID_PROOF_PENDING")
        prepare.assert_not_called()
        publish.assert_not_called()

    def test_verified_cycle_records_receipt_and_repeat_never_calls_publisher(self):
        with mock.patch.object(worker.preparer, "prepare_generation", side_effect=self._fake_prepare) as prepare, mock.patch.object(
            worker, "_plan_details_from_disk", return_value=(self._plan(), self._details())
        ), mock.patch.object(
            worker, "_validated_plan", return_value=(self._plan(), self._details())
        ), mock.patch.object(
            worker.publisher,
            "publish_generation",
            return_value={"status": "complete"},
        ) as publish:
            first = self._cycle()
            receipt_path = worker._success_path(self.state, GENERATION_ID)
            receipt_before = receipt_path.read_bytes()
            second = self._cycle()

        self.assertEqual(first["status"], "verified")
        self.assertEqual(first["scope"], "immutable_full_baseline")
        self.assertIs(first["realtime_synchronized"], False)
        self.assertEqual(second["status"], "already_published")
        self.assertIs(second["realtime_synchronized"], False)
        self.assertEqual(receipt_path.read_bytes(), receipt_before)
        self.assertEqual(self.snapshot.read_bytes(), self.snapshot_bytes)
        prepare.assert_called_once()
        publish.assert_called_once()

    def test_publish_retry_reuses_plan_without_running_prepare_again(self):
        with mock.patch.object(worker.preparer, "prepare_generation", side_effect=self._fake_prepare) as prepare, mock.patch.object(
            worker, "_plan_details_from_disk", return_value=(self._plan(), self._details())
        ), mock.patch.object(
            worker, "_validated_plan", return_value=(self._plan(), self._details())
        ), mock.patch.object(
            worker.publisher,
            "publish_generation",
            side_effect=[worker.publisher.PublishError("REMOTE_UPLOAD_FAILED"), {"status": "complete"}],
        ) as publish:
            first = self._cycle()
            second = self._cycle()
        self.assertEqual(first["status"], "failed")
        self.assertEqual(first["category"], "REMOTE_UPLOAD_FAILED")
        self.assertEqual(second["status"], "verified")
        prepare.assert_called_once()
        publish.assert_has_calls([
            mock.call(self.generation, GENERATION_ID, REMOTE, self.state, rclone_bin="fake-rclone"),
            mock.call(self.generation, GENERATION_ID, REMOTE, self.state, rclone_bin="fake-rclone"),
        ])

    def test_later_publisher_generation_does_not_roll_baseline_latest_back(self):
        success = worker._make_success_receipt(
            GENERATION_ID, REMOTE, _sha(self.source_manifest.read_bytes()), self._details()
        )
        _write_json(worker._success_path(self.state, GENERATION_ID), success)
        os.chmod(self.state, 0o700)
        self._fake_prepare(self.snapshot, self.source_manifest, self.generation, GENERATION_ID)
        with mock.patch.object(worker, "_validated_plan", return_value=(self._plan(), self._details())), mock.patch.object(
            worker.publisher,
            "_current_state",
            return_value=(self.state / "current_state.json", {
                "version": 1,
                "last_attempt": "2026-10-02T12:00:00Z",
                "last_success": "2026-10-02T12:00:00Z",
                "last_failure": None,
                "phase": "complete",
                "generation_id": OLD_GENERATION_ID,
            }),
        ), mock.patch.object(worker.publisher, "publish_generation") as publish:
            result = self._cycle()
        self.assertEqual(result["status"], "already_published")
        publish.assert_not_called()

    def test_existing_publisher_state_is_recognized_only_when_remote_latest_matches(self):
        self.state.mkdir(mode=0o700)
        self._fake_prepare(self.snapshot, self.source_manifest, self.generation, GENERATION_ID)
        payload = {
            "version": 1,
            "last_success": "2026-10-02T12:00:00Z",
            "generation_id": GENERATION_ID,
            "index_sha256": "e" * 64,
            "captured_at": None,
            "captured_at_kind": "pinned_read_transaction",
        }
        latest_bytes = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode() + b"\n"
        latest_sha = _sha(latest_bytes)
        current = {
            "version": 1,
            "last_attempt": "2026-10-02T12:00:00Z",
            "last_success": "2026-10-02T12:00:00Z",
            "last_failure": None,
            "phase": "complete",
            "generation_id": GENERATION_ID,
        }
        progress = {
            "generation_id": GENERATION_ID,
            "latest_payload": payload,
            "latest_payload_sha256": latest_sha,
        }
        fake_client = mock.Mock()
        fake_client.stat.return_value = {"Size": len(latest_bytes)}
        fake_client.readback.return_value = (len(latest_bytes), latest_sha)
        with mock.patch.object(worker.publisher, "_current_state", return_value=(self.state / "current_state.json", current)), mock.patch.object(
            worker.publisher, "_read_plan", return_value=(self._plan(), 100, "d" * 64)
        ), mock.patch.object(worker.publisher, "_load_progress_state", return_value=progress), mock.patch.object(
            worker.publisher, "Rclone", return_value=fake_client
        ), mock.patch.object(worker, "_validated_plan", return_value=(self._plan(), self._details())), mock.patch.object(
            worker.preparer, "prepare_generation"
        ) as prepare, mock.patch.object(worker.publisher, "publish_generation") as publish:
            result = self._cycle()
        self.assertEqual(result["status"], "already_published")
        prepare.assert_not_called()
        publish.assert_not_called()
        fake_client.stat.assert_called_once_with("archive:database/latest.json")
        fake_client.readback.assert_called_once_with("archive:database/latest.json")

    def test_publisher_state_from_newer_generation_blocks_stale_baseline(self):
        self.state.mkdir(mode=0o700)
        current = {
            "version": 1,
            "last_attempt": "2026-10-02T12:00:00Z",
            "last_success": "2026-10-02T12:00:00Z",
            "last_failure": None,
            "phase": "complete",
            "generation_id": OLD_GENERATION_ID,
        }
        with mock.patch.object(worker.publisher, "_current_state", return_value=(self.state / "current_state.json", current)), mock.patch.object(
            worker.preparer, "prepare_generation"
        ) as prepare, mock.patch.object(worker.publisher, "publish_generation") as publish:
            result = self._cycle()
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["category"], "REMOTE_LATEST_ADVANCED")
        prepare.assert_not_called()
        publish.assert_not_called()

    def test_remote_failure_keeps_previous_success_record(self):
        previous_path = worker._success_path(self.state, OLD_GENERATION_ID)
        previous_bytes = b'{"status":"verified","scope":"immutable_full_baseline"}\n'
        previous_path.parent.mkdir(mode=0o700)
        previous_path.write_bytes(previous_bytes)
        with mock.patch.object(worker.preparer, "prepare_generation", side_effect=self._fake_prepare), mock.patch.object(
            worker, "_plan_details_from_disk", return_value=(self._plan(), self._details())
        ), mock.patch.object(
            worker.publisher,
            "publish_generation",
            side_effect=worker.publisher.PublishError("REMOTE_UPLOAD_FAILED"),
        ):
            result = self._cycle()
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["category"], "REMOTE_UPLOAD_FAILED")
        self.assertEqual(previous_path.read_bytes(), previous_bytes)
        failure = json.loads((self.state / "baseline-worker-last-failure.json").read_text())
        self.assertEqual(failure["category"], "REMOTE_UPLOAD_FAILED")
        self.assertNotIn("exception", failure)

    def test_state_directory_is_private_and_symlinks_are_rejected(self):
        with mock.patch.object(worker.preparer, "prepare_generation", side_effect=self._fake_prepare) as prepare, mock.patch.object(
            worker, "_plan_details_from_disk", return_value=(self._plan(), self._details())
        ), mock.patch.object(
            worker.publisher, "publish_generation", return_value={"status": "complete"}
        ) as publish:
            result = self._cycle()
        self.assertEqual(result["status"], "verified")
        self.assertEqual(os.stat(self.state).st_mode & 0o777, 0o700)
        self.assertEqual(os.stat(self.state).st_uid, os.getuid())
        prepare.assert_called_once()
        publish.assert_called_once()

        other_state = self.root / "real-state"
        other_state.mkdir(mode=0o700)
        link = self.root / "state-link"
        link.symlink_to(other_state, target_is_directory=True)
        rejected = worker.baseline_cycle(
            self.snapshot, self.source_manifest, self.work, GENERATION_ID,
            REMOTE, link, "fake-rclone",
        )
        self.assertEqual(rejected["status"], "failed")
        self.assertEqual(rejected["category"], "PATH_SAFETY_ERROR")

    def test_watch_retries_pending_then_stops_after_success(self):
        results = [
            {"status": "pending_baseline_artifacts", "generation_id": GENERATION_ID},
            {"status": "pending_publication_lock", "generation_id": GENERATION_ID},
            {"status": "verified", "generation_id": GENERATION_ID,
             "scope": "immutable_full_baseline", "realtime_synchronized": False},
        ]
        with mock.patch.object(worker, "baseline_cycle", side_effect=results) as cycle, mock.patch.object(
            worker.time, "sleep"
        ) as sleep, mock.patch("sys.stdout"):
            exit_code = worker.main([
                "--snapshot", str(self.snapshot),
                "--source-manifest", str(self.source_manifest),
                "--work-dir", str(self.work),
                "--generation-id", GENERATION_ID,
                "--remote", REMOTE,
                "--state-dir", str(self.state),
                "--rclone-bin", "fake-rclone",
                "--watch", "--interval", "5",
            ])
        self.assertEqual(exit_code, 0)
        self.assertEqual(cycle.call_count, 3)
        self.assertEqual([call.args for call in sleep.call_args_list], [(5,), (5,)])

    def test_rejects_source_generation_overlap_before_prepare(self):
        nested_snapshot = self.generation / "source.sqlite3"
        nested_snapshot.write_bytes(self.snapshot_bytes)
        manifest = self.root / "nested-manifest.json"
        doc = json.loads(self.source_manifest.read_text())
        doc["raw_snapshot"]["basename"] = nested_snapshot.name
        _write_json(manifest, doc)
        with mock.patch.object(worker.preparer, "prepare_generation", side_effect=self._fake_prepare) as prepare, mock.patch.object(
            worker.publisher, "publish_generation"
        ) as publish:
            result = worker.baseline_cycle(
                nested_snapshot, manifest, self.work, GENERATION_ID, REMOTE,
                self.state, "fake-rclone",
            )
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["category"], "PATH_OVERLAP_ERROR")
        prepare.assert_not_called()
        publish.assert_not_called()

    def test_worker_singleton_lock_covers_prepare_and_publish(self):
        self.state.mkdir(mode=0o700)
        with worker._worker_lock(self.state), mock.patch.object(
            worker.preparer, "prepare_generation"
        ) as prepare, mock.patch.object(worker.publisher, "publish_generation") as publish:
            result = self._cycle()
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["category"], "WORKER_LOCK_BUSY")
        prepare.assert_not_called()
        publish.assert_not_called()

    def test_shared_publisher_lock_waits_without_recording_failure_or_preparing(self):
        self.state.mkdir(mode=0o700)
        with worker.publisher.PublishLock(self.state), mock.patch.object(
            worker.preparer, 'prepare_generation') as prepare, mock.patch.object(
            worker.publisher, 'publish_generation') as publish:
            result = self._cycle()
        self.assertEqual(result['status'], 'pending_publication_lock')
        self.assertFalse((self.state / 'baseline-worker-last-failure.json').exists())
        prepare.assert_not_called()
        publish.assert_not_called()

    def test_cli_does_not_accept_database_capture_argument(self):
        with self.assertRaises(SystemExit) as raised:
            worker.main(["--db", str(self.snapshot)])
        self.assertEqual(raised.exception.code, 2)


if __name__ == "__main__":
    unittest.main()
