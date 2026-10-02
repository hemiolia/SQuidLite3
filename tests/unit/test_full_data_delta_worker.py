import hashlib
import inspect
import json
import os
from pathlib import Path
import sqlite3
import signal
import sys
import tempfile
import threading
import time
import unittest
from datetime import timedelta
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src/python"))
sys.path.insert(0, str(ROOT / "scripts"))

import full_data_delta_worker as worker_module
import prepare_full_data_delta as preparer


BASE = "20261002T111654Z-b0ac6787"
G1 = "20261003T010001Z-11111111"
G2 = "20261003T010002Z-22222222"
G3 = "20261003T010003Z-33333333"
MANUAL = "20261003T010004Z-44444444"
_PREPARER_SUPPORTS_BASELINE_TOKEN = (
    callable(getattr(preparer, "verify_baseline_once", None))
    and "baseline_verification" in inspect.signature(preparer.prepare_delta).parameters
)


def sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


class FullDataDeltaWorkerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir=ROOT, prefix=".test-delta-worker-")
        self.root = Path(self.temp.name)
        self.source = self.root / "source.sqlite3"
        self.baseline = self.root / "baseline.sqlite3"
        self.manifest = self.root / "baseline-manifest.json"
        self.work = self.root / "generations"
        self.work.mkdir(mode=0o700)
        self.state = self.root / "private-state"
        self.connection = sqlite3.connect(self.source)
        self.connection.executescript(
            "CREATE TABLE records(value BLOB);"
            "CREATE TABLE empty_table(key TEXT PRIMARY KEY) WITHOUT ROWID;"
            "INSERT INTO records(rowid,value) VALUES(-7,'baseline');"
        )
        self.connection.commit()
        target = sqlite3.connect(self.baseline)
        self.connection.backup(target)
        target.close()
        snapshot_digest = preparer.file_digest(self.baseline)
        self.manifest.write_text(json.dumps({
            "storage": "plaintext",
            "encryption": None,
            "raw_snapshot": {
                "basename": self.baseline.name,
                "quick_check": "ok",
                **snapshot_digest,
            },
        }), encoding="utf-8")
        os.chmod(self.manifest, 0o600)
        from ikarchive.change_feed import install_change_feed
        from ikarchive.writer_guards import install_writer_guards
        install_change_feed(self.connection)
        self.connection.execute("PRAGMA recursive_triggers=ON")
        self.assertEqual(self.connection.execute("PRAGMA recursive_triggers").fetchone()[0], 1)
        install_writer_guards(self.connection)
        self.connection.commit()
        self.connection.execute("UPDATE records SET value=? WHERE rowid=-7", (b"initial\x00delta",))
        self.connection.commit()

    def tearDown(self):
        self.connection.close()
        self.temp.cleanup()

    def config(self, *, state=None, initial=None, interval=30):
        return worker_module.WorkerConfig(
            db=self.source,
            baseline=self.baseline,
            baseline_manifest=self.manifest,
            baseline_generation_id=BASE,
            work_dir=self.work,
            state_dir=state or self.state,
            remote="archive:database",
            rclone_bin="fake-rclone",
            initial_generation_id=initial,
            interval_seconds=interval,
        )

    def generation_ids(self, *values):
        items = iter(values)
        return lambda: next(items)

    def success_publisher(self, seen=None):
        def publish(generation_dir, remote, state_dir, *, rclone_bin):
            generation_dir = Path(generation_dir)
            plan_bytes = (generation_dir / "delta-plan.json").read_bytes()
            plan = json.loads(plan_bytes.decode("utf-8"))
            checkpoint = {
                "version": 1,
                "status": "complete",
                "scope": "immutable_lossless_delta_generation",
                "generation_id": plan["generation_id"],
                "plan_sha256": sha(plan_bytes),
                "baseline_generation_id": plan["baseline_generation_id"],
                "baseline_source_sha256": plan["baseline_source_sha256"],
                "parent_generation_id": plan["parent_generation_id"],
                "latest_generation_id": plan["generation_id"],
                "index_sha256": sha((plan["generation_id"] + "index").encode()),
                "latest_sha256": sha((plan["generation_id"] + "latest").encode()),
                "through_event_id": plan["through_event_id"],
                "last_success": "2026-10-03T01:30:00+00:00",
                "rclone_compare_and_swap": False,
            }
            worker_module._atomic_json(Path(state_dir) / worker_module.PUBLISHED_STATE_NAME, checkpoint)
            if seen is not None:
                seen.append(plan["generation_id"])
            return {
                "status": "complete",
                "scope": "immutable_lossless_delta_generation",
                "generation_id": plan["generation_id"],
                "checkpoint_advanced": True,
                "checkpoint": checkpoint,
            }
        return publish

    def make_worker(self, ids=(G1, G2, G3), *, publish=None, prepare=None, initial=None, state=None, probe=None):
        return worker_module.DeltaWorker(
            self.config(state=state, initial=initial),
            prepare_fn=prepare,
            publish_fn=publish or self.success_publisher(),
            generation_id_fn=self.generation_ids(*ids),
            probe_fn=probe,
        )

    def queued_ids(self, worker):
        return list(worker.status()["queue"])

    def test_once_builds_and_publishes_one_verified_generation_read_only(self):
        source_before = preparer.file_digest(self.source)
        worker = self.make_worker((G1,))

        result = worker.run_once()

        self.assertEqual(result["status"], "cycle_complete")
        self.assertEqual(result["preparation"]["status"], "prepared")
        self.assertEqual(result["publication"]["status"], "complete")
        self.assertEqual(result["publication"]["published_generation_id"], G1)
        self.assertEqual(self.queued_ids(worker), [])
        self.assertEqual(preparer.file_digest(self.source), source_before)
        prepared = json.loads(worker.prepared_checkpoint_path.read_text())
        published = json.loads(worker.published_checkpoint_path.read_text())
        plan = json.loads((self.work / G1 / "delta-plan.json").read_text())
        self.assertEqual(prepared["last_prepared_generation_id"], G1)
        self.assertEqual(prepared["chain_anchor"]["generation_id"], G1)
        self.assertEqual(plan["metadata"]["writer_guard_status"]["status"], "verified")
        self.assertIs(plan["metadata"]["all_writers_contract_enforced"], True)
        self.assertEqual(published["generation_id"], G1)
        state = json.loads(worker.worker_state_path.read_text())
        self.assertEqual(state["prepare"]["last_success_generation_id"], G1)
        self.assertEqual(state["publish"]["last_success_generation_id"], G1)
        expected_lag = (
            worker_module._parse_utc(worker_module._utc_now())
            - worker_module._parse_utc(plan["captured_at"])
        ).total_seconds()
        self.assertAlmostEqual(result["published_lag_seconds"], expected_lag, delta=0.1)
        self.assertEqual(result["queued_capture_lag_seconds"], 0.0)

    @unittest.skipUnless(
        _PREPARER_SUPPORTS_BASELINE_TOKEN,
        "prepare module has not exposed the runtime baseline token argument yet",
    )
    def test_default_preparer_verifies_baseline_once_and_reuses_runtime_token(self):
        real_verify = preparer.verify_baseline_once
        real_prepare = preparer.prepare_delta
        tokens = []

        def verify_once(*args, **kwargs):
            token = real_verify(*args, **kwargs)
            tokens.append(token)
            return token

        def pending(generation_dir, remote, state_dir, *, rclone_bin):
            plan = json.loads((Path(generation_dir) / "delta-plan.json").read_text())
            return {"status": "pending_baseline_publication", "generation_id": plan["generation_id"],
                    "checkpoint_advanced": False}

        source_before = preparer.file_digest(self.source)
        with mock.patch.object(preparer, "verify_baseline_once", side_effect=verify_once) as verify_spy:
            with mock.patch.object(preparer, "prepare_delta", wraps=real_prepare) as prepare_spy:
                prepare_spy.__signature__ = inspect.signature(real_prepare)
                worker = self.make_worker((G1, G2), publish=pending)
                first = worker.prepare_cycle()
                self.assertEqual(first["status"], "prepared")
                self.assertEqual(preparer.file_digest(self.source), source_before)
                self.connection.execute("UPDATE records SET value='token reuse' WHERE rowid=-7")
                self.connection.commit()
                source_before_second = preparer.file_digest(self.source)
                second = worker.prepare_cycle()
                self.assertEqual(second["status"], "prepared")
                self.assertEqual(preparer.file_digest(self.source), source_before_second)

                self.assertEqual(verify_spy.call_count, 1)
                self.assertEqual(len(tokens), 1)
                self.assertEqual(prepare_spy.call_count, 2)
                self.assertIs(prepare_spy.call_args_list[0].kwargs["baseline_verification"], tokens[0])
                self.assertIs(prepare_spy.call_args_list[1].kwargs["baseline_verification"], tokens[0])

    @unittest.skipUnless(
        _PREPARER_SUPPORTS_BASELINE_TOKEN,
        "prepare module has not exposed the runtime baseline token argument yet",
    )
    def test_runtime_baseline_token_rejects_changed_fingerprint_without_rehash(self):
        real_verify = preparer.verify_baseline_once
        with mock.patch.object(preparer, "verify_baseline_once", wraps=real_verify) as verify_spy:
            worker = self.make_worker((G1, G2))
            first = worker.prepare_cycle()
            self.assertEqual(first["status"], "prepared")
            self.assertEqual(verify_spy.call_count, 1)
            baseline_digest = preparer.file_digest(self.baseline)

            info = self.baseline.stat()
            os.utime(self.baseline, ns=(info.st_atime_ns, info.st_mtime_ns + 1_000_000_000))
            self.connection.execute("UPDATE records SET value='baseline became stale' WHERE rowid=-7")
            self.connection.commit()
            failed = worker.prepare_cycle()

        self.assertEqual(failed["status"], "failed")
        self.assertEqual(failed["category"], "BASELINE_VERIFICATION_STALE")
        self.assertEqual(verify_spy.call_count, 1)
        self.assertEqual(preparer.file_digest(self.baseline), baseline_digest)
        checkpoint = json.loads(worker.prepared_checkpoint_path.read_text())
        self.assertEqual(checkpoint["last_prepared_generation_id"], G1)
        self.assertEqual(self.queued_ids(worker), [G1])
        self.assertIsNone(checkpoint["active_generation"])
        self.assertFalse((self.work / G2).exists())

    def test_lightweight_probe_idles_when_event_schema_and_guard_are_unchanged(self):
        worker = self.make_worker((G1, G2))
        self.assertEqual(worker.prepare_cycle()["status"], "prepared")
        first = json.loads((self.work / G1 / "delta-plan.json").read_text())
        self.assertIs(first["metadata"]["requires_baseline_reconciliation"], False)
        self.assertEqual(first["metadata"]["writer_guard_status"]["status"], "verified")
        self.assertIs(first["metadata"]["all_writers_contract_enforced"], True)
        idle = worker.prepare_cycle()
        self.assertEqual(idle["status"], "idle")
        self.assertEqual(idle["generation_id"], G1)
        self.assertFalse((self.work / G2).exists())

        self.connection.execute("UPDATE records SET value='next' WHERE rowid=-7")
        self.connection.commit()
        changed = worker.prepare_cycle()
        self.assertEqual(changed["status"], "prepared")
        self.assertEqual(changed["generation_id"], G2)
        second = json.loads((self.work / G2 / "delta-plan.json").read_text())
        self.assertEqual(second["kind"], "change_feed")
        self.assertEqual(second["parent_generation_id"], G1)

    def test_missing_writer_guard_prevents_idle_and_forces_full_reconciliation(self):
        worker = self.make_worker((G1, G2))
        self.assertEqual(worker.prepare_cycle()["status"], "prepared")
        guard_name = self.connection.execute(
            "SELECT name FROM sqlite_master WHERE type='trigger' "
            "AND name GLOB 'ia_writer_guard_*' ORDER BY name LIMIT 1"
        ).fetchone()[0]
        self.connection.execute('DROP TRIGGER "' + guard_name.replace('"', '""') + '"')
        self.connection.commit()

        result = worker.prepare_cycle()

        self.assertEqual(result["status"], "prepared")
        self.assertEqual(result["generation_id"], G2)
        plan = json.loads((self.work / G2 / "delta-plan.json").read_text())
        self.assertEqual(plan["kind"], "baseline_reconciliation")
        self.assertTrue(plan["metadata"]["requires_baseline_reconciliation"])
        self.assertFalse(plan["metadata"]["all_writers_contract_enforced"])
        self.assertNotEqual(plan["metadata"]["writer_guard_status"]["status"], "verified")

    def test_published_lag_is_current_age_and_queued_gap_is_separate(self):
        worker = self.make_worker((G1, G2))
        worker.prepare_cycle()
        time.sleep(0.025)
        self.connection.execute("UPDATE records SET value='later capture' WHERE rowid=-7")
        self.connection.commit()
        worker.prepare_cycle()
        older = json.loads((self.work / G1 / "delta-plan.json").read_text())
        newest = json.loads((self.work / G2 / "delta-plan.json").read_text())
        published_capture = worker_module._parse_utc(older["captured_at"])
        newest_capture = worker_module._parse_utc(newest["captured_at"])
        self.assertGreater(newest_capture, published_capture)
        fixed_now = published_capture + timedelta(hours=1)

        with mock.patch.object(worker_module, "_utc_now", return_value=fixed_now.isoformat()):
            publication = worker.publish_cycle()
            self.assertEqual(publication["status"], "complete")
            self.assertAlmostEqual(worker._published_lag_seconds(), 3600.0, places=6)
            capture_gap = (newest_capture - published_capture).total_seconds()
            self.assertAlmostEqual(worker._queued_capture_lag_seconds(), capture_gap, places=6)
            first_state = json.loads(worker.worker_state_path.read_text())
            self.assertAlmostEqual(first_state["published_lag_seconds"], 3600.0, places=6)
            self.assertAlmostEqual(first_state["queued_capture_lag_seconds"], capture_gap, places=6)

            later_publication = worker.publish_cycle()
            self.assertEqual(later_publication["status"], "complete")
            expected_new_age = (fixed_now - newest_capture).total_seconds()
            self.assertAlmostEqual(worker._published_lag_seconds(), expected_new_age, places=6)
            self.assertEqual(worker._queued_capture_lag_seconds(), 0.0)

    def test_prepare_keeps_advancing_while_remote_baseline_is_pending(self):
        def pending(generation_dir, remote, state_dir, *, rclone_bin):
            plan = json.loads((Path(generation_dir) / "delta-plan.json").read_text())
            return {"status": "pending_baseline_publication", "generation_id": plan["generation_id"],
                    "checkpoint_advanced": False}

        worker = self.make_worker((G1, G2), publish=pending)
        first = worker.run_once()
        self.assertEqual(first["publication"]["status"], "pending_baseline_publication")
        self.connection.execute("UPDATE records SET value='while cloud pending' WHERE rowid=-7")
        self.connection.commit()
        second = worker.run_once()
        self.assertEqual(second["preparation"]["status"], "prepared")
        self.assertEqual(second["publication"]["status"], "pending_baseline_publication")
        self.assertEqual(self.queued_ids(worker), [G1, G2])
        self.assertFalse(worker.published_checkpoint_path.exists())

    def test_publish_lock_busy_leaves_prepared_and_published_checkpoints_unchanged(self):
        def busy(_generation_dir, _remote, _state_dir, *, rclone_bin):
            error = RuntimeError("synthetic publish lock busy")
            error.category = "LOCK_BUSY"
            raise error

        worker = self.make_worker((G1,), publish=busy)
        self.assertEqual(worker.prepare_cycle()["status"], "prepared")
        prepared_before = worker.prepared_checkpoint_path.read_bytes()

        result = worker.publish_cycle()

        self.assertEqual(result["status"], "pending_publish_lock")
        self.assertTrue(result["retry_pending"])
        self.assertEqual(worker.prepared_checkpoint_path.read_bytes(), prepared_before)
        self.assertFalse(worker.published_checkpoint_path.exists())
        self.assertEqual(self.queued_ids(worker), [G1])

    def test_queued_reset_waits_behind_older_prepared_generation(self):
        published_order = []
        worker = self.make_worker((G1, G2), publish=self.success_publisher(published_order))
        first = worker.prepare_cycle()
        self.assertEqual(first["generation_id"], G1)
        self.connection.execute("CREATE TABLE schema_added(value TEXT)")
        self.connection.execute("INSERT INTO schema_added VALUES('complete schema')")
        self.connection.commit()
        second = worker.prepare_cycle()
        self.assertEqual(second["generation_id"], G2)
        reset_plan = json.loads((self.work / G2 / "delta-plan.json").read_text())
        self.assertTrue(reset_plan["replaces_delta_chain"])
        self.assertEqual(reset_plan["expected_previous_generation_id"], G1)
        self.assertEqual(reset_plan["supersedes_generation_id"], G1)
        self.assertEqual(self.queued_ids(worker), [G1, G2])

        one = worker.publish_cycle()
        self.assertEqual(one["generation_id"], G1)
        self.assertEqual(self.queued_ids(worker), [G2])
        two = worker.publish_cycle()
        self.assertEqual(two["generation_id"], G2)
        self.assertEqual(published_order, [G1, G2])
        self.assertEqual(self.queued_ids(worker), [])

    def test_manual_initial_generation_waits_then_adopts_verified_existing_plan(self):
        worker = self.make_worker((G1,), initial=MANUAL)
        waiting = worker.prepare_cycle()
        self.assertEqual(waiting["status"], "waiting_initial_reconciliation")
        self.assertFalse((self.work / MANUAL).exists())
        self.assertFalse((self.work / G1).exists())
        self.assertEqual(self.queued_ids(worker), [])

        writer_guard_names = [
            row[0] for row in self.connection.execute(
                "SELECT name FROM sqlite_master WHERE type='trigger' AND name GLOB 'ia_writer_guard_*'"
            )
        ]
        self.assertTrue(writer_guard_names)
        for name in writer_guard_names:
            self.connection.execute('DROP TRIGGER "' + name.replace('"', '""') + '"')
        self.connection.commit()
        plan = preparer.prepare_delta(
            self.source, self.baseline, self.manifest, self.work / MANUAL,
            MANUAL, BASE, previous=None,
        )
        self.assertEqual(plan["kind"], "baseline_reconciliation")
        self.assertFalse(plan["metadata"]["all_writers_contract_enforced"])
        from ikarchive.writer_guards import install_writer_guards
        install_writer_guards(self.connection)
        self.connection.commit()
        with mock.patch.object(preparer, "verify_baseline_once", wraps=preparer.verify_baseline_once) as verify_spy:
            worker = self.make_worker((G1,), initial=MANUAL)
            adopted = worker.prepare_cycle()
            self.assertEqual(adopted["status"], "prepared_adopted")
            self.assertEqual(adopted["generation_id"], MANUAL)
            self.assertEqual(self.queued_ids(worker), [MANUAL])
            verify_spy.assert_not_called()

            # The manually captured snapshot predates guard DDL. Normal preparation
            # detects that live schema difference and creates a full reset after it.
            reset = worker.prepare_cycle()
            self.assertEqual(reset["status"], "prepared", reset)
            self.assertEqual(reset["generation_id"], G1)
            reset_plan = json.loads((self.work / G1 / "delta-plan.json").read_text())
            self.assertTrue(reset_plan["replaces_delta_chain"])
            self.assertEqual(self.queued_ids(worker), [MANUAL, G1])
            verify_spy.assert_called_once()

    def test_manual_initial_source_probe_failure_retries_without_failing_checkpoint(self):
        waiting_worker = self.make_worker((G1,), initial=MANUAL)
        self.assertEqual(
            waiting_worker.prepare_cycle()["status"], "waiting_initial_reconciliation"
        )
        plan = preparer.prepare_delta(
            self.source, self.baseline, self.manifest, self.work / MANUAL,
            MANUAL, BASE, previous=None,
        )
        self.assertEqual(plan["kind"], "baseline_reconciliation")

        probe_calls = 0

        def fail_once_then_probe(db):
            nonlocal probe_calls
            probe_calls += 1
            if probe_calls == 1:
                raise worker_module.WorkerError("SOURCE_PROBE_FAILED")
            return worker_module._source_probe(db)

        worker = self.make_worker((G1,), initial=MANUAL, probe=fail_once_then_probe)
        first = worker.prepare_cycle()
        self.assertEqual(first["status"], "failed")
        self.assertEqual(first["category"], "SOURCE_PROBE_FAILED")
        self.assertTrue(first["retry_pending"])
        checkpoint = json.loads(worker.prepared_checkpoint_path.read_text())
        self.assertEqual(checkpoint["initial_status"], "waiting")
        self.assertEqual(checkpoint["queue"], [])
        self.assertIsNone(checkpoint["last_prepared_generation_id"])

        adopted = worker.prepare_cycle()
        self.assertEqual(adopted["status"], "prepared_adopted", adopted)
        self.assertEqual(adopted["generation_id"], MANUAL)
        self.assertEqual(probe_calls, 2)
        self.assertEqual(self.queued_ids(worker), [MANUAL])
        checkpoint = json.loads(worker.prepared_checkpoint_path.read_text())
        self.assertEqual(checkpoint["initial_status"], "adopted")
        self.assertEqual(checkpoint["queue"], [{
            "generation_id": MANUAL,
            "plan_sha256": sha((self.work / MANUAL / "delta-plan.json").read_bytes()),
        }])

        # Once adopted, this initial generation cannot be enqueued a second time.
        self.assertIsNone(worker._manual_initial_cycle(worker._checkpoint_copy()))
        self.assertEqual(self.queued_ids(worker), [MANUAL])

    def test_manual_initial_invalid_proof_remains_sticky_failure(self):
        worker = self.make_worker((G1,), initial=MANUAL)
        self.assertEqual(worker.prepare_cycle()["status"], "waiting_initial_reconciliation")
        preparer.prepare_delta(
            self.source, self.baseline, self.manifest, self.work / MANUAL,
            MANUAL, BASE, previous=None,
        )
        proof_path = self.work / MANUAL / "xlsx" / "verification.json"
        proof = json.loads(proof_path.read_text(encoding="utf-8"))
        proof["status"] = "not_verified"
        proof_path.write_text(json.dumps(proof, ensure_ascii=False), encoding="utf-8")

        first = worker.prepare_cycle()
        self.assertEqual(first["status"], "failed")
        self.assertFalse(first["retry_pending"])
        checkpoint = json.loads(worker.prepared_checkpoint_path.read_text())
        self.assertEqual(checkpoint["initial_status"], "failed")
        self.assertEqual(checkpoint["queue"], [])

        second = worker.prepare_cycle()
        self.assertEqual(second["status"], "failed")
        self.assertFalse(second["retry_pending"])
        self.assertEqual(self.queued_ids(worker), [])

    def test_restart_recovers_only_the_durably_active_complete_generation(self):
        worker = self.make_worker((G1,))
        worker._checkpoint_copy()
        worker._mutate_prepared(lambda current: current.update({
            "active_generation": {
                "generation_id": G1,
                "parent_generation_id": BASE,
                "started_at": "2026-10-03T01:00:00+00:00",
            },
        }))
        preparer.prepare_delta(
            self.source, self.baseline, self.manifest, self.work / G1,
            G1, BASE, previous=None,
        )

        restarted = self.make_worker((G2,))
        result = restarted.prepare_cycle()

        self.assertEqual(result["status"], "prepared_recovered", result)
        self.assertEqual(result["generation_id"], G1)
        self.assertEqual(self.queued_ids(restarted), [G1])
        self.assertIsNone(json.loads(restarted.prepared_checkpoint_path.read_text())["active_generation"])

    def test_failed_manual_initial_generation_is_not_restarted(self):
        root = self.work / MANUAL
        root.mkdir(mode=0o700)
        worker_module._atomic_json(root / "failed.json", {
            "status": "failed", "generation_id": MANUAL,
            "phase": "lossless_xlsx", "error_code": "DELTA_XLSX_PROOF_INVALID",
        })
        prepare_mock = mock.Mock()
        worker = self.make_worker((G1,), prepare=prepare_mock, initial=MANUAL)
        first = worker.prepare_cycle()
        second = worker.prepare_cycle()
        self.assertEqual(first["status"], "failed")
        self.assertEqual(first["category"], "DELTA_XLSX_PROOF_INVALID")
        self.assertEqual(second["category"], "INITIAL_RECONCILIATION_FAILED")
        self.assertEqual(self.queued_ids(worker), [])
        prepare_mock.assert_not_called()

    def test_manual_initial_dangling_plan_symlink_is_rejected(self):
        root = self.work / MANUAL
        root.mkdir(mode=0o700)
        (root / "delta-plan.json").symlink_to(root / "missing-plan.json")
        worker = self.make_worker((G1,), initial=MANUAL)

        result = worker.prepare_cycle()

        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["category"], "PATH_SAFETY_ERROR")
        self.assertEqual(self.queued_ids(worker), [])

    def test_failed_new_attempt_preserves_previous_success_and_checkpoints(self):
        calls = 0

        def prepare_once_then_fail(db, baseline, manifest, generation, generation_id,
                                   baseline_generation_id, *, previous, max_part_bytes):
            nonlocal calls
            calls += 1
            if calls == 1:
                return preparer.prepare_delta(
                    db, baseline, manifest, generation, generation_id,
                    baseline_generation_id, previous=previous, max_part_bytes=max_part_bytes,
                )
            Path(generation).mkdir(mode=0o700)
            worker_module._atomic_json(Path(generation) / "failed.json", {
                "status": "failed", "generation_id": generation_id,
                "phase": "transport_fragmentation", "error_code": "SYNTHETIC_PREPARE_FAILURE",
            })
            raise RuntimeError("synthetic failure")

        worker = self.make_worker((G1, G2), prepare=prepare_once_then_fail)
        self.assertEqual(worker.run_once()["publication"]["status"], "complete")
        prepared_before = worker.prepared_checkpoint_path.read_bytes()
        published_before = worker.published_checkpoint_path.read_bytes()
        self.connection.execute("UPDATE records SET value='later failure' WHERE rowid=-7")
        self.connection.commit()

        failure = worker.prepare_cycle()

        self.assertEqual(failure["status"], "failed")
        self.assertEqual(failure["category"], "PREPARE_FAILED")
        self.assertTrue((self.work / G2 / "failed.json").is_file())
        self.assertEqual(worker.published_checkpoint_path.read_bytes(), published_before)
        prepared_after = json.loads(worker.prepared_checkpoint_path.read_text())
        prepared_old = json.loads(prepared_before.decode())
        self.assertEqual(prepared_after["last_prepared_generation_id"], G1)
        self.assertEqual(prepared_after["queue"], prepared_old["queue"])
        state = json.loads(worker.worker_state_path.read_text())
        self.assertEqual(state["prepare"]["last_success_generation_id"], G1)
        self.assertEqual(state["prepare"]["failure_category"], "PREPARE_FAILED")

    def test_restart_verifies_ordered_queue_and_does_not_adopt_unknown_generation(self):
        def pending(generation_dir, remote, state_dir, *, rclone_bin):
            plan = json.loads((Path(generation_dir) / "delta-plan.json").read_text())
            return {"status": "pending_baseline_publication", "generation_id": plan["generation_id"],
                    "checkpoint_advanced": False}

        worker = self.make_worker((G1, G2), publish=pending)
        worker.prepare_cycle()
        self.connection.execute("UPDATE records SET value='second queued generation' WHERE rowid=-7")
        self.connection.commit()
        worker.prepare_cycle()
        queue_before = self.queued_ids(worker)

        unknown = self.work / G3
        unknown.mkdir(mode=0o700)
        (unknown / "delta-plan.json").write_text('{"status":"prepared","role":"lossless_delta_generation"}')
        restarted = self.make_worker(("20261003T010005Z-55555555",), publish=pending)
        status = restarted.status()
        self.assertEqual(status["queue"], queue_before)
        self.assertEqual(status["queue"], [G1, G2])
        self.assertNotIn(G3, status["queue"])

        piece = next((self.work / G1 / "xlsx").glob("*.xlsx"))
        with piece.open("ab") as stream:
            stream.write(b"tamper")
        second_restart = self.make_worker(("20261003T010006Z-66666666",), publish=pending)
        with self.assertRaises(worker_module.WorkerError):
            second_restart.status()

    def test_worker_watch_keeps_preparation_loop_independent_of_slow_publish(self):
        release_publish = threading.Event()
        stop = threading.Event()
        counts = {"prepare": 0, "publish": 0}
        counts_lock = threading.Lock()
        emitted = []
        # The worker's injected wait is a fake timer; production still enforces
        # the configured 5..60 second interval.
        config = self.config(interval=5)
        worker = worker_module.DeltaWorker(
            config, generation_id_fn=self.generation_ids(G1),
            wait_fn=lambda event, _seconds: event.wait(0.005),
        )

        def prepare_cycle():
            with counts_lock:
                counts["prepare"] += 1
                if counts["prepare"] >= 2:
                    release_publish.set()
                    stop.set()
            return {"status": "idle", "retry_pending": False}

        def publish_cycle():
            with counts_lock:
                counts["publish"] += 1
                first = counts["publish"] == 1
            if first:
                self.assertTrue(release_publish.wait(2), "prepare loop stalled behind publish")
            return {"status": "idle", "retry_pending": False}

        worker.prepare_cycle = prepare_cycle
        worker.publish_cycle = publish_cycle
        worker._emit = emitted.append
        self.assertEqual(worker.run_watch(stop), 0)
        self.assertGreaterEqual(counts["prepare"], 2)
        self.assertGreaterEqual(counts["publish"], 1)
        self.assertTrue(emitted)
        for result in emitted:
            self.assertIn("published_lag_seconds", result)
            self.assertIn("queued_capture_lag_seconds", result)

    def _assert_main_watch_signal_drains_inflight_cycles(self, signum, expected_exit):
        worker = self.make_worker((G1,))
        prepare_started = threading.Event()
        publish_started = threading.Event()
        release_cycles = threading.Event()
        prepare_finished = threading.Event()
        calls = {"prepare": 0, "publish": 0}
        errors = []
        original_prepare = worker.prepare_cycle
        original_publish = worker.publish_cycle

        def prepare_cycle():
            calls["prepare"] += 1
            prepare_started.set()
            if not release_cycles.wait(5):
                raise AssertionError("prepare cycle was not released")
            try:
                return original_prepare()
            finally:
                prepare_finished.set()

        def publish_cycle():
            calls["publish"] += 1
            publish_started.set()
            if not release_cycles.wait(5):
                raise AssertionError("publish cycle was not released")
            if not prepare_finished.wait(5):
                raise AssertionError("prepare cycle did not finish before publication")
            return original_publish()

        worker.prepare_cycle = prepare_cycle
        worker.publish_cycle = publish_cycle
        worker.wait_fn = lambda event, _seconds: event.wait(0.005)
        worker._emit = lambda _result: None

        old_term = signal.getsignal(signal.SIGTERM)
        old_int = signal.getsignal(signal.SIGINT)

        def send_signal():
            try:
                if not (prepare_started.wait(5) and publish_started.wait(5)):
                    errors.append("both worker cycles did not start")
                    return
                os.kill(os.getpid(), signum)
            except BaseException as exc:
                errors.append(repr(exc))
            finally:
                release_cycles.set()

        sender = threading.Thread(target=send_signal, name="test-worker-signal")
        sender.start()
        try:
            with mock.patch.object(worker_module, "DeltaWorker", return_value=worker):
                exit_code = worker_module.main([
                    "--db", str(self.source),
                    "--baseline", str(self.baseline),
                    "--baseline-manifest", str(self.manifest),
                    "--baseline-generation-id", BASE,
                    "--work-dir", str(self.work),
                    "--state-dir", str(self.state),
                    "--remote", "archive:database",
                    "--watch",
                    "--interval", "5",
                ])
        finally:
            release_cycles.set()
            sender.join(5)

        self.assertFalse(sender.is_alive(), "signal sender did not finish")
        self.assertEqual(errors, [])
        self.assertEqual(exit_code, expected_exit)
        self.assertEqual(calls, {"prepare": 1, "publish": 1})
        self.assertEqual(signal.getsignal(signal.SIGTERM), old_term)
        self.assertEqual(signal.getsignal(signal.SIGINT), old_int)
        prepared = json.loads(worker.prepared_checkpoint_path.read_text(encoding="utf-8"))
        published = json.loads(worker.published_checkpoint_path.read_text(encoding="utf-8"))
        self.assertEqual(prepared["last_prepared_generation_id"], G1)
        self.assertEqual(published["generation_id"], G1)
        self.assertEqual(worker.status()["queue"], [])

    def test_main_watch_sigterm_waits_for_cycles_and_preserves_receipts(self):
        self._assert_main_watch_signal_drains_inflight_cycles(signal.SIGTERM, 0)

    def test_main_watch_sigint_waits_for_cycles_and_returns_130(self):
        self._assert_main_watch_signal_drains_inflight_cycles(signal.SIGINT, 130)

    def test_main_watch_restores_signal_handlers_when_watch_raises(self):
        class RaisingWatch:
            def run_watch(self, _stop_event):
                raise RuntimeError("synthetic watch failure")

        old_term = signal.getsignal(signal.SIGTERM)
        old_int = signal.getsignal(signal.SIGINT)
        with mock.patch.object(worker_module, "DeltaWorker", return_value=RaisingWatch()):
            with self.assertRaisesRegex(RuntimeError, "synthetic watch failure"):
                worker_module.main([
                    "--db", str(self.source),
                    "--baseline", str(self.baseline),
                    "--baseline-manifest", str(self.manifest),
                    "--baseline-generation-id", BASE,
                    "--work-dir", str(self.work),
                    "--state-dir", str(self.state),
                    "--remote", "archive:database",
                    "--watch",
                ])
        self.assertEqual(signal.getsignal(signal.SIGTERM), old_term)
        self.assertEqual(signal.getsignal(signal.SIGINT), old_int)

    def test_once_mode_does_not_install_signal_handlers(self):
        worker = self.make_worker((G1,))
        with mock.patch.object(worker_module, "DeltaWorker", return_value=worker):
            with mock.patch.object(worker_module.signal, "signal", wraps=signal.signal) as set_signal:
                exit_code = worker_module.main([
                    "--db", str(self.source),
                    "--baseline", str(self.baseline),
                    "--baseline-manifest", str(self.manifest),
                    "--baseline-generation-id", BASE,
                    "--work-dir", str(self.work),
                    "--state-dir", str(self.state),
                    "--remote", "archive:database",
                    "--once",
                ])
        self.assertEqual(exit_code, 0)
        set_signal.assert_not_called()

    def test_state_permissions_symlinks_and_worker_lock_are_checked(self):
        config = self.config()
        worker = self.make_worker((G1,))
        self.assertEqual(os.stat(worker.state_dir).st_mode & 0o777, 0o700)

        real = self.root / "real-state"
        real.mkdir(mode=0o700)
        link = self.root / "state-link"
        link.symlink_to(real, target_is_directory=True)
        with self.assertRaises(worker_module.WorkerError) as rejected:
            worker_module.DeltaWorker(self.config(state=link))
        self.assertEqual(rejected.exception.category, "PATH_SAFETY_ERROR")

        with worker_module._worker_lock(worker.state_dir):
            result = worker.run_once()
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["category"], "WORKER_LOCK_BUSY")

        bad_interval = worker_module.WorkerConfig(**{**config.__dict__, "interval_seconds": 4})
        with self.assertRaises(worker_module.WorkerError) as interval_error:
            worker_module.DeltaWorker(bad_interval)
        self.assertEqual(interval_error.exception.category, "INTERVAL_INVALID")

        bad_cap = worker_module.WorkerConfig(**{
            **config.__dict__, "max_part_bytes": worker_module.MIN_MAX_BYTES - 1,
        })
        with self.assertRaises(worker_module.WorkerError) as cap_error:
            worker_module.DeltaWorker(bad_cap)
        self.assertEqual(cap_error.exception.category, "PART_LIMIT_INVALID")
        accepted_cap = worker_module.WorkerConfig(**{
            **config.__dict__, "state_dir": self.root / "minimum-cap-state",
            "max_part_bytes": worker_module.MIN_MAX_BYTES,
        })
        self.assertEqual(worker_module.DeltaWorker(accepted_cap).max_part_bytes,
                         worker_module.MIN_MAX_BYTES)


if __name__ == "__main__":
    unittest.main()
