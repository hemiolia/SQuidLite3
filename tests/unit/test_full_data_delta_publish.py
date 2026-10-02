import hashlib
import contextlib
import io
import json
import os
from pathlib import Path
import shutil
import sqlite3
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src/python"))
sys.path.insert(0, str(ROOT / "scripts"))

import nas_full_data_publish as publisher  # noqa: E402
from ikarchive.delta_reader import DeltaChainReader  # noqa: E402
from ikarchive import lossless_sqlite  # noqa: E402
from ikarchive.change_feed import install_change_feed  # noqa: E402
from ikarchive.slice_selectors import export_selectors, verify_selectors  # noqa: E402
from ikarchive.shard_reader import LosslessShardReader  # noqa: E402
from ikarchive.writer_guards import install_writer_guards  # noqa: E402
from prepare_full_data_delta import prepare_delta  # noqa: E402
import publish_full_data_delta as delta_publisher  # noqa: E402
from publish_full_data_delta import main as publisher_main, publish_delta  # noqa: E402


REMOTE = "archive:database"
BASELINE_ID = "20261002T150000Z-11112222"


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def _canonical_sha(value):
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    return _sha(raw)


def _compact(value):
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _tree_snapshot(root):
    return {
        path.relative_to(root).as_posix(): (path.stat().st_size, _sha(path.read_bytes()))
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def _write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(_compact(value))


def _make_lossless_baseline_package(source_path, package_path, generation_id):
    raw = source_path.read_bytes()
    source_sha = _sha(raw)
    connection = sqlite3.connect(source_path.as_uri() + "?mode=ro&immutable=1", uri=True)
    connection.execute("PRAGMA query_only=ON")
    try:
        manifest = lossless_sqlite.export_sqlite_shards(
            connection, package_path, snapshot_id=generation_id,
        )
        manifest["source_sha256"] = source_sha
        export_selectors(connection, package_path, manifest, source_sha)
        _write_json(package_path / "manifest.json", manifest)
        verification = lossless_sqlite.verify_sqlite_shards(connection, package_path, manifest)
        verification["source_sha256"] = source_sha
        _write_json(package_path / "verification.json", verification)
        selector_verification = verify_selectors(connection, package_path, manifest, source_sha)
        _write_json(package_path / "selectors-verification.json", selector_verification)
    finally:
        connection.close()
    return source_sha


def _typed_row_key(values):
    result = []
    for value in values:
        if value is None:
            result.append(("null", None))
        elif type(value) is bytes:
            result.append(("blob", value.hex()))
        elif type(value) is float:
            result.append(("real", value.hex()))
        elif type(value) is int:
            result.append(("integer", value))
        elif type(value) is str:
            result.append(("text", value))
        else:
            result.append((type(value).__name__, repr(value)))
    return tuple(result)


class MemoryRemote:
    def __init__(self):
        self.objects = {}
        self.copy_calls = []
        self.readback_calls = []
        self.stat_counts = {}
        self.latest_path = f"{REMOTE}/latest.json"
        self.mutate_latest_on_stat = None
        self.fail_copy_path = None

    def stat(self, remote_path):
        count = self.stat_counts.get(remote_path, 0) + 1
        self.stat_counts[remote_path] = count
        if remote_path == self.latest_path and self.mutate_latest_on_stat == count:
            self.objects[remote_path] = b'{"concurrent_writer":true}\n'
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
        raw = self.objects.get(remote_path)
        if raw is None or (len(raw), _sha(raw)) != (expected_bytes, expected_sha):
            raise publisher.PublishError("REMOTE_READBACK_MISMATCH")
        return raw

    def copyto(self, source, remote_path, *, immutable):
        self.copy_calls.append((remote_path, immutable))
        if remote_path == self.fail_copy_path:
            raise publisher.PublishError("REMOTE_UPLOAD_FAILED")
        if immutable and remote_path in self.objects:
            raise publisher.PublishError("REMOTE_UPLOAD_FAILED")
        self.objects[remote_path] = Path(source).read_bytes()


class FullDataDeltaPublishTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name).resolve()
        self.state = self.root / "state"
        self.state.mkdir(mode=0o700)
        self.remote = MemoryRemote()
        self.baseline_path = self.root / "baseline.sqlite3"
        self.current_path = self.root / "current.sqlite3"
        self.manifest_path = self.root / "baseline-manifest.json"
        self._create_sources()
        self._install_baseline_remote()

    def tearDown(self):
        self.temporary.cleanup()

    def _open_current_writer(self):
        conn = sqlite3.connect(self.current_path)
        conn.execute("PRAGMA recursive_triggers=ON")
        return conn

    def _create_sources(self):
        conn = sqlite3.connect(self.baseline_path)
        try:
            conn.execute("CREATE TABLE records(id INTEGER PRIMARY KEY, payload, ratio REAL)")
            conn.execute("INSERT INTO records VALUES(1, ?, ?)", ("baseline", 1.5))
            conn.commit()
        finally:
            conn.close()
        conn = self._open_current_writer()
        try:
            conn.execute("CREATE TABLE records(id INTEGER PRIMARY KEY, payload, ratio REAL)")
            conn.execute("INSERT INTO records VALUES(1, ?, ?)", ("initial", 1.5))
            install_change_feed(conn)
            install_writer_guards(conn)
            conn.execute("UPDATE records SET payload=? WHERE id=1", ("first delta",))
            conn.commit()
        finally:
            conn.close()
        raw = self.baseline_path.read_bytes()
        manifest = {
            "storage": "plaintext", "encryption": None,
            "raw_snapshot": {"basename": self.baseline_path.name, "bytes": len(raw),
                             "sha256": _sha(raw), "quick_check": "ok"},
            "verification": {"quick_check": "ok"},
        }
        self.manifest_path.write_bytes(_compact(manifest))
        self.baseline_sha = _sha(raw)

    def _install_baseline_remote(self):
        conn = sqlite3.connect(self.baseline_path)
        try:
            rows = conn.execute(
                "SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name"
            ).fetchall()
            schema = [{"name": name, "type": kind, "tbl_name": table, "sql": sql}
                      for kind, name, table, sql in rows]
        finally:
            conn.close()
        source_bytes = self.baseline_path.stat().st_size
        source_schema_sha = _canonical_sha(schema)
        required = [
            "source.sqlite3", "source-manifest.json", "slices/manifest.json",
            "slices/verification.json", "slices/selectors-verification.json",
            "xlsx/index.json", "xlsx/manifest.json", "xlsx/verification.json",
        ]
        files = []
        for local in required:
            if local == "source.sqlite3":
                remote = f"unified/generations/{BASELINE_ID}/archive.sqlite3"
            elif local.startswith("slices/"):
                remote = f"slices/generations/{BASELINE_ID}/{local.removeprefix('slices/')}"
            elif local.startswith("xlsx/"):
                remote = f"xlsx-full/generations/{BASELINE_ID}/{local.removeprefix('xlsx/')}"
            else:
                remote = f"unified/generations/{BASELINE_ID}/source-manifest.json"
            files.append({"local": local, "remote": remote, "bytes": 1,
                          "sha256": _sha(local.encode())})
        index = {
            "version": 1, "generation_id": BASELINE_ID,
            "captured_at": "2026-10-02T15:00:00Z",
            "captured_at_kind": "pinned_read_transaction",
            "completed_at": "2026-10-02T15:01:00Z",
            "source": {"bytes": source_bytes, "sha256": self.baseline_sha},
            "source_schema_sha256": source_schema_sha,
            "counts": {"xlsx_pieces": 0},
            "sqlite_verification": {
                "status": "verified", "source_sha256": self.baseline_sha,
                "snapshot_identifier": BASELINE_ID,
                "source_schema_sha256": source_schema_sha,
                "coverage": {"all_tables": True, "all_rows": True, "all_columns": True,
                             "all_values": True, "external_values": True},
            },
            "selector_verification": {"source_sha256": self.baseline_sha,
                                       "all_shared_files_reachable": True,
                                       "all_mode_matches": True, "all_rule_matches": True},
            "xlsx_verification": {"status": "verified", "snapshot_identifier": BASELINE_ID,
                                  "snapshot_sha256": self.baseline_sha,
                                  "source_rowids_verified": True},
            "verification_receipt_files": {},
            "files": files,
            "verification": {"full_readback": True, "file_count": len(files)},
        }
        index_raw = _compact(index)
        index_sha = _sha(index_raw)
        index_remote = f"{REMOTE}/generations/{BASELINE_ID}/index.json"
        self.remote.objects[index_remote] = index_raw
        latest = {
            "version": 1, "generation_id": BASELINE_ID,
            "index_sha256": index_sha,
            "captured_at": index["captured_at"],
            "captured_at_kind": index["captured_at_kind"],
            "last_success": index["completed_at"],
        }
        self.remote.objects[self.remote.latest_path] = _compact(latest)
        self.baseline_index_sha = index_sha
        self.baseline_schema_sha = source_schema_sha

    def prepare(self, generation_id, *, previous=None, max_part_bytes=20 * 1024 * 1024):
        generation = self.root / "generations" / generation_id
        generation.parent.mkdir(exist_ok=True)
        plan = prepare_delta(
            self.current_path, self.baseline_path, self.manifest_path,
            generation, generation_id, BASELINE_ID, previous=previous,
            max_part_bytes=max_part_bytes,
        )
        return generation, plan

    def publish(self, generation, *, remote=None):
        with patch.object(publisher, "Rclone", return_value=self.remote):
            return publish_delta(generation, remote or REMOTE, self.state)

    def read_latest(self):
        return json.loads(self.remote.objects[self.remote.latest_path])

    def load_state(self):
        return json.loads((self.state / "current_state.json").read_text())

    def _refresh_plan_file(self, generation, plan, local):
        path = generation / local
        raw = path.read_bytes()
        entry = next(item for item in plan["files"] if item["local"] == local)
        entry["bytes"] = len(raw)
        entry["sha256"] = _sha(raw)
        (generation / "delta-plan.json").write_bytes(_compact(plan))

    def test_initial_and_incremental_delta_append_chain_and_reread_every_file(self):
        first_id = "20261003T010101Z-aaaa0001"
        second_id = "20261003T010202Z-aaaa0002"
        first_dir, first_plan = self.prepare(first_id)
        first_result = self.publish(first_dir)
        self.assertEqual(first_result["status"], "complete")
        first_index = f"{REMOTE}/deltas/generations/{first_id}/index.json"
        first_index_doc = json.loads(self.remote.objects[first_index])
        self.assertTrue(first_index_doc["verification"]["full_readback"])
        self.assertEqual(first_index_doc["verification"]["file_count"],
                         len(first_plan["files"]) + 1)
        self.assertEqual(first_index_doc["files"][:-1], first_plan["files"])
        self.assertEqual(first_index_doc["files"][-1]["local"], "delta-plan.json")
        self.assertEqual(first_index_doc["files"][-1]["remote"],
                         f"deltas/generations/{first_id}/delta-plan.json")
        self.assertEqual(first_index_doc["files"][-1]["sha256"],
                         first_index_doc["plan_sha256"])
        published_plan = f"{REMOTE}/deltas/generations/{first_id}/delta-plan.json"
        self.assertEqual(self.remote.objects[published_plan], (first_dir / "delta-plan.json").read_bytes())
        self.assertIn(published_plan, self.remote.readback_calls)
        self.assertTrue(first_plan["replaces_delta_chain"])
        self.assertEqual(len(self.read_latest()["delta_chain"]), 1)

        conn = self._open_current_writer()
        try:
            conn.execute("UPDATE records SET payload=?, ratio=? WHERE id=1", ("second value", 2.25))
            conn.commit()
        finally:
            conn.close()
        second_dir, second_plan = self.prepare(second_id, previous=first_plan)
        self.assertFalse(second_plan["replaces_delta_chain"])
        second_result = self.publish(second_dir)
        self.assertEqual(second_result["status"], "complete")

        latest = self.read_latest()
        self.assertEqual(latest["role"], "lossless_full_state")
        self.assertEqual([row["generation_id"] for row in latest["delta_chain"]],
                         [first_id, second_id])
        self.assertEqual(latest["source_schema_sha256"], second_plan["source_schema_sha256"])
        self.assertEqual(latest["source_row_counts"], second_plan["source_row_counts"])
        self.assertEqual(latest["through_event_id"], second_plan["through_event_id"])
        self.assertIn(first_index, self.remote.objects)

        for generation_dir, plan in ((first_dir, first_plan), (second_dir, second_plan)):
            for item in plan["files"]:
                full_remote = f"{REMOTE}/{item['remote']}"
                self.assertIn(full_remote, self.remote.readback_calls)
                self.assertEqual((len(self.remote.objects[full_remote]),
                                  _sha(self.remote.objects[full_remote])),
                                 (item["bytes"], item["sha256"]))
        self.assertEqual(self.load_state()["phase"], "complete")
        self.assertEqual(self.load_state()["generation_id"], second_id)

    def test_remote_plan_missing_or_corrupt_blocks_completed_generation_checkpoint(self):
        generation_id = "20261003T010303Z-aaaa0003"
        generation, _plan = self.prepare(generation_id)
        self.assertEqual(self.publish(generation)["status"], "complete")
        checkpoint_path = self.state / "delta-published-checkpoint.json"
        checkpoint_before = checkpoint_path.read_bytes()
        latest_before = self.remote.objects[self.remote.latest_path]
        state_before = self.load_state()
        plan_remote = f"{REMOTE}/deltas/generations/{generation_id}/delta-plan.json"
        original_plan = self.remote.objects.pop(plan_remote)

        with self.assertRaises(publisher.PublishError) as missing:
            self.publish(generation)
        self.assertEqual(missing.exception.category, "DELTA_PLAN_REMOTE_MISSING")
        self.assertEqual(self.remote.objects[self.remote.latest_path], latest_before)
        self.assertEqual(checkpoint_path.read_bytes(), checkpoint_before)
        self.assertEqual(self.load_state()["last_success"], state_before["last_success"])

        self.remote.objects[plan_remote] = original_plan + b" "
        with self.assertRaises(publisher.PublishError) as corrupt:
            self.publish(generation)
        self.assertEqual(corrupt.exception.category, "DELTA_PLAN_READBACK_MISMATCH")
        self.assertEqual(self.remote.objects[self.remote.latest_path], latest_before)
        self.assertEqual(checkpoint_path.read_bytes(), checkpoint_before)
        self.assertEqual(self.load_state()["last_success"], state_before["last_success"])

    def test_remote_plan_and_artifacts_reconstruct_without_raw_transport_cache(self):
        reader_baseline_id = BASELINE_ID
        generation_id = "20261003T010404Z-aaaa0004"
        baseline_db = self.root / "reader-baseline.sqlite3"
        current_db = self.root / "reader-current.sqlite3"
        source_manifest_path = self.root / "reader-source-manifest.json"
        baseline_package = self.root / "reader-baseline-package"
        with contextlib.closing(sqlite3.connect(baseline_db)) as connection:
            connection.executescript("""
                PRAGMA journal_mode=DELETE;
                PRAGMA foreign_keys=ON;
                CREATE TABLE records(id INTEGER PRIMARY KEY, payload, exact_int, ratio);
                CREATE TABLE matches(account TEXT,kind TEXT,match_key TEXT,payload BLOB,
                    PRIMARY KEY(account,kind,match_key));
                CREATE TABLE match_classification(account TEXT,kind TEXT,match_key TEXT,
                    analysis_set TEXT NOT NULL,rule_raw TEXT,raw_classification BLOB,
                    PRIMARY KEY(account,kind,match_key),
                    FOREIGN KEY(account,kind,match_key) REFERENCES matches(account,kind,match_key));
                CREATE TABLE empty_rows(label TEXT);
                INSERT INTO records VALUES(1,'baseline text',9223372036854775807,1.5);
                INSERT INTO matches VALUES('acct','vs','match-1',X'006d61746368');
                INSERT INTO match_classification VALUES('acct','vs','match-1','xmatch','AREA',X'0072');
            """)
            connection.commit()
        shutil.copy2(baseline_db, current_db)
        baseline_raw = baseline_db.read_bytes()
        baseline_manifest = {
            "storage": "plaintext", "encryption": None,
            "raw_snapshot": {"basename": baseline_db.name, "bytes": len(baseline_raw),
                             "sha256": _sha(baseline_raw), "quick_check": "ok"},
            "verification": {"quick_check": "ok"},
        }
        _write_json(source_manifest_path, baseline_manifest)
        baseline_sha = _make_lossless_baseline_package(
            baseline_db, baseline_package, reader_baseline_id,
        )
        baseline_connection = sqlite3.connect(baseline_db.as_uri() + "?mode=ro&immutable=1", uri=True)
        try:
            baseline_schema = [
                {"name": name, "type": kind, "tbl_name": table, "sql": sql}
                for kind, name, table, sql in baseline_connection.execute(
                    "SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name"
                )
            ]
        finally:
            baseline_connection.close()
        baseline_index_path = f"{REMOTE}/generations/{reader_baseline_id}/index.json"
        baseline_index = json.loads(self.remote.objects[baseline_index_path])
        baseline_index["source"] = {"bytes": len(baseline_raw), "sha256": baseline_sha}
        baseline_index["source_schema_sha256"] = _canonical_sha(baseline_schema)
        baseline_index["sqlite_verification"].update({
            "source_sha256": baseline_sha,
            "source_schema_sha256": _canonical_sha(baseline_schema),
        })
        baseline_index["selector_verification"]["source_sha256"] = baseline_sha
        baseline_index["xlsx_verification"]["snapshot_sha256"] = baseline_sha
        baseline_index_raw = _compact(baseline_index)
        self.remote.objects[baseline_index_path] = baseline_index_raw
        baseline_latest = {
            "version": 1, "generation_id": reader_baseline_id,
            "index_sha256": _sha(baseline_index_raw),
            "captured_at": baseline_index["captured_at"],
            "captured_at_kind": baseline_index["captured_at_kind"],
            "last_success": baseline_index["completed_at"],
        }
        self.remote.objects[self.remote.latest_path] = _compact(baseline_latest)

        with contextlib.closing(sqlite3.connect(current_db)) as connection:
            connection.execute("PRAGMA foreign_keys=ON")
            connection.execute("PRAGMA recursive_triggers=ON")
            install_change_feed(connection)
            install_writer_guards(connection)
            connection.execute(
                "UPDATE records SET payload=?,exact_int=?,ratio=? WHERE id=1",
                (bytes(range(256)) * 1600, 9223372036854775806, -0.0),
            )
            connection.execute(
                "INSERT INTO records VALUES(?,?,?,?)",
                (-9, "new\x00text", 9223372036854775807, 0.125),
            )
            connection.commit()

        generation = self.root / "remote-only-delta"
        plan = prepare_delta(
            current_db, baseline_db, source_manifest_path, generation,
            generation_id, reader_baseline_id, max_part_bytes=256 * 1024,
        )
        self.assertEqual(plan["transport"]["kind"], "lossless_sqlite_shards")
        result = self.publish(generation)
        self.assertEqual(result["status"], "complete")

        index_remote = f"{REMOTE}/deltas/generations/{generation_id}/index.json"
        index = json.loads(self.remote.objects[index_remote])
        plan_remote = f"{REMOTE}/deltas/generations/{generation_id}/delta-plan.json"
        self.assertEqual(self.remote.objects[plan_remote], (generation / "delta-plan.json").read_bytes())
        self.assertEqual(index["files"][-1], {
            "local": "delta-plan.json",
            "remote": f"deltas/generations/{generation_id}/delta-plan.json",
            "bytes": len(self.remote.objects[plan_remote]),
            "sha256": _sha(self.remote.objects[plan_remote]),
        })
        self.assertEqual(index["plan_sha256"], _sha(self.remote.objects[plan_remote]))
        self.assertEqual(index["files"][:-1], plan["files"])
        self.assertEqual(index["verification"]["file_count"], len(index["files"]))
        self.assertNotIn("changes.sqlite3", {item["local"] for item in index["files"]})
        self.assertNotIn("source.sqlite3", {item["local"] for item in index["files"]})

        remote_generation = self.root / "remote-generation-copy"
        for item in index["files"]:
            raw = self.remote.objects[f"{REMOTE}/{item['remote']}"]
            target = remote_generation / item["local"]
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(raw)
            self.assertEqual((len(raw), _sha(raw)), (item["bytes"], item["sha256"]))
        self.assertTrue((remote_generation / "delta-plan.json").is_file())
        self.assertFalse((remote_generation / "changes.sqlite3").exists())
        self.assertFalse((remote_generation / "source.sqlite3").exists())

        with LosslessShardReader(baseline_package, expected_generation=reader_baseline_id) as baseline:
            with DeltaChainReader(baseline, [remote_generation]) as reader:
                self.assertEqual(reader.generation_id, generation_id)
                self.assertEqual(reader.schema_objects(), plan["schemas"])
                self.assertEqual(reader.source_schema_sha256, plan["source_schema_sha256"])
                self.assertEqual(
                    {table["name"] for table in reader.tables()},
                    set(plan["source_row_counts"]),
                )
                with contextlib.closing(sqlite3.connect(
                        current_db.as_uri() + "?mode=ro&immutable=1", uri=True)) as source_reader:
                    source_reader.execute("PRAGMA query_only=ON")
                    for table_name, expected_count in plan["source_row_counts"].items():
                        self.assertEqual(reader.row_count(table_name), expected_count)
                        self.assertEqual(reader.columns(table_name), plan["source_table_columns"][table_name])
                        self.assertEqual(reader.foreign_keys(table_name), plan["source_foreign_keys"][table_name])
                        visible_columns = [
                            item["name"] for item in plan["source_table_columns"][table_name]
                            if item["hidden"] != 1
                        ]
                        quote = lambda name: '"' + name.replace('"', '""') + '"'
                        expected_rows = source_reader.execute(
                            "SELECT " + ",".join(quote(name) for name in visible_columns)
                            + " FROM " + quote(table_name)
                        ).fetchall()
                        actual_rows = [values for _, _source_rowid, values in reader.iter_rows(table_name)]
                        self.assertEqual(
                            sorted(map(_typed_row_key, actual_rows), key=repr),
                            sorted(map(_typed_row_key, expected_rows), key=repr),
                            table_name,
                        )
                    records = list(reader.iter_rows("records"))
                    by_rowid = {source_rowid: values for _, source_rowid, values in records}
                    self.assertEqual(set(by_rowid), {1, -9})
                    self.assertEqual(by_rowid[1][1], bytes(range(256)) * 1600)
                    self.assertEqual(by_rowid[1][2], 9223372036854775806)
                    self.assertEqual(by_rowid[1][3].hex(), "-0x0.0p+0")
                    self.assertEqual(by_rowid[-9][1], "new\x00text")
                    self.assertEqual(by_rowid[-9][2], 9223372036854775807)
                    self.assertEqual(by_rowid[-9][3].hex(), "0x1.0000000000000p-3")
                    self.assertEqual(reader.row_count("empty_rows"), 0)
        self.assertEqual(baseline_sha, _sha(baseline_db.read_bytes()))

    def test_schema_reset_replaces_chain_and_keeps_superseded_objects(self):
        first_id = "20261003T011101Z-bbbb0001"
        reset_id = "20261003T011202Z-bbbb0002"
        first_dir, first_plan = self.prepare(first_id)
        self.publish(first_dir)
        first_object_set = set(self.remote.objects)
        conn = self._open_current_writer()
        try:
            conn.execute("ALTER TABLE records ADD COLUMN added_note TEXT")
            conn.execute("UPDATE records SET added_note=? WHERE id=1", ("schema reset",))
            conn.commit()
        finally:
            conn.close()
        reset_dir, reset_plan = self.prepare(reset_id, previous=first_plan)
        self.assertTrue(reset_plan["replaces_delta_chain"])
        self.assertEqual(reset_plan["parent_generation_id"], BASELINE_ID)
        self.assertEqual(reset_plan["supersedes_generation_id"], first_id)
        self.publish(reset_dir)
        latest = self.read_latest()
        self.assertEqual([row["generation_id"] for row in latest["delta_chain"]], [reset_id])
        self.assertEqual(latest["delta_chain"][0]["parent_generation_id"], BASELINE_ID)
        self.assertTrue(first_object_set.issubset(self.remote.objects))
        self.assertEqual(latest["source_schema_sha256"], reset_plan["source_schema_sha256"])

    def test_unavailable_baseline_returns_pending_without_checkpoint_advance(self):
        generation_id = "20261003T012101Z-cccc0001"
        generation, _plan = self.prepare(generation_id)
        self.remote.objects.clear()
        result = self.publish(generation)
        self.assertEqual(result["status"], "pending_baseline_publication")
        self.assertIs(result["checkpoint_advanced"], False)
        self.assertEqual(self.remote.copy_calls, [])
        self.assertFalse((self.state / "delta-published-checkpoint.json").exists())
        state = self.load_state()
        self.assertEqual(state["phase"], "pending_baseline_publication")
        self.assertIsNone(state["last_failure"])

    def test_missing_baseline_index_is_pending_but_does_not_publish(self):
        generation_id = "20261003T012201Z-cccc0002"
        generation, _plan = self.prepare(generation_id)
        self.remote.objects.pop(f"{REMOTE}/generations/{BASELINE_ID}/index.json")

        result = self.publish(generation)

        self.assertEqual(result["status"], "pending_baseline_publication")
        self.assertIs(result["checkpoint_advanced"], False)
        self.assertEqual(self.remote.copy_calls, [])
        self.assertFalse((self.state / "delta-published-checkpoint.json").exists())
        state = self.load_state()
        self.assertEqual(state["phase"], "pending_baseline_publication")
        self.assertIsNone(state["last_failure"])

    def test_broken_baseline_proof_raises_and_records_failure_without_upload(self):
        generation_id = "20261003T012301Z-cccc0003"
        generation, _plan = self.prepare(generation_id)
        index_path = f"{REMOTE}/generations/{BASELINE_ID}/index.json"
        index = json.loads(self.remote.objects[index_path])
        index["sqlite_verification"]["coverage"]["all_values"] = False
        index_raw = _compact(index)
        self.remote.objects[index_path] = index_raw
        latest = self.read_latest()
        latest["index_sha256"] = _sha(index_raw)
        self.remote.objects[self.remote.latest_path] = _compact(latest)

        with self.assertRaises(publisher.PublishError) as caught:
            self.publish(generation)

        self.assertEqual(caught.exception.category, "BASELINE_PROOF_INCOMPLETE")
        self.assertEqual(self.remote.copy_calls, [])
        self.assertFalse((self.state / "delta-published-checkpoint.json").exists())
        self.assertEqual(self.load_state()["last_failure"]["category"], "BASELINE_PROOF_INCOMPLETE")

    def test_unknown_latest_role_raises_and_records_failure_without_upload(self):
        generation_id = "20261003T012401Z-cccc0004"
        generation, _plan = self.prepare(generation_id)
        latest = self.read_latest()
        latest["role"] = "unknown_generation_role"
        self.remote.objects[self.remote.latest_path] = _compact(latest)

        with self.assertRaises(publisher.PublishError) as caught:
            self.publish(generation)

        self.assertEqual(caught.exception.category, "LATEST_ROLE_INVALID")
        self.assertEqual(self.remote.copy_calls, [])
        self.assertFalse((self.state / "delta-published-checkpoint.json").exists())
        self.assertEqual(self.load_state()["last_failure"]["category"], "LATEST_ROLE_INVALID")

    def test_pending_baseline_cli_prints_only_available_fields_and_exits_zero(self):
        generation_id = "20261003T012501Z-cccc0005"
        generation, _plan = self.prepare(generation_id)
        self.remote.objects.clear()
        output = io.StringIO()

        with patch.object(publisher, "Rclone", return_value=self.remote), contextlib.redirect_stdout(output):
            result = publisher_main([
                "--generation-dir", str(generation), "--remote", REMOTE,
                "--state-dir", str(self.state),
            ])

        self.assertEqual(result, 0)
        printed = json.loads(output.getvalue())
        self.assertEqual(printed, {
            "status": "pending_baseline_publication",
            "scope": "immutable_lossless_delta_generation",
            "generation_id": generation_id,
            "baseline_generation_id": BASELINE_ID,
            "checkpoint_advanced": False,
        })
        self.assertEqual(self.remote.copy_calls, [])
        self.assertFalse((self.state / "delta-published-checkpoint.json").exists())

    def test_negative_coverage_fails_before_remote_upload(self):
        generation_id = "20261003T013101Z-dddd0001"
        generation, _plan = self.prepare(generation_id)
        plan_path = generation / "delta-plan.json"
        plan = json.loads(plan_path.read_text())
        plan["coverage"]["all_changed_values"] = False
        plan_path.write_bytes(_compact(plan))
        with self.assertRaises(publisher.PublishError) as caught:
            self.publish(generation)
        self.assertEqual(caught.exception.category, "PLAN_COVERAGE_INCOMPLETE")
        self.assertEqual(self.remote.copy_calls, [])
        self.assertEqual(self.remote.objects[self.remote.latest_path],
                         _compact({
                             "version": 1, "generation_id": BASELINE_ID,
                             "index_sha256": self.baseline_index_sha,
                             "captured_at": "2026-10-02T15:00:00Z",
                             "captured_at_kind": "pinned_read_transaction",
                             "last_success": "2026-10-02T15:01:00Z",
                         }))

    def test_missing_piece_fails_local_inventory_before_remote_upload(self):
        generation_id = "20261003T014101Z-eeee0001"
        generation, plan = self.prepare(generation_id)
        piece = next(row["local"] for row in plan["files"] if row["local"].startswith("xlsx/")
                     and row["local"].endswith(".xlsx"))
        (generation / piece).unlink()
        with self.assertRaises(publisher.PublishError) as caught:
            self.publish(generation)
        self.assertIn(caught.exception.category,
                      {"GENERATION_FILE_SET_MISMATCH", "GENERATION_LAYOUT_INVALID"})
        self.assertEqual(self.remote.copy_calls, [])

    def test_large_external_value_shards_are_published_without_transport_cache(self):
        large = bytes(range(256)) * 1300
        conn = self._open_current_writer()
        try:
            conn.execute("UPDATE records SET payload=? WHERE id=1", (large,))
            conn.commit()
        finally:
            conn.close()
        generation_id = "20261003T015101Z-ffff0001"
        generation, plan = self.prepare(generation_id, max_part_bytes=256 * 1024)
        self.assertEqual(plan["transport"]["kind"], "lossless_sqlite_shards")
        input_before = _tree_snapshot(generation)
        result = self.publish(generation)
        self.assertEqual(result["status"], "complete")
        self.assertEqual(_tree_snapshot(generation), input_before)
        remotes = {path for path, _immutable in self.remote.copy_calls}
        self.assertNotIn(f"{REMOTE}/deltas/generations/{generation_id}/changes.sqlite3", remotes)
        self.assertTrue(any("/transport/" in path and path.endswith(".sqlite3") for path in remotes))
        self.assertEqual(self.read_latest()["generation_id"], generation_id)

    def test_shard_verifier_failure_does_not_modify_prepared_input(self):
        large = bytes(range(256)) * 1300
        conn = self._open_current_writer()
        try:
            conn.execute("UPDATE records SET payload=? WHERE id=1", (large,))
            conn.commit()
        finally:
            conn.close()
        generation_id = "20261003T015201Z-ffff0002"
        generation, plan = self.prepare(generation_id, max_part_bytes=256 * 1024)
        self.assertEqual(plan["transport"]["kind"], "lossless_sqlite_shards")
        input_before = _tree_snapshot(generation)
        verify = lossless_sqlite.verify_sqlite_shards
        called = []

        def verify_then_fail(sourceconn, root, manifest, *, write_receipt=True):
            called.append(write_receipt)
            proof = verify(sourceconn, root, manifest, write_receipt=write_receipt)
            raise ValueError("synthetic failure after independent verification")

        with patch.object(delta_publisher.lossless_sqlite, "verify_sqlite_shards",
                          side_effect=verify_then_fail):
            with self.assertRaises(publisher.PublishError) as caught:
                self.publish(generation)

        self.assertEqual(caught.exception.category, "TRANSPORT_SHARD_VERIFICATION_FAILED")
        self.assertEqual(called, [False])
        self.assertEqual(_tree_snapshot(generation), input_before)
        self.assertEqual(self.remote.copy_calls, [])
        self.assertFalse((self.state / "delta-published-checkpoint.json").exists())

    def test_native_transport_over_part_limit_is_rejected_before_upload(self):
        large = b"native-cache-limit" * 30000
        conn = self._open_current_writer()
        try:
            conn.execute("UPDATE records SET payload=? WHERE id=1", (large,))
            conn.commit()
        finally:
            conn.close()
        generation_id = "20261003T015301Z-ffff0003"
        generation, plan = self.prepare(generation_id)
        self.assertEqual(plan["transport"]["kind"], "native_sqlite")
        self.assertGreater(plan["transport_database"]["bytes"], lossless_sqlite.MIN_MAX_BYTES)
        plan["max_part_bytes"] = lossless_sqlite.MIN_MAX_BYTES
        (generation / "delta-plan.json").write_bytes(_compact(plan))

        with self.assertRaises(publisher.PublishError) as caught:
            self.publish(generation)

        self.assertEqual(caught.exception.category, "TRANSPORT_NATIVE_EXCEEDS_PART_LIMIT")
        self.assertEqual(self.remote.copy_calls, [])
        self.assertFalse((self.state / "delta-published-checkpoint.json").exists())

    def test_xlsx_piece_over_20_mib_is_rejected_before_upload(self):
        generation_id = "20261003T015401Z-ffff0004"
        generation, plan = self.prepare(generation_id)
        index_path = generation / "xlsx/index.json"
        index = json.loads(index_path.read_text())
        self.assertTrue(index["pieces"])
        index["pieces"][0]["bytes"] = 20 * 1024 * 1024 + 1
        index_path.write_bytes(_compact(index))
        self._refresh_plan_file(generation, plan, "xlsx/index.json")

        with self.assertRaises(publisher.PublishError) as caught:
            self.publish(generation)

        self.assertEqual(caught.exception.category, "XLSX_PIECE_TOO_LARGE")
        self.assertEqual(self.remote.copy_calls, [])
        self.assertFalse((self.state / "delta-published-checkpoint.json").exists())

    def test_part_limit_below_shard_minimum_is_rejected(self):
        generation_id = "20261003T015501Z-ffff0005"
        generation, plan = self.prepare(generation_id)
        plan["max_part_bytes"] = lossless_sqlite.MIN_MAX_BYTES - 1
        (generation / "delta-plan.json").write_bytes(_compact(plan))

        with self.assertRaises(publisher.PublishError) as caught:
            self.publish(generation)

        self.assertEqual(caught.exception.category, "PLAN_PART_LIMIT_INVALID")
        self.assertEqual(self.remote.copy_calls, [])

    def test_racing_latest_is_preserved_and_not_replaced(self):
        generation_id = "20261003T020101Z-aaaa0003"
        generation, _plan = self.prepare(generation_id)
        initial_latest = self.remote.objects[self.remote.latest_path]
        # latest is statted twice while its bytes are pinned in preflight; the
        # next stat is the mandatory final compare immediately before update.
        self.remote.mutate_latest_on_stat = 3
        with self.assertRaises(publisher.PublishError) as caught:
            self.publish(generation)
        self.assertEqual(caught.exception.category, "LATEST_CHANGED")
        self.assertNotEqual(self.remote.objects[self.remote.latest_path], initial_latest)
        self.assertEqual(self.remote.objects[self.remote.latest_path], b'{"concurrent_writer":true}\n')
        previous_remote = f"{REMOTE}/generations/{generation_id}/previous-latest.json"
        self.assertEqual(self.remote.objects[previous_remote], initial_latest)
        history_copy = self.state / "history" / f"{generation_id}-previous-latest.json"
        self.assertEqual(history_copy.read_bytes(), initial_latest)
        self.assertFalse((self.state / "delta-published-checkpoint.json").exists())
        self.assertIsNone(self.load_state()["last_success"])

    def test_upload_failure_keeps_prior_latest_and_last_success(self):
        first_id = "20261003T020901Z-bbbb0002"
        generation_id = "20261003T021101Z-bbbb0003"
        first_generation, first_plan = self.prepare(first_id)
        with patch.dict(os.environ, {}, clear=True):
            self.publish(first_generation)
        successful_state = self.load_state()
        successful_checkpoint = (self.state / "delta-published-checkpoint.json").read_bytes()
        conn = self._open_current_writer()
        try:
            conn.execute("UPDATE records SET payload=? WHERE id=1", ("change before failed upload",))
            conn.commit()
        finally:
            conn.close()
        generation, plan = self.prepare(generation_id, previous=first_plan)
        input_before = _tree_snapshot(generation)
        original_latest = self.remote.objects[self.remote.latest_path]
        failing_item = plan["files"][0]
        self.remote.fail_copy_path = f"{REMOTE}/{failing_item['remote']}"
        self.assertGreaterEqual(len(plan["files"]), 3)
        first_wave = threading.Barrier(3)
        original_copyto = self.remote.copyto

        def synchronized_copyto(source, remote_path, *, immutable):
            if remote_path in {f"{REMOTE}/{item['remote']}" for item in plan["files"]}:
                first_wave.wait(timeout=5)
            return original_copyto(source, remote_path, immutable=immutable)

        self.remote.copyto = synchronized_copyto
        with patch.dict(os.environ, {"IKARING_ARCHIVE_PUBLISH_FILE_WORKERS": "3"}, clear=True), \
                self.assertRaises(publisher.PublishError) as caught:
            self.publish(generation)
        self.assertEqual(caught.exception.category, "REMOTE_UPLOAD_FAILED")
        self.assertEqual(self.remote.objects[self.remote.latest_path], original_latest)
        self.assertEqual(self.load_state()["last_success"], successful_state["last_success"])
        self.assertEqual(self.load_state()["last_failure"]["category"], "REMOTE_UPLOAD_FAILED")
        self.assertEqual((self.state / "delta-published-checkpoint.json").read_bytes(), successful_checkpoint)
        self.assertEqual(json.loads(successful_checkpoint)["generation_id"], first_id)
        self.assertNotIn(f"{REMOTE}/deltas/generations/{generation_id}/index.json", self.remote.objects)
        progress = json.loads((self.state / f"{generation_id}.progress.json").read_text())
        self.assertGreaterEqual(len(progress["receipts"]), 2)
        self.assertNotIn(failing_item["local"], progress["receipts"])
        self.assertEqual(_tree_snapshot(generation), input_before)

    def test_repeat_of_completed_generation_keeps_latest_and_index_bytes_stable(self):
        generation_id = "20261003T022101Z-cccc0003"
        generation, plan = self.prepare(generation_id)
        first = self.publish(generation)
        self.assertEqual(first["status"], "complete")
        latest_path = self.remote.latest_path
        index_path = f"{REMOTE}/deltas/generations/{generation_id}/index.json"
        latest_before = self.remote.objects[latest_path]
        index_before = self.remote.objects[index_path]
        latest_copy_count = sum(path == latest_path for path, _immutable in self.remote.copy_calls)
        readback_before = len(self.remote.readback_calls)

        repeated = self.publish(generation)

        self.assertEqual(repeated["status"], "complete")
        self.assertEqual(self.remote.objects[latest_path], latest_before)
        self.assertEqual(self.remote.objects[index_path], index_before)
        self.assertEqual(sum(path == latest_path for path, _immutable in self.remote.copy_calls),
                         latest_copy_count)
        self.assertGreater(len(self.remote.readback_calls), readback_before)
        for item in plan["files"]:
            self.assertIn(f"{REMOTE}/{item['remote']}", self.remote.readback_calls)


if __name__ == "__main__":
    unittest.main()
