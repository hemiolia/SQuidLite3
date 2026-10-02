import contextlib
import hashlib
import io
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
import nas_full_data_publish as publisher  # noqa: E402


GENERATION = "20261002T120000Z-abc012ef"


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def canonical_sha(value):
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return digest(raw)


def write_json(path, value):
    raw = (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(raw)
    return len(raw), digest(raw)


def refresh_plan_file_entry(generation_dir, local):
    plan_path = generation_dir / "generation-plan.json"
    plan = json.loads(plan_path.read_text())
    actual = (generation_dir / local).read_bytes()
    for row in plan["files"]:
        if row["local"] == local:
            row["bytes"] = len(actual)
            row["sha256"] = digest(actual)
    write_json(plan_path, plan)


def add_file(root, local, raw, generation_id=GENERATION):
    path = root / local
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(raw)
    return {"local": local, "remote": publisher._expected_remote(generation_id, local),
            "bytes": len(raw), "sha256": digest(raw)}


def add_selector(root, local, dependencies, generation_id=GENERATION):
    path = root / local
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    try:
        connection.execute(
            "CREATE TABLE shared_files(file TEXT PRIMARY KEY,bytes INTEGER NOT NULL,sha256 TEXT NOT NULL)"
        )
        connection.executemany(
            "INSERT INTO shared_files(file,bytes,sha256) VALUES(?,?,?)",
            ((row["file"], row["bytes"], row["sha256"]) for row in dependencies),
        )
        connection.commit()
    finally:
        connection.close()
    size, sha = publisher.hash_regular_file(path)
    return {"local": local, "remote": publisher._expected_remote(generation_id, local),
            "bytes": size, "sha256": sha}


def make_generation(root, *, modes=16, rules=20, populated_modes=None,
                    xlsx_pieces=2, generation_id=GENERATION):
    if populated_modes is None:
        populated_modes = min(modes, 3)
    source_bytes = b"synthetic static SQLite source for transport tests"
    source_sha = digest(source_bytes)
    source = {"bytes": len(source_bytes), "sha256": source_sha}
    (root / "slices/by-mode").mkdir(parents=True)
    (root / "slices/by-rule").mkdir(parents=True)
    (root / "xlsx").mkdir()
    files = [add_file(root, "source.sqlite3", source_bytes, generation_id)]

    source_manifest = {
        "storage": "plaintext",
        "encryption": None,
        "raw_snapshot": {
            "basename": "archive_test.sqlite3", "bytes": source["bytes"],
            "sha256": source_sha, "quick_check": "ok",
        },
        "verification": {"sha256_match": True, "quick_check": "ok"},
    }
    source_manifest_info = write_json(root / "source-manifest.json", source_manifest)
    files.append({"local": "source-manifest.json",
                  "remote": publisher._expected_remote(generation_id, "source-manifest.json"),
                  "bytes": source_manifest_info[0], "sha256": source_manifest_info[1]})

    shard = b"synthetic shared table shard"
    table_id = "t0001_abcdef123456"
    shard_rel = f"shared/{table_id}/part000001.sqlite3"
    shard_info = add_file(root, "slices/" + shard_rel, shard, generation_id)
    value_chunk = b"external-value-chunk"
    value_rel = f"shared/{table_id}/value/part000001.sqlite3"
    value_info = add_file(root, "slices/" + value_rel, value_chunk, generation_id)
    data_files = [
        {"file": shard_rel, "bytes": shard_info["bytes"], "sha256": shard_info["sha256"]},
        {"file": value_rel, "bytes": value_info["bytes"], "sha256": value_info["sha256"]},
    ]

    schema_objects = [
        {"name": "records", "type": "table", "tbl_name": "records",
         "sql": "CREATE TABLE records(id INTEGER PRIMARY KEY, body BLOB)"},
    ]
    schema_sha = canonical_sha(schema_objects)
    columns = [
        {"cid": 0, "name": "id", "type": "INTEGER", "notnull": 0,
         "dflt_value": None, "pk": 1, "hidden": 0},
        {"cid": 1, "name": "body", "type": "BLOB", "notnull": 0,
         "dflt_value": None, "pk": 0, "hidden": 0},
    ]
    table = {
        "name": "records", "table_id": table_id, "columns": ["id", "body"],
        "column_schema": columns, "row_count": 1,
        "parts": [{"file": shard_rel, "bytes": shard_info["bytes"],
                   "sha256": shard_info["sha256"], "table_id": table_id,
                   "row_start": 0, "row_end": 1, "row_count": 1,
                   "page_count": 1, "page_size": 4096}],
        "foreign_keys": [], "row_stream_sha256": digest(b"row stream proof"),
        "archive_metadata_tables": {"rows": "_archive_rows", "external_cells": "_archive_external_cells"},
        "rowid_kind": "rowid", "source_rowid_column": "rowid",
    }

    by_mode = []
    by_rule = []
    for index in range(modes):
        name = f"mode_{index:02d}.sqlite3"
        local = f"slices/by-mode/{name}"
        info = add_selector(root, local, data_files, generation_id)
        row = {"file": name, "bytes": info["bytes"], "sha256": info["sha256"],
               "analysis_set": f"analysis_mode_{index}", "matches": 1 if index < populated_modes else 0}
        by_mode.append(row)

    for mode_index in range(populated_modes):
        for rule_index in range(rules):
            name = f"mode_{mode_index:02d}__RULE_{rule_index:03d}.sqlite3"
            local = f"slices/by-rule/{name}"
            info = add_selector(root, local, data_files, generation_id)
            by_rule.append({"file": name, "bytes": info["bytes"], "sha256": info["sha256"],
                            "analysis_set": f"analysis_mode_{mode_index}", "matches": 1,
                            "rule_raw": f"RULE_{rule_index:03d}"})

    distinct_modes = populated_modes
    distinct_rules = rules
    product = distinct_modes * distinct_rules
    counts = {
        "mode_files": modes,
        "rule_files": product,
        "distinct_modes_with_matches": distinct_modes,
        "distinct_rules": distinct_rules,
        "rule_mode_product": product,
    }
    slices_manifest = {
        "version": 2,
        "role": "lossless_sqlite_shards",
        "snapshot_identifier": generation_id,
        "source_sha256": source_sha,
        "source_schema_sha256": schema_sha,
        "schema_objects": schema_objects,
        "tables": [table],
        "external_values": [{"file": value_rel, "bytes": value_info["bytes"],
                              "sha256": value_info["sha256"], "table_id": table_id,
                              "table_name": "records", "chunk_count": 1,
                              "cell_count": 1, "page_count": 1, "page_size": 4096}],
        "by_mode": by_mode,
        "by_rule": by_rule,
        "counts": counts,
    }
    slice_manifest_info = write_json(root / "slices/manifest.json", slices_manifest)

    sqlite_verification = {
        "status": "verified", "snapshot_identifier": generation_id,
        "source_sha256": source_sha, "source_schema_sha256": schema_sha,
        "table_count": 1, "row_counts": {"records": 1}, "file_count": 2,
        "coverage": {"all_tables": True, "all_rows": True, "all_columns": True,
                     "all_values": True, "external_values": True},
    }
    receipt_info = write_json(root / "slices/verification.json", sqlite_verification)
    selector_verification = {
        "status": "verified", "snapshot_identifier": generation_id,
        "source_sha256": source_sha, "all_shared_files_reachable": True,
        "mode_files": modes, "rule_files": product, "rule_mode_product": product,
        "distinct_modes_with_matches": distinct_modes, "distinct_rules": distinct_rules,
        "all_mode_matches": True, "all_rule_matches": True,
    }
    selector_info = write_json(root / "slices/selectors-verification.json", selector_verification)

    piece_map = {}
    for index in range(xlsx_pieces):
        name = f"records_{index:03d}.xlsx"
        raw = f"synthetic xlsx piece {index}".encode()
        piece_map[name] = {"name": name, "bytes": len(raw), "sha256": digest(raw)}
        files.append(add_file(root, f"xlsx/{name}", raw, generation_id))
    xlsx_source = {"path": str(root / "source.sqlite3"),
                   "bytes": source["bytes"], "sha256": source_sha}
    xlsx_counts = {
        "exported_tables": 1,
        "exported_rows": 1,
        "exported_cells": 2,
        "chunks": 0,
        "pieces": xlsx_pieces,
        "source_bytes": source["bytes"],
    }
    xlsx_index = {
        'row_identity_format': 'source_rowid_column_v1',
        "status": "verified", "verification_status": "verified",
        "snapshot_identifier": generation_id, "snapshot_id": generation_id,
        "snapshot_sha256": source_sha, "source": xlsx_source,
        "pieces": list(piece_map.values()), "piece_names": list(piece_map),
        "counts": xlsx_counts,
        "verification_receipt": "verification.json",
    }
    xlsx_index_info = write_json(root / "xlsx/index.json", xlsx_index)
    xlsx_files = [*piece_map.values(), {"name": "index.json", "bytes": xlsx_index_info[0],
                                        "sha256": xlsx_index_info[1]}]
    xlsx_manifest = {
        "version": 1, "status": "verified", "snapshot_identifier": generation_id,
        "snapshot_sha256": source_sha, "source": xlsx_source,
        "counts": xlsx_counts, "files": xlsx_files,
        "verification_receipt": "verification.json",
    }
    xlsx_receipt = {
        'source_rowids_verified': True,
        "version": 1, "status": "verified", "snapshot_identifier": generation_id,
        "snapshot_sha256": source_sha, "source": xlsx_source,
        "counts": xlsx_counts, "files": xlsx_files,
        "manifest": "manifest.json", "index": "index.json",
    }
    xlsx_manifest_info = write_json(root / "xlsx/manifest.json", xlsx_manifest)
    xlsx_receipt_info = write_json(root / "xlsx/verification.json", xlsx_receipt)

    for local, info in (
        ("slices/manifest.json", slice_manifest_info),
        ("slices/verification.json", receipt_info),
        ("slices/selectors-verification.json", selector_info),
        ("xlsx/manifest.json", xlsx_manifest_info),
        ("xlsx/index.json", xlsx_index_info),
        ("xlsx/verification.json", xlsx_receipt_info),
    ):
        files.append({"local": local, "remote": publisher._expected_remote(generation_id, local),
                      "bytes": info[0], "sha256": info[1]})
    files.extend([{"local": f"slices/by-mode/{row['file']}",
                   "remote": publisher._expected_remote(generation_id, f"slices/by-mode/{row['file']}"),
                   "bytes": row["bytes"], "sha256": row["sha256"]} for row in by_mode])
    files.extend([{"local": f"slices/by-rule/{row['file']}",
                   "remote": publisher._expected_remote(generation_id, f"slices/by-rule/{row['file']}"),
                   "bytes": row["bytes"], "sha256": row["sha256"]} for row in by_rule])
    files.extend([shard_info, value_info])
    # The helper-created rows for shard/value paths already use their actual local paths.

    plan_counts = {**counts, "xlsx_pieces": xlsx_pieces}
    plan = {
        "version": 1, "generation_id": generation_id, "captured_at": None,
        "captured_at_kind": "unknown_legacy_snapshot", "source": source,
        "counts": plan_counts, "files": files,
        "sqlite_verification": sqlite_verification,
        "sqlite_verification_path": "slices/verification.json",
        "selector_verification": selector_verification,
        "xlsx_verification": xlsx_receipt,
    }
    write_json(root / "generation-plan.json", plan)
    return plan


class MemoryRemote:
    def __init__(self):
        self.objects = {}
        self.copy_calls = []
        self.readback_calls = []
        self.fail_remaining = {}
        self.stat_errors = {}
        self.stat_counts = {}
        self.mutate_latest_on_stat = None
        self.latest_path = None

    def stat(self, remote_path):
        self.stat_counts[remote_path] = self.stat_counts.get(remote_path, 0) + 1
        if remote_path in self.stat_errors:
            raise self.stat_errors[remote_path]
        if remote_path == self.latest_path and self.mutate_latest_on_stat == self.stat_counts[remote_path]:
            self.objects[remote_path] = b"concurrent writer changed latest payload"
        raw = self.objects.get(remote_path)
        if raw is None:
            return None
        return {"IsDir": False, "Size": len(raw)}

    def readback(self, remote_path):
        self.readback_calls.append(remote_path)
        raw = self.objects.get(remote_path)
        if raw is None:
            raise publisher.PublishError("REMOTE_OBJECT_MISSING")
        return len(raw), digest(raw)

    def readback_bytes(self, remote_path, expected_bytes, expected_sha):
        raw = self.objects.get(remote_path)
        if raw is None or (len(raw), digest(raw)) != (expected_bytes, expected_sha):
            raise publisher.PublishError("REMOTE_READBACK_MISMATCH")
        return raw

    def copyto(self, source, remote_path, *, immutable):
        self.copy_calls.append((remote_path, immutable))
        remaining = self.fail_remaining.get(remote_path, 0)
        if remaining:
            self.fail_remaining[remote_path] = remaining - 1
            raise publisher.TemporaryRemoteError("REMOTE_TEMPORARY")
        raw = Path(source).read_bytes()
        if immutable and remote_path in self.objects:
            raise publisher.PublishError("REMOTE_UPLOAD_FAILED")
        self.objects[remote_path] = raw


class FullDataPublishTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name).resolve()
        self.state = self.root / "state"
        self.state.mkdir(mode=0o700)
        self.fake = MemoryRemote()

    def tearDown(self):
        self.tmp.cleanup()

    def build(self, *, modes=16, rules=20, populated_modes=None, pieces=2, generation_id=GENERATION):
        generation_dir = self.root / ("generation-" + generation_id)
        generation_dir.mkdir(mode=0o700)
        make_generation(generation_dir, modes=modes, rules=rules, populated_modes=populated_modes,
                        xlsx_pieces=pieces, generation_id=generation_id)
        return generation_dir

    def remote_path(self, relative, generation_id=GENERATION):
        return f"mock:database/{relative}" if relative == "latest.json" else f"mock:database/{relative}"

    def test_publishes_old36_new76_and_variable_xlsx_piece_counts(self):
        for generation_id, modes, rules, populated_modes, pieces in (
                (GENERATION, 16, 20, 1, 1),
                ("20261002T120100Z-abc012ef", 16, 20, 3, 5)):
            generation_dir = self.build(modes=modes, rules=rules, populated_modes=populated_modes,
                                        pieces=pieces, generation_id=generation_id)
            copy_start = len(self.fake.copy_calls)
            args = ["--generation-dir", str(generation_dir), "--generation-id", generation_id,
                    "--remote", "mock:database", "--state-dir", str(self.state)]
            with patch.object(publisher, "Rclone", return_value=self.fake), \
                    patch.object(publisher.time, "sleep"), \
                    contextlib.redirect_stdout(io.StringIO()) as output, \
                    contextlib.redirect_stderr(io.StringIO()) as error:
                result = publisher.main(args)
            self.assertEqual(result, 0, error.getvalue())
            published = json.loads(output.getvalue())
            self.assertEqual(published["counts"]["mode_files"], 16)
            self.assertEqual(published["counts"]["rule_files"], rules * populated_modes)
            self.assertEqual(published["counts"]["xlsx_pieces"], pieces)
            index_path = f"mock:database/generations/{generation_id}/index.json"
            index = json.loads(self.fake.objects[index_path])
            self.assertTrue(index["verification"]["full_readback"])
            self.assertTrue(index["sqlite_verification"]["coverage"]["all_values"])
            plan = json.loads((generation_dir / "generation-plan.json").read_text())
            self.assertEqual(index["sqlite_verification"], plan["sqlite_verification"])
            self.assertEqual(index["selector_verification"], plan["selector_verification"])
            self.assertEqual(index["xlsx_verification"], plan["xlsx_verification"])
            for receipt_name, local in (
                    ("sqlite", "slices/verification.json"),
                    ("selectors", "slices/selectors-verification.json"),
                    ("xlsx", "xlsx/verification.json")):
                plan_row = next(row for row in plan["files"] if row["local"] == local)
                self.assertEqual(index["verification_receipt_files"][receipt_name], {
                    "local": local, "remote": plan_row["remote"],
                    "bytes": plan_row["bytes"], "sha256": plan_row["sha256"],
                })
            self.assertIsNone(index["captured_at"])
            self.assertEqual(index["captured_at_kind"], "unknown_legacy_snapshot")
            self.assertEqual(self.fake.objects["mock:database/latest.json"] and
                             json.loads(self.fake.objects["mock:database/latest.json"])["generation_id"], generation_id)
            copies = [path for path, _immutable in self.fake.copy_calls[copy_start:]]
            self.assertLess(copies.index(f"mock:database/xlsx-full/generations/{generation_id}/verification.json"),
                            copies.index(f"mock:database/xlsx-full/generations/{generation_id}/index.json"))
            self.assertLess(copies.index(f"mock:database/generations/{generation_id}/index.json"),
                            copies.index("mock:database/latest.json"))

    def test_restart_skips_matching_objects_and_does_not_duplicate_uploads(self):
        generation_dir = self.build(modes=2, rules=2, pieces=1)
        plan = json.loads((generation_dir / "generation-plan.json").read_text())
        args = ["--generation-dir", str(generation_dir), "--generation-id", GENERATION,
                "--remote", "mock:database", "--state-dir", str(self.state)]
        with patch.object(publisher, "Rclone", return_value=self.fake), patch.object(publisher.time, "sleep"), \
                contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(publisher.main(args), 0)
            first_calls = list(self.fake.copy_calls)
            readback_start = len(self.fake.readback_calls)
            self.assertEqual(publisher.main(args), 0)
        self.assertEqual(self.fake.copy_calls, first_calls)
        readbacks = self.fake.readback_calls[readback_start:]
        for item in plan["files"]:
            self.assertIn(f"mock:database/{item['remote']}", readbacks)

    def test_publish_files_uses_bounded_workers_and_parent_thread_progress(self):
        root = self.root / "parallel-root"
        root.mkdir()
        items = []
        for index in range(7):
            local = f"part-{index}.bin"
            raw = f"payload-{index}".encode()
            (root / local).write_bytes(raw)
            items.append({"local": local, "remote": f"objects/{local}",
                          "bytes": len(raw), "sha256": digest(raw)})

        class BlockingRemote:
            def __init__(self):
                self.objects = {}
                self.lock = threading.Lock()
                self.active = 0
                self.max_active = 0
                self.copy_starts = 0
                self.first_wave = threading.Barrier(3)
                self.readbacks = []

            def stat(self, remote_path):
                with self.lock:
                    raw = self.objects.get(remote_path)
                return None if raw is None else {"IsDir": False, "Size": len(raw)}

            def readback(self, remote_path):
                with self.lock:
                    raw = self.objects[remote_path]
                    self.readbacks.append(remote_path)
                return len(raw), digest(raw)

            def copyto(self, source, remote_path, *, immutable):
                with self.lock:
                    self.active += 1
                    self.max_active = max(self.max_active, self.active)
                    self.copy_starts += 1
                    first_wave = self.copy_starts <= 3
                try:
                    # The first wave cannot finish until all N workers have
                    # entered copyto, proving actual overlap deterministically.
                    if first_wave:
                        self.first_wave.wait(timeout=5)
                    raw = Path(source).read_bytes()
                    with self.lock:
                        self.objects[remote_path] = raw
                finally:
                    with self.lock:
                        self.active -= 1

        remote = BlockingRemote()
        progress_path = self.state / "parallel.progress.json"
        progress = {"path": progress_path, "remote": "mock:database", "receipts": {}}
        caller_thread = threading.get_ident()
        callback_threads = []

        with patch.dict(os.environ, {"IKARING_ARCHIVE_PUBLISH_FILE_WORKERS": "3"}, clear=False):
            count = publisher._publish_files(
                remote, root, self.state, items, progress,
                on_verified=lambda _count, _total, _item: callback_threads.append(threading.get_ident()),
            )

        self.assertEqual(count, len(items))
        self.assertGreaterEqual(remote.max_active, 2)
        self.assertLessEqual(remote.max_active, 3)
        self.assertEqual(callback_threads, [caller_thread] * len(items))
        self.assertEqual(set(remote.readbacks), {f"mock:database/{item['remote']}" for item in items})
        saved = json.loads(progress_path.read_text())
        self.assertNotIn("_runtime_lock", saved)
        self.assertEqual(saved["receipts"], {
            item["local"]: {"bytes": item["bytes"], "sha256": item["sha256"], "verified": True}
            for item in items
        })

    def test_default_worker_count_preserves_sequential_order_and_duplicate_rejected(self):
        items = [
            {"local": f"part-{index}.bin", "remote": f"objects/part-{index}.bin",
             "bytes": 1, "sha256": "a" * 64}
            for index in range(3)
        ]
        seen = []
        progress = {"path": self.state / "seq.progress.json", "remote": "mock:database", "receipts": {}}
        with patch.dict(os.environ, {}, clear=True), patch.object(
                publisher, "_publish_file", side_effect=lambda _client, _root, _state, item, _progress: seen.append(item["local"])):
            self.assertEqual(publisher._publish_files(object(), self.root, self.state, items, progress), 3)
        self.assertEqual(seen, [item["local"] for item in items])

        with patch.dict(os.environ, {"IKARING_ARCHIVE_PUBLISH_FILE_WORKERS": "2"}, clear=True):
            with self.assertRaises(publisher.PublishError) as caught:
                publisher._publish_files(object(), self.root, self.state, [items[0], dict(items[0])], progress)
        self.assertEqual(caught.exception.category, "FILE_INVENTORY_DUPLICATE")

        with patch.dict(os.environ, {"IKARING_ARCHIVE_PUBLISH_FILE_WORKERS": "5"}, clear=True), \
                patch.object(publisher, "_publish_file") as publish_file:
            with self.assertRaises(publisher.PublishError) as caught:
                publisher._publish_files(object(), self.root, self.state, items, progress)
        self.assertEqual(caught.exception.category, "CONFIG_ERROR")
        publish_file.assert_not_called()

    def test_corrupt_parallel_file_blocks_generation_controls_and_keeps_other_receipts(self):
        generation_dir = self.build(modes=2, rules=2, pieces=1)
        before = {
            path.relative_to(generation_dir).as_posix(): digest(path.read_bytes())
            for path in generation_dir.rglob("*") if path.is_file()
        }
        plan = json.loads((generation_dir / "generation-plan.json").read_text())
        bad_remote = f"mock:database/{plan['files'][0]['remote']}"
        wave = threading.Barrier(3)

        class ParallelCorruptRemote(MemoryRemote):
            def copyto(self, source, remote_path, *, immutable):
                wave.wait(timeout=5)
                if remote_path == bad_remote:
                    raw = Path(source).read_bytes() + b"corruption"
                    self.copy_calls.append((remote_path, immutable))
                    self.objects[remote_path] = raw
                    return
                return super().copyto(source, remote_path, immutable=immutable)

        remote = ParallelCorruptRemote()
        args = ["--generation-dir", str(generation_dir), "--generation-id", GENERATION,
                "--remote", "mock:database", "--state-dir", str(self.state)]
        with patch.dict(os.environ, {"IKARING_ARCHIVE_PUBLISH_FILE_WORKERS": "3"}, clear=False), \
                patch.object(publisher, "Rclone", return_value=remote), \
                patch.object(publisher.time, "sleep"), contextlib.redirect_stdout(io.StringIO()), \
                contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(publisher.main(args), 1)

        self.assertNotIn(f"mock:database/generations/{GENERATION}/index.json", remote.objects)
        self.assertNotIn("mock:database/latest.json", remote.objects)
        progress = json.loads((self.state / f"{GENERATION}.progress.json").read_text())
        self.assertGreaterEqual(len(progress["receipts"]), 2)
        self.assertNotIn(plan["files"][0]["local"], progress["receipts"])
        after = {
            path.relative_to(generation_dir).as_posix(): digest(path.read_bytes())
            for path in generation_dir.rglob("*") if path.is_file()
        }
        self.assertEqual(after, before)

    def test_interrupted_attempt_resumes_without_reuploading_completed_object(self):
        generation_dir = self.build(modes=2, rules=2, pieces=1)
        plan = json.loads((generation_dir / "generation-plan.json").read_text())
        failing_local = "slices/shared/t0001_abcdef123456/part000001.sqlite3"
        failing_remote = f"mock:database/{publisher._expected_remote(GENERATION, failing_local)}"
        source_remote = f"mock:database/{publisher._expected_remote(GENERATION, 'source.sqlite3')}"
        self.fake.fail_remaining[failing_remote] = publisher.RETRY_LIMIT
        args = ["--generation-dir", str(generation_dir), "--generation-id", GENERATION,
                "--remote", "mock:database", "--state-dir", str(self.state)]
        first_error = io.StringIO()
        first_output = io.StringIO()
        with patch.object(publisher, "Rclone", return_value=self.fake), patch.object(publisher.time, "sleep") as sleep_mock, \
                contextlib.redirect_stdout(first_output), contextlib.redirect_stderr(first_error):
            self.assertEqual(publisher.main(args), 1)
        self.assertIn("PUBLISH_FAILED REMOTE_RETRY_EXHAUSTED", first_error.getvalue())
        self.assertEqual(sleep_mock.call_count, publisher.RETRY_LIMIT - 1)
        source_uploads_after_failure = sum(path == source_remote for path, _ in self.fake.copy_calls)
        self.assertEqual(source_uploads_after_failure, 1)
        with patch.object(publisher, "Rclone", return_value=self.fake), patch.object(publisher.time, "sleep"), \
                contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(publisher.main(args), 0)
        self.assertEqual(sum(path == source_remote for path, _ in self.fake.copy_calls), 1)
        self.assertEqual(len(plan["files"]), len({row["local"] for row in plan["files"]}))

    def test_existing_mismatch_fails_without_changing_latest(self):
        generation_dir = self.build(modes=2, rules=2, pieces=1)
        plan = json.loads((generation_dir / "generation-plan.json").read_text())
        first_remote = f"mock:database/{plan['files'][0]['remote']}"
        first_size = next(row["bytes"] for row in plan["files"] if row["remote"] == plan["files"][0]["remote"])
        self.fake.objects[first_remote] = b"x" * first_size
        latest_path = "mock:database/latest.json"
        old_latest = b'{"generation_id":"older","index_sha256":"old"}\n'
        self.fake.objects[latest_path] = old_latest
        args = ["--generation-dir", str(generation_dir), "--generation-id", GENERATION,
                "--remote", "mock:database", "--state-dir", str(self.state)]
        with patch.object(publisher, "Rclone", return_value=self.fake), \
                patch.object(publisher.time, "sleep"), contextlib.redirect_stdout(io.StringIO()), \
                contextlib.redirect_stderr(io.StringIO()):
            result = publisher.main(args)
        self.assertEqual(result, 1)
        self.assertEqual(self.fake.objects[latest_path], old_latest)
        self.assertEqual(self.fake.objects[first_remote], b"x" * first_size)
        self.assertFalse(any(path == latest_path for path, _ in self.fake.copy_calls))

    def test_rejects_traversal_and_symlink_before_remote_mutation(self):
        for use_symlink in (False, True):
            generation_dir = self.build(modes=2, rules=2, pieces=1,
                                        generation_id="20261002T120200Z-abc012ef" if use_symlink else GENERATION)
            if use_symlink:
                target = generation_dir / "xlsx/records_000.xlsx"
                target.unlink()
                target.symlink_to(generation_dir / "source.sqlite3")
            else:
                plan_path = generation_dir / "generation-plan.json"
                plan = json.loads(plan_path.read_text())
                plan["files"][0]["local"] = "../escape"
                write_json(plan_path, plan)
            before = dict(self.fake.objects)
            args = ["--generation-dir", str(generation_dir),
                    "--generation-id", "20261002T120200Z-abc012ef" if use_symlink else GENERATION,
                    "--remote", "mock:database", "--state-dir", str(self.state)]
            with patch.object(publisher, "Rclone", return_value=self.fake), \
                    patch.object(publisher.time, "sleep"), contextlib.redirect_stdout(io.StringIO()), \
                    contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(publisher.main(args), 1)
            self.assertEqual(self.fake.objects, before)

    def test_rejects_false_full_coverage_proof_preflight(self):
        generation_dir = self.build(modes=2, rules=2, pieces=1)
        proof_path = generation_dir / "slices/verification.json"
        proof = json.loads(proof_path.read_text())
        proof["coverage"]["all_values"] = False
        proof_info = write_json(proof_path, proof)
        plan_path = generation_dir / "generation-plan.json"
        plan = json.loads(plan_path.read_text())
        plan["sqlite_verification"] = proof
        for row in plan["files"]:
            if row["local"] == "slices/verification.json":
                row["bytes"], row["sha256"] = proof_info
        write_json(plan_path, plan)
        args = ["--generation-dir", str(generation_dir), "--generation-id", GENERATION,
                "--remote", "mock:database", "--state-dir", str(self.state)]
        with patch.object(publisher, "Rclone", return_value=self.fake), \
                patch.object(publisher.time, "sleep"), contextlib.redirect_stdout(io.StringIO()), \
                contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(publisher.main(args), 1)
        self.assertEqual(self.fake.objects, {})

    def test_requires_explicit_sqlite_verification_path_preflight(self):
        generation_dir = self.build()
        plan_path = generation_dir / "generation-plan.json"
        plan = json.loads(plan_path.read_text())
        del plan["sqlite_verification_path"]
        write_json(plan_path, plan)
        args = ["--generation-dir", str(generation_dir), "--generation-id", GENERATION,
                "--remote", "mock:database", "--state-dir", str(self.state)]
        with patch.object(publisher, "Rclone", return_value=self.fake), \
                patch.object(publisher.time, "sleep"), contextlib.redirect_stdout(io.StringIO()), \
                contextlib.redirect_stderr(io.StringIO()) as error:
            self.assertEqual(publisher.main(args), 1)
        self.assertIn("SQLITE_VERIFICATION_PATH_INVALID", error.getvalue())
        self.assertFalse(self.fake.objects)

    def test_rejects_missing_xlsx_source_snapshot_binding_preflight(self):
        generation_dir = self.build()
        receipt_path = generation_dir / "xlsx/verification.json"
        receipt = json.loads(receipt_path.read_text())
        del receipt["snapshot_sha256"]
        receipt_info = write_json(receipt_path, receipt)
        plan_path = generation_dir / "generation-plan.json"
        plan = json.loads(plan_path.read_text())
        plan["xlsx_verification"] = receipt
        for row in plan["files"]:
            if row["local"] == "xlsx/verification.json":
                row["bytes"], row["sha256"] = receipt_info
        write_json(plan_path, plan)
        args = ["--generation-dir", str(generation_dir), "--generation-id", GENERATION,
                "--remote", "mock:database", "--state-dir", str(self.state)]
        with patch.object(publisher, "Rclone", return_value=self.fake), \
                patch.object(publisher.time, "sleep"), contextlib.redirect_stdout(io.StringIO()), \
                contextlib.redirect_stderr(io.StringIO()) as error:
            self.assertEqual(publisher.main(args), 1)
        self.assertIn("XLSX_SOURCE_MISMATCH", error.getvalue())
        self.assertFalse(self.fake.objects)

    def test_rejects_missing_xlsx_verification_receipt_pointer_preflight(self):
        generation_dir = self.build()
        index_path = generation_dir / "xlsx/index.json"
        index = json.loads(index_path.read_text())
        del index["verification_receipt"]
        write_json(index_path, index)
        refresh_plan_file_entry(generation_dir, "xlsx/index.json")
        args = ["--generation-dir", str(generation_dir), "--generation-id", GENERATION,
                "--remote", "mock:database", "--state-dir", str(self.state)]
        with patch.object(publisher, "Rclone", return_value=self.fake), \
                patch.object(publisher.time, "sleep"), contextlib.redirect_stdout(io.StringIO()), \
                contextlib.redirect_stderr(io.StringIO()) as error:
            self.assertEqual(publisher.main(args), 1)
        self.assertIn("XLSX_INDEX_INVALID", error.getvalue())
        self.assertFalse(self.fake.objects)

    def test_rejects_missing_source_rowid_proof_before_any_upload(self):
        generation_dir = self.build()
        path = generation_dir / 'xlsx/verification.json'
        receipt = json.loads(path.read_text())
        del receipt['source_rowids_verified']
        info = write_json(path, receipt)
        plan_path = generation_dir / 'generation-plan.json'
        plan = json.loads(plan_path.read_text())
        plan['xlsx_verification'] = receipt
        for row in plan['files']:
            if row['local'] == 'xlsx/verification.json':
                row['bytes'], row['sha256'] = info
        write_json(plan_path, plan)
        args = ['--generation-dir', str(generation_dir), '--generation-id', GENERATION,
                '--remote', 'mock:database', '--state-dir', str(self.state)]
        with patch.object(publisher, 'Rclone', return_value=self.fake), \
                contextlib.redirect_stdout(io.StringIO()), \
                contextlib.redirect_stderr(io.StringIO()) as error:
            self.assertEqual(publisher.main(args), 1)
        self.assertIn('XLSX_ROW_IDENTITIES_NOT_VERIFIED', error.getvalue())
        self.assertFalse(self.fake.objects)

    def test_rejects_legacy_partial_slice_role(self):
        generation_dir = self.build(modes=1, rules=1, pieces=1)
        manifest_path = generation_dir / "slices/manifest.json"
        manifest = json.loads(manifest_path.read_text())
        manifest["role"] = "analysis_slice"
        write_json(manifest_path, manifest)
        refresh_plan_file_entry(generation_dir, "slices/manifest.json")
        args = ["--generation-dir", str(generation_dir), "--generation-id", GENERATION,
                "--remote", "mock:database", "--state-dir", str(self.state)]
        with patch.object(publisher, "Rclone", return_value=self.fake), \
                patch.object(publisher.time, "sleep"), contextlib.redirect_stdout(io.StringIO()), \
                contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(publisher.main(args), 1)
        self.assertEqual(self.fake.objects, {})

    def test_preserves_pinned_read_transaction_timestamp(self):
        generation_id = "20261002T121500Z-abc012ef"
        generation_dir = self.build(modes=1, rules=1, pieces=1, generation_id=generation_id)
        plan_path = generation_dir / "generation-plan.json"
        plan = json.loads(plan_path.read_text())
        plan["captured_at"] = "2026-10-02T12:15:00Z"
        plan["captured_at_kind"] = "pinned_read_transaction"
        write_json(plan_path, plan)
        args = ["--generation-dir", str(generation_dir), "--generation-id", generation_id,
                "--remote", "mock:database", "--state-dir", str(self.state)]
        with patch.object(publisher, "Rclone", return_value=self.fake), \
                patch.object(publisher.time, "sleep"), contextlib.redirect_stdout(io.StringIO()), \
                contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(publisher.main(args), 0)
        index = json.loads(self.fake.objects[f"mock:database/generations/{generation_id}/index.json"])
        latest = json.loads(self.fake.objects["mock:database/latest.json"])
        self.assertEqual(index["captured_at"], plan["captured_at"])
        self.assertEqual(index["captured_at_kind"], "pinned_read_transaction")
        self.assertEqual(latest["captured_at"], plan["captured_at"])

    def test_rejects_bool_sizes_counts_and_malformed_hashes(self):
        mutations = (
            lambda plan: plan["source"].update(bytes=True),
            lambda plan: plan["files"][0].update(bytes=True),
            lambda plan: plan["counts"].update(mode_files=True),
            lambda plan: plan["files"][0].update(sha256="g" * 64),
        )
        for index, mutate in enumerate(mutations):
            generation_id = f"20261002T12030{index}Z-abc012ef"
            generation_dir = self.build(modes=1, rules=1, pieces=1, generation_id=generation_id)
            plan_path = generation_dir / "generation-plan.json"
            plan = json.loads(plan_path.read_text())
            mutate(plan)
            write_json(plan_path, plan)
            args = ["--generation-dir", str(generation_dir), "--generation-id", generation_id,
                    "--remote", "mock:database", "--state-dir", str(self.state)]
            with self.subTest(index=index), patch.object(publisher, "Rclone", return_value=self.fake), \
                    patch.object(publisher.time, "sleep"), contextlib.redirect_stdout(io.StringIO()), \
                    contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(publisher.main(args), 1)
            self.assertEqual(self.fake.objects, {})

    def test_source_manifest_hash_mismatch_is_preflight_failure(self):
        generation_dir = self.build(modes=1, rules=1, pieces=1)
        manifest_path = generation_dir / "source-manifest.json"
        manifest = json.loads(manifest_path.read_text())
        manifest["raw_snapshot"]["sha256"] = "0" * 64
        write_json(manifest_path, manifest)
        refresh_plan_file_entry(generation_dir, "source-manifest.json")
        args = ["--generation-dir", str(generation_dir), "--generation-id", GENERATION,
                "--remote", "mock:database", "--state-dir", str(self.state)]
        with patch.object(publisher, "Rclone", return_value=self.fake), \
                patch.object(publisher.time, "sleep"), contextlib.redirect_stdout(io.StringIO()), \
                contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(publisher.main(args), 1)
        self.assertEqual(self.fake.objects, {})

    def test_selector_reference_set_must_cover_all_shared_value_files(self):
        generation_dir = self.build(modes=1, rules=1, pieces=1)
        selector_path = generation_dir / "slices/by-mode/mode_00.sqlite3"
        connection = sqlite3.connect(selector_path)
        try:
            connection.execute(
                "DELETE FROM shared_files WHERE file=?",
                ("shared/t0001_abcdef123456/value/part000001.sqlite3",),
            )
            connection.commit()
        finally:
            connection.close()
        args = ["--generation-dir", str(generation_dir), "--generation-id", GENERATION,
                "--remote", "mock:database", "--state-dir", str(self.state)]
        with patch.object(publisher, "Rclone", return_value=self.fake), \
                patch.object(publisher.time, "sleep"), contextlib.redirect_stdout(io.StringIO()), \
                contextlib.redirect_stderr(io.StringIO()) as error:
            self.assertEqual(publisher.main(args), 1)
        self.assertIn("SELECTOR_REACHABILITY_FAILED", error.getvalue())
        self.assertEqual(self.fake.objects, {})

    def test_preserves_previous_latest_as_full_local_and_remote_bytes(self):
        generation_dir = self.build(modes=1, rules=1, pieces=1)
        latest_path = "mock:database/latest.json"
        old_latest = b'{ "legacy" : [1, 2, 3], "padding":"' + (b"x" * 1024) + b'" }\n'
        self.fake.objects[latest_path] = old_latest
        args = ["--generation-dir", str(generation_dir), "--generation-id", GENERATION,
                "--remote", "mock:database", "--state-dir", str(self.state)]
        with patch.object(publisher, "Rclone", return_value=self.fake), patch.object(publisher.time, "sleep"), \
                contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(publisher.main(args), 0)
        local_history = self.state / "history" / f"{GENERATION}-previous-latest.json"
        remote_history = f"mock:database/generations/{GENERATION}/previous-latest.json"
        self.assertEqual(local_history.read_bytes(), old_latest)
        self.assertEqual(self.fake.objects[remote_history], old_latest)

    def test_stale_latest_compare_and_swap_fails_and_keeps_last_success(self):
        generation_dir = self.build(modes=1, rules=1, pieces=1)
        latest_path = "mock:database/latest.json"
        old_latest = b'{"generation_id":"older","index_sha256":"old"}\n'
        self.fake.objects[latest_path] = old_latest
        self.fake.latest_path = latest_path
        self.fake.mutate_latest_on_stat = 2
        current_path = self.state / "current_state.json"
        write_json(current_path, {"version": 1, "last_attempt": None,
                                  "last_success": "2026-10-01T00:00:00Z",
                                  "last_failure": None, "phase": "complete",
                                  "generation_id": "20261001T120000Z-abc012ef"})
        args = ["--generation-dir", str(generation_dir), "--generation-id", GENERATION,
                "--remote", "mock:database", "--state-dir", str(self.state)]
        with patch.object(publisher, "Rclone", return_value=self.fake), patch.object(publisher.time, "sleep"), \
                contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(publisher.main(args), 1)
        state = json.loads(current_path.read_text())
        self.assertEqual(state["last_success"], "2026-10-01T00:00:00Z")
        self.assertEqual(state["phase"], "failed")
        self.assertNotEqual(self.fake.objects[latest_path], old_latest)
        new_bytes = self.fake.objects[latest_path]
        self.assertEqual(new_bytes, b"concurrent writer changed latest payload")

    def test_lock_refusal_is_nonblocking(self):
        generation_dir = self.build(modes=1, rules=1, pieces=1)
        lock_path = self.state / ".nas-full-publish.lock"
        with lock_path.open("a+") as stream:
            import fcntl
            fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            args = ["--generation-dir", str(generation_dir), "--generation-id", GENERATION,
                    "--remote", "mock:database", "--state-dir", str(self.state)]
            with patch.object(publisher, "Rclone", return_value=self.fake), \
                    contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(publisher.main(args), 1)
            fcntl.flock(stream.fileno(), fcntl.LOCK_UN)
        self.assertEqual(self.fake.objects, {})

    def test_rclone_stat_only_accepts_explicit_missing_diagnostics(self):
        missing = subprocess.CompletedProcess(["rclone", "lsjson"], 3, b"", b"object not found")
        missing_dir = subprocess.CompletedProcess(["rclone", "lsjson"], 4, b"", b"dir not found")
        forbidden = subprocess.CompletedProcess(["rclone", "lsjson"], 3, b"", b"403 Forbidden")
        with patch.object(publisher, "run_stat_process", side_effect=[missing, missing_dir]):
            self.assertIsNone(publisher.Rclone().stat("mock:database/missing"))
            self.assertIsNone(publisher.Rclone().stat("mock:database/missing-dir"))
        with patch.object(publisher, "run_stat_process", side_effect=[forbidden]):
            with self.assertRaises(publisher.PublishError) as raised:
                publisher.Rclone().stat("mock:database/forbidden")
        self.assertEqual(raised.exception.category, "REMOTE_STAT_FAILED")


if __name__ == "__main__":
    unittest.main()
