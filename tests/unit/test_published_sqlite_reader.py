import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src/python"))
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "tests/unit"))

import test_full_data_delta_publish as delta_fixtures  # noqa: E402
from ikarchive.change_feed import install_change_feed  # noqa: E402
from ikarchive.writer_guards import install_writer_guards  # noqa: E402
from published_sqlite_reader import (  # noqa: E402
    PublishedSQLiteReader,
    PublishedSQLiteReaderError,
)


BASELINE_ID = delta_fixtures.BASELINE_ID
REMOTE = delta_fixtures.REMOTE
BASELINE_ONLY_LATEST = f"{REMOTE}/latest.json"


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _write_rich_sources(case):
    """Use a tiny artificial source with typed values, hidden rowids and FKs."""
    for path in (case.baseline_path, case.current_path):
        try:
            path.unlink()
        except FileNotFoundError:
            pass
    ddl = (
        "CREATE TABLE records(id INTEGER PRIMARY KEY, payload, ratio REAL, blob_value BLOB)",
        "CREATE TABLE related(name TEXT PRIMARY KEY, record_id INTEGER REFERENCES records(id), payload BLOB)",
        "CREATE TABLE matches(account TEXT, kind TEXT, match_key TEXT, payload BLOB)",
        "CREATE TABLE match_classification(account TEXT, kind TEXT, match_key TEXT, analysis_set TEXT, rule_raw TEXT)",
    )
    rows = (
        "INSERT INTO records VALUES(?,?,?,?)",
        "INSERT INTO related VALUES(?,?,?)",
        "INSERT INTO matches VALUES(?,?,?,?)",
        "INSERT INTO match_classification VALUES(?,?,?,?,?)",
    )
    data = (
        (-42, "original", 1.5, b"\x00base\xff"),
        ("foreign", -42, b"related\x00"),
        ("acct", "regular", "match-1", b"match\x00bytes"),
        ("acct", "regular", "match-1", "xmatch", "TOWER"),
    )
    for path in (case.baseline_path, case.current_path):
        conn = sqlite3.connect(path)
        try:
            conn.execute("PRAGMA foreign_keys=ON")
            for statement in ddl:
                conn.execute(statement)
            for statement, values in zip(rows, data):
                conn.execute(statement, values)
            conn.commit()
        finally:
            conn.close()

    conn = sqlite3.connect(case.current_path)
    try:
        conn.execute("PRAGMA recursive_triggers=ON")
        conn.execute("PRAGMA foreign_keys=ON")
        install_change_feed(conn)
        install_writer_guards(conn)
        conn.execute(
            "UPDATE records SET payload=?, blob_value=? WHERE id=?",
            ("captured\x00text", b"\x00first\xff", -42),
        )
        conn.execute("UPDATE related SET payload=? WHERE name='foreign'", (b"\x00first",))
        conn.execute("UPDATE matches SET payload=? WHERE match_key='match-1'", (b"\x00captured",))
        conn.commit()
    finally:
        conn.close()

    raw = case.baseline_path.read_bytes()
    case.baseline_sha = _sha(raw)
    manifest = {
        "storage": "plaintext", "encryption": None,
        "raw_snapshot": {"basename": case.baseline_path.name, "bytes": len(raw),
                         "sha256": case.baseline_sha, "quick_check": "ok"},
        "verification": {"quick_check": "ok"},
    }
    case.manifest_path.write_bytes(delta_fixtures._compact(manifest))
    case._install_baseline_remote()


class PublishedSQLiteReaderTests(unittest.TestCase):
    def setUp(self):
        self.case = delta_fixtures.FullDataDeltaPublishTests(
            "test_initial_and_incremental_delta_append_chain_and_reread_every_file"
        )
        self.case.setUp()
        _write_rich_sources(self.case)
        self.control = self.case.root / "publisher-control"
        self.delta_root = self.case.root / "local-deltas"
        self.control.mkdir()
        self.delta_root.mkdir()
        self.baseline_package = self.case.root / "baseline-shards"
        delta_fixtures._make_lossless_baseline_package(
            self.case.baseline_path, self.baseline_package, BASELINE_ID,
        )
        self._sync_baseline_remote()

    def _sync_baseline_remote(self):
        from ikarchive.shard_reader import LosslessShardReader
        with LosslessShardReader(self.baseline_package, expected_generation=BASELINE_ID) as reader:
            records = dict(reader._verified_records)

        index_remote = f"{REMOTE}/generations/{BASELINE_ID}/index.json"
        index = json.loads(self.case.remote.objects[index_remote].decode("utf-8"))

        files = []
        for local in ("source.sqlite3", "source-manifest.json"):
            if local == "source.sqlite3":
                files.append({
                    "local": local,
                    "remote": f"unified/generations/{BASELINE_ID}/archive.sqlite3",
                    "bytes": self.case.baseline_path.stat().st_size,
                    "sha256": self.case.baseline_sha,
                })
            else:
                raw_manifest = self.case.manifest_path.read_bytes()
                files.append({
                    "local": local,
                    "remote": f"unified/generations/{BASELINE_ID}/source-manifest.json",
                    "bytes": len(raw_manifest),
                    "sha256": _sha(raw_manifest),
                })
        for rel in sorted(records):
            files.append({
                "local": f"slices/{rel}",
                "remote": f"slices/generations/{BASELINE_ID}/{rel}",
                "bytes": records[rel]["bytes"],
                "sha256": records[rel]["sha256"],
            })
        for xlsx_name in ("index.json", "manifest.json", "verification.json"):
            files.append({
                "local": f"xlsx/{xlsx_name}",
                "remote": f"xlsx-full/generations/{BASELINE_ID}/{xlsx_name}",
                "bytes": 1,
                "sha256": _sha(f"xlsx/{xlsx_name}".encode()),
            })
        index["files"] = files
        index["verification"]["file_count"] = len(files)
        index_raw = delta_fixtures._compact(index)
        self.case.remote.objects[index_remote] = index_raw

        latest_remote = BASELINE_ONLY_LATEST
        latest = json.loads(self.case.remote.objects[latest_remote].decode("utf-8"))
        latest["index_sha256"] = _sha(index_raw)
        self.case.remote.objects[latest_remote] = delta_fixtures._compact(latest)

    def tearDown(self):
        self.case.tearDown()

    def _copy_publisher_controls(self, generations=()):
        entries = [BASELINE_ONLY_LATEST, f"{REMOTE}/generations/{BASELINE_ID}/index.json"]
        for generation_id in generations:
            entries.extend((
                f"{REMOTE}/deltas/generations/{generation_id}/index.json",
                f"{REMOTE}/deltas/generations/{generation_id}/delta-plan.json",
            ))
        for remote_path in entries:
            raw = self.case.remote.objects[remote_path]
            relative = remote_path[len(REMOTE) + 1:]
            path = self.control / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(raw)

    def _publish_reset_and_incremental(self):
        first_id = "20261003T010101Z-a0010001"
        second_id = "20261003T010202Z-a0010002"
        first_dir, first_plan = self.case.prepare(first_id)
        self.assertEqual(self.case.publish(first_dir)["status"], "complete")

        conn = sqlite3.connect(self.case.current_path)
        try:
            conn.execute("PRAGMA recursive_triggers=ON")
            conn.execute("PRAGMA foreign_keys=ON")
            conn.execute("UPDATE records SET payload=?, ratio=?, blob_value=? WHERE id=-42",
                         (b"final\x00blob", 2.25, b"\x00second\xff"))
            conn.execute("UPDATE related SET payload=? WHERE name='foreign'", (b"\xfffinal\x00",))
            conn.execute("UPDATE matches SET payload=? WHERE match_key='match-1'", (b"selected\x00final",))
            conn.commit()
        finally:
            conn.close()
        second_dir, _second_plan = self.case.prepare(second_id, previous=first_plan)
        self.assertEqual(self.case.publish(second_dir)["status"], "complete")

        self._copy_publisher_controls((first_id, second_id))
        for generation_id, generation_dir in ((first_id, first_dir), (second_id, second_dir)):
            local = self.delta_root / generation_id
            shutil.copytree(generation_dir, local)
            index_path = local / "index.json"
            index_path.write_bytes(self.case.remote.objects[
                f"{REMOTE}/deltas/generations/{generation_id}/index.json"
            ])
        return first_id, second_id

    def test_baseline_only_pins_latest_and_exposes_full_baseline_metadata(self):
        self._copy_publisher_controls()
        latest_raw = (self.control / "latest.json").read_bytes()
        package_before = delta_fixtures._tree_snapshot(self.baseline_package)
        with PublishedSQLiteReader(
            self.control, self.baseline_package, self.delta_root,
            expected_generation_id=BASELINE_ID,
        ) as reader:
            self.assertEqual(reader.generation_id, BASELINE_ID)
            self.assertEqual(reader.baseline_generation_id, BASELINE_ID)
            self.assertEqual(reader.pinned_latest_sha256, _sha(latest_raw))
            self.assertTrue(reader.published_control_binding_verified)
            self.assertFalse(reader.published_deltas_verified)
            self.assertEqual(reader.row_count("records"), 1)
            self.assertEqual(list(reader.iter_rows("records"))[0][2][1], "original")
            manifest = json.loads((self.baseline_package / "manifest.json").read_text())
            self.assertEqual(reader.schema_objects(), manifest["schema_objects"])
        self.assertEqual(delta_fixtures._tree_snapshot(self.baseline_package), package_before)
        with self.assertRaises(RuntimeError):
            _ = reader.generation_id

    def test_all_public_apis_require_open_context(self):
        self._copy_publisher_controls()
        reader = PublishedSQLiteReader(self.control, self.baseline_package, self.delta_root)
        api_calls = (
            lambda: reader.generation_id,
            lambda: reader.baseline_generation_id,
            lambda: reader.pinned_latest_sha256,
            lambda: reader.captured_at,
            lambda: reader.published_control_binding_verified,
            lambda: reader.published_deltas_verified,
            lambda: reader.tables(),
            lambda: reader.columns("records"),
            lambda: reader.foreign_keys("records"),
            lambda: reader.schema_objects(),
            lambda: reader.row_count("records"),
            lambda: reader.iter_rows("records"),
            lambda: reader.iter_selected_matches("xmatch"),
        )
        for call in api_calls:
            with self.subTest(call=call), self.assertRaises(RuntimeError):
                call()

        reader.__enter__()
        reader.__exit__(None, None, None)
        for call in api_calls:
            with self.subTest(call=call, state="closed"), self.assertRaises(RuntimeError):
                call()

    def test_keyboard_interrupt_during_enter_closes_opened_readers_and_temp_state(self):
        self._copy_publisher_controls()
        reader = PublishedSQLiteReader(self.control, self.baseline_package, self.delta_root)
        opened = {}

        def interrupt_after_readers_open():
            opened["baseline"] = reader._baseline_reader
            opened["delta"] = reader._delta_reader
            opened["temp_root"] = reader._delta_reader._temp_root
            raise KeyboardInterrupt("controlled test interrupt")

        with patch.object(PublishedSQLiteReader, "_validate_reader_tail",
                          side_effect=interrupt_after_readers_open):
            with self.assertRaisesRegex(KeyboardInterrupt, "controlled test interrupt"):
                reader.__enter__()

        self.assertFalse(reader._active)
        self.assertIsNone(reader._stack)
        self.assertFalse(opened["baseline"]._active)
        self.assertFalse(opened["delta"]._active)
        self.assertIsNone(opened["delta"]._connection)
        self.assertIsNone(opened["delta"]._temp)
        self.assertFalse(opened["temp_root"].exists())

    def test_reset_then_incremental_binds_typed_rows_rowids_schema_and_selectors(self):
        _first_id, second_id = self._publish_reset_and_incremental()
        with PublishedSQLiteReader(
            self.control, self.baseline_package, self.delta_root,
            expected_generation_id=second_id,
        ) as reader:
            self.assertTrue(reader.published_control_binding_verified)
            self.assertTrue(reader.published_deltas_verified)
            self.assertEqual(reader.generation_id, second_id)
            records = list(reader.iter_rows("records"))
            by_rowid = {rowid: values for _ordinal, rowid, values in records}
            self.assertEqual(set(by_rowid), {-42})
            self.assertEqual(by_rowid[-42][0], -42)
            self.assertEqual(by_rowid[-42][1], b"final\x00blob")
            self.assertEqual(by_rowid[-42][3], b"\x00second\xff")
            self.assertIs(type(by_rowid[-42][2]), float)
            self.assertEqual(by_rowid[-42][2], 2.25)
            related = list(reader.iter_rows("related"))
            self.assertEqual(related[0][1], 1)
            self.assertEqual(related[0][2][2], b"\xfffinal\x00")
            self.assertEqual(reader.foreign_keys("related")[0]["table"], "records")
            self.assertEqual(len(list(reader.iter_selected_matches("xmatch"))), 1)
            latest = json.loads((self.control / "latest.json").read_text())
            self.assertEqual(reader.schema_objects(), latest["schemas"])

    def test_latest_index_sha_mismatch_is_rejected(self):
        self._copy_publisher_controls()
        path = self.control / "latest.json"
        latest = json.loads(path.read_text())
        latest["index_sha256"] = "0" * 64
        path.write_bytes(delta_fixtures._compact(latest))
        with self.assertRaises(PublishedSQLiteReaderError) as caught:
            with PublishedSQLiteReader(self.control, self.baseline_package, self.delta_root):
                pass
        self.assertIn("PUBLISHED_CONTROL_INVALID", caught.exception.category)

    def test_tail_metadata_mismatch_is_rejected(self):
        _first_id, second_id = self._publish_reset_and_incremental()
        path = self.control / "latest.json"
        latest = json.loads(path.read_text())
        latest["source_row_counts"]["records"] += 1
        path.write_bytes(delta_fixtures._compact(latest))
        with self.assertRaises(PublishedSQLiteReaderError):
            with PublishedSQLiteReader(self.control, self.baseline_package, self.delta_root,
                                       expected_generation_id=second_id):
                pass

    def test_delta_remote_reference_path_traversal_is_rejected(self):
        self._publish_reset_and_incremental()
        path = self.control / "latest.json"
        latest = json.loads(path.read_text())
        latest["delta_chain"][0]["index_remote"] = "deltas/generations/../index.json"
        path.write_bytes(delta_fixtures._compact(latest))
        with self.assertRaises(PublishedSQLiteReaderError) as caught:
            with PublishedSQLiteReader(self.control, self.baseline_package, self.delta_root):
                pass
        self.assertEqual(caught.exception.category, "PUBLISHED_CONTROL_INVALID")

    def test_missing_reordered_or_duplicate_chain_entries_are_rejected(self):
        first_id, second_id = self._publish_reset_and_incremental()
        original = json.loads((self.control / "latest.json").read_text())
        changes = (
            [],
            list(reversed(original["delta_chain"])),
            [original["delta_chain"][0], original["delta_chain"][0]],
        )
        for changed in changes:
            latest = dict(original)
            latest["delta_chain"] = changed
            (self.control / "latest.json").write_bytes(delta_fixtures._compact(latest))
            with self.subTest(chain=changed), self.assertRaises(PublishedSQLiteReaderError):
                with PublishedSQLiteReader(self.control, self.baseline_package, self.delta_root):
                    pass
        (self.control / "latest.json").write_bytes(delta_fixtures._compact(original))

    def test_missing_local_published_index_rejects_prepared_generation(self):
        first_id, second_id = self._publish_reset_and_incremental()
        (self.delta_root / first_id / "index.json").unlink()
        with self.assertRaises(PublishedSQLiteReaderError) as caught:
            with PublishedSQLiteReader(self.control, self.baseline_package, self.delta_root,
                                       expected_generation_id=second_id):
                pass
        self.assertEqual(caught.exception.category, "PUBLISHED_CONTROL_FILE_INVALID")

    def test_local_mirror_index_and_plan_must_match_control_bytes(self):
        first_id, second_id = self._publish_reset_and_incremental()
        for name in ("index.json", "delta-plan.json"):
            path = self.delta_root / first_id / name
            original = path.read_bytes()
            path.write_bytes(original + b" ")
            with self.subTest(name=name), self.assertRaises(PublishedSQLiteReaderError) as caught:
                with PublishedSQLiteReader(self.control, self.baseline_package, self.delta_root):
                    pass
            self.assertEqual(caught.exception.category, "PUBLISHED_LOCAL_DELTA_MISMATCH")
            path.write_bytes(original)

    def test_symlink_root_duplicate_json_path_ref_and_expected_generation_fail(self):
        self._copy_publisher_controls()
        link = self.case.root / "control-link"
        link.symlink_to(self.control, target_is_directory=True)
        with self.assertRaises(PublishedSQLiteReaderError):
            PublishedSQLiteReader(link, self.baseline_package, self.delta_root)

        latest_path = self.control / "latest.json"
        valid_raw = latest_path.read_bytes()
        latest_path.write_bytes(b'{"version":1,"version":1}')
        with self.assertRaises(PublishedSQLiteReaderError) as duplicate:
            with PublishedSQLiteReader(self.control, self.baseline_package, self.delta_root):
                pass
        self.assertEqual(duplicate.exception.category, "PUBLISHED_CONTROL_JSON_INVALID")

        latest_path.write_bytes(b'{"version":NaN}')
        with self.assertRaises(PublishedSQLiteReaderError) as nonstandard:
            with PublishedSQLiteReader(self.control, self.baseline_package, self.delta_root):
                pass
        self.assertEqual(nonstandard.exception.category, "PUBLISHED_CONTROL_JSON_INVALID")

        latest_path.write_bytes(b'{"version":1,"generation_id":"../escape","role":null}')
        with self.assertRaises(PublishedSQLiteReaderError):
            with PublishedSQLiteReader(self.control, self.baseline_package, self.delta_root):
                pass
        latest_path.write_bytes(valid_raw)
        with self.assertRaises(PublishedSQLiteReaderError) as wrong_expected:
            with PublishedSQLiteReader(self.control, self.baseline_package, self.delta_root,
                                       expected_generation_id="20261003T010101Z-a0010001"):
                pass
        self.assertEqual(wrong_expected.exception.category, "PUBLISHED_EXPECTED_GENERATION_MISMATCH")

    def test_same_bytes_inode_exchange_of_immutable_control_is_rejected(self):
        self._copy_publisher_controls()
        index_path = self.control / "generations" / BASELINE_ID / "index.json"
        with self.assertRaises(PublishedSQLiteReaderError) as caught:
            with PublishedSQLiteReader(self.control, self.baseline_package, self.delta_root) as reader:
                _ = reader.generation_id
                original = index_path.read_bytes()
                replacement = index_path.with_suffix(".replacement")
                replacement.write_bytes(original)
                os.replace(replacement, index_path)
                _ = reader.generation_id
        self.assertEqual(caught.exception.category, "PUBLISHED_CONTROL_CHANGED")

    def test_partial_iterator_close_checks_guards_and_closes_connections(self):
        first_id, second_id = self._publish_reset_and_incremental()
        reader = PublishedSQLiteReader(self.control, self.baseline_package, self.delta_root,
                                       expected_generation_id=second_id)
        with self.assertRaises(PublishedSQLiteReaderError) as caught:
            with reader as opened:
                delta_reader = opened._delta_reader
                iterator = opened.iter_rows("records")
                next(iterator)
                index_path = self.control / "deltas" / "generations" / first_id / "index.json"
                replacement = index_path.with_suffix(".replacement")
                replacement.write_bytes(index_path.read_bytes())
                os.replace(replacement, index_path)
                iterator.close()
        self.assertEqual(caught.exception.category, "PUBLISHED_CONTROL_CHANGED")
        self.assertIsNone(delta_reader._connection)

    def test_metadata_getter_rechecks_local_delta_artifacts(self):
        first_id, second_id = self._publish_reset_and_incremental()
        delta_plan = json.loads((self.delta_root / first_id / "delta-plan.json").read_text())
        artifact_rel = delta_plan["files"][0]["local"]
        artifact_path = self.delta_root / first_id / artifact_rel
        with self.assertRaises(PublishedSQLiteReaderError):
            with PublishedSQLiteReader(self.control, self.baseline_package, self.delta_root,
                                       expected_generation_id=second_id) as reader:
                _ = reader.generation_id
                original = artifact_path.read_bytes()
                artifact_path.write_bytes(original + b" ")
                _ = reader.generation_id

    def test_latest_can_advance_while_open_reader_keeps_its_pin(self):
        self._copy_publisher_controls()
        latest_path = self.control / "latest.json"
        original = latest_path.read_bytes()
        with PublishedSQLiteReader(self.control, self.baseline_package, self.delta_root) as reader:
            pinned = reader.pinned_latest_sha256
            advanced = latest_path.with_suffix(".next")
            advanced.write_bytes(original + b" ")
            os.replace(advanced, latest_path)
            self.assertEqual(reader.generation_id, BASELINE_ID)
            self.assertEqual(reader.pinned_latest_sha256, pinned)
            self.assertEqual(reader.row_count("records"), 1)

    def test_global_index_slices_manifest_sha_mismatch_rejected(self):
        self._copy_publisher_controls()
        index_path = self.control / "generations" / BASELINE_ID / "index.json"
        index = json.loads(index_path.read_text())
        for entry in index["files"]:
            if entry.get("local") == "slices/manifest.json":
                entry["sha256"] = "0" * 64
                break
        index_raw = delta_fixtures._compact(index)
        index_path.write_bytes(index_raw)
        latest_path = self.control / "latest.json"
        latest = json.loads(latest_path.read_text())
        latest["index_sha256"] = _sha(index_raw)
        latest_path.write_bytes(delta_fixtures._compact(latest))

        with self.assertRaises(PublishedSQLiteReaderError) as caught:
            with PublishedSQLiteReader(self.control, self.baseline_package, self.delta_root):
                pass
        self.assertEqual(caught.exception.category, "PUBLISHED_BASELINE_BINDING_MISMATCH")

    def test_global_index_slices_part_sha_mismatch_rejected(self):
        self._copy_publisher_controls()
        index_path = self.control / "generations" / BASELINE_ID / "index.json"
        index = json.loads(index_path.read_text())
        found = False
        for entry in index["files"]:
            if entry.get("local", "").startswith("slices/") and entry.get("local", "").endswith(".sqlite3"):
                entry["sha256"] = "0" * 64
                found = True
                break
        self.assertTrue(found)
        index_raw = delta_fixtures._compact(index)
        index_path.write_bytes(index_raw)
        latest_path = self.control / "latest.json"
        latest = json.loads(latest_path.read_text())
        latest["index_sha256"] = _sha(index_raw)
        latest_path.write_bytes(delta_fixtures._compact(latest))

        with self.assertRaises(PublishedSQLiteReaderError) as caught:
            with PublishedSQLiteReader(self.control, self.baseline_package, self.delta_root):
                pass
        self.assertEqual(caught.exception.category, "PUBLISHED_BASELINE_BINDING_MISMATCH")

    def test_global_index_slices_duplicate_or_missing_entry_rejected(self):
        self._copy_publisher_controls()
        index_path = self.control / "generations" / BASELINE_ID / "index.json"
        latest_path = self.control / "latest.json"

        # Missing shard part entry
        index = json.loads(index_path.read_text())
        part_entry = next(f for f in index["files"]
                          if f.get("local", "").startswith("slices/") and f.get("local", "").endswith(".sqlite3"))
        index["files"] = [f for f in index["files"] if f.get("local") != part_entry["local"]]
        index["verification"]["file_count"] = len(index["files"])
        index_raw = delta_fixtures._compact(index)
        index_path.write_bytes(index_raw)
        latest = json.loads(latest_path.read_text())
        latest["index_sha256"] = _sha(index_raw)
        latest_path.write_bytes(delta_fixtures._compact(latest))
        with self.assertRaises(PublishedSQLiteReaderError) as caught:
            with PublishedSQLiteReader(self.control, self.baseline_package, self.delta_root):
                pass
        self.assertEqual(caught.exception.category, "PUBLISHED_BASELINE_BINDING_MISMATCH")

    def test_baseline_only_latest_captured_at_mismatch_rejected(self):
        self._copy_publisher_controls()
        latest_path = self.control / "latest.json"
        latest = json.loads(latest_path.read_text())
        latest["captured_at"] = "2026-10-02T19:59:59Z"
        latest_path.write_bytes(delta_fixtures._compact(latest))
        with self.assertRaises(PublishedSQLiteReaderError) as caught:
            with PublishedSQLiteReader(self.control, self.baseline_package, self.delta_root):
                pass
        self.assertEqual(caught.exception.category, "PUBLISHED_CONTROL_BINDING_MISMATCH")

    def test_baseline_only_latest_captured_at_kind_mismatch_rejected(self):
        self._copy_publisher_controls()
        latest_path = self.control / "latest.json"
        latest = json.loads(latest_path.read_text())
        latest["captured_at_kind"] = "other_kind"
        latest_path.write_bytes(delta_fixtures._compact(latest))
        with self.assertRaises(PublishedSQLiteReaderError) as caught:
            with PublishedSQLiteReader(self.control, self.baseline_package, self.delta_root):
                pass
        self.assertIn(caught.exception.category, ("PUBLISHED_CONTROL_BINDING_MISMATCH", "PUBLISHED_CONTROL_INVALID"))


if __name__ == "__main__":
    unittest.main()
