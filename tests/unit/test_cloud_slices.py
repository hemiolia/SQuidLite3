import hashlib
import json
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
import publish_cloud_slices as publisher


SNAPSHOT = "20261002T041520Z-29b7872e"


def generation(mode_count=16, rule_count=20, *, include_index_counts=True):
    by_mode = []
    by_rule = []
    files = []
    for number in range(mode_count):
        rel = f"by-mode/mode_{number:02d}.sqlite3"
        size = 1000 + number
        by_mode.append({"file": rel, "bytes": size, "matches": number})
        files.append({"file": rel, "bytes": size, "sha256": hashlib.sha256(rel.encode()).hexdigest()})
    for number in range(rule_count):
        rel = f"by-rule/mode_{number % max(mode_count, 1):02d}__RULE_{number:02d}.sqlite3"
        size = 2000 + number
        by_rule.append({"file": rel, "bytes": size, "matches": 1})
        files.append({"file": rel, "bytes": size, "sha256": hashlib.sha256(rel.encode()).hexdigest()})
    manifest = {
        "counts": {"mode_files": len(by_mode), "rule_files": len(by_rule)},
        "by_mode": by_mode,
        "by_rule": by_rule,
    }
    index = {"snapshot_id": SNAPSHOT, "files": files}
    if include_index_counts:
        index["counts"] = {"mode_files": len(by_mode), "rule_files": len(by_rule)}
    return index, manifest


class CloudSliceValidationTests(unittest.TestCase):
    def test_accepts_old_36_and_new_76_file_generations(self):
        for rule_count, total in ((20, 36), (60, 76)):
            with self.subTest(rule_count=rule_count):
                index, manifest = generation(rule_count=rule_count)
                files = publisher.validate_generation(index, manifest, SNAPSHOT)
                self.assertEqual(len(files), total)

    def test_legacy_index_without_counts_uses_source_manifest_counts(self):
        index, manifest = generation(rule_count=20, include_index_counts=False)
        self.assertEqual(len(publisher.validate_generation(index, manifest, SNAPSHOT)), 36)

    def test_rejects_empty_files_and_invalid_size_or_digest_metadata(self):
        index, manifest = generation(mode_count=1, rule_count=1)
        bad_indexes = []

        empty = json.loads(json.dumps(index))
        empty["files"] = []
        bad_indexes.append(("empty", empty))

        negative = json.loads(json.dumps(index))
        negative["files"][0]["bytes"] = -1
        bad_indexes.append(("negative size", negative))

        boolean_size = json.loads(json.dumps(index))
        boolean_size["files"][0]["bytes"] = True
        bad_indexes.append(("boolean size", boolean_size))

        zero = json.loads(json.dumps(index))
        zero["files"][0]["bytes"] = 0
        bad_indexes.append(("empty object", zero))

        bad_hash = json.loads(json.dumps(index))
        bad_hash["files"][0]["sha256"] = "not-a-hash"
        bad_indexes.append(("bad hash", bad_hash))

        non_object = json.loads(json.dumps(index))
        non_object["files"][0] = "by-mode/mode_00.sqlite3"
        bad_indexes.append(("non-object row", non_object))

        for label, invalid in bad_indexes:
            with self.subTest(label=label), self.assertRaises(RuntimeError):
                publisher.validate_generation(invalid, manifest, SNAPSHOT)

    def test_rejects_traversal_absolute_backslash_and_wrong_category_paths(self):
        invalid_paths = (
            "../by-mode/mode_00.sqlite3",
            "/by-mode/mode_00.sqlite3",
            "by-mode/../mode_00.sqlite3",
            "by-mode/./mode_00.sqlite3",
            "by-mode//mode_00.sqlite3",
            r"by-mode\mode_00.sqlite3",
            "by-mode/C:mode_00.sqlite3",
            "by-mode/%2e%2e%2fmode_00.sqlite3",
            "by-mode/UPPERCASE.sqlite3",
            "by-rule/has-hyphen.sqlite3",
            "by-rule/nested/name.sqlite3",
        )
        for rel in invalid_paths:
            index, manifest = generation(mode_count=1, rule_count=0)
            index["files"][0]["file"] = rel
            with self.subTest(path=rel), self.assertRaises(RuntimeError):
                publisher.validate_generation(index, manifest, SNAPSHOT)

    def test_rejects_bad_snapshot_id_and_duplicate_paths(self):
        index, manifest = generation(mode_count=1, rule_count=1)
        index["snapshot_id"] = "../" + SNAPSHOT
        with self.assertRaises(RuntimeError):
            publisher.validate_generation(index, manifest, "../" + SNAPSHOT)
        index["snapshot_id"] = SNAPSHOT
        index["files"][1]["file"] = index["files"][0]["file"]
        with self.assertRaisesRegex(RuntimeError, "duplicate index path"):
            publisher.validate_generation(index, manifest, SNAPSHOT)

    def test_rejects_source_count_file_list_and_size_mismatches(self):
        cases = []
        index, manifest = generation(mode_count=1, rule_count=1)
        bad = json.loads(json.dumps(manifest))
        bad["counts"]["mode_files"] = 2
        cases.append(("count", index, bad))

        bad = json.loads(json.dumps(manifest))
        bad["by_rule"][0]["file"] = "by-rule/different.sqlite3"
        cases.append(("file map", index, bad))

        bad = json.loads(json.dumps(manifest))
        bad["by_mode"][0]["bytes"] += 1
        cases.append(("size", index, bad))

        bad_index = json.loads(json.dumps(index))
        bad_index["counts"]["rule_files"] = 2
        cases.append(("index count", bad_index, manifest))

        for label, bad_index, bad_manifest in cases:
            with self.subTest(label=label), self.assertRaises(RuntimeError):
                publisher.validate_generation(bad_index, bad_manifest, SNAPSHOT)

    def test_rejects_non_integer_source_sizes_counts_and_duplicate_manifest_paths(self):
        index, manifest = generation(mode_count=1, rule_count=1)
        bad = json.loads(json.dumps(manifest))
        bad["by_mode"][0]["bytes"] = True
        with self.assertRaises(RuntimeError):
            publisher.validate_generation(index, bad, SNAPSHOT)

        bad = json.loads(json.dumps(manifest))
        del bad["counts"]
        with self.assertRaises(RuntimeError):
            publisher.validate_generation(index, bad, SNAPSHOT)

        bad = json.loads(json.dumps(manifest))
        bad["counts"]["rule_files"] = True
        with self.assertRaises(RuntimeError):
            publisher.validate_generation(index, bad, SNAPSHOT)

        bad = json.loads(json.dumps(manifest))
        bad["by_rule"][0]["file"] = bad["by_mode"][0]["file"]
        with self.assertRaises(RuntimeError):
            publisher.validate_generation(index, bad, SNAPSHOT)


class CloudSlicePublishFlowTests(unittest.TestCase):
    def test_legacy_partial_publisher_refuses_before_any_remote_work(self):
        index, manifest = generation(mode_count=1, rule_count=1, include_index_counts=False)
        index_bytes = json.dumps(index).encode()
        manifest_bytes = json.dumps(manifest).encode()
        events = []

        def read_staged(_snapshot, rel):
            events.append("read " + rel)
            return {"index.json": index_bytes,
                    "slices-source-manifest.json": manifest_bytes}[rel]

        def upload(_snapshot, rel, _size, _digest):
            events.append("upload " + rel)

        with patch.object(sys, "argv", ["publish_cloud_slices.py", SNAPSHOT]), \
                patch.object(publisher, "_read_staged_json", side_effect=read_staged), \
                patch.object(publisher, "verified_upload", side_effect=upload):
            with self.assertRaises(SystemExit) as raised:
                publisher.main()
        self.assertEqual(raised.exception.code, 2)
        self.assertEqual(events, [])

    def test_existing_remote_hash_mismatch_fails_without_upload_or_delete(self):
        expected = (123, "a" * 64)
        with patch.object(publisher, "stream_digest", side_effect=[expected, (123, "b" * 64)]), \
                patch.object(publisher, "remote_meta", return_value={"Size": 123}), \
                patch.object(publisher, "remote_command", return_value=["fake-rclone", "cat"]), \
                patch.object(publisher.subprocess, "run") as run:
            with self.assertRaisesRegex(RuntimeError, "remote readback mismatch"):
                publisher.verified_upload(SNAPSHOT, "by-mode/mode_00.sqlite3", *expected)
        run.assert_not_called()

    def test_stat_recognizes_explicit_missing_but_not_other_errors(self):
        missing = subprocess.CompletedProcess([], 4, b"", b"googleapi: Error 404: File not found")
        with patch.object(publisher, "remote_command", return_value=["fake-rclone"]), \
                patch.object(publisher.subprocess, "run", return_value=missing):
            self.assertIsNone(publisher.remote_meta("fake:path"))

        failure = subprocess.CompletedProcess([], 4, b"", b"permission denied")
        with patch.object(publisher, "remote_command", return_value=["fake-rclone"]), \
                patch.object(publisher.subprocess, "run", return_value=failure):
            with self.assertRaisesRegex(RuntimeError, "remote stat failed: 4"):
                publisher.remote_meta("fake:path")

    def test_rate_limit_is_retried_as_temporary_stat_error(self):
        limited = subprocess.CompletedProcess([], 1, b"", b"403 rateLimitExceeded")
        found = subprocess.CompletedProcess([], 0, b'{"Size": 100}', b"")
        with patch.object(publisher, "remote_command", return_value=["fake-rclone"]), \
                patch.object(publisher.subprocess, "run", side_effect=[limited, found]) as run, \
                patch.object(publisher.time, "sleep") as sleep:
            self.assertEqual(publisher.remote_meta("fake:path"), {"Size": 100})
        self.assertEqual(run.call_count, 2)
        sleep.assert_called_once_with(60)

    def test_permission_error_containing_missing_words_is_not_missing(self):
        denied = subprocess.CompletedProcess([], 4, b'', b'403 forbidden: file not found')
        with patch.object(publisher, 'remote_command', return_value=['fake-rclone']), \
                patch.object(publisher.subprocess, 'run', return_value=denied):
            with self.assertRaisesRegex(RuntimeError, 'remote stat failed'):
                publisher.remote_meta('fake:path')


if __name__ == "__main__":
    unittest.main()
