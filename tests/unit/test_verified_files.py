from __future__ import annotations

import copy
import hashlib
import os
from pathlib import Path
import pickle
import sys
import tempfile
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src/python"))

from ikarchive import verified_files


class VerifiedFilesTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(dir=ROOT, prefix=".test-verified-files-")
        self.root = Path(self.temporary.name)
        self.parent = self.root / "nested"
        self.parent.mkdir()
        self.path = self.parent / "part.sqlite3"
        self.content = b"synthetic immutable file\x00payload"
        self.path.write_bytes(self.content)

    def tearDown(self):
        self.temporary.cleanup()

    def record(self, relative="nested/part.sqlite3", *, raw=None, size=None, extra=None):
        if raw is None:
            raw = self.content
        result = {
            "bytes": len(raw) if size is None else size,
            "sha256": hashlib.sha256(raw).hexdigest(),
        }
        if extra:
            result.update(extra)
        return {relative: result}

    def test_hashes_registered_file_and_binds_canonical_path(self):
        records = self.record(extra={"table_id": "ignored-caller-metadata"})
        with mock.patch.object(
            verified_files, "_hash_descriptor", wraps=verified_files._hash_descriptor
        ) as hash_spy:
            token = verified_files.verify_files(self.root, records)
        self.assertEqual(hash_spy.call_count, 1)
        self.assertEqual(token.root, self.root)
        self.assertEqual(token.checked_path("nested/part.sqlite3", records["nested/part.sqlite3"]), self.path)
        token.assert_matches(self.root, {"nested/part.sqlite3": {
            **records["nested/part.sqlite3"], "unrelated": 123,
        }})

    def test_repeated_guards_and_stat_checks_do_not_rehash_contents(self):
        token = verified_files.verify_files(self.root, self.record())
        with mock.patch.object(
            verified_files, "_hash_descriptor", wraps=verified_files._hash_descriptor
        ) as hash_spy:
            with token.guard():
                token.checked_path("nested/part.sqlite3", self.record()["nested/part.sqlite3"])
            token.assert_unchanged()
            with token.guard():
                pass
        hash_spy.assert_not_called()

    def test_assert_matches_requires_the_same_root_and_complete_record_set(self):
        token = verified_files.verify_files(self.root, self.record())
        with self.assertRaisesRegex(verified_files.VerifiedFilesError, "BINDING_MISMATCH"):
            token.assert_matches(self.root, {})
        other_root = self.root / "other"
        other_root.mkdir()
        with self.assertRaisesRegex(verified_files.VerifiedFilesError, "BINDING_MISMATCH"):
            token.assert_matches(other_root, self.record())
        changed = self.record()
        changed["nested/part.sqlite3"]["bytes"] += 1
        with self.assertRaisesRegex(verified_files.VerifiedFilesError, "BINDING_MISMATCH"):
            token.assert_matches(self.root, changed)

    def test_same_size_rewrite_with_restored_mtime_is_rejected_by_ctime(self):
        token = verified_files.verify_files(self.root, self.record())
        before = self.path.stat()
        rewritten = bytes([self.content[0] ^ 0x01]) + self.content[1:]
        self.path.write_bytes(rewritten)
        os.utime(self.path, ns=(before.st_atime_ns, before.st_mtime_ns))
        after = self.path.stat()
        self.assertEqual(after.st_size, before.st_size)
        self.assertEqual(after.st_mtime_ns, before.st_mtime_ns)
        self.assertNotEqual(after.st_ctime_ns, before.st_ctime_ns)
        with self.assertRaisesRegex(verified_files.VerifiedFilesError, "VERIFIED_FILES_CHANGED"):
            token.assert_unchanged()

    def test_same_bytes_replacement_with_new_inode_is_rejected(self):
        token = verified_files.verify_files(self.root, self.record())
        replacement = self.parent / "replacement.sqlite3"
        replacement.write_bytes(self.content)
        os.replace(replacement, self.path)
        with self.assertRaisesRegex(verified_files.VerifiedFilesError, "VERIFIED_FILES_CHANGED"):
            token.assert_unchanged()

    def test_initial_file_and_parent_symlinks_are_rejected(self):
        target = self.parent / "target.sqlite3"
        target.write_bytes(self.content)
        self.path.unlink()
        self.path.symlink_to(target)
        with self.assertRaisesRegex(verified_files.VerifiedFilesError, "VERIFIED_FILES_SYMLINK"):
            verified_files.verify_files(self.root, self.record())

        link = self.root / "nested-link"
        link.symlink_to(self.parent, target_is_directory=True)
        with self.assertRaisesRegex(verified_files.VerifiedFilesError, "VERIFIED_FILES_SYMLINK"):
            verified_files.verify_files(self.root, self.record("nested-link/target.sqlite3"))

    def test_root_ancestor_symlink_is_rejected(self):
        alias = self.root.parent / (self.root.name + "-link")
        alias.symlink_to(self.root, target_is_directory=True)
        try:
            with self.assertRaisesRegex(verified_files.VerifiedFilesError, "VERIFIED_FILES_SYMLINK"):
                verified_files.verify_files(alias, self.record())
        finally:
            alias.unlink()

    def test_file_mutation_during_hash_is_rejected(self):
        real_hash = verified_files._hash_descriptor

        def hash_then_mutate(descriptor):
            result = real_hash(descriptor)
            with self.path.open("r+b") as stream:
                first = stream.read(1)
                stream.seek(0)
                stream.write(bytes([first[0] ^ 0x01]))
                stream.flush()
                os.fsync(stream.fileno())
            return result

        with mock.patch.object(verified_files, "_hash_descriptor", side_effect=hash_then_mutate):
            with self.assertRaisesRegex(verified_files.VerifiedFilesError, "VERIFIED_FILES_CHANGED"):
                verified_files.verify_files(self.root, self.record())

    def test_short_hash_read_and_expected_bytes_mismatch_are_rejected(self):
        with mock.patch.object(
            verified_files, "_hash_descriptor",
            return_value=(len(self.content) - 1, hashlib.sha256(self.content[:-1]).hexdigest()),
        ):
            with self.assertRaisesRegex(verified_files.VerifiedFilesError, "VERIFIED_FILES_CHANGED"):
                verified_files.verify_files(self.root, self.record())

        with self.assertRaisesRegex(verified_files.VerifiedFilesError, "HASH_MISMATCH"):
            verified_files.verify_files(self.root, self.record(size=len(self.content) + 1))

    def test_wrong_sha_and_malformed_records_are_rejected(self):
        bad_sha = self.record()
        bad_sha["nested/part.sqlite3"]["sha256"] = "0" * 64
        with self.assertRaisesRegex(verified_files.VerifiedFilesError, "HASH_MISMATCH"):
            verified_files.verify_files(self.root, bad_sha)
        for bad_record in (
            {"bytes": True, "sha256": "0" * 64},
            {"bytes": -1, "sha256": "0" * 64},
            {"bytes": 1, "sha256": "A" * 64},
        ):
            with self.subTest(record=bad_record):
                with self.assertRaisesRegex(verified_files.VerifiedFilesError, "RECORDS_INVALID"):
                    verified_files.verify_files(self.root, {"nested/part.sqlite3": bad_record})

    def test_noncanonical_relative_paths_are_rejected(self):
        for relative in ("", ".", "./part", "../part", "nested/../part", "/absolute", "nested//part", "nested/", "nested\\part", "nul\x00name"):
            with self.subTest(relative=relative):
                with self.assertRaisesRegex(verified_files.VerifiedFilesError, "RELATIVE_PATH_INVALID"):
                    verified_files.verify_files(self.root, {relative: self.record()["nested/part.sqlite3"]})

    def test_parent_change_during_capture_is_compared_to_initial_snapshot(self):
        real_hash = verified_files._hash_descriptor

        def hash_with_transient_parent_change(descriptor):
            result = real_hash(descriptor)
            transient = self.parent / "created-during-hash"
            transient.write_bytes(b"x")
            transient.unlink()
            return result

        with mock.patch.object(
            verified_files, "_hash_descriptor", side_effect=hash_with_transient_parent_change
        ):
            with self.assertRaisesRegex(verified_files.VerifiedFilesError, "VERIFIED_FILES_CHANGED"):
                verified_files.verify_files(self.root, self.record())

    def test_directory_change_after_capture_is_rejected(self):
        token = verified_files.verify_files(self.root, self.record())
        transient = self.parent / "created-and-removed"
        transient.write_bytes(b"x")
        transient.unlink()
        with self.assertRaisesRegex(verified_files.VerifiedFilesError, "VERIFIED_FILES_CHANGED"):
            token.assert_unchanged()

    def test_hardlinks_are_allowed_and_each_registered_path_is_bound(self):
        hardlink = self.root / "same-inode.sqlite3"
        os.link(self.path, hardlink)
        records = self.record()
        records["same-inode.sqlite3"] = self.record()["nested/part.sqlite3"]
        token = verified_files.verify_files(self.root, records)
        self.assertEqual(token.checked_path("same-inode.sqlite3", records["same-inode.sqlite3"]), hardlink)
        token.assert_matches(self.root, records)

    def test_checked_path_rejects_unregistered_or_different_record(self):
        token = verified_files.verify_files(self.root, self.record())
        with self.assertRaisesRegex(verified_files.VerifiedFilesError, "RECORD_MISMATCH"):
            token.checked_path("nested/part.sqlite3", {"bytes": 1, "sha256": "0" * 64})
        with self.assertRaisesRegex(verified_files.VerifiedFilesError, "RECORD_MISMATCH"):
            token.checked_path("another.sqlite3", self.record()["nested/part.sqlite3"])

    def test_guard_checks_after_early_generator_close_and_exception(self):
        token = verified_files.verify_files(self.root, self.record())

        def guarded_generator():
            with token.guard():
                yield "entered"

        iterator = guarded_generator()
        self.assertEqual(next(iterator), "entered")
        self.path.write_bytes(self.content + b"changed")
        with self.assertRaisesRegex(verified_files.VerifiedFilesError, "VERIFIED_FILES_CHANGED"):
            iterator.close()

        token = verified_files.verify_files(self.root, self.record(raw=self.path.read_bytes()))
        with self.assertRaisesRegex(RuntimeError, "synthetic failure"):
            with token.guard():
                raise RuntimeError("synthetic failure")

    def test_guard_runs_final_check_when_body_raises_after_mutation(self):
        token = verified_files.verify_files(self.root, self.record())
        with self.assertRaisesRegex(verified_files.VerifiedFilesError, "VERIFIED_FILES_CHANGED"):
            with token.guard():
                self.path.write_bytes(self.content + b"changed")
                raise RuntimeError("body failure")

    def test_token_cannot_be_constructed_copied_or_serialized_and_rejects_fakes(self):
        token = verified_files.verify_files(self.root, self.record())
        with self.assertRaisesRegex(TypeError, "verify_files"):
            verified_files.VerifiedFiles()
        with self.assertRaisesRegex(TypeError, "verify_files"):
            verified_files.VerifiedFiles._create(object(), self.root, {}, {}, {})
        with self.assertRaisesRegex(TypeError, "copied"):
            copy.copy(token)
        with self.assertRaisesRegex(TypeError, "copied"):
            copy.deepcopy(token)
        with self.assertRaisesRegex(TypeError, "serialized"):
            pickle.dumps(token)
        forged = object.__new__(verified_files.VerifiedFiles)
        with self.assertRaisesRegex(TypeError, "unrecognized"):
            forged.assert_unchanged()

        class FakeToken:
            pass

        with self.assertRaisesRegex(TypeError, "unrecognized"):
            verified_files.VerifiedFiles.assert_unchanged(FakeToken())
        with self.assertRaisesRegex(AttributeError, "immutable"):
            token._root = str(self.root.parent)


if __name__ == "__main__":
    unittest.main()
