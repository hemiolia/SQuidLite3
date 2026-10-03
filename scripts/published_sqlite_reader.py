#!/usr/bin/env python3
"""Bind a local lossless SQLite package to one pinned publisher generation.

This reader verifies the local package and the publisher control objects that
name it.  It does not connect to Drive and does not verify remote XLSX or raw
SQLite objects.  The publisher's readback fields remain publisher receipts,
not live remote checks performed by this module.
"""

from __future__ import annotations

from contextlib import ExitStack
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat
import sys
from typing import Any, Generator, Iterator, Optional

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src/python"))
sys.path.insert(0, str(ROOT / "scripts"))

import nas_full_data_publish as publisher  # noqa: E402
import publish_full_data_delta as delta_publisher  # noqa: E402
from ikarchive.delta_reader import DeltaChainReader  # noqa: E402
from ikarchive.shard_reader import LosslessShardReader  # noqa: E402
from ikarchive.verified_files import verify_files  # noqa: E402


_MAX_CONTROL_BYTES = publisher.MAX_CONTROL_BYTES
_DUMMY_REMOTE = "published-local:database"
_STAT_FIELDS = ("st_dev", "st_ino", "st_mode", "st_size", "st_mtime_ns", "st_ctime_ns")


class PublishedSQLiteReaderError(ValueError):
    """A stable, value-free category for a local published-generation failure."""

    def __init__(self, category: str, cause_category: Optional[str] = None):
        super().__init__(category)
        self.category = category
        self.cause_category = cause_category


def _fail(category: str) -> None:
    raise PublishedSQLiteReaderError(category)


def _fingerprint(info: os.stat_result) -> tuple[int, ...]:
    return tuple(int(getattr(info, field)) for field in _STAT_FIELDS)


def _safe_components(path: Path) -> None:
    """Reject symlinks in an absolute path, including every ancestor."""
    for component in reversed((path, *path.parents)):
        try:
            info = os.lstat(component)
        except OSError as exc:
            raise PublishedSQLiteReaderError("PUBLISHED_PATH_INVALID") from exc
        if stat.S_ISLNK(info.st_mode):
            _fail("PUBLISHED_PATH_SYMLINK")


def _absolute_path(value: str | os.PathLike[str], *, directory: bool,
                   allow_missing: bool = False) -> Path:
    try:
        raw = os.fspath(value)
        if not isinstance(raw, str) or not raw or "\x00" in raw:
            _fail("PUBLISHED_PATH_INVALID")
        supplied = Path(raw).expanduser()
        if ".." in supplied.parts:
            _fail("PUBLISHED_PATH_INVALID")
        if not supplied.is_absolute():
            supplied = Path.cwd() / supplied
        absolute = Path(os.path.abspath(os.fspath(supplied)))
    except (OSError, RuntimeError, TypeError) as exc:
        raise PublishedSQLiteReaderError("PUBLISHED_PATH_INVALID") from exc

    # Existing ancestors are inspected without resolving the path, so a
    # symlink cannot be hidden by Path.resolve().
    probe = absolute
    while True:
        try:
            os.lstat(probe)
            break
        except FileNotFoundError:
            if not allow_missing:
                raise PublishedSQLiteReaderError("PUBLISHED_PATH_INVALID")
            if probe.parent == probe:
                raise PublishedSQLiteReaderError("PUBLISHED_PATH_INVALID")
            probe = probe.parent
        except OSError as exc:
            raise PublishedSQLiteReaderError("PUBLISHED_PATH_INVALID") from exc
    _safe_components(probe)
    if not allow_missing or absolute.exists():
        try:
            info = os.lstat(absolute)
        except OSError as exc:
            raise PublishedSQLiteReaderError("PUBLISHED_PATH_INVALID") from exc
        if stat.S_ISLNK(info.st_mode):
            _fail("PUBLISHED_PATH_SYMLINK")
        if directory and not stat.S_ISDIR(info.st_mode):
            _fail("PUBLISHED_PATH_INVALID")
        if not directory and not stat.S_ISREG(info.st_mode):
            _fail("PUBLISHED_PATH_INVALID")
    return absolute


def _directory_chain(path: Path) -> tuple[tuple[Path, tuple[int, int, int]], ...]:
    chain = []
    for component in reversed((path, *path.parents)):
        try:
            info = os.lstat(component)
        except OSError as exc:
            raise PublishedSQLiteReaderError("PUBLISHED_PATH_INVALID") from exc
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
            _fail("PUBLISHED_PATH_SYMLINK" if stat.S_ISLNK(info.st_mode) else "PUBLISHED_PATH_INVALID")
        chain.append((component, (info.st_dev, info.st_ino, stat.S_IFMT(info.st_mode))))
    return tuple(chain)


def _same_directory_chain(before: tuple[tuple[Path, tuple[int, int, int]], ...],
                          after: tuple[tuple[Path, tuple[int, int, int]], ...]) -> bool:
    return before == after


def _read_regular_file(path: Path, *, max_bytes: int = _MAX_CONTROL_BYTES) -> tuple[bytes, tuple[int, ...]]:
    """Read one regular file through O_NOFOLLOW and check fd/path identity."""
    try:
        parent_chain_before = _directory_chain(path.parent)
        path_before = os.lstat(path)
        if stat.S_ISLNK(path_before.st_mode) or not stat.S_ISREG(path_before.st_mode):
            _fail("PUBLISHED_CONTROL_FILE_INVALID")
        if path_before.st_size <= 0 or path_before.st_size > max_bytes:
            _fail("PUBLISHED_CONTROL_SIZE_INVALID")
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
        fd = os.open(path, flags)
    except PublishedSQLiteReaderError:
        raise
    except OSError as exc:
        raise PublishedSQLiteReaderError("PUBLISHED_CONTROL_FILE_INVALID") from exc

    try:
        fd_before = os.fstat(fd)
        if stat.S_ISLNK(fd_before.st_mode) or not stat.S_ISREG(fd_before.st_mode):
            _fail("PUBLISHED_CONTROL_FILE_INVALID")
        if _fingerprint(fd_before) != _fingerprint(path_before):
            _fail("PUBLISHED_CONTROL_FILE_CHANGED")
        if fd_before.st_size <= 0 or fd_before.st_size > max_bytes:
            _fail("PUBLISHED_CONTROL_SIZE_INVALID")
        blocks = []
        remaining = fd_before.st_size
        while remaining:
            block = os.read(fd, min(1024 * 1024, remaining))
            if not block:
                _fail("PUBLISHED_CONTROL_FILE_CHANGED")
            blocks.append(block)
            remaining -= len(block)
        if os.read(fd, 1):
            _fail("PUBLISHED_CONTROL_FILE_CHANGED")
        raw = b"".join(blocks)
        fd_after = os.fstat(fd)
        path_after = os.lstat(path)
        parent_chain_after = _directory_chain(path.parent)
        if (_fingerprint(fd_after) != _fingerprint(fd_before)
                or _fingerprint(path_after) != _fingerprint(fd_before)
                or not _same_directory_chain(parent_chain_before, parent_chain_after)):
            _fail("PUBLISHED_CONTROL_FILE_CHANGED")
        return raw, _fingerprint(fd_after)
    except PublishedSQLiteReaderError:
        raise
    except OSError as exc:
        raise PublishedSQLiteReaderError("PUBLISHED_CONTROL_FILE_CHANGED") from exc
    finally:
        os.close(fd)


def _strict_json_object(raw: bytes) -> dict[str, Any]:
    def pairs_no_duplicates(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate key")
            result[key] = value
        return result

    def reject_constant(_value):
        raise ValueError("non-standard JSON constant")

    try:
        document = json.loads(raw.decode("utf-8"), object_pairs_hook=pairs_no_duplicates,
                              parse_constant=reject_constant)
    except (UnicodeError, json.JSONDecodeError, ValueError) as exc:
        raise PublishedSQLiteReaderError("PUBLISHED_CONTROL_JSON_INVALID") from exc
    if not isinstance(document, dict):
        _fail("PUBLISHED_CONTROL_JSON_INVALID")
    return document


def _require_control_version(document: dict[str, Any]) -> None:
    if type(document.get("version")) is not int or document["version"] != 1:
        _fail("PUBLISHED_CONTROL_VERSION_INVALID")


def _control_token(root: Path, expected: dict[str, bytes]):
    records = {name: {"bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}
               for name, raw in expected.items()}
    try:
        token = verify_files(root, records)
        token.assert_matches(root, records)
    except Exception as exc:
        raise PublishedSQLiteReaderError("PUBLISHED_CONTROL_CHANGED") from exc
    return token, records


class _FrozenControlClient:
    """Minimal read-only adapter for the existing publisher validators."""

    def __init__(self, objects: dict[str, bytes]):
        self._objects = dict(objects)

    @staticmethod
    def _relative(remote_path: str) -> str:
        prefix = _DUMMY_REMOTE + "/"
        if not isinstance(remote_path, str) or not remote_path.startswith(prefix):
            raise publisher.PublishError("LOCAL_CONTROL_PATH_INVALID")
        relative = remote_path[len(prefix):]
        parsed = PurePosixPath(relative)
        if (not relative or parsed.is_absolute() or parsed.as_posix() != relative
                or any(part in ("", ".", "..") for part in relative.split("/"))):
            raise publisher.PublishError("LOCAL_CONTROL_PATH_INVALID")
        return relative

    def stat(self, remote_path: str):
        relative = self._relative(remote_path)
        raw = self._objects.get(relative)
        if raw is None:
            raise publisher.PublishError("REMOTE_OBJECT_MISSING")
        return {"Size": len(raw)}

    def readback(self, remote_path: str):
        relative = self._relative(remote_path)
        raw = self._objects.get(relative)
        if raw is None:
            raise publisher.PublishError("REMOTE_OBJECT_MISSING")
        return len(raw), hashlib.sha256(raw).hexdigest()

    def readback_bytes(self, remote_path: str, expected_bytes: int, expected_sha: str) -> bytes:
        relative = self._relative(remote_path)
        raw = self._objects.get(relative)
        if raw is None:
            raise publisher.PublishError("REMOTE_OBJECT_MISSING")
        if len(raw) != expected_bytes or hashlib.sha256(raw).hexdigest() != expected_sha:
            raise publisher.PublishError("REMOTE_READBACK_MISMATCH")
        return raw


class PublishedSQLiteReader:
    """Read a local lossless SQLite baseline plus a publisher-pinned delta chain.

    ``published_control_binding_verified`` means that the pinned local
    latest/index/plan controls passed the publisher's structural validators,
    the immutable control objects remain stat-bound, and local baseline/delta
    packages are bound to those controls. It is not a live Drive check and
    does not mean the remote raw database or XLSX objects were locally read.
    A baseline-only context has ``published_deltas_verified == False``.
    """

    def __init__(self, control_dir, baseline_package, delta_generations_dir,
                 *, expected_generation_id=None):
        self._control_dir = _absolute_path(control_dir, directory=True)
        self._baseline_package = _absolute_path(baseline_package, directory=True)
        self._delta_root = _absolute_path(delta_generations_dir, directory=True, allow_missing=True)
        if expected_generation_id is not None and not publisher.valid_generation_id(expected_generation_id):
            raise ValueError("expected_generation_id is invalid")
        self._expected_generation_id = expected_generation_id
        self._active = False
        self._stack: Optional[ExitStack] = None
        self._baseline_reader: Optional[LosslessShardReader] = None
        self._delta_reader: Optional[DeltaChainReader] = None
        self._iterators: set[Generator] = set()
        self._control_guards: list[tuple[Path, Any, dict[str, dict[str, Any]]]] = []
        self._frozen_control: dict[str, bytes] = {}
        self._control_fingerprints: dict[str, tuple[int, ...]] = {}
        self._latest: Optional[dict[str, Any]] = None
        self._latest_raw: Optional[bytes] = None
        self._pinned_latest_sha256: Optional[str] = None
        self._baseline_generation_id: Optional[str] = None
        self._generation_id: Optional[str] = None
        self._captured_at: Optional[str] = None

    def __enter__(self) -> "PublishedSQLiteReader":
        if self._active:
            raise RuntimeError("PublishedSQLiteReader is already open")
        self._control_guards = []
        self._frozen_control = {}
        self._control_fingerprints = {}
        self._latest = None
        self._latest_raw = None
        self._pinned_latest_sha256 = None
        self._baseline_generation_id = None
        self._generation_id = None
        self._captured_at = None
        stack = ExitStack()
        try:
            self._pin_controls()
            self._validate_publisher_controls()
            self._assert_control_objects_unchanged()

            baseline = LosslessShardReader(
                self._baseline_package, expected_generation=self._baseline_generation_id,
            )
            self._baseline_reader = stack.enter_context(baseline)
            self._assert_baseline_binding(baseline)

            roots = []
            for generation_id in self._chain_generation_ids:
                delta_root = _absolute_path(self._delta_root / generation_id, directory=True)
                roots.append(delta_root)
                self._check_local_delta_mirror(generation_id, delta_root)

            delta = DeltaChainReader(
                baseline, roots, require_published_deltas=True,
            )
            self._delta_reader = stack.enter_context(delta)
            # Close the compare/open window: the local control mirror remains
            # tied to the publisher bytes captured before the inner reader
            # validated its complete generation package.
            self._assert_control_objects_unchanged()
            self._validate_reader_tail()
            self._stack = stack
            self._active = True
            self._assert_public_read_state()
            return self
        except BaseException as exc:
            try:
                stack.close()
            except BaseException:
                pass
            self._reset_after_close()
            if not isinstance(exc, Exception):
                raise
            if isinstance(exc, PublishedSQLiteReaderError):
                raise
            category = getattr(exc, "category", None)
            if isinstance(category, str) and re.fullmatch(r"[A-Z0-9_]+", category):
                raise PublishedSQLiteReaderError("PUBLISHED_VALIDATION_FAILED", category) from None
            message = str(exc)
            if re.fullmatch(r"[A-Z0-9_]+", message):
                raise PublishedSQLiteReaderError("PUBLISHED_VALIDATION_FAILED", message) from None
            raise PublishedSQLiteReaderError("PUBLISHED_VALIDATION_FAILED") from None

    def __exit__(self, exc_type, exc, traceback) -> None:
        close_error: Optional[BaseException] = None
        guard_error: Optional[BaseException] = None
        cleanup_error: Optional[BaseException] = None
        try:
            for iterator in tuple(self._iterators):
                try:
                    iterator.close()
                except BaseException as error:
                    if close_error is None:
                        close_error = error
            if self._active:
                try:
                    self._assert_public_read_state()
                except BaseException as error:
                    guard_error = error
        finally:
            try:
                if self._stack is not None:
                    self._stack.close()
            except BaseException as error:
                cleanup_error = error
            finally:
                self._reset_after_close()
        if guard_error is not None:
            raise guard_error
        if close_error is not None:
            raise close_error
        if cleanup_error is not None:
            raise cleanup_error

    def _reset_after_close(self) -> None:
        self._active = False
        self._stack = None
        self._baseline_reader = None
        self._delta_reader = None
        self._iterators.clear()

    def _require_open(self) -> None:
        if not self._active or self._baseline_reader is None or self._delta_reader is None:
            raise RuntimeError("PublishedSQLiteReader must be used inside a with block")

    def _read_control(self, relative: str, path: Path) -> bytes:
        raw, fingerprint_value = _read_regular_file(path)
        document = _strict_json_object(raw)
        _require_control_version(document)
        self._frozen_control[relative] = raw
        self._control_fingerprints[relative] = fingerprint_value
        return raw

    @staticmethod
    def _assert_file_fingerprints(root: Path, expected: dict[str, tuple[int, ...]]) -> None:
        for relative, fingerprint_value in expected.items():
            _raw, actual = _read_regular_file(root / relative)
            if actual != fingerprint_value:
                _fail("PUBLISHED_CONTROL_CHANGED")

    def _pin_controls(self) -> None:
        latest_path = self._control_dir / "latest.json"
        latest_raw, _ = _read_regular_file(latest_path)
        latest = _strict_json_object(latest_raw)
        _require_control_version(latest)
        if latest.get("role") is None:
            baseline_id = latest.get("generation_id")
            chain_ids = []
        elif latest.get("role") == "lossless_full_state":
            baseline_id = latest.get("baseline_generation_id")
            chain = latest.get("delta_chain")
            if not isinstance(chain, list):
                _fail("PUBLISHED_CHAIN_INVALID")
            chain_ids = []
            seen = set()
            for entry in chain:
                if not isinstance(entry, dict):
                    _fail("PUBLISHED_CHAIN_INVALID")
                generation_id = entry.get("generation_id")
                if not publisher.valid_generation_id(generation_id) or generation_id in seen:
                    _fail("PUBLISHED_CHAIN_INVALID")
                seen.add(generation_id)
                chain_ids.append(generation_id)
        else:
            _fail("PUBLISHED_LATEST_ROLE_INVALID")

        if not publisher.valid_generation_id(baseline_id):
            _fail("PUBLISHED_BASELINE_ID_INVALID")
        if self._expected_generation_id is not None and latest.get("generation_id") != self._expected_generation_id:
            _fail("PUBLISHED_EXPECTED_GENERATION_MISMATCH")
        self._latest = latest
        self._latest_raw = latest_raw
        self._pinned_latest_sha256 = hashlib.sha256(latest_raw).hexdigest()
        self._baseline_generation_id = baseline_id
        self._generation_id = latest.get("generation_id")
        self._captured_at = latest.get("captured_at")
        self._chain_generation_ids = tuple(chain_ids)

        latest_rel = "latest.json"
        self._frozen_control[latest_rel] = latest_raw
        baseline_rel = f"generations/{baseline_id}/index.json"
        baseline_path = self._control_dir / baseline_rel
        baseline_raw = self._read_control(baseline_rel, baseline_path)
        self._baseline_index = _strict_json_object(baseline_raw)
        baseline_root = baseline_path.parent
        token, records = _control_token(baseline_root, {"index.json": baseline_raw})
        self._assert_file_fingerprints(baseline_root, {
            "index.json": self._control_fingerprints[baseline_rel],
        })
        self._control_guards.append((baseline_root, token, records))

        for generation_id in self._chain_generation_ids:
            root = self._control_dir / "deltas" / "generations" / generation_id
            index_relative = f"deltas/generations/{generation_id}/index.json"
            plan_relative = f"deltas/generations/{generation_id}/delta-plan.json"
            index_raw = self._read_control(index_relative, root / "index.json")
            plan_raw = self._read_control(plan_relative, root / "delta-plan.json")
            token, records = _control_token(root, {
                "index.json": index_raw, "delta-plan.json": plan_raw,
            })
            self._assert_file_fingerprints(root, {
                "index.json": self._control_fingerprints[index_relative],
                "delta-plan.json": self._control_fingerprints[plan_relative],
            })
            self._control_guards.append((root, token, records))

    def _validate_publisher_controls(self) -> None:
        # Reuse the publisher's complete baseline/latest/delta-chain checks. The
        # adapter serves only bytes pinned above and has no write or network API.
        plan = {
            "baseline_generation_id": self._baseline_generation_id,
            "baseline_source_sha256": self._baseline_index.get("source", {}).get("sha256"),
        }
        adapter = _FrozenControlClient(self._frozen_control)
        try:
            result = delta_publisher._remote_baseline(adapter, _DUMMY_REMOTE, plan)
        except Exception as exc:
            category = getattr(exc, "category", None)
            if isinstance(category, str) and re.fullmatch(r"[A-Z0-9_]+", category):
                raise PublishedSQLiteReaderError("PUBLISHED_CONTROL_INVALID", category) from None
            _fail("PUBLISHED_CONTROL_INVALID")
        (_baseline, baseline_index, validated_latest, latest_raw,
         _latest_size, latest_sha, _chain_state) = result
        if (latest_raw != self._latest_raw or latest_sha != self._pinned_latest_sha256
                or baseline_index != self._baseline_index
                or hashlib.sha256(latest_raw).hexdigest() != self._pinned_latest_sha256):
            _fail("PUBLISHED_CONTROL_BINDING_MISMATCH")
        if validated_latest.get("role") is None:
            latest_captured_at = validated_latest.get("captured_at")
            latest_captured_kind = validated_latest.get("captured_at_kind")
            baseline_captured_at = baseline_index.get("captured_at")
            baseline_captured_kind = baseline_index.get("captured_at_kind")
            if (not isinstance(latest_captured_at, str)
                    or not isinstance(latest_captured_kind, str)
                    or latest_captured_at != baseline_captured_at
                    or latest_captured_kind != baseline_captured_kind):
                _fail("PUBLISHED_CONTROL_BINDING_MISMATCH")
        self._baseline_index = baseline_index
        self._validated_latest = validated_latest

    def _check_local_delta_mirror(self, generation_id: str, root: Path) -> None:
        control_index = self._frozen_control.get(f"deltas/generations/{generation_id}/index.json")
        control_plan = self._frozen_control.get(f"deltas/generations/{generation_id}/delta-plan.json")
        if control_index is None or control_plan is None:
            _fail("PUBLISHED_CONTROL_MISSING")
        try:
            local_index, local_index_fingerprint = _read_regular_file(root / "index.json")
            local_plan, local_plan_fingerprint = _read_regular_file(root / "delta-plan.json")
        except PublishedSQLiteReaderError:
            raise
        if local_index != control_index or local_plan != control_plan:
            _fail("PUBLISHED_LOCAL_DELTA_MISMATCH")
        token, records = _control_token(root, {
            "index.json": control_index, "delta-plan.json": control_plan,
        })
        self._assert_file_fingerprints(root, {
            "index.json": local_index_fingerprint,
            "delta-plan.json": local_plan_fingerprint,
        })
        self._control_guards.append((root, token, records))

    def _assert_baseline_binding(self, baseline: LosslessShardReader) -> None:
        source = self._baseline_index.get("source")
        if not isinstance(source, dict) or "sha256" not in source:
            _fail("PUBLISHED_BASELINE_BINDING_MISMATCH")
        if (baseline.snapshot_identifier != self._baseline_generation_id
                or baseline.source_sha256 != source["sha256"]
                or baseline.source_schema_sha256 != self._baseline_index.get("source_schema_sha256")):
            _fail("PUBLISHED_BASELINE_BINDING_MISMATCH")

        files = self._baseline_index.get("files")
        if not isinstance(files, list):
            _fail("PUBLISHED_BASELINE_BINDING_MISMATCH")

        global_slices: dict[str, dict[str, Any]] = {}
        for entry in files:
            if not isinstance(entry, dict):
                _fail("PUBLISHED_BASELINE_BINDING_MISMATCH")
            local = entry.get("local")
            if not isinstance(local, str):
                _fail("PUBLISHED_BASELINE_BINDING_MISMATCH")
            if local.startswith("slices/"):
                rel = local[len("slices/"):]
                if not rel or rel in global_slices:
                    _fail("PUBLISHED_BASELINE_BINDING_MISMATCH")
                size = entry.get("bytes")
                sha = entry.get("sha256")
                if type(size) is not int or size < 0 or not isinstance(sha, str):
                    _fail("PUBLISHED_BASELINE_BINDING_MISMATCH")
                global_slices[rel] = {"bytes": size, "sha256": sha}

        required_controls = ("manifest.json", "verification.json", "selectors-verification.json")
        for required in required_controls:
            if required not in global_slices:
                _fail("PUBLISHED_BASELINE_BINDING_MISMATCH")

        actual_records = getattr(baseline, "_verified_records", None)
        if not isinstance(actual_records, dict):
            _fail("PUBLISHED_BASELINE_BINDING_MISMATCH")

        if set(actual_records.keys()) != set(global_slices.keys()):
            _fail("PUBLISHED_BASELINE_BINDING_MISMATCH")

        for rel, record in actual_records.items():
            expected = global_slices[rel]
            if record.get("bytes") != expected["bytes"] or record.get("sha256") != expected["sha256"]:
                _fail("PUBLISHED_BASELINE_BINDING_MISMATCH")

    def _validate_reader_tail(self) -> None:
        baseline = self._baseline_reader
        reader = self._delta_reader
        if baseline is None or reader is None:
            _fail("PUBLISHED_READER_NOT_OPEN")
        self._assert_baseline_binding(baseline)

        if self._latest.get("role") == "lossless_full_state":
            latest = self._validated_latest
            tables = reader.tables()
            by_name = {item["name"]: item for item in tables}
            row_counts = {name: item["row_count"] for name, item in by_name.items()}
            columns = {name: item["column_schema"] for name, item in by_name.items()}
            foreign_keys = {name: item["foreign_keys"] for name, item in by_name.items()}
            if (reader.generation_id != latest.get("generation_id")
                    or reader.source_schema_sha256 != latest.get("source_schema_sha256")
                    or row_counts != latest.get("source_row_counts")
                    or columns != latest.get("source_table_columns")
                    or foreign_keys != latest.get("source_foreign_keys")
                    or reader.schema_objects() != latest.get("schemas")):
                _fail("PUBLISHED_TAIL_METADATA_MISMATCH")
        else:
            # The baseline manifest is the generation metadata source when the
            # publisher latest points directly at a baseline.
            manifest_tables = baseline.tables()
            current_tables = reader.tables()
            manifest_by_name = {item["name"]: item for item in manifest_tables}
            current_by_name = {item["name"]: item for item in current_tables}
            comparable_fields = ("columns", "column_schema", "foreign_keys", "row_count")
            if (reader.generation_id != baseline.snapshot_identifier
                    or reader.source_schema_sha256 != baseline.source_schema_sha256
                    or reader.schema_objects() != baseline.schema_objects()
                    or set(current_by_name) != set(manifest_by_name)
                    or any(any(current_by_name[name].get(field) != manifest_by_name[name].get(field)
                               for field in comparable_fields)
                           for name in manifest_by_name)):
                _fail("PUBLISHED_BASELINE_METADATA_MISMATCH")

    def _assert_control_objects_unchanged(self) -> None:
        for root, token, records in self._control_guards:
            try:
                token.assert_matches(root, records)
            except Exception as exc:
                raise PublishedSQLiteReaderError("PUBLISHED_CONTROL_CHANGED") from exc

    def _assert_public_read_state(self, *, guard_delta_reader: bool = True) -> None:
        self._require_open()
        self._assert_control_objects_unchanged()
        try:
            if self._baseline_reader is not None:
                self._baseline_reader._assert_verified_package()
            if guard_delta_reader and self._delta_reader is not None:
                _ = self._delta_reader.published_deltas_verified
        except Exception as exc:
            raise PublishedSQLiteReaderError("PUBLISHED_READER_INPUT_CHANGED") from exc
        self._assert_control_objects_unchanged()

    def _track_iterator(self, factory) -> Generator[Any, None, None]:
        def rows():
            inner = None
            try:
                self._assert_public_read_state()
                inner = iter(factory())
                while True:
                    self._require_open()
                    try:
                        value = next(inner)
                    except StopIteration:
                        break
                    yield value
                self._assert_public_read_state()
            finally:
                close_error: Optional[BaseException] = None
                guard_error: Optional[BaseException] = None
                try:
                    if inner is not None and hasattr(inner, "close"):
                        inner.close()
                except BaseException as error:
                    close_error = error
                try:
                    if self._active:
                        self._assert_public_read_state()
                except BaseException as error:
                    guard_error = error
                finally:
                    self._iterators.discard(outer)
                if guard_error is not None:
                    raise guard_error
                if close_error is not None:
                    raise close_error

        outer = rows()
        self._iterators.add(outer)
        return outer

    def _public_value(self, callback, *, inner_guard: bool = False):
        self._assert_public_read_state(guard_delta_reader=not inner_guard)
        value = callback()
        self._assert_public_read_state(guard_delta_reader=not inner_guard)
        return value

    @property
    def generation_id(self) -> str:
        return self._public_value(lambda: self._generation_id)

    @property
    def baseline_generation_id(self) -> str:
        return self._public_value(lambda: self._baseline_generation_id)

    @property
    def pinned_latest_sha256(self) -> str:
        return self._public_value(lambda: self._pinned_latest_sha256)

    @property
    def captured_at(self) -> Optional[str]:
        return self._public_value(lambda: self._captured_at)

    @property
    def published_control_binding_verified(self) -> bool:
        """Local pinned-control binding passed; this is not a remote re-read."""
        def checked() -> bool:
            _ = self._delta_reader.published_deltas_verified
            return True
        return self._public_value(checked, inner_guard=True)

    @property
    def published_deltas_verified(self) -> bool:
        return self._public_value(lambda: self._delta_reader.published_deltas_verified,
                                  inner_guard=True)

    def tables(self) -> list[dict[str, Any]]:
        return self._public_value(lambda: self._delta_reader.tables(), inner_guard=True)

    def columns(self, table: str) -> list[dict[str, Any]]:
        return self._public_value(lambda: self._delta_reader.columns(table), inner_guard=True)

    def foreign_keys(self, table: str) -> list[dict[str, Any]]:
        return self._public_value(lambda: self._delta_reader.foreign_keys(table), inner_guard=True)

    def schema_objects(self) -> list[dict[str, Any]]:
        return self._public_value(lambda: self._delta_reader.schema_objects(), inner_guard=True)

    def row_count(self, table: str) -> int:
        return self._public_value(lambda: self._delta_reader.row_count(table), inner_guard=True)

    def iter_rows(self, table: str) -> Iterator[tuple[int, Optional[int], tuple[Any, ...]]]:
        self._assert_public_read_state()
        return self._track_iterator(lambda: self._delta_reader.iter_rows(table))

    def iter_selected_matches(self, mode: str, rule_token: Optional[str] = None) -> Iterator[tuple[Any, ...]]:
        self._assert_public_read_state()
        return self._track_iterator(
            lambda: self._delta_reader.iter_selected_matches(mode, rule_token)
        )


__all__ = ["PublishedSQLiteReader", "PublishedSQLiteReaderError"]
