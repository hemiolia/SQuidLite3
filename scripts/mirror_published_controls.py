#!/usr/bin/env python3
"""Mirror and validate publisher control JSON without downloading data files.

The mirror is a local control cache for PublishedSQLiteReader. A successful
receipt covers only the pinned latest/index/plan controls: it does not mean the
publisher's SQLite, XLSX, or other remote artifacts were downloaded or
verified, and it does not prove real-time synchronization.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat
import sys
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import nas_full_data_publish as publisher  # noqa: E402
import publish_full_data_delta as delta_publisher  # noqa: E402


MAX_CONTROL_BYTES = publisher.MAX_CONTROL_BYTES
MAX_CONTROL_OBJECTS = 1024
MAX_TOTAL_CONTROL_BYTES = 256 * 1024 * 1024
_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z", re.ASCII)
_STAMP_RE = re.compile(r"[0-9]{8}T[0-9]{6}\.[0-9]{6}Z\Z", re.ASCII)


def _fail(category: str) -> None:
    raise publisher.PublishError(category)


def _strict_json_object(raw: bytes) -> dict[str, Any]:
    def reject_duplicates(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate JSON key")
            result[key] = value
        return result

    def reject_constant(_value):
        raise ValueError("non-standard JSON number")

    try:
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=reject_duplicates,
            parse_constant=reject_constant,
        )
    except (UnicodeError, json.JSONDecodeError, ValueError):
        _fail("REMOTE_CONTROL_JSON_INVALID")
    if not isinstance(value, dict):
        _fail("REMOTE_CONTROL_JSON_INVALID")
    if type(value.get("version")) is not int or value["version"] != 1:
        _fail("REMOTE_CONTROL_VERSION_INVALID")
    return value


def _safe_relative(relative: str) -> str:
    try:
        relative = publisher.safe_rel_path(relative)
    except publisher.PublishError:
        _fail("CONTROL_PATH_INVALID")
    parts = PurePosixPath(relative).parts
    allowed = False
    if relative == "latest.json":
        allowed = True
    elif len(parts) == 3 and parts[0] == "generations" and parts[2] == "index.json":
        allowed = publisher.valid_generation_id(parts[1])
    elif (len(parts) == 4 and parts[:2] == ("deltas", "generations")
          and publisher.valid_generation_id(parts[2])
          and parts[3] in ("index.json", "delta-plan.json")):
        allowed = True
    if not allowed:
        _fail("CONTROL_PATH_INVALID")
    return relative


class _RemoteControlCache:
    """Read and pin only allowlisted publisher control objects."""

    def __init__(self, client, remote: str):
        self.client = client
        self.remote = remote
        self.objects: dict[str, dict[str, Any]] = {}
        self.total_bytes = 0

    def relative(self, remote_path: str) -> str:
        prefix = self.remote + "/"
        if not isinstance(remote_path, str) or not remote_path.startswith(prefix):
            _fail("CONTROL_PATH_INVALID")
        return _safe_relative(remote_path[len(prefix):])

    def load(self, relative: str) -> dict[str, Any]:
        relative = _safe_relative(relative)
        cached = self.objects.get(relative)
        if cached is not None:
            return cached
        if len(self.objects) >= MAX_CONTROL_OBJECTS:
            _fail("CONTROL_OBJECT_LIMIT")

        remote_path = publisher.join_remote(self.remote, relative)
        try:
            metadata = self.client.stat(remote_path)
        except publisher.PublishError:
            raise
        except Exception:
            _fail("REMOTE_CONTROL_READ_FAILED")
        if metadata is None:
            _fail("REMOTE_OBJECT_MISSING")
        if (not isinstance(metadata, dict) or type(metadata.get("Size")) is not int
                or not 1 <= metadata["Size"] <= MAX_CONTROL_BYTES
                or metadata.get("IsDir") is not False):
            _fail("REMOTE_CONTROL_METADATA_INVALID")
        expected_size = metadata["Size"]
        if self.total_bytes + expected_size > MAX_TOTAL_CONTROL_BYTES:
            _fail("CONTROL_TOTAL_BYTE_LIMIT")

        try:
            size, digest = self.client.readback(remote_path)
        except publisher.PublishError:
            raise
        except Exception:
            _fail("REMOTE_CONTROL_READ_FAILED")
        if (type(size) is not int or size != expected_size
                or not isinstance(digest, str) or not _SHA256_RE.fullmatch(digest)):
            _fail("REMOTE_CONTROL_READBACK_MISMATCH")
        try:
            raw = self.client.readback_bytes(remote_path, size, digest)
        except publisher.PublishError:
            raise
        except Exception:
            _fail("REMOTE_CONTROL_READ_FAILED")
        if (not isinstance(raw, bytes) or len(raw) != size
                or hashlib.sha256(raw).hexdigest() != digest):
            _fail("REMOTE_CONTROL_READBACK_MISMATCH")
        document = _strict_json_object(raw)
        cached = {"raw": raw, "document": document, "bytes": size, "sha256": digest}
        self.objects[relative] = cached
        self.total_bytes += size
        return cached


class _CachedReadOnlyClient:
    """Read-only client adapter used by the existing delta publisher checks."""

    def __init__(self, cache: _RemoteControlCache):
        self.cache = cache

    def stat(self, remote_path: str):
        control = self.cache.load(self.cache.relative(remote_path))
        return {"IsDir": False, "Size": control["bytes"]}

    def readback(self, remote_path: str):
        control = self.cache.load(self.cache.relative(remote_path))
        return control["bytes"], control["sha256"]

    def readback_bytes(self, remote_path: str, expected_bytes: int, expected_sha: str):
        control = self.cache.load(self.cache.relative(remote_path))
        if (expected_bytes != control["bytes"] or expected_sha != control["sha256"]):
            _fail("REMOTE_CONTROL_READBACK_MISMATCH")
        return control["raw"]


def _absolute_control_dir(value) -> Path:
    try:
        raw = os.fspath(value)
        if not isinstance(raw, str) or not raw or "\x00" in raw:
            _fail("CONTROL_DIRECTORY_INVALID")
        supplied = Path(raw).expanduser()
        if ".." in supplied.parts:
            _fail("CONTROL_DIRECTORY_INVALID")
        if not supplied.is_absolute():
            supplied = Path.cwd() / supplied
        path = Path(os.path.abspath(os.fspath(supplied)))
        publisher.verify_no_symlink_components(path)

        missing = []
        cursor = path
        while True:
            try:
                info = os.lstat(cursor)
            except FileNotFoundError:
                if cursor.parent == cursor:
                    _fail("CONTROL_DIRECTORY_INVALID")
                missing.append(cursor)
                cursor = cursor.parent
                continue
            except OSError:
                _fail("CONTROL_DIRECTORY_INVALID")
            if not stat.S_ISDIR(info.st_mode):
                _fail("CONTROL_DIRECTORY_INVALID")
            break

        for directory in reversed(missing):
            publisher.verify_no_symlink_components(directory.parent)
            created = False
            try:
                previous_umask = os.umask(0o077)
                try:
                    os.mkdir(directory, 0o700)
                    created = True
                finally:
                    os.umask(previous_umask)
            except FileExistsError:
                # A concurrently created ordinary directory is treated as
                # existing; only directories created here receive a new mode.
                pass
            except publisher.PublishError:
                raise
            except Exception:
                _fail("CONTROL_DIRECTORY_INVALID")
            publisher.verify_no_symlink_components(directory)
            created_info = os.lstat(directory)
            if not stat.S_ISDIR(created_info.st_mode):
                _fail("CONTROL_DIRECTORY_INVALID")
            if created and created_info.st_mode & 0o777 != 0o700:
                _fail("CONTROL_DIRECTORY_NOT_PRIVATE")
        publisher.verify_no_symlink_components(path)
        info = os.lstat(path)
    except publisher.PublishError:
        raise
    except Exception:
        _fail("CONTROL_DIRECTORY_INVALID")
    if not stat.S_ISDIR(info.st_mode) or info.st_mode & 0o077:
        _fail("CONTROL_DIRECTORY_NOT_PRIVATE")
    return path


def _ensure_directory(path: Path) -> None:
    try:
        publisher.verify_no_symlink_components(path)
        if not path.exists():
            path.mkdir(mode=0o700)
        info = os.lstat(path)
    except publisher.PublishError:
        raise
    except Exception:
        _fail("LOCAL_CONTROL_PATH_INVALID")
    if not stat.S_ISDIR(info.st_mode) or info.st_mode & 0o077:
        _fail("LOCAL_CONTROL_DIRECTORY_NOT_PRIVATE")


def _ensure_relative_parent(root: Path, relative: str) -> Path:
    relative = _safe_relative(relative)
    parent_parts = PurePosixPath(relative).parts[:-1]
    current = root
    for part in parent_parts:
        current = current / part
        _ensure_directory(current)
    return current


def _safe_local_file(root: Path, relative: str, *, ensure_parent: bool = False) -> Path:
    relative = _safe_relative(relative)
    if ensure_parent:
        parent = _ensure_relative_parent(root, relative)
        target = parent / PurePosixPath(relative).name
    else:
        target = root.joinpath(*PurePosixPath(relative).parts)
    try:
        publisher.verify_no_symlink_components(target)
    except publisher.PublishError:
        _fail("LOCAL_CONTROL_PATH_INVALID")
    return target


def _read_existing_control(path: Path) -> bytes | None:
    try:
        before = os.lstat(path)
    except FileNotFoundError:
        return None
    except OSError:
        _fail("LOCAL_CONTROL_READ_FAILED")
    if not stat.S_ISREG(before.st_mode) or before.st_mode & 0o077:
        _fail("LOCAL_CONTROL_FILE_INVALID")
    if not 1 <= before.st_size <= MAX_CONTROL_BYTES:
        _fail("LOCAL_CONTROL_FILE_INVALID")
    try:
        fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
                     | getattr(os, "O_CLOEXEC", 0))
    except OSError:
        _fail("LOCAL_CONTROL_FILE_INVALID")
    try:
        opened = os.fstat(fd)
        fingerprint = (opened.st_dev, opened.st_ino, opened.st_mode, opened.st_size,
                       opened.st_mtime_ns, opened.st_ctime_ns)
        before_fingerprint = (before.st_dev, before.st_ino, before.st_mode, before.st_size,
                              before.st_mtime_ns, before.st_ctime_ns)
        if not stat.S_ISREG(opened.st_mode) or fingerprint != before_fingerprint:
            _fail("LOCAL_CONTROL_FILE_CHANGED")
        blocks = []
        remaining = opened.st_size
        while remaining:
            block = os.read(fd, min(1024 * 1024, remaining))
            if not block:
                _fail("LOCAL_CONTROL_FILE_CHANGED")
            blocks.append(block)
            remaining -= len(block)
        if os.read(fd, 1):
            _fail("LOCAL_CONTROL_FILE_CHANGED")
        raw = b"".join(blocks)
        after = os.fstat(fd)
        path_after = os.lstat(path)
        after_fingerprint = (after.st_dev, after.st_ino, after.st_mode, after.st_size,
                             after.st_mtime_ns, after.st_ctime_ns)
        path_fingerprint = (path_after.st_dev, path_after.st_ino, path_after.st_mode,
                            path_after.st_size, path_after.st_mtime_ns, path_after.st_ctime_ns)
        if after_fingerprint != fingerprint or path_fingerprint != fingerprint:
            _fail("LOCAL_CONTROL_FILE_CHANGED")
        return raw
    except publisher.PublishError:
        raise
    except OSError:
        _fail("LOCAL_CONTROL_FILE_CHANGED")
    finally:
        os.close(fd)


def _write_immutable_control(path: Path, raw: bytes) -> None:
    existing = _read_existing_control(path)
    if existing is not None:
        if existing != raw:
            _fail("LOCAL_IMMUTABLE_CONTROL_CONFLICT")
        return
    try:
        temporary = publisher.write_temp_bytes(path.parent, raw, ".published-control-")
    except publisher.PublishError:
        raise
    except Exception:
        _fail("LOCAL_CONTROL_WRITE_FAILED")
    try:
        try:
            os.link(temporary, path, follow_symlinks=False)
            publisher.fsync_dir(path.parent)
        except FileExistsError:
            raced = _read_existing_control(path)
            if raced != raw:
                _fail("LOCAL_IMMUTABLE_CONTROL_CONFLICT")
        except publisher.PublishError:
            raise
        except OSError:
            _fail("LOCAL_CONTROL_WRITE_FAILED")
    finally:
        try:
            temporary.unlink()
            publisher.fsync_dir(path.parent)
        except FileNotFoundError:
            pass
        except OSError:
            _fail("LOCAL_CONTROL_WRITE_FAILED")


def _history_name(control_dir: Path, raw: bytes) -> Path:
    history = control_dir / "history"
    _ensure_directory(history)
    digest = hashlib.sha256(raw).hexdigest()
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    if not _STAMP_RE.fullmatch(stamp):
        _fail("LOCAL_HISTORY_PATH_INVALID")
    for suffix in range(1000):
        tail = "" if suffix == 0 else f"-{suffix:03d}"
        candidate = history / f"{stamp}-{digest}{tail}.json"
        try:
            existing = _read_existing_control(candidate)
        except publisher.PublishError:
            raise
        if existing is None or existing == raw:
            return candidate
    _fail("LOCAL_HISTORY_PATH_INVALID")


def _replace_latest(control_dir: Path, raw: bytes, previous_raw: bytes | None) -> None:
    latest_path = _safe_local_file(control_dir, "latest.json")
    current_raw = _read_existing_control(latest_path)
    if current_raw != previous_raw:
        _fail("LOCAL_LATEST_CHANGED")
    if current_raw == raw:
        return
    if current_raw is not None:
        history_path = _history_name(control_dir, current_raw)
        _write_immutable_control(history_path, current_raw)
    try:
        temporary = publisher.write_temp_bytes(control_dir, raw, ".published-latest-")
        os.replace(temporary, latest_path)
        publisher.fsync_dir(control_dir)
    except publisher.PublishError:
        raise
    except Exception:
        _fail("LOCAL_LATEST_WRITE_FAILED")
    finally:
        try:
            temporary.unlink()
        except (FileNotFoundError, UnboundLocalError):
            pass
        except OSError:
            _fail("LOCAL_LATEST_WRITE_FAILED")


def _verify_installed_immutable_controls(
    root: Path, immutable_targets: list[tuple[str, Path, bytes]]
) -> None:
    for relative, _target, expected in immutable_targets:
        try:
            target = _safe_local_file(root, relative, ensure_parent=False)
            actual = _read_existing_control(target)
        except publisher.PublishError:
            _fail("LOCAL_IMMUTABLE_CONTROL_CONFLICT")
        if actual != expected:
            _fail("LOCAL_IMMUTABLE_CONTROL_CONFLICT")


class _MirrorLock:
    def __init__(self, control_dir: Path):
        self.path = control_dir / ".published-controls.lock"
        self.fd: int | None = None

    def __enter__(self):
        try:
            publisher.verify_no_symlink_components(self.path)
            flags = os.O_CREAT | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0)
            self.fd = os.open(self.path, flags, 0o600)
            info = os.fstat(self.fd)
            if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077:
                _fail("CONTROL_LOCK_INVALID")
            fcntl.flock(self.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except publisher.PublishError:
            if self.fd is not None:
                os.close(self.fd)
                self.fd = None
            raise
        except (OSError, BlockingIOError):
            if self.fd is not None:
                os.close(self.fd)
                self.fd = None
            _fail("CONTROL_LOCK_BUSY")
        return self

    def __exit__(self, *_args):
        if self.fd is not None:
            try:
                fcntl.flock(self.fd, fcntl.LOCK_UN)
            finally:
                os.close(self.fd)
                self.fd = None


def _verify_remote_directories(client) -> None:
    verifier = getattr(client, "verify_directory_bindings", None)
    if not callable(verifier):
        _fail("REMOTE_DIRECTORY_VERIFICATION_FAILED")
    try:
        verifier()
    except publisher.PublishError:
        raise
    except Exception:
        _fail("REMOTE_DIRECTORY_VERIFICATION_FAILED")


def mirror_published_controls(client, remote: str, control_dir) -> dict[str, Any]:
    """Fetch and validate latest/index/plan controls into a private local mirror.

    Only ``stat``, ``readback``, ``readback_bytes`` and
    ``verify_directory_bindings`` are used on *client*. Data artifacts are
    never downloaded and the receipt does not claim they were verified.
    """
    remote = publisher.validate_remote(remote)
    root = _absolute_control_dir(control_dir)
    for method in ("stat", "readback", "readback_bytes", "verify_directory_bindings"):
        if not callable(getattr(client, method, None)):
            _fail("REMOTE_CLIENT_INVALID")

    with _MirrorLock(root):
        _verify_remote_directories(client)
        cache = _RemoteControlCache(client, remote)
        latest_control = cache.load("latest.json")
        latest_document = latest_control["document"]
        role = latest_document.get("role")
        if role is None:
            baseline_id = latest_document.get("generation_id")
            generation_id = baseline_id
        elif role == "lossless_full_state":
            baseline_id = latest_document.get("baseline_generation_id")
            generation_id = latest_document.get("generation_id")
        else:
            _fail("LATEST_ROLE_INVALID")
        if (not publisher.valid_generation_id(baseline_id)
                or not publisher.valid_generation_id(generation_id)):
            _fail("LATEST_GENERATION_ID_INVALID")

        if role is None:
            baseline_control = cache.load(f"generations/{baseline_id}/index.json")
            baseline_source = baseline_control["document"].get("source")
            baseline_source_sha = (baseline_source.get("sha256")
                                   if isinstance(baseline_source, dict) else None)
        else:
            baseline_source_sha = latest_document.get("baseline_source_sha256")

        adapter = _CachedReadOnlyClient(cache)
        try:
            validated = delta_publisher._remote_baseline(
                adapter,
                remote,
                {"baseline_generation_id": baseline_id,
                 "baseline_source_sha256": baseline_source_sha},
            )
        except publisher.PublishError:
            raise
        except Exception:
            _fail("REMOTE_CONTROL_VALIDATION_FAILED")

        baseline_index = validated[1]
        if (role is None
                and (latest_document.get("captured_at") != baseline_index.get("captured_at")
                     or latest_document.get("captured_at_kind") !=
                     baseline_index.get("captured_at_kind"))):
            _fail("LATEST_BASELINE_MISMATCH")

        _verify_remote_directories(client)
        pinned_latest = cache.objects.get("latest.json")
        if pinned_latest is None:
            _fail("LATEST_CONTROL_MISSING")
        if len(cache.objects) > MAX_CONTROL_OBJECTS or cache.total_bytes > MAX_TOTAL_CONTROL_BYTES:
            _fail("CONTROL_LIMIT_EXCEEDED")

        immutable_relatives = sorted(relative for relative in cache.objects
                                     if relative != "latest.json")
        immutable_targets: list[tuple[str, Path, bytes]] = []
        for relative in immutable_relatives:
            target = _safe_local_file(root, relative, ensure_parent=False)
            raw = cache.objects[relative]["raw"]
            existing = _read_existing_control(target)
            if existing is not None and existing != raw:
                _fail("LOCAL_IMMUTABLE_CONTROL_CONFLICT")
            immutable_targets.append((relative, target, raw))

        latest_path = _safe_local_file(root, "latest.json", ensure_parent=False)
        previous_latest = _read_existing_control(latest_path)
        for relative, _target, raw in immutable_targets:
            target = _safe_local_file(root, relative, ensure_parent=True)
            _write_immutable_control(target, raw)
        _verify_installed_immutable_controls(root, immutable_targets)
        _replace_latest(root, pinned_latest["raw"], previous_latest)

        return {
            "generation_id": generation_id,
            "baseline_generation_id": baseline_id,
            "control_count": len(cache.objects),
            "control_bytes": cache.total_bytes,
            "pinned_latest_sha256": pinned_latest["sha256"],
            "controls_full_readback": True,
            "all_remote_artifacts_verified": False,
            "realtime_synchronized": False,
        }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--remote", required=True)
    parser.add_argument("--control-dir", required=True)
    parser.add_argument("--rclone-bin", default="rclone")
    args = parser.parse_args(argv)
    try:
        client = publisher.Rclone(args.rclone_bin)
        receipt = mirror_published_controls(client, args.remote, args.control_dir)
    except publisher.PublishError as exc:
        category = getattr(exc, "category", "")
        if not isinstance(category, str) or not re.fullmatch(r"[A-Z0-9_]+", category):
            category = "CONTROL_MIRROR_FAILED"
        print(json.dumps({"status": "error", "category": category}, sort_keys=True),
              file=sys.stderr)
        return 1
    except Exception:
        print(json.dumps({"status": "error", "category": "CONTROL_MIRROR_FAILED"},
                         sort_keys=True), file=sys.stderr)
        return 1
    print(json.dumps(receipt, ensure_ascii=True, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
