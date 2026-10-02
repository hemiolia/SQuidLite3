#!/usr/bin/env python3
"""継続更新の準備と公開を独立threadで進めるworker。

API contract:
  ``prepare_fn(db, baseline, manifest, generation_dir, generation_id,
  baseline_generation_id, *, previous=plan, max_part_bytes=limit,
  baseline_verification=runtime_token) -> prepared plan``
  ``publish_fn(generation_dir, remote, state_dir, rclone_bin=...) -> result``

The default functions are ``prepare_full_data_delta.prepare_delta`` and
``publish_full_data_delta.publish_delta``. The latter owns the common remote
publish lock and writes ``delta-published-checkpoint.json`` only after the
immutable generation and latest pointer have passed full remote readback.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import fcntl
import hashlib
import inspect
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import signal
import stat
import sys
import tempfile
import threading
from typing import Any, Callable

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src/python"))
sys.path.insert(0, str(ROOT / "scripts"))

import prepare_full_data_delta as preparer  # noqa: E402
import publish_full_data_delta as delta_publisher  # noqa: E402
from ikarchive.change_feed import (  # noqa: E402
    CHANGE_TABLE,
    _feed_integrity_guard_status,
    _schema_sha256,
)
from ikarchive.writer_guards import inspect_writer_guards  # noqa: E402
from ikarchive.lossless_sqlite import MAX_ALLOWED_BYTES, MIN_MAX_BYTES  # noqa: E402


WORKER_ROLE = "lossless_delta_worker"
PREPARED_ROLE = "lossless_delta_prepared_checkpoint"
PREPARED_STATE_NAME = "delta-prepared-checkpoint.json"
WORKER_STATE_NAME = "delta-worker-state.json"
PUBLISHED_STATE_NAME = "delta-published-checkpoint.json"
LOCK_NAME = ".full-data-delta-worker.lock"
MAX_STATE_BYTES = 16 * 1024 * 1024
GENERATION_ID_RE = re.compile(r"[0-9]{8}T[0-9]{6}Z-[0-9a-f]{8}\Z")
SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
ERROR_CATEGORY_RE = re.compile(r"[A-Z][A-Z0-9_]{0,127}\Z")


class WorkerError(Exception):
    def __init__(self, category: str):
        super().__init__(category)
        self.category = category


def _exception_category(error: Exception, fallback: str) -> str:
    explicit = getattr(error, "category", None)
    if isinstance(explicit, str) and ERROR_CATEGORY_RE.fullmatch(explicit):
        return explicit
    message = str(error)
    return message if ERROR_CATEGORY_RE.fullmatch(message) else fallback


@dataclass(frozen=True)
class WorkerConfig:
    db: Path
    baseline: Path
    baseline_manifest: Path
    baseline_generation_id: str
    work_dir: Path
    state_dir: Path
    remote: str
    rclone_bin: str = "rclone"
    initial_generation_id: str | None = None
    interval_seconds: int = 30
    max_part_bytes: int = 20 * 1024 * 1024


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_utc(value: Any) -> datetime:
    if not isinstance(value, str):
        raise WorkerError("TIMESTAMP_INVALID")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise WorkerError("TIMESTAMP_INVALID") from exc
    if parsed.tzinfo is None:
        raise WorkerError("TIMESTAMP_INVALID")
    return parsed.astimezone(timezone.utc)


def _valid_id(value: Any) -> bool:
    return isinstance(value, str) and GENERATION_ID_RE.fullmatch(value) is not None


def _path_is_within(path: Path, parent: Path) -> bool:
    try:
        path.absolute().relative_to(parent.absolute())
        return True
    except ValueError:
        return False


def _verify_no_symlink_components(path: Path) -> None:
    path = Path(path).expanduser().absolute()
    if ".." in Path(path).parts:
        raise WorkerError("PATH_SAFETY_ERROR")
    for item in (path, *path.parents):
        try:
            info = item.lstat()
        except FileNotFoundError:
            continue
        except OSError as exc:
            raise WorkerError("PATH_SAFETY_ERROR") from exc
        if stat.S_ISLNK(info.st_mode):
            raise WorkerError("PATH_SAFETY_ERROR")


def _ensure_private_state_dir(path: Path) -> Path:
    path = Path(path).expanduser().absolute()
    _verify_no_symlink_components(path)
    existed = path.exists()
    if not existed:
        try:
            path.mkdir(parents=True, mode=0o700, exist_ok=False)
        except FileExistsError:
            existed = True
        except OSError as exc:
            raise WorkerError("STATE_DIRECTORY_ERROR") from exc
        if not existed:
            os.chmod(path, 0o700, follow_symlinks=False)
    _verify_no_symlink_components(path)
    try:
        info = path.lstat()
    except OSError as exc:
        raise WorkerError("STATE_DIRECTORY_ERROR") from exc
    if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid()
            or stat.S_IMODE(info.st_mode) != 0o700):
        raise WorkerError("STATE_DIRECTORY_ERROR")
    return path


def _ensure_work_dir(path: Path) -> Path:
    path = Path(path).expanduser().absolute()
    _verify_no_symlink_components(path)
    if not path.exists():
        try:
            path.mkdir(parents=True, mode=0o700)
        except OSError as exc:
            raise WorkerError("WORK_DIRECTORY_ERROR") from exc
    _verify_no_symlink_components(path)
    if not path.is_dir():
        raise WorkerError("WORK_DIRECTORY_ERROR")
    return path


def _read_regular_bytes(path: Path, limit: int = MAX_STATE_BYTES) -> bytes:
    _verify_no_symlink_components(path)
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
    try:
        fd = os.open(path, flags)
    except OSError as exc:
        raise WorkerError("STATE_READ_ERROR") from exc
    try:
        info = os.fstat(fd)
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                or info.st_mode & 0o077 or info.st_size > limit):
            raise WorkerError("STATE_FILE_INVALID")
        with os.fdopen(fd, "rb") as stream:
            fd = -1
            data = stream.read(limit + 1)
        if len(data) > limit:
            raise WorkerError("STATE_FILE_INVALID")
        return data
    except WorkerError:
        raise
    except OSError as exc:
        raise WorkerError("STATE_READ_ERROR") from exc
    finally:
        if fd >= 0:
            os.close(fd)


def _regular_file_exists(path: Path) -> bool:
    """Return presence without following symlinks; reject non-file entries."""
    _verify_no_symlink_components(path)
    try:
        info = Path(path).lstat()
    except FileNotFoundError:
        return False
    except OSError as exc:
        raise WorkerError("GENERATION_PATH_INVALID") from exc
    if not stat.S_ISREG(info.st_mode):
        raise WorkerError("GENERATION_PATH_INVALID")
    return True


def _validate_generation_directory(path: Path) -> None:
    _verify_no_symlink_components(path)
    try:
        info = Path(path).lstat()
    except OSError as exc:
        raise WorkerError("GENERATION_PATH_INVALID") from exc
    if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid()
            or stat.S_IMODE(info.st_mode) != 0o700):
        raise WorkerError("GENERATION_PATH_INVALID")


def _read_json(path: Path, *, missing_ok: bool = True) -> dict[str, Any] | None:
    try:
        raw = _read_regular_bytes(path)
    except WorkerError as exc:
        if missing_ok and exc.category == "STATE_READ_ERROR" and not Path(path).exists():
            return None
        raise
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise WorkerError("STATE_JSON_INVALID") from exc
    if not isinstance(value, dict):
        raise WorkerError("STATE_JSON_INVALID")
    return value


def _atomic_json(path: Path, value: dict[str, Any]) -> None:
    path = Path(path)
    _verify_no_symlink_components(path)
    data = (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
    if len(data) > MAX_STATE_BYTES:
        raise WorkerError("STATE_FILE_TOO_LARGE")
    fd = None
    temporary = None
    try:
        fd, temporary_name = tempfile.mkstemp(prefix=".delta-worker-", dir=path.parent)
        temporary = Path(temporary_name)
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "wb") as stream:
            fd = None
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory_fd = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0))
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except WorkerError:
        raise
    except OSError as exc:
        raise WorkerError("STATE_WRITE_ERROR") from exc
    finally:
        if fd is not None:
            os.close(fd)
        if temporary is not None:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass


@contextmanager
def _worker_lock(state_dir: Path):
    path = state_dir / LOCK_NAME
    _verify_no_symlink_components(path)
    fd = None
    try:
        fd = os.open(path, os.O_CREAT | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0), 0o600)
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
            raise WorkerError("WORKER_LOCK_ERROR")
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except WorkerError:
        if fd is not None:
            os.close(fd)
        raise
    except OSError as exc:
        if fd is not None:
            os.close(fd)
        raise WorkerError("WORKER_LOCK_BUSY") from exc
    try:
        yield
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


def _baseline_binding(db: Path, manifest_path: Path) -> dict[str, Any]:
    _verify_no_symlink_components(db)
    _verify_no_symlink_components(manifest_path)
    try:
        info = db.lstat()
        manifest_bytes = _read_regular_bytes(manifest_path)
        manifest = json.loads(manifest_bytes.decode("utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise WorkerError("BASELINE_MANIFEST_INVALID") from exc
    if not isinstance(manifest, dict) or not isinstance(manifest.get("raw_snapshot"), dict):
        raise WorkerError("BASELINE_MANIFEST_INVALID")
    raw = manifest["raw_snapshot"]
    if (not stat.S_ISREG(info.st_mode) or manifest.get("storage") != "plaintext"
            or manifest.get("encryption") is not None or raw.get("quick_check") != "ok"
            or raw.get("basename") != db.name
            or type(raw.get("bytes")) is not int or raw["bytes"] <= 0
            or raw["bytes"] != info.st_size
            or not isinstance(raw.get("sha256"), str) or not SHA256_RE.fullmatch(raw["sha256"])):
        raise WorkerError("BASELINE_MANIFEST_INVALID")
    return {"bytes": raw["bytes"], "sha256": raw["sha256"], "manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest()}


def _source_probe(db: Path) -> dict[str, Any]:
    _verify_no_symlink_components(db)
    try:
        info = db.lstat()
        if not stat.S_ISREG(info.st_mode):
            raise WorkerError("SOURCE_DATABASE_INVALID")
        connection = sqlite3.connect(db.as_uri() + "?mode=ro", uri=True, timeout=10)
    except WorkerError:
        raise
    except (OSError, sqlite3.Error) as exc:
        raise WorkerError("SOURCE_PROBE_FAILED") from exc
    try:
        connection.execute("PRAGMA query_only=ON")
        connection.execute("BEGIN")
        captured = connection.execute("SELECT strftime('%Y-%m-%dT%H:%M:%fZ','now')").fetchone()[0]
        objects = [
            {"type": row[0], "name": row[1], "tbl_name": row[2], "sql": row[3]}
            for row in connection.execute(
                "SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name"
            )
        ]
        if not any(item["type"] == "table" and item["name"] == CHANGE_TABLE for item in objects):
            raise WorkerError("CHANGE_FEED_NOT_INSTALLED")
        highwater = connection.execute(
            f'SELECT COALESCE(MAX(event_id),0) FROM "{CHANGE_TABLE}"'
        ).fetchone()[0]
        guard = _feed_integrity_guard_status(connection)
        writer_guard = inspect_writer_guards(connection)
        if type(highwater) is not int or highwater < 0:
            raise WorkerError("CHANGE_FEED_HIGHWATER_INVALID")
        return {
            "observed_at": captured,
            "through_event_id": highwater,
            "source_schema_sha256": _schema_sha256(objects),
            "feed_integrity_guard_status": guard,
            "writer_guard_status": writer_guard,
            "all_writers_contract_enforced": writer_guard.get("all_writers_contract_enforced") is True,
        }
    except WorkerError:
        raise
    except (sqlite3.Error, ValueError) as exc:
        raise WorkerError("SOURCE_PROBE_FAILED") from exc
    finally:
        connection.rollback()
        connection.close()


def _capture_anchor(plan: dict[str, Any], plan_sha256: str) -> dict[str, Any]:
    return {
        "generation_id": plan["generation_id"],
        "plan_sha256": plan_sha256,
        "through_event_id": plan["through_event_id"],
        "source_schema_sha256": plan["source_schema_sha256"],
        "captured_at": plan["captured_at"],
    }


class DeltaWorker:
    """Prepare immutable delta generations while a separate thread publishes them.

    The callbacks have the signatures documented in the module docstring.
    ``probe_fn(db_path)`` may be injected for deterministic tests; production
uses a read-only SQLite probe that reads only schema objects, feed guards,
and MAX(event_id), never per-table COUNT(*). Injected preparation callbacks
retain their original keyword contract; only the default preparer receives the
process-local baseline verification token when that API is available.
    """

    def __init__(
        self,
        config: WorkerConfig,
        prepare_fn: Callable[..., dict[str, Any]] | None = None,
        publish_fn: Callable[..., dict[str, Any]] | None = None,
        *,
        generation_id_fn: Callable[[], str] | None = None,
        probe_fn: Callable[[Path], dict[str, Any]] | None = None,
        wait_fn: Callable[[threading.Event, float], Any] | None = None,
    ) -> None:
        self.config = config
        self._uses_default_preparer = prepare_fn is None
        self.prepare_fn = prepare_fn or preparer.prepare_delta
        self._baseline_verifier = getattr(preparer, "verify_baseline_once", None)
        try:
            prepare_parameters = inspect.signature(self.prepare_fn).parameters
        except (TypeError, ValueError):
            prepare_parameters = {}
        self._baseline_token_supported = (
            self._uses_default_preparer
            and callable(self._baseline_verifier)
            and "baseline_verification" in prepare_parameters
        )
        self._baseline_verification: Any = None
        self.publish_fn = publish_fn or delta_publisher.publish_delta
        self.generation_id_fn = generation_id_fn or preparer.new_generation_id
        self.probe_fn = probe_fn or _source_probe
        self.wait_fn = wait_fn or (lambda event, seconds: event.wait(seconds))
        self.db = Path(config.db).expanduser().absolute()
        self.baseline = Path(config.baseline).expanduser().absolute()
        self.baseline_manifest = Path(config.baseline_manifest).expanduser().absolute()
        self.work_dir = _ensure_work_dir(Path(config.work_dir))
        self.state_dir = _ensure_private_state_dir(Path(config.state_dir))
        if not _valid_id(config.baseline_generation_id):
            raise WorkerError("BASELINE_GENERATION_ID_INVALID")
        if config.initial_generation_id is not None and not _valid_id(config.initial_generation_id):
            raise WorkerError("INITIAL_GENERATION_ID_INVALID")
        if type(config.interval_seconds) is not int or not 5 <= config.interval_seconds <= 60:
            raise WorkerError("INTERVAL_INVALID")
        if (type(config.max_part_bytes) is not int
                or not MIN_MAX_BYTES <= config.max_part_bytes <= MAX_ALLOWED_BYTES):
            raise WorkerError("PART_LIMIT_INVALID")
        for path in (self.db, self.baseline, self.baseline_manifest):
            _verify_no_symlink_components(path)
        if self.db == self.baseline:
            raise WorkerError("SOURCE_PATH_INVALID")
        input_paths = (self.db, self.baseline, self.baseline_manifest)
        output_dirs = (self.work_dir, self.state_dir)
        if (any(_path_is_within(source, output) for source in input_paths for output in output_dirs)
                or any(_path_is_within(output, source) for source in input_paths for output in output_dirs)
                or _path_is_within(self.state_dir, self.work_dir)
                or _path_is_within(self.work_dir, self.state_dir)):
            raise WorkerError("PATH_OVERLAP_ERROR")
        if not self.db.is_file() or not self.baseline.is_file() or not self.baseline_manifest.is_file():
            raise WorkerError("SOURCE_PATH_INVALID")
        try:
            self.remote = delta_publisher.publisher.validate_remote(config.remote)
        except Exception as exc:
            raise WorkerError("REMOTE_INVALID") from exc
        self.rclone_bin = config.rclone_bin
        self.interval_seconds = config.interval_seconds
        self.max_part_bytes = config.max_part_bytes
        self.initial_generation_id = config.initial_generation_id
        self.baseline_binding = _baseline_binding(self.baseline, self.baseline_manifest)
        self._mutex = threading.RLock()
        self._checkpoint_init_lock = threading.Lock()
        self._prepared: dict[str, Any] | None = None
        self._plan_cache: dict[str, tuple[dict[str, Any], str]] = {}
        self._published_cache: dict[str, Any] | None = None
        self._worker_state: dict[str, Any] | None = None
        self._emit_lock = threading.Lock()

    @property
    def prepared_checkpoint_path(self) -> Path:
        return self.state_dir / PREPARED_STATE_NAME

    @property
    def worker_state_path(self) -> Path:
        return self.state_dir / WORKER_STATE_NAME

    @property
    def published_checkpoint_path(self) -> Path:
        return self.state_dir / PUBLISHED_STATE_NAME

    def _default_prepared_checkpoint(self) -> dict[str, Any]:
        return {
            "version": 1,
            "role": PREPARED_ROLE,
            "baseline_generation_id": self.config.baseline_generation_id,
            "baseline_source_sha256": self.baseline_binding["sha256"],
            "initial_generation_id": self.initial_generation_id,
            "initial_status": "waiting" if self.initial_generation_id else "not_configured",
            "chain_anchor": {
                "generation_id": self.config.baseline_generation_id,
                "plan_sha256": self.baseline_binding["sha256"],
                "through_event_id": None,
                "source_schema_sha256": None,
                "captured_at": None,
            },
            "queue": [],
            "last_prepared_generation_id": None,
            "last_prepared_plan_sha256": None,
            "active_generation": None,
            "updated_at": _utc_now(),
        }

    def _save_prepared(self, value: dict[str, Any]) -> None:
        value = dict(value)
        value["updated_at"] = _utc_now()
        with self._mutex:
            _atomic_json(self.prepared_checkpoint_path, value)
            self._prepared = value

    def _mutate_prepared(self, mutate: Callable[[dict[str, Any]], None]) -> dict[str, Any]:
        """Atomically edit the shared checkpoint without losing the other thread's updates."""
        with self._mutex:
            if self._prepared is None:
                raise WorkerError("PREPARED_CHECKPOINT_NOT_LOADED")
            value = json.loads(json.dumps(self._prepared))
            mutate(value)
            value["updated_at"] = _utc_now()
            _atomic_json(self.prepared_checkpoint_path, value)
            self._prepared = value
            return json.loads(json.dumps(value))

    def _load_published_checkpoint(self) -> dict[str, Any] | None:
        document = _read_json(self.published_checkpoint_path)
        if document is None:
            return None
        expected = {
            "version", "status", "scope", "generation_id", "plan_sha256",
            "baseline_generation_id", "baseline_source_sha256", "parent_generation_id",
            "latest_generation_id", "index_sha256", "latest_sha256", "through_event_id",
            "last_success", "rclone_compare_and_swap",
        }
        if (set(document) != expected or document.get("version") != 1
                or document.get("status") != "complete"
                or document.get("scope") != "immutable_lossless_delta_generation"
                or not _valid_id(document.get("generation_id"))
                or document.get("latest_generation_id") != document.get("generation_id")
                or not isinstance(document.get("plan_sha256"), str)
                or not SHA256_RE.fullmatch(document["plan_sha256"])
                or not isinstance(document.get("index_sha256"), str)
                or not SHA256_RE.fullmatch(document["index_sha256"])
                or not isinstance(document.get("latest_sha256"), str)
                or not SHA256_RE.fullmatch(document["latest_sha256"])
                or document.get("baseline_generation_id") != self.config.baseline_generation_id
                or document.get("baseline_source_sha256") != self.baseline_binding["sha256"]
                or type(document.get("through_event_id")) is not int
                or document["through_event_id"] < 0
                or document.get("rclone_compare_and_swap") is not False):
            raise WorkerError("PUBLISHED_CHECKPOINT_INVALID")
        _parse_utc(document.get("last_success"))
        plan, plan_sha = self._load_plan(document["generation_id"])
        if (plan_sha != document["plan_sha256"]
                or plan["through_event_id"] != document["through_event_id"]
                or plan["baseline_generation_id"] != document["baseline_generation_id"]
                or plan["baseline_source_sha256"] != document["baseline_source_sha256"]
                or plan["parent_generation_id"] != document["parent_generation_id"]):
            raise WorkerError("PUBLISHED_CHECKPOINT_PLAN_MISMATCH")
        self._published_cache = {**document, "plan": plan}
        return document

    def _load_plan(self, generation_id: str) -> tuple[dict[str, Any], str]:
        if not _valid_id(generation_id):
            raise WorkerError("GENERATION_ID_INVALID")
        cached = self._plan_cache.get(generation_id)
        if cached is not None:
            return cached
        root = self.work_dir / generation_id
        _verify_no_symlink_components(root)
        failed_path = root / "failed.json"
        plan_path = root / "delta-plan.json"
        if _regular_file_exists(failed_path):
            raise WorkerError("GENERATION_HAS_FAILURE_RECEIPT")
        try:
            info = delta_publisher._preflight(root)
        except Exception as exc:
            category = getattr(exc, "category", "PREPARED_PLAN_INVALID")
            raise WorkerError(category) from exc
        plan = info["plan"]
        plan_sha = info["plan_sha256"]
        if (plan.get("generation_id") != generation_id
                or plan.get("baseline_generation_id") != self.config.baseline_generation_id
                or plan.get("baseline_source_sha256") != self.baseline_binding["sha256"]
                or not _regular_file_exists(plan_path)):
            raise WorkerError("PREPARED_PLAN_BINDING_MISMATCH")
        self._plan_cache[generation_id] = (plan, plan_sha)
        return plan, plan_sha

    def _validate_anchor(self, anchor: Any) -> dict[str, Any]:
        if not isinstance(anchor, dict) or set(anchor) != {
            "generation_id", "plan_sha256", "through_event_id", "source_schema_sha256", "captured_at"
        }:
            raise WorkerError("PREPARED_CHECKPOINT_INVALID")
        anchor_id = anchor.get("generation_id")
        if anchor_id == self.config.baseline_generation_id:
            if (anchor.get("plan_sha256") != self.baseline_binding["sha256"]
                    or anchor.get("through_event_id") is not None
                    or anchor.get("source_schema_sha256") is not None
                    or anchor.get("captured_at") is not None):
                raise WorkerError("PREPARED_CHECKPOINT_INVALID")
            return anchor
        if not _valid_id(anchor_id):
            raise WorkerError("PREPARED_CHECKPOINT_INVALID")
        plan, digest = self._load_plan(anchor_id)
        if (digest != anchor.get("plan_sha256")
                or plan.get("through_event_id") != anchor.get("through_event_id")
                or plan.get("source_schema_sha256") != anchor.get("source_schema_sha256")
                or plan.get("captured_at") != anchor.get("captured_at")):
            raise WorkerError("PREPARED_CHECKPOINT_ANCHOR_MISMATCH")
        return anchor

    def _validate_plan_transition(self, prior: dict[str, Any], plan: dict[str, Any]) -> None:
        previous_id = prior["generation_id"]
        if plan.get("expected_previous_generation_id") != previous_id:
            raise WorkerError("PREPARED_CHAIN_GAP")
        if plan.get("replaces_delta_chain") is True:
            if (plan.get("kind") != "baseline_reconciliation"
                    or plan.get("parent_generation_id") != self.config.baseline_generation_id
                    or plan.get("supersedes_generation_id") != (None if previous_id == self.config.baseline_generation_id else previous_id)):
                raise WorkerError("PREPARED_CHAIN_RESET_INVALID")
            return
        if (plan.get("kind") != "change_feed"
                or previous_id == self.config.baseline_generation_id
                or plan.get("parent_generation_id") != previous_id
                or plan.get("supersedes_generation_id") is not None
                or plan.get("after_event_id") != prior.get("through_event_id")
                or plan.get("source_schema_sha256") != prior.get("source_schema_sha256")):
            raise WorkerError("PREPARED_CHAIN_GAP")

    def _load_prepared_checkpoint(self) -> dict[str, Any]:
        with self._checkpoint_init_lock:
            with self._mutex:
                if self._prepared is not None:
                    return self._prepared
            document = _read_json(self.prepared_checkpoint_path)
            if document is None:
                document = self._default_prepared_checkpoint()
                self._save_prepared(document)
                try:
                    self._validate_checkpoint_binding(document)
                except Exception:
                    with self._mutex:
                        self._prepared = None
                    raise
                return document
            expected_keys = {
                "version", "role", "baseline_generation_id", "baseline_source_sha256",
                "initial_generation_id", "initial_status", "chain_anchor", "queue",
                "last_prepared_generation_id", "last_prepared_plan_sha256", "active_generation", "updated_at",
            }
            if (set(document) != expected_keys or document.get("version") != 1
                    or document.get("role") != PREPARED_ROLE
                    or document.get("baseline_generation_id") != self.config.baseline_generation_id
                    or document.get("baseline_source_sha256") != self.baseline_binding["sha256"]
                    or not isinstance(document.get("queue"), list)
                    or not isinstance(document.get("updated_at"), str)):
                raise WorkerError("PREPARED_CHECKPOINT_INVALID")
            stored_initial = document.get("initial_generation_id")
            stored_initial_status = document.get("initial_status")
            if (stored_initial is not None and not _valid_id(stored_initial)):
                raise WorkerError("PREPARED_CHECKPOINT_INVALID")
            if stored_initial_status not in ("waiting", "adopted", "failed", "not_configured"):
                raise WorkerError("PREPARED_CHECKPOINT_INVALID")
            if (stored_initial is None) != (stored_initial_status == "not_configured"):
                raise WorkerError("PREPARED_CHECKPOINT_INVALID")
            if self.initial_generation_id is not None and stored_initial != self.initial_generation_id:
                raise WorkerError("INITIAL_GENERATION_BINDING_MISMATCH")
            if stored_initial is not None and stored_initial_status == "waiting" and self.initial_generation_id is None:
                raise WorkerError("INITIAL_GENERATION_REQUIRED")
            queue_ids: list[str] = []
            for item in document["queue"]:
                if (not isinstance(item, dict) or set(item) != {"generation_id", "plan_sha256"}
                        or not _valid_id(item.get("generation_id"))
                        or not isinstance(item.get("plan_sha256"), str)
                        or not SHA256_RE.fullmatch(item["plan_sha256"])):
                    raise WorkerError("PREPARED_CHECKPOINT_INVALID")
                queue_ids.append(item["generation_id"])
            if len(queue_ids) != len(set(queue_ids)):
                raise WorkerError("PREPARED_CHECKPOINT_DUPLICATE_GENERATION")
            active = document.get("active_generation")
            if active is not None and (
                not isinstance(active, dict)
                or set(active) != {"generation_id", "parent_generation_id", "started_at"}
                or not _valid_id(active.get("generation_id"))
                or not _valid_id(active.get("parent_generation_id"))
                or not isinstance(active.get("started_at"), str)
                or active.get("generation_id") in queue_ids
            ):
                raise WorkerError("PREPARED_CHECKPOINT_INVALID")
            last_id = document.get("last_prepared_generation_id")
            last_sha = document.get("last_prepared_plan_sha256")
            if last_id is None:
                if last_sha is not None or queue_ids:
                    raise WorkerError("PREPARED_CHECKPOINT_INVALID")
            elif (not _valid_id(last_id) or not isinstance(last_sha, str)
                  or not SHA256_RE.fullmatch(last_sha)):
                raise WorkerError("PREPARED_CHECKPOINT_INVALID")
            if queue_ids and last_id != queue_ids[-1]:
                raise WorkerError("PREPARED_CHECKPOINT_TAIL_MISMATCH")
            self._validate_checkpoint_binding(document)
            self._save_prepared(document)
            return document

    def _validate_checkpoint_binding(self, document: dict[str, Any]) -> None:
        published = self._load_published_checkpoint()
        anchor = self._validate_anchor(document.get("chain_anchor"))
        queue = document["queue"]
        plans: list[dict[str, Any]] = []
        for item in queue:
            plan, digest = self._load_plan(item["generation_id"])
            if digest != item["plan_sha256"]:
                raise WorkerError("PREPARED_PLAN_HASH_MISMATCH")
            plans.append(plan)

        # A crash after the publisher's durable success checkpoint but before
        # our prepared checkpoint update leaves the published generation in
        # the local queue. Verify the entire parent chain, then consume only
        # the prefix proven complete by that publisher receipt.
        prior = anchor
        published_position = None
        for position, plan in enumerate(plans):
            self._validate_plan_transition(prior, plan)
            if published is not None and plan["generation_id"] == published["generation_id"]:
                published_position = position
            prior = _capture_anchor(plan, queue[position]["plan_sha256"])
        if published is not None:
            if published_position is not None:
                document["queue"] = queue[published_position + 1:]
                document["chain_anchor"] = _capture_anchor(
                    plans[published_position], queue[published_position]["plan_sha256"]
                )
            elif anchor["generation_id"] != published["generation_id"]:
                raise WorkerError("PUBLISHED_CHECKPOINT_UNEXPECTED")
        elif anchor["generation_id"] != self.config.baseline_generation_id:
            raise WorkerError("PUBLISHED_CHECKPOINT_MISSING")

        last_id = document.get("last_prepared_generation_id")
        last_sha = document.get("last_prepared_plan_sha256")
        if document["queue"]:
            tail = document["queue"][-1]
            if (last_id != tail["generation_id"] or last_sha != tail["plan_sha256"]):
                raise WorkerError("PREPARED_CHECKPOINT_TAIL_MISMATCH")
        else:
            new_anchor = document["chain_anchor"]
            if ((last_id is None) != (new_anchor["generation_id"] == self.config.baseline_generation_id)
                    or (last_id is not None and (last_id != new_anchor["generation_id"]
                                                 or last_sha != new_anchor["plan_sha256"]))):
                raise WorkerError("PREPARED_CHECKPOINT_TAIL_MISMATCH")
        # Persist crash reconciliation before either thread can use the queue.
        self._save_prepared(document)

    def _load_worker_state(self) -> dict[str, Any]:
        if self._worker_state is not None:
            return self._worker_state
        document = _read_json(self.worker_state_path)
        if document is None:
            document = {
                "version": 1,
                "role": WORKER_ROLE,
                "baseline_generation_id": self.config.baseline_generation_id,
                "baseline_source_sha256": self.baseline_binding["sha256"],
                "prepare": {"last_success": None, "last_failure": None},
                "publish": {"last_success": None, "last_failure": None},
                "updated_at": _utc_now(),
            }
        elif (document.get("version") != 1 or document.get("role") != WORKER_ROLE
              or document.get("baseline_generation_id") != self.config.baseline_generation_id
              or document.get("baseline_source_sha256") != self.baseline_binding["sha256"]
              or not isinstance(document.get("prepare"), dict)
              or not isinstance(document.get("publish"), dict)):
            raise WorkerError("WORKER_STATE_INVALID")
        self._worker_state = document
        return document

    def _record_worker_state(self, section: str, update: dict[str, Any]) -> dict[str, Any]:
        with self._mutex:
            state = dict(self._load_worker_state())
            old_section = dict(state.get(section, {}))
            old_section.update(update)
            state[section] = old_section
            state["updated_at"] = _utc_now()
            checkpoint = self._prepared if self._prepared is not None else None
            if checkpoint is not None:
                state["prepared_queue_length"] = len(checkpoint["queue"])
                state["prepared_tail_generation_id"] = checkpoint["last_prepared_generation_id"]
                state["published_anchor_generation_id"] = checkpoint["chain_anchor"]["generation_id"]
            state["published_lag_seconds"] = self._published_lag_seconds()
            state["queued_capture_lag_seconds"] = self._queued_capture_lag_seconds()
            _atomic_json(self.worker_state_path, state)
            self._worker_state = state
            return state

    def _published_lag_seconds(self) -> float | None:
        """Age of the latest fully published source capture, in seconds."""
        try:
            published = self._published_cache
            if published is None:
                _ = self._load_published_checkpoint()
                published = self._published_cache
            if published is None:
                return None
            delta = (
                _parse_utc(_utc_now())
                - _parse_utc(published["plan"]["captured_at"])
            ).total_seconds()
            return max(0.0, delta) if math.isfinite(delta) else None
        except WorkerError:
            return None

    def _queued_capture_lag_seconds(self) -> float | None:
        """Capture-time coverage gap between newest prepared and published plans."""
        try:
            prepared = self._prepared
            if not prepared:
                return None
            newest_id = prepared.get("last_prepared_generation_id")
            if newest_id is None:
                return None
            published = self._published_cache
            if published is None:
                _ = self._load_published_checkpoint()
                published = self._published_cache
            if published is None:
                return None
            newest_plan, _ = self._load_plan(newest_id)
            delta = (
                _parse_utc(newest_plan["captured_at"])
                - _parse_utc(published["plan"]["captured_at"])
            ).total_seconds()
            return max(0.0, delta) if math.isfinite(delta) else None
        except WorkerError:
            return None

    def _emit(self, result: dict[str, Any]) -> None:
        safe = {key: value for key, value in result.items() if key not in {"plan", "exception"}}
        with self._emit_lock:
            print(json.dumps(safe, ensure_ascii=False, sort_keys=True), flush=True)

    def _checkpoint_copy(self) -> dict[str, Any]:
        checkpoint = self._load_prepared_checkpoint()
        return json.loads(json.dumps(checkpoint))

    def _active_recovery(self, checkpoint: dict[str, Any]) -> dict[str, Any] | None:
        active = checkpoint.get("active_generation")
        if active is None:
            return None
        generation_id = active["generation_id"]
        root = self.work_dir / generation_id
        _verify_no_symlink_components(root)
        if root.exists():
            _validate_generation_directory(root)
        failure = root / "failed.json"
        plan_path = root / "delta-plan.json"
        if _regular_file_exists(failure):
            try:
                failure_doc = json.loads(_read_regular_bytes(failure).decode("utf-8"))
                category = failure_doc.get("error_code") if isinstance(failure_doc, dict) else None
            except Exception:
                category = None
            def clear_failed(current):
                active_now = current.get("active_generation")
                if isinstance(active_now, dict) and active_now.get("generation_id") == generation_id:
                    current["active_generation"] = None
            self._mutate_prepared(clear_failed)
            return {"status": "failed", "category": category or "PREPARATION_FAILED",
                    "generation_id": generation_id, "retained_partial": True}
        if not _regular_file_exists(plan_path):
            # The failed/partial attempt is preserved but never checkpointed.
            def clear_interrupted(current):
                active_now = current.get("active_generation")
                if isinstance(active_now, dict) and active_now.get("generation_id") == generation_id:
                    current["active_generation"] = None
            self._mutate_prepared(clear_interrupted)
            return {"status": "failed", "category": "PREPARATION_INTERRUPTED",
                    "generation_id": generation_id, "retained_partial": root.exists()}
        try:
            plan, plan_sha = self._load_plan(generation_id)
            previous_tail_id = checkpoint["last_prepared_generation_id"]
            previous_id = previous_tail_id or self.config.baseline_generation_id
            if plan.get("expected_previous_generation_id") != previous_id:
                raise WorkerError("PREPARED_CHAIN_GAP")
            if checkpoint["last_prepared_generation_id"] is not None:
                tail, tail_sha = self._load_plan(checkpoint["last_prepared_generation_id"])
                prior = _capture_anchor(tail, tail_sha)
            else:
                prior = checkpoint["chain_anchor"]
            self._validate_plan_transition(prior, plan)
            def adopt(current):
                active_now = current.get("active_generation")
                if (not isinstance(active_now, dict)
                        or active_now.get("generation_id") != generation_id
                        or current.get("last_prepared_generation_id") != previous_tail_id):
                    raise WorkerError("ACTIVE_PREPARATION_CHECKPOINT_MISMATCH")
                current["queue"].append({"generation_id": generation_id, "plan_sha256": plan_sha})
                current["last_prepared_generation_id"] = generation_id
                current["last_prepared_plan_sha256"] = plan_sha
                current["active_generation"] = None
            self._mutate_prepared(adopt)
            return {"status": "prepared_recovered", "generation_id": generation_id,
                    "kind": plan["kind"], "through_event_id": plan["through_event_id"]}
        except WorkerError as exc:
            def clear_invalid(current):
                active_now = current.get("active_generation")
                if isinstance(active_now, dict) and active_now.get("generation_id") == generation_id:
                    current["active_generation"] = None
            self._mutate_prepared(clear_invalid)
            return {"status": "failed", "category": exc.category,
                    "generation_id": generation_id, "retained_partial": True}

    def _manual_initial_cycle(self, checkpoint: dict[str, Any]) -> dict[str, Any] | None:
        if checkpoint["initial_status"] in ("not_configured", "adopted"):
            return None
        if checkpoint["initial_status"] == "failed":
            return {"status": "failed", "category": "INITIAL_RECONCILIATION_FAILED",
                    "generation_id": checkpoint["initial_generation_id"], "retry_pending": False}
        generation_id = checkpoint["initial_generation_id"]
        if generation_id != self.initial_generation_id:
            raise WorkerError("INITIAL_GENERATION_BINDING_MISMATCH")
        root = self.work_dir / generation_id
        try:
            root_info = root.lstat()
        except FileNotFoundError:
            return {"status": "waiting_initial_reconciliation", "generation_id": generation_id,
                    "retry_pending": True}
        if (not stat.S_ISDIR(root_info.st_mode) or root_info.st_uid != os.getuid()
                or stat.S_IMODE(root_info.st_mode) != 0o700):
            raise WorkerError("INITIAL_GENERATION_PATH_INVALID")
        failure = root / "failed.json"
        plan_path = root / "delta-plan.json"
        if _regular_file_exists(failure):
            checkpoint["initial_status"] = "failed"
            self._save_prepared(checkpoint)
            try:
                failure_doc = json.loads(_read_regular_bytes(failure).decode("utf-8"))
                category = failure_doc.get("error_code") if isinstance(failure_doc, dict) else None
            except Exception:
                category = None
            return {"status": "failed", "category": category or "INITIAL_RECONCILIATION_FAILED",
                    "generation_id": generation_id, "retry_pending": False}
        if not _regular_file_exists(plan_path):
            return {"status": "waiting_initial_reconciliation", "generation_id": generation_id,
                    "retry_pending": True}
        try:
            plan, plan_sha = self._load_plan(generation_id)
            if (plan.get("kind") != "baseline_reconciliation"
                    or plan.get("parent_generation_id") != self.config.baseline_generation_id
                    or plan.get("expected_previous_generation_id") != self.config.baseline_generation_id
                    or plan.get("replaces_delta_chain") is not True
                    or plan.get("supersedes_generation_id") is not None):
                raise WorkerError("INITIAL_RECONCILIATION_BINDING_MISMATCH")
            try:
                probe = self.probe_fn(self.db)
            except WorkerError as exc:
                # The immutable initial generation has already passed the full
                # plan/proof preflight above. A transient inability to open the
                # live source (for example while SQLite is resolving WAL
                # sidecars) must not turn that verified generation into a
                # sticky failure. Keep the checkpoint at waiting and retry the
                # source probe on the next prepare cycle.
                if exc.category == "SOURCE_PROBE_FAILED":
                    return {"status": "failed", "category": exc.category,
                            "generation_id": generation_id, "retry_pending": True}
                raise
            # A pinned manual full reconciliation can legitimately predate the
            # current source schema (for example, writer guards installed while
            # it was running). Its complete file/hash/value proof binds the
            # generation to its own captured source snapshot; the next normal
            # prepare cycle compares that schema and reconciles any difference.
            if probe.get("through_event_id", -1) < plan["through_event_id"]:
                raise WorkerError("INITIAL_RECONCILIATION_AHEAD_OF_SOURCE")
            def adopt(current):
                if (current.get("initial_generation_id") != generation_id
                        or current.get("initial_status") != "waiting"
                        or current["queue"] or current.get("last_prepared_generation_id") is not None):
                    raise WorkerError("INITIAL_GENERATION_CHECKPOINT_MISMATCH")
                current["queue"].append({"generation_id": generation_id, "plan_sha256": plan_sha})
                current["last_prepared_generation_id"] = generation_id
                current["last_prepared_plan_sha256"] = plan_sha
                current["initial_status"] = "adopted"
            self._mutate_prepared(adopt)
            return {"status": "prepared_adopted", "generation_id": generation_id,
                    "kind": plan["kind"], "through_event_id": plan["through_event_id"]}
        except WorkerError as exc:
            self._mutate_prepared(lambda current: current.update({"initial_status": "failed"})
                                  if current.get("initial_generation_id") == generation_id else None)
            return {"status": "failed", "category": exc.category,
                    "generation_id": generation_id, "retry_pending": False}

    def _new_generation_id(self) -> str:
        for _ in range(20):
            value = self.generation_id_fn()
            if not _valid_id(value):
                raise WorkerError("GENERATION_ID_INVALID")
            path = self.work_dir / value
            _verify_no_symlink_components(path)
            if not path.exists():
                return value
        raise WorkerError("GENERATION_ID_COLLISION")

    def prepare_cycle(self) -> dict[str, Any]:
        started = _utc_now()
        try:
            checkpoint = self._checkpoint_copy()
            recovered = self._active_recovery(checkpoint)
            if recovered is not None:
                result = recovered
            else:
                manual = self._manual_initial_cycle(checkpoint)
                if manual is not None:
                    result = manual
                else:
                    probe = self.probe_fn(self.db)
                    last_id = checkpoint["last_prepared_generation_id"]
                    previous = None
                    if last_id is not None:
                        previous, previous_sha = self._load_plan(last_id)
                        if previous_sha != checkpoint["last_prepared_plan_sha256"]:
                            raise WorkerError("PREPARED_CHECKPOINT_TAIL_MISMATCH")
                    if previous is not None:
                        previous_meta = previous.get("metadata", {})
                        guard = probe.get("feed_integrity_guard_status", {})
                        writer_guard = probe.get("writer_guard_status", {})
                        previous_writer_guard = previous_meta.get("writer_guard_status", {})
                        idle = (
                            previous_meta.get("requires_baseline_reconciliation") is False
                            and previous_meta.get("feed_integrity_guard_status", {}).get("status") == "verified"
                            and previous_meta.get("all_writers_contract_enforced") is True
                            and previous_writer_guard.get("status") == "verified"
                            and guard.get("status") == "verified"
                            and probe.get("all_writers_contract_enforced") is True
                            and writer_guard.get("status") == "verified"
                            and probe.get("through_event_id") == previous.get("through_event_id")
                            and probe.get("source_schema_sha256") == previous.get("source_schema_sha256")
                        )
                        if idle:
                            result = {"status": "idle", "generation_id": last_id,
                                      "through_event_id": previous["through_event_id"],
                                      "observed_at": probe["observed_at"], "retry_pending": False}
                        else:
                            result = self._prepare_new_generation(checkpoint, previous, probe)
                    else:
                        result = self._prepare_new_generation(checkpoint, None, probe)
            result.setdefault("started_at", started)
            self._record_worker_state("prepare", {
                "status": result["status"],
                "last_attempt": started,
                "retry_pending": result.get("retry_pending", result["status"] in {
                    "waiting_initial_reconciliation", "failed"
                }),
                "failure_category": result.get("category"),
                **({"last_success": _utc_now(), "last_success_generation_id": result["generation_id"]}
                   if result["status"] in ("prepared", "prepared_recovered", "prepared_adopted") else {}),
            })
            return result
        except WorkerError as exc:
            result = {"status": "failed", "category": exc.category, "retry_pending": True}
            self._record_worker_state("prepare", {
                "status": "failed", "last_attempt": started, "retry_pending": True,
                "failure_category": exc.category,
            })
            return result
        except Exception as exc:
            category = _exception_category(exc, "PREPARE_FAILED")
            result = {"status": "failed", "category": category, "retry_pending": True}
            self._record_worker_state("prepare", {
                "status": "failed", "last_attempt": started, "retry_pending": True,
                "failure_category": category,
            })
            return result

    def _prepare_new_generation(
        self, checkpoint: dict[str, Any], previous: dict[str, Any] | None, probe: dict[str, Any]
    ) -> dict[str, Any]:
        generation_id = self._new_generation_id()
        parent_id = previous["generation_id"] if previous else self.config.baseline_generation_id
        previous_id = previous["generation_id"] if previous else None
        def begin(current):
            current_tail = current.get("last_prepared_generation_id")
            if current_tail != previous_id or current.get("active_generation") is not None:
                raise WorkerError("PREPARED_CHECKPOINT_CHANGED")
            current["active_generation"] = {
                "generation_id": generation_id,
                "parent_generation_id": parent_id,
                "started_at": _utc_now(),
            }
        self._mutate_prepared(begin)  # Durable before output creation begins.
        root = self.work_dir / generation_id
        ready_to_recover = False
        try:
            prepare_kwargs: dict[str, Any] = {
                "previous": previous,
                "max_part_bytes": self.max_part_bytes,
            }
            if self._baseline_token_supported:
                if self._baseline_verification is None:
                    self._baseline_verification = self._baseline_verifier(
                        self.baseline, self.baseline_manifest
                    )
                    if self._baseline_verification is None:
                        raise WorkerError("BASELINE_VERIFICATION_TOKEN_INVALID")
                prepare_kwargs["baseline_verification"] = self._baseline_verification
            prepared = self.prepare_fn(
                self.db, self.baseline, self.baseline_manifest, root, generation_id,
                self.config.baseline_generation_id, **prepare_kwargs,
            )
            if not isinstance(prepared, dict):
                raise WorkerError("PREPARE_RESULT_INVALID")
            plan, plan_sha = self._load_plan(generation_id)
            if (prepared.get("generation_id") != generation_id
                    or prepared.get("status") != "prepared"
                    or plan.get("expected_previous_generation_id") != parent_id):
                raise WorkerError("PREPARED_PLAN_BINDING_MISMATCH")
            ready_to_recover = True
            def finish(current):
                active_now = current.get("active_generation")
                if (not isinstance(active_now, dict)
                        or active_now.get("generation_id") != generation_id
                        or current.get("last_prepared_generation_id") != previous_id):
                    raise WorkerError("ACTIVE_PREPARATION_CHECKPOINT_MISMATCH")
                if previous is None:
                    prior = current["chain_anchor"]
                else:
                    prior_sha = current.get("last_prepared_plan_sha256")
                    prior = _capture_anchor(previous, prior_sha)
                self._validate_plan_transition(prior, plan)
                current["queue"].append({"generation_id": generation_id, "plan_sha256": plan_sha})
                current["last_prepared_generation_id"] = generation_id
                current["last_prepared_plan_sha256"] = plan_sha
                current["active_generation"] = None
            current = self._mutate_prepared(finish)
            return {
                "status": "prepared", "generation_id": generation_id, "kind": plan["kind"],
                "through_event_id": plan["through_event_id"], "captured_at": plan["captured_at"],
                "queue_length": len(current["queue"]), "observed_at": probe.get("observed_at"),
                "retry_pending": False,
            }
        except Exception as exc:
            category = _exception_category(exc, "PREPARE_FAILED")
            if not ready_to_recover:
                def clear(current):
                    active_now = current.get("active_generation")
                    if isinstance(active_now, dict) and active_now.get("generation_id") == generation_id:
                        current["active_generation"] = None
                try:
                    self._mutate_prepared(clear)
                except WorkerError:
                    pass
            return {"status": "failed", "category": category, "generation_id": generation_id,
                    "retained_partial": root.exists(), "retry_pending": True}

    def _publish_candidate(self) -> tuple[dict[str, Any], dict[str, Any], str] | None:
        checkpoint = self._checkpoint_copy()
        if not checkpoint["queue"]:
            return None
        item = checkpoint["queue"][0]
        plan, digest = self._load_plan(item["generation_id"])
        if digest != item["plan_sha256"]:
            raise WorkerError("PREPARED_PLAN_HASH_MISMATCH")
        return checkpoint, plan, digest

    def _complete_published_checkpoint(
        self, result: dict[str, Any], generation_id: str, plan: dict[str, Any], plan_sha: str
    ) -> dict[str, Any]:
        checkpoint = self._load_published_checkpoint()
        if (result.get("status") != "complete" or result.get("checkpoint_advanced") is not True
                or checkpoint is None or checkpoint.get("generation_id") != generation_id
                or checkpoint.get("plan_sha256") != plan_sha
                or checkpoint.get("through_event_id") != plan["through_event_id"]
                or checkpoint.get("parent_generation_id") != plan["parent_generation_id"]
                or checkpoint.get("baseline_source_sha256") != plan["baseline_source_sha256"]):
            raise WorkerError("PUBLISHED_CHECKPOINT_NOT_VERIFIED")
        returned = result.get("checkpoint")
        if returned is not None and returned != {key: value for key, value in checkpoint.items() if key != "plan"}:
            raise WorkerError("PUBLISHED_RESULT_CHECKPOINT_MISMATCH")
        return checkpoint

    def publish_cycle(self) -> dict[str, Any]:
        started = _utc_now()
        try:
            self._checkpoint_copy()
            candidate = self._publish_candidate()
            if candidate is None:
                result = {"status": "idle", "retry_pending": False}
            else:
                checkpoint, plan, plan_sha = candidate
                generation_id = plan["generation_id"]
                result = self.publish_fn(
                    self.work_dir / generation_id,
                    self.remote,
                    self.state_dir,
                    rclone_bin=self.rclone_bin,
                )
                if not isinstance(result, dict):
                    raise WorkerError("PUBLISH_RESULT_INVALID")
                if result.get("status") == "pending_baseline_publication":
                    result = {**result, "retry_pending": True}
                elif result.get("status") == "complete":
                    published = self._complete_published_checkpoint(result, generation_id, plan, plan_sha)
                    def mark_published(current):
                        if (not current["queue"]
                                or current["queue"][0]["generation_id"] != generation_id):
                            raise WorkerError("PREPARED_QUEUE_CHANGED")
                        current["queue"] = current["queue"][1:]
                        current["chain_anchor"] = _capture_anchor(plan, plan_sha)
                        if not current["queue"]:
                            current["last_prepared_generation_id"] = generation_id
                            current["last_prepared_plan_sha256"] = plan_sha
                    checkpoint = self._mutate_prepared(mark_published)
                    result = {**result, "retry_pending": bool(checkpoint["queue"]),
                              "published_generation_id": published["generation_id"]}
                elif result.get("status") in ("pending", "pending_publish_lock"):
                    result = {**result, "retry_pending": True}
                else:
                    result = {**result, "status": "failed",
                              "category": result.get("category", "PUBLISH_NOT_COMPLETE"),
                              "retry_pending": True}
            self._record_worker_state("publish", {
                "status": result["status"], "last_attempt": started,
                "retry_pending": result.get("retry_pending", result["status"] in ("failed", "pending_baseline_publication", "pending_publish_lock")),
                "failure_category": result.get("category"),
                **({"last_success": _utc_now(), "last_success_generation_id": result["published_generation_id"]}
                   if result.get("status") == "complete" else {}),
            })
            return result
        except WorkerError as exc:
            self._record_worker_state("publish", {
                "status": "failed", "last_attempt": started, "retry_pending": True,
                "failure_category": exc.category,
            })
            return {"status": "failed", "category": exc.category, "retry_pending": True}
        except Exception as exc:
            category = _exception_category(exc, "PUBLISH_FAILED")
            status = "pending_publish_lock" if category in ("LOCK_BUSY", "PUBLISH_LOCK_BUSY") else "failed"
            self._record_worker_state("publish", {
                "status": status, "last_attempt": started, "retry_pending": True,
                "failure_category": category,
            })
            return {"status": status, "category": category, "retry_pending": True}

    def run_once(self) -> dict[str, Any]:
        try:
            with _worker_lock(self.state_dir):
                prepared = self.prepare_cycle()
                published = self.publish_cycle()
        except WorkerError as exc:
            return {"status": "failed", "category": exc.category}
        return {
            "status": "cycle_complete",
            "preparation": prepared,
            "publication": published,
            "retry_pending": bool(prepared.get("retry_pending") or published.get("retry_pending")),
            "published_lag_seconds": self._published_lag_seconds(),
            "queued_capture_lag_seconds": self._queued_capture_lag_seconds(),
        }

    def _loop(self, cycle: Callable[[], dict[str, Any]], stop_event: threading.Event) -> None:
        while not stop_event.is_set():
            try:
                result = cycle()
            except Exception as exc:
                result = {"status": "failed", "category": getattr(exc, "category", "WORKER_INTERNAL_ERROR"),
                          "retry_pending": True}
            result = {
                **result,
                "published_lag_seconds": self._published_lag_seconds(),
                "queued_capture_lag_seconds": self._queued_capture_lag_seconds(),
            }
            self._emit(result)
            if not stop_event.is_set():
                self.wait_fn(stop_event, float(self.interval_seconds))

    def run_watch(self, stop_event: threading.Event | None = None) -> int:
        stop_event = stop_event or threading.Event()
        try:
            with _worker_lock(self.state_dir):
                prepare_thread = threading.Thread(
                    target=self._loop, args=(self.prepare_cycle, stop_event),
                    name="delta-prepare-loop", daemon=False,
                )
                publish_thread = threading.Thread(
                    target=self._loop, args=(self.publish_cycle, stop_event),
                    name="delta-publish-loop", daemon=False,
                )
                threads: list[threading.Thread] = []
                try:
                    prepare_thread.start()
                    threads.append(prepare_thread)
                    publish_thread.start()
                    threads.append(publish_thread)
                    while any(thread.is_alive() for thread in threads):
                        if stop_event.is_set():
                            break
                        prepare_thread.join(timeout=0.25)
                        publish_thread.join(timeout=0.25)
                finally:
                    stop_event.set()
                    for thread in threads:
                        thread.join()
        except KeyboardInterrupt:
            stop_event.set()
            return 130
        except WorkerError as exc:
            self._emit({"status": "failed", "category": exc.category, "retry_pending": True})
            return 1
        return 0

    def status(self) -> dict[str, Any]:
        checkpoint = self._checkpoint_copy()
        self._validate_checkpoint_binding(checkpoint)
        return {
            "role": WORKER_ROLE,
            "baseline_generation_id": self.config.baseline_generation_id,
            "last_prepared_generation_id": checkpoint["last_prepared_generation_id"],
            "published_generation_id": checkpoint["chain_anchor"]["generation_id"],
            "queue": [row["generation_id"] for row in checkpoint["queue"]],
            "active_generation": checkpoint["active_generation"],
            "published_lag_seconds": self._published_lag_seconds(),
            "queued_capture_lag_seconds": self._queued_capture_lag_seconds(),
        }


def _config_from_args(args: argparse.Namespace) -> WorkerConfig:
    return WorkerConfig(
        db=args.db,
        baseline=args.baseline,
        baseline_manifest=args.baseline_manifest,
        baseline_generation_id=args.baseline_generation_id,
        work_dir=args.work_dir,
        state_dir=args.state_dir,
        remote=args.remote,
        rclone_bin=args.rclone_bin,
        initial_generation_id=args.initial_generation_id,
        interval_seconds=args.interval,
        max_part_bytes=args.max_part_bytes,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, required=True, help="read-only NAS source SQLite database")
    parser.add_argument("--baseline", type=Path, required=True, help="immutable raw baseline SQLite database")
    parser.add_argument("--baseline-manifest", type=Path, required=True)
    parser.add_argument("--baseline-generation-id", required=True)
    parser.add_argument("--work-dir", type=Path, required=True)
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument("--remote", required=True)
    parser.add_argument("--rclone-bin", default="rclone")
    parser.add_argument("--initial-generation-id", help="adopt an externally running initial reconciliation after proof")
    parser.add_argument("--interval", type=int, default=30, help="watch interval in seconds (5..60; default 30)")
    parser.add_argument("--max-part-bytes", type=int, default=20 * 1024 * 1024)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--once", action="store_true", help="one preparation cycle and one publication cycle")
    mode.add_argument("--watch", action="store_true", help="run independent preparation and publication loops")
    args = parser.parse_args(argv)
    try:
        worker = DeltaWorker(_config_from_args(args))
        if args.watch:
            stop_event = threading.Event()
            termination_event = threading.Event()
            interrupt_event = threading.Event()

            def request_stop(signum, _frame):
                # Signal handlers only request shutdown. The worker threads
                # finish their current cycle and persist their normal receipts.
                stop_event.set()
                if signum == signal.SIGTERM:
                    termination_event.set()
                elif signum == signal.SIGINT:
                    interrupt_event.set()

            previous_handlers = {}
            try:
                for signum in (signal.SIGTERM, signal.SIGINT):
                    previous_handlers[signum] = signal.signal(signum, request_stop)
                result = worker.run_watch(stop_event)
            finally:
                for signum, previous in reversed(tuple(previous_handlers.items())):
                    signal.signal(signum, previous)
            if interrupt_event.is_set():
                return 130
            if termination_event.is_set():
                return 0
            return result
        result = worker.run_once()
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
        return 0 if result["status"] == "cycle_complete" else 1
    except WorkerError as exc:
        print(json.dumps({"status": "failed", "category": exc.category}), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
