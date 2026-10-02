#!/usr/bin/env python3
"""Publish one already-captured, fully verified immutable baseline.

This worker never captures a database. It consumes an existing plaintext
snapshot and its manifest, and only prepares/publishes a generation after the
slice, selector, and XLSX verification artifacts are already present.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import stat
import sys
import tempfile
import time
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
for _entry in (str(ROOT / "scripts"), str(ROOT / "src/python")):
    if _entry not in sys.path:
        sys.path.insert(0, _entry)

import nas_full_data_publish as publisher  # noqa: E402
import prepare_full_data_generation as preparer  # noqa: E402


class BaselineError(Exception):
    def __init__(self, category: str, *, pending: bool = False):
        super().__init__(category)
        self.category = category
        self.pending = pending


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _safe_path(value: str | os.PathLike[str]) -> Path:
    path = Path(value).expanduser()
    publisher.verify_no_symlink_components(path)
    return path.absolute()


def _path_within(path: Path, directory: Path) -> bool:
    try:
        path.relative_to(directory)
        return True
    except ValueError:
        return False


def _read_json(path: Path, *, missing_pending: bool = True) -> dict[str, Any] | None:
    publisher.verify_no_symlink_components(path)
    try:
        info = path.lstat()
    except FileNotFoundError:
        if missing_pending:
            return None
        raise BaselineError("SOURCE_MANIFEST_MISSING")
    except OSError as exc:
        raise BaselineError("PATH_SAFETY_ERROR") from exc
    if not stat.S_ISREG(info.st_mode) or info.st_size > publisher.MAX_CONTROL_BYTES:
        raise BaselineError("ARTIFACT_INVALID")
    try:
        data = path.read_bytes()
        value = publisher._json_object(data)
    except Exception as exc:
        raise BaselineError("ARTIFACT_INVALID") from exc
    return value


def _pending(category: str = "ARTIFACTS_PENDING") -> None:
    raise BaselineError(category, pending=True)


def _bound(doc: dict[str, Any], generation_id: str, source_sha: str) -> bool:
    return (
        doc.get("snapshot_identifier", doc.get("generation_id")) == generation_id
        and doc.get("source_sha256", doc.get("snapshot_sha256")) == source_sha
    )


def _source_identity(snapshot: Path, manifest_path: Path) -> tuple[dict[str, Any], str]:
    manifest = _read_json(manifest_path, missing_pending=False)
    assert manifest is not None
    raw = manifest.get("raw_snapshot")
    verification = manifest.get("verification")
    if (
        manifest.get("storage") != "plaintext"
        or manifest.get("encryption") is not None
        or not isinstance(raw, dict)
        or raw.get("basename") != snapshot.name
        or raw.get("quick_check") != "ok"
        or type(raw.get("bytes")) is not int
        or raw["bytes"] <= 0
        or not publisher.valid_sha(raw.get("sha256"))
        or not isinstance(verification, dict)
        or verification.get("sha256_match") is not True
        or verification.get("quick_check") != "ok"
    ):
        raise BaselineError("SOURCE_MANIFEST_INVALID")
    for suffix in ("-wal", "-journal", "-shm"):
        sidecar = Path(str(snapshot) + suffix)
        publisher.verify_no_symlink_components(sidecar)
        try:
            sidecar_info = sidecar.lstat()
        except FileNotFoundError:
            continue
        except OSError as exc:
            raise BaselineError("SOURCE_SNAPSHOT_INVALID") from exc
        if not stat.S_ISREG(sidecar_info.st_mode):
            raise BaselineError("SOURCE_SNAPSHOT_INVALID")
        if sidecar_info.st_size:
            raise BaselineError("SOURCE_SNAPSHOT_NOT_STATIC")
    return {"bytes": raw["bytes"], "sha256": raw["sha256"]}, hashlib.sha256(
        manifest_path.read_bytes()
    ).hexdigest()


def _preflight_artifacts(
    generation_root: Path,
    generation_id: str,
    source: dict[str, Any],
) -> dict[str, Any]:
    """Read-only readiness check; pending artifacts never reach prepare_generation."""
    controls = {
        name: _read_json(generation_root / name)
        for name in (
            "slices/manifest.json",
            "slices/verification.json",
            "slices/selectors-verification.json",
            "xlsx/manifest.json",
            "xlsx/index.json",
            "xlsx/verification.json",
        )
    }
    if any(value is None for value in controls.values()):
        _pending()
    slices_manifest = controls["slices/manifest.json"]
    slices_verification = controls["slices/verification.json"]
    selectors_verification = controls["slices/selectors-verification.json"]
    xlsx_manifest = controls["xlsx/manifest.json"]
    xlsx_index = controls["xlsx/index.json"]
    xlsx_verification = controls["xlsx/verification.json"]
    assert all(isinstance(item, dict) for item in controls.values())

    assert slices_manifest is not None
    if (
        slices_manifest.get("version") != 2
        or slices_manifest.get("role") != "lossless_sqlite_shards"
        or slices_manifest.get("snapshot_identifier") != generation_id
        or slices_manifest.get("source_sha256") != source["sha256"]
    ):
        raise BaselineError("ARTIFACT_BINDING_MISMATCH")

    assert slices_verification is not None
    if slices_verification.get("status") != "verified":
        _pending("SLICE_VERIFICATION_PENDING")
    if not _bound(slices_verification, generation_id, source["sha256"]):
        raise BaselineError("ARTIFACT_BINDING_MISMATCH")
    required_coverage = (
        "all_tables", "all_rows", "all_columns", "all_values", "external_values"
    )
    coverage = slices_verification.get("coverage")
    if not isinstance(coverage, dict) or any(coverage.get(key) is not True for key in required_coverage):
        _pending("SLICE_COVERAGE_PENDING")
    source_schema_sha = slices_verification.get("source_schema_sha256")
    if (
        not publisher.valid_sha(source_schema_sha)
        or slices_manifest.get("source_schema_sha256") != source_schema_sha
    ):
        raise BaselineError("ARTIFACT_BINDING_MISMATCH")

    assert selectors_verification is not None
    if selectors_verification.get("status") != "verified":
        _pending("SELECTOR_VERIFICATION_PENDING")
    if not _bound(selectors_verification, generation_id, source["sha256"]):
        raise BaselineError("ARTIFACT_BINDING_MISMATCH")
    for key in ("all_shared_files_reachable", "all_mode_matches", "all_rule_matches"):
        if selectors_verification.get(key) is not True:
            _pending("SELECTOR_COVERAGE_PENDING")

    assert xlsx_manifest is not None
    if xlsx_manifest.get("status") != "verified":
        _pending("XLSX_MANIFEST_PENDING")
    if not _bound(xlsx_manifest, generation_id, source["sha256"]):
        raise BaselineError("ARTIFACT_BINDING_MISMATCH")
    manifest_source = xlsx_manifest.get("source")
    expected_source_path = str(generation_root / "source.sqlite3")
    if (
        not isinstance(manifest_source, dict)
        or manifest_source.get("path") != expected_source_path
        or manifest_source.get("bytes") != source["bytes"]
        or manifest_source.get("sha256") != source["sha256"]
    ):
        raise BaselineError("ARTIFACT_BINDING_MISMATCH")

    assert xlsx_index is not None
    if xlsx_index.get("status") != "verified" or xlsx_index.get("verification_status") != "verified":
        _pending("XLSX_INDEX_PENDING")
    if (
        xlsx_index.get("row_identity_format") != "source_rowid_column_v1"
        or not _bound(xlsx_index, generation_id, source["sha256"])
    ):
        if xlsx_index.get("row_identity_format") != "source_rowid_column_v1":
            _pending("ROWID_PROOF_PENDING")
        raise BaselineError("ARTIFACT_BINDING_MISMATCH")
    index_source = xlsx_index.get("source")
    if (
        not isinstance(index_source, dict)
        or index_source.get("path") != expected_source_path
        or index_source.get("bytes") != source["bytes"]
        or index_source.get("sha256") != source["sha256"]
    ):
        raise BaselineError("ARTIFACT_BINDING_MISMATCH")
    xlsx_counts = xlsx_index.get("counts")
    if not isinstance(xlsx_counts, dict) or any(
        type(xlsx_counts.get(key)) is not int or xlsx_counts[key] < 0
        for key in ("exported_tables", "exported_rows", "exported_cells", "chunks", "pieces")
    ):
        _pending("XLSX_COVERAGE_PENDING")
    xlsx_pieces = xlsx_index.get("pieces")
    if not isinstance(xlsx_pieces, list) or len(xlsx_pieces) != xlsx_counts["pieces"]:
        _pending("XLSX_COVERAGE_PENDING")
    xlsx_coverage = xlsx_index.get("coverage")
    if xlsx_coverage is not None and (
        not isinstance(xlsx_coverage, dict)
        or any(value is not True for value in xlsx_coverage.values())
    ):
        _pending("XLSX_COVERAGE_PENDING")

    assert xlsx_verification is not None
    if xlsx_verification.get("status") != "verified":
        _pending("XLSX_VERIFICATION_PENDING")
    if xlsx_verification.get("source_rowids_verified") is not True:
        _pending("ROWID_PROOF_PENDING")
    if not _bound(xlsx_verification, generation_id, source["sha256"]):
        raise BaselineError("ARTIFACT_BINDING_MISMATCH")
    xlsx_source = xlsx_verification.get("source")
    if (
        not isinstance(xlsx_source, dict)
        or xlsx_source.get("bytes") != source["bytes"]
        or xlsx_source.get("sha256") != source["sha256"]
    ):
        raise BaselineError("ARTIFACT_BINDING_MISMATCH")

    return {
        "source_schema_sha256": slices_verification.get("source_schema_sha256"),
        "counts": {
            "tables": slices_verification.get("table_count"),
            "rows": slices_verification.get("row_counts"),
            "xlsx": xlsx_counts,
        },
    }


def _ensure_private_state_dir(path: Path) -> Path:
    path = _safe_path(path)
    existed = path.exists()
    if not existed:
        try:
            path.mkdir(parents=True, mode=0o700, exist_ok=False)
        except FileExistsError:
            existed = True
        except OSError as exc:
            raise BaselineError("STATE_DIRECTORY_ERROR") from exc
    publisher.verify_no_symlink_components(path)
    try:
        if not existed:
            os.chmod(path, 0o700, follow_symlinks=False)
        fd = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0))
    except OSError as exc:
        raise BaselineError("STATE_DIRECTORY_ERROR") from exc
    try:
        info = os.fstat(fd)
    finally:
        os.close(fd)
    if (
        not stat.S_ISDIR(info.st_mode)
        or info.st_uid != os.getuid()
        or stat.S_IMODE(info.st_mode) != 0o700
    ):
        raise BaselineError("STATE_DIRECTORY_ERROR")
    return path


@contextmanager
def _worker_lock(state_dir: Path):
    path = state_dir / ".full-data-baseline-worker.lock"
    publisher.verify_no_symlink_components(path)
    fd = None
    try:
        fd = os.open(path, os.O_CREAT | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0), 0o600)
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
            raise BaselineError("WORKER_LOCK_ERROR")
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BaselineError:
        if fd is not None:
            os.close(fd)
        raise
    except (OSError, BlockingIOError) as exc:
        if fd is not None:
            os.close(fd)
        raise BaselineError("WORKER_LOCK_BUSY") from exc
    try:
        yield
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


def _atomic_json(path: Path, value: dict[str, Any]) -> None:
    data = (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
    fd, temporary = tempfile.mkstemp(prefix=".baseline-worker-", dir=path.parent)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "wb", closefd=True) as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory_fd = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


def _write_failure(state_dir: Path, generation_id: str, category: str) -> None:
    try:
        _atomic_json(
            state_dir / "baseline-worker-last-failure.json",
            {
                "version": 1,
                "generation_id": generation_id,
                "category": category,
                "recorded_at": _utc_now(),
            },
        )
    except Exception:
        # The caller still receives a category-only result. Never include the
        # exception text because it can contain private paths or values.
        pass


def _success_path(state_dir: Path, generation_id: str) -> Path:
    return state_dir / f"baseline-{generation_id}.success.json"


def _validated_plan(root: Path, generation_id: str) -> tuple[dict[str, Any], dict[str, Any]]:
    try:
        plan, plan_size, plan_sha = publisher._read_plan(root)
        details = publisher.validate_generation(root, generation_id, plan, plan_size, plan_sha)
    except Exception as exc:
        category = exc.category if isinstance(exc, publisher.PublishError) else "GENERATION_PROOF_INVALID"
        raise BaselineError(category) from exc
    return plan, details


def _plan_details_from_disk(root: Path, generation_id: str) -> tuple[dict[str, Any], dict[str, Any]]:
    """Read the small plan file without rehashing all generation payloads.

    The publisher performs the full local validation before upload. This helper
    is used after that check to bind the worker receipt to the exact plan bytes.
    """
    try:
        plan, plan_size, plan_sha = publisher._read_plan(root)
    except Exception as exc:
        category = exc.category if isinstance(exc, publisher.PublishError) else "GENERATION_PROOF_INVALID"
        raise BaselineError(category) from exc
    source = plan.get("source")
    sqlite_verification = plan.get("sqlite_verification")
    if (
        plan.get("generation_id") != generation_id
        or not isinstance(source, dict)
        or type(source.get("bytes")) is not int
        or not publisher.valid_sha(source.get("sha256"))
        or not isinstance(sqlite_verification, dict)
        or not publisher.valid_sha(sqlite_verification.get("source_schema_sha256"))
        or not isinstance(plan.get("files"), list)
        or not isinstance(plan.get("xlsx_verification"), dict)
        or plan["xlsx_verification"].get("source_rowids_verified") is not True
    ):
        raise BaselineError("GENERATION_PROOF_INVALID")
    files_by_local = {}
    for row in plan["files"]:
        if (
            not isinstance(row, dict)
            or not isinstance(row.get("local"), str)
            or type(row.get("bytes")) is not int
            or not publisher.valid_sha(row.get("sha256"))
        ):
            raise BaselineError("GENERATION_PROOF_INVALID")
        files_by_local[row["local"]] = {
            "local": row["local"],
            "remote": row.get("remote"),
            "bytes": row["bytes"],
            "sha256": row["sha256"],
        }
    files = publisher.normalized_file_order(files_by_local)
    details = {
        "source": {"bytes": source["bytes"], "sha256": source["sha256"]},
        "plan_sha256": plan_sha,
        "plan_bytes": plan_size,
        "source_schema_sha256": sqlite_verification["source_schema_sha256"],
        "files": files,
    }
    return plan, details


def _verify_snapshot_and_manifest_binding(
    snapshot: Path,
    manifest_path: Path,
    generation_root: Path,
    expected_manifest_sha: str,
) -> None:
    linked_snapshot = generation_root / "source.sqlite3"
    copied_manifest = generation_root / "source-manifest.json"
    try:
        if not os.path.samefile(snapshot, linked_snapshot):
            raise BaselineError("SOURCE_BINDING_MISMATCH")
        current_manifest = manifest_path.read_bytes()
        copied_manifest_bytes = copied_manifest.read_bytes()
    except BaselineError:
        raise
    except OSError as exc:
        raise BaselineError("SOURCE_BINDING_MISMATCH") from exc
    if hashlib.sha256(current_manifest).hexdigest() != expected_manifest_sha:
        raise BaselineError("SOURCE_MANIFEST_CHANGED")
    if current_manifest != copied_manifest_bytes:
        raise BaselineError("SOURCE_BINDING_MISMATCH")


def _receipt_matches(
    receipt: dict[str, Any],
    *,
    generation_id: str,
    remote: str,
    source_manifest_sha: str,
    plan: dict[str, Any],
    details: dict[str, Any],
) -> bool:
    return (
        receipt.get("version") == 1
        and receipt.get("status") == "verified"
        and receipt.get("scope") == "immutable_full_baseline"
        and receipt.get("realtime_synchronized") is False
        and receipt.get("generation_id") == generation_id
        and receipt.get("remote") == remote
        and receipt.get("source_sha256") == details["source"]["sha256"]
        and receipt.get("source_bytes") == details["source"]["bytes"]
        and receipt.get("source_manifest_sha256") == source_manifest_sha
        and receipt.get("generation_plan_sha256") == details["plan_sha256"]
        and receipt.get("source_schema_sha256") == details["source_schema_sha256"]
        and receipt.get("local_files") == [
            {"local": row["local"], "bytes": row["bytes"], "sha256": row["sha256"]}
            for row in details["files"]
        ]
        and plan.get("generation_id") == generation_id
        and plan.get("source") == details["source"]
        and plan.get("sqlite_verification", {}).get("coverage", {}).get("all_values") is True
        and plan.get("xlsx_verification", {}).get("source_rowids_verified") is True
    )


def _load_success_receipt(path: Path) -> dict[str, Any] | None:
    value = _read_json(path)
    return value


def _publisher_state_decision(
    state_dir: Path,
    generation_root: Path,
    generation_id: str,
    remote: str,
    rclone_bin: str,
) -> str:
    """Return first_publish, already_published, or refuse_stale_baseline."""
    try:
        _state_path, state = publisher._current_state(state_dir)
    except Exception as exc:
        category = exc.category if isinstance(exc, publisher.PublishError) else "STATE_ERROR"
        raise BaselineError(category) from exc
    if state.get("generation_id") == generation_id and (
        state.get("phase") == "complete" or state.get("last_success") is not None
    ):
        progress_path = state_dir / f"{generation_id}.progress.json"
        try:
            _plan, _plan_size, plan_sha = publisher._read_plan(generation_root)
            progress = publisher._load_progress_state(state_dir, generation_id, plan_sha, remote)
        except Exception as exc:
            category = exc.category if isinstance(exc, publisher.PublishError) else "STATE_ERROR"
            raise BaselineError(category) from exc
        payload = progress.get("latest_payload")
        if (
            isinstance(payload, dict)
            and payload.get("generation_id") == generation_id
            and isinstance(payload.get("index_sha256"), str)
            and progress.get("latest_payload_sha256")
        ):
            expected = (
                json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
            ).encode("utf-8")
            expected_sha = hashlib.sha256(expected).hexdigest()
            if progress["latest_payload_sha256"] != expected_sha:
                raise BaselineError("STATE_ERROR")
            latest_remote = publisher.join_remote(remote, "latest.json")
            try:
                client = publisher.Rclone(rclone_bin)
                metadata = client.stat(latest_remote)
                if metadata is not None and metadata.get("Size") == len(expected):
                    if client.readback(latest_remote) == (len(expected), expected_sha):
                        return "already_published"
            except Exception as exc:
                category = exc.category if isinstance(exc, publisher.PublishError) else "REMOTE_STATE_CHECK_FAILED"
                raise BaselineError(category) from exc
            return "refuse_stale_baseline"
        return "refuse_stale_baseline"
    if state.get("last_success") is not None and state.get("generation_id") != generation_id:
        return "refuse_stale_baseline"
    return "first_publish"


def baseline_cycle(
    snapshot: str | os.PathLike[str],
    source_manifest: str | os.PathLike[str],
    work_dir: str | os.PathLike[str],
    generation_id: str,
    remote: str,
    state_dir: str | os.PathLike[str],
    rclone_bin: str = "rclone",
) -> dict[str, Any]:
    """Run one safe baseline cycle; all results contain metadata only.

    The caller supplies an existing immutable snapshot. Generation artifacts
    are checked read-only before `prepare_generation` is allowed to run.
    """
    if not publisher.valid_generation_id(generation_id):
        return {"status": "failed", "category": "CONFIG_ERROR"}
    state_path: Path | None = None
    try:
        validated_remote = publisher.validate_remote(remote)
        snapshot_path = _safe_path(snapshot)
        manifest_path = _safe_path(source_manifest)
        work_path = _safe_path(work_dir)
        try:
            work_info = work_path.lstat()
        except FileNotFoundError:
            _pending()
        except OSError as exc:
            raise BaselineError("PATH_SAFETY_ERROR") from exc
        if not stat.S_ISDIR(work_info.st_mode):
            raise BaselineError("PATH_SAFETY_ERROR")
        generation_root = work_path / generation_id
        state_path = _ensure_private_state_dir(Path(state_dir))
        publisher.verify_no_symlink_components(generation_root)

        if (
            _path_within(snapshot_path, generation_root)
            or _path_within(generation_root, snapshot_path)
            or _path_within(manifest_path, generation_root)
            or _path_within(generation_root, manifest_path)
        ):
            raise BaselineError("PATH_OVERLAP_ERROR")

        try:
            snapshot_info = snapshot_path.lstat()
        except OSError as exc:
            raise BaselineError("SOURCE_SNAPSHOT_INVALID") from exc
        if not stat.S_ISREG(snapshot_info.st_mode):
            raise BaselineError("SOURCE_SNAPSHOT_INVALID")
        source, source_manifest_sha = _source_identity(snapshot_path, manifest_path)

        try:
            root_info = generation_root.lstat()
        except FileNotFoundError:
            _pending()
        except OSError as exc:
            raise BaselineError("PATH_SAFETY_ERROR") from exc
        if not stat.S_ISDIR(root_info.st_mode):
            raise BaselineError("PATH_SAFETY_ERROR")

        artifact_info = _preflight_artifacts(generation_root, generation_id, source)

        with _worker_lock(state_path):
            # This lock is shared with the publisher CLI. Holding it across
            # prepare and publish prevents a stale baseline from racing a
            # newer generation's latest.json update.
            with publisher.PublishLock(state_path):
                receipt_path = _success_path(state_path, generation_id)
                existing_receipt = _load_success_receipt(receipt_path)
                if existing_receipt is not None:
                    plan, details = _validated_plan(generation_root, generation_id)
                    _verify_snapshot_and_manifest_binding(
                        snapshot_path, manifest_path, generation_root, source_manifest_sha
                    )
                    if not _receipt_matches(
                        existing_receipt,
                        generation_id=generation_id,
                        remote=validated_remote,
                        source_manifest_sha=source_manifest_sha,
                        plan=plan,
                        details=details,
                    ):
                        raise BaselineError("SUCCESS_RECEIPT_MISMATCH")
                    return {
                        "status": "already_published",
                        "generation_id": generation_id,
                        "scope": "immutable_full_baseline",
                        "realtime_synchronized": False,
                        "source_sha256": source["sha256"],
                        "generation_plan_sha256": details["plan_sha256"],
                    }

                decision = _publisher_state_decision(
                    state_path, generation_root, generation_id, validated_remote, rclone_bin
                )
                if decision == "refuse_stale_baseline":
                    raise BaselineError("REMOTE_LATEST_ADVANCED")

                if decision == "already_published":
                    plan, details = _validated_plan(generation_root, generation_id)
                    _verify_snapshot_and_manifest_binding(
                        snapshot_path, manifest_path, generation_root, source_manifest_sha
                    )
                    receipt = _make_success_receipt(
                        generation_id, validated_remote, source_manifest_sha, details
                    )
                    _atomic_json(receipt_path, receipt)
                    return {
                        "status": "already_published",
                        "generation_id": generation_id,
                        "scope": "immutable_full_baseline",
                        "realtime_synchronized": False,
                        "source_sha256": source["sha256"],
                        "generation_plan_sha256": details["plan_sha256"],
                    }

                plan_path = generation_root / "generation-plan.json"
                publisher.verify_no_symlink_components(plan_path)
                if plan_path.exists():
                    # A prior publish attempt may already have a complete plan.
                    # Revalidate it in place so its plan hash remains stable
                    # for publisher resume receipts.
                    plan, details = _validated_plan(generation_root, generation_id)
                else:
                    try:
                        preparer.prepare_generation(
                            snapshot_path,
                            manifest_path,
                            generation_root,
                            generation_id,
                        )
                    except Exception as exc:
                        category = exc.category if isinstance(exc, publisher.PublishError) else "PREPARE_FAILED"
                        raise BaselineError(category) from exc
                    plan, details = _plan_details_from_disk(generation_root, generation_id)
                _verify_snapshot_and_manifest_binding(
                    snapshot_path, manifest_path, generation_root, source_manifest_sha
                )
                try:
                    publish_result = publisher.publish_generation(
                        generation_root,
                        generation_id,
                        validated_remote,
                        state_path,
                        rclone_bin=rclone_bin,
                    )
                except Exception as exc:
                    category = exc.category if isinstance(exc, publisher.PublishError) else "PUBLISH_FAILED"
                    raise BaselineError(category) from exc
                if not isinstance(publish_result, dict) or publish_result.get("status") != "complete":
                    raise BaselineError("PUBLISH_FAILED")

                # publish_generation has already validated every local file
                # and completed full remote readback. Bind the receipt to the
                # same plan bytes without hashing the 28 GB payloads again.
                plan_after, details_after = _plan_details_from_disk(generation_root, generation_id)
                _verify_snapshot_and_manifest_binding(
                    snapshot_path, manifest_path, generation_root, source_manifest_sha
                )
                if (plan_after, details_after) != (plan, details):
                    raise BaselineError("GENERATION_CHANGED_DURING_PUBLISH")
                receipt = _make_success_receipt(
                    generation_id, validated_remote, source_manifest_sha, details
                )
                _atomic_json(receipt_path, receipt)
                return {
                    "status": "verified",
                    "generation_id": generation_id,
                    "scope": "immutable_full_baseline",
                    "realtime_synchronized": False,
                    "source_sha256": source["sha256"],
                    "generation_plan_sha256": details["plan_sha256"],
                    "counts": artifact_info["counts"],
                }
    except BaselineError as exc:
        if state_path is not None and not exc.pending:
            _write_failure(state_path, generation_id, exc.category)
        return {
            "status": "pending_baseline_artifacts" if exc.pending else "failed",
            "generation_id": generation_id,
            "category": exc.category,
        }
    except publisher.PublishError as exc:
        if exc.category == 'LOCK_BUSY':
            return {'status': 'pending_publication_lock', 'generation_id': generation_id,
                    'category': 'LOCK_BUSY'}
        if state_path is not None:
            _write_failure(state_path, generation_id, exc.category)
        return {"status": "failed", "generation_id": generation_id, "category": exc.category}
    except Exception:
        if state_path is not None:
            _write_failure(state_path, generation_id, "INTERNAL_ERROR")
        return {"status": "failed", "generation_id": generation_id, "category": "INTERNAL_ERROR"}


def _make_success_receipt(
    generation_id: str,
    remote: str,
    source_manifest_sha: str,
    details: dict[str, Any],
) -> dict[str, Any]:
    return {
        "version": 1,
        "status": "verified",
        "scope": "immutable_full_baseline",
        "realtime_synchronized": False,
        "generation_id": generation_id,
        "remote": remote,
        "source_bytes": details["source"]["bytes"],
        "source_sha256": details["source"]["sha256"],
        "source_manifest_sha256": source_manifest_sha,
        "generation_plan_bytes": details["plan_bytes"],
        "generation_plan_sha256": details["plan_sha256"],
        "source_schema_sha256": details["source_schema_sha256"],
        "local_files": [
            {"local": row["local"], "bytes": row["bytes"], "sha256": row["sha256"]}
            for row in details["files"]
        ],
        "recorded_at": _utc_now(),
    }


def _printable_result(result: dict[str, Any]) -> dict[str, Any]:
    allowed = {
        "status", "category", "generation_id", "scope", "realtime_synchronized",
        "source_sha256", "generation_plan_sha256", "counts",
    }
    return {key: result[key] for key in allowed if key in result}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", required=True, type=Path)
    parser.add_argument("--source-manifest", required=True, type=Path)
    parser.add_argument("--work-dir", required=True, type=Path)
    parser.add_argument("--generation-id", required=True)
    parser.add_argument("--remote", required=True)
    parser.add_argument("--state-dir", required=True, type=Path)
    parser.add_argument("--rclone-bin", default="rclone")
    parser.add_argument("--watch", action="store_true")
    parser.add_argument("--interval", type=int, default=30)
    args = parser.parse_args(argv)
    if not 5 <= args.interval <= 60:
        parser.error("--interval must be between 5 and 60 seconds")
    while True:
        result = baseline_cycle(
            args.snapshot,
            args.source_manifest,
            args.work_dir,
            args.generation_id,
            args.remote,
            args.state_dir,
            args.rclone_bin,
        )
        print(json.dumps(_printable_result(result), ensure_ascii=False, sort_keys=True, separators=(",", ":")))
        if args.watch and result.get("status") in ('pending_baseline_artifacts', 'pending_publication_lock'):
            time.sleep(args.interval)
            continue
        return 0 if result.get("status") in ("verified", "already_published") else 1


if __name__ == "__main__":
    raise SystemExit(main())
