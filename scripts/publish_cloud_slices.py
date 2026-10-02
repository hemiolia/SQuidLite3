#!/usr/bin/env python3
"""Publish one verified, already-staged NAS slice generation.

The source is a private immutable staging copy. A generation is advertised by
uploading its index last, after the source manifest and every slice have been
checked locally and read back from the remote store. Existing cloud objects
are never replaced or removed.
"""
import argparse
import hashlib
import json
from pathlib import Path
import re
import shlex
import subprocess
import sys
import time


NAS_ROOT = Path("/home/Natsuki/ikaring-archive/backups/cloud-staging")
REMOTE = "ikaring_exports:database/slices/generations"
NAME = re.compile(r"\d{8}T\d{6}Z-[0-9a-f]{8}\Z", re.ASCII)
MODE_FILE = re.compile(r"by-mode/[a-z0-9_]{1,64}\.sqlite3\Z", re.ASCII)
RULE_FILE = re.compile(r"by-rule/[A-Za-z0-9_]+\.sqlite3\Z", re.ASCII)
SHA256 = re.compile(r"[0-9a-f]{64}\Z", re.ASCII)


_SAFE_STAGED_FILE = r'''import os, re, stat, sys
root, snapshot, rel, read = sys.argv[1:]
if not re.fullmatch(r"[0-9]{8}T[0-9]{6}Z-[0-9a-f]{8}", snapshot):
    raise SystemExit("invalid snapshot")
if rel not in ("index.json", "slices-source-manifest.json") and not (
    re.fullmatch(r"by-mode/[a-z0-9_]{1,64}\.sqlite3", rel)
    or re.fullmatch(r"by-rule/[A-Za-z0-9_]+\.sqlite3", rel)
):
    raise SystemExit("invalid relative path")
current = "/"
for part in root.strip("/").split("/"):
    current = os.path.join(current, part)
    info = os.lstat(current)
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
        raise SystemExit("unsafe staging root")
for part in (snapshot, *rel.split("/")):
    current = os.path.join(current, part)
    info = os.lstat(current)
    if stat.S_ISLNK(info.st_mode):
        raise SystemExit("symlink in staging path")
    if current != os.path.join(root, snapshot, rel) and not stat.S_ISDIR(info.st_mode):
        raise SystemExit("non-directory staging parent")
if not stat.S_ISREG(info.st_mode):
    raise SystemExit("staged object is not a regular file")
fd = os.open(current, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
try:
    if not stat.S_ISREG(os.fstat(fd).st_mode):
        raise SystemExit("staged object changed type")
    if read == "1":
        with os.fdopen(fd, "rb", closefd=False) as source:
            while True:
                chunk = source.read(1024 * 1024)
                if not chunk:
                    break
                sys.stdout.buffer.write(chunk)
finally:
    os.close(fd)
'''


def call(*args, capture=False):
    result = subprocess.run(args, stdout=subprocess.PIPE if capture else None, check=True)
    return result.stdout


def remote_file(snapshot_id, rel):
    if not _valid_snapshot_id(snapshot_id) or not _valid_generation_path(rel):
        raise ValueError("invalid remote generation path")
    return f"{REMOTE}/{snapshot_id}/{rel}"


def _valid_snapshot_id(snapshot_id):
    return isinstance(snapshot_id, str) and bool(NAME.fullmatch(snapshot_id))


def _valid_slice_path(rel):
    return isinstance(rel, str) and bool(MODE_FILE.fullmatch(rel) or RULE_FILE.fullmatch(rel))


def _valid_generation_path(rel):
    return rel in ("index.json", "slices-source-manifest.json") or _valid_slice_path(rel)


def _staged_python_command(snapshot_id, rel, *, read):
    if not _valid_snapshot_id(snapshot_id):
        raise ValueError("invalid snapshot id")
    if not _valid_generation_path(rel):
        raise ValueError("invalid staged path")
    return shlex.join([
        "python3", "-c", _SAFE_STAGED_FILE, str(NAS_ROOT), snapshot_id, rel,
        "1" if read else "0",
    ])


def staged_file_command(snapshot_id, rel, *, read):
    remote = _staged_python_command(snapshot_id, rel, read=read)
    return ["ssh", "-o", "BatchMode=yes", "nas", remote]


def local_file(snapshot_id, rel):
    """Return the remote staging path after applying the same strict path rules."""
    if not _valid_snapshot_id(snapshot_id):
        raise ValueError("invalid snapshot id")
    if not _valid_generation_path(rel):
        raise ValueError("invalid staged path")
    return str(NAS_ROOT / snapshot_id / rel)


def remote_command(*args):
    return ["ssh", "-o", "BatchMode=yes", "nas",
            shlex.join(["docker", "exec", "ikaring-archive-export-publisher",
                        "rclone", *args])]


def stream_digest(command):
    digest = hashlib.sha256()
    size = 0
    with subprocess.Popen(command, stdout=subprocess.PIPE) as proc:
        if proc.stdout is None:
            raise RuntimeError("could not read command output")
        for chunk in iter(lambda: proc.stdout.read(1024 * 1024), b""):
            size += len(chunk)
            digest.update(chunk)
        if proc.wait():
            raise RuntimeError(f"read failed: {command[0]}")
    return size, digest.hexdigest()


def _is_rate_limit(stderr):
    text = stderr.decode("utf-8", errors="replace").lower()
    return any(marker in text for marker in (
        "ratelimitexceeded", "userratelimitexceeded", "rate limit exceeded",
        "rate-limit exceeded", "too many requests",
    ))


def _is_missing(stderr):
    text = stderr.decode("utf-8", errors="replace").lower()
    # Rate limiting takes precedence: an HTTP error body can contain unrelated
    # text and must never be reclassified as an absent object.
    if _is_rate_limit(stderr) or any(marker in text for marker in (
        '403', '401', 'permission denied', 'access denied', 'unauthorized',
        'forbidden', 'invalid_grant', 'invalid credentials',
        'connection refused', 'timed out', 'timeout',
    )):
        return False
    return any(marker in text for marker in (
        "file not found", "object not found", "directory not found",
        "notfound", "not found", "no such file or directory", "no such object",
        "no object found", "does not exist",
        "doesn't exist", "cannot find", "could not find",
    ))


def remote_meta(path):
    for attempt in range(5):
        result = subprocess.run(
            remote_command("lsjson", "--stat", "--hash", path),
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        if result.returncode == 0:
            try:
                metadata = json.loads(result.stdout)
            except (UnicodeError, json.JSONDecodeError) as exc:
                raise RuntimeError("remote stat returned invalid JSON") from exc
            if not isinstance(metadata, dict):
                raise RuntimeError("remote stat returned invalid metadata")
            return metadata
        if _is_rate_limit(result.stderr):
            if attempt == 4:
                raise RuntimeError("remote stat rate limit persisted after retries")
            delay = 60 * (attempt + 1)
            print(f"stat retry, wait {delay}s", flush=True)
            time.sleep(delay)
            continue
        if result.returncode in (3, 4) and _is_missing(result.stderr):
            return None
        raise RuntimeError(f"remote stat failed: {result.returncode}")
    raise RuntimeError("remote stat failed after retries")


def _require_count(counts, key, expected, *, source):
    if not isinstance(counts, dict):
        raise RuntimeError(f"{source} counts are invalid")
    value = counts.get(key)
    if type(value) is not int or value != expected:
        raise RuntimeError(f"{source} {key} mismatch")


def _manifest_file_map(rows, *, category):
    if not isinstance(rows, list):
        raise RuntimeError(f"source manifest {category} list is invalid")
    if category == "by_mode" and not rows:
        raise RuntimeError("source manifest has no mode files")
    matcher = MODE_FILE if category == "by_mode" else RULE_FILE
    found = {}
    for row in rows:
        if not isinstance(row, dict):
            raise RuntimeError(f"source manifest {category} row is invalid")
        rel = row.get("file")
        if not isinstance(rel, str) or not matcher.fullmatch(rel):
            raise RuntimeError("invalid source manifest path")
        if rel in found:
            raise RuntimeError("duplicate source manifest path")
        size = row.get("bytes")
        if type(size) is not int or size <= 0:
            raise RuntimeError("invalid source manifest file size")
        found[rel] = size
    return found


def validate_generation(index, source_manifest, snapshot_id):
    """Validate the complete file map before any remote upload can start."""
    if not _valid_snapshot_id(snapshot_id):
        raise RuntimeError("invalid snapshot id")
    if not isinstance(index, dict):
        raise RuntimeError("index must be an object")
    if index.get("snapshot_id") != snapshot_id:
        raise RuntimeError("index snapshot mismatch")
    if not isinstance(source_manifest, dict):
        raise RuntimeError("source manifest must be an object")

    by_mode = _manifest_file_map(source_manifest.get("by_mode"), category="by_mode")
    by_rule = _manifest_file_map(source_manifest.get("by_rule"), category="by_rule")
    if set(by_mode) & set(by_rule):
        raise RuntimeError("source manifest file categories overlap")

    source_counts = source_manifest.get("counts")
    _require_count(source_counts, "mode_files", len(by_mode), source=True)
    _require_count(source_counts, "rule_files", len(by_rule), source=True)

    rows = index.get("files")
    if not isinstance(rows, list) or not rows:
        raise RuntimeError("index files must be a non-empty list")
    indexed = {}
    for row in rows:
        if not isinstance(row, dict):
            raise RuntimeError("index file row must be an object")
        rel = row.get("file")
        if not _valid_slice_path(rel):
            raise RuntimeError("invalid staged path")
        if rel in indexed:
            raise RuntimeError("duplicate index path")
        size = row.get("bytes")
        if type(size) is not int or size <= 0:
            raise RuntimeError("invalid index file size")
        digest = row.get("sha256")
        if not isinstance(digest, str) or not SHA256.fullmatch(digest):
            raise RuntimeError("invalid index SHA-256")
        indexed[rel] = row

    expected_sizes = {**by_mode, **by_rule}
    if set(indexed) != set(expected_sizes):
        raise RuntimeError("index and source manifest file lists mismatch")
    for rel, expected_size in expected_sizes.items():
        if indexed[rel]["bytes"] != expected_size:
            raise RuntimeError("index and source manifest file size mismatch")

    index_counts = index.get("counts")
    if "counts" in index:
        _require_count(index_counts, "mode_files", len(by_mode), source="index")
        _require_count(index_counts, "rule_files", len(by_rule), source="index")
    return rows


def verified_upload(snapshot_id, rel, expected_bytes, expected_sha):
    target = remote_file(snapshot_id, rel)
    source_command = staged_file_command(snapshot_id, rel, read=True)
    if stream_digest(source_command) != (expected_bytes, expected_sha):
        raise RuntimeError(f"staging hash mismatch before upload: {rel}")
    metadata = remote_meta(target)
    if metadata is None:
        command = ("set -o pipefail; " + _staged_python_command(snapshot_id, rel, read=True) +
                   " | docker exec -i ikaring-archive-export-publisher " +
                   shlex.join(["rclone", "rcat", "--immutable", target]))
        # NAS login shell is bash; pipefail prevents a truncated source stream
        # from being mistaken for a successful rclone transfer. Partial remote
        # objects are retained and cause a hard failure; they are not deleted.
        for attempt in range(5):
            status = subprocess.run(["ssh", "-o", "BatchMode=yes", "nas",
                                     "bash", "-c", shlex.quote(command)]).returncode
            if status == 0:
                break
            metadata = remote_meta(target)
            if metadata is not None:
                raise RuntimeError(f"upload failed with remote object present: {rel}")
            if attempt == 4:
                raise RuntimeError(f"upload failed after retries: {rel}, status={status}")
            delay = 60 * (attempt + 1)
            print(f"upload retry {rel}, wait {delay}s", flush=True)
            time.sleep(delay)
        metadata = remote_meta(target)
    size = metadata.get("Size") if isinstance(metadata, dict) else None
    if type(size) is not int or size != expected_bytes or size <= 0:
        raise RuntimeError(f"remote size mismatch: {rel}")
    if stream_digest(remote_command("cat", target)) != (expected_bytes, expected_sha):
        raise RuntimeError(f"remote readback mismatch: {rel}")
    print(f"verified {rel} {expected_bytes}", flush=True)


def _read_staged_json(snapshot_id, rel):
    return call(*staged_file_command(snapshot_id, rel, read=True), capture=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("snapshot_id")
    args = parser.parse_args()
    if not NAME.fullmatch(args.snapshot_id):
        parser.error("invalid snapshot id")
    # Historical helper retained for auditing its receipts only. It proves
    # transfer integrity of an incomplete projection, never full information.
    parser.error("旧partial sliceの配信は廃止しました。nas_full_data_publish.pyの全情報検証を使用してください。")

    # Read and validate both control documents before touching cloud storage.
    index_bytes = _read_staged_json(args.snapshot_id, "index.json")
    source_manifest_bytes = _read_staged_json(args.snapshot_id, "slices-source-manifest.json")
    try:
        index = json.loads(index_bytes)
        source_manifest = json.loads(source_manifest_bytes)
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise RuntimeError("staged generation metadata is invalid JSON") from exc
    files = validate_generation(index, source_manifest, args.snapshot_id)

    for row in files:
        verified_upload(args.snapshot_id, row["file"], row["bytes"], row["sha256"])
    verified_upload(args.snapshot_id, "slices-source-manifest.json",
                    len(source_manifest_bytes), hashlib.sha256(source_manifest_bytes).hexdigest())
    # This is the only completion marker and must remain the final upload.
    verified_upload(args.snapshot_id, "index.json",
                    len(index_bytes), hashlib.sha256(index_bytes).hexdigest())
    print(f"complete {args.snapshot_id}", flush=True)


if __name__ == "__main__":
    sys.exit(main())
