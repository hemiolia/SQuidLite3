#!/usr/bin/env python3
"""
NAS GUI Publisher (One-cycle CLI).

Publishes only ``gui/index.html`` from the NAS exports directory to a
pre-authorized Google Drive remote via rclone. The legacy ``分析.xlsx`` is not
read or communicated. If an old state file records both artifacts, its exact
bytes are retained locally before the GUI-only state replaces it.

Design Decision:
- One-cycle push only. Not a bi-directional generic sync.
- Only the HTML GUI is in scope; this receipt never represents database synchronization.
- No collector, SQLite DB, or authentication handling is added here.
- Strict single-cycle execution with non-blocking flock.
- The GUI source is snapshotted before transfer. State is committed only after
  its remote readback verifies.

Pre-upload Cloud Verification Notice:
The pre-upload state verification (re-checking stat and cat immediately before transfer)
is NOT an atomic cloud Compare-And-Swap (CAS). rclone does not provide end-to-end atomic
conditional PUT semantics across remote cloud storage providers. A race condition window
inherently exists between the pre-check and the subsequent rclone copyto command.
This verification detects concurrent modifications that occurred during the publish cycle,
but cannot eliminate race conditions occurring during the final cloud copyto itself.
"""

import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import tempfile
import uuid


PERMITTED_FILES = ("gui/index.html",)
LEGACY_STATE_FILES = frozenset(("gui/index.html", "分析.xlsx"))
LEGACY_STATE_HISTORY_DIR = "history"
MAX_FILE_BYTES = 64 * 1024 * 1024  # 64 MiB (67,108,864 bytes)
MAX_STATE_BYTES = 1024 * 1024
STATE_FILE_NAME = ".nas-publish-state.json"
LOCK_FILE_NAME = ".nas-publish.lock"
STATE_SCHEMA_VERSION = 1
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
REMOTE_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")
MAX_RCLONE_STAT_BYTES = 64 * 1024

# Failure categories for safe stderr reporting
CATEGORY_CONFIG_ERROR = "CONFIG_ERROR"
CATEGORY_LOCK_BUSY = "LOCK_BUSY"
CATEGORY_PATH_SAFETY_ERROR = "PATH_SAFETY_ERROR"
CATEGORY_SOURCE_MISSING = "SOURCE_MISSING"
CATEGORY_SOURCE_OVERSIZE = "SOURCE_OVERSIZE"
CATEGORY_STATE_ERROR = "STATE_ERROR"
CATEGORY_REMOTE_ERROR = "REMOTE_ERROR"
CATEGORY_PRECONDITION_FAILED = "PRECONDITION_FAILED"
CATEGORY_VERIFY_ERROR = "VERIFY_ERROR"
CATEGORY_INTERNAL_ERROR = "INTERNAL_ERROR"


class PublishError(Exception):
    """Base error for nas_publish_exports with safe failure category."""

    def __init__(self, category: str, message: str = ""):
        super().__init__(message)
        self.category = category
        self.message = message


def verify_no_symlinks(path: Path) -> None:
    """Reject a symlink at path or at any lexical ancestor."""
    path = Path(path).expanduser()
    if '..' in path.parts:
        raise PublishError(CATEGORY_PATH_SAFETY_ERROR, "Parent traversal rejected")
    absolute = path.absolute()
    for candidate in (absolute, *absolute.parents):
        if os.path.islink(candidate):
            raise PublishError(CATEGORY_PATH_SAFETY_ERROR, f"Symlink rejected: {candidate}")


def verify_safe_path(base_dir: Path, rel_str: str) -> Path:
    """
    Resolves relative path under base_dir while strictly prohibiting directory,
    parent, and file symlinks, as well as path traversal.
    """
    rel_path = Path(rel_str)
    if ".." in rel_path.parts or rel_path.is_absolute():
        raise PublishError(CATEGORY_PATH_SAFETY_ERROR, f"Path traversal rejected: {rel_str}")

    curr = base_dir
    verify_no_symlinks(curr)

    for part in rel_path.parts:
        curr = curr / part
        verify_no_symlinks(curr)

    return curr


def verify_state_dir(state_dir: Path) -> None:
    """Ensures state_dir exists, is a regular directory, and contains no symlinks."""
    verify_no_symlinks(state_dir)
    if not state_dir.exists():
        state_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        verify_no_symlinks(state_dir)
    elif not state_dir.is_dir():
        raise PublishError(CATEGORY_PATH_SAFETY_ERROR, f"State directory is not a directory: {state_dir}")


class PublishLock:
    """Non-blocking flock context manager to serialize publish cycles."""

    def __init__(self, state_dir: Path):
        self.state_dir = state_dir
        self.lock_path = state_dir / LOCK_FILE_NAME
        self.fd = None

    def __enter__(self):
        verify_no_symlinks(self.lock_path)
        flags = os.O_CREAT | os.O_RDWR
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            self.fd = os.open(self.lock_path, flags, 0o600)
        except OSError as exc:
            raise PublishError(CATEGORY_LOCK_BUSY, f"Failed to open lock file: {exc}") from exc

        try:
            if os.name == "nt":
                import msvcrt

                try:
                    msvcrt.locking(self.fd, msvcrt.LK_NBLCK, 1)
                except OSError as exc:
                    raise PublishError(CATEGORY_LOCK_BUSY, "Another publish cycle is running") from exc
            else:
                try:
                    fcntl.flock(self.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except (BlockingIOError, OSError) as exc:
                    raise PublishError(CATEGORY_LOCK_BUSY, "Another publish cycle is running") from exc
        except Exception:
            if self.fd is not None:
                os.close(self.fd)
                self.fd = None
            raise
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.fd is not None:
            try:
                if os.name == "nt":
                    import msvcrt

                    try:
                        msvcrt.locking(self.fd, msvcrt.LK_UNLCK, 1)
                    except OSError:
                        pass
                else:
                    try:
                        fcntl.flock(self.fd, fcntl.LOCK_UN)
                    except OSError:
                        pass
            finally:
                os.close(self.fd)
                self.fd = None


def snapshot_source_files(exports_dir: Path) -> dict:
    """
    Reads and snapshots the GUI export only. It must be a regular, non-symlink
    file and must not exceed 64MiB. Legacy XLSX artifacts are never opened.
    """
    verify_no_symlinks(exports_dir)
    snapshots = {}

    for rel_path in PERMITTED_FILES:
        target_path = verify_safe_path(exports_dir, rel_path)
        flags = os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0) | getattr(os, 'O_NONBLOCK', 0)
        try:
            fd = os.open(target_path, flags)
        except FileNotFoundError as exc:
            raise PublishError(CATEGORY_SOURCE_MISSING, f"Required source file missing: {rel_path}") from exc
        except OSError as exc:
            raise PublishError(CATEGORY_PATH_SAFETY_ERROR, f"Cannot safely open source: {rel_path}") from exc
        with os.fdopen(fd, 'rb') as source:
            st = os.fstat(source.fileno())
            if not stat.S_ISREG(st.st_mode):
                raise PublishError(CATEGORY_PATH_SAFETY_ERROR, f"Source is not a regular file: {rel_path}")
            if st.st_size > MAX_FILE_BYTES:
                raise PublishError(
                    CATEGORY_SOURCE_OVERSIZE,
                    f"Source file {rel_path} exceeds {MAX_FILE_BYTES} bytes",
                )
            data = source.read(MAX_FILE_BYTES + 1)
        if len(data) > MAX_FILE_BYTES:
            raise PublishError(
                CATEGORY_SOURCE_OVERSIZE,
                f"Source file {rel_path} bytes {len(data)} exceeds {MAX_FILE_BYTES} bytes",
            )

        digest = hashlib.sha256(data).hexdigest()
        snapshots[rel_path] = {
            "bytes": data,
            "sha256": digest,
            "size": len(data),
        }

    return snapshots


def _read_regular_bytes(path: Path, max_bytes: int) -> bytes:
    """Read one bounded regular file through a no-follow descriptor."""
    verify_no_symlinks(path)
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
    try:
        fd = os.open(path, flags)
    except OSError as exc:
        raise PublishError(CATEGORY_STATE_ERROR, "State evidence file is missing or unreadable") from exc
    try:
        st = os.fstat(fd)
        if not stat.S_ISREG(st.st_mode) or st.st_size > max_bytes:
            raise PublishError(CATEGORY_STATE_ERROR, "State evidence file has an invalid type or size")
        with os.fdopen(fd, "rb") as stream:
            fd = -1
            data = stream.read(max_bytes + 1)
        if len(data) > max_bytes:
            raise PublishError(CATEGORY_STATE_ERROR, "State evidence file exceeds its size limit")
        return data
    except PublishError:
        raise
    except OSError as exc:
        raise PublishError(CATEGORY_STATE_ERROR, "State evidence file is unreadable") from exc
    finally:
        if fd >= 0:
            os.close(fd)


def _validate_retained_legacy_state(state_dir: Path, value: object) -> dict:
    if not isinstance(value, dict) or set(value) != {"file", "bytes", "sha256"}:
        raise PublishError(CATEGORY_STATE_ERROR, "Invalid retained legacy state pointer")
    relative = value.get("file")
    size = value.get("bytes")
    digest = value.get("sha256")
    match = re.fullmatch(r"history/legacy-analysis-state-([0-9a-f]{64})\.json", relative or "")
    if (
        match is None
        or type(size) is not int
        or size < 0
        or not isinstance(digest, str)
        or not SHA256_RE.fullmatch(digest)
        or match.group(1) != digest
    ):
        raise PublishError(CATEGORY_STATE_ERROR, "Invalid retained legacy state pointer")
    retained_path = verify_safe_path(state_dir, relative)
    retained_bytes = _read_regular_bytes(retained_path, MAX_STATE_BYTES)
    if len(retained_bytes) != size or hashlib.sha256(retained_bytes).hexdigest() != digest:
        raise PublishError(CATEGORY_STATE_ERROR, "Retained legacy state does not match its pointer")
    return {"file": relative, "bytes": size, "sha256": digest}


def _load_state_with_bytes(state_dir: Path, expected_remote: str) -> tuple[dict | None, bytes | None]:
    """
    Loads and validates state while retaining its exact original bytes for a
    possible non-destructive legacy-state archive before state replacement.
    """
    state_file = state_dir / STATE_FILE_NAME
    verify_no_symlinks(state_file)
    if not state_file.exists():
        return None, None

    try:
        content_bytes = _read_regular_bytes(state_file, MAX_STATE_BYTES)
        data = json.loads(content_bytes.decode("utf-8"))
    except Exception as exc:
        if isinstance(exc, PublishError):
            raise
        raise PublishError(CATEGORY_STATE_ERROR, "State file is corrupt or unreadable") from exc

    if not isinstance(data, dict):
        raise PublishError(CATEGORY_STATE_ERROR, "State root must be a dict")
    if type(data.get("version")) is not int or data["version"] != STATE_SCHEMA_VERSION:
        raise PublishError(CATEGORY_STATE_ERROR, f"Unsupported state version: {data.get('version')}")
    if data.get("remote") != expected_remote:
        raise PublishError(CATEGORY_STATE_ERROR, f"State remote mismatch: {data.get('remote')} != {expected_remote}")

    files = data.get("files")
    if not isinstance(files, dict):
        raise PublishError(CATEGORY_STATE_ERROR, "State files field must be a dict")

    for k, v in files.items():
        if k not in LEGACY_STATE_FILES or not isinstance(v, str) or not SHA256_RE.fullmatch(v):
            raise PublishError(CATEGORY_STATE_ERROR, f"Invalid state files entry: {k}={v}")
    if "gui/index.html" not in files:
        raise PublishError(CATEGORY_STATE_ERROR, "State does not contain the permitted GUI hash")

    if "scope" in data and data["scope"] != "gui_only":
        raise PublishError(CATEGORY_STATE_ERROR, "State scope is not gui_only")
    if "full_database_synchronized" in data and data["full_database_synchronized"] is not False:
        raise PublishError(CATEGORY_STATE_ERROR, "State cannot claim full database synchronization")

    retained = None
    if "retained_legacy_state" in data:
        retained = _validate_retained_legacy_state(state_dir, data["retained_legacy_state"])
        if (
            "分析.xlsx" in files
            and hashlib.sha256(content_bytes).hexdigest() != retained["sha256"]
        ):
            raise PublishError(CATEGORY_STATE_ERROR, "Legacy state differs from retained state bytes")
        data["retained_legacy_state"] = retained

    return data, content_bytes


def load_state(state_dir: Path, expected_remote: str) -> dict | None:
    """Load a publisher state, accepting only the GUI state and known legacy XLSX hash."""
    return _load_state_with_bytes(state_dir, expected_remote)[0]


def _preserve_legacy_state(state_dir: Path, content: bytes) -> dict:
    """Write the exact old two-file state once and verify the durable readback."""
    digest = hashlib.sha256(content).hexdigest()
    relative = f"{LEGACY_STATE_HISTORY_DIR}/legacy-analysis-state-{digest}.json"
    history_dir = verify_safe_path(state_dir, LEGACY_STATE_HISTORY_DIR)
    if history_dir.exists():
        if not history_dir.is_dir():
            raise PublishError(CATEGORY_PATH_SAFETY_ERROR, "Legacy state history is not a directory")
    else:
        history_dir.mkdir(mode=0o700)
    verify_no_symlinks(history_dir)
    target = verify_safe_path(state_dir, relative)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(target, flags, 0o600)
    except FileExistsError:
        existing = _read_regular_bytes(target, MAX_STATE_BYTES)
        if len(existing) != len(content) or hashlib.sha256(existing).hexdigest() != digest or existing != content:
            raise PublishError(CATEGORY_STATE_ERROR, "Existing legacy state archive differs from source bytes")
    except OSError as exc:
        raise PublishError(CATEGORY_STATE_ERROR, "Cannot create legacy state archive") from exc
    else:
        try:
            with os.fdopen(fd, "wb") as stream:
                view = memoryview(content)
                while view:
                    written = stream.write(view)
                    if written is None or written <= 0:
                        raise OSError("short write")
                    view = view[written:]
                stream.flush()
                os.fsync(stream.fileno())
        except Exception as exc:
            raise PublishError(CATEGORY_STATE_ERROR, "Cannot write legacy state archive") from exc

        try:
            dir_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
            dir_fd = os.open(history_dir, dir_flags)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except OSError as exc:
            raise PublishError(CATEGORY_STATE_ERROR, "Cannot sync legacy state archive directory") from exc

    archived = _read_regular_bytes(target, MAX_STATE_BYTES)
    if len(archived) != len(content) or hashlib.sha256(archived).hexdigest() != digest or archived != content:
        raise PublishError(CATEGORY_STATE_ERROR, "Legacy state archive readback mismatch")
    return {"file": relative, "bytes": len(content), "sha256": digest}


def save_state(
    state_dir: Path,
    remote: str,
    files_hash: dict,
    *,
    retained_legacy_state: dict | None = None,
) -> None:
    """
    Atomically writes state JSON via 0600 temp file + fsync + os.replace.
    """
    state_file = state_dir / STATE_FILE_NAME
    verify_no_symlinks(state_file)

    payload = {
        "version": STATE_SCHEMA_VERSION,
        "remote": remote,
        "files": files_hash,
        "scope": "gui_only",
        "full_database_synchronized": False,
    }
    if retained_legacy_state is not None:
        payload["retained_legacy_state"] = retained_legacy_state
    encoded = (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode("utf-8")

    fd, temp_path_str = tempfile.mkstemp(prefix=".tmp_publish_state_", dir=state_dir)
    temp_path = Path(temp_path_str)
    try:
        if hasattr(os, "fchmod"):
            os.fchmod(fd, 0o600)
        else:
            os.chmod(temp_path, 0o600)

        with os.fdopen(fd, "wb") as f:
            f.write(encoded)
            f.flush()
            os.fsync(f.fileno())

        os.replace(temp_path, state_file)
    except Exception as exc:
        if temp_path.exists():
            try:
                temp_path.unlink()
            except OSError:
                pass
        raise PublishError(CATEGORY_STATE_ERROR, "Failed to atomically save state") from exc


def check_command_args(cmd: list) -> None:
    """Validates subprocess arguments array to prevent shell injection."""
    if not isinstance(cmd, (list, tuple)):
        raise PublishError(CATEGORY_CONFIG_ERROR, "Command must be a list of strings")
    for arg in cmd:
        if not isinstance(arg, str) or "\0" in arg:
            raise PublishError(CATEGORY_CONFIG_ERROR, "Invalid command argument")


def run_rclone_cmd(cmd: list) -> subprocess.CompletedProcess:
    """Run rclone without shell; bound stat output and discard unused output."""
    check_command_args(cmd)
    proc = None
    try:
        is_stat = len(cmd) > 1 and cmd[1] == 'lsjson'
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE if is_stat else subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            shell=False,
            env=os.environ,
        )
        if is_stat:
            output = proc.stdout.read(MAX_RCLONE_STAT_BYTES + 1)
            if len(output) > MAX_RCLONE_STAT_BYTES:
                proc.kill()
                proc.wait()
                raise PublishError(CATEGORY_REMOTE_ERROR, 'rclone stat output exceeds limit')
            proc.stdout.close()
        else:
            output = b''
        proc.wait()
        return subprocess.CompletedProcess(cmd, proc.returncode, output, b'')
    except PublishError:
        raise
    except Exception as exc:
        raise PublishError(CATEGORY_REMOTE_ERROR, "Failed to execute rclone process") from exc
    finally:
        if proc is not None:
            if proc.stdout is not None and not proc.stdout.closed:
                proc.stdout.close()
            if proc.poll() is None:
                proc.kill()
                proc.wait()


def join_remote(remote_prefix: str, rel_path: str) -> str:
    """Combines remote prefix and relative path."""
    if not isinstance(remote_prefix, str) or ":" not in remote_prefix:
        raise PublishError(CATEGORY_CONFIG_ERROR, "Remote prefix must be NAME:PATH")
    name, path = remote_prefix.split(":", 1)
    if (not REMOTE_NAME_RE.fullmatch(name) or not path or path.startswith(('/', '\\'))
            or '\\' in path or any(ord(ch) < 32 or ord(ch) == 127 for ch in path)
            or any(part in ('.', '..', '') for part in path.split('/'))):
        raise PublishError(CATEGORY_CONFIG_ERROR, "Remote prefix must be a safe NAME:PATH")
    return f"{name}:{path}/{rel_path}"


def check_remote_file(rclone_bin: str, remote_file_path: str) -> dict | None:
    """
    Checks existence and metadata of remote file via rclone lsjson --stat --hash.
    Returns metadata dict if present.
    Returns None only if exit code is 3 (DirNotFound) or 4 (FileNotFound).
    Raises PublishError on all other exit codes or format issues.
    """
    cmd = [rclone_bin, "lsjson", "--stat", "--hash", remote_file_path]
    proc = run_rclone_cmd(cmd)

    if proc.returncode in (3, 4):
        # 3: DirNotFound, 4: FileNotFound
        return None

    if proc.returncode != 0:
        raise PublishError(CATEGORY_REMOTE_ERROR, f"rclone lsjson failed with exit code {proc.returncode}")

    try:
        data = json.loads(proc.stdout.decode("utf-8"))
    except Exception as exc:
        raise PublishError(CATEGORY_REMOTE_ERROR, "Failed to parse lsjson output as JSON") from exc

    if not isinstance(data, dict) or data.get('IsDir') is not False:
        raise PublishError(CATEGORY_REMOTE_ERROR, "Unexpected lsjson file metadata")

    size = data.get("Size")
    if type(size) is not int or size < 0:
        raise PublishError(CATEGORY_REMOTE_ERROR, "Invalid remote file size")
    if size > MAX_FILE_BYTES:
        raise PublishError(
            CATEGORY_SOURCE_OVERSIZE,
            f"Remote file exceeds {MAX_FILE_BYTES} bytes: {remote_file_path}",
        )

    return data


def cat_remote_file(rclone_bin: str, remote_file_path: str) -> tuple[bytes, str]:
    """
    Retrieves entire remote file content using rclone cat and calculates SHA256.
    Aborts immediately if content exceeds MAX_FILE_BYTES during streaming.
    """
    cmd = [rclone_bin, "cat", remote_file_path]
    check_command_args(cmd)
    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            shell=False,
            env=os.environ,
        )
    except Exception as exc:
        raise PublishError(CATEGORY_REMOTE_ERROR, "Failed to spawn rclone cat process") from exc

    chunks = []
    total_bytes = 0
    h = hashlib.sha256()
    chunk_size = 65536

    try:
        while True:
            chunk = proc.stdout.read(chunk_size)
            if not chunk:
                break
            total_bytes += len(chunk)
            if total_bytes > MAX_FILE_BYTES:
                proc.kill()
                proc.wait()
                raise PublishError(
                    CATEGORY_SOURCE_OVERSIZE,
                    f"Remote file exceeds {MAX_FILE_BYTES} bytes during streaming",
                )
            h.update(chunk)
            chunks.append(chunk)

        proc.stdout.close()
        proc.wait()
        if proc.returncode != 0:
            raise PublishError(CATEGORY_REMOTE_ERROR, f"rclone cat failed with exit code {proc.returncode}")
    finally:
        if proc.stdout and not proc.stdout.closed:
            proc.stdout.close()
        if proc.poll() is None:
            proc.kill()
            proc.wait()

    data = b"".join(chunks)
    return data, h.hexdigest()


def handle_conflict(
    rclone_bin: str,
    state_dir: Path,
    remote_prefix: str,
    rel_path: str,
    remote_bytes: bytes,
    remote_sha: str,
) -> None:
    """
    Preserves remote conflict file:
    1. Writes to state-dir/conflicts/UUID/relativepath with mode 0600.
    2. Uploads to remotePrefix/.sync-conflicts/UUID/relativepath via rclone copyto --immutable.
    3. Reads back full content via rclone cat and verifies SHA256 roundtrip.
    """
    conflict_uuid = str(uuid.uuid4())
    local_conflict_path = verify_safe_path(state_dir, f"conflicts/{conflict_uuid}/{rel_path}")
    local_conflict_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    verify_no_symlinks(local_conflict_path.parent)

    try:
        fd = os.open(local_conflict_path,
                     os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, 'O_NOFOLLOW', 0), 0o600)
    except OSError as exc:
        raise PublishError(CATEGORY_PATH_SAFETY_ERROR, "Cannot safely create local conflict file") from exc
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(remote_bytes)
            f.flush()
            os.fsync(f.fileno())
    except Exception as exc:
        raise PublishError(CATEGORY_REMOTE_ERROR, "Failed to write local conflict file") from exc

    remote_conflict_dest = join_remote(remote_prefix, f".sync-conflicts/{conflict_uuid}/{rel_path}")
    copy_cmd = [rclone_bin, "copyto", "--immutable", str(local_conflict_path), remote_conflict_dest]
    proc = run_rclone_cmd(copy_cmd)
    if proc.returncode != 0:
        raise PublishError(
            CATEGORY_REMOTE_ERROR,
            f"rclone copyto conflict failed with exit code {proc.returncode}",
        )

    _, readback_sha = cat_remote_file(rclone_bin, remote_conflict_dest)
    if readback_sha != remote_sha:
        raise PublishError(
            CATEGORY_VERIFY_ERROR,
            f"Conflict upload SHA verification mismatch on {remote_conflict_dest}",
        )


def upload_file_with_precheck(
    rclone_bin: str,
    state_dir: Path,
    remote_prefix: str,
    rel_path: str,
    snapshot_bytes: bytes,
    snapshot_sha: str,
    initial_exists: bool,
    initial_sha: str | None,
) -> None:
    """
    Executes pre-upload verification against initial inspection state, writes snapshot bytes
    to a 0600 temp file in state_dir, uploads via rclone copyto, and verifies roundtrip SHA256 via cat.
    Temp file is cleaned up in a finally block.
    """
    remote_dest_path = join_remote(remote_prefix, rel_path)

    # Pre-upload verification
    current_meta = check_remote_file(rclone_bin, remote_dest_path)
    if initial_exists:
        if current_meta is None:
            raise PublishError(CATEGORY_PRECONDITION_FAILED, f"Remote file disappeared before upload: {rel_path}")
        _, current_sha = cat_remote_file(rclone_bin, remote_dest_path)
        if current_sha != initial_sha:
            raise PublishError(CATEGORY_PRECONDITION_FAILED, f"Remote file changed before upload: {rel_path}")
    else:
        if current_meta is not None:
            raise PublishError(CATEGORY_PRECONDITION_FAILED, f"Remote file appeared before upload: {rel_path}")

    # Write snapshot bytes to 0600 temp file in state_dir
    fd, temp_path_str = tempfile.mkstemp(prefix=".tmp_upload_", dir=state_dir)
    temp_path = Path(temp_path_str)
    try:
        if hasattr(os, "fchmod"):
            os.fchmod(fd, 0o600)
        else:
            os.chmod(temp_path, 0o600)

        with os.fdopen(fd, "wb") as f:
            f.write(snapshot_bytes)
            f.flush()
            os.fsync(f.fileno())

        copy_cmd = [rclone_bin, "copyto", str(temp_path), remote_dest_path]
        proc = run_rclone_cmd(copy_cmd)
        if proc.returncode != 0:
            raise PublishError(
                CATEGORY_REMOTE_ERROR,
                f"rclone copyto failed with exit code {proc.returncode}",
            )

        # Full cat verification
        _, readback_sha = cat_remote_file(rclone_bin, remote_dest_path)
        if readback_sha != snapshot_sha:
            raise PublishError(
                CATEGORY_VERIFY_ERROR,
                f"Readback SHA mismatch after upload for {rel_path}",
            )
    finally:
        if temp_path.exists():
            try:
                temp_path.unlink()
            except OSError:
                pass


def publish_cycle(exports_dir: Path, state_dir: Path, remote_prefix: str, rclone_bin: str) -> dict:
    """Publish and verify the GUI artifact, then commit a GUI-only receipt/state."""
    join_remote(remote_prefix, PERMITTED_FILES[0])
    verify_state_dir(state_dir)

    with PublishLock(state_dir):
        # 1. Snapshot source files into memory
        snapshots = snapshot_source_files(exports_dir)

        # 2. Load existing state
        old_state, old_state_bytes = _load_state_with_bytes(state_dir, remote_prefix)
        old_files = old_state.get("files", {}) if old_state else {}

        # 3. Inspect remote status for the GUI only
        file_plans = {}
        for rel_path, snap in snapshots.items():
            remote_path = join_remote(remote_prefix, rel_path)
            meta = check_remote_file(rclone_bin, remote_path)
            if meta is not None:
                remote_bytes, remote_sha = cat_remote_file(rclone_bin, remote_path)
                exists = True
            else:
                remote_bytes, remote_sha = None, None
                exists = False

            source_sha = snap["sha256"]
            if exists and remote_sha == source_sha:
                # Byte identical: skip upload and adopt in state
                file_plans[rel_path] = {
                    "action": "skipped",
                    "initial_exists": exists,
                    "initial_sha": remote_sha,
                    "target_sha": source_sha,
                    "bytes": snap["size"],
                    "conflict_saved": False,
                }
            else:
                # Needs upload. Evaluate conflict condition:
                # If state has no record of file while remote exists, OR remote SHA differs from known state SHA
                need_conflict_save = False
                if exists:
                    if old_state is None or rel_path not in old_files or remote_sha != old_files[rel_path]:
                        need_conflict_save = True

                file_plans[rel_path] = {
                    "action": "uploaded",
                    "initial_exists": exists,
                    "initial_sha": remote_sha,
                    "remote_bytes": remote_bytes,
                    "target_sha": source_sha,
                    "bytes": snap["size"],
                    "need_conflict_save": need_conflict_save,
                    "conflict_saved": False,
                }

        # 4. Handle remote conflicts before modifying destination
        for rel_path, plan in file_plans.items():
            if plan.get("need_conflict_save"):
                handle_conflict(
                    rclone_bin=rclone_bin,
                    state_dir=state_dir,
                    remote_prefix=remote_prefix,
                    rel_path=rel_path,
                    remote_bytes=plan["remote_bytes"],
                    remote_sha=plan["initial_sha"],
                )
                plan["conflict_saved"] = True

        # 5. Upload files requiring update with pre-check
        for rel_path, plan in file_plans.items():
            if plan["action"] == "uploaded":
                upload_file_with_precheck(
                    rclone_bin=rclone_bin,
                    state_dir=state_dir,
                    remote_prefix=remote_prefix,
                    rel_path=rel_path,
                    snapshot_bytes=snapshots[rel_path]["bytes"],
                    snapshot_sha=plan["target_sha"],
                    initial_exists=plan["initial_exists"],
                    initial_sha=plan["initial_sha"],
                )

        # 6. Preserve an old two-file state exactly before replacing it with the
        # new GUI-only state. The legacy XLSX hash is validated as state metadata
        # only; no XLSX source or remote path has been opened.
        retained_legacy_state = old_state.get("retained_legacy_state") if old_state else None
        if old_state is not None and "分析.xlsx" in old_files:
            if old_state_bytes is None:
                raise PublishError(CATEGORY_STATE_ERROR, "Legacy state bytes are unavailable")
            legacy_digest = hashlib.sha256(old_state_bytes).hexdigest()
            if retained_legacy_state is not None:
                if retained_legacy_state["sha256"] != legacy_digest or retained_legacy_state["bytes"] != len(old_state_bytes):
                    raise PublishError(CATEGORY_STATE_ERROR, "Retained legacy state pointer does not match source state")
            else:
                retained_legacy_state = _preserve_legacy_state(state_dir, old_state_bytes)

        # GUI verification succeeded: atomically commit the scoped state.
        new_files_hash = {rel_path: snapshots[rel_path]["sha256"] for rel_path in PERMITTED_FILES}
        save_state(
            state_dir,
            remote_prefix,
            new_files_hash,
            retained_legacy_state=retained_legacy_state,
        )

        # Build safe JSON receipt
        receipt_files = {}
        uploaded_count = 0
        skipped_count = 0
        conflicts_count = 0

        for rel_path in PERMITTED_FILES:
            plan = file_plans[rel_path]
            action = plan["action"]
            if plan.get("conflict_saved"):
                action = "conflict_uploaded"
                conflicts_count += 1
            if plan["action"] == "uploaded":
                uploaded_count += 1
            elif plan["action"] == "skipped":
                skipped_count += 1

            receipt_files[rel_path] = {
                "status": action,
                "sha256": plan["target_sha"],
                "bytes": plan["bytes"],
            }

        return {
            "status": "success",
            "scope": "gui_only",
            "full_database_synchronized": False,
            "files": receipt_files,
            "counts": {
                "total": len(PERMITTED_FILES),
                "uploaded": uploaded_count,
                "skipped": skipped_count,
                "conflicts": conflicts_count,
            },
        }


def parse_args(args=None):
    parser = argparse.ArgumentParser(
        description="Publish only the NAS GUI export (gui/index.html) to Google Drive via rclone."
    )
    parser.add_argument("--exports-dir", required=True, type=Path, help="Path to exports directory")
    parser.add_argument("--state-dir", required=True, type=Path, help="Path to state directory")
    parser.add_argument(
        "--remote",
        required=True,
        type=str,
        help="Rclone remote prefix (e.g. gdrive:ikaring3-exports)",
    )
    parser.add_argument(
        "--rclone-bin",
        default="rclone",
        type=str,
        help="Rclone executable path (default: rclone)",
    )
    return parser.parse_args(args)


def main(argv=None) -> int:
    try:
        args = parse_args(argv)
        receipt = publish_cycle(
            exports_dir=args.exports_dir,
            state_dir=args.state_dir,
            remote_prefix=args.remote,
            rclone_bin=args.rclone_bin,
        )
        print(json.dumps(receipt, indent=2, ensure_ascii=False))
        return 0
    except PublishError as exc:
        sys.stderr.write(f"{exc.category}\n")
        return 1
    except Exception:
        sys.stderr.write(f"{CATEGORY_INTERNAL_ERROR}\n")
        return 1


if __name__ == "__main__":
    sys.exit(main())
