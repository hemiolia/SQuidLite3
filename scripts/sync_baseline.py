"""Persistent common hashes for conservative three-way file synchronization."""

import json
import os
import re
import tempfile
from contextlib import contextmanager
from pathlib import Path


SHA256 = re.compile(r'[0-9a-f]{64}\Z')


@contextmanager
def sync_lock(root: Path):
    """Serialize NAS and Drive syncs across their full scan-to-commit window."""
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    path = root / '.sync-state.lock'
    if path.is_symlink():
        raise ValueError(f'Sync lock must not be a symlink: {path}')
    fd = os.open(path, os.O_CREAT | os.O_RDWR | getattr(os, 'O_NOFOLLOW', 0), 0o600)
    locked = False
    try:
        if os.name == 'nt':
            import msvcrt
            os.lseek(fd, 0, os.SEEK_SET)
            os.write(fd, b'0')
            os.lseek(fd, 0, os.SEEK_SET)
            try:
                msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
                locked = True
            except OSError as exc:
                raise RuntimeError(f'Another sync is running for {root}') from exc
        else:
            import fcntl
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                locked = True
            except BlockingIOError as exc:
                raise RuntimeError(f'Another sync is running for {root}') from exc
        yield
    finally:
        try:
            if os.name == 'nt' and locked:
                os.lseek(fd, 0, os.SEEK_SET)
                msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
        finally:
            os.close(fd)


def load_baseline(root: Path, name: str) -> dict:
    path = root / name
    if path.is_symlink():
        raise ValueError(f'Sync state must not be a symlink: {path}')
    if not path.exists():
        return {}
    state = json.loads(path.read_text(encoding='utf-8'))
    if not isinstance(state, dict) or state.get('schema_version') != 1 or not isinstance(state.get('hashes'), dict):
        raise ValueError(f'Malformed sync state: {path}')
    hashes = state['hashes']
    for rel, digest in hashes.items():
        if (not isinstance(rel, str) or not rel or Path(rel).is_absolute()
                or '..' in Path(rel).parts or str(Path(rel)) != rel
                or not isinstance(digest, str) or not SHA256.fullmatch(digest)):
            raise ValueError(f'Malformed sync state entry: {path}')
    return hashes


def merged_baseline(previous: dict, local_files: dict, remote_files: dict) -> dict:
    """Advance only files verified equal on both sides; retain divergent history."""
    present = set(local_files) | set(remote_files)
    result = {rel: digest for rel, digest in previous.items() if rel in present}
    for rel in set(local_files) & set(remote_files):
        local_hash = local_files[rel]['sha256']
        if local_hash is not None and local_hash == remote_files[rel]['sha256']:
            result[rel] = local_hash
    return result


def save_baseline(root: Path, name: str, hashes: dict) -> None:
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    target = root / name
    if target.is_symlink():
        raise ValueError(f'Sync state must not be a symlink: {target}')
    payload = (json.dumps({'schema_version': 1, 'hashes': hashes}, ensure_ascii=False,
                          sort_keys=True) + '\n').encode('utf-8')
    fd, temp_name = tempfile.mkstemp(prefix='.tmp_sync_state_', dir=root)
    try:
        if hasattr(os, 'fchmod'):
            os.fchmod(fd, 0o600)
        else:
            os.chmod(temp_name, 0o600)
        with os.fdopen(fd, 'wb') as out:
            out.write(payload)
            out.flush()
            os.fsync(out.fileno())
        os.replace(temp_name, target)
        if hasattr(os, 'O_DIRECTORY'):
            directory = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
    except BaseException:
        try:
            os.unlink(temp_name)
        except FileNotFoundError:
            pass
        raise


def destination_unchanged(current_hash: str, baseline_hash: str) -> bool:
    return baseline_hash is not None and current_hash == baseline_hash
