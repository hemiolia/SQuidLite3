"""Process-local proof that an explicit set of immutable files was SHA-256 checked.

This module covers only the listed files and their known parent directories. It
does not establish package inventory, sidecar absence, receipt validity, or the
meaning of any file contents; callers must verify those separately.
"""

from __future__ import annotations

from contextlib import contextmanager
import errno
import hashlib
import os
from pathlib import Path, PurePosixPath
import re
import stat
from types import MappingProxyType
from typing import Any, Iterator, Mapping


_READ_BYTES = 1024 * 1024
_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
_TOKEN_SEAL = object()
_FINGERPRINT_FIELDS = ("st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns")


class VerifiedFilesError(ValueError):
    """A file-set verification failure with a stable, non-sensitive category."""

    def __init__(self, category: str):
        super().__init__(category)
        self.category = category


def _fail(category: str) -> VerifiedFilesError:
    return VerifiedFilesError(category)


def _fingerprint(info: os.stat_result) -> tuple[int, int, int, int, int]:
    return tuple(int(getattr(info, field)) for field in _FINGERPRINT_FIELDS)  # type: ignore[return-value]


def _canonical_root(root: str | os.PathLike[str]) -> Path:
    try:
        raw = os.fspath(root)
        if not isinstance(raw, str) or not raw or "\x00" in raw:
            raise _fail("VERIFIED_FILES_ROOT_INVALID")
        supplied = Path(raw).expanduser()
        if ".." in supplied.parts:
            raise _fail("VERIFIED_FILES_ROOT_INVALID")
        if not supplied.is_absolute():
            supplied = Path.cwd() / supplied
        absolute = Path(os.path.abspath(os.fspath(supplied)))
    except (OSError, RuntimeError, TypeError) as exc:
        raise _fail("VERIFIED_FILES_ROOT_INVALID") from exc
    _check_root_components(absolute, "VERIFIED_FILES_ROOT_INVALID")
    try:
        info = os.lstat(absolute)
    except OSError as exc:
        raise _fail("VERIFIED_FILES_ROOT_INVALID") from exc
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
        raise _fail("VERIFIED_FILES_ROOT_INVALID")
    return absolute


def _check_root_components(root: Path, category: str) -> None:
    """Reject symlinks in the absolute root path, including ancestors."""
    for component in reversed((root, *root.parents)):
        try:
            info = os.lstat(component)
        except OSError as exc:
            raise _fail(category) from exc
        if stat.S_ISLNK(info.st_mode):
            raise _fail("VERIFIED_FILES_SYMLINK")
        if not stat.S_ISDIR(info.st_mode):
            raise _fail(category)


def _canonical_relative(relative: Any) -> str:
    if not isinstance(relative, str) or not relative or "\x00" in relative or "\\" in relative:
        raise _fail("VERIFIED_FILES_RELATIVE_PATH_INVALID")
    if relative.startswith("/") or relative.endswith("/"):
        raise _fail("VERIFIED_FILES_RELATIVE_PATH_INVALID")
    parts = relative.split("/")
    if any(part in ("", ".", "..") for part in parts):
        raise _fail("VERIFIED_FILES_RELATIVE_PATH_INVALID")
    parsed = PurePosixPath(relative)
    if parsed.is_absolute() or parsed.as_posix() != relative:
        raise _fail("VERIFIED_FILES_RELATIVE_PATH_INVALID")
    return relative


def _normalize_records(records: Mapping[str, Mapping[str, Any]]) -> dict[str, tuple[int, str]]:
    if not isinstance(records, Mapping):
        raise _fail("VERIFIED_FILES_RECORDS_INVALID")
    normalized: dict[str, tuple[int, str]] = {}
    try:
        items = records.items()
    except Exception as exc:
        raise _fail("VERIFIED_FILES_RECORDS_INVALID") from exc
    for raw_relative, record in items:
        relative = _canonical_relative(raw_relative)
        if relative in normalized or not isinstance(record, Mapping):
            raise _fail("VERIFIED_FILES_RECORDS_INVALID")
        size = record.get("bytes")
        digest = record.get("sha256")
        if type(size) is not int or size < 0:
            raise _fail("VERIFIED_FILES_RECORDS_INVALID")
        if not isinstance(digest, str) or not _SHA256_RE.fullmatch(digest):
            raise _fail("VERIFIED_FILES_RECORDS_INVALID")
        normalized[relative] = (size, digest)
    return normalized


def _path_for(root: Path, relative: str) -> Path:
    return root.joinpath(*relative.split("/"))


def _parent_relative_paths(relative: str) -> tuple[str, ...]:
    parts = relative.split("/")[:-1]
    parents = [""]
    for index in range(1, len(parts) + 1):
        parents.append("/".join(parts[:index]))
    return tuple(parents)


def _directory_fingerprint(root: Path, relative: str, category: str) -> tuple[int, int, int, int, int]:
    path = root if not relative else _path_for(root, relative)
    try:
        info = os.lstat(path)
    except OSError as exc:
        raise _fail(category) from exc
    if stat.S_ISLNK(info.st_mode):
        raise _fail("VERIFIED_FILES_SYMLINK")
    if not stat.S_ISDIR(info.st_mode):
        raise _fail(category)
    return _fingerprint(info)


def _capture_directories(root: Path, relative_paths: tuple[str, ...]) -> dict[str, tuple[int, int, int, int, int]]:
    _check_root_components(root, "VERIFIED_FILES_CHANGED")
    relatives = {""}
    for relative in relative_paths:
        relatives.update(_parent_relative_paths(relative))
    ordered = sorted(relatives, key=lambda item: (item.count("/"), item))
    return {relative: _directory_fingerprint(root, relative, "VERIFIED_FILES_CHANGED")
            for relative in ordered}


def _assert_directories(
    root: Path,
    expected: Mapping[str, tuple[int, int, int, int, int]],
    *,
    category: str = "VERIFIED_FILES_CHANGED",
) -> None:
    _check_root_components(root, category)
    for relative, fingerprint in expected.items():
        if _directory_fingerprint(root, relative, category) != fingerprint:
            raise _fail(category)


def _file_lstat(path: Path, category: str) -> tuple[int, int, int, int, int]:
    try:
        info = os.lstat(path)
    except OSError as exc:
        raise _fail(category) from exc
    if stat.S_ISLNK(info.st_mode):
        raise _fail("VERIFIED_FILES_SYMLINK")
    if not stat.S_ISREG(info.st_mode):
        raise _fail(category)
    return _fingerprint(info)


def _open_flags() -> int:
    nofollow = getattr(os, "O_NOFOLLOW", None)
    if nofollow is None:
        raise _fail("VERIFIED_FILES_NOFOLLOW_UNAVAILABLE")
    return os.O_RDONLY | nofollow | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NONBLOCK", 0)


def _open_checked(path: Path, expected: tuple[int, int, int, int, int] | None = None) -> tuple[int, tuple[int, int, int, int, int]]:
    try:
        descriptor = os.open(path, _open_flags())
    except VerifiedFilesError:
        raise
    except OSError as exc:
        if exc.errno == errno.ELOOP:
            raise _fail("VERIFIED_FILES_SYMLINK") from exc
        raise _fail("VERIFIED_FILES_CHANGED") from exc
    try:
        fd_info = os.fstat(descriptor)
        path_fingerprint = _file_lstat(path, "VERIFIED_FILES_CHANGED")
        fd_fingerprint = _fingerprint(fd_info)
        if not stat.S_ISREG(fd_info.st_mode) or fd_fingerprint != path_fingerprint:
            raise _fail("VERIFIED_FILES_CHANGED")
        if expected is not None and fd_fingerprint != expected:
            raise _fail("VERIFIED_FILES_CHANGED")
        return descriptor, fd_fingerprint
    except BaseException:
        os.close(descriptor)
        raise


def _hash_descriptor(descriptor: int) -> tuple[int, str]:
    os.lseek(descriptor, 0, os.SEEK_SET)
    digest = hashlib.sha256()
    byte_count = 0
    while True:
        block = os.read(descriptor, _READ_BYTES)
        if not block:
            break
        byte_count += len(block)
        digest.update(block)
    return byte_count, digest.hexdigest()


def _assert_file_path(
    root: Path,
    relative: str,
    expected: tuple[int, int, int, int, int],
    directories: Mapping[str, tuple[int, int, int, int, int]],
    category: str,
) -> None:
    _check_root_components(root, category)
    for parent in _parent_relative_paths(relative):
        parent_expected = directories.get(parent)
        if parent_expected is None or _directory_fingerprint(root, parent, category) != parent_expected:
            raise _fail(category)
    if _file_lstat(_path_for(root, relative), category) != expected:
        raise _fail(category)


def verify_files(
    root: str | os.PathLike[str],
    records: Mapping[str, Mapping[str, Any]],
) -> "VerifiedFiles":
    """Hash an explicit file set once and return a process-local reuse token.

    Records may contain additional metadata fields; only exact ``bytes`` and
    lowercase ``sha256`` values are bound. Unknown files and sidecars are the
    caller's inventory responsibility.
    """
    root_path = _canonical_root(root)
    normalized = _normalize_records(records)
    relatives = tuple(sorted(normalized))
    directories = _capture_directories(root_path, relatives)

    # Capture all listed path identities before starting any full-file hash.
    file_fingerprints: dict[str, tuple[int, int, int, int, int]] = {}
    for relative in relatives:
        _assert_directories(root_path, directories)
        file_fingerprints[relative] = _file_lstat(
            _path_for(root_path, relative), "VERIFIED_FILES_CHANGED"
        )

    # Hash files one at a time so the token does not require one open descriptor
    # per shard. Each descriptor is checked against the earlier path snapshot.
    for relative in relatives:
        _assert_directories(root_path, directories)
        path = _path_for(root_path, relative)
        descriptor, before = _open_checked(path, file_fingerprints[relative])
        try:
            byte_count, digest = _hash_descriptor(descriptor)
            after_fd = os.fstat(descriptor)
            after_path = _file_lstat(path, "VERIFIED_FILES_CHANGED")
            if (_fingerprint(after_fd) != before or after_path != before
                    or byte_count != before[2]):
                raise _fail("VERIFIED_FILES_CHANGED")
            expected_bytes, expected_sha = normalized[relative]
            if byte_count != expected_bytes or digest != expected_sha:
                raise _fail("VERIFIED_FILES_HASH_MISMATCH")
        except VerifiedFilesError:
            raise
        except OSError as exc:
            raise _fail("VERIFIED_FILES_CHANGED") from exc
        finally:
            os.close(descriptor)

    # Compare against the initial directory snapshot, not a new per-file
    # baseline, so a parent changed and restored during hashing is detected.
    _assert_directories(root_path, directories)
    for relative in relatives:
        _assert_file_path(
            root_path, relative, file_fingerprints[relative], directories,
            "VERIFIED_FILES_CHANGED",
        )

    return VerifiedFiles._create(
        _TOKEN_SEAL, root_path, normalized, directories, file_fingerprints
    )


class VerifiedFiles:
    """Opaque same-process proof for one exact root and explicit file set."""

    __slots__ = ("_seal", "_root", "_records", "_directories", "_files")

    def __init__(self, *_args: Any, **_kwargs: Any):
        raise TypeError("VerifiedFiles tokens are created only by verify_files()")

    def __init_subclass__(cls, **_kwargs: Any):
        raise TypeError("VerifiedFiles cannot be subclassed")

    def __setattr__(self, _name: str, _value: Any) -> None:
        raise AttributeError("VerifiedFiles tokens are immutable")

    def __delattr__(self, _name: str) -> None:
        raise AttributeError("VerifiedFiles tokens are immutable")

    def __copy__(self):
        raise TypeError("VerifiedFiles tokens cannot be copied")

    def __deepcopy__(self, _memo):
        raise TypeError("VerifiedFiles tokens cannot be copied")

    def __reduce__(self):
        raise TypeError("VerifiedFiles tokens are process-local and cannot be serialized")

    def __reduce_ex__(self, _protocol):
        raise TypeError("VerifiedFiles tokens are process-local and cannot be serialized")

    @classmethod
    def _create(cls, seal: object, root: Path,
                records: Mapping[str, tuple[int, str]],
                directories: Mapping[str, tuple[int, int, int, int, int]],
                files: Mapping[str, tuple[int, int, int, int, int]]) -> "VerifiedFiles":
        if seal is not _TOKEN_SEAL or cls is not VerifiedFiles:
            raise TypeError("VerifiedFiles tokens are created only by verify_files()")
        token = object.__new__(cls)
        object.__setattr__(token, "_seal", seal)
        object.__setattr__(token, "_root", str(root))
        object.__setattr__(token, "_records", MappingProxyType(dict(records)))
        object.__setattr__(token, "_directories", MappingProxyType(dict(directories)))
        object.__setattr__(token, "_files", MappingProxyType(dict(files)))
        return token

    def _authenticate(self) -> None:
        if type(self) is not VerifiedFiles:
            raise TypeError("unrecognized VerifiedFiles token")
        try:
            seal = object.__getattribute__(self, "_seal")
        except AttributeError as exc:
            raise TypeError("unrecognized VerifiedFiles token") from exc
        if seal is not _TOKEN_SEAL:
            raise TypeError("unrecognized VerifiedFiles token")

    @property
    def root(self) -> Path:
        VerifiedFiles._authenticate(self)
        return Path(object.__getattribute__(self, "_root"))

    def assert_unchanged(self) -> None:
        """Check bound path/stat identities without rehashing file contents."""
        VerifiedFiles._authenticate(self)
        root = Path(object.__getattribute__(self, "_root"))
        directories = object.__getattribute__(self, "_directories")
        files = object.__getattribute__(self, "_files")
        _assert_directories(root, directories)
        for relative, fingerprint in files.items():
            _assert_file_path(root, relative, fingerprint, directories,
                              "VERIFIED_FILES_CHANGED")

    def assert_matches(
        self,
        root: str | os.PathLike[str],
        records: Mapping[str, Mapping[str, Any]],
    ) -> None:
        """Require the exact captured root and complete registered file set."""
        VerifiedFiles._authenticate(self)
        root_path = _canonical_root(root)
        normalized = _normalize_records(records)
        if (str(root_path) != object.__getattribute__(self, "_root")
                or normalized != dict(object.__getattribute__(self, "_records"))):
            raise _fail("VERIFIED_FILES_BINDING_MISMATCH")
        self.assert_unchanged()

    def derive(
        self,
        prefix: str,
        records: Mapping[str, Mapping[str, Any]],
    ) -> "VerifiedFiles":
        """Derive a no-rehash proof for a nonempty subset below one directory."""
        VerifiedFiles._authenticate(self)
        self.assert_unchanged()
        try:
            normalized_prefix = _canonical_relative(prefix)
            normalized = _normalize_records(records)
            if not normalized:
                raise _fail("VERIFIED_FILES_RECORDS_INVALID")

            parent_root = Path(object.__getattribute__(self, "_root"))
            child_root = _canonical_root(_path_for(parent_root, normalized_prefix))
            parent_records = object.__getattribute__(self, "_records")
            parent_directories = object.__getattribute__(self, "_directories")
            parent_files = object.__getattribute__(self, "_files")

            child_files: dict[str, tuple[int, int, int, int, int]] = {}
            child_directories: dict[str, tuple[int, int, int, int, int]] = {}
            for relative, record in normalized.items():
                parent_relative = f"{normalized_prefix}/{relative}"
                if (parent_records.get(parent_relative) != record
                        or parent_relative not in parent_files):
                    raise _fail("VERIFIED_FILES_BINDING_MISMATCH")
                child_files[relative] = parent_files[parent_relative]

                for child_parent in _parent_relative_paths(relative):
                    parent_parent = (normalized_prefix if not child_parent
                                     else f"{normalized_prefix}/{child_parent}")
                    fingerprint = parent_directories.get(parent_parent)
                    if fingerprint is None:
                        raise _fail("VERIFIED_FILES_BINDING_MISMATCH")
                    child_directories[child_parent] = fingerprint

            child = VerifiedFiles._create(
                _TOKEN_SEAL, child_root, normalized, child_directories, child_files
            )
            child.assert_unchanged()
            return child
        finally:
            # Keep the parent package bound across both validation and token
            # construction, including when validation itself fails.
            self.assert_unchanged()

    def checked_path(
        self,
        relative: str,
        record: Mapping[str, Any],
    ) -> Path:
        """Return one registered path after checking its current stat binding."""
        VerifiedFiles._authenticate(self)
        normalized_relative = _canonical_relative(relative)
        normalized_record = _normalize_records({normalized_relative: record})
        records = object.__getattribute__(self, "_records")
        expected_record = records.get(normalized_relative)
        if expected_record is None or normalized_record[normalized_relative] != expected_record:
            raise _fail("VERIFIED_FILES_RECORD_MISMATCH")
        root = Path(object.__getattribute__(self, "_root"))
        directories = object.__getattribute__(self, "_directories")
        files = object.__getattribute__(self, "_files")
        _assert_file_path(root, normalized_relative, files[normalized_relative],
                          directories, "VERIFIED_FILES_CHANGED")
        return _path_for(root, normalized_relative)

    @contextmanager
    def guard(self) -> Iterator["VerifiedFiles"]:
        """Check the whole captured set before and after a protected operation."""
        VerifiedFiles.assert_unchanged(self)
        try:
            yield self
        finally:
            VerifiedFiles.assert_unchanged(self)
