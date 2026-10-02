"""Process-local cache for resolving Google Drive folders by parent ID.

The caller owns rclone execution, network retries, and publication. The
``stat_directory_callback(remote_path, parent_folder_id)`` must return the
remote's ``lsjson --stat`` object, or ``None`` when that directory is missing.
Top-level directories are statted by their complete remote path. Descendants
are statted by basename below an already verified parent ID. The configured
remote alias itself is never statted because Drive root metadata may omit ID.
"""

from __future__ import annotations

import re
import threading
from typing import Any, Callable, NoReturn


_REMOTE_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]*\Z", re.ASCII)
_PATH_SEGMENT = re.compile(r"[A-Za-z0-9_.-]+\Z", re.ASCII)
_FOLDER_ID = re.compile(r"[A-Za-z0-9_-]{1,256}\Z", re.ASCII)


class DriveFolderCache:
    """Resolve and revalidate directory IDs without retaining file metadata.

    ``route('remote:dir/subdir/file.bin')`` returns
    ``('remote:file.bin', subdir_folder_id)`` when every directory is found.
    If a directory is absent, it returns the original full remote path and
    ``None`` so the caller can use its normal path-based upload behavior. A
    file directly below ``remote:`` is unchanged and has no cached folder ID.
    Missing directories are never cached.
    """

    def __init__(
        self,
        stat_directory_callback: Callable[[str, str | None], dict[str, Any] | None],
        error_factory: Callable[[str], BaseException],
    ) -> None:
        if not callable(stat_directory_callback) or not callable(error_factory):
            raise TypeError("callbacks must be callable")
        self._stat_directory = stat_directory_callback
        self._error_factory = error_factory
        # Keys are complete directory paths including the remote, e.g.
        # ``drive:database`` and ``drive:database/generations``. The remote
        # alias/root is not cached because it need not expose a usable ID.
        self._folders: dict[str, dict[str, Any]] = {}
        # Folder discovery can happen from concurrent artifact workers. Keep
        # lookup and final binding validation atomic with respect to each other;
        # copyto/cat operations happen after route() returns and never hold it.
        self._lock = threading.RLock()

    def _fail(self, category: str) -> NoReturn:
        error = self._error_factory(category)
        if not isinstance(error, BaseException):
            raise TypeError("error_factory must return an exception")
        raise error

    @staticmethod
    def _parse_file_remote_path(value: str) -> tuple[str, list[str]]:
        if not isinstance(value, str) or value.count(":") != 1:
            raise ValueError("REMOTE_PATH_INVALID")
        remote, relative = value.split(":", 1)
        if not _REMOTE_NAME.fullmatch(remote) or not relative:
            raise ValueError("REMOTE_PATH_INVALID")
        if relative.startswith("/") or relative.endswith("/") or "\\" in relative:
            raise ValueError("REMOTE_PATH_INVALID")
        parts = relative.split("/")
        if any(
            part in ("", ".", "..") or not _PATH_SEGMENT.fullmatch(part)
            for part in parts
        ):
            raise ValueError("REMOTE_PATH_INVALID")
        return remote, parts

    def _folder_id(self, metadata: Any, category: str) -> str:
        if (not isinstance(metadata, dict) or metadata.get("IsDir") is not True
                or not isinstance(metadata.get("ID"), str)
                or not _FOLDER_ID.fullmatch(metadata["ID"])):
            self._fail(category)
        return metadata["ID"]

    @staticmethod
    def _remote_path(remote: str, parts: list[str]) -> str:
        return remote + ":" + "/".join(parts)

    def _resolve_folder(
        self,
        remote: str,
        parts: list[str],
        parent_key: str | None,
        parent_folder_id: str | None,
        lookup_path: str,
    ) -> tuple[str, str | None]:
        key = self._remote_path(remote, parts)
        cached = self._folders.get(key)
        if cached is not None:
            return key, cached["folder_id"]

        metadata = self._stat_directory(lookup_path, parent_folder_id)
        if metadata is None:
            return key, None
        folder_id = self._folder_id(metadata, "REMOTE_DIRECTORY_INVALID")
        self._folders[key] = {
            "key": key,
            "remote": remote,
            "remote_path": key,
            "lookup_path": lookup_path,
            "parent_key": parent_key,
            "folder_id": folder_id,
            "depth": len(parts),
        }
        return key, folder_id

    def route(self, file_remote_path: str) -> tuple[str, str | None]:
        """Return a basename-only remote path plus its containing folder ID.

        Missing directories use the unchanged input path and ``None``. Callback
        exceptions and invalid metadata remain visible.
        """
        with self._lock:
            return self._route_locked(file_remote_path)

    def _route_locked(self, file_remote_path: str) -> tuple[str, str | None]:
        try:
            remote, parts = self._parse_file_remote_path(file_remote_path)
        except ValueError:
            self._fail("REMOTE_PATH_INVALID")

        directories = parts[:-1]
        if not directories:
            return file_remote_path, None

        prefix: list[str] = []
        parent_key = None
        parent_id = None
        for segment in directories:
            prefix.append(segment)
            lookup_path = (
                self._remote_path(remote, prefix)
                if parent_key is None
                else self._remote_path(remote, [segment])
            )
            current_key, folder_id = self._resolve_folder(
                remote, prefix, parent_key, parent_id, lookup_path
            )
            if folder_id is None:
                return file_remote_path, None
            parent_key, parent_id = current_key, folder_id

        return self._remote_path(remote, [parts[-1]]), parent_id

    def verify_bindings(self) -> None:
        """Re-stat every cached folder without following changed IDs.

        Directories are verified root-to-leaf. Each child lookup uses the ID
        returned by its parent during this verification pass. Any missing,
        renamed, moved, non-directory, or unsafe-ID binding fails closed.
        """
        with self._lock:
            self._verify_bindings_locked()

    def _verify_bindings_locked(self) -> None:
        ordered = sorted(
            self._folders.values(),
            key=lambda item: (item["depth"], item["remote"], item["remote_path"]),
        )
        verified_ids: dict[str, str] = {}
        for entry in ordered:
            parent_key = entry["parent_key"]
            if parent_key is None:
                parent_id = None
            else:
                parent_id = verified_ids.get(parent_key)
                if parent_id is None:
                    self._fail("REMOTE_DIRECTORY_BINDING_CHANGED")
            metadata = self._stat_directory(entry["lookup_path"], parent_id)
            if metadata is None:
                self._fail("REMOTE_DIRECTORY_BINDING_CHANGED")
            folder_id = self._folder_id(metadata, "REMOTE_DIRECTORY_BINDING_CHANGED")
            if folder_id != entry["folder_id"]:
                self._fail("REMOTE_DIRECTORY_BINDING_CHANGED")
            verified_ids[entry["key"]] = folder_id


__all__ = ["DriveFolderCache"]
