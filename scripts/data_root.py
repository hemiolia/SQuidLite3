"""Resolve local archive storage without creating directories.

An explicit IKARING_ARCHIVE_DATA_DIR keeps the distributed local workflow.
On macOS, the presence of the client marker selects the small NAS client root.
Marker validity is checked by the NAS command, never inferred here.
"""

import os
import sys
from pathlib import Path


def client_root() -> Path:
    return Path.home() / 'Library' / 'Application Support' / 'ikaring-archive' / 'client'


def legacy_root() -> Path:
    return Path.home() / 'Documents' / 'イカリング3アーカイブ'


def marker_path(root: Path) -> Path:
    return root / 'config' / 'storage-location.json'


def marker_present(root: Path) -> bool:
    marker = marker_path(root)
    return marker.exists() or marker.is_symlink()


def data_root() -> Path:
    if 'IKARING_ARCHIVE_DATA_DIR' in os.environ:
        value = os.environ['IKARING_ARCHIVE_DATA_DIR']
        if not value:
            raise ValueError('IKARING_ARCHIVE_DATA_DIR must not be empty')
        return Path(value).expanduser()
    if sys.platform == 'darwin' and marker_present(client_root()):
        return client_root()
    return legacy_root()


def client_mode(root: Path | None = None) -> bool:
    selected = data_root() if root is None else Path(root)
    return selected.expanduser().resolve() == client_root().resolve() and marker_present(client_root())
