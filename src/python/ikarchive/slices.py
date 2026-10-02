"""全情報分割の入口と更新要求。旧OMITS付き分析sliceの生成を廃止する。"""
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import tempfile
from datetime import datetime, timezone
import uuid

from .slice_selectors import MODE_SLICES, UNCLASSIFIED, rule_token as _rule_token
from .slice_selectors import export_selectors, verify_selectors, safe_path, digest


def _atomic_json(path, data):
    path = Path(path)
    for parent in (path, *path.parents):
        if parent.is_symlink():
            raise ValueError('SLICE_PATH_SYMLINK')
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix='.request-', dir=path.parent)
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as handle:
            json.dump(data, handle, ensure_ascii=False, sort_keys=True, indent=2)
            handle.write('\n')
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _ensure_dir(path):
    path = Path(path).absolute()
    for parent in (path, *path.parents):
        if parent.is_symlink():
            raise ValueError('SLICE_PATH_SYMLINK')
    missing = []
    cursor = path
    while not cursor.exists():
        missing.append(cursor)
        cursor = cursor.parent
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    for directory in (*missing, path):
        os.chmod(directory, 0o700)


def export_slices(source, destination):
    """読み取り静止点から全値断片と全参照入口を新しい世代へ保存する。

    巨大exportを収集syncのスレッドで実行しない。明示export専用である。
    """
    from .lossless_sqlite import export_sqlite_shards, verify_sqlite_shards
    source, destination = Path(source).absolute(), Path(destination).absolute()
    for path in (source, destination):
        for parent in (path, *path.parents):
            if parent.is_symlink():
                raise ValueError('SLICE_PATH_SYMLINK')
    if not source.is_file() or destination == source or destination in source.parents:
        raise ValueError('SLICE_SOURCE_OR_DEST_INVALID')
    _ensure_dir(destination)
    generation_id = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '-' + uuid.uuid4().hex[:8]
    generation = destination / 'generations' / generation_id
    _ensure_dir(generation)
    snapshot = generation / 'source.sqlite3'
    src = sqlite3.connect(source.as_uri() + '?mode=ro', uri=True, timeout=120)
    dst = sqlite3.connect(snapshot)
    try:
        src.execute('BEGIN')
        src.execute('PRAGMA page_count').fetchone()
        src.backup(dst)
    finally:
        dst.close()
        src.rollback()
        src.close()
    identity = digest(snapshot)
    connection = sqlite3.connect(snapshot.as_uri() + '?mode=ro&immutable=1', uri=True)
    try:
        if connection.execute('PRAGMA quick_check').fetchone()[0] != 'ok':
            raise ValueError('SLICE_SOURCE_INTEGRITY_FAILED')
        store = generation / 'data'
        manifest = export_sqlite_shards(connection, store, snapshot_id=generation_id)
        receipt = verify_sqlite_shards(connection, store, manifest)
        receipt['source_sha256'] = identity['sha256']
        manifest['source_sha256'] = identity['sha256']
        export_selectors(connection, store, manifest, identity['sha256'])
        selectors = verify_selectors(connection, store, manifest, identity['sha256'])
        _atomic_json(store / 'manifest.json', manifest)
        _atomic_json(store / 'verification.json', receipt)
        _atomic_json(store / 'selectors-verification.json', selectors)
    finally:
        connection.close()
    # This pointer is published last; retained generations are never overwritten.
    _atomic_json(destination / 'current-generation.json', {
        'version': 2, 'generation_id': generation_id, 'root': 'generations/' + generation_id + '/data',
        'source': identity, 'status': 'verified',
    })
    return {'status': 'verified', 'generation_id': generation_id, **manifest['counts']}


def refresh_slices(source, destination):
    """全情報更新を要求する。件数や値の合計だけで変更なしと判定しない。"""
    source, destination = Path(source).absolute(), Path(destination).absolute()
    for path in (source, destination):
        for parent in (path, *path.parents):
            if parent.is_symlink():
                raise ValueError('SLICE_PATH_SYMLINK')
    if not destination.exists():
        return {'slice_refresh': 'absent', 'updated': [], 'errors': []}
    _ensure_dir(destination)
    metadata = []
    for suffix in ('', '-wal', '-journal'):
        path = safe_path(source.parent, source.name + suffix)
        if path.exists():
            stat = path.stat()
            metadata.append({'suffix': suffix, 'bytes': stat.st_size, 'mtime_ns': stat.st_mtime_ns})
    request = {'version': 2, 'requested_at': datetime.now(timezone.utc).isoformat(),
               'request_id': uuid.uuid4().hex, 'information_scope': 'all_tables_all_values',
               'source_state': metadata, 'reason': 'committed_collection_or_tag_update'}
    _atomic_json(destination / 'update-request.json', request)
    return {'slice_refresh': 'pending_full_generation', 'request_id': request['request_id'],
            'updated': [], 'errors': []}


def sync_tags_to_slices(database, account, match_key, tags):
    # A tag change is part of the same full generation, never an in-place
    # mutation of an immutable shared store or an old partial selector.
    result = refresh_slices(database, Path(database).absolute().parent / 'slices')
    return {'slice_sync': 'absent' if result['slice_refresh'] == 'absent' else 'pending_full_generation',
            'updated': [], 'errors': []}


def list_datasets(database):
    """旧partialファイルを完成したデータセットとして案内しない。"""
    database = Path(database).absolute()
    safe_path(database.parent, database.name)
    root = database.parent / 'slices'
    pointer = safe_path(root, 'current-generation.json')
    items = [{'token': 'unified', 'axis': 'unified'}]
    if not pointer.exists():
        old = safe_path(root, 'manifest.json')
        return {'items': items, 'partition_status': 'legacy_incomplete' if old.exists() else 'absent'}
    current = json.loads(pointer.read_text(encoding='utf-8'))
    if current.get('version') != 2 or current.get('status') != 'verified':
        raise ValueError('SLICE_GENERATION_INVALID')
    generation = safe_path(root, current['root'])
    manifest = json.loads(safe_path(generation, 'manifest.json').read_text(encoding='utf-8'))
    receipt = json.loads(safe_path(generation, 'verification.json').read_text(encoding='utf-8'))
    if (manifest.get('role') != 'lossless_sqlite_shards' or manifest.get('version') != 2
            or manifest.get('snapshot_identifier') != current['generation_id']
            or receipt.get('status') != 'verified'
            or receipt.get('source_sha256') != current['source']['sha256']):
        raise ValueError('SLICE_GENERATION_INVALID')
    # The current GUI reader cannot open the new multi-file store yet. Preserve
    # truthful availability while its shard reader is implemented separately.
    return {'items': items, 'partition_status': 'verified_store_reader_pending',
            'generation_id': current['generation_id'], 'counts': manifest['counts']}
