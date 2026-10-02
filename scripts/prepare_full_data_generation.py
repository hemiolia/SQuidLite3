#!/usr/bin/env python3
"""同じ平文静止点から全情報の小分けSQLite・入口・xlsxを生成する。"""
import argparse
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import subprocess
import sys
import tempfile
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src/python'))
from ikarchive.slice_selectors import export_selectors, verify_selectors, data_files

GENERATION_ID = re.compile(r'[0-9]{8}T[0-9]{6}Z-[0-9a-f]{8}\Z')


def utcnow():
    return datetime.now(timezone.utc).isoformat()


def safe_path(path):
    if '..' in Path(path).parts:
        raise ValueError('parent traversal')
    path = Path(os.path.abspath(path))
    for parent in (path, *path.parents):
        if parent.is_symlink():
            raise ValueError('symlink path')
    return path


def file_digest(path):
    digest = hashlib.sha256()
    size = 0
    with open(path, 'rb') as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b''):
            size += len(chunk)
            digest.update(chunk)
    return {'bytes': size, 'sha256': digest.hexdigest()}


def atomic_json(path, data):
    fd, name = tempfile.mkstemp(prefix='.json-', dir=path.parent)
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as handle:
            json.dump(data, handle, ensure_ascii=False, sort_keys=True, indent=2)
            handle.write('\n')
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(name, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def create_snapshot(db, work):
    snapshots = work / 'snapshots'
    snapshots.mkdir(mode=0o700, exist_ok=True)
    result = subprocess.run([
        sys.executable, str(ROOT / 'scripts/verified_backup_support.py'),
        'plaintext-snapshot', str(db), str(snapshots),
    ], stdout=subprocess.PIPE, text=True, check=True)
    data = json.loads(result.stdout.splitlines()[-1])
    if data.get('status') != 'ok':
        raise RuntimeError('snapshot creation failed')
    return safe_path(data['raw_path']), safe_path(data['manifest_path'])


def preserve_preparation_history(generation):
    """Keep working receipts and retained attempts outside the strict package.

    Only this workflow's explicit metadata and retained-attempt names move.
    Unknown files remain in place so publisher inventory checks reject them.
    Current slices/XLSX and every source value stay at their verified paths.
    """
    history = safe_path(generation.parent / 'preparation-history' / generation.name)
    candidates = []
    for child in generation.iterdir():
        safe_path(child)
        if child.name in ('preparation-input.json', 'sqlite-preparation.json'):
            if not child.is_file():
                raise ValueError('preparation metadata is not a regular file')
            candidates.append(child)
        elif child.name.startswith(('xlsx-retained-', 'slices-retained-')):
            if not child.is_dir():
                raise ValueError('retained attempt is not a directory')
            # Retained trees are never followed through symbolic links.
            for current, dirs, files in os.walk(child, followlinks=False):
                for name in dirs + files:
                    safe_path(Path(current) / name)
            candidates.append(child)
    if candidates:
        history.mkdir(parents=True, mode=0o700, exist_ok=True)
    for child in candidates:
        target = history / child.name
        if target.exists():
            target = history / (child.name + '-retained-' + uuid.uuid4().hex[:8])
        child.rename(target)


def prepare_generation(snapshot, source_manifest, generation, generation_id, *, sqlite_only=False):
    """再実行時は同じ静止点を再検証し、完成した生成物を利用する。"""
    if not GENERATION_ID.fullmatch(generation_id):
        raise ValueError('invalid generation id')
    snapshot, source_manifest, generation = map(safe_path, (snapshot, source_manifest, generation))
    if not snapshot.is_file() or not source_manifest.is_file():
        raise ValueError('snapshot or manifest absent')
    for suffix in ('-wal', '-journal', '-shm'):
        sidecar = safe_path(str(snapshot) + suffix)
        if sidecar.exists() and sidecar.stat().st_size:
            raise ValueError('snapshot has an active sidecar')
    manifest = json.loads(source_manifest.read_text())
    raw = manifest.get('raw_snapshot', {})
    if (manifest.get('storage') != 'plaintext' or manifest.get('encryption') is not None
            or raw.get('basename') != snapshot.name or raw.get('quick_check') != 'ok'):
        raise ValueError('snapshot manifest invalid')
    if type(raw.get('bytes')) is not int or raw['bytes'] <= 0:
        raise ValueError('snapshot byte count invalid')
    verified = file_digest(snapshot)
    if verified != {key: raw.get(key) for key in ('bytes', 'sha256')}:
        raise ValueError('snapshot hash mismatch')
    generation.mkdir(parents=True, mode=0o700, exist_ok=True)
    linked = generation / 'source.sqlite3'
    if not linked.exists():
        # Keep the already immutable snapshot without a second 28GB raw copy.
        # Do not chmod the hard link: that would change the retained snapshot.
        os.link(snapshot, linked)
    elif not os.path.samefile(snapshot, linked):
        raise ValueError('generation bound to another snapshot')
    manifest_target = generation / 'source-manifest.json'
    source_bytes = source_manifest.read_bytes()
    if manifest_target.exists():
        if manifest_target.read_bytes() != source_bytes:
            raise ValueError('generation source manifest changed')
    else:
        with manifest_target.open('xb') as handle:
            handle.write(source_bytes)
            handle.flush()
            os.fsync(handle.fileno())
    slices = generation / 'slices'
    connection = sqlite3.connect(linked.as_uri() + '?mode=ro&immutable=1', uri=True)
    connection.execute('PRAGMA query_only=ON')
    try:
        from ikarchive.lossless_sqlite import export_sqlite_shards, verify_sqlite_shards
        created_sqlite = not (slices / 'verification.json').is_file()
        if created_sqlite:
            attempt = generation / ('slices-attempt-' + uuid.uuid4().hex[:8])
            slice_manifest = export_sqlite_shards(connection, attempt, snapshot_id=generation_id)
            sqlite_verification = verify_sqlite_shards(connection, attempt, slice_manifest)
            sqlite_verification['source_sha256'] = verified['sha256']
            slice_manifest['source_sha256'] = verified['sha256']
            export_selectors(connection, attempt, slice_manifest, verified['sha256'])
            selector_verification = verify_selectors(connection, attempt, slice_manifest, verified['sha256'])
            atomic_json(attempt / 'manifest.json', slice_manifest)
            atomic_json(attempt / 'verification.json', sqlite_verification)
            atomic_json(attempt / 'selectors-verification.json', selector_verification)
            if slices.exists():
                slices.rename(generation / ('slices-retained-' + uuid.uuid4().hex[:8]))
            attempt.rename(slices)
        slice_manifest = json.loads((slices / 'manifest.json').read_text())
        sqlite_verification = json.loads((slices / 'verification.json').read_text())
        selector_verification = json.loads((slices / 'selectors-verification.json').read_text())
        if (slice_manifest.get('version') != 2 or slice_manifest.get('role') != 'lossless_sqlite_shards'
                or slice_manifest.get('snapshot_identifier') != generation_id
                or sqlite_verification.get('status') != 'verified'
                or sqlite_verification.get('source_sha256') != verified['sha256']
                or sqlite_verification.get('snapshot_identifier') != generation_id
                or any(sqlite_verification.get('coverage', {}).get(key) is not True for key in
                       ('all_tables', 'all_rows', 'all_columns', 'all_values', 'external_values'))):
            raise ValueError('lossless SQLite proof incomplete')
        # Resume receipts are accepted only after all current dependency bytes
        # and all mode/rule seed values are verified again.
        if not created_sqlite:
            verify_selectors(connection, slices, slice_manifest, verified['sha256'])
    finally:
        connection.close()
    counts = slice_manifest['counts']
    if counts['rule_files'] != counts['rule_mode_product']:
        raise ValueError('rule product incomplete')
    if sqlite_only:
        # A separate process may already be exporting XLSX from this immutable
        # source. Do not rename its working directory or start a duplicate job.
        receipt = {
            'status': 'sqlite_prepared', 'generation_id': generation_id,
            'source': verified, 'counts': counts,
            'sqlite_verification': sqlite_verification,
            'selector_verification': selector_verification,
        }
        atomic_json(generation / 'sqlite-preparation.json', receipt)
        return receipt
    xlsx = generation / 'xlsx'
    if not (xlsx / 'verification.json').is_file():
        # Retain an incomplete attempt for diagnosis, then build in a fresh dir.
        attempt = generation / ('xlsx-attempt-' + uuid.uuid4().hex[:8])
        subprocess.run([
            sys.executable, str(ROOT / 'scripts/export_full_xlsx.py'),
            '--db', str(linked), '--output', str(attempt),
            '--snapshot-id', generation_id,
        ], check=True)
        if xlsx.exists():
            xlsx.rename(generation / ('xlsx-retained-' + uuid.uuid4().hex[:8]))
        attempt.rename(xlsx)
    xlsx_index = json.loads((xlsx / 'index.json').read_text())
    xlsx_verification = json.loads((xlsx / 'verification.json').read_text())
    if xlsx_verification.get('status') != 'verified':
        raise ValueError('xlsx source verification absent')
    if (xlsx_index.get('row_identity_format') != 'source_rowid_column_v1'
            or xlsx_verification.get('source_rowids_verified') is not True):
        raise ValueError('xlsx source row identities not verified')
    if xlsx_index.get('snapshot_identifier') != generation_id:
        raise ValueError('xlsx bound to another snapshot')
    if (xlsx_verification.get('snapshot_identifier') != generation_id
            or xlsx_verification.get('snapshot_sha256') != verified['sha256']
            or xlsx_verification.get('source', {}).get('bytes') != verified['bytes']):
        raise ValueError('xlsx source identity mismatch')
    files = []

    def add(local, remote, known=None):
        path = safe_path(generation / local)
        path.relative_to(generation)
        entry = known or file_digest(path)
        files.append({'local': local, 'remote': remote, **entry})

    add('source.sqlite3', f'unified/generations/{generation_id}/archive.sqlite3', verified)
    add('source-manifest.json', f'unified/generations/{generation_id}/source-manifest.json')
    for row in data_files(slice_manifest) + slice_manifest['by_mode'] + slice_manifest['by_rule']:
        add('slices/' + row['file'], f'slices/generations/{generation_id}/' + row['file'])
    add('slices/manifest.json', f'slices/generations/{generation_id}/manifest.json')
    add('slices/verification.json', f'slices/generations/{generation_id}/verification.json')
    add('slices/selectors-verification.json', f'slices/generations/{generation_id}/selectors-verification.json')
    for piece in xlsx_index['pieces']:
        add('xlsx/' + piece['name'], f'xlsx-full/generations/{generation_id}/' + piece['name'],
            {key: piece[key] for key in ('bytes', 'sha256')})
    add('xlsx/index.json', f'xlsx-full/generations/{generation_id}/index.json')
    add('xlsx/verification.json', f'xlsx-full/generations/{generation_id}/verification.json')
    add('xlsx/manifest.json', f'xlsx-full/generations/{generation_id}/manifest.json')
    plan = {
        'version': 1, 'generation_id': generation_id,
        'captured_at': manifest.get('captured_at'),
        'captured_at_kind': manifest.get('captured_at_kind', 'unknown_legacy_snapshot'),
        'prepared_at': utcnow(), 'source': verified, 'files': files,
        'counts': {**counts, 'xlsx_pieces': len(xlsx_index['pieces'])},
        'xlsx_verification': xlsx_verification,
        'sqlite_verification': sqlite_verification,
        'sqlite_verification_path': 'slices/verification.json',
        'selector_verification': selector_verification,
    }
    preserve_preparation_history(generation)
    atomic_json(generation / 'generation-plan.json', plan)
    return plan


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument('--db', type=Path)
    source.add_argument('--snapshot', type=Path)
    parser.add_argument('--source-manifest', type=Path)
    parser.add_argument('--work-dir', type=Path, required=True)
    parser.add_argument('--generation-id')
    parser.add_argument('--sqlite-only', action='store_true')
    args = parser.parse_args()
    work = safe_path(args.work_dir)
    work.mkdir(mode=0o700, parents=True, exist_ok=True)
    with (work / '.prepare.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if args.db:
            snapshot, manifest = create_snapshot(safe_path(args.db), work)
        else:
            if not args.source_manifest:
                parser.error('--snapshot requires --source-manifest')
            snapshot, manifest = args.snapshot, args.source_manifest
        generation_id = args.generation_id or datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '-' + uuid.uuid4().hex[:8]
        plan = prepare_generation(snapshot, manifest, work / generation_id, generation_id,
                                  sqlite_only=args.sqlite_only)
        print(json.dumps({'generation_id': generation_id, 'counts': plan['counts'],
                          'status': 'sqlite_prepared' if args.sqlite_only else 'prepared'}))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
