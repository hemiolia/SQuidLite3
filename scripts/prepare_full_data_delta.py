#!/usr/bin/env python3
"""NAS正本の確定静止点を、全値・全schemaを保持する変更世代へ束ねる。"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import sys
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src/python'))
sys.path.insert(0, str(ROOT / 'scripts'))
from prepare_full_data_generation import safe_path, file_digest, atomic_json, GENERATION_ID
from export_full_xlsx import export_full_xlsx
from ikarchive.change_feed import read_change_batch, CHANGE_TABLE
from ikarchive.reconciliation import read_reconciliation
from ikarchive.delta_transport import build_delta_database, verify_delta_database
from ikarchive.lossless_sqlite import export_sqlite_shards, verify_sqlite_shards, MIN_MAX_BYTES
from ikarchive.slice_selectors import data_files


def new_generation_id():
    return datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ-') + uuid.uuid4().hex[:8]


def _immutable(path):
    path = safe_path(path)
    if not path.is_file():
        raise ValueError('IMMUTABLE_SOURCE_MISSING')
    for suffix in ('-wal', '-shm', '-journal'):
        sidecar = safe_path(str(path) + suffix)
        if sidecar.exists() and sidecar.stat().st_size:
            raise ValueError('IMMUTABLE_SOURCE_HAS_SIDECAR')
    connection = sqlite3.connect(path.as_uri() + '?mode=ro&immutable=1', uri=True)
    connection.execute('PRAGMA query_only=ON')
    return connection


_BASELINE_HASH_CHUNK_BYTES = 1024 * 1024
_BASELINE_SIDECARS = ('-wal', '-shm', '-journal')
_SHA256_RE = re.compile(r'[0-9a-f]{64}\Z')
_TOKEN_SEAL = object()


def _fingerprint(info):
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def _open_regular_nofollow(path, error_code):
    try:
        absolute = safe_path(path)
        nofollow = getattr(os, 'O_NOFOLLOW', None)
        if nofollow is None:
            raise ValueError('O_NOFOLLOW is unavailable')
        flags = os.O_RDONLY | nofollow | getattr(os, 'O_CLOEXEC', 0)
        descriptor = os.open(absolute, flags)
        try:
            info = os.fstat(descriptor)
            if not stat.S_ISREG(info.st_mode):
                raise ValueError('not a regular file')
            fingerprint = _fingerprint(info)
            path_info = os.stat(absolute, follow_symlinks=False)
            if (not stat.S_ISREG(path_info.st_mode)
                    or _fingerprint(path_info) != fingerprint):
                raise ValueError('opened file no longer matches its path')
            safe_path(absolute)
            return absolute, descriptor, fingerprint
        except BaseException:
            os.close(descriptor)
            raise
    except (OSError, ValueError, TypeError) as error:
        raise ValueError(error_code) from error


def _read_regular_bytes(path, error_code):
    absolute, descriptor, fingerprint = _open_regular_nofollow(path, error_code)
    try:
        content = bytearray()
        while True:
            chunk = os.read(descriptor, _BASELINE_HASH_CHUNK_BYTES)
            if not chunk:
                break
            content.extend(chunk)
        _assert_fd_path_unchanged(absolute, descriptor, fingerprint, error_code)
        if len(content) != fingerprint[2]:
            raise ValueError(error_code)
        return bytes(content), fingerprint
    except (OSError, ValueError) as error:
        if str(error) == error_code:
            raise
        raise ValueError(error_code) from error
    finally:
        os.close(descriptor)


def _hash_fd(descriptor):
    os.lseek(descriptor, 0, os.SEEK_SET)
    digest = hashlib.sha256()
    size = 0
    while True:
        chunk = os.read(descriptor, _BASELINE_HASH_CHUNK_BYTES)
        if not chunk:
            break
        size += len(chunk)
        digest.update(chunk)
    return {'bytes': size, 'sha256': digest.hexdigest()}


def _assert_fd_path_unchanged(path, descriptor, expected, error_code):
    try:
        after_fd = os.fstat(descriptor)
        after_path = os.stat(path, follow_symlinks=False)
        safe_path(path)
        if (not stat.S_ISREG(after_fd.st_mode) or not stat.S_ISREG(after_path.st_mode)
                or _fingerprint(after_fd) != expected
                or _fingerprint(after_path) != expected):
            raise ValueError(error_code)
    except (OSError, ValueError) as error:
        if str(error) == error_code:
            raise
        raise ValueError(error_code) from error


def _check_baseline_sidecars(path, error_code):
    for suffix in _BASELINE_SIDECARS:
        sidecar = Path(str(path) + suffix)
        try:
            safe_path(sidecar)
            info = os.stat(sidecar, follow_symlinks=False)
        except FileNotFoundError:
            continue
        except (OSError, ValueError) as error:
            raise ValueError(error_code) from error
        if not stat.S_ISREG(info.st_mode) or info.st_size:
            raise ValueError(error_code)


def _manifest_source_digest(manifest_bytes, source_path):
    try:
        document = json.loads(manifest_bytes)
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError) as error:
        raise ValueError('BASELINE_MANIFEST_INVALID') from error
    if not isinstance(document, dict):
        raise ValueError('BASELINE_MANIFEST_INVALID')
    raw = document.get('raw_snapshot')
    if (document.get('storage') != 'plaintext' or document.get('encryption') is not None
            or not isinstance(raw, dict) or raw.get('basename') != source_path.name
            or raw.get('quick_check') != 'ok'
            or type(raw.get('bytes')) is not int or raw['bytes'] <= 0
            or not isinstance(raw.get('sha256'), str)
            or not _SHA256_RE.fullmatch(raw['sha256'])):
        raise ValueError('BASELINE_MANIFEST_INVALID')
    return {'bytes': raw['bytes'], 'sha256': raw['sha256']}


class VerifiedBaseline:
    """Process-local proof binding one plaintext baseline and its manifest.

    Instances can only be created by :func:`verify_baseline_once`; they are
    intentionally not serializable. ``validate`` checks the original manifest
    bytes, sidecars, and exact file identity/stat fingerprint without reading
    the baseline contents again.
    """

    __slots__ = ('_source_path', '_manifest_path', '_manifest_sha256',
                 '_source_fingerprint', '_digest')

    def __init__(self, *_args, **_kwargs):
        raise TypeError('VerifiedBaseline tokens come from verify_baseline_once()')

    def __setattr__(self, _name, _value):
        raise AttributeError('VerifiedBaseline tokens are immutable')

    def __reduce__(self):
        raise TypeError('VerifiedBaseline tokens are process-local and cannot be serialized')

    def __reduce_ex__(self, _protocol):
        raise TypeError('VerifiedBaseline tokens are process-local and cannot be serialized')

    @classmethod
    def _create(cls, seal, source_path, manifest_path, manifest_sha256,
                source_fingerprint, digest):
        if seal is not _TOKEN_SEAL:
            raise TypeError('VerifiedBaseline tokens come from verify_baseline_once()')
        token = object.__new__(cls)
        object.__setattr__(token, '_source_path', str(source_path))
        object.__setattr__(token, '_manifest_path', str(manifest_path))
        object.__setattr__(token, '_manifest_sha256', manifest_sha256)
        object.__setattr__(token, '_source_fingerprint', tuple(source_fingerprint))
        object.__setattr__(token, '_digest', dict(digest))
        return token

    @property
    def source_path(self):
        return self._source_path

    @property
    def manifest_path(self):
        return self._manifest_path

    @property
    def manifest_sha256(self):
        return self._manifest_sha256

    @property
    def source_fingerprint(self):
        return self._source_fingerprint

    @property
    def digest(self):
        return dict(self._digest)

    def validate(self, path, manifest):
        """Return the bound source digest, or fail if any binding has changed."""
        try:
            source_path = safe_path(path)
            manifest_path = safe_path(manifest)
            if (str(source_path) != self._source_path
                    or str(manifest_path) != self._manifest_path):
                raise ValueError('baseline path binding changed')
            manifest_bytes, _manifest_fingerprint = _read_regular_bytes(
                manifest_path, 'BASELINE_VERIFICATION_STALE')
            if hashlib.sha256(manifest_bytes).hexdigest() != self._manifest_sha256:
                raise ValueError('baseline manifest bytes changed')
            _check_baseline_sidecars(source_path, 'BASELINE_VERIFICATION_STALE')
            opened_path, descriptor, fingerprint = _open_regular_nofollow(
                source_path, 'BASELINE_VERIFICATION_STALE')
            try:
                if fingerprint != self._source_fingerprint:
                    raise ValueError('baseline file fingerprint changed')
                _assert_fd_path_unchanged(
                    opened_path, descriptor, self._source_fingerprint,
                    'BASELINE_VERIFICATION_STALE')
            finally:
                os.close(descriptor)
            _check_baseline_sidecars(source_path, 'BASELINE_VERIFICATION_STALE')
            return dict(self._digest)
        except (OSError, ValueError, TypeError) as error:
            if str(error) == 'BASELINE_VERIFICATION_STALE':
                raise
            raise ValueError('BASELINE_VERIFICATION_STALE') from error


def verify_baseline_once(path, manifest):
    """Read and hash a plaintext baseline once, returning a runtime token.

    The content digest is computed from an ``O_NOFOLLOW`` file descriptor.
    Descriptor and path fingerprints must remain equal before and after the
    full read, and active nonzero SQLite sidecars are rejected.
    """
    try:
        source_path = safe_path(path)
        manifest_path = safe_path(manifest)
    except (OSError, ValueError, TypeError) as error:
        raise ValueError('BASELINE_SOURCE_INVALID') from error
    _check_baseline_sidecars(source_path, 'BASELINE_SOURCE_HAS_SIDECAR')
    manifest_bytes, _manifest_fingerprint = _read_regular_bytes(
        manifest_path, 'BASELINE_MANIFEST_INVALID')
    expected = _manifest_source_digest(manifest_bytes, source_path)
    opened_path, descriptor, fingerprint = _open_regular_nofollow(
        source_path, 'BASELINE_SOURCE_INVALID')
    try:
        if fingerprint[2] <= 0:
            raise ValueError('BASELINE_SOURCE_INVALID')
        digest = _hash_fd(descriptor)
        _assert_fd_path_unchanged(
            opened_path, descriptor, fingerprint, 'BASELINE_SOURCE_CHANGED_DURING_VERIFICATION')
        if digest != expected:
            raise ValueError('BASELINE_DIGEST_MISMATCH')
    finally:
        os.close(descriptor)
    _check_baseline_sidecars(source_path, 'BASELINE_SOURCE_HAS_SIDECAR')
    # Repeat ancestor/path checks after the large read so a replaced path or
    # symlinked ancestor cannot be bound to the descriptor's old digest.
    try:
        if str(safe_path(path)) != str(source_path):
            raise ValueError('BASELINE_SOURCE_CHANGED_DURING_VERIFICATION')
    except (OSError, ValueError) as error:
        if str(error) == 'BASELINE_SOURCE_CHANGED_DURING_VERIFICATION':
            raise
        raise ValueError('BASELINE_SOURCE_CHANGED_DURING_VERIFICATION') from error
    return VerifiedBaseline._create(
        _TOKEN_SEAL, source_path, manifest_path,
        hashlib.sha256(manifest_bytes).hexdigest(), fingerprint, digest)


def _verified_baseline(path, manifest):
    token = verify_baseline_once(path, manifest)
    return token.validate(path, manifest)


def _previous_document(previous, baseline_id, baseline_sha):
    if previous is None:
        return None
    if isinstance(previous, (str, os.PathLike)):
        previous = json.loads(safe_path(previous).read_text())
    if (not isinstance(previous, dict) or previous.get('status') != 'prepared'
            or previous.get('role') != 'lossless_delta_generation'
            or previous.get('baseline_generation_id') != baseline_id
            or previous.get('baseline_source_sha256') != baseline_sha
            or not GENERATION_ID.fullmatch(previous.get('generation_id', ''))
            or previous.get('value_verification', {}).get('status') != 'verified'):
        raise ValueError('PREVIOUS_GENERATION_INVALID')
    if type(previous.get('through_event_id')) is not int or previous['through_event_id'] < 0:
        raise ValueError('PREVIOUS_GENERATION_INVALID')
    return previous


def prepare_delta(current_db, baseline_db, baseline_manifest, generation, generation_id,
                  baseline_generation_id, *, previous=None, max_part_bytes=20 * 1024 * 1024,
                  baseline_verification=None):
    """準備済みreceiptまで生成する。公開済みcheckpointは変更しない。"""
    for value in (generation_id, baseline_generation_id):
        if not isinstance(value, str) or not GENERATION_ID.fullmatch(value):
            raise ValueError('GENERATION_ID_INVALID')
    current_db, baseline_db, generation = map(safe_path, (current_db, baseline_db, generation))
    if not current_db.is_file() or not baseline_db.is_file() or current_db == baseline_db:
        raise ValueError('SOURCE_PATH_INVALID')
    if generation.exists():
        raise ValueError('GENERATION_ALREADY_EXISTS')
    if type(max_part_bytes) is not int or max_part_bytes < MIN_MAX_BYTES or max_part_bytes > 20 * 1024 * 1024:
        raise ValueError('PART_LIMIT_INVALID')
    if baseline_verification is None:
        baseline_verification = verify_baseline_once(baseline_db, baseline_manifest)
    elif type(baseline_verification) is not VerifiedBaseline:
        raise ValueError('BASELINE_VERIFICATION_TOKEN_INVALID')
    baseline_digest = baseline_verification.validate(baseline_db, baseline_manifest)
    previous = _previous_document(previous, baseline_generation_id, baseline_digest['sha256'])
    generation.mkdir(parents=True, mode=0o700)
    baseline = current = None
    phase = 'pin_source'
    try:
        baseline_verification.validate(baseline_db, baseline_manifest)
        baseline = _immutable(baseline_db)
        current = sqlite3.connect(current_db.as_uri() + '?mode=ro', uri=True, timeout=30)
        current.execute('PRAGMA query_only=ON')
        current.execute('BEGIN')
        # This first database read pins both the clock and the source state.
        capture = current.execute(
            "SELECT strftime('%Y-%m-%dT%H:%M:%fZ','now'),COUNT(*) FROM sqlite_master"
        ).fetchone()[0]
        feed_exists = current.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (CHANGE_TABLE,)
        ).fetchone()
        if not feed_exists:
            raise ValueError('CHANGE_FEED_NOT_INSTALLED')
        highwater = current.execute('SELECT COALESCE(MAX(event_id),0) FROM archive_change_feed').fetchone()[0]
        after = previous['through_event_id'] if previous else 0
        batch = read_change_batch(current, min(after, highwater))
        with batch:
            reset = (previous is None or after > highwater
                     or batch.metadata['requires_baseline_reconciliation']
                     or batch.metadata['source_schema_sha256'] != previous['source_schema_sha256'])
        reader = read_reconciliation(current, baseline) if reset else read_change_batch(current, after)
        phase = 'full_source_value_verification'
        with reader:
            parent_id = baseline_generation_id if reset else previous['generation_id']
            metadata = {
                **reader.metadata,
                'generation_id': generation_id,
                'parent_generation_id': parent_id,
                'baseline_generation_id': baseline_generation_id,
                'baseline_source_sha256': baseline_digest['sha256'],
                'captured_at': capture,
                'captured_at_kind': 'pinned_read_transaction',
                'kind': 'baseline_reconciliation' if reset else 'change_feed',
                'replaces_delta_chain': bool(reset),
                'supersedes_generation_id': previous['generation_id'] if previous and reset else None,
            }
            transport = generation / 'changes.sqlite3'
            document = build_delta_database(reader.iter_current_changes(), metadata, transport)
            receipt = verify_delta_database(current, transport, document,
                                            baseline_conn=baseline if reset else None)
            # The source comparison above remains exhaustive. This stat-only
            # check additionally proves the previously hashed baseline stayed
            # at the same inode and content-affecting metadata through it.
            baseline_verification.validate(baseline_db, baseline_manifest)
            atomic_json(generation / 'transport-document.json', document)
            atomic_json(generation / 'value-verification.json', receipt)
        current.rollback()
        current.close()
        current = None
        baseline.close()
        baseline = None
        # Everything after this point reads the immutable verified transport.
        # Release the live WAL snapshot before XLSX generation or cloud work.
        phase = 'transport_fragmentation'
        transport_manifest = transport_proof = None
        if document['database']['bytes'] > max_part_bytes:
            connection = _immutable(transport)
            try:
                transport_manifest = export_sqlite_shards(
                    connection, generation / 'transport', snapshot_id=generation_id,
                    max_bytes=max_part_bytes)
                transport_proof = verify_sqlite_shards(connection, generation / 'transport', transport_manifest)
            finally:
                connection.close()
        phase = 'lossless_xlsx'
        export_full_xlsx(transport, generation / 'xlsx', snapshot_identifier=generation_id)
        xlsx_index = json.loads((generation / 'xlsx/index.json').read_text())
        xlsx_proof = json.loads((generation / 'xlsx/verification.json').read_text())
        if (xlsx_proof.get('status') != 'verified' or xlsx_proof.get('source_rowids_verified') is not True
                or xlsx_index.get('row_identity_format') != 'source_rowid_column_v1'
                or xlsx_proof.get('snapshot_sha256') != document['database']['sha256']):
            raise ValueError('DELTA_XLSX_PROOF_INVALID')
        phase = 'generation_plan'
        files = []
        def add(relative, known=None):
            digest = known or file_digest(generation / relative)
            files.append({'local': relative,
                          'remote': f'deltas/generations/{generation_id}/{relative}', **digest})
        if transport_manifest is None:
            add('changes.sqlite3', {key: document['database'][key] for key in ('bytes', 'sha256')})
            representation = {'kind': 'native_sqlite', 'path': 'changes.sqlite3'}
        else:
            for item in data_files(transport_manifest):
                add('transport/' + item['file'], {key: item[key] for key in ('bytes', 'sha256')})
            add('transport/manifest.json')
            add('transport/verification.json')
            representation = {'kind': 'lossless_sqlite_shards', 'path': 'transport/manifest.json',
                              'verification': transport_proof}
        add('transport-document.json')
        add('value-verification.json')
        for piece in xlsx_index['pieces']:
            add('xlsx/' + piece['name'], {key: piece[key] for key in ('bytes', 'sha256')})
        for name in ('index.json', 'verification.json', 'manifest.json'):
            add('xlsx/' + name)
        plan = {
            'version': 1, 'status': 'prepared', 'role': 'lossless_delta_generation',
            'generation_id': generation_id, 'baseline_generation_id': baseline_generation_id,
            'baseline_source_sha256': baseline_digest['sha256'],
            'parent_generation_id': parent_id,
            'expected_previous_generation_id': previous['generation_id'] if previous else baseline_generation_id,
            'replaces_delta_chain': bool(reset),
            'supersedes_generation_id': metadata['supersedes_generation_id'],
            'kind': metadata['kind'], 'captured_at': capture,
            'after_event_id': metadata['after_event_id'], 'through_event_id': metadata['through_event_id'],
            'source_schema_sha256': metadata['source_schema_sha256'],
            'source_row_counts': metadata['source_row_counts'],
            'source_table_columns': metadata['source_table_columns'],
            'source_foreign_keys': metadata['source_foreign_keys'],
            'schemas': metadata['schemas'], 'metadata': metadata,
            'transport': representation, 'transport_database': document['database'],
            'value_verification': receipt, 'xlsx_verification': xlsx_proof,
            'files': files, 'max_part_bytes': max_part_bytes,
            'coverage': {'all_changed_values': True, 'all_source_tables_metadata': True,
                         'full_schema': True, 'original_row_identities': True,
                         'baseline_gap_reconciled': bool(reset)},
        }
        # Bind the plan to the same verified baseline at the last point before
        # a success receipt can become visible. A stale token leaves only the
        # retained partial artifacts and failed.json.
        baseline_verification.validate(baseline_db, baseline_manifest)
        atomic_json(generation / 'delta-plan.json', plan)
        return plan
    except Exception as error:
        error_code = getattr(error, 'code', None)
        if not isinstance(error_code, str) or not re.fullmatch(r'[A-Z0-9_]{1,80}', error_code):
            candidate = str(error)
            error_code = candidate if re.fullmatch(r'[A-Z0-9_]{1,80}', candidate) else 'DELTA_PREPARATION_FAILED'
        atomic_json(generation / 'failed.json', {
            'status': 'failed', 'generation_id': generation_id, 'phase': phase,
            'error_type': type(error).__name__,
            'error_code': error_code,
            'captured_at': locals().get('capture'), 'published_checkpoint_advanced': False,
        })
        raise
    finally:
        if current is not None:
            current.close()
        if baseline is not None:
            baseline.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--db', type=Path, required=True)
    parser.add_argument('--baseline', type=Path, required=True)
    parser.add_argument('--baseline-manifest', type=Path, required=True)
    parser.add_argument('--baseline-generation-id', required=True)
    parser.add_argument('--work-dir', type=Path, required=True)
    parser.add_argument('--previous-plan', type=Path)
    parser.add_argument('--generation-id')
    args = parser.parse_args(argv)
    generation_id = args.generation_id or new_generation_id()
    plan = prepare_delta(args.db, args.baseline, args.baseline_manifest,
                         safe_path(args.work_dir) / generation_id, generation_id,
                         args.baseline_generation_id, previous=args.previous_plan)
    print(json.dumps({'status': plan['status'], 'generation_id': generation_id,
                      'kind': plan['kind'], 'through_event_id': plan['through_event_id']}))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
