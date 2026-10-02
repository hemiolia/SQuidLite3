#!/usr/bin/env python3
"""検証済み全情報SQLite群を先行配信する。全世代latestは更新しない。"""
import argparse
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src/python'))
sys.path.insert(0, str(ROOT / 'scripts'))
import nas_full_data_publish as publisher
from ikarchive.shard_reader import LosslessShardReader
from ikarchive.slice_selectors import data_files


def stage_sqlite(package, generation_id, captured_at, remote, state_dir, *, rclone_bin='rclone'):
    if not publisher.valid_generation_id(generation_id):
        raise publisher.PublishError('GENERATION_ID_INVALID')
    publisher.parse_utc_timestamp(captured_at)
    package = publisher.generation_root(package)
    remote = publisher.validate_remote(remote)
    state_dir = publisher.ensure_private_state_dir(state_dir)
    with publisher.PublishLock(state_dir):
        with LosslessShardReader(package, expected_generation=generation_id) as reader:
            source_sha = reader.source_sha256
            schema_sha = reader.source_schema_sha256
            # The reader has just independently validated all proofs, dependency
            # inventories, and local bytes. Preserve those exact control files.
            manifest = json.loads((package / 'manifest.json').read_text())
            value_proof = json.loads((package / 'verification.json').read_text())
            selector_proof = json.loads((package / 'selectors-verification.json').read_text())
        files = []
        for item in data_files(manifest) + manifest['by_mode'] + manifest['by_rule']:
            files.append({'local': item['file'], 'remote': f"slices/generations/{generation_id}/" + item['file'],
                          'bytes': item['bytes'], 'sha256': item['sha256']})
        for name in ('manifest.json', 'verification.json', 'selectors-verification.json'):
            size, sha = publisher.hash_regular_file(package / name)
            files.append({'local': name, 'remote': f'slices/generations/{generation_id}/{name}', 'bytes': size, 'sha256': sha})
        index = {
            'version': 1, 'role': 'lossless_full_sqlite_component', 'generation_id': generation_id,
            'captured_at': captured_at, 'captured_at_kind': 'pinned_read_transaction',
            'source_sha256': source_sha, 'source_schema_sha256': schema_sha,
            'sqlite_verification': value_proof, 'selector_verification': selector_proof,
            'files': files, 'includes_all_source_database_values': True,
            'includes_xlsx': False, 'global_generation_complete': False,
            'realtime_synchronized': False,
            'verification': {'full_readback': True, 'file_count': len(files)},
        }
        encoded = (json.dumps(index, ensure_ascii=False, sort_keys=True, separators=(',', ':'))+'\n').encode()
        index_sha = hashlib.sha256(encoded).hexdigest()
        path = state_dir / f'sqlite-component-{generation_id}.progress.json'
        progress = publisher.read_private_json(path)
        if progress is None:
            progress = {'generation_id': generation_id, 'index_sha256': index_sha,
                        'remote_prefix': remote, 'receipts': {}}
        if (progress.get('generation_id') != generation_id or progress.get('index_sha256') != index_sha
                or progress.get('remote_prefix') != remote):
            raise publisher.PublishError('COMPONENT_PROGRESS_MISMATCH')
        progress['path'] = path
        progress['remote'] = remote
        publisher._save_progress(path, progress)
        client = publisher.Rclone(rclone_bin)
        def report_progress(count, total, _item):
            if count % 50 == 0:
                print(json.dumps({'phase':'sqlite_component_readback','verified_files':count,
                                  'total_files':total}), flush=True)
        publisher._publish_files(client, package, state_dir, files, progress,
                                 on_verified=report_progress)
        publisher._ensure_immutable_bytes(client, state_dir,
            publisher.join_remote(remote, f'slices/generations/{generation_id}/sqlite-component-index.json'), encoded, index_sha,
            progress=progress, receipt_key='component_index')
        publisher.verify_remote_directories(client)
        receipt = {
            'status':'verified', 'generation_id':generation_id,
            'scope':'lossless_full_sqlite_component', 'source_sha256':source_sha,
            'full_remote_readback':True, 'files':len(files), 'index_sha256':index_sha,
            'global_generation_complete':False, 'realtime_synchronized':False,
            'verified_at':publisher.now_utc(),
        }
        publisher.atomic_json(state_dir / f'sqlite-component-{generation_id}.verified.json', receipt)
        return receipt


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--package', type=Path, required=True)
    parser.add_argument('--generation-id', required=True)
    parser.add_argument('--captured-at', required=True)
    parser.add_argument('--remote', required=True)
    parser.add_argument('--state-dir', type=Path, required=True)
    parser.add_argument('--rclone-bin', default='rclone')
    args=parser.parse_args(argv)
    receipt=stage_sqlite(args.package,args.generation_id,args.captured_at,args.remote,args.state_dir,
                         rclone_bin=args.rclone_bin)
    print(json.dumps(receipt))
    return 0

if __name__=='__main__':
    raise SystemExit(main())
