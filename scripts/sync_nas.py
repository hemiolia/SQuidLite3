#!/usr/bin/env python3
"""
NAS (nas:/home/Natsuki/ikaring-archive) とローカルのイカリング3アーカイブを双方向同期するスクリプト。
UGOSのrsync制限を回避し、SSH経由でタイムスタンプ・サイズ比較により差分のみを安全かつ高速に転送します。
database/、secrets/、認証データ、rclone設定(rclone.conf/rclone-*.conf)、SQLite、暗号化ファイル、runtime、spool、.history、.git、backups等は除外されます。
"""

import argparse
import fnmatch
import hashlib
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
import tempfile
import time
import uuid

if __package__:
    from .sync_baseline import load_baseline, merged_baseline, save_baseline, sync_lock
else:
    from sync_baseline import load_baseline, merged_baseline, save_baseline, sync_lock

DEFAULT_LOCAL = Path(os.environ.get('IKARING_ARCHIVE_DATA_DIR', str(Path.home() / 'Documents/イカリング3アーカイブ')))
DEFAULT_REMOTE_HOST = 'nas'
DEFAULT_REMOTE_DIR = '/home/Natsuki/ikaring-archive'

IGNORE_PATTERNS = {'.DS_Store', '._.DS_Store'}
IGNORE_SUFFIXES = {'.tmp', '.lock', '-wal', '-shm'}
IGNORE_GLOBS = {'*.sqlite*', '*.db*', '*.gpg', '*token*', '*credential*', '.env*', 'rclone.conf', 'rclone-*.conf', '.sync-state-*.json'}
IGNORE_DIR_NAMES = {
    'database', 'secrets', 'nxapi-nodejs',
    'runtime', 'spool', '.history', '.git', 'backups', '.sync-conflicts'
}

MAX_FILE_BYTES = 1024 * 1024 * 1024  # 1GiB

def should_ignore(rel_path: str) -> bool:
    parts = Path(rel_path).parts
    if parts == ('config', 'storage-location.json'):
        return True
    for p in parts:
        p_lower = p.lower()
        if p in IGNORE_PATTERNS or p.startswith('._'):
            return True
        for s in IGNORE_SUFFIXES:
            if p_lower.endswith(s):
                return True
        if p_lower in IGNORE_DIR_NAMES or 'auth' in p_lower:
            return True
        for pat in IGNORE_GLOBS:
            if fnmatch.fnmatch(p_lower, pat.lower()):
                return True
    return False

def verify_path_safety(root: Path, target: Path):
    """
    target自身およびrootからの親ディレクトリパスにsymlinkが含まれていないこと、
    およびrootの外へ脱出（root escape）していないことを検証する。
    違反した場合はValueErrorを発生させる。
    """
    root_resolved = root.resolve()
    try:
        if target.is_absolute():
            try:
                rel = target.relative_to(root)
            except ValueError:
                rel = target.relative_to(root_resolved)
        else:
            rel = target
    except ValueError:
        raise ValueError(f"Target is not under root: {target} (root: {root})")

    if '..' in rel.parts:
        raise ValueError(f"Path traversal detected: {rel}")

    curr = root
    for part in rel.parts:
        curr = curr / part
        if os.path.islink(curr):
            raise ValueError(f"Symlink rejected: {curr}")
        if not os.path.lexists(curr):
            break

    check_target = target if target.exists() or os.path.islink(target) else target.parent
    if check_target.exists() or os.path.islink(check_target):
        try:
            check_target.resolve().relative_to(root_resolved)
        except ValueError:
            raise ValueError(f"Root escape detected: {target} resolves outside {root}")

def _on_walk_error(err):
    raise err

def compute_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        while chunk := f.read(65536):
            h.update(chunk)
    return h.hexdigest()

def scan_local(base_dir: Path) -> dict:
    result = {}
    if not base_dir.exists():
        return result
    for root, dirs, files in os.walk(base_dir, followlinks=False, onerror=_on_walk_error):
        dirs[:] = [d for d in dirs if not (Path(root, d).is_symlink() or should_ignore(d))]
        for f in files:
            p = Path(root) / f
            if p.is_symlink():
                continue
            rel = str(p.relative_to(base_dir))
            if should_ignore(rel):
                continue
            stat = p.stat()
            if stat.st_size > MAX_FILE_BYTES:
                sha256 = None
            else:
                sha256 = compute_sha256(p)
            result[rel] = {'size': stat.st_size, 'mtime': int(stat.st_mtime), 'sha256': sha256}
    return result

def scan_remote(remote_host: str, remote_dir: str) -> dict:
    remote_code = f"""
import os, json, fnmatch, hashlib
from pathlib import Path

base = Path({json.dumps(remote_dir)})
result = {{}}
ignore_patterns = {tuple(IGNORE_PATTERNS)}
ignore_suffixes = {tuple(IGNORE_SUFFIXES)}
ignore_globs = {tuple(IGNORE_GLOBS)}
ignore_dir_names = {tuple(IGNORE_DIR_NAMES)}

def _on_walk_error(err):
    raise err

def should_ignore_part(p):
    p_lower = p.lower()
    if p in ignore_patterns or p.startswith('._'):
        return True
    for s in ignore_suffixes:
        if p_lower.endswith(s):
            return True
    if p_lower in ignore_dir_names or 'auth' in p_lower:
        return True
    for pat in ignore_globs:
        if fnmatch.fnmatch(p_lower, pat.lower()):
            return True
    return False

def should_ignore(rel_path):
    if Path(rel_path).parts == ('config', 'storage-location.json'):
        return True
    for part in Path(rel_path).parts:
        if should_ignore_part(part):
            return True
    return False

def compute_sha256(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        while chunk := f.read(65536):
            h.update(chunk)
    return h.hexdigest()

if base.exists():
    for root, dirs, files in os.walk(base, followlinks=False, onerror=_on_walk_error):
        dirs[:] = [d for d in dirs if not (os.path.islink(os.path.join(root, d)) or should_ignore_part(d))]
        for f in files:
            p = Path(root) / f
            if p.is_symlink():
                continue
            rel = str(p.relative_to(base))
            if should_ignore(rel):
                continue
            stat = p.stat()
            if stat.st_size > 1024 * 1024 * 1024:
                sha256 = None
            else:
                sha256 = compute_sha256(p)
            result[rel] = {{'size': stat.st_size, 'mtime': int(stat.st_mtime), 'sha256': sha256}}
print(json.dumps(result))
"""
    cmd = ['ssh', remote_host, 'python3 -']
    res = subprocess.run(cmd, input=remote_code, capture_output=True, text=True, check=True)
    return json.loads(res.stdout.strip())

def push_file(local_path: Path, rel_path: str, remote_host: str, remote_dir: str, is_conflict: bool = False, local_dir: Path = None, expected_dest_sha: str = None):
    if local_dir is None:
        parts_count = len(Path(rel_path).parts)
        local_dir = local_path.parents[parts_count - 1] if parts_count <= len(local_path.parents) else local_path.parent

    # 転送直前のローカルパス検証（symlink / root escape 拒否）
    verify_path_safety(local_dir, local_path)

    conflict_id = uuid.uuid4().hex if is_conflict else ""
    stage_prefix = '.tmp_sync_stage_' + uuid.uuid4().hex + '_'
    filename = local_path.name
    env = os.environ.copy()
    env['COPYFILE_DISABLE'] = '1'
    tar_cmd = ['tar', '--no-mac-metadata', '--no-xattrs', '-C', str(local_path.parent), '-cf', '-', '--', filename]

    # Stage 1: tarだけをstdinに送り、PythonコードはSSHコマンド引数で渡す。
    stage_code = f"""
import os, sys, shutil, tempfile

remote_dir = {json.dumps(remote_dir)}
rel_path = {json.dumps(rel_path)}
stage_prefix = {json.dumps(stage_prefix)}

if os.path.isabs(rel_path) or not rel_path or '..' in rel_path.split('/'):
    sys.stderr.write("Path traversal detected\\n")
    sys.exit(1)

if os.path.islink(remote_dir) or not os.path.isdir(remote_dir):
    sys.stderr.write("Remote root is missing or a symlink\\n")
    sys.exit(1)

curr = remote_dir
for part in rel_path.split('/'):
    curr = os.path.join(curr, part)
    if os.path.islink(curr):
        sys.stderr.write(f"Symlink rejected: {{curr}}\\n")
        sys.exit(1)
    if not os.path.lexists(curr):
        break

filename = os.path.basename(rel_path)
tmpdir = tempfile.mkdtemp(prefix=stage_prefix, dir=remote_dir)
try:
    import subprocess
    res = subprocess.run(['tar', '-C', tmpdir, '-xf', '-'], stdin=sys.stdin)
    if res.returncode != 0:
        shutil.rmtree(tmpdir, ignore_errors=True)
        sys.exit(res.returncode)

    tmp_file = os.path.join(tmpdir, filename)
    if not os.path.isfile(tmp_file) or os.path.islink(tmp_file):
        sys.stderr.write(f"File not extracted: {{tmp_file}}\\n")
        shutil.rmtree(tmpdir, ignore_errors=True)
        sys.exit(1)

    with open(tmp_file, 'rb') as f:
        os.fsync(f.fileno())

    print(f"STAGE_OK:{{tmpdir}}")
except Exception as e:
    shutil.rmtree(tmpdir, ignore_errors=True)
    sys.stderr.write(str(e) + "\\n")
    sys.exit(1)
"""
    # ssh joins post-host argv into a remote shell command. Quote the entire
    # -c argument so embedded spaces, quotes and newlines survive that shell.
    ssh_cmd = ['ssh', remote_host, 'python3 -c ' + shlex.quote(stage_code)]

    p1 = subprocess.Popen(tar_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
    p2 = subprocess.Popen(ssh_cmd, stdin=p1.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    p1.stdout.close()
    out2, err2 = p2.communicate()
    out1, err1 = p1.communicate()
    p1_ret = p1.wait()
    p2_ret = p2.returncode

    stage_dir = None
    for line in out2.decode(errors='replace').splitlines():
        if line.startswith("STAGE_OK:"):
            stage_dir = line.split(":", 1)[1].strip()

    if p1_ret != 0 or p2_ret != 0:
        if stage_dir:
            cleanup_code = f"""
import os, shutil
root = {json.dumps(remote_dir)}
stage = {json.dumps(stage_dir)}
prefix = {json.dumps(stage_prefix)}
if (os.path.dirname(stage) == root and
        os.path.basename(stage).startswith(prefix) and
        not os.path.islink(root) and not os.path.islink(stage)):
    shutil.rmtree(stage, ignore_errors=True)
"""
            cleanup_cmd = ['ssh', remote_host, 'python3 -c ' + shlex.quote(cleanup_code)]
            subprocess.run(cleanup_cmd, capture_output=True)
        raise RuntimeError(
            f"Failed to push {rel_path}: tar={p1_ret} ({err1.decode(errors='replace').strip()}), "
            f"ssh={p2_ret} ({err2.decode(errors='replace').strip()})"
        )

    if not stage_dir:
        raise RuntimeError(f"Failed to push {rel_path}: stage directory not returned by remote")

    # Stage 2: 上流 exit 0 を確認した後の別SSH検証commit
    commit_code = f"""
import os, sys, shutil, hashlib

remote_dir = {json.dumps(remote_dir)}
rel_path = {json.dumps(rel_path)}
stage_dir = {json.dumps(stage_dir)}
stage_prefix = {json.dumps(stage_prefix)}
conflict_id = {json.dumps(conflict_id)}
expected_dest_sha = {repr(expected_dest_sha)}

filename = os.path.basename(rel_path)
tmp_file = os.path.join(stage_dir, filename)

if (os.path.dirname(stage_dir) != remote_dir or
        not os.path.basename(stage_dir).startswith(stage_prefix) or
        os.path.islink(remote_dir) or os.path.islink(stage_dir) or
        not os.path.isdir(stage_dir) or
        not os.path.isfile(tmp_file) or os.path.islink(tmp_file)):
    sys.stderr.write(f"Staged file not found: {{tmp_file}}\\n")
    sys.exit(1)

if os.path.isabs(rel_path) or not rel_path or '..' in rel_path.split('/'):
    sys.stderr.write("Path traversal detected\\n")
    sys.exit(1)

if os.path.islink(remote_dir) or not os.path.isdir(remote_dir):
    sys.stderr.write("Remote root is missing or a symlink\\n")
    sys.exit(1)

curr = remote_dir
for part in rel_path.split('/'):
    curr = os.path.join(curr, part)
    if os.path.islink(curr):
        sys.stderr.write(f"Symlink rejected: {{curr}}\\n")
        shutil.rmtree(stage_dir, ignore_errors=True)
        sys.exit(1)
    if not os.path.lexists(curr):
        break

target_path = os.path.join(remote_dir, rel_path)
target_dir = os.path.dirname(target_path)

try:
    os.makedirs(target_dir, exist_ok=True)
    if expected_dest_sha is not None:
        if expected_dest_sha == '':
            if os.path.lexists(target_path):
                raise RuntimeError('Destination appeared during sync')
        else:
            if not os.path.isfile(target_path) or os.path.islink(target_path):
                raise RuntimeError('Destination changed during sync')
            digest = hashlib.sha256()
            with open(target_path, 'rb') as current:
                for chunk in iter(lambda: current.read(65536), b''):
                    digest.update(chunk)
            if digest.hexdigest() != expected_dest_sha:
                raise RuntimeError('Destination changed during sync')
    if conflict_id and os.path.exists(target_path):
        conflict_path = os.path.join(remote_dir, '.sync-conflicts', conflict_id, rel_path)
        curr = remote_dir
        for part in os.path.relpath(os.path.dirname(conflict_path), remote_dir).split(os.sep):
            curr = os.path.join(curr, part)
            if os.path.islink(curr):
                sys.stderr.write(f"Symlink rejected: {{curr}}\\n")
                sys.exit(1)
            if not os.path.lexists(curr):
                break
        os.makedirs(os.path.dirname(conflict_path), exist_ok=True)
        with open(target_path, 'rb') as f_in, open(conflict_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
            f_out.flush()
            os.fsync(f_out.fileno())
        shutil.copystat(target_path, conflict_path)

    os.replace(tmp_file, target_path)
finally:
    if (os.path.dirname(stage_dir) == remote_dir and
            os.path.basename(stage_dir).startswith(stage_prefix) and
            not os.path.islink(stage_dir)):
        shutil.rmtree(stage_dir, ignore_errors=True)

print("COMMIT_OK")
"""
    commit_res = subprocess.run(['ssh', remote_host, 'python3 -'], input=commit_code, capture_output=True, text=True)
    if commit_res.returncode != 0:
        raise RuntimeError(f"Failed to commit {rel_path} on remote: {commit_res.stderr.strip()}")

def pull_file(remote_host: str, remote_dir: str, rel_path: str, local_path: Path, local_dir: Path = None, is_conflict: bool = False, expected_dest_sha: str = None):
    if local_dir is None:
        parts_count = len(Path(rel_path).parts)
        local_dir = local_path.parents[parts_count - 1] if parts_count <= len(local_path.parents) else local_path.parent

    # 転送直前のローカルパス検証（symlink / root escape 拒否）
    verify_path_safety(local_dir, local_path)

    local_path.parent.mkdir(parents=True, exist_ok=True)
    filename = Path(rel_path).name

    # SSH経由でリモート側もsymlink/root escapeを検証した上でtarを出力
    remote_code = f"""
import os, sys, subprocess

remote_dir = {json.dumps(remote_dir)}
rel_path = {json.dumps(rel_path)}

if os.path.isabs(rel_path) or '..' in rel_path.split('/'):
    sys.stderr.write("Path traversal detected\\n")
    sys.exit(1)

curr = remote_dir
for part in rel_path.split('/'):
    curr = os.path.join(curr, part)
    if os.path.islink(curr):
        sys.stderr.write(f"Symlink rejected: {{curr}}\\n")
        sys.exit(2)
    if not os.path.lexists(curr):
        sys.stderr.write(f"Path does not exist: {{curr}}\\n")
        sys.exit(3)

parent = os.path.dirname(rel_path)
src_dir = os.path.join(remote_dir, parent) if parent else remote_dir
cmd = ['tar', '-C', src_dir, '-cf', '-', '--', os.path.basename(rel_path)]
res = subprocess.run(cmd)
sys.exit(res.returncode)
"""
    ssh_cmd = ['ssh', remote_host, 'python3 -c ' + shlex.quote(remote_code)]

    with tempfile.TemporaryDirectory(dir=str(local_path.parent)) as tmpdir:
        tar_cmd = ['tar', '-C', tmpdir, '-xf', '-']

        p1 = subprocess.Popen(ssh_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        p2 = subprocess.Popen(tar_cmd, stdin=p1.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        p1.stdout.close()
        out2, err2 = p2.communicate()
        out1, err1 = p1.communicate()
        p1_ret = p1.wait()
        p2_ret = p2.returncode

        if p1_ret != 0 or p2_ret != 0:
            raise RuntimeError(
                f"Failed to pull {rel_path}: ssh={p1_ret} ({err1.decode(errors='replace').strip()}), "
                f"tar={p2_ret} ({err2.decode(errors='replace').strip()})"
            )

        extracted_file = Path(tmpdir) / filename
        if not extracted_file.exists():
            raise FileNotFoundError(f"Extracted file not found: {extracted_file}")

        with open(extracted_file, 'rb') as f:
            os.fsync(f.fileno())

        if expected_dest_sha is not None:
            verify_path_safety(local_dir, local_path)
            if expected_dest_sha == '':
                if os.path.lexists(local_path):
                    raise RuntimeError(f'Destination appeared during sync: {rel_path}')
            elif not local_path.is_file() or compute_sha256(local_path) != expected_dest_sha:
                raise RuntimeError(f'Destination changed during sync: {rel_path}')

        if is_conflict and local_path.exists():
            root = local_dir if local_dir is not None else local_path.parents[len(Path(rel_path).parts) - 1]
            conflict_id = uuid.uuid4().hex
            conflict_path = root / '.sync-conflicts' / conflict_id / rel_path
            verify_path_safety(root, conflict_path)
            conflict_path.parent.mkdir(parents=True, exist_ok=True)
            with open(local_path, 'rb') as f_in, open(conflict_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
                f_out.flush()
                os.fsync(f_out.fileno())
            shutil.copystat(local_path, conflict_path)

        os.replace(extracted_file, local_path)

def plan_sync(local_files: dict, remote_files: dict, mode: str = 'both', baseline: dict = None) -> tuple:
    """
    同期プランを計算する。両側サイズ最大値で1GiB上限を検査し、
    内容一致（SHA256同一）ならスキップ、異内容はis_conflict=Trueで退避対象とする。
    """
    all_keys = sorted(set(local_files.keys()) | set(remote_files.keys()))
    to_push = []
    to_pull = []

    for k in all_keys:
        in_local = k in local_files
        in_remote = k in remote_files

        if in_local and in_remote:
            max_size = max(local_files[k]['size'], remote_files[k]['size'])
        elif in_local:
            max_size = local_files[k]['size']
        else:
            max_size = remote_files[k]['size']

        if max_size > MAX_FILE_BYTES:
            continue

        if in_local and not in_remote:
            if mode in ('both', 'push'):
                to_push.append((k, '新規作成', False))
        elif in_remote and not in_local:
            if mode in ('both', 'pull'):
                to_pull.append((k, '新規取得', False))
        else:
            loc = local_files[k]
            rem = remote_files[k]
            if loc['sha256'] == rem['sha256']:
                continue

            common = baseline.get(k) if baseline is not None else None
            push_safe = common is not None and rem['sha256'] == common
            pull_safe = common is not None and loc['sha256'] == common

            if mode == 'push':
                to_push.append((k, f"更新 (size {loc['size']} vs {rem['size']})", not push_safe))
            elif mode == 'pull':
                to_pull.append((k, f"更新 (size {rem['size']} vs {loc['size']})", not pull_safe))
            else:  # both
                if push_safe:
                    to_push.append((k, 'ローカルのみ更新', False))
                elif pull_safe:
                    to_pull.append((k, 'リモートのみ更新', False))
                elif loc['mtime'] > rem['mtime']:
                    to_push.append((k, f"ローカルが新しい ({loc['mtime']} > {rem['mtime']})", True))
                elif rem['mtime'] > loc['mtime']:
                    to_pull.append((k, f"リモートが新しい ({rem['mtime']} > {loc['mtime']})", True))
                else:
                    to_pull.append((k, f"異内容（mtime同一、リモートを採用）", True))

    return to_push, to_pull

def _run_sync(args, local_dir):
    print(f"=== NAS同期開始 [{args.mode}] ===")
    print(f"ローカル: {local_dir}")
    print(f"リモート: {args.remote_host}:{args.remote_dir}")

    baseline = load_baseline(local_dir, '.sync-state-nas.json')
    print("ファイル一覧を取得中...")
    local_files = scan_local(local_dir)
    remote_files = scan_remote(args.remote_host, args.remote_dir)

    to_push, to_pull = plan_sync(local_files, remote_files, mode=args.mode, baseline=baseline)

    print(f"\n同期予定: Push={len(to_push)}件, Pull={len(to_pull)}件")

    if args.dry_run:
        for k, reason, _ in to_push:
            print(f"  [DRY-RUN PUSH] {k} ({reason})")
        for k, reason, _ in to_pull:
            print(f"  [DRY-RUN PULL] {k} ({reason})")
        print("\nDry-run 完了。変更はありません。")
        return

    for idx, (k, reason, is_conflict) in enumerate(to_push, 1):
        print(f"[{idx}/{len(to_push)}] Push: {k} ({reason})")
        expected = None if is_conflict else (remote_files[k]['sha256'] if k in remote_files else '')
        push_file(local_dir / k, k, args.remote_host, args.remote_dir, is_conflict=is_conflict,
                  local_dir=local_dir, expected_dest_sha=expected)

    for idx, (k, reason, is_conflict) in enumerate(to_pull, 1):
        print(f"[{idx}/{len(to_pull)}] Pull: {k} ({reason})")
        expected = None if is_conflict else (local_files[k]['sha256'] if k in local_files else '')
        pull_file(args.remote_host, args.remote_dir, k, local_dir / k, local_dir=local_dir,
                  is_conflict=is_conflict, expected_dest_sha=expected)

    final_local = scan_local(local_dir)
    final_remote = scan_remote(args.remote_host, args.remote_dir)
    save_baseline(local_dir, '.sync-state-nas.json',
                  merged_baseline(baseline, final_local, final_remote))

    print("\n=== NAS同期完了 ===")

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--local', type=Path, default=DEFAULT_LOCAL, help='ローカルデータディレクトリ')
    parser.add_argument('--remote-host', default=DEFAULT_REMOTE_HOST, help='SSHホスト名 (例: nas)')
    parser.add_argument('--remote-dir', default=DEFAULT_REMOTE_DIR, help='NAS側データディレクトリ')
    parser.add_argument('--mode', choices=['both', 'push', 'pull'], default='both', help='同期モード')
    parser.add_argument('--dry-run', action='store_true', help='変更を行わず確認のみ')
    args = parser.parse_args()
    local_dir = args.local.resolve()
    with sync_lock(local_dir):
        _run_sync(args, local_dir)

if __name__ == '__main__':
    main()
