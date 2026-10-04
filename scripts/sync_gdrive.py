#!/usr/bin/env python3
"""
Google Drive (マイドライブ/SQuidLite3) とローカルのイカリング3アーカイブを双方向同期するスクリプト。
database/、secrets/、認証データ、rclone設定(rclone.conf/rclone-*.conf)、SQLite、暗号化ファイル、runtime、spool、.history、.git、backups等は除外されます。
旧部分ブックの ``exports/分析.xlsx`` と ``exports/analysis.xlsx`` も、その正確な相対パスだけ除外されます。
"""

import argparse
import fnmatch
import hashlib
import os
from pathlib import Path
import shutil
import sys
import tempfile
import uuid

if __package__:
    from .sync_baseline import load_baseline, merged_baseline, save_baseline, sync_lock
    from .data_root import client_mode, data_root
else:
    from sync_baseline import load_baseline, merged_baseline, save_baseline, sync_lock
    from data_root import client_mode, data_root

DEFAULT_GDRIVE = Path.home() / 'Library/CloudStorage/GoogleDrive-originnatsumikanf@gmail.com/マイドライブ/SQuidLite3'

IGNORE_PATTERNS = {'.DS_Store', '._.DS_Store'}
IGNORE_SUFFIXES = {'.tmp', '.lock', '-wal', '-shm'}
IGNORE_GLOBS = {'*.sqlite*', '*.db*', '*.gpg', '*token*', '*credential*', '.env*', 'rclone.conf', 'rclone-*.conf', '.sync-state-*.json'}
IGNORE_DIR_NAMES = {
    'database', 'secrets', 'nxapi-nodejs',
    'runtime', 'spool', '.history', '.git', 'backups', '.sync-conflicts'
}
RETIRED_LEGACY_EXPORT_PATHS = {'exports/分析.xlsx', 'exports/analysis.xlsx'}

MAX_FILE_BYTES = 1024 * 1024 * 1024  # 1GiB

def is_retired_legacy_export_path(rel_path: str) -> bool:
    return tuple(Path(rel_path).parts) in {
        ('exports', '分析.xlsx'),
        ('exports', 'analysis.xlsx'),
    }

def should_ignore(rel_path: str) -> bool:
    if is_retired_legacy_export_path(rel_path):
        return True
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

def compute_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        while chunk := f.read(65536):
            h.update(chunk)
    return h.hexdigest()

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

def scan_dir(base_dir: Path) -> dict:
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

def copy_file(src: Path, dst: Path, root_dir: Path = None, rel_path: str = None, is_conflict: bool = False, src_root: Path = None, expected_dest_sha: str = None):
    """
    元ファイルは完全転送+fsync済み一時fileからatomic replace。
    双方に存在する異内容は上書き前に宛先旧版を当該ルートの.sync-conflicts/<uuid>/<relative>へ保存。
    転送直前にsrc/dst自身および親ディレクトリのsymlink/root escapeを検証して拒否。
    """
    if root_dir is None:
        root_dir = dst.parent
    candidate_rel_path = rel_path
    if candidate_rel_path is None:
        try:
            candidate_rel_path = str(dst.relative_to(root_dir))
        except ValueError:
            candidate_rel_path = dst.name
    if is_retired_legacy_export_path(candidate_rel_path):
        raise ValueError(f'Legacy analysis workbook sync is retired: {candidate_rel_path}')
    if rel_path is None:
        rel_path = dst.name
    if src_root is None:
        parts_count = len(Path(rel_path).parts)
        src_root = src.parents[parts_count - 1] if parts_count <= len(src.parents) else src.parent

    # 転送直前のsymlink/root escape検証
    verify_path_safety(src_root, src)
    verify_path_safety(root_dir, dst)

    dst.parent.mkdir(parents=True, exist_ok=True)

    prefix = f".tmp_sync_{dst.name}_"
    tmp_fd, tmp_path_str = tempfile.mkstemp(prefix=prefix, dir=str(dst.parent))
    tmp_path = Path(tmp_path_str)

    try:
        with os.fdopen(tmp_fd, 'wb') as f_out, open(src, 'rb') as f_in:
            shutil.copyfileobj(f_in, f_out)
            f_out.flush()
            os.fsync(f_out.fileno())

        shutil.copystat(src, tmp_path)

        # A normal update is allowed only while the destination still matches
        # the common version observed at planning time. An absent destination
        # must remain absent; a concurrent edit is retried as a conflict later.
        if expected_dest_sha is not None:
            verify_path_safety(root_dir, dst)
            if expected_dest_sha == '':
                if os.path.lexists(dst):
                    raise RuntimeError(f'Destination appeared during sync: {rel_path}')
            elif not dst.is_file() or compute_sha256(dst) != expected_dest_sha:
                raise RuntimeError(f'Destination changed during sync: {rel_path}')

        if is_conflict and dst.exists():
            conflict_id = uuid.uuid4().hex
            conflict_path = root_dir / '.sync-conflicts' / conflict_id / rel_path
            conflict_path.parent.mkdir(parents=True, exist_ok=True)
            verify_path_safety(root_dir, conflict_path)
            with open(dst, 'rb') as f_in, open(conflict_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
                f_out.flush()
                os.fsync(f_out.fileno())
            shutil.copystat(dst, conflict_path)

        os.replace(tmp_path, dst)
    finally:
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except OSError:
                pass

def plan_sync(local_files: dict, gdrive_files: dict, mode: str = 'both', baseline: dict = None) -> tuple:
    """
    同期プランを計算する。両側サイズ最大値で1GiB上限を検査し、
    内容一致（SHA256同一）ならスキップ、異内容はis_conflict=Trueで退避対象とする。
    """
    all_keys = sorted(set(local_files.keys()) | set(gdrive_files.keys()))
    to_push = []
    to_pull = []

    for k in all_keys:
        if is_retired_legacy_export_path(k):
            continue
        in_local = k in local_files
        in_gdrive = k in gdrive_files

        if in_local and in_gdrive:
            max_size = max(local_files[k]['size'], gdrive_files[k]['size'])
        elif in_local:
            max_size = local_files[k]['size']
        else:
            max_size = gdrive_files[k]['size']

        if max_size > MAX_FILE_BYTES:
            continue

        if in_local and not in_gdrive:
            if mode in ('both', 'push'):
                to_push.append((k, 'Google Driveへ新規追加', False))
        elif in_gdrive and not in_local:
            if mode in ('both', 'pull'):
                to_pull.append((k, 'ローカルへ新規取得', False))
        else:
            loc = local_files[k]
            gdr = gdrive_files[k]
            if loc['sha256'] == gdr['sha256']:
                continue

            common = baseline.get(k) if baseline is not None else None
            push_safe = common is not None and gdr['sha256'] == common
            pull_safe = common is not None and loc['sha256'] == common

            if mode == 'push':
                to_push.append((k, f"更新 (size {loc['size']} vs {gdr['size']})", not push_safe))
            elif mode == 'pull':
                to_pull.append((k, f"更新 (size {gdr['size']} vs {loc['size']})", not pull_safe))
            else:  # both
                if push_safe:
                    to_push.append((k, 'ローカルのみ更新', False))
                elif pull_safe:
                    to_pull.append((k, 'Google Driveのみ更新', False))
                elif loc['mtime'] > gdr['mtime']:
                    to_push.append((k, f"ローカルが新しい ({loc['mtime']} > {gdr['mtime']})", True))
                elif gdr['mtime'] > loc['mtime']:
                    to_pull.append((k, f"Google Driveが新しい ({gdr['mtime']} > {loc['mtime']})", True))
                else:
                    to_pull.append((k, f"異内容（mtime同一、Google Driveを採用）", True))

    return to_push, to_pull

def _run_sync(args, local_dir, gdrive_dir):
    print(f"=== Google Drive同期開始 [{args.mode}] ===")
    print(f"ローカル: {local_dir}")
    print(f"Google Drive: {gdrive_dir}")

    if not gdrive_dir.exists():
        print(f"エラー: Google Driveディレクトリが存在しません: {gdrive_dir}", file=sys.stderr)
        sys.exit(1)

    baseline = load_baseline(local_dir, '.sync-state-gdrive.json')
    print("ファイル一覧を取得中...")
    local_files = scan_dir(local_dir)
    gdrive_files = scan_dir(gdrive_dir)

    to_push, to_pull = plan_sync(local_files, gdrive_files, mode=args.mode, baseline=baseline)

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
        expected = None if is_conflict else (gdrive_files[k]['sha256'] if k in gdrive_files else '')
        copy_file(local_dir / k, gdrive_dir / k, root_dir=gdrive_dir, rel_path=k,
                  is_conflict=is_conflict, src_root=local_dir, expected_dest_sha=expected)

    for idx, (k, reason, is_conflict) in enumerate(to_pull, 1):
        print(f"[{idx}/{len(to_pull)}] Pull: {k} ({reason})")
        expected = None if is_conflict else (local_files[k]['sha256'] if k in local_files else '')
        copy_file(gdrive_dir / k, local_dir / k, root_dir=local_dir, rel_path=k,
                  is_conflict=is_conflict, src_root=gdrive_dir, expected_dest_sha=expected)

    final_local = scan_dir(local_dir)
    final_gdrive = scan_dir(gdrive_dir)
    save_baseline(local_dir, '.sync-state-gdrive.json',
                  merged_baseline(baseline, final_local, final_gdrive))

    print("\n=== Google Drive同期完了 ===")

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--local', type=Path, help='ローカルデータディレクトリ')
    parser.add_argument('--gdrive', type=Path, default=DEFAULT_GDRIVE, help='Google Drive側データディレクトリ')
    parser.add_argument('--mode', choices=['both', 'push', 'pull'], default='both', help='同期モード')
    parser.add_argument('--dry-run', action='store_true', help='変更を行わず確認のみ')
    args = parser.parse_args()
    local_dir = (args.local if args.local is not None else data_root()).resolve()
    gdrive_dir = args.gdrive.resolve()
    if client_mode(local_dir):
        raise ValueError('NAS_CLIENT_MODE_FILE_SYNC_DISABLED: local file sync cannot use a NAS client root')
    with sync_lock(local_dir):
        _run_sync(args, local_dir, gdrive_dir)

if __name__ == '__main__':
    main()
