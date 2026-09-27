#!/usr/bin/env python3
"""
macOS軽量ファイル定期同期LaunchAgent (local.ikaring3.file-sync) インストーラー
"""
import argparse
from datetime import datetime, timezone
import os
from pathlib import Path
import plistlib
import subprocess
import sys
from typing import Any
import uuid

SERVICE_LABEL = 'local.ikaring3.file-sync'
DEFAULT_START_INTERVAL = 300
DEFAULT_PLIST_DIR = Path.home() / 'Library' / 'LaunchAgents'
DEFAULT_PLIST_PATH = DEFAULT_PLIST_DIR / f'{SERVICE_LABEL}.plist'
DEFAULT_LOGS_DIR = Path.home() / 'Library' / 'Logs' / 'ikaring-archive-file-sync'


def get_repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def generate_plist_payload(
    repo_root: Path,
    logs_dir: Path | None = None,
    python_bin: str = sys.executable,
    interval: int = DEFAULT_START_INTERVAL,
    label: str = SERVICE_LABEL,
) -> dict[str, Any]:
    repo_root = repo_root.resolve()
    sync_script = (repo_root / 'scripts' / 'run_file_sync.py').resolve()
    if logs_dir is None:
        logs_dir = DEFAULT_LOGS_DIR
    logs_dir = logs_dir.resolve()

    return {
        'Label': label,
        'ProgramArguments': [
            python_bin,
            str(sync_script),
        ],
        'WorkingDirectory': str(repo_root),
        'RunAtLoad': True,
        'StartInterval': interval,
        'EnvironmentVariables': {
            'PATH': '/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin',
            'PYTHON': python_bin,
            'PYTHONUNBUFFERED': '1',
        },
        'StandardOutPath': str(logs_dir / 'stdout.log'),
        'StandardErrorPath': str(logs_dir / 'stderr.log'),
    }


def write_plist_file(
    dest_plist: Path,
    payload_bytes: bytes,
) -> tuple[bool, Path | None]:
    """
    plistファイルを書き込む。
    - 既存plistが存在し内容が同じなら再作成不要 (False, None)
    - 既存plistが存在し内容が異なる場合は同ディレクトリ内に時刻とuuid付き旧版を保存 (True, backup_path)
    - 新規作成の場合は (True, None)
    - atomic書き込みかつpermission 0600
    """
    if dest_plist.is_symlink():
        raise ValueError(f'LaunchAgent plist must not be a symlink: {dest_plist}')
    dest_plist = dest_plist.resolve()
    dest_dir = dest_plist.parent
    dest_dir.mkdir(parents=True, exist_ok=True, mode=0o700)

    backup_path: Path | None = None
    if dest_plist.exists():
        existing_bytes = dest_plist.read_bytes()
        if existing_bytes == payload_bytes:
            return False, None
        now_str = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
        uid_str = uuid.uuid4().hex[:8]
        backup_path = dest_dir / f"{dest_plist.name}.{now_str}_{uid_str}.bak"
        backup_path.write_bytes(existing_bytes)
        try:
            os.chmod(backup_path, 0o600)
        except OSError:
            pass

    tmp_file = dest_dir / f".{dest_plist.name}.tmp.{uuid.uuid4().hex[:8]}"
    try:
        fd = os.open(tmp_file, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, 'wb') as f:
            f.write(payload_bytes)
            f.flush()
            os.fsync(f.fileno())
        os.chmod(tmp_file, 0o600)
        os.replace(tmp_file, dest_plist)
    finally:
        if tmp_file.exists():
            try:
                tmp_file.unlink()
            except OSError:
                pass

    return True, backup_path


def register_launchctl(
    label: str,
    plist_path: Path,
    uid: int | None = None,
) -> None:
    """
    launchctl bootout -> enable -> bootstrap を実行する。
    - bootout: 既存無し許容（エラーでも続行）
    - enable: 失敗時例外送出（エラーexit非0）
    - bootstrap: 失敗時例外送出（エラーexit非0）
    - collector (local.ikaring3.archive) へは触らない
    """
    if uid is None:
        uid = os.getuid()

    target_domain = f"gui/{uid}"
    service_target = f"{target_domain}/{label}"

    # 1. bootout (既存無し許容、エラーでも続行)
    bootout_cmd = ['launchctl', 'bootout', service_target]
    subprocess.run(
        bootout_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )

    # 2. enable (失敗時エラーexit非0)
    enable_cmd = ['launchctl', 'enable', service_target]
    res_enable = subprocess.run(
        enable_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if res_enable.returncode != 0:
        err_msg = res_enable.stderr.decode('utf-8', errors='replace').strip()
        raise RuntimeError(
            f"launchctl enable に失敗しました (code: {res_enable.returncode}): {err_msg}"
        )

    # 3. bootstrap (失敗時エラーexit非0)
    bootstrap_cmd = ['launchctl', 'bootstrap', target_domain, str(plist_path.resolve())]
    res_bootstrap = subprocess.run(
        bootstrap_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if res_bootstrap.returncode != 0:
        err_msg = res_bootstrap.stderr.decode('utf-8', errors='replace').strip()
        raise RuntimeError(
            f"launchctl bootstrap に失敗しました (code: {res_bootstrap.returncode}): {err_msg}"
        )


def install_service(
    repo_root: Path,
    plist_path: Path | None = None,
    logs_dir: Path | None = None,
    run_launchctl: bool = True,
    uid: int | None = None,
) -> dict[str, Any]:
    if sys.platform != 'darwin':
        raise RuntimeError("LaunchAgentのインストールはmacOS (darwin) のみ対応しています")

    if plist_path is None:
        plist_path = DEFAULT_PLIST_PATH
    if logs_dir is None:
        logs_dir = DEFAULT_LOGS_DIR

    logs_dir = logs_dir.resolve()
    logs_dir.mkdir(parents=True, exist_ok=True, mode=0o700)

    payload = generate_plist_payload(
        repo_root=repo_root,
        logs_dir=logs_dir,
    )
    payload_bytes = plistlib.dumps(payload)

    written, backup = write_plist_file(plist_path, payload_bytes)

    if run_launchctl:
        register_launchctl(SERVICE_LABEL, plist_path, uid=uid)

    return {
        'label': SERVICE_LABEL,
        'plist': str(plist_path.resolve()),
        'written': written,
        'backup': str(backup.resolve()) if backup else None,
        'logs_dir': str(logs_dir),
    }


def main(argv: list[str] | None = None) -> int:
    if argv is None:
        argv = sys.argv[1:]

    parser = argparse.ArgumentParser(
        description="macOS軽量ファイル定期同期LaunchAgent (local.ikaring3.file-sync) インストーラー"
    )
    parser.add_argument(
        '--install',
        action='store_true',
        help="LaunchAgent (local.ikaring3.file-sync.plist) を作成・登録する",
    )
    parser.add_argument(
        '--print',
        action='store_true',
        dest='print_plist',
        help="生成されるplistの内容を標準出力に出力する",
    )
    # テスト・内部制御用オプション
    parser.add_argument(
        '--plist-path',
        type=Path,
        default=None,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        '--logs-dir',
        type=Path,
        default=None,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        '--repo-dir',
        type=Path,
        default=None,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        '--skip-launchctl',
        action='store_true',
        help=argparse.SUPPRESS,
    )

    if len(argv) == 0:
        parser.print_help()
        return 0

    args = parser.parse_args(argv)

    repo_root = (args.repo_dir or get_repo_root()).resolve()

    if args.print_plist:
        payload = generate_plist_payload(
            repo_root=repo_root,
            logs_dir=args.logs_dir or DEFAULT_LOGS_DIR,
        )
        sys.stdout.write(plistlib.dumps(payload).decode('utf-8'))
        return 0

    if args.install:
        if sys.platform != 'darwin':
            sys.stderr.write("エラー: LaunchAgentのインストールはmacOS (darwin) のみ対応しています。\n")
            return 1
        try:
            result = install_service(
                repo_root=repo_root,
                plist_path=args.plist_path or DEFAULT_PLIST_PATH,
                logs_dir=args.logs_dir or DEFAULT_LOGS_DIR,
                run_launchctl=not args.skip_launchctl,
            )
            print(f"インストール成功: {result['plist']}")
            if result['backup']:
                print(f"旧設定バックアップ: {result['backup']}")
            return 0
        except Exception as exc:
            sys.stderr.write(f"エラー: インストールに失敗しました: {exc}\n")
            return 1

    parser.print_help()
    return 0


if __name__ == '__main__':
    sys.exit(main())
