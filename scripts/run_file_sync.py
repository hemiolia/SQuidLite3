#!/usr/bin/env python3
"""
イカリング3アーカイブ 統合リモート同期 (Python版ラッパー)
sync_nas.py -> sync_gdrive.py の順に実行し、CLI引数をそのまま伝播する。
"""
from pathlib import Path
import argparse
import subprocess
import sys
if __package__:
    from .data_root import client_mode, data_root
else:
    from data_root import client_mode, data_root


def run_sync(args: list[str] | None = None) -> int:
    if args is None:
        args = sys.argv[1:]
    local_parser = argparse.ArgumentParser(add_help=False)
    local_parser.add_argument('--local', type=Path)
    local_args, _ = local_parser.parse_known_args(args)
    local_root = local_args.local if local_args.local is not None else data_root()
    if client_mode(local_root):
        raise RuntimeError('NAS_CLIENT_MODE_FILE_SYNC_DISABLED: file sync cannot run from a NAS client root')

    script_dir = Path(__file__).resolve().parent
    nas_script = (script_dir / 'sync_nas.py').resolve()
    gdrive_script = (script_dir / 'sync_gdrive.py').resolve()

    cmd_nas = [sys.executable, str(nas_script)] + list(args)
    res_nas = subprocess.run(cmd_nas)
    if res_nas.returncode != 0:
        return res_nas.returncode

    cmd_gdrive = [sys.executable, str(gdrive_script)] + list(args)
    res_gdrive = subprocess.run(cmd_gdrive)
    return res_gdrive.returncode


def main() -> int:
    return run_sync(sys.argv[1:])


if __name__ == '__main__':
    sys.exit(main())
