#!/usr/bin/env python3
"""
イカリング3アーカイブ 統合リモート同期 (Python版ラッパー)
sync_nas.py -> sync_gdrive.py の順に実行し、CLI引数をそのまま伝播する。
"""
from pathlib import Path
import subprocess
import sys


def run_sync(args: list[str] | None = None) -> int:
    if args is None:
        args = sys.argv[1:]

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
