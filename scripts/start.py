#!/usr/bin/env python3
"""Download ZIP, install dependencies, authenticate locally, start collecting."""
import argparse, os, shutil, subprocess, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from scripts.data_root import data_root, marker_present
from scripts.nas_archive import read_marker


def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument('--check', action='store_true')
    p.add_argument('--no-login', action='store_true')
    p.add_argument('--static', action='store_true')
    a = p.parse_args(argv)

    if sys.version_info < (3, 10):
        raise RuntimeError('Python 3.10以上が必要です: https://www.python.org/downloads/')

    if marker_present(data_root()):
        read_marker()
        if a.check:
            if not shutil.which('ssh'):
                raise RuntimeError('sshコマンドが見つかりません。NASモードにはSSHクライアントが必要です。')
            print('NAS起動条件を満たしています。')
            return 0
        if a.static:
            nas_script = ROOT / 'scripts' / 'nas_archive.py'
            res = subprocess.run([sys.executable, str(nas_script), 'gui'])
            return res.returncode
        res = subprocess.run([sys.executable, str(ROOT / 'scripts' / 'desktop_app.py')])
        return res.returncode

    node = shutil.which('node')
    npm = shutil.which('npm')
    if not node or not npm:
        raise RuntimeError('Node.js 22以上をインストールしてください: https://nodejs.org/')
    major = int(subprocess.check_output([node, '-p', 'process.versions.node.split(".")[0]'], text=True))
    if major < 22:
        raise RuntimeError('Node.js 22以上に更新してください。')
    if a.check:
        print('Python/Node.js/npmの起動条件を満たしています。')
        return 0
    os.chdir(ROOT)
    os.environ['NODE'] = node
    # Invoke npm's JS CLI on Windows too, avoiding shell command construction.
    npm_script = Path(node).parent / 'node_modules/npm/bin/npm-cli.js'
    npm_command = [node, str(npm_script)] if npm_script.exists() else [npm]
    subprocess.run([*npm_command, 'ci', '--ignore-scripts', '--no-audit', '--no-fund'], check=True)
    command = [sys.executable, str(ROOT / 'archive.py')]
    if not a.no_login:
        subprocess.run([*command, 'login'], check=True)
    subprocess.run([*command, 'refresh-catalog'], check=True)
    subprocess.run([*command, 'sync'], check=True)
    if sys.platform == 'darwin':
        subprocess.run([*command, 'install-service'], check=True)
        print('2分間隔の自動収集を登録しました。保存先: ~/Documents/イカリング3アーカイブ')
    else:
        print('定期収集を開始します。この画面を開いたままにしてください。終了: Ctrl+C')
        subprocess.run([*command, 'watch'], check=True)
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (RuntimeError, subprocess.CalledProcessError, ValueError) as e:
        print(str(e), file=sys.stderr)
        sys.exit(1)
