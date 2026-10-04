#!/usr/bin/env python3
"""SQuidLite3 の端末の導入案内。Python 標準ライブラリのみ。

段と文言は scripts/setup_steps.py にある。ここは表示と、既存の処理を呼ぶことだけを行う。
"""
import argparse
import json
import os
import shutil
import subprocess
import sys
import webbrowser
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import setup_steps as S
from scripts.data_root import data_root, marker_present

STATE_SCHEMA = 1
MSG = S.MESSAGES


def _platform():
    return sys.platform


def say(text=''):
    print(text, flush=True)


def _utf8_stdout():
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, 'reconfigure', None)
        if reconfigure is not None:
            try:
                reconfigure(encoding='utf-8', errors='replace')
            except (OSError, ValueError):
                pass


class Quit(Exception):
    pass


# ---------- 状態ファイル ----------

def state_path(root):
    return Path(root) / 'config' / 'setup-state.json'


def load_state(root):
    try:
        data = json.loads(state_path(root).read_text(encoding='utf-8'))
    except (OSError, UnicodeError, ValueError):
        return {'schema_version': STATE_SCHEMA, 'completed': [], 'finished': False}
    if not isinstance(data, dict) or data.get('schema_version') != STATE_SCHEMA or not isinstance(data.get('completed'), list):
        return {'schema_version': STATE_SCHEMA, 'completed': [], 'finished': False}
    known = {s['id'] for s in S.STEPS}
    return {
        'schema_version': STATE_SCHEMA,
        'completed': [c for c in data['completed'] if c in known],
        'finished': data.get('finished') is True,
    }


def save_state(root, state):
    path = state_path(root)
    try:
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        tmp = path.with_name(path.name + '.tmp')
        tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
        os.replace(tmp, path)
    except OSError:
        say(MSG['state_not_saved'])


# ---------- 対話 ----------

class Wizard:
    def __init__(self, non_interactive=False):
        self.non_interactive = non_interactive

    def ask(self, choices):
        """番号で選ばせる。Enter だけならおすすめ。選ばれた選択肢の dict を返す。"""
        default = next(c for c in choices if c['recommended'])
        low, high = choices[0]['number'], choices[-1]['number']
        for c in choices:
            mark = MSG['recommended_mark'] if c['recommended'] else ''
            say('  {}. {}{}'.format(c['number'], c['label'], mark))
            say('     ' + c['description'])
        if self.non_interactive:
            say('おすすめを選びます: {}'.format(default['label']))
            return default
        prompt = MSG['prompt'].format(default=default['number'])
        while True:
            try:
                raw = input(prompt)
            except EOFError:
                say(MSG['eof_quit'])
                raise Quit()
            raw = raw.strip()
            if raw == '':
                return default
            for c in choices:
                if raw == str(c['number']):
                    return c
            say(MSG['invalid_choice'].format(low=low, high=high))

    def wait_enter(self):
        if self.non_interactive:
            return
        try:
            input(MSG['prompt_wait_enter'])
        except EOFError:
            raise Quit()

    def header(self, step):
        say()
        say(MSG['step_header'].format(number=step['number'], total=S.STEP_COUNT, heading=step['heading']))
        say(step['summary'])
        if step.get('duration_note'):
            say(step['duration_note'])
        say()

    def show_help(self, step):
        say(MSG['help_title'])
        for line in step.get('help', ()):
            say('  ' + line)
        say()


# ---------- 既存処理の呼び出し ----------

def _env():
    env = dict(os.environ)
    node = shutil.which('node')
    if node:
        env['NODE'] = node
    return env


def run_child(command):
    """子プロセスの出力はそのまま流す。戻り値は終了コード。"""
    try:
        return subprocess.run([str(c) for c in command], cwd=str(ROOT), env=_env()).returncode
    except OSError:
        return 127


def archive_command(*args):
    return [sys.executable, str(ROOT / 'archive.py'), *args]


def npm_command():
    node = shutil.which('node')
    npm = shutil.which('npm')
    if node:
        script = Path(node).parent / 'node_modules' / 'npm' / 'bin' / 'npm-cli.js'
        if script.exists():
            return [node, str(script)]
    return [npm]


def missing_prerequisites():
    """足りないものの文言と入手先を返す。足りていれば空。"""
    missing = []
    if sys.version_info < (3, 10):
        missing.append((MSG['prerequisite_python'], S.PREREQUISITE_DOWNLOAD_URLS[0]))
    node = shutil.which('node')
    npm = shutil.which('npm')
    major = 0
    if node and npm:
        try:
            res = subprocess.run([node, '-p', 'process.versions.node.split(".")[0]'],
                                 capture_output=True, text=True)
            major = int((res.stdout or '').strip())
        except (OSError, ValueError):
            major = 0
    if major < 22:
        missing.append((MSG['prerequisite_node'], S.PREREQUISITE_DOWNLOAD_URLS[1]))
    return missing


def step_by_id(step_id):
    return next(s for s in S.STEPS if s['id'] == step_id)


# 各段の実行。'ok' / 'fail' / 'quit' を返す。

def do_prerequisites(w, step, choice):
    if choice['key'] == 'quit':
        return 'quit'
    while True:
        missing = missing_prerequisites()
        if not missing:
            break
        for text, _url in missing:
            say(text)
        if w.non_interactive:
            return 'fail'
        pick = w.ask(S.PREREQUISITE_MISSING_CHOICES)
        if pick['key'] == 'quit':
            return 'quit'
        for _text, url in missing:
            try:
                webbrowser.open(url)
            except Exception:
                say(url)
        say(MSG['prerequisite_opened'])
        w.wait_enter()
    say(MSG['prerequisite_ok'])
    code = run_child([*npm_command(), 'ci', '--ignore-scripts', '--no-audit', '--no-fund'])
    return 'ok' if code == 0 else _failed(code)


_last_code = [0]


def _failed(code):
    _last_code[0] = code
    return 'fail'


def do_storage(w, step, choice):
    if choice['key'] == 'nas':
        say(step['nas_not_ready'])
    say('{}: {}'.format(step['location_label'], data_root()))
    return 'ok'


def do_login(w, step, choice):
    if choice['key'] == 'quit':
        return 'quit'
    for i, line in enumerate(step['procedure'], 1):
        say('  {}. {}'.format(i, line))
    say(step['password_note'])
    say()
    code = run_child(archive_command('login'))
    return 'ok' if code == 0 else _failed(code)


def do_first_fetch(w, step, choice):
    if choice['key'] == 'quit':
        return 'quit'
    for command in (archive_command('refresh-catalog'), archive_command('sync')):
        code = run_child(command)
        if code != 0:
            return _failed(code)
    return 'ok'


def do_auto_fetch(w, step, choice):
    if choice['key'] == 'manual':
        say(step['manual_note'])
        return 'ok'
    if _platform() == 'darwin':
        code = run_child(archive_command('install-service'))
        if code != 0:
            return _failed(code)
        say(step['installed_note_darwin'])
    else:
        say(step['watch_note_other'])
        say('  ' + step['watch_command'])
    return 'ok'


def open_viewer():
    return run_child([sys.executable, str(ROOT / 'scripts' / 'desktop_app.py')])


def do_open_viewer(w, step, choice):
    if choice['key'] == 'skip':
        return 'ok'
    code = open_viewer()
    return 'ok' if code == 0 else _failed(code)


def do_drive(w, step, choice):
    if choice['key'] == 'yes':
        say(step['not_ready'])
    else:
        say(MSG['drive_skipped'])
    return 'ok'


RUNNERS = {
    'prerequisites': do_prerequisites,
    'storage': do_storage,
    'login': do_login,
    'first_fetch': do_first_fetch,
    'auto_fetch': do_auto_fetch,
    'open_viewer': do_open_viewer,
    'drive': do_drive,
}


def run_step(w, step):
    """一つの段を、失敗したら選択肢を出しながら完了まで進める。'ok' か 'quit' か 'fail'。"""
    w.header(step)
    if step['id'] == 'open_viewer':
        say(step['phone_note'])
    choice = w.ask(step['choices'])
    while True:
        result = RUNNERS[step['id']](w, step, choice)
        if result in ('ok', 'quit'):
            return result
        say(MSG['step_failed'].format(heading=step['heading']))
        say(MSG['step_failed_exit'].format(code=_last_code[0]))
        if w.non_interactive:
            return 'fail'
        while True:
            pick = w.ask(S.FAILURE_CHOICES)
            if pick['key'] == 'retry':
                break
            if pick['key'] == 'help':
                w.show_help(step)
                continue
            return 'quit'


# ---------- 全体 ----------

def run_install(w, root, use_state):
    state = load_state(root) if use_state else {'schema_version': STATE_SCHEMA, 'completed': [], 'finished': False}
    if state['completed']:
        say(MSG['resume_note'])
    for step in S.STEPS:
        if step['id'] in state['completed']:
            continue
        result = run_step(w, step)
        if result == 'quit':
            say(MSG['quit_done'])
            return 0
        if result == 'fail':
            return 1
        state['completed'].append(step['id'])
        if use_state:
            save_state(root, state)
    state['finished'] = True
    if use_state:
        save_state(root, state)
    say()
    say(MSG['all_done'])
    return 0


def run_menu(w, menu, nas):
    while True:
        say()
        say(menu['title'])
        key = w.ask(menu['choices'])['key']
        if key == 'quit':
            say(MSG['quit_menu_done'])
            return 0
        if key == 'viewer':
            code = open_viewer()
            if code != 0:
                say(MSG['viewer_failed'])
            return code
        if key == 'status':
            if nas:
                run_child([sys.executable, str(ROOT / 'scripts' / 'nas_archive.py'), 'status'])
            else:
                run_child(archive_command('status'))
        elif key == 'settings':
            for step_id in ('storage', 'auto_fetch'):
                if run_step(w, step_by_id(step_id)) != 'ok':
                    break
        elif key == 'relogin':
            run_step(w, step_by_id('login'))
        if w.non_interactive:
            return 0


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--non-interactive', action='store_true')
    args = parser.parse_args(argv)
    _utf8_stdout()
    w = Wizard(non_interactive=args.non_interactive)
    try:
        root = data_root()
        if marker_present(root):
            return run_menu(w, S.NAS_MENU, nas=True)
        state = load_state(root)
        if state['finished']:
            return run_menu(w, S.RETURNING_MENU, nas=False)
        return run_install(w, root, use_state=True)
    except Quit:
        say(MSG['quit_done'])
        return 0
    except ValueError as exc:
        say(str(exc))
        return 1


if __name__ == '__main__':
    sys.exit(main())
