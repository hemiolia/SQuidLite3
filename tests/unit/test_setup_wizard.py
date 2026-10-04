import io
import json
import os
import tempfile
import unicodedata
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from scripts import setup_steps, setup_wizard


class WizardTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.calls = []
        self.fail_on = None  # コマンドに含まれるこの語で失敗させる
        self.patches = [
            patch.dict(os.environ, {'IKARING_ARCHIVE_DATA_DIR': str(self.root)}),
            patch('scripts.setup_wizard.subprocess.run', side_effect=self.fake_run),
            patch('scripts.setup_wizard.shutil.which', side_effect=lambda n: '/usr/bin/' + n),
            patch('scripts.setup_wizard._platform', return_value='darwin'),
            patch('scripts.setup_wizard.webbrowser.open'),
        ]
        for p in self.patches:
            p.start()
            self.addCleanup(p.stop)

    def fake_run(self, command, **kwargs):
        if len(command) > 1 and command[1] == '-p':
            return SimpleNamespace(returncode=0, stdout='22\n', stderr='')
        self.calls.append(list(command))
        if self.fail_on and self.fail_on in command:
            return SimpleNamespace(returncode=1, stdout='', stderr='')
        return SimpleNamespace(returncode=0, stdout='', stderr='')

    def run_main(self, inputs, argv=()):
        out = io.StringIO()
        with patch('builtins.input', side_effect=list(inputs)), redirect_stdout(out):
            code = setup_wizard.main(list(argv))
        return code, out.getvalue()

    def names(self):
        return [Path(c[1]).name if c[0].endswith('python3') or c[0].endswith('python') or 'python' in Path(c[0]).name else c[0] for c in self.calls]

    def tail_args(self):
        result = []
        for c in self.calls:
            if 'ci' in c:
                result.append('ci')
            elif Path(c[1]).name == 'desktop_app.py':
                result.append('desktop_app')
            else:
                result.append(c[2])
        return result

    def test_a_enter_only_runs_recommended_order(self):
        code, out = self.run_main([''] * 7)
        self.assertEqual(code, 0)
        self.assertEqual(self.tail_args(), ['ci', 'login', 'refresh-catalog', 'sync', 'install-service', 'desktop_app'])
        for n in range(1, 8):
            self.assertIn('{}/全7段'.format(n), out)
        state = json.loads((self.root / 'config' / 'setup-state.json').read_text(encoding='utf-8'))
        self.assertTrue(state['finished'])

    def test_a2_non_interactive_same(self):
        code, _ = self.run_main([], ['--non-interactive'])
        self.assertEqual(code, 0)
        self.assertEqual(self.tail_args(), ['ci', 'login', 'refresh-catalog', 'sync', 'install-service', 'desktop_app'])

    def test_a3_other_platform_guides_watch(self):
        with patch('scripts.setup_wizard._platform', return_value='win32'):
            code, out = self.run_main([''] * 7)
        self.assertNotIn('install-service', self.tail_args())
        self.assertIn(setup_steps.STEPS[4]['watch_command'], out)

    def test_b_failure_quit_then_resume(self):
        self.fail_on = 'sync'
        # 段1..3 は Enter、段4 は Enter のあと失敗 -> 3 (やめる)
        code, out = self.run_main(['', '', '', '', '3'])
        self.assertEqual(code, 0)
        self.assertIn('もう一度試す', out)
        self.assertIn('やり方を見る', out)
        state = json.loads((self.root / 'config' / 'setup-state.json').read_text(encoding='utf-8'))
        self.assertEqual(state['completed'], ['prerequisites', 'storage', 'login'])
        self.calls.clear()
        self.fail_on = None
        code, out = self.run_main([''] * 4)
        self.assertEqual(code, 0)
        self.assertEqual(self.tail_args(), ['refresh-catalog', 'sync', 'install-service', 'desktop_app'])
        self.assertIn('4/全7段', out)
        self.assertNotIn('1/全7段', out)

    def test_b2_retry_and_help(self):
        self.fail_on = 'sync'
        calls_before = None

        def go():
            self.fail_on = None
        # 失敗 -> やり方を見る(2) -> もう一度試す(Enter)。再試行の前に失敗条件を外す。
        seq = ['', '', '', '', '2']

        def inputs():
            for x in seq:
                yield x
            go()
            yield ''
            for _ in range(3):
                yield ''
        with patch('builtins.input', side_effect=inputs()), redirect_stdout(io.StringIO()) as out:
            code = setup_wizard.main([])
        self.assertEqual(code, 0)
        self.assertIn('やり方', out.getvalue())

    def test_c_nas_marker_shows_menu_only(self):
        marker = self.root / 'config' / 'storage-location.json'
        marker.parent.mkdir(parents=True)
        marker.write_text('{}', encoding='utf-8')
        code, out = self.run_main([''])
        self.assertEqual(code, 0)
        self.assertEqual(self.tail_args(), ['desktop_app'])
        self.assertNotIn('全7段', out)
        self.assertFalse((self.root / 'config' / 'setup-state.json').exists())

    def test_c2_nas_status(self):
        marker = self.root / 'config' / 'storage-location.json'
        marker.parent.mkdir(parents=True)
        marker.write_text('{}', encoding='utf-8')
        self.run_main(['2', '3'])
        self.assertEqual([Path(c[1]).name + ' ' + c[2] for c in self.calls], ['nas_archive.py status'])

    def test_c3_returning_menu_local(self):
        (self.root / 'config').mkdir(parents=True)
        (self.root / 'config' / 'setup-state.json').write_text(
            json.dumps({'schema_version': 1, 'completed': [s['id'] for s in setup_steps.STEPS], 'finished': True}),
            encoding='utf-8')
        code, out = self.run_main(['4', '5'])
        self.assertEqual(self.tail_args(), ['status'])
        self.assertIn('設定を変える', out)
        self.assertIn('認証をやり直す', out)
        self.calls.clear()
        self.run_main(['2', '', '', '5'])
        self.assertEqual(self.tail_args(), ['install-service'])
        self.calls.clear()
        self.run_main(['3', '', '5'])
        self.assertEqual(self.tail_args(), ['login'])

    def test_d_no_emoji_or_decoration(self):
        _, out1 = self.run_main([''] * 7)
        self.calls.clear()
        _, out2 = self.run_main(['4', '5'])
        self.fail_on = 'sync'
        for e in (out1, out2, repr(setup_steps.STEPS), repr(setup_steps.MESSAGES)):
            for ch in e:
                self.assertNotEqual(unicodedata.category(ch), 'So', repr(ch))

    def test_e_all_text_from_steps_module(self):
        _, out = self.run_main([''] * 7)
        for step in setup_steps.STEPS:
            self.assertIn(step['heading'], out)
            self.assertIn(step['summary'], out)
            for c in step['choices']:
                self.assertIn(c['label'], out)
        self.assertIn(setup_steps.STEPS[1]['location_label'], out)
        self.assertIn(setup_steps.MESSAGES['all_done'], out)
        source = Path(setup_wizard.__file__).read_text(encoding='utf-8')
        for step in setup_steps.STEPS:
            self.assertNotIn(step['heading'], source)
            self.assertNotIn(step['summary'], source)

    def test_drive_yes_not_ready_then_continues(self):
        code, out = self.run_main([''] * 6 + ['2'])
        self.assertIn('この版ではまだ案内できません。docs/operations/使い方.md を見てください', out)
        self.assertEqual(code, 0)

    def test_prerequisite_missing_non_interactive_fails(self):
        with patch('scripts.setup_wizard.shutil.which', return_value=None):
            code, out = self.run_main([], ['--non-interactive'])
        self.assertEqual(code, 1)
        self.assertIn('Node.js 22 以上が見つかりません', out)


if __name__ == '__main__':
    unittest.main()
