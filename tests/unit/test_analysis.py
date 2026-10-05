import base64, json, re, tempfile, unittest, zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'src/python'))
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from ikarchive.store import Store, now, js
from ikarchive.gui import contrast, ON_FIELD, FIELD, INK, PAPER, LINK
from ikarchive.planner import Planner
import archive

ROOT = Path(__file__).resolve().parents[2]
MANIFEST = json.loads((ROOT / 'config/query-catalog.snapshot.json').read_text())

def encoded(text):
    return base64.b64encode(text.encode()).decode()

def vs_id(mode, suffix):
    return encoded(f'VsHistoryDetail-u-analysis:{mode}:20260922T010101_{suffix}')

def coop_id(suffix):
    return encoded(f'CoopHistoryDetail-u-analysis:{suffix}')

def team(count):
    return {'players': [{'id': f'p{i}'} for i in range(count)]}

def vs_detail(mode, suffix, counts=(4, 4), bankara=None, rule='AREA'):
    mine, *others = counts
    detail = {
        'id': vs_id(mode, suffix),
        'vsMode': {'mode': mode},
        'vsRule': {'rule': rule, 'name': rule},
        'myTeam': team(mine),
        'otherTeams': [team(n) for n in others],
        'judgement': 'WIN',
        'playedTime': '2026-09-22T01:01:01Z',
    }
    if bankara:
        detail['bankaraMatch'] = {'mode': bankara}
    return detail

def response(op, body, account='account-a'):
    raw = js(body).encode()
    return {
        'event_id': encoded(op + body['data'][next(iter(body['data']))]['id']),
        'account': account,
        'fetched_at': now(),
        'operation': op,
        'variables': {},
        'status': 200,
        'body_base64': base64.b64encode(raw).decode(),
    }

class AnalysisTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.temp_root = Path(self.tmp.name).resolve()
        self.store = Store(self.temp_root / 'a.sqlite')
        self.planner = Planner(MANIFEST)

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def ingest(self, op, body):
        event = response(op, body)
        rid = self.store.record(event)
        self.store.project(rid, self.planner)
        return rid

    def sets(self):
        return {row['analysis_set'] for row in self.store.db.execute('SELECT analysis_set FROM match_classification')}

    def test_public_modes_stay_together_when_roster_is_short(self):
        self.ingest('VsHistoryDetailQuery', {'data': {'vsHistoryDetail': vs_detail('REGULAR', 'nav', (4, 3), rule='TURF_WAR')}})
        self.ingest('VsHistoryDetailQuery', {'data': {'vsHistoryDetail': vs_detail('BANKARA', 'open', (4, 4), bankara='OPEN', rule='AREA')}})
        self.ingest('VsHistoryDetailQuery', {'data': {'vsHistoryDetail': vs_detail('BANKARA', 'challenge', (3, 4), bankara='CHALLENGE', rule='LOFT')}})
        self.ingest('VsHistoryDetailQuery', {'data': {'vsHistoryDetail': vs_detail('X_MATCH', 'x', (4, 4), rule='GOAL')}})
        self.ingest('VsHistoryDetailQuery', {'data': {'vsHistoryDetail': vs_detail('LEAGUE', 'event', (4, 4), rule='CLAM')}})
        self.ingest('VsHistoryDetailQuery', {'data': {'vsHistoryDetail': vs_detail('FEST', 'fest', (4, 2, 2), rule='TURF_WAR')}})
        self.assertEqual(self.sets(), {'nawabari', 'bankara_open', 'bankara_challenge', 'xmatch', 'event', 'fest'})
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM analysis_nawabari').fetchone()[0], 1)
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM analysis_fest').fetchone()[0], 1)
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM analysis_nawabari WHERE rule_raw!='TURF_WAR'").fetchone()[0], 0)
        self.assertFalse(self.store.db.execute("SELECT 1 FROM sqlite_master WHERE type='view' AND name='analysis_nawabari_one_vs_one'").fetchone())

    def test_only_private_splits_by_roster_and_tags_do_not_move_it(self):
        self.ingest('VsHistoryDetailQuery', {'data': {'vsHistoryDetail': vs_detail('PRIVATE', 'p4', (4, 4), rule='AREA')}})
        self.ingest('VsHistoryDetailQuery', {'data': {'vsHistoryDetail': vs_detail('PRIVATE', 'p3', (3, 3), rule='AREA')}})
        self.ingest('VsHistoryDetailQuery', {'data': {'vsHistoryDetail': vs_detail('PRIVATE', 'p2', (2, 2), rule='LOFT')}})
        self.ingest('VsHistoryDetailQuery', {'data': {'vsHistoryDetail': vs_detail('PRIVATE', 'p1', (1, 1), rule='AREA')}})
        self.ingest('VsHistoryDetailQuery', {'data': {'vsHistoryDetail': vs_detail('PRIVATE', 'px', (4, 3), rule='AREA')}})
        self.assertEqual(self.sets(), {'private_four_vs_four', 'private_three_vs_three', 'private_two_vs_two', 'private_one_vs_one', 'private_other'})
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM match_tags WHERE tag='イカップル'").fetchone()[0], 0)
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM analysis_bankara_open').fetchone()[0], 0)
        key = self.store.db.execute("SELECT match_key FROM match_classification WHERE analysis_set='private_four_vs_four'").fetchone()[0]
        archive.apply_tag(self.store, type('A', (), {'tag_action': 'add', 'account': 'account-a', 'match_key': key, 'tag': '対抗戦', 'note': None})())
        self.assertEqual(self.store.db.execute("SELECT analysis_set FROM match_classification WHERE match_key=?", (key,)).fetchone()[0], 'private_four_vs_four')
        tagged = self.store.db.execute('SELECT tag FROM analysis_private_four_vs_four_tags').fetchone()[0]
        self.assertEqual(tagged, '対抗戦')

    def test_salmon_special_modes_are_separate(self):
        for rule, suffix in (('REGULAR', 'regular'), ('BIG_RUN', 'big'), ('TEAM_CONTEST', 'team')):
            detail = {'id': coop_id(suffix), 'rule': rule, 'playedTime': '2026-09-22T01:01:01Z', 'dangerRate': 0}
            self.ingest('CoopHistoryDetailQuery', {'data': {'coopHistoryDetail': detail}})
        self.assertEqual(self.sets(), {'salmon_regular', 'big_run', 'team_contest'})
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM analysis_big_run').fetchone()[0], 1)
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM analysis_team_contest').fetchone()[0], 1)
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM analysis_salmon_regular').fetchone()[0], 1)
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM analysis_fest').fetchone()[0], 0)

    def test_public_fixture_open_is_not_private(self):
        body = json.loads((ROOT / 'tests/fixtures/public/VsHistoryDetailQuery.json').read_text())
        self.ingest('VsHistoryDetailQuery', body)
        genres = {row[0] for row in self.store.db.execute('SELECT genre FROM match_classification')}
        self.assertIn('bankara_open', genres)
        self.assertNotIn('private', genres)

    def test_reauth_rewalks_history_without_duplicating_the_match(self):
        body = {'data': {'vsHistoryDetail': vs_detail('PRIVATE', 'dup', (2, 2))}}
        self.ingest('VsHistoryDetailQuery', body)
        again = response('VsHistoryDetailQuery', body)
        again['event_id'] = encoded('second-identical-body')
        rid = self.store.record(again)
        self.store.project(rid, self.planner)
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM responses').fetchone()[0], 1)
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM matches').fetchone()[0], 1)
        self.assertEqual(self.store.db.execute("SELECT analysis_set FROM match_classification").fetchone()[0], 'private_two_vs_two')
        self.store.queue('account-a', 'PrivateBattleHistoriesQuery', {})
        self.store.db.execute("UPDATE jobs SET state='done' WHERE operation='PrivateBattleHistoriesQuery'")
        # 詳細が保存済みの試合の詳細の仕事と、一覧ではない履歴（名前に History を含む）の仕事は、遡りで戻さない
        key = self.store.db.execute('SELECT match_key FROM matches').fetchone()[0]
        self.store.queue('account-a', 'VsHistoryDetailQuery', {'vsResultId': 'saved-detail'}, 'vs', key)
        self.store.queue('account-a', 'HeroHistoryQuery', {})
        self.store.db.execute("UPDATE jobs SET state='done' WHERE operation IN ('VsHistoryDetailQuery','HeroHistoryQuery')")
        self.store.db.commit()
        expiry = (datetime.now(timezone.utc) + timedelta(days=700)).timestamp() * 1000
        self.store.remember_auth({'session_iat': 100, 'session_expires_at': expiry})
        self.store.remember_auth_failure('SESSION_EXPIRED')
        self.store.remember_auth({'session_iat': 200, 'session_expires_at': expiry})
        self.assertTrue(self.store.auth_status()['backfill_armed'])
        self.assertGreaterEqual(self.store.apply_backfill('account-a'), 1)
        self.assertEqual(self.store.db.execute("SELECT state FROM jobs WHERE operation='PrivateBattleHistoriesQuery'").fetchone()[0], 'pending')
        self.assertEqual(self.store.db.execute("SELECT state FROM jobs WHERE operation='VsHistoryDetailQuery'").fetchone()[0], 'done')
        self.assertEqual(self.store.db.execute("SELECT state FROM jobs WHERE operation='HeroHistoryQuery'").fetchone()[0], 'done')
        self.assertEqual(self.store.apply_backfill('account-a'), 0)
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM matches').fetchone()[0], 1)

    def test_xlsx_separates_genres_and_does_not_store_raw_json(self):
        from ikarchive.xlsx_export import export_xlsx
        self.ingest('VsHistoryDetailQuery', {'data': {'vsHistoryDetail': vs_detail('BANKARA', 'openxlsx', (4, 4), bankara='OPEN', rule='AREA')}})
        self.ingest('VsHistoryDetailQuery', {'data': {'vsHistoryDetail': vs_detail('X_MATCH', 'xxlsx', (4, 4), rule='GOAL')}})
        self.ingest('VsHistoryDetailQuery', {'data': {'vsHistoryDetail': vs_detail('PRIVATE', 'pairxlsx', (2, 2), rule='LOFT')}})
        path = self.temp_root / '分析.xlsx'
        result = export_xlsx(self.store, path)
        self.assertFalse(result['edits_return_to_database'])
        self.assertEqual(result['canonical'], 'sqlite')
        self.assertEqual(result['rows']['バンカラマッチ（オープン）'], 1)
        self.assertEqual(result['rows']['Xマッチ'], 1)
        self.assertEqual(result['rows']['プライベートマッチ（2対2）'], 1)
        with zipfile.ZipFile(path) as book:
            workbook = book.read('xl/workbook.xml').decode()
            names = [part.split('"', 1)[0] for part in workbook.split('name="')[1:]]
            texts = {name: book.read(f'xl/worksheets/sheet{index}.xml').decode() for index, name in enumerate(names, 1)}
        self.assertIn('openxlsx', texts['バンカラマッチ（オープン）'])
        self.assertNotIn('openxlsx', texts['Xマッチ'])
        self.assertIn('xxlsx', texts['Xマッチ'])
        self.assertNotIn('xxlsx', texts['バンカラマッチ（オープン）'])
        self.assertIn('pairxlsx', texts['プライベートマッチ（2対2）'])
        self.assertNotIn('pairxlsx', texts['プライベートマッチ（4対4）'])
        self.assertNotIn('イカップル', texts['プライベートマッチ（2対2）'])
        for xml in texts.values():
            self.assertNotIn('json_text', xml)
            for text in re.findall(r'<t xml:space="preserve">(.*?)</t>', xml):
                self.assertLess(len(text), 32767)

    def test_xlsx_bytes_stay_the_same_when_the_tables_do(self):
        from ikarchive.xlsx_export import export_xlsx
        self.ingest('VsHistoryDetailQuery', {'data': {'vsHistoryDetail': vs_detail('BANKARA', 'stablexlsx', (4, 4), bankara='OPEN', rule='AREA')}})
        first = self.temp_root / 'first.xlsx'
        second = self.temp_root / 'second.xlsx'
        export_xlsx(self.store, first)
        export_xlsx(self.store, second)
        self.assertEqual(first.read_bytes(), second.read_bytes())
        with zipfile.ZipFile(first) as book:
            for info in book.infolist():
                self.assertEqual(info.date_time, (1980, 1, 1, 0, 0, 0))

    def test_session_failure_is_visible_and_cleared_by_later_success(self):
        later = (datetime.now(timezone.utc) + timedelta(days=700)).timestamp() * 1000
        bullet = (datetime.now(timezone.utc) + timedelta(hours=2)).timestamp() * 1000
        self.store.remember_auth({'session_expires_at': later, 'bullet_expires_at': bullet})
        status = self.store.auth_status()
        self.assertFalse(status['reauth_required'])
        self.assertFalse(status['session_expires_soon'])
        self.assertIsNotNone(status['session_expires_at'])
        self.store.remember_auth_failure('SESSION_EXPIRED')
        self.assertTrue(self.store.auth_status()['reauth_required'])
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM issues WHERE code='AUTH_INCIDENT'").fetchone()[0], 1)
        soon = (datetime.now(timezone.utc) + timedelta(days=3)).timestamp() * 1000
        self.store.remember_auth({'session_expires_at': soon, 'bullet_expires_at': bullet})
        status = self.store.auth_status()
        self.assertFalse(status['reauth_required'])
        self.assertTrue(status['session_expires_soon'])

    def test_every_rate_like_number_is_kept_and_combat_stats_are_not(self):
        self.assertGreaterEqual(contrast(ON_FIELD, FIELD), 4.5)
        self.assertGreaterEqual(contrast(INK, PAPER), 4.5)
        self.assertGreaterEqual(contrast(LINK, PAPER), 4.5)
        challenge = vs_detail('BANKARA', 'wp', (4, 4), bankara='CHALLENGE', rule='AREA')
        challenge['bankaraMatch']['bankaraPower'] = {'power': 2100, 'weaponPower': 1980}
        challenge['player'] = {'result': {'kill': 9}}
        challenge['surprisePower'] = 1234
        self.ingest('VsHistoryDetailQuery', {'data': {'vsHistoryDetail': challenge}})
        xmatch = vs_detail('X_MATCH', 'xp', (4, 4), rule='GOAL')
        xmatch['xMatch'] = {'lastXPower': 2400.5, 'entireXPower': 2300}
        self.ingest('VsHistoryDetailQuery', {'data': {'vsHistoryDetail': xmatch}})
        job = {'id': coop_id('eggs'), 'rule': 'BIG_RUN', 'playedTime': '2026-09-22T02:00:00Z', 'jobRate': 120, 'jobScore': 140,
               'myResult': {'goldenDeliverCount': 40}, 'memberResults': [{'goldenDeliverCount': 99}],
               'waveResults': [{'teamDeliverCount': 20}, {'teamDeliverCount': 22}]}
        self.ingest('CoopHistoryDetailQuery', {'data': {'coopHistoryDetail': job}})
        for suffix, when in (('a', '2026-09-22T03:00:00Z'), ('b', '2026-09-22T03:01:00Z'), ('c', '2026-09-22T03:02:00Z')):
            detail = vs_detail('REGULAR', 'form'+suffix, (4, 4), rule='TURF_WAR')
            detail['playedTime'] = when
            detail['judgement'] = 'WIN' if suffix != 'c' else 'LOSE'
            self.ingest('VsHistoryDetailQuery', {'data': {'vsHistoryDetail': detail}})
        labels = {row[0] for row in self.store.db.execute('SELECT label FROM rate_points')}
        self.assertIn('ブキチャレパワー', labels)
        self.assertIn('バンカラパワー', labels)
        self.assertIn('Xパワー', labels)
        self.assertIn('surprisePower', labels)
        self.assertNotIn('kill', {row[0] for row in self.store.db.execute("SELECT series_id FROM rate_points")})
        self.assertEqual(self.store.db.execute("SELECT value FROM rate_points WHERE label='集めた金イクラ'").fetchone()[0], 40)
        self.assertEqual(self.store.db.execute("SELECT priority FROM rate_points WHERE label='評価レート'").fetchone()[0], 'secondary')
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM rate_points WHERE source='derived_judgement'").fetchone()[0], 0)
        from ikarchive.gui import write_gui
        page = self.temp_root / 'index.html'
        write_gui(self.store, page)
        text = page.read_text(encoding='utf-8')
        self.assertIn('ブキチャレパワー', text)
        self.assertNotIn('チョーシ', text)
        self.assertIn('surprisePower', text)
        self.assertIn(FIELD, text)
        self.assertIn('<table>', text)

    def test_gui_font_embedding_and_fallback(self):
        from ikarchive.gui import BUNDLED_FONT, write_gui
        self.assertTrue(BUNDLED_FONT.is_file())

        # 0. リポジトリ同梱フォントが既定で完全埋め込みされる。
        page_default = self.temp_root / 'default_font.html'
        write_gui(self.store, page_default)
        html_default = page_default.read_text(encoding='utf-8')
        default_match = re.search(r"url\('data:font/otf;base64,([A-Za-z0-9+/=]+)'\)", html_default)
        self.assertIsNotNone(default_match, 'bundled font data URL not found in HTML')
        self.assertEqual(base64.b64decode(default_match.group(1)), BUNDLED_FONT.read_bytes())

        # 1. フォント完全埋め込みの検査: 一時ファイルに任意のバイト列を書き、data URLデコード一致を検査
        dummy_font_bytes = b'\x00\x01\x00\x00arbitrary-splatoon2-unified-otf-data\xfe\xff'
        dummy_font_path = self.temp_root / 'Splatoon2-Unified.otf'
        dummy_font_path.write_bytes(dummy_font_bytes)

        page_with_font = self.temp_root / 'with_font.html'
        write_gui(self.store, page_with_font, font_path=dummy_font_path)
        html_with_font = page_with_font.read_text(encoding='utf-8')

        match = re.search(r"url\('data:font/otf;base64,([A-Za-z0-9+/=]+)'\)", html_with_font)
        self.assertIsNotNone(match, 'base64 data URL font-face not found in HTML')
        decoded_bytes = base64.b64decode(match.group(1))
        self.assertEqual(decoded_bytes, dummy_font_bytes)
        self.assertIn('@font-face', html_with_font)
        self.assertIn("'Splatoon2-Unified'", html_with_font)
        self.assertFalse(re.search(r'(?<!sans-)serif\b', html_with_font), 'Standalone serif found in HTML')

        # 2. フォント無し時の sans-serif フォールバック検査
        page_no_font = self.temp_root / 'no_font.html'
        write_gui(self.store, page_no_font, font_path=self.temp_root / 'nonexistent.otf')
        html_no_font = page_no_font.read_text(encoding='utf-8')

        self.assertNotIn('@font-face', html_no_font)
        self.assertIn('system-ui, "Hiragino Kaku Gothic ProN", "Yu Gothic", Meiryo, sans-serif', html_no_font)
        self.assertFalse(re.search(r'(?<!sans-)serif\b', html_no_font), 'Standalone serif found in HTML')

    def test_gui_graph_elements_and_series_summary(self):
        from ikarchive.gui import write_gui
        # 複数点系列 (バンカラパワーで0を跨ぐ複数点): -10, 0, 10
        for suffix, when, val in (('a', '2026-09-22T03:00:00Z', -10), ('b', '2026-09-22T03:01:00Z', 0), ('c', '2026-09-22T03:02:00Z', 10)):
            detail = vs_detail('BANKARA', 'multi_ui_' + suffix, (4, 4), bankara='CHALLENGE', rule='AREA')
            detail['playedTime'] = when
            detail['bankaraMatch'] = {'mode': 'CHALLENGE', 'bankaraPower': {'power': val}}
            self.ingest('VsHistoryDetailQuery', {'data': {'vsHistoryDetail': detail}})

        # 1点だけの系列 (surprisePower)
        single = vs_detail('BANKARA', 'single_ui', (4, 4), bankara='CHALLENGE', rule='AREA')
        single['surprisePower'] = 1500
        single['playedTime'] = '2026-09-22T04:00:00Z'
        self.ingest('VsHistoryDetailQuery', {'data': {'vsHistoryDetail': single}})

        # 不正な日時の系列
        bad_time = vs_detail('BANKARA', 'bad_time_ui', (4, 4), bankara='CHALLENGE', rule='AREA')
        bad_time['bankaraMatch']['bankaraPower'] = {'power': 2000}
        bad_time['playedTime'] = 'not-a-valid-iso-time'
        self.ingest('VsHistoryDetailQuery', {'data': {'vsHistoryDetail': bad_time}})

        page = self.temp_root / 'graph_test.html'
        write_gui(self.store, page)
        html_text = page.read_text(encoding='utf-8')

        # 最新値・直前からの増減・最低・最高・点数の表示検査
        self.assertIn('最新値', html_text)
        self.assertIn('直前からの増減', html_text)
        self.assertIn('最低', html_text)
        self.assertIn('最高', html_text)
        self.assertIn('点数', html_text)
        self.assertIn('バンカラマッチ（チャレンジ） / ガチエリア / バンカラパワー', html_text)

        # 1点だけの系列では増減が「—」であること
        self.assertIn('—', html_text)

        # 縦軸・横軸の明示
        self.assertIn('class="axis y-axis"', html_text)
        self.assertIn('class="axis x-axis"', html_text)

        # 水平補助線と目盛りラベル
        self.assertIn('class="grid-line"', html_text)
        self.assertIn('class="tick-label y-tick-label"', html_text)

        # 0が表示範囲内にある場合の明瞭なゼロ線
        self.assertIn('class="grid-line zero-line"', html_text)

        # X軸目盛り（日付目盛り・ヒゲ線）
        self.assertIn('class="tick-label x-tick-label"', html_text)
        self.assertIn('class="x-tick-mark"', html_text)
        self.assertIn('09/22 03:00', html_text)

        # 不正な日時の場合でも生成を失敗させず文字列が含まれていること
        self.assertIn('not-a-valid-iso-', html_text)

        # SVGアクセシビリティ（role="img", aria-labelledby, title, desc）
        self.assertIn('role="img"', html_text)
        self.assertIn('aria-labelledby="chart-title-', html_text)
        self.assertIn('<title id="chart-title-', html_text)
        self.assertIn('<desc id="chart-desc-', html_text)

if __name__ == '__main__':
    unittest.main()
