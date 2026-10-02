"""GUI改修（正式名称・順序・アカウント分離・目盛り・キケン度・非表示制御・フォント）の検証テスト。"""
import base64
import hashlib
import html
import json
from pathlib import Path
import re
import tempfile
import unittest

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'src/python'))

from ikarchive.display import (
    GENRE_LABELS,
    RULE_LABELS,
    GENRE_ORDER,
    RULE_ORDER,
    format_genre,
    format_rule,
    genre_sort_index,
    rule_sort_index,
)
from ikarchive.gui import (
    BUNDLED_FONT,
    FALLBACK_FONTS,
    FONT_UNICODE_RANGES_FILE,
    render,
    write_gui,
)
from ikarchive.store import Store

ROOT = Path(__file__).resolve().parents[2]

class FakeStore:
    """人工データを直接 rate_points に注入して GUI の挙動を検証するためのストア。"""
    def __init__(self, db_path):
        self.store = Store(db_path)
        self.db = self.store.db

    def close(self):
        self.store.close()

    def sync_health(self):
        return self.store.sync_health()

    def insert_point(self, account, series_id, label, genre, rule_raw, match_key, played_time, value, source='api', priority='primary'):
        self.db.execute(
            '''INSERT INTO rate_points(account, series_id, label, genre, rule_raw, match_key, played_time, value, source, priority)
               VALUES(?,?,?,?,?,?,?,?,?,?)''',
            (account, series_id, label, genre, rule_raw, match_key, played_time, value, source, priority)
        )
        self.db.commit()

class GuiRevisionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmp.name) / 'test.sqlite'
        self.store = FakeStore(self.db_path)

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def test_mode_and_rule_order(self):
        """公式順序（フェス、レギュラー、チャレンジ、オープン、X、イベント、プラベ、バイト等）＋ルール順で並ぶこと。"""
        # 逆順やランダムな順序で投入
        test_entries = [
            ('acc', 'hold||val', '保留値', 'hold', None, 'm_hold', 100),
            ('acc', 'team_contest|TEAM_CONTEST|val', 'バチコン納品', 'team_contest', 'TEAM_CONTEST', 'm_tc', 100),
            ('acc', 'big_run|BIG_RUN|val', 'ビッグラン納品', 'big_run', 'BIG_RUN', 'm_br', 100),
            ('acc', 'salmon_regular|REGULAR|val', 'いつものバイト納品', 'salmon_regular', 'REGULAR', 'm_sr', 100),
            ('acc', 'private_four_vs_four|AREA|val', 'プラベ4v4エリア', 'private_four_vs_four', 'AREA', 'm_p4', 100),
            ('acc', 'event|AREA|val', 'イベマエリア', 'event', 'AREA', 'm_ev', 100),
            ('acc', 'xmatch|CLAM|val', 'Xアサリ', 'xmatch', 'CLAM', 'm_x_clam', 100),
            ('acc', 'xmatch|AREA|val', 'Xエリア', 'xmatch', 'AREA', 'm_x_area', 100),
            ('acc', 'bankara_open|AREA|val', 'オープンエリア', 'bankara_open', 'AREA', 'm_bo', 100),
            ('acc', 'bankara_challenge|LOFT|val', 'チャレンジヤグラ', 'bankara_challenge', 'LOFT', 'm_bc_loft', 100),
            ('acc', 'bankara_challenge|AREA|val', 'チャレンジエリア', 'bankara_challenge', 'AREA', 'm_bc_area', 100),
            ('acc', 'nawabari|TURF_WAR|val', 'ナワバリ', 'nawabari', 'TURF_WAR', 'm_naw', 100),
            ('acc', 'fest|TURF_WAR|val', 'フェス', 'fest', 'TURF_WAR', 'm_fest', 100),
        ]
        for acc, sid, lbl, genre, rule, mkey, val in test_entries:
            self.store.insert_point(acc, sid, lbl, genre, rule, mkey, '2026-09-22T01:00:00Z', val)

        html = render(self.store)

        # 見出しの出現順を検証
        expected_order = [
            'フェスマッチ / ナワバリバトル / フェス',
            'レギュラーマッチ / ナワバリバトル / ナワバリ',
            'バンカラマッチ（チャレンジ） / ガチエリア / チャレンジエリア',
            'バンカラマッチ（チャレンジ） / ガチヤグラ / チャレンジヤグラ',
            'バンカラマッチ（オープン） / ガチエリア / オープンエリア',
            'Xマッチ / ガチエリア / Xエリア',
            'Xマッチ / ガチアサリ / Xアサリ',
            'イベントマッチ / ガチエリア / イベマエリア',
            'プライベートマッチ（4対4） / ガチエリア / プラベ4v4エリア',
            'いつものバイト / いつものバイト / いつものバイト納品',
            'ビッグラン / ビッグラン / ビッグラン納品',
            'バイトチームコンテスト / バイトチームコンテスト / バチコン納品',
            '区分保留 / 保留値',
        ]

        last_pos = -1
        for title in expected_order:
            pos = html.find(title)
            self.assertNotEqual(pos, -1, f'{title} がHTML内に見つかりません')
            self.assertGreater(pos, last_pos, f'{title} の出現順序が不正です')
            last_pos = pos

    def test_official_rule_names_and_unknown_handling(self):
        """正式ルール名が表示され、未知の値も消されずに「未対応のルール（値）」として表示されること。"""
        # 既知のルール
        for raw_rule, expected_label in RULE_LABELS.items():
            self.assertEqual(format_rule(raw_rule), expected_label)

        # 未知のルール
        self.assertEqual(format_rule('MYSTERY_RULE'), '未対応のルール（MYSTERY_RULE）')

        # 未知ルールを含む系列をHTML描画
        self.store.insert_point(
            'acc',
            'xmatch|MYSTERY_RULE|xPower',
            'Xパワー',
            'xmatch',
            'MYSTERY_RULE',
            'm_unknown',
            '2026-09-22T01:00:00Z',
            2000
        )
        html = render(self.store)
        self.assertIn('未対応のルール（MYSTERY_RULE）', html)
        self.assertIn('Xマッチ / 未対応のルール（MYSTERY_RULE） / Xパワー', html)

    def test_separate_accounts_and_unique_svg_ids(self):
        """別アカウントで同一series_idがあっても分離され、見出しで識別され、SVG IDが一意であること。"""
        # アカウントが1つの場合: アカウント1の識別は出ない
        self.store.insert_point('acc_alpha', 'bankara_open|AREA|power', 'バンカラパワー', 'bankara_open', 'AREA', 'm1', '2026-09-22T01:00:00Z', 1800)
        html_single = render(self.store)
        self.assertNotIn('アカウント1', html_single)
        self.assertNotIn('acc_alpha', html_single)  # 生のアカウントIDは出さない

        # アカウントが2つの場合
        self.store.insert_point('acc_beta', 'bankara_open|AREA|power', 'バンカラパワー', 'bankara_open', 'AREA', 'm2', '2026-09-22T01:00:00Z', 1900)
        html_multi = render(self.store)

        # アカウント1, アカウント2で識別されること（生IDは出さない）
        self.assertIn('（アカウント1）', html_multi)
        self.assertIn('（アカウント2）', html_multi)
        self.assertNotIn('acc_alpha', html_multi)
        self.assertNotIn('acc_beta', html_multi)

        # SVG IDが一意であること（重複がないこと）
        svg_title_ids = re.findall(r'<title id="(chart-title-[^"]+)"', html_multi)
        self.assertEqual(len(svg_title_ids), 2)
        self.assertEqual(len(set(svg_title_ids)), 2, 'SVGのtitle IDが重複しています')

        svg_desc_ids = re.findall(r'<desc id="(chart-desc-[^"]+)"', html_multi)
        self.assertEqual(len(svg_desc_ids), 2)
        self.assertEqual(len(set(svg_desc_ids)), 2, 'SVGのdesc IDが重複しています')

    def test_grade_point_ticks_and_unfixed_rates(self):
        """afterGradePoint等のY軸目盛りが0,200,400,600,800,999となり、>999で拡張され、jobRate等は非固定であること。"""
        # 1. afterGradePoint (値: 400) -> 0, 200, 400, 600, 800, 999
        self.store.insert_point('acc', 'salmon_regular|REGULAR|afterGradePoint', '評価ポイント', 'salmon_regular', 'REGULAR', 'm_gp1', '2026-09-22T01:00:00Z', 400)
        html = render(self.store)
        for tick_val in ('0', '200', '400', '600', '800', '999'):
            self.assertIn(f'>{tick_val}<', html)

        # 2. gradePoint で 999 を超える未知値 (例: 1150) -> クリップされず 999 超へ拡張される
        self.store.insert_point('acc', 'salmon_regular|REGULAR|gradePoint', '評価ポイント', 'salmon_regular', 'REGULAR', 'm_gp2', '2026-09-22T01:00:00Z', 1150)
        html2 = render(self.store)
        self.assertIn('>999<', html2)
        self.assertIn('>1,200<', html2)  # 拡張された目盛り

        # 3. jobRate / jobScore / 納品数は固定されない (通常の nice ticks)
        # jobRate: 120 -> 999 などの目盛りは出ない
        self.store.insert_point('acc', 'salmon_regular|REGULAR|jobRate', '評価レート', 'salmon_regular', 'REGULAR', 'm_jr', '2026-09-22T01:00:00Z', 120)
        html3 = render(self.store)
        # jobRate セクションのSVGを取得
        job_rate_section = [s for s in html3.split('<section>') if '評価レート' in s][0]
        self.assertNotIn('>999<', job_rate_section)

    def test_danger_rate_display_conversion_and_units(self):
        """キケン度dangerRateが表示のみ100倍＋%となり、DBは変更されず、最低/最高表記となること。"""
        raw_danger = 1.66
        self.store.insert_point('acc', 'salmon_regular|REGULAR|dangerRate', 'キケン度', 'salmon_regular', 'REGULAR', 'm_dr', '2026-09-22T01:00:00Z', raw_danger)

        # DB内の値が変更されていないこと
        db_val = self.store.db.execute("SELECT value FROM rate_points WHERE label='キケン度'").fetchone()[0]
        self.assertEqual(db_val, raw_danger)

        html = render(self.store)
        # 1.66 -> 166%
        self.assertIn('166%', html)
        self.assertIn('最低', html)
        self.assertIn('最高', html)
        self.assertNotIn('最小', html)
        self.assertNotIn('最大', html)

        # 単位検証: ポイントp, 金イクラ個, パワー数値
        self.store.insert_point('acc', 'salmon_regular|REGULAR|afterGradePoint', '評価ポイント', 'salmon_regular', 'REGULAR', 'm_p', '2026-09-22T01:00:00Z', 300)
        self.store.insert_point('acc', 'salmon_regular|REGULAR|goldenDeliverCount', '集めた金イクラ', 'salmon_regular', 'REGULAR', 'm_g', '2026-09-22T01:00:00Z', 45)
        self.store.insert_point('acc', 'bankara_challenge|AREA|weaponPower', 'ブキチャレパワー', 'bankara_challenge', 'AREA', 'm_w', '2026-09-22T01:00:00Z', 2150)

        html_units = render(self.store)
        self.assertIn('300p', html_units)
        self.assertIn('45個', html_units)
        self.assertIn('2,150', html_units)

    def test_hidden_pseudo_form_and_earned_udemae_point(self):
        """earnedUdemaePoint増減系列とderived_judgementは既定グラフに非表示、原データ保持、ウデマエ説明文が出ること。"""
        # 1. derived_judgement (疑似チョーシ)
        self.store.insert_point('acc', 'nawabari||form', 'チョーシ', 'nawabari', 'TURF_WAR', 'm_f', '2026-09-22T01:00:00Z', 3, source='derived_judgement')
        # 2. earnedUdemaePoint (増減系列)
        self.store.insert_point('acc', 'bankara_open|AREA|earnedUdemaePoint', 'ウデマエポイント増減', 'bankara_open', 'AREA', 'm_u', '2026-09-22T01:00:00Z', 8, source='api')
        # 3. 通常の表示系列
        self.store.insert_point('acc', 'xmatch|AREA|xPower', 'Xパワー', 'xmatch', 'AREA', 'm_x', '2026-09-22T01:00:00Z', 2300, source='api')

        # DBに原データが保持されていること
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM rate_points WHERE source='derived_judgement'").fetchone()[0], 1)
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM rate_points WHERE series_id LIKE '%earnedUdemaePoint%'").fetchone()[0], 1)

        html = render(self.store)
        # グラフセクションとして非表示
        self.assertNotIn('<h2>レギュラーマッチ / ナワバリバトル / チョーシ', html)
        self.assertNotIn('<h2>バンカラマッチ（オープン） / ガチエリア / ウデマエポイント増減', html)
        # 通常系列は表示
        self.assertIn('<h2>Xマッチ / ガチエリア / Xパワー', html)
        # ウデマエポイントについての説明文が表示されていること
        self.assertIn('現在の取得項目には所持ウデマエポイントがないため、累積ポイントの推移はまだ表示できません。', html)

        # 4. source=api_snapshot の場合の説明文と表見出し
        self.store.insert_point('acc', 'xmatch|AREA|snapshot_val', 'スナップショット記録', 'xmatch', 'AREA', 'm_snap', '2026-09-22T01:00:00Z', 100, source='api_snapshot')
        html_snap = render(self.store)
        self.assertIn('取得時点の記録。試合ごとの値ではありません。', html_snap)
        self.assertIn('<th>取得日時</th>', html_snap)

    def test_font_unicode_range_and_fallback(self):
        """同梱フォント指定時に正確なunicode-rangeが適用され、任意フォントには適用されず、日本語フォールバックが存在すること。"""
        # 1. 同梱フォント (既定)
        html_bundled = render(self.store, font_path=BUNDLED_FONT)
        ranges_data = json.loads(FONT_UNICODE_RANGES_FILE.read_text(encoding='utf-8'))
        expected_range = ranges_data['unicode_range']

        self.assertIn('@font-face', html_bundled)
        self.assertIn(f'unicode-range: {expected_range};', html_bundled)
        self.assertIn(FALLBACK_FONTS, html_bundled)

        # 2. 任意のカスタムフォント（ダミー）
        dummy_font = Path(self.tmp.name) / 'dummy.otf'
        dummy_font.write_bytes(b'arbitrary-font-data')
        html_custom = render(self.store, font_path=dummy_font)
        self.assertIn('@font-face', html_custom)
        self.assertNotIn('unicode-range:', html_custom, '任意フォントには同梱用unicode-rangeを適用してはいけません')

        # 3. フォント無し時
        html_no_font = render(self.store, font_path=Path(self.tmp.name) / 'nonexistent.otf')
        self.assertNotIn('@font-face', html_no_font)
        self.assertIn(FALLBACK_FONTS, html_no_font)

    def test_each_history_path_is_named_in_the_freshness_notice(self):
        page = render(self.store)
        self.assertIn('white-space: pre-wrap', page)
        match = re.search(r'data-history-times="([^"]*)"', page)
        self.assertIsNotNone(match)
        rows = json.loads(html.unescape(match.group(1)))
        self.assertEqual([row['label'] for row in rows], [
            '最新の履歴', 'レギュラーマッチ', 'バンカラマッチ', 'Xマッチ',
            'イベントマッチ', 'プライベートマッチ', 'バイト',
        ])
        self.assertEqual(len(rows), 7)
        self.assertTrue(all(row['at'] is None and row['stale'] is True for row in rows))
        self.assertIn('レギュラーマッチ 遅延 未確認', page)
        self.assertIn('バイト 遅延 未確認', page)
        self.assertIn("lines.join('\\n')", page)

    def test_mobile_layout_wraps_long_text_and_uses_nonshrinking_grid(self):
        self.store.insert_point(
            'acc', 'nawabari|TURF_WAR|vibes', 'チョーシ / 非常に長いブキ名',
            'nawabari', 'TURF_WAR', 'm1', '2026-09-22T01:00:00Z', 12
        )
        page = render(self.store)
        self.assertIn('overflow-wrap: anywhere', page)
        self.assertIn('grid-template-columns: repeat(auto-fit, minmax(104px, 1fr))', page)
        self.assertIn('min-width: 0', page)
        self.assertIn('overflow: hidden', page)

if __name__ == '__main__':
    unittest.main()
