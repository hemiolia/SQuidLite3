"""ローカルで開く分析画面。配色は NOAHS の卓上と紙を使い、文字と線は実測した対比だけを置く。"""
import base64
from datetime import datetime
import hashlib
import html
import json
import math
from pathlib import Path

from .display import (
    GENRE_LABELS,
    RULE_LABELS,
    GENRE_ORDER,
    RULE_ORDER,
    format_genre,
    format_rule,
    genre_sort_index,
    rule_sort_index,
)

# NOAHS app/src/app.css の :root。対比が足りない muted は本文に使わない。
FIELD = '#2a0063'
ON_FIELD = '#fffff5'
PAPER = '#fbf5ec'
INK = '#241a33'
GOLD = '#bd9a45'
LINK = '#5b3bb0'
GRID = '#7a6a8a'
BUNDLED_FONT = Path(__file__).resolve().parents[3] / 'assets' / 'fonts' / 'Splatoon2-Unified.otf'
CONFIG_DIR = Path(__file__).resolve().parents[3] / 'config'
HISTORY_PATHS = (
    ('LatestBattleHistoriesQuery', '最新の履歴'),
    ('RegularBattleHistoriesQuery', 'レギュラーマッチ'),
    ('BankaraBattleHistoriesQuery', 'バンカラマッチ'),
    ('XBattleHistoriesQuery', 'Xマッチ'),
    ('EventBattleHistoriesQuery', 'イベントマッチ'),
    ('PrivateBattleHistoriesQuery', 'プライベートマッチ'),
    ('CoopHistoryQuery', 'バイト'),
)
FONT_UNICODE_RANGES_FILE = CONFIG_DIR / 'font-unicode-ranges.json'
FALLBACK_FONTS = 'system-ui, "Hiragino Kaku Gothic ProN", "Yu Gothic", Meiryo, sans-serif'

GRADE_POINT_KEYS = {'afterGradePoint', 'gradePoint', 'regularGradePoint', 'highestGradePoint'}

def _channel(value):
    value = value / 255
    return value / 12.92 if value <= 0.04045 else ((value + 0.055) / 1.055) ** 2.4

def contrast(foreground, background):
    def lum(color):
        red, green, blue = int(color[1:3], 16), int(color[3:5], 16), int(color[5:7], 16)
        return 0.2126 * _channel(red) + 0.7152 * _channel(green) + 0.0722 * _channel(blue)
    dark, light = sorted((lum(foreground), lum(background)))
    return (light + 0.05) / (dark + 0.05)

TEXT_PAIRS = ((ON_FIELD, FIELD), (INK, PAPER), (LINK, PAPER))
GRAPHIC_PAIRS = ((INK, PAPER), (LINK, PAPER), (GRID, PAPER))

def assert_contrast():
    for foreground, background in TEXT_PAIRS:
        if contrast(foreground, background) < 4.5:
            raise ValueError('TEXT_CONTRAST')
    for foreground, background in GRAPHIC_PAIRS:
        if contrast(foreground, background) < 3:
            raise ValueError('GRAPHIC_CONTRAST')

def _format_date(val):
    if not val or not isinstance(val, str):
        return str(val or '')
    try:
        clean = val.strip()
        if clean.endswith('Z'):
            clean = clean[:-1] + '+00:00'
        dt = datetime.fromisoformat(clean)
        return f'{dt.month:02d}/{dt.day:02d} {dt.hour:02d}:{dt.minute:02d}'
    except Exception:
        return str(val)[:16]

def _format_number(value):
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return str(value)
    if not math.isfinite(value):
        return str(value)
    if abs(value - round(value)) < 1e-9:
        return f'{int(round(value)):,}'
    magnitude = abs(value)
    digits = 2 if magnitude >= 1 else 4
    return f'{value:,.{digits}f}'.rstrip('0').rstrip('.')

def _get_metric_key(series_id):
    if not series_id:
        return ''
    return series_id.split('|')[-1].split('.')[-1]

def _is_grade_point_series(series_id):
    return _get_metric_key(series_id) in GRADE_POINT_KEYS

def _is_danger_rate(series_id, label):
    key = _get_metric_key(series_id)
    return key == 'dangerRate' or 'キケン度' in (label or '')

def _format_unit_value(value, metric_key, label):
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return str(value)
    formatted_num = _format_number(value)
    if metric_key == 'dangerRate' or 'キケン度' in (label or ''):
        return f'{formatted_num}%'
    if metric_key.endswith('Point') or 'ポイント' in (label or ''):
        return f'{formatted_num}p'
    if (
        'goldenDeliverCount' in metric_key
        or 'deliverCount' in metric_key
        or '金イクラ' in (label or '')
        or '納品数' in (label or '')
    ):
        return f'{formatted_num}個'
    return formatted_num

def _format_diff(diff, metric_key, label):
    if not isinstance(diff, (int, float)) or isinstance(diff, bool):
        return str(diff)
    if abs(diff) < 1e-9:
        diff_str = _format_unit_value(0, metric_key, label)
        return f'±{diff_str}'
    prefix = '+' if diff > 0 else ''
    formatted = _format_unit_value(diff, metric_key, label)
    if diff > 0 and not formatted.startswith('+'):
        return f'+{formatted}'
    return formatted

def _calc_grade_point_ticks(low, high):
    base_ticks = [0, 200, 400, 600, 800, 999]
    low_ticks = []
    if low < 0:
        step = 200
        cur = -step
        while cur >= low - step:
            low_ticks.append(cur)
            cur -= step
        low_ticks.reverse()
    high_ticks = []
    if high > 999:
        step = 200
        cur = 1200
        while cur < high + step:
            high_ticks.append(cur)
            cur += step
    return low_ticks + base_ticks + high_ticks

def _calc_y_ticks(low, high):
    if low == high:
        if low == 0:
            low, high = -2.0, 2.0
        else:
            delta = max(1.0, abs(low) * 0.1)
            low, high = low - delta, low + delta
    raw_span = high - low
    raw_step = raw_span / 5.0
    power = 10 ** math.floor(math.log10(raw_step))
    fraction = raw_step / power
    if fraction <= 1.0:
        step = 1.0 * power
    elif fraction <= 2.0:
        step = 2.0 * power
    elif fraction <= 2.5:
        step = 2.5 * power
    elif fraction <= 5.0:
        step = 5.0 * power
    else:
        step = 10.0 * power
    nice_min = math.floor(low / step) * step
    nice_max = math.ceil(high / step) * step
    n_steps = round((nice_max - nice_min) / step)
    if n_steps < 4:
        if abs(step / power - 5.0) < 1e-6:
            step = 2.0 * power
        elif abs(step / power - 2.0) < 1e-6:
            step = 1.0 * power
        elif abs(step / power - 10.0) < 1e-6:
            step = 5.0 * power
        nice_min = math.floor(low / step) * step
        nice_max = math.ceil(high / step) * step
        n_steps = round((nice_max - nice_min) / step)
    ticks = [round(nice_min + i * step, 6) for i in range(n_steps + 1)]
    return ticks

def _svg(points, unique_id, series_id, label, genre_label, rule_label):
    width, height = 720, 280
    pad_left, pad_right = 64, 32
    pad_top, pad_bottom = 28, 44
    plot_x = pad_left
    plot_y = pad_top
    plot_w = width - pad_left - pad_right
    plot_h = height - pad_top - pad_bottom
    x_min, x_max = plot_x, plot_x + plot_w
    y_min, y_max = plot_y, plot_y + plot_h

    metric_key = _get_metric_key(series_id)
    values = [p['value'] for p in points]
    val_min, val_max = min(values), max(values)
    if _is_grade_point_series(series_id):
        ticks = _calc_grade_point_ticks(val_min, val_max)
    else:
        ticks = _calc_y_ticks(val_min, val_max)

    y_tick_min, y_tick_max = ticks[0], ticks[-1]
    y_span = y_tick_max - y_tick_min or 1.0

    n = len(points)
    def x_at(idx):
        if n == 1:
            return plot_x + plot_w / 2.0
        return plot_x + plot_w * idx / (n - 1)

    def y_at(val):
        return y_max - (val - y_tick_min) / y_span * plot_h

    is_danger = _is_danger_rate(series_id, label)
    y_elements = []
    for t in ticks:
        y = y_at(t)
        is_zero = abs(t) < 1e-6
        t_label = f'{_format_number(t)}%' if is_danger else _format_number(t)
        if is_zero:
            y_elements.append(
                f'<line class="grid-line zero-line" x1="{x_min}" y1="{y:.1f}" x2="{x_max}" y2="{y:.1f}" stroke="{INK}" stroke-width="1.5" />'
            )
        else:
            y_elements.append(
                f'<line class="grid-line" x1="{x_min}" y1="{y:.1f}" x2="{x_max}" y2="{y:.1f}" stroke="{GRID}" stroke-width="1" stroke-dasharray="3,3" />'
            )
        y_elements.append(
            f'<text class="tick-label y-tick-label" x="{x_min - 8}" y="{y + 4:.1f}" text-anchor="end" fill="{INK}" font-size="12">{t_label}</text>'
        )

    if n <= 6:
        x_indices = list(range(n))
    else:
        target_count = 5
        x_indices = sorted(set(round(i * (n - 1) / (target_count - 1)) for i in range(target_count)))

    x_elements = []
    for idx in x_indices:
        x = x_at(idx)
        raw_time = points[idx].get('played_time') or ''
        date_str = _format_date(raw_time)
        x_elements.append(
            f'<line class="x-tick-mark" x1="{x:.1f}" y1="{y_max}" x2="{x:.1f}" y2="{y_max + 5}" stroke="{INK}" stroke-width="1.5" />'
        )
        x_elements.append(
            f'<text class="tick-label x-tick-label" data-time="{html.escape(raw_time,quote=True)}" x="{x:.1f}" y="{y_max + 20}" text-anchor="middle" fill="{INK}" font-size="11">{html.escape(date_str)}</text>'
        )

    axes = (
        f'<line class="axis y-axis" x1="{x_min}" y1="{y_min}" x2="{x_min}" y2="{y_max}" stroke="{INK}" stroke-width="1.5" />'
        f'<line class="axis x-axis" x1="{x_min}" y1="{y_max}" x2="{x_max}" y2="{y_max}" stroke="{INK}" stroke-width="1.5" />'
    )

    coords = [(x_at(i), y_at(p['value'])) for i, p in enumerate(points)]
    line_pts = ' '.join(f'{x:.1f},{y:.1f}' for x, y in coords)

    area_el = ''
    line_el = ''
    if n >= 2:
        area_pts = line_pts + f' {coords[-1][0]:.1f},{y_max:.1f} {coords[0][0]:.1f},{y_max:.1f}'
        area_el = f'<polygon class="chart-area" points="{area_pts}" fill="{LINK}" fill-opacity="0.12" />'
        line_el = f'<polyline class="chart-line" fill="none" stroke="{LINK}" stroke-width="2.5" points="{line_pts}" />'

    dots = []
    for i, ((x, y), p) in enumerate(zip(coords, points)):
        tooltip = f'{html.escape(str(p.get("played_time") or ""))} {_format_unit_value(p["value"], metric_key, label)}'
        if i == n - 1 and n > 1:
            dots.append(
                f'<circle class="dot-focus" cx="{x:.1f}" cy="{y:.1f}" r="6.5" fill="none" stroke="{LINK}" stroke-width="2" />'
                f'<circle class="dot dot-latest" cx="{x:.1f}" cy="{y:.1f}" r="3.5" fill="{INK}"><title>{tooltip} (最新)</title></circle>'
            )
        else:
            dots.append(
                f'<circle class="dot" cx="{x:.1f}" cy="{y:.1f}" r="3.5" fill="{INK}"><title>{tooltip}</title></circle>'
            )
    dots_el = ''.join(dots)

    title_id = f'chart-title-{unique_id}'
    desc_id = f'chart-desc-{unique_id}'
    latest = points[-1]['value']
    rule_desc = f' {rule_label}' if rule_label else ''
    desc_text = (
        f'{genre_label}{rule_desc} {label}。'
        f'データ数{n}点、最新値{_format_unit_value(latest, metric_key, label)}、'
        f'最低値{_format_unit_value(val_min, metric_key, label)}、最高値{_format_unit_value(val_max, metric_key, label)}。'
    )

    return f'''<svg viewBox="0 0 {width} {height}" role="img" aria-labelledby="{title_id} {desc_id}">
<title id="{title_id}">{html.escape(label)}の推移グラフ</title>
<desc id="{desc_id}">{html.escape(desc_text)}</desc>
<rect width="{width}" height="{height}" fill="{PAPER}" />
{''.join(y_elements)}
{axes}
{''.join(x_elements)}
{area_el}
{line_el}
{dots_el}
</svg>'''

def _table(points, metric_key, label):
    is_snapshot = points[0].get('source') == 'api_snapshot'
    time_th = '取得日時' if is_snapshot else '日時'
    rows = ''.join(
        f'<tr><td><time datetime="{html.escape(point.get("played_time") or "",quote=True)}">{html.escape(point.get("played_time") or "")}</time></td>'
        f'<td>{html.escape(format_rule(point.get("rule_raw") or "") or "—")}</td>'
        f'<td>{_format_unit_value(point["value"], metric_key, label)}</td></tr>'
        for point in points
    )
    return (
        f'<div class="table-wrap"><table><caption>同じ数値の表</caption>'
        f'<thead><tr><th>{time_th}</th><th>ルール</th><th>値</th></tr></thead>'
        f'<tbody>{rows}</tbody></table></div>'
    )

def _summary_stats(points, metric_key, label):
    n = len(points)
    latest = points[-1]['value']
    if n >= 2:
        diff = latest - points[-2]['value']
        diff_str = _format_diff(diff, metric_key, label)
    else:
        diff_str = '—'
    min_val = min(p['value'] for p in points)
    max_val = max(p['value'] for p in points)
    return f'''<div class="series-stats" aria-label="系列の要約">
  <div class="stat-item"><span class="stat-label">最新値</span><span class="stat-value">{_format_unit_value(latest, metric_key, label)}</span></div>
  <div class="stat-item"><span class="stat-label">直前からの増減</span><span class="stat-value">{diff_str}</span></div>
  <div class="stat-item"><span class="stat-label">最低</span><span class="stat-value">{_format_unit_value(min_val, metric_key, label)}</span></div>
  <div class="stat-item"><span class="stat-label">最高</span><span class="stat-value">{_format_unit_value(max_val, metric_key, label)}</span></div>
  <div class="stat-item"><span class="stat-label">点数</span><span class="stat-value">{n}</span></div>
</div>'''

def _should_display(points):
    if not points:
        return False
    first = points[0]
    # source=derived_judgement は既定グラフに表示しない
    if first.get('source') == 'derived_judgement':
        return False
    # earnedUdemaePoint の増減系列は既定グラフに表示しない
    metric_key = _get_metric_key(first.get('series_id') or '')
    if metric_key == 'earnedUdemaePoint' or first.get('label') == 'ウデマエポイント増減':
        return False
    return True

def render(store, font_path=None):
    assert_contrast()

    font_face = ''
    font_family = FALLBACK_FONTS
    if font_path is None:
        font_path = BUNDLED_FONT
    if font_path:
        p = Path(font_path)
        if p.is_file():
            font_bytes = p.read_bytes()
            b64 = base64.b64encode(font_bytes).decode('ascii')
            unicode_range_css = ''
            if FONT_UNICODE_RANGES_FILE.is_file():
                try:
                    ranges_data = json.loads(FONT_UNICODE_RANGES_FILE.read_text(encoding='utf-8'))
                    font_sha256 = hashlib.sha256(font_bytes).hexdigest()
                    if font_sha256 == ranges_data.get('font_sha256'):
                        u_range = ranges_data.get('unicode_range')
                        if u_range:
                            unicode_range_css = f"\n  unicode-range: {u_range};"
                except Exception:
                    pass
            font_face = f'''@font-face {{
  font-family: 'Splatoon2-Unified';
  src: url('data:font/otf;base64,{b64}') format('opentype');
  font-display: swap;{unicode_range_css}
}}'''
            font_family = f"'Splatoon2-Unified', {FALLBACK_FONTS}"

    account_rows = store.db.execute('SELECT DISTINCT account FROM rate_points ORDER BY account').fetchall()
    all_accounts = [r['account'] for r in account_rows if r['account']]
    is_multi_account = len(all_accounts) > 1
    account_labels = {
        acc: f'アカウント{idx + 1}' for idx, acc in enumerate(all_accounts)
    }

    grouped_series = {}
    for row in store.db.execute('SELECT * FROM rate_points ORDER BY account, genre, series_id, played_time, match_key'):
        key = (row['account'], row['series_id'])
        grouped_series.setdefault(key, []).append(dict(row))

    def _sort_key(item):
        (acc, sid), pts = item
        genre = pts[0].get('genre') or ''
        rule_raw = pts[0].get('rule_raw') or ''
        acc_idx = all_accounts.index(acc) if acc in all_accounts else 999
        return (
            genre_sort_index(genre),
            rule_sort_index(rule_raw),
            acc_idx,
            sid,
        )

    sorted_groups = sorted(grouped_series.items(), key=_sort_key)

    sections = []
    group_idx = 0
    for (acc, series_id), points in sorted_groups:
        if not _should_display(points):
            continue
        group_idx += 1
        label = points[0]['label']
        genre = points[0].get('genre') or ''
        rule_raw = points[0].get('rule_raw') or ''
        metric_key = _get_metric_key(series_id)

        if _is_danger_rate(series_id, label):
            points = [{**p, 'value': p['value'] * 100.0} for p in points]

        genre_label = format_genre(genre)
        rule_label = format_rule(rule_raw)

        heading_parts = [genre_label]
        if rule_label:
            heading_parts.append(rule_label)
        heading_parts.append(label)
        heading_base = ' / '.join(heading_parts)

        if is_multi_account and acc in account_labels:
            acc_name = account_labels[acc]
            heading_base = f'{heading_base}（{acc_name}）'

        priority = '副指標' if points[0].get('priority') == 'secondary' else '主指標'

        if points[0].get('source') == 'api_snapshot':
            source_desc = '取得時点の記録。試合ごとの値ではありません。'
        else:
            source_desc = '応答に含まれていた数値。'

        acc_key = f'a{all_accounts.index(acc) + 1}' if acc in all_accounts else f'g{group_idx}'
        safe_sid = "".join(c if c.isalnum() else '_' for c in str(series_id))
        unique_id = f'{acc_key}_{safe_sid}_{group_idx}'

        chart = f'<div class="chart-wrap">{_svg(points, unique_id, series_id, label, genre_label, rule_label)}</div>' if len(points) else ''
        stats = _summary_stats(points, metric_key, label) if len(points) else ''
        sections.append(
            f'<section><h2>{html.escape(heading_base)} <span>{priority}</span></h2>'
            f'<p class="series-desc">{source_desc}</p>'
            f'{stats}'
            f'{chart}'
            f'{_table(points, metric_key, label)}</section>'
        )

    health=store.sync_health()
    by_operation={h['operation']: h for h in health['histories']}
    observed=[h['last_success_at'] for h in health['histories'] if h['last_success_at']]
    latest_observed=max(observed) if observed else None
    history_rows=[]
    history_lines=[]
    for operation, label in HISTORY_PATHS:
        row=by_operation.get(operation) or {}
        at=row.get('last_success_at') if isinstance(row.get('last_success_at'), str) else None
        stale=True if at is None else bool(row.get('stale'))
        history_rows.append({'label': label, 'at': at, 'stale': stale})
        history_lines.append(label + (' 遅延 ' if stale else ' 確認済み ') + (at or '未確認'))
    if health['state']=='current':
        sync_notice='最新履歴を定期確認しています。'
    else:
        sync_notice='一部の履歴の確認が遅れています。未保存の試合がないか収集状態を確認してください。'
    sync_notice+='\n'+'\n'.join(history_lines)
    times=html.escape(json.dumps(history_rows), quote=True)
    freshness=f'<p class="notice" id="sync-freshness" role="status" data-history-times="{times}" data-last-sync="{html.escape(latest_observed or "")}">{html.escape(sync_notice)}</p>'
    space=health.get('storage_health')
    if space and space['state']!='normal':
        message=('空き容量が不足し、取得を一時停止しています。' if space['state']=='critical' else '空き容量が少ないため、最新の対戦・バイトの取得を優先しています。ランキングや画像は容量回復後に再開します。')
        freshness+=f'<p class="notice" role="alert">{message} 空き容量: {space["free_bytes"]/1024**3:.1f} GB</p>'
    auth=health.get('auth',{})
    if auth.get('reauth_required'):
        freshness+='<p class="notice" role="alert">認証が切れているため、新しい記録を保存できていません。再ログインが必要です。</p>'
    udemae_notice = freshness+'<p class="notice">現在の取得項目には所持ウデマエポイントがないため、累積ポイントの推移はまだ表示できません。</p>'
    body = ''.join(sections)
    if not body:
        content = f'{udemae_notice}<p>まだレートの数値はありません。取得が進むと、応答に含まれるパワー・ポイント・納品数・レートがここへ並びます。</p>'
    else:
        content = f'{udemae_notice}{body}'

    return f'''<!DOCTYPE html><html lang="ja"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>SQuidLite3 レートの推移</title>
<style>
*, *::before, *::after {{ box-sizing: border-box; }}
{font_face}
body {{
  margin: 0;
  background: {PAPER};
  color: {INK};
  font-family: {font_family};
  font-size: 16px;
  line-height: 1.6;
  -webkit-text-size-adjust: 100%;
}}
header {{
  background: {FIELD};
  color: {ON_FIELD};
  padding: 20px 16px;
  word-break: break-word;
}}
header h1 {{
  margin: 0 0 8px;
  font-size: 24px;
}}
header p {{
  margin: 0;
  font-size: 14px;
  opacity: 0.9;
}}
header a {{ color: {ON_FIELD}; }}
main {{
  max-width: 800px;
  margin: 0 auto;
  padding: 20px 16px;
  width: 100%;
}}
.notice {{
  margin: 0 0 24px;
  padding: 12px 16px;
  background: rgba(36, 26, 51, 0.04);
  border-left: 4px solid {LINK};
  font-size: 14px;
  color: {INK};
  overflow-wrap: anywhere;
  white-space: pre-wrap;
}}
section {{
  margin: 0 0 40px;
  padding-bottom: 24px;
  border-bottom: 1px solid {INK};
  min-width: 0;
}}
h2 {{
  font-size: 20px;
  margin: 0 0 8px;
  overflow-wrap: anywhere;
  word-break: normal;
}}
h2 span {{
  font-size: 14px;
  font-weight: normal;
  margin-left: 8px;
}}
.series-desc {{
  margin: 0 0 12px;
  font-size: 14px;
  color: {INK};
  overflow-wrap: anywhere;
}}
.series-stats {{
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(104px, 1fr));
  gap: 8px;
  margin: 12px 0 16px;
}}
.stat-item {{
  display: flex;
  flex-direction: column;
  background: rgba(36, 26, 51, 0.04);
  border: 1px solid rgba(36, 26, 51, 0.2);
  border-radius: 4px;
  padding: 6px 10px;
  min-width: 0;
}}
.stat-label {{
  font-size: 11px;
  color: {INK};
  opacity: 0.8;
  overflow-wrap: anywhere;
}}
.stat-value {{
  font-size: 17px;
  font-weight: bold;
  color: {INK};
  margin-top: 2px;
  overflow-wrap: anywhere;
}}
.chart-wrap {{
  width: 100%;
  margin: 16px 0;
  background: {PAPER};
  overflow: hidden;
}}
svg {{
  width: 100%;
  height: auto;
  display: block;
}}
.table-wrap {{
  width: 100%;
  overflow-x: auto;
  -webkit-overflow-scrolling: touch;
  margin-top: 16px;
}}
table {{
  border-collapse: collapse;
  width: 100%;
  min-width: 280px;
}}
caption {{
  text-align: left;
  font-weight: bold;
  margin-bottom: 6px;
  font-size: 14px;
}}
th, td {{
  border-bottom: 1px solid {INK};
  text-align: left;
  padding: 6px 8px;
  font-size: 14px;
  white-space: nowrap;
}}
</style></head><body>
<header><h1>レートの推移</h1><p>シートやグラフをまたいで、別のジャンルを一つの数値として足さない。</p></header>
<main>{content}</main>
<script>
(() => {{
 const notice=document.getElementById('sync-freshness');
 const full=new Intl.DateTimeFormat('ja-JP',{{year:'numeric',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit',second:'2-digit',timeZoneName:'short'}});
 const short=new Intl.DateTimeFormat('ja-JP',{{month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit'}});
 for(const el of document.querySelectorAll('time[datetime], .x-tick-label[data-time]')){{
   const at=Date.parse(el.dateTime||el.dataset.time);
   if(Number.isFinite(at))el.textContent=(el.tagName.toLowerCase()==='time'?full:short).format(at);
 }}
 const rows=JSON.parse(notice.dataset.historyTimes);
 const draw=()=>{{
   const late=(row)=>{{
     const at=Date.parse(row&&row.at);
     return !Number.isFinite(at)||Date.now()-at>600000;
   }};
   const stale=!Array.isArray(rows)||rows.length!==7||rows.some(late);
   const lines=[stale?'履歴の確認が遅れています。画面を再読み込みし、収集状態を確認してください。':'最新履歴を定期確認しています。'];
   if(Array.isArray(rows)){{
     for(const row of rows){{
       const at=Date.parse(row&&row.at);
       lines.push((row&&row.label?row.label:'未確認')+' '+(late(row)?'遅延':'確認済み')+' '+(Number.isFinite(at)?full.format(at):'未確認'));
     }}
   }}
   notice.textContent=lines.join('\\n');
 }};
 draw();setInterval(draw,60000);
}})();
</script></body></html>'''

def write_gui(store, destination, font_path=None):
    import os, tempfile
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    content=render(store, font_path=font_path)
    fd,name=tempfile.mkstemp(prefix=destination.name+'.',suffix='.tmp',dir=destination.parent)
    temporary=Path(name)
    try:
        with os.fdopen(fd,'w',encoding='utf-8') as stream:
            stream.write(content);stream.flush();os.fsync(stream.fileno())
        temporary.replace(destination)
    finally:
        temporary.unlink(missing_ok=True)
    destination.chmod(0o600)
    count = content.count('<section>')
    return {'path': str(destination), 'series': count}
