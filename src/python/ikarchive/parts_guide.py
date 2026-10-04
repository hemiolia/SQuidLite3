"""SQuidLite3 の部品の読み方（目録の recipes と README_FOR_AI.md の本文）。

目録 catalog.sqlite3 の recipes 表に入れる行と、Drive の db/README_FOR_AI.md を作る。
どこに何があり、何を知りたいときにどの部品とどの部品をどのキーで合わせればよいかを、
推論なしに辿れるように書く。規則の正本は docs/design/SQuidLite3_データ配置.md。
"""

from __future__ import annotations

import sqlite3

# (question_ja, steps_ja, sql)。sql は記載の部品を開いた（または ATTACH した）状態で、そのまま動く文。
RECIPES: list[tuple[str, str, str]] = [
    (
        "あるモードのルール別の勝敗数を知りたい",
        "catalog.sqlite3 の match_index だけで足りる。analysis_set でモードを、rule_raw でルールを選ぶ。",
        "SELECT analysis_set, rule_raw, judgement, count(*) FROM match_index "
        "WHERE kind='vs' GROUP BY analysis_set, rule_raw, judgement ORDER BY 1, 2, 3",
    ),
    (
        "試合の一覧（日時・ステージ・勝敗・自分のブキ・タグ）が欲しい",
        "catalog.sqlite3 の match_index。part_path がその試合の全データを持つ部品の住所。",
        "SELECT played_time, analysis_set, rule_name, stage, judgement, my_weapon, tags, part_path "
        "FROM match_index ORDER BY played_time",
    ),
    (
        "ある試合の全データ（原文 JSON を含む）が欲しい",
        "match_index で part_path を引き、その部品を開く。matches・match_classification・documents・"
        "sightings・responses・bodies（応答本文のバイト列）がそろっている。正本と同じビューも使える。",
        "SELECT m.*, c.*, d.json_text FROM matches m "
        "JOIN match_classification c USING(account, kind, match_key) "
        "JOIN documents d ON d.response_id = m.detail_response_id AND d.match_key = m.match_key "
        "WHERE m.match_key = :match_key",
    ),
    (
        "あるモード・ルールの全試合の全プレイヤーのブキや成績を見たい",
        "matches/<analysis_set>/<rule_raw>/ の下の月ごとの部品を開き、battle_players ビューを読む。"
        "複数月は ATTACH して UNION ALL する。どの月の部品があるかは files 表の analysis_set・rule_raw・month で引く。",
        "SELECT match_key, team_index, is_my_team, name, weapon, paint, kills, assists, deaths, specials "
        "FROM battle_players ORDER BY match_key, team_index, player_index",
    ),
    (
        "バイトの WAVE ごとの結果やオオモノを見たい",
        "matches/salmon_regular/（ビッグランは big_run、バイトチームコンテストは team_contest）の部品を開き、"
        "salmon_waves・salmon_bosses・salmon_players ビューを読む。",
        "SELECT match_key, wave_index, json_text FROM salmon_waves ORDER BY match_key, wave_index",
    ),
    (
        "パワー・ポイント・レート・納品数の推移を見たい",
        "試合に付くレートは各試合部品の rate_points 表にある（series_id ごとの系列）。"
        "ブキのチョーシなど取得時点の値は match_key が fetch: で始まる行で、その取得の応答と同じ部品にある。"
        "系列の一覧と所在は files 表から rate_points を含む部品を引く。",
        "SELECT series_id, label, played_time, value FROM rate_points ORDER BY series_id, played_time",
    ),
    (
        "タグ（下げラン・練習・エンジョイ・ガチ・対抗戦・イカップル など）で絞りたい",
        "match_index の tags 列（、区切り）で絞る。タグの正本は各試合部品の match_tags 表。"
        "タグは母集団を動かさない（分析セットは analysis_set のまま）。",
        "SELECT * FROM match_index WHERE tags LIKE '%' || :tag || '%'",
    ),
    (
        "画像（ブキ・ギア・バッジ・ステージなど）を取り出したい",
        "画像は試合部品には入っていない。原文や asset_refs で見つけた url を catalog.sqlite3 の asset_index で引き、"
        "part_path の images/<SHA-256先頭2桁>.sqlite3 を開いて assets と bodies を body_sha256 で結ぶ。",
        "SELECT a.url, a.content_type, b.body FROM assets a JOIN bodies b ON b.sha256 = a.body_sha256 "
        "WHERE a.url = :url",
    ),
    (
        "試合以外の取得記録（ランキング、ステージ情報、ブキ記録、ヒーローモード など）を見たい",
        "応答の種類（operation）ごとに responses/<operation>/ の下にある。どの種類があるかは files 表の operation、"
        "個々の応答の住所は response_index。entities 表に応答から取り出した型と ID ごとの記録がある。",
        "SELECT operation, count(*) FROM response_index GROUP BY operation ORDER BY 2 DESC",
    ),
    (
        "取得がうまくいったか、何が未取得かを知りたい",
        "system/jobs.sqlite3（取得キュー。state が done・retry・unavailable・out_of_scope など）、"
        "system/runs.sqlite3（収集の各回）、system/issues.sqlite3 と各部品の issues 表（監査記録）。",
        "SELECT state, count(*) FROM jobs GROUP BY state",
    ),
    (
        "全体を一つの SQLite として扱いたい",
        "Drive の統合版（正本の静止点）を使うか、必要な部品を ATTACH する。部品はどれも正本と同じ表定義なので、"
        "同じ表名どうしを UNION ALL すれば正本の該当範囲になる。部品どうしで同じ行が重なるのは bodies の写しだけ"
        "（各部品の _copies 表に載る）。",
        "SELECT * FROM main.matches UNION ALL SELECT * FROM other.matches",
    ),
]


def write_recipes(catalog: sqlite3.Connection) -> int:
    """catalog の recipes 表を RECIPES で置き換える。書いた行数を返す。"""
    catalog.execute("DELETE FROM recipes")
    catalog.executemany(
        "INSERT INTO recipes(question_ja, steps_ja, sql) VALUES (?, ?, ?)", RECIPES
    )
    return len(RECIPES)


def _rows(catalog: sqlite3.Connection, sql: str) -> list[tuple]:
    try:
        return catalog.execute(sql).fetchall()
    except sqlite3.Error:
        return []


def render_readme(catalog: sqlite3.Connection) -> str:
    """目録の内容から README_FOR_AI.md の本文を作る。"""
    status = dict(_rows(catalog, "SELECT key, value FROM status"))
    labels = _rows(catalog, "SELECT kind, code, ja FROM labels ORDER BY kind, code")
    homes = _rows(catalog, "SELECT table_name, rule_ja, path_pattern FROM table_homes ORDER BY table_name")
    sets = _rows(
        catalog,
        "SELECT analysis_set, rule_raw, count(*) FROM match_index GROUP BY 1, 2 ORDER BY 1, 2",
    )
    lines: list[str] = []
    add = lines.append
    add("# SQuidLite3 データの読み方（人と AI 向け）")
    add("")
    add("スプラトゥーン3のアプリ「イカリング3」に出る対戦とバイトの記録を、取得した応答の原文を含めて一切省略せずに保存したデータである。"
        "任天堂とは無関係の非公式ツール SQuidLite3 が作っている。")
    add("")
    add("## まず開くもの")
    add("")
    add("1. `catalog.sqlite3`（同じ内容の `catalog.xlsx` もある）。全試合の一覧 `match_index`、全部品の住所 `files`、"
        "応答の住所録 `response_index`、画像の住所録 `asset_index`、表ごとの置き場所の規則 `table_homes`、"
        "日本語名 `labels`、分析の手引き `recipes`、更新状況 `status` が入っている。")
    add("2. たいていの集計は `match_index` だけで足りる。各試合の全データが要るときは、その行の `part_path` の部品を開く。")
    add("")
    add("## 置き場所の規則（どの行も、この規則でただ一つの部品に決まる）")
    add("")
    add("- `matches/<analysis_set>/<rule_raw>/<YYYY-MM>.sqlite3`: 試合ごとのデータ。モード（分析セット）とルールと、試合日時（日本時間）の年月で分かれる。")
    add("- `responses/<operation>/<期間>.sqlite3`: 試合に属さない取得記録。応答の種類ごと。")
    add("- `images/<SHA-256 の先頭2桁>.sqlite3`: 画像。")
    add("- `system/`: 取得キュー・収集の各回・監査・設定などの運用記録。`system/archive_change_feed/` は変更の記録。")
    add("- `unplaced/`: 上の規則で決まらなかった行（参照先の無い行など）。捨てずにここに置く。")
    add("- `xlsx/`: 上と同じ階層の xlsx 版。")
    add("")
    add("値が無いときの名前: 分類なし `unclassified`、ルールなし `no-rule`、試合日時不明 `unknown-month`。"
        "英数字・`_`・`-` 以外の文字は `~` と16進2桁で書く。")
    add("")
    add("どの部品も正本と同じ表・索引・ビューを持つ。部品を一つ開けば、正本と同じ SQL（`battle_players` や `analysis_xmatch` などのビュー）がその範囲でそのまま動く。"
        "複数の部品は ATTACH して同じ表名どうしを UNION ALL すればよい。")
    if homes:
        add("")
        add("| 表 | 置き場所 | パス |")
        add("| --- | --- | --- |")
        for table_name, rule_ja, pattern in homes:
            add(f"| {table_name} | {rule_ja} | `{pattern}` |")
    add("")
    add("## 分析セット（モード）の分け方")
    add("")
    add("ナワバリ、オープン、チャレンジ、イベマ、Xマッチ、フェス、プラベ（4対4・3対3・2対2・1対1・それ以外）、バイト、ビッグラン、バイトチームコンテストは別の母集団で、混ぜて集計しない。"
        "公開戦の人数差は回線落ちとしてそのモードに残す。区分できない試合は `hold`。")
    if labels:
        add("")
        add("| 種類 | コード | 日本語名 |")
        add("| --- | --- | --- |")
        for kind, code, ja in labels:
            add(f"| {kind} | `{code}` | {ja} |")
    if sets:
        add("")
        add("現在の試合数:")
        add("")
        add("| analysis_set | rule_raw | 試合数 |")
        add("| --- | --- | --- |")
        for analysis_set, rule_raw, count in sets:
            add(f"| {analysis_set} | {rule_raw} | {count} |")
    add("")
    add("## 何を知りたいとき、何と何をどう合わせるか")
    add("")
    for question, steps, sql in RECIPES:
        add(f"### {question}")
        add("")
        add(steps)
        add("")
        add("```sql")
        add(sql)
        add("```")
        add("")
    add("## 更新と完全性")
    add("")
    add("NAS の正本が更新されると、変わった部品だけが作り直され、Drive へ送られ、Drive 側の SHA-256 で照合されたあとで最後に `catalog.sqlite3` が置き換わる。"
        "`catalog.sqlite3` の `files` にある SHA-256 と部品が一致しないときは、更新の途中なので少し待って読み直す。")
    if status:
        add("")
        for key in ("through_event_id", "built_at", "published_at", "last_audit_at", "last_audit_result", "rule_version"):
            if key in status:
                add(f"- {key}: {status[key]}")
    add("")
    add("旧方式のフォルダ（SQuidLite3 直下の `database/`）と旧 `分析.xlsx` は情報が欠けているので使わない。")
    add("")
    return "\n".join(lines)
