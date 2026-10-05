"""SQuidLite3 の部品の読み方（目録の recipes と README_FOR_AI.md の本文）。

目録 catalog.sqlite3 の recipes 表に入れる行と、Drive の db/README_FOR_AI.md を作る。
どこに何があり、何を知りたいときにどの部品とどの部品をどのキーで合わせればよいかを、
推論なしに辿れるように書く。規則の正本は docs/design/SQuidLite3_データ配置.md（本籍規則 版 3）。
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
        "試合は 1 試合 1 ファイル。match_index でその試合の part_path を引き、その 1 ファイルを開く。matches・"
        "match_classification・documents・sightings（詳細の目撃記録）・responses（詳細の応答）・bodies（応答本文のバイト列）が"
        "そろっている。正本と同じビューも使える。",
        "SELECT m.*, c.*, d.json_text FROM matches m "
        "JOIN match_classification c USING(account, kind, match_key) "
        "JOIN documents d ON d.response_id = m.detail_response_id AND d.match_key = m.match_key "
        "WHERE m.match_key = :match_key",
    ),
    (
        "あるモード・ルールの全試合の全プレイヤーのブキや成績を見たい",
        "試合は 1 試合 1 ファイルなので、複数の試合の分析は、まず次の「試合のファイルの一覧」で該当ファイルを列挙する。"
        "そのファイルを開いて（または ATTACH して）battle_players ビューを読み、同じ SQL の結果を足し合わせる。"
        "SQLite が同時に ATTACH できるのは既定で 10 個まで（上限は 125 個）なので、数が多いときは 1 ファイルずつ開いて結果を足すか、"
        "Drive の統合版（正本の静止点）を使う。件数や勝敗の集計だけなら match_index で足りる。",
        "SELECT match_key, team_index, is_my_team, name, weapon, paint, kills, assists, deaths, specials "
        "FROM battle_players ORDER BY match_key, team_index, player_index",
    ),
    (
        "試合のファイルの一覧（あるモード・ルール・期間の試合が入っているファイル）が欲しい",
        "catalog.sqlite3 の files 表で domain が matches の行を、analysis_set（モード）・rule_raw（ルール）・month・day・"
        "match_key で絞る。path がそのファイル。試合の分類や日時があとから決まると試合は新しい場所へ移り、"
        "元のファイルは空のまま残るので、rows_json が {} のファイルは飛ばす。1 試合の場所だけなら match_index の part_path。",
        "SELECT path, match_key, month, day FROM files WHERE domain = 'matches' AND analysis_set = :analysis_set "
        "AND rule_raw = :rule_raw AND rows_json <> '{}' ORDER BY day, match_key",
    ),
    (
        "バイトの WAVE ごとの結果やオオモノを見たい",
        "matches/salmon_regular/（ビッグランは big_run、バイトチームコンテストは team_contest）の部品を開き、"
        "salmon_waves・salmon_bosses・salmon_players ビューを読む。",
        "SELECT match_key, wave_index, json_text FROM salmon_waves ORDER BY match_key, wave_index",
    ),
    (
        "パワー・ポイント・レート・納品数の推移を見たい",
        "試合に付くレートは各試合のファイルの rate_points 表にある（series_id ごとの系列）。"
        "ブキのチョーシなど取得時点の値は match_key が fetch: で始まる行で、その取得の応答と同じ部品にある。"
        "系列の一覧と所在は files 表の rows_json（部品ごとの表の行数）から rate_points を含む部品を引く。",
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
        "応答の種類（operation）ごとに responses/<operation>/ の下にある（取得した時刻の日本時間の時ごとの部品。"
        "ランキング系の種類は 1 応答 1 部品）。どの種類があるかは files 表の operation（day・hour も）、個々の応答の住所は response_index。"
        "一覧の応答の目撃記録（sightings）はその一覧の応答の部品にある。"
        "entities 表に応答から取り出した型と ID ごとの記録がある。",
        "SELECT operation, count(*) FROM response_index GROUP BY operation ORDER BY 2 DESC",
    ),
    (
        "同じ応答を何回、いつ取り直したかを知りたい",
        "取得の記録（response_fetches）は fetches/<YYYY-MM>/<YYYY-MM-DD>.sqlite3 に、取得した日（日本時間）ごとにある。"
        "同じ応答を取り直しても応答の部品は変わらず、この日ごとの部品に行が増える。response_id で応答（response_index や"
        "応答の部品の responses 表）と結ぶ。fetches の日付の部品は files 表で domain が fetches の行。",
        "SELECT response_id, count(*) AS fetch_count, min(fetched_at) AS first_fetched_at, max(fetched_at) AS last_fetched_at "
        "FROM response_fetches GROUP BY response_id ORDER BY fetch_count DESC",
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
    status_row = _rows(
        catalog,
        "SELECT through_event_id, built_at, published_at, last_audit_at, last_audit_result, rule_version FROM status",
    )
    status = {}
    if status_row:
        names = ("through_event_id", "built_at", "published_at", "last_audit_at", "last_audit_result", "rule_version")
        status = {k: v for k, v in zip(names, status_row[0]) if v is not None}
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
    add("- `matches/<analysis_set>/<rule_raw>/<YYYY-MM>/<YYYY-MM-DD>/<match_key>.sqlite3`: 試合ごとのデータ。**1 試合が 1 ファイル**。"
        "モード（分析セット）とルールと、試合日時（日本時間）の年月日の下に、試合の `match_key` を名前にして置く。"
        "その試合のファイルは目録 `match_index` の `part_path` で分かる。")
    add("- `responses/<operation>/<YYYY-MM>/<YYYY-MM-DD>/<HH>.sqlite3`: 試合に属さない取得記録（一覧の応答など）。"
        "応答の種類ごと、取得日時（日本時間）の時ごと。"
        "ランキング系の種類だけは 1 応答 1 部品で `responses/<operation>/<YYYY-MM>/<YYYY-MM-DD>/<response_id>.sqlite3`。"
        "一覧の応答の目撃記録（`sightings`）は、その一覧の応答の部品にある（詳細の応答の目撃記録は試合のファイルにある）。")
    add("- `fetches/<YYYY-MM>/<YYYY-MM-DD>.sqlite3`: 取得の記録（`response_fetches`）。取得した日（日本時間）ごと。"
        "同じ応答を取り直した記録はここに増え、応答の部品は変わらない。")
    add("- `images/<SHA-256 の先頭2桁>.sqlite3`: 画像。")
    add("- `system/`: 取得キュー・収集の各回・監査・設定などの運用記録。`system/archive_change_feed/` は変更の記録。")
    add("- `unplaced/`: 上の規則で決まらなかった行（参照先の無い行など）。捨てずにここに置く。")
    add("- `xlsx/`: 上と同じ階層の xlsx 版。")
    add("")
    add("「応答の本籍」とは、その応答の行（`responses` 表）が置かれる部品のこと。試合の詳細の応答（`documents` に行がある応答）は試合のファイル、"
        "それ以外は `responses/` の下の部品。`sightings`・`asset_refs`・`entities` と応答の本文 `bodies` は、その `response_id` の応答の本籍に置かれる"
        "（`bodies` は同じ本文を持つ応答の部品ごとに写しが入る。写しは各部品の `_copies` 表に載る）。")
    add("")
    add("値が無いときの名前: 分類なし `unclassified`、ルールなし `no-rule`、日時不明 `unknown-date`"
        "（試合は `matches/<analysis_set>/<rule_raw>/unknown-date/<match_key>.sqlite3`、応答は `responses/<operation>/unknown-date.sqlite3`、"
        "ランキング系は `responses/<operation>/unknown-date/<response_id>.sqlite3`、取得の記録は `fetches/unknown-date.sqlite3`）。"
        "英数字・`_`・`-` 以外の文字（`match_key` の `:` など）は `~` と16進2桁で書く。")
    add("")
    add("試合の分類や日時があとから決まると、その試合は新しい場所のファイルへ移り、元の場所には空のファイルが残る"
        "（`files.rows_json` が `{}`）。試合の今の場所は `match_index.part_path` が正しい。")
    add("")
    add("どの部品も正本と同じ表・索引・ビューを持つ。部品を一つ開けば、正本と同じ SQL（`battle_players` や `analysis_xmatch` などのビュー）がその範囲でそのまま動く。"
        "複数の試合を分析するときは、`files` 表（`domain` が `matches`。`analysis_set`・`rule_raw`・`month`・`day`・`match_key` で絞る）で該当ファイルを列挙し、"
        "ATTACH して同じ表名どうしを UNION ALL する（SQLite が同時に ATTACH できるのは既定で 10 個まで）。"
        "数が多いときは 1 ファイルずつ開いて結果を足すか、統合版を使う。集計だけなら `match_index` で足りる。")
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
