# SQuidLite3 データ配置（設計 0.2・2026-10-05 Opus 5.5）

## この設計が満たす前田さんの指示（要点。全文は開発ログの原文）

- 統合 DB が大きくなるのは構わない。問題は可用性。完全性・体系性を保ったまま、構造化・階層化で小さな DB 部品一つ一つに整理し、読取負荷を減らす（2026-10-05）。
- どこに何が置かれていて、何を分析したいときは何と何をどう合わせればよいかが、いつでも一意に、少ない推論で特定できるようにする（2026-10-05）。
- 体系性を失わないよう完全に構造化した上で、一つ一つは小さく切って整理する。要素還元的に、あらゆるデータの有機的つながりが破綻なく成立する（2026-10-02）。
- 分割版も一切省略しない。原文・画像 BLOB・取得状態・全表全列・全参照を保持する（2026-10-02）。
- Google Drive には NAS と同じデータを省略なく平文で置き、リアルタイムに更新する。ChatGPT から接続して使う。AI に使いやすく構造化・階層化する（2026-10-02）。
- ルールごと・モードごとに区切った版を統合版とは別に持つ（2026-10-01）。
- xlsx にも SQLite の情報をすべて省略せず載せる。複数の xlsx に分かれても、部分的に重複してもよい（2026-10-02）。

## 旧方式（Sol の lossless baseline + delta chain）をやめる理由

1. 置き場所が意味で決まらない。表ごと・行番号区間ごとの断片なので、ある試合のデータがどのファイルにあるかは manifest を引かないとわからない。モード・ルール別の入口は全共通断片 1,346 個への参照を持つだけで、入口を開いても中身が無い。
2. 現在の状態を読むには baseline に差分を順に適用する必要がある。差分は 345 本たまっており、読み手の負荷と推論がむしろ増える。
3. 公開が全情報 XLSX（959 断片・約 11GB）の完了に縛られ、10-02 から一度も latest が公開されていない。

旧方式の生成物（NAS `database/slices/`、`backups/full-data-*`、Drive `database/`）は削除しない。新方式の公開後に README で「旧方式・使わない」と明示する。

## 原則

1. **本籍は一つ、行の中身から決まる。** どの行も、その行と、それが参照する行の値だけから計算できる一つの「本籍」部品を持つ。規則は下の表のとおりで例外を作らない。規則で決まらない行は `unplaced/` に入れる。捨てる行は無い。
2. **部品は正本と同じ形。** すべての部品に、正本の全表・全索引・全ビューを正本と同じ `CREATE` 文で作る（トリガーは作らない。正本のトリガー定義は目録に記録する）。行は `rowid` を含めてそのまま写す。型・BLOB・NULL・巨大整数・テキストのバイト列を変換しない。どの部品を開いても、正本と同じ SQL（`analysis_xmatch` などのビューを含む）がその部品の範囲でそのまま動く。複数の部品を `ATTACH` して `UNION ALL` すれば広い範囲を読める。
3. **写しは本文だけ。** 応答の本文 `bodies` は、その応答と同じ部品に必ず入れる。複数の部品の応答が同じ本文を共有するときは、各部品に同じ行が入る（写し）。画像は写さない（下の画像の規則）。
4. **目録が一枚ある。** `catalog.sqlite3` に、全部品の住所・バイト数・SHA-256・表ごとの行数、全試合の一覧、応答と画像の住所録、本籍規則、日本語名の対応、分析の手引きを置く。たいていの集計は目録だけで済む。目録は公開の確定点であり、最後に置く。
5. **更新は変わった部品だけ。** 正本の変更追跡 `archive_change_feed`（表名・旧 rowid・新 rowid・旧キー・新キー）と、状態 DB に持つ前回の本籍 `row_homes` から、変わった行の移動元と移動先の部品を求め、その部品だけを作り直す。
6. **完全性は部品の和で検査する。** 全表について、本籍部品の行（写しを除く）の和が正本の行全体と一致すること（`rowid` と全列の値のハッシュで照合）を検査し、結果を目録に記録する。

## 置き場所

NAS: `/home/Natsuki/ikaring-archive/database/parts/`（正本 DB と同じボリューム。正本の置き場所は変えない）。
Drive: フォルダ `SQuidLite3`（ID `1wkKHKcc5YYlQBfHVspuY5n0cRuuGh6dp`）の下の `db/`。NAS の `parts/` と同じ相対パス。

```
db/
  README_FOR_AI.md          目録から生成する読み方の案内（人と AI 向け）
  catalog.sqlite3           目録（公開の確定点）
  catalog.xlsx              目録の xlsx 版
  matches/<analysis_set>/<rule_raw>/<YYYY-MM>.sqlite3
  responses/<operation>/<期間>.sqlite3
  images/<SHA-256 の先頭2桁>.sqlite3
  images/no-body.sqlite3
  system/<表名>.sqlite3
  system/archive_change_feed/<YYYY-MM-DD>/<HH>.sqlite3
  unplaced/<表名>.sqlite3
  xlsx/                     上と同じ階層の xlsx 版
```

ディレクトリ名とファイル名は正本の値（`analysis_set`、`rule_raw`、`operation`）をそのまま使う。値が無いときは `unclassified`（分類なし）、`no-rule`（ルールなし）、`unknown-month`（試合日時不明）とする。`[A-Za-z0-9_-]` 以外の文字は `~` と 2 桁の 16 進で書く。日本語名は目録の `labels` 表と README で対応させる。

## 本籍規則（版 1）

用語: 「試合の本籍」は `matches/<analysis_set>/<rule_raw>/<月>`。`analysis_set` と `rule_raw` は `match_classification` の値、月は `matches.detail_response_id` の `documents.json_text` の `$.playedTime` を日本時間にした年月。分類が無ければ `unclassified/no-rule`、playedTime が無ければ `unknown-month`（観測日時で埋めない）。
「応答の本籍」は、その応答を `response_id` に持つ `documents` 行があれば、その試合の本籍（複数あれば (account, kind, match_key) が最小のもの）。無ければ `responses/<operation>/<期間>`（期間の粒度は実測で確定。下の未確定を参照）。

| 正本の表 | 本籍 |
| --- | --- |
| matches, match_classification | その試合の本籍 |
| match_refs, sightings, documents | (account, kind, match_key) の試合の本籍。試合行が無ければ `unplaced/` |
| match_tags | (account, match_key) が一致する試合の本籍。kind が二つ当たれば vs を優先。無ければ `unplaced/` |
| rate_points（match_key が `fetch:` で始まらない） | (account, match_key) の試合の本籍（kind は vs を優先） |
| rate_points（match_key が `fetch:<event_id>`） | `response_fetches.event_id` がその event_id の応答の本籍 |
| jobs（kind と match_key がある） | その試合の本籍。試合行が無ければ `system/jobs` |
| jobs（それ以外） | `system/jobs` |
| responses | 応答の本籍 |
| response_fetches, asset_refs, entities | `response_id` の応答の本籍 |
| issues（response_id がある） | その応答の本籍 |
| issues（それ以外） | `system/issues` |
| bodies（応答の本文） | それを参照する各応答の本籍（写しを含む）。本籍の代表は最小の response_id の応答の部品 |
| assets と、画像の本文 bodies | `images/<body_sha256 の先頭2桁>`。本文が無い assets は `images/no-body` |
| runs, control, manifests, endpoint_heads, page_fingerprints, schema_version, analysis_genre | `system/<表名>` |
| archive_change_feed | `system/archive_change_feed/<changed_at の JST 日付>/<時>` |
| 上のどれにも当たらない表・行 | `unplaced/<表名>` |

`sqlite_sequence` と `sqlite_stat*` は部品に入れず、目録の `source_sqlite_internal` 表へ全行を写す。

## 目録 catalog.sqlite3

- `files`: path, domain, analysis_set, rule_raw, month, operation, period, bytes, sha256, rows_json（表ごとの本籍行数と写し行数）, built_through_event_id, built_at
- `match_index`: account, kind, match_key, analysis_set, rule_raw, rule_name, played_time, stage, judgement, my_weapon, tags, detail_available, part_path
- `response_index`: response_id, account, operation, fetched_at, http_status, part_path
- `asset_index`: url, state, body_sha256, content_type, part_path
- `table_homes`: table_name, rule_ja, path_pattern
- `labels`: kind（analysis_set / rule_raw / operation）, code, ja
- `recipes`: question_ja, steps_ja, sql（例: Xマッチのルール別勝率は `match_index` だけで足りる。Xマッチの各試合の全プレイヤーのブキは `matches/xmatch/*/*.sqlite3` を開き `battle_players` ビュー。画像は `asset_index` で url から部品を引く）
- `status`: through_event_id, built_at, published_at, last_audit_at, last_audit_result, rule_version
- `source_schema`: 正本の sqlite_master 全行（トリガー含む）
- `source_sqlite_internal`: sqlite_sequence と sqlite_stat* の全行

## 更新の手順（worker）

1. 正本を読み取り専用で開き、一つの読み取りトランザクションの中で以下を行う（同じ静止点）。
2. `through = MAX(archive_change_feed.event_id)`。
3. 全試合の本籍と全応答の本籍を計算し、状態 DB の前回値と比べる。変わった試合・応答は、旧本籍と新本籍の両方を「作り直す部品」に入れる。
4. 前回の through より後の変更追跡の各行について、`old_rowid` を状態 DB の `row_homes` で引いた部品と、`new_rowid` の行の現在の本籍を「作り直す部品」に入れる。
5. 作り直す部品ごとに、一時ファイルへ全表・全索引・全ビューを作り、本籍がその部品である行と写しを入れ、`_part` 表（住所・規則版・through・作成時刻・正本スキーマのハッシュ）を書き、fsync して置き換える。`row_homes` をその部品の行で置き換える。
6. 作り直した部品を Drive へ送り、Drive が計算した SHA-256 と照合する。一致しなければ再送し、成功するまで目録を進めない。
7. 目録を作り直して送り、照合する。状態 DB の through を進める。
8. 初回は全部品を作る。

完全性の監査（日次）は、全表について正本と本籍部品の和を `rowid` と全列値のハッシュで照合し、結果を目録の `status` に記録する。

## 統合版

統合 SQLite（正本の静止点）も Drive に置く。部品の和が正本と同じであることを常時の保証とし、統合版の写しの更新頻度は転送量を測って決める。

## XLSX

各部品と目録に、同じ住所の xlsx を作る（`xlsx/` 以下）。セル上限を超える値は既存の無損失エンコード（`lossless_xlsx.py`）で順序付き断片に分ける。SQLite 部品の公開を xlsx の完了で待たせない。

## 未確定

- `responses/<operation>/<期間>` の期間の粒度（月か日か、ランキングは開催回ごとか）。静止点 20261002T111654Z の実測で決める。
- 統合版の写しの更新頻度。
