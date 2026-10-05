# SQuidLite3 データ配置（設計 0.4・2026-10-05 Opus 5.5。2026-10-06 に空の部品の片付け・fetch 行の rate_points の本籍・目録 files の列を追記）

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


## 版 3 の改定（設計 0.4、2026-10-05 23時の実測にもとづく）

実測（版 2、NAS 試運転）: 追いついた後のふだんの1周期が 531秒・48部品・1,206MB。1日1モード1ルールの部品が最大 141.9MB。原因は、(1) 同じ応答の取り直しのたびに収集が一覧に載る全試合の `matches.last_seen` を書き換えていた（store.py の再観測の経路）、(2) 一覧の目撃記録 `sightings` と取り直しの記録 `response_fetches` が古い部品に住所を持ち、古い部品を毎回作り直させていた、(3) 試合の部品が日単位で大きかった。

前田さんの判断（2026-10-05）: 同じ内容の再観測では書き換えない。

### 収集側の変更（正本）

- 同じ応答の取り直し（再観測）では、`matches` の行を書き換えない。新しい内容の応答でも、値が変わらない更新はしない。
- `entities` は JSON が変わったときだけ書き換える（`response_id` は、その内容を最初に観測した応答を指す）。
- 正本にビュー `match_observations(account, kind, match_key, first_observed_at, last_observed_at, observing_responses)` を足す。`last_observed_at` は、その試合を載せた応答の最新の取得時刻（取り直しを含む）。2026-10-05 の実測で、変更前の `matches.last_seen` と全992試合で一致する式（`first_observed_at` も `first_seen` と全件一致）。以後の `matches.last_seen` は「その試合を載せた新しい内容の応答を最後に保存した時刻」になる。

### 本籍規則 版 3

- 試合は1試合1部品: `matches/<analysis_set>/<rule_raw>/<YYYY-MM>/<YYYY-MM-DD>/<match_key>.sqlite3`（日時不明は `matches/<analysis_set>/<rule_raw>/unknown-date/<match_key>.sqlite3`）。
- 試合詳細でない応答: `responses/<operation>/<YYYY-MM>/<YYYY-MM-DD>/<HH>.sqlite3`（fetched_at の日本時間の時）。ランキング系は従来どおり1応答1部品。
- `sightings` は、その応答（response_id）の本籍に置く（一覧の目撃記録は一覧の応答の部品へ、詳細の目撃記録は試合の部品へ）。
- `response_fetches` は、自身の fetched_at の日本時間の日付で `fetches/<YYYY-MM>/<YYYY-MM-DD>.sqlite3`。fetched_at を解析できない行は `fetches/unknown-date.sqlite3`（観測日時などで埋めない）。
- `rate_points` のうち match_key が `fetch:<event_id>` の行（ブキのチョーシなど取得時点の値）は、`response_fetches.event_id` がその event_id の行の本籍（その取得の日の `fetches/<YYYY-MM>/<YYYY-MM-DD>.sqlite3`、fetched_at を解析できなければ `fetches/unknown-date.sqlite3`）。対応する `response_fetches` の行が無ければ `unplaced/rate_points.sqlite3`。応答の本籍には依らないので、取り直しのたびに古い応答の部品が作り直されることは無い。
- そのほかは版 2 と同じ。
- 規則の版が変わったときは、空の出力先に全部品を作る（古い版の部品を同じ場所に混ぜない）。

### 空になった部品の片付け（版 3 に追加）

- 作り直した結果、本籍の行（写しを含む）が1つも無い部品（スキーマと `_part` だけになるもの）は作らない。すでにある部品がそうなったとき（試合の分類や日時があとから決まって新しい部品へ移った、行が消えた、など）は、出力先のファイルを削除し、状態 DB の `part_files`・`row_homes` と目録の `files` から外す。初回の全件作成では空の部品を作らない。したがって `files` に載る部品は、どれも行が1つ以上ある。
- 削除の対象は派生の部品だけ。出力先の `.sqlite3` のうち状態 DB の `part_files` に記録があるものだけを消す。正本・目録・部品でないファイル・記録の無い `.sqlite3` は同じ住所にあっても消さない。部品として消してよい住所でないもの（目録、出力先の外、`.sqlite3` でないものなど）が記録にあれば、何も消さずに止まる。
- worker は Drive からも消す。状態 DB の `published` にあって `part_files` に無く、手元にファイルも無い部品を `rclone deletefile <remote>/<path>`（run_rclone 経由、シェル不使用）で消し、消せたら `published` から外す。消せなかったら `published` に残し、次の周期に再試行する（Drive に既に無いとき＝rclone の終了コード 3・4 は消せたものとして扱う）。全部品の照合が済み、新しい目録を送って照合できたあとに消す（Drive 上の目録が Drive に無い部品を指す時間を作らない。Opus 判断 2026-10-06）。目録を送れなかった周期は消さずに次の周期へ持ち越す。消した部品の住所は周期の JSON 行の `deleted` に出す。

## 置き場所

NAS: `/home/Natsuki/ikaring-archive/database/parts/`（正本 DB と同じボリューム。正本の置き場所は変えない）。
Drive: フォルダ `SQuidLite3`（ID `1wkKHKcc5YYlQBfHVspuY5n0cRuuGh6dp`）の下の `db/`。NAS の `parts/` と同じ相対パス。

```
db/
  README_FOR_AI.md          目録から生成する読み方の案内（人と AI 向け）
  catalog.sqlite3           目録（公開の確定点）
  catalog.xlsx              目録の xlsx 版
  matches/<analysis_set>/<rule_raw>/<YYYY-MM>/<YYYY-MM-DD>/<match_key>.sqlite3   （版 3: 1 試合 1 部品）
  responses/<operation>/<YYYY-MM>/<YYYY-MM-DD>/<HH>.sqlite3                      （版 3: 日本時間の時ごと）
  responses/<ランキング系の operation>/<YYYY-MM>/<YYYY-MM-DD>/<response_id>.sqlite3
  fetches/<YYYY-MM>/<YYYY-MM-DD>.sqlite3                                          （版 3: response_fetches と取得時点の rate_points）
  images/<SHA-256 の先頭2桁>.sqlite3
  images/no-body.sqlite3
  system/<表名>.sqlite3
  system/archive_change_feed/<YYYY-MM-DD>/<HH>.sqlite3
  unplaced/<表名>.sqlite3
  xlsx/                     上と同じ階層の xlsx 版
```

ディレクトリ名とファイル名は正本の値（`analysis_set`、`rule_raw`、`operation`）をそのまま使う。値が無いときは `unclassified`（分類なし）、`no-rule`（ルールなし）、`unknown-month`（試合日時不明）とする。`[A-Za-z0-9_-]` 以外の文字は `~` と 2 桁の 16 進で書く。日本語名は目録の `labels` 表と README で対応させる。

## 本籍規則（版 1）

用語: 「試合の本籍」は `matches/<analysis_set>/<rule_raw>/<YYYY-MM>/<YYYY-MM-DD>.sqlite3`。`analysis_set` と `rule_raw` は `match_classification` の値、日付は `matches.detail_response_id` の `documents.json_text` の `$.playedTime` を日本時間にした年月と年月日。分類が無ければ `unclassified/no-rule`、playedTime が無ければ `matches/<analysis_set>/<rule_raw>/unknown-date.sqlite3`（観測日時で埋めない）。
「応答の本籍」は、その応答を `response_id` に持つ `documents` 行があれば、その試合の本籍（複数あれば (account, kind, match_key) が最小のもの）。無ければ `responses/<operation>/<YYYY-MM>/<YYYY-MM-DD>.sqlite3`（fetched_at の日本時間の日付）。ただし下の「ランキング系の operation」は 1 応答 1 部品で `responses/<operation>/<YYYY-MM>/<YYYY-MM-DD>/<response_id>.sqlite3`。

| 正本の表 | 本籍 |
| --- | --- |
| matches, match_classification | その試合の本籍 |
| match_refs, sightings, documents | (account, kind, match_key) の試合の本籍。試合行が無ければ `unplaced/` |
| match_tags | (account, match_key) が一致する試合の本籍。kind が二つ当たれば vs を優先。無ければ `unplaced/` |
| rate_points（match_key が `fetch:` で始まらない） | (account, match_key) の試合の本籍（kind は vs を優先） |
| rate_points（match_key が `fetch:<event_id>`） | `response_fetches.event_id` がその event_id の行の本籍（版 3: その取得の日の `fetches/<YYYY-MM>/<YYYY-MM-DD>`）。その行が無ければ `unplaced/rate_points` |
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

- `files`: path, domain, analysis_set, rule_raw, month, day, operation, period, response_id, match_key, hour, bytes, sha256, rows_json（表ごとの本籍行数と写し行数）, built_through_event_id, built_at。住所から決まる列は、`domain`＝住所の先頭の区間（matches / responses / fetches / images / system / unplaced）、matches の部品は `analysis_set`・`rule_raw`・`month`・`day`・`match_key`、responses の部品は `operation`・`month`・`day`・`period`（＝`day`）と、通常の operation は `hour`（日本時間の時、2桁）・ランキング系は `response_id`、fetches の部品は `month`・`day`。該当しない列は NULL。日時不明の部品は `month`・`day`（responses は `period` も）が `unknown-date`。`files` に載る部品は、どれも行が1つ以上ある（空の部品は載せない）
- `match_index`: account, kind, match_key, analysis_set, rule_raw, rule_name, played_time, stage, judgement, my_weapon, tags, detail_available, part_path
- `response_index`: response_id, account, operation, fetched_at, http_status, part_path
- `asset_index`: url, state, body_sha256, content_type, part_path
- `table_homes`: table_name, rule_ja, path_pattern
- `labels`: kind（analysis_set / rule_raw / operation）, code, ja
- `recipes`: question_ja, steps_ja, sql（`parts_guide.RECIPES` の13件。目録を作るたびに埋める。例: Xマッチのルール別勝率は `match_index` だけで足りる。Xマッチの各試合の全プレイヤーのブキは `matches/xmatch/*/*.sqlite3` を開き `battle_players` ビュー。画像は `asset_index` で url から部品を引く）
- `status`: through_event_id, built_at, published_at, last_audit_at, last_audit_result, rule_version
- `source_schema`: 正本の sqlite_master 全行（トリガー含む）
- `source_sqlite_internal`: sqlite_sequence と sqlite_stat* の全行

## 更新の手順（worker）

1. 正本を読み取り専用で開き、一つの読み取りトランザクションの中で以下を行う（同じ静止点）。
2. `through = MAX(archive_change_feed.event_id)`。
3. 全試合の本籍と全応答の本籍を計算し、状態 DB の前回値と比べる。変わった試合・応答は、旧本籍と新本籍の両方を「作り直す部品」に入れる。
4. 前回の through より後の変更追跡の各行について、`old_rowid` を状態 DB の `row_homes` で引いた部品と、`new_rowid` の行の現在の本籍を「作り直す部品」に入れる。
5. 作り直す部品ごとに、一時ファイルへ全表・全索引・全ビューを作り、本籍がその部品である行と写しを入れ、`_part` 表（住所・規則版・through・作成時刻・正本スキーマのハッシュ）を書き、fsync して置き換える。`row_homes` をその部品の行で置き換える。本籍の行が1つも無い部品は作らず、あれば削除して `part_files`・`row_homes`・目録から外す（上の「空になった部品の片付け」）。
6. 作り直した部品を Drive へ送り、Drive が計算した SHA-256 と照合する。一致しなければ再送し、成功するまで目録を進めない。
7. 目録を作り直して送り、照合する。状態 DB の through を進める。そのあとで、手元から削除された部品を Drive からも消す。
8. 初回は全部品を作る（空の部品は作らない）。

完全性の監査（日次）は、全表について正本と本籍部品の和を `rowid` と全列値のハッシュで照合し、結果を目録の `status` に記録する。監査の途中で、行が無くなった部品が片付けられても（ファイルが消えても）落ちず、`vanished` に住所を載せて読み飛ばす（その部品の行がほかの部品にも無ければ、不一致として数える）。

## 統合版

統合 SQLite（正本の静止点）も Drive に置く。部品の和が正本と同じであることを常時の保証とし、統合版の写しの更新頻度は転送量を測って決める。

## XLSX

各部品と目録に、同じ住所の xlsx を作る（`xlsx/` 以下）。セル上限を超える値は既存の無損失エンコード（`lossless_xlsx.py`）で順序付き断片に分ける。SQLite 部品の公開を xlsx の完了で待たせない。

## 粒度の根拠（静止点 20261002T111654Z の実測、2026-10-05）

- EventMatchRankingPeriodQuery: 応答839件、json_text 3.26GB・本文 3.30GB・entities 75.2万行 11.6GB・asset_refs 522万行。838件が 2026-09-22 の一日に取得。種類×月や種類×日では 1 部品が数 GB〜18GB になるため、1 応答 1 部品（1 件あたり約 20〜30MB）にする。
- VsHistoryDetailQuery: 1試合の詳細は json_text 約19万字で、応答・本文・documents・sightings の4か所に同じ原文がある（正本の設計どおり。省略しない）。1試合あたり約1.4MB。モード×ルール×月ではオープン1ルールで 200〜350MB になり、遊んでいる間の再作成と再送が重いので、日ごとにする（1部品 10〜30MB 程度）。
- 試合以外の多くの種類は 1 日 1〜数件で、種類×日なら 1 部品は数 MB。
- 画像は png 4,782 件 467MB・jpeg 249 件 9MB。SHA 先頭2桁の 256 部品で 1 部品約 2MB。

## ランキング系の operation（1 応答 1 部品。版 1 の固定一覧）

EventMatchRankingPeriodQuery, EventMatchRankingSeasonPaginationQuery, EventMatchRankingQuery, RankingHoldersFestTeamRankingHoldersPaginationQuery, WeaponRankingDetail_Ranking_RefetchQuery, WeaponRankingDetailQuery, XRankingDetailQuery, XRankingRefetchQuery, DetailRankingQuery, DetailTabViewWeaponTopsArRefetchQuery, DetailTabViewWeaponTopsClRefetchQuery, DetailTabViewWeaponTopsGlRefetchQuery, DetailTabViewWeaponTopsLfRefetchQuery, DetailTabViewXRankingArRefetchQuery, DetailTabViewXRankingClRefetchQuery, DetailTabViewXRankingGlRefetchQuery, DetailTabViewXRankingLfRefetchQuery。この一覧を変えたら規則の版を上げ、全部品を作り直す。

## 毎周期の計算量（版 1 の実装要件）

毎周期に全行（asset_refs だけで約650万行）の本籍を計算し直さない。試合の本籍（約千行）と応答の本籍（約八千行）は毎周期すべて計算して前回と比べ、本籍が変わった試合・応答に属する行と、変更追跡に現れた行だけについて、行ごとの本籍を計算し直す。初回と規則の版が変わったときだけ全行を計算する。

## 未確定

- 統合版の写しの更新頻度。
