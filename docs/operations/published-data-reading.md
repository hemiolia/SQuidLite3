# 公開済み全情報SQLiteのローカル読取

`scripts/published_sqlite_reader.py` の `PublishedSQLiteReader` は、ローカルにそろえた全情報baseline、差分世代、publisher control一式を検証して、指定されたgenerationのSQLite表を読み出す。Reader自身はNASやDriveへ接続せず、統合済みの `source.sqlite3` も作らない。差分を含む場合はbaseline shardと順番どおりの差分transportから必要な表を一時SQLiteへ組み立てる。

## 必要なローカル配置

Readerには3つのディレクトリを渡す。

`control_dir` はpublisher controlのローカルコピーで、少なくとも次のファイルを含む。

```text
control_dir/
  latest.json
  generations/<baseline-id>/index.json
  deltas/generations/<delta-id>/index.json
  deltas/generations/<delta-id>/delta-plan.json
```

`latest.json` が指す差分世代ごとに、その世代の `index.json` と `delta-plan.json` が必要になる。baseline-onlyの場合は差分世代のファイルは無い。ここに書いたIDは配置形式を示す名前であり、実世代IDではない。

`baseline_package` は全表のlossless shard一式とmanifest、verification、selector証明を含むbaseline packageである。Readerはmanifestが宣言する全ファイルを検証する。統合版raw SQLiteは不要。

`delta_generations_dir` は差分世代ごとのローカルpackageを置く親ディレクトリである。

```text
delta_generations_dir/
  <delta-id>/
    index.json
    delta-plan.json
    <delta-plan.jsonが宣言する全artifact>
```

各世代の `index.json` と `delta-plan.json` は、`control_dir` にある同じ世代のファイルとbyte単位で一致しなければならない。`delta-plan.json` が列挙するartifactもすべて、宣言された相対パスのまま配置する。Readerは差分index、plan、artifact inventory、各ファイルのbyte数とSHA-256、世代間のbaseline・親generation・schema・proofを照合する。publisher indexを持たない準備中packageや、control側と異なるindex/planは受け付けない。

baseline packageは、初回open時にmanifest・verification・selector証明と全宣言ファイルを検証する。さらにglobal baseline indexの `files` から `local` が `slices/` で始まる項目を取り出し、prefixを除いた相対パスの完全集合をbaseline readerの検証済みファイル集合と照合する。各項目のbyte数とSHA-256も一致しなければならない。baseline-onlyの `latest.json` では `captured_at` と `captured_at_kind` がbaseline indexの同名項目と一致することも検査される。

baseline package、control、差分packageのパスにシンボリックリンクを使わない。制御JSONの重複キーや不正値、欠落ファイル、undeclared SQLiteファイル、SQLiteのWAL・journal・SHM sidecar、path traversal、世代やschemaの不一致も読取を拒否する。差分packageはplanに対する全ファイルinventoryを照合する。Readerは入力ファイルを書き換えない。

## 読取と世代の固定

`__enter__` 時に `latest.json` のbytesを読み、その内容でbaselineと順序付き差分chainを固定する。`generation_id` はlatestが指す世代、`baseline_generation_id` はchainの基点を返す。`expected_generation_id` を指定するとlatestの世代IDとの一致を要求できる。`pinned_latest_sha256` は固定したlatest bytesのSHA-256、`captured_at` はpublisher controlに記録されたcapture時刻である。

Readerを開いた後にcontrolディレクトリのlatestが通常更新されても、開いているReaderは新しい世代へ切り替わらず、最初に固定したchainを読み続ける。更新を読むには新しいcontextを開く。これはlatestを開き直し続ける仕組みではなく、ローカルcontrol snapshotに対する読取である。

## API

公開APIはcontext内で使う。

- `tables()` は表ごとに `name`、`columns`、`column_schema`、`foreign_keys`、`row_count`、その表の `schema` を返す。全SQLite schema object一覧は別の `schema_objects()` が返す。
- `columns(table)`、`foreign_keys(table)`、`schema_objects()`、`row_count(table)` は表または全体のmetadataを返す。
- `iter_rows(table)` は `(ordinal, source_rowid, values)` を順次返す。`values` は元列順のtupleで、SQLite値はPythonの `None`、`int`、`float`、`str`、`bytes` として保持される。`source_rowid` は保存された元rowidで、元表にrowidが無い場合は `None`。`ordinal` は出力順の番号でありrowidとは別である。
- `get_unique_row(table, criteria)` は現在固定されているgenerationから、criteriaに型厳密一致する1行だけを返す。戻り値は `None` または `{"source_rowid": int | None, "values": tuple}` で、ordinalは含めない。0行なら `None`、2行以上なら `DELTA_LOOKUP_NOT_UNIQUE`、未知列や不正なcriteriaは安定した `DELTA_LOOKUP_*` categoryで失敗する。Pythonの `bool` は整数criteriaとして受理しない。INTEGER、REAL、TEXT、BLOBは型を区別し、REALはfloatの厳密表現で比較する。
- `iter_selected_matches(mode, rule_token=None)` は、現在generationの `match_classification` と `matches` 全表からselectorに一致するmatches行を順次返す。mode/ruleで保存対象の全表を削るAPIではない。
- `published_control_binding_verified` はopen時に固定したcontrol bytesとローカルpackageの対応を構造検証した状態を示す。`published_deltas_verified` は空でない有効delta chainの全世代について、publisher indexとローカルartifactの検証が成立した場合だけ真になる。baseline-onlyでは `published_control_binding_verified` が真でも `published_deltas_verified` は偽である。

baseline shardを直接読む `LosslessShardReader.lookup_rows_with_identity(table, criteria)` は、 `(ordinal, source_rowid, values)` を候補行ごとに返す。Reader open時に全宣言ファイルのbyte数・SHA-256とverification証明を検査し、process-local `VerifiedFiles` tokenへパス・ファイル・親directoryのfingerprintを結び付ける。lookup中のAPI境界ではfingerprint、完全な登録ファイル集合、SQLiteのinventory、行・列・外部値chunkのmetadataを再確認する。tokenによる境界再検査は全ファイルのSHA再計算を行わない。criteriaに合わない行では画像など大きな外部値を復元せず、候補行の値だけを読み戻す。このlookupでは全row-stream digestと全value-chunk fileの再集計は行わないため、全内容のSHA・source対照を再実施したことを意味しない。保存されている情報は変えず、通常の全行readerもその全検査を維持する。

`PublishedSQLiteReader.get_unique_row(table, criteria)` は同じ候補限定読取をbaseline-plus-deltaの、open時に固定したローカルgenerationへ適用する。固定済みのbaseline/delta control bindingとbaseline packageをlookup前後に検査し、差分readerは選択されたdelta artifactを自身の境界で検査する。open後にpublisherのlatestが進んでもreaderはその新世代へ切り替わらない。このAPIはremote latest、Drive上のobject、現在時点までの同期を確認せず、全row-stream digestも再計算しない。

差分lookupのoperation/identity mapはreaderの一時SQLite overlayに置くmetadata-onlyのquery cacheで、保存されたbaselineやdelta値の代替ではない。適格な候補経路はcache採用前に有効chain全世代のoperation metadataと、問い合わせ表の全transport row参照（条件に合わない行や後続generationで上書きされた行を含む）、baselineの全元rowid、generationごとのrow countを照合する。外部payloadを候補値として復元しない場合も、このmetadata検査は省かない。条件に合う行だけ値を完全復元する。行identity条件が適さない場合のfallbackは、問い合わせ対象の表を一時overlayへ値ごとmaterializeする。いずれの場合も宣言済みのbaselineとdelta packageが全情報の保存元であり、lookup cacheはprivate・一時的で公開artifactではない。

`VerifiedFiles.derive(prefix, records)` は、親tokenにすでに登録された `prefix/` 以下の非空ファイル集合について、相対パス・byte数・SHA-256が親記録と完全一致する場合に、親のfingerprintからchild-root用tokenを導く。親tokenを導出前と導出後にも検査し、内容の再hashはしない。派生tokenが証明する範囲は指定したchild subsetだけであり、親package全体の証明を置き換えない。chainを組む呼出元は親package tokenを保持し、child tokenと併せて親側の完全な入力guardを続ける。published `DeltaChainReader` はpublished modeでbaseline package全体と選択された各delta packageのcontrol/artifact tokenをAPI・iterator境界で再検査する。

`DeltaChainReader(require_published_deltas=True)` はbaseline-onlyの空chainも受け入れる。空chainでは `published_deltas_verified` は偽となる。空でない有効chainの場合は、選択された全世代にstrict publisher indexがそろうことを要求し、artifactとbaseline packageの境界も検査する。これは差分世代ごとのindex証明であり、baseline global index、global `latest.json`、Drive上の全artifact再読戻しやリアルタイム同期を単独では証明しない。

Readerの公開APIとiterator境界は、入力fingerprintとgeneration bindingを検査する。iteratorは消費し切るか `close()` し、context終了時には残ったiteratorもcloseする。早期closeはresourceを解放して境界検査を行うが、未完了の全行digest検査を完了したとは扱わない。closeや境界検査の失敗は成功として隠されない。APIとiteratorはcontext内だけで使える。context終了時に一時overlayとreader状態を破棄する。

使用形の例を示す。パスは説明用placeholderであり、この例は実packageを作成・検証した実行報告ではない。実際には上記条件を満たす既存のローカル配置を指定する。

```python
from pathlib import Path
import sys

sys.path.insert(0, "<code-root>/scripts")
from published_sqlite_reader import PublishedSQLiteReader

with PublishedSQLiteReader(
    control_dir=Path("<local-control-copy>"),
    baseline_package=Path("<verified-baseline-package>"),
    delta_generations_dir=Path("<local-delta-generations>"),
    expected_generation_id="<expected-generation-id>",
) as reader:
    print(reader.generation_id, reader.pinned_latest_sha256)
    for ordinal, source_rowid, values in reader.iter_rows("<table-name>"):
        # ここで必要な処理を行う。値そのものをログへ出さない。
        pass
```

controlの取得手順は、後述のPublisher controlのローカルmirrorを参照する。

## CLI使用形

`archive.py` から利用できる読取コマンドとして `published-list` と `published-read` を提供する。いずれも元のStoreやlive正本を開かず、ネットワーク接続も行わない。

### published-list

指定したローカルpackageとcontrolsを検証し、メタデータおよび表一覧をJSON形式で出力する。

```bash
python3 archive.py published-list \
  --controls "<local-control-copy>" \
  --root "<verified-baseline-package>" \
  --delta-root "<local-delta-generations>" \
  [--generation "<expected-generation-id>"]
```

出力は単一のJSON行で、以下のキーを含む。

- `role` / `reader_role`: `"published_lossless_sqlite_reader"`
- `baseline_generation`: 基点世代ID
- `latest_generation` / `generation`: 現在固定された最新世代ID
- `pinned_latest_sha256`: 固定された `latest.json` のSHA-256
- `captured_at`: publisher controlに記録されたキャプチャ時刻
- `published_control_binding_verified`: ローカルcontrol結合の構造検証結果
- `published_deltas_verified`: 差分chain検証結果（baseline-onlyの場合は `false`）
- `tables`: 全表の列情報、型情報、外部キー、行数
- `schema_objects`: 全schemaオブジェクト一覧
- `all_remote_artifacts_verified`: `false`（本Readerはローカル検証のみ）
- `realtime_synchronized`: `false`（リアルタイム同期の証明ではない）
- `scope`: `"local control + local SQLite package"`

### published-read

指定した表のデータをJSONLストリーム形式で標準出力へ出力する。

```bash
python3 archive.py published-read \
  --controls "<local-control-copy>" \
  --root "<verified-baseline-package>" \
  --delta-root "<local-delta-generations>" \
  [--generation "<expected-generation-id>"] \
  --table "<table-name>" \
  [--limit <positive-integer>] \
  [--mode "<mode-name>"] \
  [--rule "<rule-token>"]
```

ストリーム構造:

1. **Header（第1行）**:
   Readerロール、世代メタデータ、表のcolumns、`column_xinfo`、外部キー一覧、`table_full_row_count`、`ordinal_kind`（通常読取は `"current_table_stream_ordinal"`、selector時は `"selector_stream_ordinal"`）、`source_rowid_kind`（`"nullable_original_source_rowid"`）、`source_rowid_projection`（通常は `"original_source"`、selector時は `"unavailable_for_derived_rows"`）、`scope` を含む。
2. **Data Rows**:
   全表読取の1行は `{"ordinal": N, "source_rowid": {...}, "values": [...]}` 形式で出力される。selector読取の1行は `source_rowid` keyを省略し、元rowidを捏造しない。
   - SQLiteの5つの型を可逆で保持する。各cellは `sqlite_type`、`encoding`、`value` を持ち、INTEGERは10進文字列、REALは `float.hex()`、BLOBはhex、TEXTはUnicode文字列、NULLは `encoding: "none"` と `value: null` で表す。
   - 全表読取時は元表の `source_rowid`（SQLite内部rowid）を保持する。
   - mode selector読取（`matches` 表のみ指定可）では、headerの `source_rowid_projection` は `unavailable_for_derived_rows`。derived rowsに `source_rowid` keyは出力されない。元の全表と値はchainに保持され、unfiltered readで元rowidとともに読める。
3. **Footer（最終行）**:
   `{"type": "footer", "returned_rows": <count>, "truncated": <bool>, "limit": <limit-or-null>, "pinned_latest_sha256": "<sha>", "generation": "<id>", "latest_generation": "<id>"}`。
   `--limit` に達したあと、さらに行が存在する場合だけ `truncated: true` となる。実際の最終行がちょうどlimit件目なら `false`。

CLIはfooterを出す前に行iteratorを明示的にcloseし、selectorでは内部iteratorもcloseする。close時またはfinal guardでエラーが出た場合、安定したerror categoryで非0終了し、成功footerは出さない。

不正な引数（`--mode` なしの `--rule`、正整数でない `--limit`、`matches` 以外の表に対するselector指定、不正フォーマットの世代ID等）はpackageを開く前に拒否される。ファイルの破損、準備中（publisher index欠損）、期待世代不一致、パス上のシンボリックリンク等は非ゼロ終了コードと安定したエラーカテゴリ（例: `{"error": "..."}`）を標準エラーへ出力して終了し、raw exception、ファイルパス、データ内部値を漏洩しない。

## Publisher controlのローカルmirror

`scripts/mirror_published_controls.py` は、publisher control JSONだけをDriveから読戻してローカルcontrol directoryへ固定する。baseline indexとdelta index/planを含むcontrolをreadbackし、strict JSON・version・generation chain・directory bindingを検証してから配置する。raw SQLite、XLSX、その他のdata artifactは取得しない。

```bash
python3 scripts/mirror_published_controls.py \
  --remote "<rclone-remote>:<publisher-root>" \
  --control-dir "<local-control-copy>" \
  [--rclone-bin "<path-to-rclone>"]
```

`--rclone-bin` の既定値は `rclone`。成功時のreceiptは `controls_full_readback: true` を示すが、`all_remote_artifacts_verified: false` と `realtime_synchronized: false` のままである。これはcontrol一式のローカルmirrorであり、data packageが揃っていることや完全同期を示さない。

mirrorは排他 `.published-controls.lock` をnonblocking `flock` し、control directoryに対する同時実行を拒否する。新規作成するdirectoryはmode `0700`、control fileとlockは `0600` で作る。取得したimmutable index/planは、safe pathを通してstable file descriptorからraw bytesを再読戻し、取得時のbytesと一致することを全件確認してから `latest.json` を切り替える。既存のimmutable controlが同じpathで異なるbytesを持つ場合は拒否し、上書きしない。

既存の `latest.json` を更新するときは、その時点のraw bytesを `history/` へそのまま保全してから、private temporary fileをatomic replaceする。検証不一致やrenameより前の失敗なら旧latestを維持する。rename後のdirectory `fsync` が失敗した場合はCLIが非zeroを返すが、すでに置き換わったlatestをrollbackする保証はない。その場合、成功扱いせず、現在のlatestと保存されたhistoryを調べる。immutable controlsはlatest切替前に配置済みのまま残ることがある。

readerはmirror取得後にlatestを追従しない。開いたReaderは固定時点のcontrol bytes・chainを読み続け、次の世代は新しいReader contextで読む。mirror receiptのcontrol全件readbackはSQLite/XLSXなどdata artifactの全検証やremote同期完了を意味しない。

## 人工fixture

`tests/integration/nas_published_reader_fixture.py` はPython 3.11標準ライブラリと合成SQLiteだけを使い、baseline、reset、incremental deltaをin-memory fake remoteで検証するoffline fixtureである。live DBを開かずnetwork clientも起動しない。source・baseline package・delta packageの不変性、table/schema/typed rowの読取、rowid、latest pin、mirror receiptなどを確認し、`fixture_checks` とtable/row/generation countsをJSONで出力する。

```bash
python3 tests/integration/nas_published_reader_fixture.py \
  --code-root <code-root>
```

`<code-root>` はこのrepositoryのroot directoryを指す。成功結果は人工fixture内の検査だけを示し、実NAS/Driveの読取や公開、実データの完全性を証明しない。

## 検証範囲

Readerが実際に検査する対象は、与えたローカルcontrol files、ローカルbaseline package、ローカルdelta packagesである。publisher index内の全量readback記録はcontrolの内容として検査されるが、このReader自身が遠隔オブジェクトを読み直すことはない。したがって `published_control_binding_verified` や `published_deltas_verified` を、Drive上のraw SQLiteやXLSXの現在bytesをこのReaderが直接読戻し確認した証明として扱わない。実時点のlatestやリアルタイム同期状態の証明にもならない。

baseline-onlyの読取は全情報baselineをローカルで検証して読めることを示すが、deltaの公開検証にはならない。全情報XLSXの遠隔読戻し、raw統合SQLiteの遠隔読戻し、クラウド接続からの実読取、現在時点までの同期成功は、それぞれ別の検査結果で確認する。
