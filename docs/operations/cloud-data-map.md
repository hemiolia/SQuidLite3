# SQuidLite3：クラウドデータの構成（旧方式）

この案内は、全情報データの構成、完全性の証明、公開状態を説明する。ファイルが存在すること、アップロード要求が完了したこと、全バイトを遠隔から読み戻して照合したことは別の状態である。世代を「公開済み」と扱うのは、世代内の全証拠と全ファイルの照合が完了した場合に限る。

以下のパスは公開データ領域 `database/` を基準にした相対パスである。世代IDは説明用の変数であり、個別世代のIDやハッシュを示していない。

## 公開領域の構成

| 相対パス | 内容と役割 |
| --- | --- |
| `unified/generations/<generation-id>/` | 正本から作った読み取り専用の静止点SQLiteと、その取得元マニフェスト。履歴保全用の構成要素である。 |
| `slices/generations/<generation-id>/` | 静止点の全SQLite表を型を保ったまま小分けにしたlossless baseline、表・列・schema・selectorのマニフェストと検証証拠。 |
| `xlsx-full/generations/<generation-id>/` | 同じ静止点から作った全情報XLSXの分割ファイル、index、manifest、検証証拠。 |
| `deltas/generations/<generation-id>/` | baseline以降の変更を保持する差分世代。`index.json` のfile inventoryに列挙した変更transport、proof、lossless XLSX、`delta-plan.json` を含む。 |
| `deltas/generations/<generation-id>/delta-plan.json` | generationの元plan。全byte/SHAをreadbackし、差分indexの `files` 最後のentryと `plan_sha256` に結び付ける。自身のhashは循環を避けるためplan内の `files` には含めない。 |
| `deltas/generations/<generation-id>/index.json` | その差分世代の全artifactと検証証拠を束ねるimmutable index。`files` はplanの宣言順を保ち、末尾にplanを含む。全件のreadback後に公開する。 |
| `generations/<generation-id>/index.json` | baselineの全構成要素、完全性証拠、その配置とファイルSHA-256を結び付ける世代index。すべてのデータ構成要素の照合後に公開する。 |
| `latest.json` | 最後に全検証を通過したfull state（baselineと適用済みdelta chain）への参照。baselineまたはdelta indexの全量読み戻し照合後に更新する。 |
| `gui/index.html` | 記録閲覧画面の配信用ファイル。GUIの公開状態を示すもので、SQLite・baseline・delta・全情報XLSXの公開証明ではない。 |

全情報readerはlossless baselineの断片と、順序どおりに検証した差分世代を読み、現在の全表を再構成する。raw統合SQLiteをダウンロードしたり開いたりする必要はない。`unified/` の静止点は別途保全されるが、readerの実行条件ではない。CLIの差分readerには、各世代の元 `delta-plan.json` と、その `plan.files` に列挙された全artifactが必要である。publisherはplanを遠隔artifactとして公開し、差分 `index.json` の末尾entryと `plan_sha256` で元bytesを束縛する。remote-onlyで読む場合は、`deltas/generations/<id>/` からindexが指す全artifactと `index.json` を取得し、各entryの `local` 相対パスを保って一つのgeneration directoryに置く。readerはその `index.json` がある場合、planとfile inventory・bytes・SHAを厳密に照合する。publisherのprepared directoryとremote-only directoryは同じplan・宣言artifactからreader入力を構成する。lossless shard transportでは、巨大な `changes.sqlite3` は準備時の独立検証用cacheでremote inventoryに含めず、readerはshard artifactから全変更値を復元する。unified `source.sqlite3` もreaderには不要である。

## Publisherごとの公開範囲

`scripts/nas_publish_exports.py` は `gui/index.html` だけを公開するGUI専用publisherである。その完了receiptはデータベース同期を明示的に証明しない。`scripts/stage_full_sqlite_component.py` は全値を含むSQLite shard componentとそのcomponent indexだけを公開し、XLSXやglobal generation index/latestを作らない。このcomponent単体が全値を持つことと、full baseline packageが揃ったことは別である。full baseline publisherは全artifactのreadback後に `generations/<id>/index.json`、次に `latest.json` を公開・検証してから成功checkpointを進める。delta publisherは差分artifactのreadback後に `deltas/generations/<id>/index.json`、次にchain全体を示す `latest.json` を公開・検証してからdelta success checkpointを進める。各scopeのreceiptを別scopeの成功に読み替えない。

## baselineの内容と完全性

`slices/` のbaselineは、表ごとのshared shardと、巨大なTEXT・BLOB値のvalue chunkで構成する。各SQLiteデータファイルは20 MiB以下に分割する。SQLiteの保存型と元の値を維持し、表の全行・全列・空表を含める。manifestには全schema object、table・column metadata（`table_xinfo`）、foreign key、row count、partの範囲とハッシュを記録し、観測できる元のsource rowidも保持する。値はsource snapshotを基準に検証する。既存の参照切れや矛盾を補修・削除・推測で埋めず、そのまま保持する。

データpartは `shared/<table-id>/partNNNNNN.sqlite3`、その表の外部値chunkは `shared/<table-id>/value/partNNNNNN.sqlite3` に置く。selectorは `by-mode/` と `by-rule/`、根拠ファイルは `manifest.json`、`verification.json`、`selectors-verification.json` である。ファイル数は表の量と値の大きさに応じて変わり、固定本数を完全性の条件にしない。

`by-mode/` と `by-rule/` は、共通の全表shardを置き換えるデータベースではなく、該当する試合を選ぶ入口である。mode側は15個の既知分類、観測された追加分類、未分類の入口を持つ。rule側は観測された各modeと観測された全ruleの直積を列挙し、該当試合が0件の組み合わせも含める。件数は観測データにより変わる。selectorのseed行と共有データへの到達性は別の検証証拠で閉包を確認するため、入口ファイルだけを取得して全情報とみなしてはならない。

全情報XLSXも同一snapshot IDとsource SHA-256に束ね、全表・全値の検証証拠を持つ。XLSXが一部だけ、古い世代、または検証未完了であれば全情報資料として扱わない。

## 差分世代

差分はbaselineの後に親世代順で適用する。保存形式はnative SQLite transportまたは同じ変更を保持するlossless SQLite shardsで、全source tableのschema・列・foreign key・row count等のmetadataと、変更行の全型付き値、delete、table replacement/resetの意味を保持する。差分ごとの `xlsx/` はその差分transportをlosslessに表したもので、現在の全source状態を置き換える全表XLSXではない。

change feedを導入する前の履歴の取りこぼしを閉じる最初の差分は、baselineと現在snapshotを全source行・全値で照合するfull reconciliationで作る。差分追跡が成立しない場合、schema変更（DDL）、またはwriter guardが未導入・未検証の場合は、古い差分を継ぎ足さずbaselineから新しいreconciliationを行い、そこから新しいchainを始める。置き換えられたchainや失敗した生成物は監査用に保持し、削除しない。

## 公開順序と状態の読み方

公開処理は、generation plan、source・SQLite selector・XLSXのproof、各artifactのbytesとSHA-256を先に検証する。その後、各ファイルを転送し、遠隔側から全量を読み戻してbytesとSHA-256を照合する。full baselineではglobal `generations/<id>/index.json`、deltaでは `deltas/generations/<id>/index.json` をそれぞれ全artifactのreadback後に作成・公開して読み戻す。次に以前のlatest全bytesを保存し、置換直前に再照合してから新しい `latest.json` を書き、全量照合する。これらがすべて成功した場合だけローカルの成功checkpointを進める。GUI専用publisherとSQLite component publisherのreceiptは、このglobal順序や完了を証明しない。

通信断、容量不足、遠隔ファイルの不一致、proof欠落、latestの期待親世代との不一致、またはlatestの同時更新があれば公開は失敗またはpendingのままにする。失敗時は直前の成功世代を成功状態として保持し、未完成世代をlatestにしない。以前のlatestが遠隔に残っていても、それを現在世代と誤認せず、取得時刻と確認時刻から遅延を明示する。legacy snapshotの取得時刻が不明なら `captured_at` は不明のまま保持し、検証完了時刻を取得時刻へ読み替えない。

差分workerのcycle/state JSONには `published_lag_seconds` と `queued_capture_lag_seconds` がある。前者は最後に全量公開照合を通過したcapture時刻から現在までの経過秒数、後者は最新prepared captureと最後のpublished captureの時刻差である。deltaの `latest.json` にも `capture_lag_seconds` が入る。未知または比較可能なcheckpointがない場合はworker値が `null` になり得る。published lagはcapture時刻からの経過量だけを表し、それ単独では現在の未同期量を証明しない。現在の同期状態は、新しいsource probeの `MAX(event_id)`・schema SHA・feed/writer guard状態・観測時刻を、published through-event・prepared queue/tail・最後の成功検証時刻と照合して判断する。実装はlagを計測して報告するが、これらの数値に対する共通の年齢上限をpublisherが自動拒否条件にしているわけではない。準備済み・queue済みは公開成功ではなく、workerが完了checkpointを検証するまで直前の公開成功位置を維持する。この値が出力される実装があることは、workerの実運用配置や実データの同期成功を意味しない。

## 現在確認できる状態

reader、baseline・差分の生成およびpublisherには人工データによる検証がある。これはNAS/Driveの実データ全体を継続同期できた証明ではない。現在、全情報baselineのXLSXとglobal generation indexを含む一連のNAS/Drive全量readback公開は完了しておらず、完全なリアルタイム同期も確認済みではない。旧部分sliceや旧分析ブック、GUIだけの公開、過去の別データ転送の成功を、現在の全情報世代の証拠に流用しない。定期worker・serviceの実運用配置も、この文書だけでは稼働確認済みとしない。

旧 `analysis_slice` と `exports/分析.xlsx`、`exports/analysis.xlsx` は情報欠落のある資料であり、分析や全情報確認に使わない。旧Macファイル同期ではこの2つの正確な相対パスだけを両側の対象から除外し、既存のファイルを変更・削除しない。拡張子や `exports/` 全体を除外する規則ではなく、ほかのパスは従来の同期除外規則に従う。全情報baseline・差分のartifactは専用workerで扱い、検証済みlossless generationと混同しない。
