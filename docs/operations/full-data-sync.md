# NAS 全量データ差分同期 worker 運用テンプレート

この手順は、既存の NAS collector と既存の collector image を使い、稼働中 SQLite の全量差分を継続準備・公開する `full-data-sync` worker を追加するためのテンプレートである。ここでは Compose 定義と手順だけを提供する。実 NAS への配置・起動は別途行う。

worker は静止点 baseline を作らず、初回全量 reconciliation も生成しない。外部工程が作成した初回 generation を `--initial-generation-id` で採用した後、差分準備と公開を続ける。二つ目の worker は state directory 内のプロセス寿命 lock で拒否される。collector、既存 publisher、既存 image の停止・再起動・再構築はこの手順に含まれない。

## Compose 定義

[`full-data-sync.compose.yml`](../../deploy/nas/full-data-sync.compose.yml) は `full-data-sync` service だけを定義する。既存の collector image を `IKARING_FULL_SYNC_IMAGE` で指定し、Compose に build や pull をさせない。container 名は `ikaring-archive-full-data-sync`、実行ユーザーは既定で `1000:10`、再起動方針は `unless-stopped`、終了猶予は 300 秒である。外部ポートは公開しない。

すべての bind mount はホストの絶対パスと同じパスを container 側にも使う。`CODE_DIR`、`SNAPSHOT_DIR` は read-only、`DATABASE_DIR`、`DELTA_DIR`、`STATE_DIR`、`RCLONE_CONFIG_DIR` は read-write である。稼働 source の接続は `mode=ro` と `PRAGMA query_only=ON` により読み取り専用とする。`DATABASE_DIR` の read-write はSQLiteが `-wal` / `-shm` を必要時に生成できるようにするfilesystem要件で、source SQLへ書込みを許す設定ではない。両ファイルが存在しない時点では read-only mount からの WAL 読み取りが失敗する。[SQLite の WAL 読み取り条件](https://sqlite.org/wal.html#read_only_databases)を参照。稼働 source に `immutable=1` を指定して WAL やロックを回避しない。rclone 設定は OAuth refresh の書き戻しのため read-write でマウントする。

リポジトリ外のローカル環境ファイルには、実環境の絶対パスと generation ID を設定する。これは値の形を示す placeholder であり、そのままでは使わない。

```dotenv
IKARING_FULL_SYNC_IMAGE=existing-collector-image:existing-tag
IKARING_FULL_SYNC_UID=1000
IKARING_FULL_SYNC_GID=10
IKARING_FULL_SYNC_INTERVAL=30

IKARING_FULL_SYNC_CODE_DIR=/absolute/path/to/collector-code
IKARING_FULL_SYNC_DATABASE_DIR=/absolute/path/to/database
IKARING_FULL_SYNC_SNAPSHOT_DIR=/absolute/path/to/verified-snapshots
IKARING_FULL_SYNC_DELTA_DIR=/absolute/path/to/full-data-deltas
IKARING_FULL_SYNC_STATE_DIR=/absolute/path/to/full-data-sync-state
IKARING_FULL_SYNC_RCLONE_CONFIG_DIR=/absolute/path/to/full-data-sync-rclone

IKARING_FULL_SYNC_BASELINE_FILE=/absolute/path/to/verified-snapshots/baseline.sqlite3
IKARING_FULL_SYNC_BASELINE_MANIFEST=/absolute/path/to/verified-snapshots/baseline.manifest.json
IKARING_FULL_SYNC_BASELINE_GENERATION_ID=YYYYMMDDTHHMMSSZ-0123abcd
IKARING_FULL_SYNC_INITIAL_GENERATION_ID=YYYYMMDDTHHMMSSZ-abcdef12
```

`CODE_DIR/bin/rclone` は実行可能であること。Baseline database と manifest は `SNAPSHOT_DIR` 内の検証済み平文静止点を指し、source database は `DATABASE_DIR/archive.sqlite3` であること。baseline と source の symlink 不在、および静止点 baseline の非zero SQLite sidecar 不在を確認する。baseline manifestのSHA-256は静止点fileの全bytesを指す。WALを使う稼働sourceではmain database fileの物理bytes/hashがbaseline snapshotと異なることがあり、それを不整合と判定しない。稼働sourceのWALは正規の更新を含むため、その存在を異常扱いせず読み取る。workerは入力・delta work・stateのパス重複やsymlinkを拒否するため、これらは別々のディレクトリに置く。

`STATE_DIR` は container UID/GID が所有する mode `0700` の永続ディレクトリでなければならない。`RCLONE_CONFIG_DIR/rclone.conf` は同じ UID が読み書きできる mode `0600` とし、ディレクトリにも token refresh の書き込み権限を与える。認証情報を環境ファイルやこの文書に書かず、rclone 設定ファイルだけに保持する。

rclone 設定には `ikaring_exports` remote が登録済みで、配信先 `database` が意図した private destination であることを確認する。設定内容や token はログ・文書へ転記しない。

## 起動前確認

1. source collector と初回 generation producer の実行状態を確認する。初回 reconciliation は既に外部工程が開始または完了していること。`INITIAL_GENERATION_ID` は `DELTA_DIR/<generation-id>/` に対応する既存初回 generation の ID にし、別の初回 generator を起動しない。
2. 初回 generation の `delta-plan.json` と value/XLSX verification receipt が同じ generation・baseline に結び付き、準備済みなら verified であることを確認する。generation 作成中なら worker は plan が現れるまで採用待ちになる。worker 用 state が既にある場合は、baseline ID と state の binding を確認し、以前の成功・queue・failure 記録を残す。
3. `STATE_DIR` の所有者と mode `0700`、rclone config file の mode `0600`、必要な全ディレクトリの存在・絶対パス・symlink 不在を確認する。worker lock が存在する場合、それを削除せず稼働 worker の有無を調べる。
4. 同じ Compose 環境ファイルを使って、まず設定だけを検証する。これはコンテナを起動しない。

   ```sh
   docker compose --env-file /path/to/full-data-sync.env \
     -f deploy/nas/full-data-sync.compose.yml config --quiet
   ```

5. 既存 image と同じ UID/GID、read-only code mount で worker CLI の help が表示されることを確認する。これは引数・Python import の確認で、worker の常駐起動ではない。

   ```sh
   set -a
   . /path/to/full-data-sync.env
   set +a

   docker run --rm --pull=never \
     --user "${IKARING_FULL_SYNC_UID:-1000}:${IKARING_FULL_SYNC_GID:-10}" \
     --env PYTHONDONTWRITEBYTECODE=1 \
     --env PYTHONUNBUFFERED=1 \
     --env TZ=Asia/Tokyo \
     --mount "type=bind,src=${IKARING_FULL_SYNC_CODE_DIR},dst=${IKARING_FULL_SYNC_CODE_DIR},readonly" \
     --entrypoint python3 \
     "${IKARING_FULL_SYNC_IMAGE}" \
     -u "${IKARING_FULL_SYNC_CODE_DIR}/scripts/full_data_delta_worker.py" --help
   ```

   CLI の必須引数は `--db`、`--baseline`、`--baseline-manifest`、`--baseline-generation-id`、`--work-dir`、`--state-dir`、`--remote` で、初回外部 generation の adoption に `--initial-generation-id` を使う。Compose は `--watch --interval 30` を指定する。

6. 同名 container が既に存在せず、同じ `STATE_DIR` を使う worker が一つも稼働していないことを確認する。二重起動は行わない。

## 起動と状態の確認

すべての確認が終わった後、対象 service だけを起動する。

```sh
docker compose --env-file /path/to/full-data-sync.env \
  -f deploy/nas/full-data-sync.compose.yml \
  up -d --no-deps full-data-sync
```

この Compose 起動は初回 generation を作らない。既存の初回 generation を検証・採用し、その後の周期差分を準備・公開する。`pending_baseline_publication` は、full baseline の remote `latest.json` と基底 index の検証完了待ちを表し、同期失敗とは区別する。公開条件が満たされるまで prepared queue は残り、published checkpoint は進まない。

Docker container が running であることだけでは、同期完了を意味しない。worker は準備と公開を別 thread で行う。次の記録を合わせて確認する。

- `STATE_DIR/delta-worker-state.json`: prepare/publish の直近 status、failure category、retry 状態、lag
- `STATE_DIR/delta-prepared-checkpoint.json`: verified prepare 済み generation の queue と末尾
- `STATE_DIR/delta-published-checkpoint.json`: publisher の全 readback verification 後に進む公開済み位置
- `DELTA_DIR/<generation-id>/delta-plan.json` と各 verification receipt: 個別 generation の準備証拠

```sh
docker compose --env-file /path/to/full-data-sync.env \
  -f deploy/nas/full-data-sync.compose.yml logs --tail=100 full-data-sync
```

`prepared` と `published` の位置が異なる間は queue が残る。`retry_pending` や pending category は再試行待ちとして状態ファイル・ログで区別し、同期完了として扱わない。failure category が続く場合は原因を解消してから再起動する。state、generation、verification receipt を消して初期化し直さない。

## 停止・再開

```sh
docker compose --env-file /path/to/full-data-sync.env \
  -f deploy/nas/full-data-sync.compose.yml stop full-data-sync

docker compose --env-file /path/to/full-data-sync.env \
  -f deploy/nas/full-data-sync.compose.yml start full-data-sync
```

停止時は最大 300 秒の graceful shutdown を許す。delta work と state は bind mount 上に残り、再開時は既存 checkpoint から続く。履歴や state directory を削除・置換しない。collector、collector image、既存 publisher を再起動せず、この service だけを操作する。

## このテンプレートの検証範囲

引数は worker の実際の `--help` と照合済みである。Docker Compose が利用できる環境では `config --quiet` による構文確認を行ってから起動する。Compose の検証コマンドはサービスを開始しない。この作業では実 NAS への配置、Docker 起動、Drive 接続を行わない。


Google Driveへの配信では `IKARING_ARCHIVE_DRIVE_FOLDER_CACHE=1` を使う。プロセス内で元remoteから確認したフォルダーIDを再利用し、各ファイルを同じフォルダー内の相対名で扱う。全ファイルのバイト数・SHA-256読戻しは維持し、index、latest、成功checkpointの確定前に元remoteからフォルダーの対応を再検査する。対応が変われば公開を止める。キャッシュはディスクへ保存せず、Google Drive以外のbackendではこの設定を使わない。既定のRclone adapterでは無効である。

## publisherのファイル並列度と検証順

`IKARING_ARCHIVE_PUBLISH_FILE_WORKERS` はartifactファイルの並列数を指定する。未設定時は `1` で、既存の逐次順序を保つ。並列化を選ぶ場合は `1` から `4` の整数を指定する。範囲外や整数以外は設定エラーとして公開を止め、既定値へ黙って戻さない。このComposeは未設定なら `1` で動く。opt-inする場合はserviceの `environment` に例えば次を追加する。

```yaml
      IKARING_ARCHIVE_PUBLISH_FILE_WORKERS: "2"
```

並列数にかかわらず、各artifactはremoteのstat後、全bytesの読戻しとSHA-256照合を受ける。既にremoteに存在する一致ファイルも再読戻しする。artifact workerがすべて終了するまでgeneration index、`latest.json`、成功checkpointの処理には進まない。いずれかのファイルで失敗すれば新しい送信を止め、開始済みの送信だけを完了させてreceiptを保全し、未完了世代のindex・latest・checkpointを確定しない。

このComposeはDrive folder cacheも有効にしている。cacheは確認済みフォルダーIDでpath解決を行うためのもので、bytes/SHA-256の全量readback検証を省略しない。全artifactの終了後、generation indexやlatest等のcontrol objectを直列に公開・読戻しし、その前に元remote上のfolder bindingを再検査する。ID対応が変わった場合は公開を止め、cache更新で追随しない。

## 検証済み世代のローカル読取

`LosslessShardReader` は reader context を開くとき、baseline の manifest、verification receipts、全宣言済み SQLite shard/value/selector file をinventory・schema・proofと照合し、`verify_files()` で全対象のbytesとSHA-256を一度検証する。返るtokenは同一プロセス内だけで使える。各lookup、iteratorの結果境界・終了、reader context終了時には、宣言済みSQLite inventory、root/親directory/fileのfingerprintとpathを再照合するが、そこで全ファイルを再hashするわけではない。

`lookup_rows_with_identity(table, criteria)` は該当列を先に調べ、一致候補だけを全列へ展開して返す。戻り値は行ordinal、元source rowid（保存できない表では `None`）、全列のnative SQLite値である。候補判定と全行のexternal-cell metadataは検査するが、不一致行の通常列の大きなTEXT/BLOBはPythonへ取り込まず、candidate lookupでは全row-stream digestや全value-chunk inventoryを再計算しない。この読み方は保存済みの値や証明を省略・変更しない一方、全行の完全性を改めて証明する処理ではない。全行内容の再検証が必要な場合はlookup結果ではなくfull verificationの証明を使う。

差分をpublished modeで読む場合は、検証済みのbaseline context内で `DeltaChainReader(..., require_published_deltas=True)` を使う。effective chainに含まれる各deltaは、plan・file inventoryに結び付いた厳密な `index.json` と `full_readback`/file-count proofを備える必要があり、1件でも欠ければreaderを開けない。`published_deltas_verified` は開いている非空のeffective chainについてそのindex群が有効で、読取境界のfile fingerprintが保たれているとき真になり、effective chainが空なら偽である。iteratorとreader contextは保持中の読取資源を閉じ、境界検査を行う。

このAPIが検証するのはローカルbaselineとdelta package、および各delta indexに記録された公開証拠である。これはbaselineのremote index proof、global `latest.json` が指す世代、Google Drive上の現在の全量readback、またはsource DBへのリアルタイム追従を証明しない。公開全体の完了確認には、別途publisher checkpointとremoteのindex/latest/readback結果を確認する。
