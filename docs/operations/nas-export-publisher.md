# NAS GUI publisher 運用手順

この手順は [`scripts/nas_publish_exports.py`](../../scripts/nas_publish_exports.py) と [`scripts/run_nas_export_publisher.sh`](../../scripts/run_nas_export_publisher.sh) の現行動作を説明する。

## 公開範囲

publisher が読む、生成する、配信するファイルは `exports/gui/index.html` だけである。旧 `exports/分析.xlsx` は開かず、生成も配信もしない。既存の `分析.xlsx` は履歴資産としてそのまま残し、この手順から置換・再生成しない。旧状態JSONに `分析.xlsx` のhashが含まれる場合は、publisher が状態JSONの元bytesを `state/export-publisher/history/` に保存してからGUI専用状態へ更新する。これは旧状態の記録であり、XLSX自体を保存・検証するものではない。

成功receiptの `scope` は `gui_only`、`full_database_synchronized` は常に `false` である。これはHTML 1ファイルの遠隔確認だけを示し、SQLite全量、全情報XLSX、または継続同期の成功を示さない。SQLiteと全情報XLSXを含む全量・差分同期は [full-data-sync worker](full-data-sync.md) を参照し、公開領域と証拠の読み方は [cloud-data-map.md](cloud-data-map.md) を参照する。

## 配置と権限

起動scriptは NAS 上に次の構成を想定する。`exports`、`runtime` は読み取り専用でマウントし、`state` と rclone 設定は書き込み可能にする。

```text
<ROOT>/
├── exports/
│   └── gui/index.html
├── runtime/export-publisher/nas_publish_exports.py
├── state/export-publisher/
└── secrets/export-publisher/rclone.conf
```

`state/export-publisher/` は publisher の状態、排他lock、競合退避物、旧状態JSON履歴を保持する。`rclone.conf` は mode `0600` が必須で、起動scriptが検査する。UID:GID `1000:10` から設定を読めるようにする。`backupkey` や `nxapi` の認証情報はここへ置かない。起動scriptは必須pathのsymlink ancestorと親ディレクトリ移動を拒否し、コンテナはUID:GID `1000:10`、read-only rootfs、`cap-drop ALL`、`no-new-privileges`、`/tmp` の32 MiB tmpfsで起動する。

## 単発実行

常駐containerを新設する前に、実環境の既存image・絶対パス・rclone remoteを指定して単発実行する。次は値を置き換える構文例である。

```sh
/path/to/scripts/run_nas_export_publisher.sh \
  --root /path/to/nas/archive-root \
  --image existing-backup-image:tag \
  --remote exports_remote:exports \
  --once
```

`--once` は `--rm` のforeground containerで一回だけ動く。daemon containerが稼働中なら拒否される。`--remote` は `NAME:PATH` の相対prefixで指定する。source `gui/index.html` と設定先へのアクセスに問題があれば成功扱いしない。

成功時は標準出力にJSON receiptが出る。例:

```json
{
  "status": "success",
  "scope": "gui_only",
  "full_database_synchronized": false,
  "files": {
    "gui/index.html": {
      "status": "uploaded",
      "sha256": "...",
      "bytes": 12345
    }
  },
  "counts": {"total": 1, "uploaded": 1, "skipped": 0, "conflicts": 0}
}
```

ファイルのstatusは `uploaded`、`skipped`、または競合退避後の `conflict_uploaded` になる。HTMLをremoteへ送った後、publisherはremoteから全bytesを読み戻しSHA-256を照合して状態を保存する。正常終了とreceiptの `status: success` を確認する。成功してもGUI専用の範囲を超えて解釈しない。

## 常駐実行と停止

単発実行の結果を確認してから、`--once` を外してdaemonを開始する。

```sh
/path/to/scripts/run_nas_export_publisher.sh \
  --root /path/to/nas/archive-root \
  --image existing-backup-image:tag \
  --remote exports_remote:exports
```

container名は `ikaring-archive-export-publisher`、再起動方針は `unless-stopped` である。各cycle終了後300秒待って次のGUI確認を行う。既存の同名containerがあれば起動scriptは停止・削除せず終了する。

```sh
docker logs -f ikaring-archive-export-publisher
docker stop ikaring-archive-export-publisher
```

停止時、転送途中のremote状態は次cycleで再検査される。NAS sourceは読み取り専用で開くため、この処理から変更・削除しない。状態directory、履歴、競合退避物は再起動時に残し、手動で消して初期化しない。

## 競合と制限

remote上の `gui/index.html` が想定外に変わっている場合は、現在のremote bytesを `state/export-publisher/conflicts/<UUID>/gui/index.html` と `<REMOTE_PREFIX>/.sync-conflicts/<UUID>/gui/index.html` へ保全し、remote退避先も全量読戻しで照合してから通常の公開を行う。確認と最終copyの間のremote同時編集を完全に防ぐcloud CASはない。

このpublisherはHTML一つだけを扱い、collectorやSQLite正本、WAL、スプールにはアクセスしない。以前の「HTMLと `分析.xlsx` を2ファイル配信する」手順は現行実装と一致しないため、適用しない。旧分析ブックの新規生成・読取・配信はこの手順では行わない。
