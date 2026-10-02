# NAS移行およびバックアップ運用手順書

本手順書は、ikaring-archiveにおけるMacローカル環境からNAS環境へのデータコレクター移行、定期バックアップ、クラウド保全、軽量生成物配信、およびリストアに関する運用仕様と手順を定義する。

## 1. 基本方針とアーキテクチャ

### 1.1 NAS DBローカルディスクとコレクターの共置
- NAS上のデータコレクター（Dockerコンテナ）とSQLiteデータベースは、NASローカルのファイルシステム上に同一ホスト内で共置して運用する。
- ネットワークマウント（SMBやNFS等）越しにSQLiteデータベースへ直接アクセス・書き込みを行う運用は行わない。ネットワークファイルシステム越しのロック制御不安定による破損を防止するためである。

### 1.2 稼働中DBの直接同期・コピー禁止（稼働WALcopy禁止）
- 稼働中（書き込みプロセスが動作中）のSQLiteデータベースファイルを、SMB、Google Drive、rsync、双方向ファイル同期ツール等によって直接コピー・転送することは厳禁とする。
- 稼働中DBの転送はWALファイルとの不整合やページ破損、スプリットブレインの原因となる。バックアップや転送は、必ずSQLiteのオンラインバックアップAPIを用いて整合性のある静止点スナップショットを取得し、完全性検査（`PRAGMA integrity_check` / `PRAGMA quick_check` およびハッシュ照合）を行った上で実施する。
- WAL使用中の読み取りプロセスでは、SQLiteが `archive.sqlite3-wal` / `archive.sqlite3-shm` を開き、未作成の場合に生成することがある。そのためNASバックアップschedulerのbind mountは `database/` ディレクトリをread-writeにする。これはSQLite接続へSQL書込みを許す設定ではない。source接続はread-only URI (`mode=ro`) を使い、worker接続では `PRAGMA query_only=ON` も維持する。静止点出力先はsourceとは別のbackup directoryに置く。
- pinしたread transactionからオンラインバックアップAPIで作る静止点は、同時刻の稼働DB本体ファイルと物理バイト列・SHA-256が一致する必要はない。WAL内の確定更新を含む整合したsnapshotを作り、そのsnapshotの `raw_snapshot.bytes` / SHA-256 / `quick_check` を検証した後、Driveから同じsnapshotを全量読み戻して照合する。ライブDB本体の単独file hashをsnapshotやDriveのhashと比較しない。

### 1.3 軽量生成物の配信とファイル同期の境界
- **GUI専用publisher**:
  - [nas_publish_exports.py](../../scripts/nas_publish_exports.py) と起動シェル [run_nas_export_publisher.sh](../../scripts/run_nas_export_publisher.sh) は `exports/gui/index.html` だけを読み取り、認可済みprivate Google Driveへ配信する。旧 `exports/分析.xlsx` は読み取り・生成・更新・配信をせず、既存ファイルがあっても履歴資産として残す。詳細と起動方法は [nas-export-publisher.md](nas-export-publisher.md) を参照する。
  - 成功receiptは `scope: gui_only`、`full_database_synchronized: false` であり、GUI HTML一つの確認だけを示す。SQLite全量や全情報XLSXの同期完了を示さない。全情報SQLite/XLSXの全量・差分配信は別pipelineである [full-data-sync worker](full-data-sync.md) を参照し、世代と公開証拠は [cloud-data-map.md](cloud-data-map.md) で確認する。GUI receiptを全量・差分pipelineのreceiptやcheckpointと読み替えない。
  - **競合退避と全量読戻し検証**: `gui/index.html` が想定外に変更されていた場合は、リモート側の退避先（`.sync-conflicts/<UUID>/gui/index.html`）へ退避保全する。退避upload後、`rclone cat` による全量読戻しSHA-256一致を確認してから配信先を更新する。
  - **CASの制限**: 配信直前にリモートのstatとhashを確認するが厳密なCompare-And-Swap (CAS) ではなく、確認からcopy完了まで微小な競合窓がある。対象はHTML一つなので複数ファイル間のtransactionはなく、全量読戻し後にGUI専用状態とreceiptを確定する。
- **旧Macファイル同期スクリプトの拒否と分離**:
  - NASクライアントモードでは、旧Macファイル同期のインストーラー（[install_file_sync_service.py](../../scripts/install_file_sync_service.py)）、実行ラッパー（[run_file_sync.py](../../scripts/run_file_sync.py)）、および同期スクリプト（[sync_nas.py](../../scripts/sync_nas.py)、[sync_gdrive.py](../../scripts/sync_gdrive.py)）の実行は明示的に拒否される（`NAS_CLIENT_MODE_FILE_SYNC_DISABLED`）。
  - NAS移行後はMac側コレクターと旧file-sync（LaunchAgent `local.ikaring3.file-sync`）を停止・解除し、二重コレクターの動作や旧フォルダ再作成を防ぐ。NASクライアントモードではこれらを現行稼働として扱ってはならない。
  - 旧双方向同期スクリプトは、明示した別ローカルルート（`--local` 引数指定時）や未移行のローカル利用向けとして残されており、NASクライアント運転と混同してはならない。

### 1.4 クラウドへの平文スナップショット全数一致と、既存暗号世代の扱い
- 2026-09-29 に確定した現行規則: 新しいクラウドバックアップは平文の SQLite 静止点である。成功は、ローカル静止点とクラウド上のファイルのバイト数・MD5・読み戻し SHA-256 が全部一致したときだけである。明示的かつ合理的な理由の無い暗号化はしない。「Google Drive に置くから」「個人情報だから」は理由にならない。
- 2026-09-24 から 2026-09-28 に作られた `.zst.gpg` 世代は削除しない。以下のチャンク手順と復号手順は、その既存世代を読むためのものである。新しい日次バックアップの成功条件には使わない。
- **Google Drive connectorの二段上限と64MiBチャンク分割**:
  - Google Drive connectorにおける転送開始前上限（512MiB）および後段HTTP 413上限（100MiB）の二段上限を確実に回避するため、暗号文を既定64MiB part（[backup_chunks.py](../../scripts/backup_chunks.py)、最大256MiB対応）に分割して転送するchunk方式を採用する。
  - 暗号文cipher本体をpart分割（`"<cipher>.part-XXXXXX"`）、元暗号マニフェストの複製、およびチャンクマニフェスト（`"<cipher>.chunks.json"`）で構成し、クラウド上およびローカルのflat folderで同一の元uuid付きbasenameを共有して管理する。
- **全partアップロード後のストリーム全量往復検証**:
  - 全partのアップロード完了後、確定前に [verify_cloud_chunks.py](../../scripts/verify_cloud_chunks.py) を用いて、クラウドから各partを `rclone cat` でストリーム読取し、GPG復号＋zstd伸長を行いながら、暗号文SHA256ハッシュおよび復号後rawサイズ・raw SHA256ハッシュが元マニフェストと完全一致することを検証する。
  - `verify_cloud_chunks.py` はストリーム復号によるハッシュ照合を実施するが、新規のSQLite `quick_check` は実行しない（元静止点スナップショット作成時の `quick_check=ok` と、復号rawバイト列およびSHA256ハッシュの完全一致が健全性の証拠となるためである）。
- **チャンク形式における2マニフェストのアップロードと確定**:
  - チャンク形式では、全partのストリーム往復検証が成功した後にのみ、2つのマニフェスト（元暗号マニフェストとチャンクマニフェスト）をアップロードし、クラウドからの読み戻しハッシュ一致を記録する（単一暗号文直接経路では元暗号マニフェスト1本をアップロードする）。
  - `gdrive_upload_verified.sh` 単体は結果を標準出力のJSONで返し、レシートファイルは作らない。[nas_backup_cycle.py](../../scripts/nas_backup_cycle.py) 経由のサイクルだけが、成功後に `cloud-receipts/` へレシートをアトミックに記録する。
- **バックアップ経路2種の峻別**:
  - 2026-09-29 以降の定期 [nas_backup_cycle.py](../../scripts/nas_backup_cycle.py) は、平文の SQLite 静止点を rclone で直接アップロードする。成功はローカル静止点とクラウド上のバイト数、MD5、読み戻し SHA-256 の一致である。暗号化しない。
  - 2026-09-28 までの定期サイクルは、単一暗号文（単一cipher）の直接アップロードだった。その世代の検証は暗号文の読み戻しであり、クラウドからの raw 復号は別検証だった。新しい日次には使わない。
  - Google Drive connectorの二段上限（512MiB/100MiB）を回避するためのchunk分割経路は、[backup_chunks.py](../../scripts/backup_chunks.py) および [verify_cloud_chunks.py](../../scripts/verify_cloud_chunks.py) を用いる手動chunk経路である。
  - これら2つは独立した別経路であり、同一処理であると混同してはならない。

### 1.5 安全原則: クラウド未検証時のローカル削除不可と旧版保持
- **削除対象各々の全量往復検証必須**: 2026-09-29 以降に新しく作るバックアップの成功は、平文静止点とクラウド上のファイルのバイト数および SHA-256 が一致したときだけである。この節は削除を許可しない。稼働中のデータベース、NAS 上の平文、既存の `.zst.gpg` は削除しない。過去に行った暗号化バックアップの往復検証（チャンク形式における全partからのストリーム復号・rawバイト数・raw SHA-256完全一致、両マニフェストの読み戻し一致、または単一暗号文経路における暗号文読み戻し検証とレシート生成）は、既存の暗号世代を読む手順として残す。それを経ていないからといって、ローカルの正本DBや既存のローカルバックアップファイルを削除してはならない。[nas_backup_cycle.py](../../scripts/nas_backup_cycle.py) を使う場合はレシート生成も確認する。また、ローカルDB削除前のraw全量復号確認は定期サイクルの自動処理と混同せず別検証として確実に実施する。
- **削除前の厳格な事前安全検査**:
  - Mac側コレクターの停止（無効化/未登録）および二重コレクター不在の確認
  - NASマーカーの存在確認
  - ローカルhash検証時からのinode / mtime / size不変の確認
  - WALファイルが存在しない、またはサイズ0であることの確認
  - ライターロック下での排他確認
- **旧版の永続保持**: NASおよびクラウド上に存在する過去世代の暗号化スナップショット・マニフェストは、破棄せず安全に保持する。

### 1.6 移行マーカー、クライアントルート、およびスプリットブレイン防止
- **Mac NASクライアントルートと配置**:
  - Mac側におけるNASクライアントのルートディレクトリは `~/Library/Application Support/ikaring-archive/client` とする。
  - 移行マーカーは `~/Library/Application Support/ikaring-archive/client/config/storage-location.json` に配置・保持する（設定ファイルのみでDB本体は含まない）。
  - エクスポート成果物をMacローカルで取得・キャッシュする先は `~/Library/Caches/ikaring-archive/exports` 配下とする。
  - NAS移行後のクライアント環境では、旧Documentsルート（`~/Documents/イカリング3アーカイブ`）のディレクトリを再作成しない（未移行のローカル利用環境では従来のDocumentsディレクトリ利用が有効である）。
  - 環境変数 `IKARING_ARCHIVE_DATA_DIR` が設定されている場合は、そのパスを最優先として使用する。
  - 未移行のローカル配布モード（マーカーが存在しない場合等）は、従来のDocumentsディレクトリ（`~/Documents/イカリング3アーカイブ`）を既定とする。
- **スプリットブレイン防止と自動フォールバック禁止**:
  - マーカーが存在する場合、Macローカルの [archive.py](../../archive.py) はローカル既定DBへのアクセスを遮断（`NAS_STORAGE_ACTIVE` 例外を発生）し、操作は [nas_archive.py](../../scripts/nas_archive.py) を経由してNAS側コンテナで実行することが強制される。
  - NASとの接続切断や障害が発生した場合に、自動的にMacローカルDBへ書き込み先をフォールバックさせる処理は行わない。自動フォールバックを許容すると、NASとMacの双方に異なる新データが書き込まれる「スプリットブレイン」が生じ、データの整合性回復が困難になるためである。障害時は取得を停止し、手動で状態を確認して復旧する。
- **Start.command による安全なルーティング**:
  - Macの起動スクリプト `Start.command`（[start.py](../../scripts/start.py)）は、起動時に上記 Application Support 配下のNASマーカーを検証し、正常なNASマーカーを検出した場合は自動的に [nas_archive.py](../../scripts/nas_archive.py) `gui` へ処理をルーティングする。
  - これにより、Mac側でのNode.js/npm依存関係やニンテンドーアカウントへのログイン処理を一切不要とし、NAS側で生成された最新GUIを安全に表示する。
  - マーカー不正時は即座に処理を拒否し、ローカルへの自動フォールバックは行わない。Documentsフォルダの自動再作成も防止される。

### 1.7 復元手順の安全性
- バックアップからの復元（リストア）は、既存の本番稼働中DBへ直接上書きしてはならない。
- ダウンロード、チャンク結合、復号・伸長、ハッシュ検証、`PRAGMA quick_check` などの復元検証作業中からコレクターを止める必要はない。独立した安全な作業ディレクトリで検証を完了させた後、最後の正本切替直前に対象コレクターを停止して排他状態を確保する。
- 切替時は旧DB本体だけでなく関連ファイル（`-wal`, `-shm`, `-journal`）を一組でユニーク退避し、同一ファイルシステム上で検証済みDBをステージング・ハッシュ再照合した上で `os.replace` によりアトミックに差し替える。安易な `mv` 2本による置換はWAL混在や非アトミック破損を招くため厳禁とする。

### 1.8 暗号鍵の分離保全
- バックアップの暗号化および復号に使用する暗号鍵（パスフレーズ）は、単一障害点や環境侵害時の漏洩を防ぐため、NASローカルの制限領域（パーミッション0600）とMacのKeychain（`ikaring-archive-gdrive-backup`）等にそれぞれ独立して別々に保全する。

### 1.9 ネットワークとポート設計
- NAS上で動作するDockerコンテナは、外部公開ポートを必要としない（`-p` によるポート開放は不要）。
- 外部やMacからの操作は、セキュアなSSH経由および `docker exec` による内部コマンド実行によって完結する。

### 1.10 ソース公開範囲の境界、取込原文保持、秘密/実データGit禁止
- **取込原文byte保持**:
  - 任天堂サーバーからのAPI応答は、転送圧縮展開後のHTTP entity bodyをバイト列として重複排除BLOB（`bodies` テーブル）に完全保持する。
  - SQLiteの数値変換やJSONパースによる情報落ちを防ぎ、将来のスキーマ拡張や未知フィールドの再解析を可能にする。
- **秘密情報・実データのGitコミット禁止**:
  - GitHub等のリポジトリへの公開は、コレクターおよび管理ツールのソースコードのみを対象とする。
  - 実戦績データ、SQLiteデータベース、スプール、トークンや秘密情報（secrets/auth）、ローカルログ、個人を特定するデータは一切リポジトリへ含めず、Gitコミットを厳禁とする。
- **GitHub暗号化DB保管の役割方針**:
  - GitHubリポジトリへの暗号化DB自体の保管役割については今後の方針が未決定である。そのため、公開リポジトリはソースコードと復元手順のみを保持し、暗号化DBファイル等は含めない。

---

## 2. 実装スクリプトの仕様と役割

| スクリプト / サービス | 実行場所 | 主な役割と安全機構 |
| :--- | :--- | :--- |
| [deploy_nas_container.sh](../../scripts/deploy_nas_container.sh) | Macから実行 | コレクターのソースコードのみを抽出し、tarストリームでSSH経由でNASへ転送。NAS側で排他ロック（flock）を取得しDockerイメージをビルド。旧版ソースを `runtime/releases/` に退避しアトミックに差し替え。 |
| [nas_create_verified_backup.sh](../../scripts/nas_create_verified_backup.sh) | NAS内（既存暗号世代用） | 既存 `.zst.gpg` 世代に関係する旧暗号化入口。現在の日次schedulerが使う平文snapshot経路とは分離して扱い、既存暗号化成果物を削除・再生成しない。 |
| [verified_backup_support.py](../../scripts/verified_backup_support.py) | NAS内（日次backup helper） | `mode=ro` のsourceからread transactionをpinし、SQLiteオンラインbackup APIで平文静止点を作る。容量確認、snapshot `quick_check`、raw bytes/SHA-256付きmanifestを検証・生成する。ライブsource fileの物理hash一致は要求しない。 |
| [run_nas_daily_backup.sh](../../scripts/run_nas_daily_backup.sh) | NASホスト内（手動補助） | 日次バックアップ実行用シェル。非ブロッキング排他ロック（`logs/backup/.daily.lock`）、日次ログ生成、バックアップコンテナ（`ikaring-archive-backup:current`）実行およびステータス管理。 |
| Docker schedulerコンテナ | NAS内（常駐サービス） | NASホストcron権限制限を回避し、日次バックアップを毎朝4:00 Asia/Tokyoに自動起動（UID 1000:10、`restart: unless-stopped`、失敗時15分retry、同日成功後重複なし）。Docker socketをマウントせず安全に実行。 |
| [gdrive_upload_verified.sh](../../scripts/gdrive_upload_verified.sh) | Mac/NAS（旧経路） | 既存の暗号化成果物を扱う旧アップロード補助。現在の日次平文backup cycleとは別経路であり、新しいsnapshotのuploadに使わない。 |
| [nas_archive.py](../../scripts/nas_archive.py) | Macから実行 | `~/Library/Application Support/ikaring-archive/client/config/storage-location.json` を検証し、SSH経由でNASコンテナ内の `archive.py` コマンドを実行。GUI HTMLだけを安全にローカルキャッシュ（`~/Library/Caches/ikaring-archive/exports/`）へ取得し、`records` など読み取りコマンドを転送実行。旧 `export-xlsx` は `LEGACY_ANALYSIS_XLSX_RETIRED` で拒否される。全情報XLSXは [full-data-sync worker](full-data-sync.md) の別pipelineで扱う。未移行時やマーカー不正時は即時エラー。 |
| [start.py](../../scripts/start.py) / `Start.command` | Macから実行 | 起動時に `~/Library/Application Support/ikaring-archive/client/config/storage-location.json` のNAS markerを検証し、正常時は `scripts/nas_archive.py gui` へルーティング。MacローカルでのNode/npm/loginを不要化。マーカー不正時は拒否しローカルフォールバック禁止。移行後クライアントにおけるDocumentsフォルダ自動再作成防止。 |
| [nas_backup_cycle.py](../../scripts/nas_backup_cycle.py) | NAS内（定期実行） | 排他ロック取得、平文SQLite snapshot作成、snapshot manifest照合、rclone upload、remote size/MD5/全量SHA-256 readback、成功receipt（`cloud-receipts/`）生成を調整する。readback対象は固定snapshotであり、同時更新されるlive database fileの物理hashではない。 |
| [backup_chunks.py](../../scripts/backup_chunks.py) | Mac/NAS | 暗号化暗号文を既定64MiB（最大256MiB）のチャンクに分割（`split`）およびアトミック再結合（`join`）。暗号文全体SHA256、各partのSHA256、元暗号マニフェストの参照整合性を二重検証。既存出力先の上書き拒否。 |
| [verify_cloud_chunks.py](../../scripts/verify_cloud_chunks.py) | Mac/NAS | クラウド上のチャンク分割バックアップを `rclone cat` でストリーム読取し、暗号文結合・GPG復号・zstd伸長をパイプライン処理して暗号文SHA256および展開後rawバイト数・SHA256を元マニフェストと照合。ローカルに暗号文や平文ファイルを作らず完全性検証を実施（新規quick_checkは行わず元snapshotの証拠と照合）。 |
| [nas_publish_exports.py](../../scripts/nas_publish_exports.py) / [run_nas_export_publisher.sh](../../scripts/run_nas_export_publisher.sh) | NASホスト / Dockerコンテナ | `gui/index.html` 一つだけを認可済みGoogle Driveへ一方通行配信する。receiptは `scope: gui_only`、`full_database_synchronized: false`。旧 `分析.xlsx` を読取・生成・配信しない。SQLite全量と全情報XLSXは別の [full-data-sync worker](full-data-sync.md) が扱う。競合時はGUI HTMLを退避し、`rclone cat` による全量SHA-256読戻し後に配信先を更新する。DB正本、WAL、スプール、認証情報は除外し、最小権限コンテナ（非root `1000:10`、read-only rootfs、cap-drop ALL、tmpfs）で実行する。詳細は [nas-export-publisher.md](nas-export-publisher.md) 参照。 |
| [archive.py](../../archive.py) `records` / [nas_archive.py](../../scripts/nas_archive.py) `records` | NASコンテナ内（Macから転送実行可） | 新共通読取層 `RecordReader` を用いて、対戦およびバイトの記録を一覧取得（`records list`）および単一詳細取得（`records get`）するCLI。JSON形式で出力。本文BLOBが存在する場合はbase64エンコードして出力。正本DBを安全に読み取る。統合GUI全完成ではなく、CLI機能としての提供。デスクトップ読取画面は進行中であり完成扱いしない。 |
| [data_root.py](../../scripts/data_root.py) | Mac/NAS | ローカルデータルートをディレクトリ自動作成なしで解決。環境変数 `IKARING_ARCHIVE_DATA_DIR` を最優先、macOSかつNASクライアントマーカー存在時は `~/Library/Application Support/ikaring-archive/client`、それ以外（未移行local配布mode）は従来の `~/Documents/イカリング3アーカイブ` を既定とする。 |
| [sync_nas.py](../../scripts/sync_nas.py) / [sync_gdrive.py](../../scripts/sync_gdrive.py) / [run_file_sync.py](../../scripts/run_file_sync.py) | Macから実行 | 従来の軽量ファイル双方向同期スクリプト。NASクライアントモード（マーカー存在時）では明示的に実行が拒否される（`NAS_CLIENT_MODE_FILE_SYNC_DISABLED`）。明示した別ローカルルート（`--local` 指定時）や未移行環境のローカル利用向けとして保持され、NASクライアント運転とは混同しない。旧部分ブック `exports/分析.xlsx` と `exports/analysis.xlsx` はこの2つの正確な相対パスに限って両側の一覧・同期計画から除外し、既存ファイルを変更・削除しない。ほかのXLSXや全情報世代のmetadata/artifactは従来のpath規則に従い、baseline・差分の公開には別の [full-data-sync worker](full-data-sync.md) を使う。 |
| [install_file_sync_service.py](../../scripts/install_file_sync_service.py) | Macから実行 | 旧Mac軽量ファイル同期LaunchAgentのインストーラー。NASクライアントモードでは明示的にインストールを拒否する（`NAS_CLIENT_MODE_FILE_SYNC_DISABLED`）。NAS移行後の環境ではLaunchAgentは登録せず、NAS直送publisherを使用する。 |

---

## 3. 検証と完了判定

運用・移行・保守の各段階において、単にサービス（DockerコンテナやLaunchAgent）が登録されていることや、プロセスが存在することだけで「稼働成功」とみなしてはならない。以下の各実測基準を厳密に検査し、すべてが確認された状態を健全・完了と判定する。

### 3.1 稼働状態の検証基準（合格判定条件）
1. **認証の正常性**:
   - 認証トークン（セッショントークン等）が正しく読み込まれ、失効（`AUTH_REQUIRED`, `AUTH_EXPIRED`, `SESSION_EXPIRED` 等）が発生していないこと。
   - `archive.py status` において `auth.reauth_required` が `false` であること。加えて、未認証状態（未ログイン）の通過を防ぐため、`auth.last_ok_at` に有効な日時が記録され、直近の認証成功実績が存在すること。
2. **各7履歴経路の取得**:
   - `src/python/ikarchive/collector.py` の `HISTORIES` に定義された7つの履歴操作（`LatestBattleHistoriesQuery`, `RegularBattleHistoriesQuery`, `BankaraBattleHistoriesQuery`, `XBattleHistoriesQuery`, `EventBattleHistoriesQuery`, `PrivateBattleHistoriesQuery`, `CoopHistoryQuery`）のすべてにおいて、API取得が正常に試行・成功していること（単に7件の戦績が存在することを意味しない）。
   - 詳細取得キュー（jobs）において、保留（pending）が正常に消化されていること。取得不能な詳細（unavailable）がある場合は理由が記録されていること。
3. **GUIと全量データ成果物の検証範囲**:
   - GUI publisherの対象は `exports/gui/index.html` 一つである。HTML出力とremote全量読戻しを確認し、receiptの `scope: gui_only` と `full_database_synchronized: false` を維持する。旧 `exports/分析.xlsx` は履歴資産であり、新規生成・読取・配信の合格条件にしない。
   - 全情報XLSXとSQLite全量・差分の同期は [full-data-sync worker](full-data-sync.md) の別pipelineで検証する。GUI receiptだけで全量同期を完了扱いせず、同pipelineの世代・proof・全artifact readback・published checkpointを [cloud-data-map.md](cloud-data-map.md) と照合する。
4. **クラウド全量読み戻し・照合**:
   - **平文SQLite snapshot直接保全経路（日次バックアップ）**:
     - `nas_backup_cycle.py` は、sourceのread transactionからオンラインbackup APIで固定した平文snapshotを作成する。snapshotのローカルmanifest照合と `quick_check` の後、remote objectのbytes・MD5を確認し、`rclone cat` の全量読み戻しSHA-256をsnapshotのSHA-256と照合する。
     - この比較対象はローカルの固定snapshotとDrive上の同じsnapshotである。WALへの同時commitによりlive database本体fileのhashが異なるのは正常であり、live fileとの物理bytes/SHA一致を成功条件にしない。サイクルが証明するのは、作成した固定snapshotの全量転送・読戻し一致であり、Drive上で新たにSQLite `quick_check` やraw restore testを行った証拠ではない。
   - **既存暗号世代の検証**:
     - 2026-09-24から2026-09-28の `.zst.gpg` 世代では、暗号文readbackまたはchunkからの復号・伸長readbackを行う。これは既存世代を読むための履歴手順で、現在の日次平文サイクルの成功証拠に混ぜない。
   - **手動チャンク分割保全経路（chunk形式）**:
     - クラウドストレージ上の全partからストリーム読取・復号を実施し、得られたrawバイト列およびSHA-256ハッシュが元静止点スナップショットのマニフェストと完全一致（full readback一致）すること。
     - 2つのマニフェスト（元暗号マニフェストおよびチャンクマニフェスト `*.chunks.json`）についても、クラウドからの読み戻しハッシュが一致していること（2マニフェスト検証はchunk形式限定）。
5. **定期自然サイクルの自律完走**:
   - NAS日次バックアップとGUI-only publisherが、それぞれの設定周期（毎朝4:00、またはGUI確認の300秒間隔等）で独立して起動すること。全量・差分workerも別pipelineとして独立に検証し、prepared queueではなくpublished checkpointを確認する。各receipt/checkpointはその処理範囲だけを示すため、GUIまたは日次backupの成功を全量・差分同期の成功としない。
   - 同日成功後の重複実行防止や、エラー時の適切な再試行（指数バックオフや15分retry）が設計通りに機能していること。
   - プロセス終了コードが0であり、出力ログに致命的エラーカテゴリが出力されていないこと。

### 3.2 共通読取層とGUIの現在の進捗境界
- **RecordReader / records CLI**:
  - 新共通読取層 `RecordReader` および `archive.py records`（`list` / `get` サブコマンド）は実装済みであり、正本DBから対戦・バイトの記録を安全に読み取ることができる。
  - **統合GUI全完成ではない**: 本CLIの実装は内部読取APIおよびCLIとしての整備であり、統合GUIが完成したことを意味しない。デスクトップ読取画面は進行中であり、本書において完成扱いとしない。

### 3.3 未完了項目と現在の制約事項（進行中・未完了タスク）
- **GitHub暗号化DB保管の役割方針**:
  - GitHubリポジトリに暗号化DB自体の保管役割を持たせるか、あるいは公開ソースコードと復元手順のみに留めるかについては今後の方針が未決定である。公開リポジトリには実データや暗号化DBを含めてはならない。
- **統合GUIおよびデスクトップ/スマートフォン連携**:
  - 統合GUI（一覧・詳細、検索・絞り込み、タグ付け、分析、書き出し、バックアップを統合する全機能GUI）は着手中・未完了である（デスクトップ読取画面やRecordReader CLI等は進行中であるが、全機能統合GUIとしては完成していない）。
  - 全取得網羅性の検証および実画面照合、スマートフォンアプリの接続・同期方式設計、初心者向け導入設計、GitHub Actionsのworkflow scope設定は未完了の課題として保持する。

> [!WARNING]
> **重要な安全注意事項**:
> - NAS移行後はMac側コレクターおよび旧file-sync（LaunchAgent）を停止・解除し、二重コレクターと旧フォルダ再作成を防ぐこと。
> - NAS移行後のクライアント環境では旧Documentsフォルダの再作成を禁止し、マーカーは `~/Library/Application Support/ikaring-archive/client/config/storage-location.json` を使用すること（未移行ローカル環境を除く）。
> - 共通読取層 RecordReader は実装済みであるが、統合GUI全完成ではない。デスクトップ読取画面は進行中である。
> - GitHubへの暗号化DB保管の役割は今後の方針が未決定であり、公開リポジトリへ実データや暗号化DBを含めてはならない。
> - 稼働中DBのネットワーク直接コピー禁止、暗号鍵の分離保全、およびアトミック置換手順等の安全原則を厳守する。

---

## 4. 運用手順

### 4.1 【参考手順】Mac正本から初回移行・再同期時の手順（初回切替用）

> [!NOTE]
> **移行手順の位置付け**: 本節の手順は、Mac正本環境からNAS環境へ初回移行を行う場合や、緊急切戻し後に再度NASへ正本切替・同期を行う場合に用いる参考手順である。
> NAS移行後はMac側コレクターおよび旧file-syncを停止・解除し、二重コレクター稼働と旧フォルダ再作成を防ぐ。Macローカル原本DBを削除する場合は、NAS側の保全およびクラウド全量復号照合の確認など安全原則を満たした後にのみ行う。

1. **Mac側コレクターの停止**:
   二重取得および転送中のDB更新を防ぐため、MacのLaunchAgentを停止する。
   ```bash
   launchctl bootout gui/$(id -u) ~/Library/LaunchAgents/local.ikaring3.archive.plist
   ```
2. **MacローカルDBの静止点検証**:
   書込みプロセスをすべて停止し、その排他状態を転送完了まで維持する。WALを `TRUNCATE` checkpoint して `busy=0` とWALが空であることを確認する。この条件を満たす場合だけDB本体単独のコピーを許可する。満たさなければDB本体だけをコピーせず、原因を解消するかSQLiteオンラインバックアップAPIで静止点を作る。
   ```bash
   DB="/path/to/local/database/archive.sqlite3"
   python3 - "$DB" <<'PY'
   import os, sqlite3, sys
   db = sys.argv[1]
   con = sqlite3.connect(db)
   try:
       busy, log, checkpointed = con.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchone()
       assert busy == 0 and log == 0, (busy, log, checkpointed)
       assert con.execute('PRAGMA quick_check').fetchall() == [('ok',)]
   finally:
       con.close()
   assert not os.path.exists(db + '-wal') or os.path.getsize(db + '-wal') == 0
   PY
   ```
3. **NASへの最新DB静止点転送**:
   停止状態の最新DBをNASの作業ディレクトリへ安全にストリーム転送し、NAS側でSHA256ハッシュ検証および `PRAGMA quick_check` を実施する。
4. **NAS側正規配置への切替**:
   NAS側の既存DBを退避した上で、検証済み最新DBを正規パス（`/path/to/nas/database/archive.sqlite3`）として配置する。

### 4.2 NASコンテナの起動と検証手順
NAS環境でのコンテナ起動は、外部ポートを開放せず、ボリュームマウントとユーザー権限を厳密に設定して行う。

1. **コンテナ起動パラメーター**:
   - 実行ユーザー: `UID=1000:GID=10`
   - 環境変数:
     - `IKARING_ARCHIVE_DATA_DIR=/data`
     - `NXAPI_DATA_PATH=/auth`
   - ボリュームマウント（RW）:
     - NASローカルの `database` -> `/data/database`
     - NASローカルの `spool` -> `/data/spool`
     - NASローカルの `exports` -> `/data/exports`
     - NASローカルの `logs` -> `/data/logs`
     - NASローカルの `secrets/nxapi-nodejs` -> `/auth`
   - 暗号鍵ファイルやMac固有ランタイムはコンテナ内へマウントしない。
   - 再起動ポリシー: `--restart unless-stopped`
   - 外部ポート公開: 不要（`-p` 指定なし）
2. **起動後の実測動作確認**:
   プロセスが存在することだけで成功とみなさず、コンテナ内で以下を実測する。
   - 認証トークンの読み込み確認
   - 7履歴経路の取得テスト実行（7件の戦績を意味しない）
   - エクスポートファイルおよびDB更新の確認
3. **日次バックアップDocker schedulerコンテナの起動パラメーター**:
   - ホストcronの権限制限を回避するため、Dockerコンテナとしてスケジューラーを常駐実行する。
   - 実行ユーザー: `UID=1000:GID=10`
   - 再起動ポリシー: `--restart unless-stopped`
   - スケジュール設定: 毎朝4:00（Asia/Tokyo）
   - 再試行および重複防止: 失敗時は15分間隔で再試行、同日成功後の重複実行なし（`last_success_due_day` による整合性管理）。
   - 実行処理: 常駐backupコンテナ内の `scripts/nas_backup_scheduler.py` が `scripts/nas_backup_cycle.py` を直接実行する。Docker socketをマウントせず、コンテナ内から別コンテナを起動しない。[run_nas_daily_backup.sh](../../scripts/run_nas_daily_backup.sh) はNASホストから単発実行する補助手段である。

### 4.3 移行マーカーの配置とクライアント運用切替
NAS側のコンテナ収集が安定して稼働したことを実測した後、移行マーカーを作成する。

1. **マーカーファイルの作成・保持** (`~/Library/Application Support/ikaring-archive/client/config/storage-location.json`):
   ```json
   {
     "schema_version": 1,
     "backend": "nas",
     "ssh_host": "nas",
     "container": "ikaring-archive",
     "database": "/data/database/archive.sqlite3"
   }
   ```
   ※NAS移行後のクライアント環境では、旧Documentsフォルダではなく上記 `Application Support` 配下にマーカーを配置・保持する（設定ファイルのみでDB本体は含まない）。
2. **Macクライアント操作の切り替えとStart.commandのルーティング**:
   - `Start.command`（[start.py](../../scripts/start.py)）を実行すると、Node.js/npmの検査や依存関係インストール、ログイン処理を行う前に `~/Library/Application Support/ikaring-archive/client/config/storage-location.json`（または環境変数 `IKARING_ARCHIVE_DATA_DIR` 配下）を検証する。
   - 正常なNASマーカーが確認された場合は、自動的に [nas_archive.py](../../scripts/nas_archive.py) `gui` が呼び出され、MacローカルでのNode/npm/loginを一切不要としてNAS側で生成された最新GUI HTMLを取得してブラウザで開く。マーカー不正時は即座に処理を拒否し、ローカルへの自動フォールバックは行わない。Documentsフォルダの自動再作成も防止される。
   - CLI操作を行う場合は [nas_archive.py](../../scripts/nas_archive.py) を使用する。
     ```bash
     python3 scripts/nas_archive.py status
     python3 scripts/nas_archive.py gui
     python3 scripts/nas_archive.py records list --account <ACCOUNT_ID> --kind vs
     ```
   - 旧 `export-xlsx` commandは `LEGACY_ANALYSIS_XLSX_RETIRED` で拒否される。全情報XLSXとSQLite全量・差分の扱いは [full-data-sync worker](full-data-sync.md) を参照する。GUI-only publisherのreceiptは全量同期を証明しない。
   - Macローカルの [archive.py](../../archive.py) 直接実行は安全機構（`NAS_STORAGE_ACTIVE`）によりブロックされることを確認する。

### 4.4 バックアップおよびクラウド保全運用
2026-09-29 以降、定期 [nas_backup_cycle.py](../../scripts/nas_backup_cycle.py) の経路は平文SQLite snapshotである。成功条件は固定snapshotのbytes/MD5/remote readback SHA-256一致であり、ライブ `archive.sqlite3` の物理hashとの一致ではない。暗号化しない。以下の履歴節は、2026-09-24から2026-09-28に作られた既存 `.zst.gpg` 世代を読むためのもので、新しい日次backupには使わない。

#### 現行日次scheduler向けの読み取り専用source条件

- `database/` のディレクトリはSQLite WAL読み取りに必要な `-wal` / `-shm` の作成を許すためread-write mountとする。snapshot helperはsourceを `mode=ro` で開き、読み取りtransactionをpinする。別の同期workerでは `mode=ro` に加えて `PRAGMA query_only=ON` を維持する。ディレクトリのrw権限はSQLiteのsource SQLをwrite可能にするものではない。
- `BEGIN` でread transactionを確定してから `page_count` を読むため、オンラインbackup APIはそのtransactionの静止点をコピーする。snapshot側manifestが持つSHA-256は生成したsnapshot fileそのもののhashである。稼働中のmain DB fileはWALと組み合わさって論理状態を表し、checkpointやcollector更新に伴い物理bytes/hashが変わり得るため、snapshot hashとの直接一致を要求しない。
- 現在稼働中のscheduler/container/imageとreceiptは、今回の候補overlay buildでは停止・上書き・削除しない。進行中のサイクルがremote全量readbackとreceipt/state確定まで完了してから、candidate imageを別tagで作り、schedulerのみを計画的に置換する。既存image、停止済み旧container、receipt、scheduler stateは保持する。

#### 候補overlayの公開code-only build/deploy手順

[`PlaintextOverlay.Dockerfile`](../../deploy/nas/PlaintextOverlay.Dockerfile) は `ikaring-archive-backup:current` をbaseとして、公開コードの `verified_backup_support.py`、`nas_backup_cycle.py`、`nas_create_verified_backup.sh` だけを `/app/scripts/` に差し替える候補overlayである。scheduler本体、entrypoint、container設定、collectorは変更せず、新tagの候補imageとしてbuildする。

BuildKit/Dockerへ送るbuild contextは、repository外の空のprivate staging directoryに作る。含めるファイルは上記3つだけとし、SQLite本体、`-wal` / `-shm`、snapshot、rclone config、secrets、logs、receipt、scheduler state、既存暗号artifactを入れない。Dockerfileは `-f` でrepositoryから指定し、既存root `.dockerignore` やscheduler向けの許可済み設定は変更しない。

```sh
OVERLAY_CONTEXT=/path/to/private-empty/plaintext-overlay-context
CANDIDATE_TAG=ikaring-archive-backup:plaintext-pinned-read-unique-id
mkdir -m 700 -p "$OVERLAY_CONTEXT"
install -m 0644 scripts/verified_backup_support.py "$OVERLAY_CONTEXT/"
install -m 0644 scripts/nas_backup_cycle.py "$OVERLAY_CONTEXT/"
install -m 0644 scripts/nas_create_verified_backup.sh "$OVERLAY_CONTEXT/"
docker build -f deploy/nas/PlaintextOverlay.Dockerfile \
  -t "$CANDIDATE_TAG" "$OVERLAY_CONTEXT"
```

Build前にstaging directoryのファイル名を全件列挙し、この3ファイル以外がないことを確認する。build後はcandidate tagとimage IDを記録し、`ikaring-archive-backup:current` のtag/image ID、既存scheduler container、state、receiptが保持されていることを確認する。現cycle完了後にのみ置換へ進み、旧image/containerは退避名で保持してpruneしない。新containerは既存と同じUID/GID、database directoryのrw mount、backup/state/rclone設定の配置、Asia/Tokyo scheduleを維持する。置換後に実際の次cycleが固定snapshotの作成、全量remote readback、receipt確定まで成功したことを確認するまで、candidate buildやcontainer起動だけをbackup成功と扱わない。

既存schedulerを切り替える前に、candidate imageを隔離した小さなsynthetic SQLite fixtureで実行する。実行入口は [`tests/integration/nas_backup_fixture.py`](../../tests/integration/nas_backup_fixture.py) であり、test helperをread-only mountし、空のprivate work directoryだけを書込み可能にして、UID `1000:10`・network disabledで実行する。`--work-dir` はmode `0700` 以下で、fixtureが使ったsource・WAL・snapshot・fake remote・receipt・argv logを保持し、自動削除しない。

```sh
python3 /path/to/nas_backup_fixture.py \
  --work-dir /private/path/to/backup-fixture-work \
  --cycle-script /app/scripts/nas_backup_cycle.py
```

テスト用sourceにはWAL経由でcommit済みの行を含め、NULL、空文字、大整数、REAL、NUL入りTEXT、BLOBをnative typeのままsnapshotと照合する。fake rcloneは`fixture:` remoteだけを許可し、保存先もwork directory配下に限定する。snapshotがpinした静止点の値を含み、manifestのbytes/SHA-256とsnapshot本体が一致し、`quick_check` とupload対象全体のサイズ・MD5・SHA-256 readbackおよびreceiptが一致するところまで確認する。live DB本体fileのhash一致は条件にしない。fixtureから実NAS source、Drive remote、scheduler state、現行receiptへ接続させず、この検査の完了後も実際の次cycleの全量readbackとreceiptが確認できるまでは切替を成功扱いしない。

#### 4.4.1 履歴: 2026-09-28 までの単一cipher直接保全経路
これは現行手順ではない。2026-09-28 まで NAS 内で無人実行されていたバックアップサイクルであり、当時は以下の処理を一貫して実施していた。

1. [nas_create_verified_backup.sh](../../scripts/nas_create_verified_backup.sh) を呼び出し、オンラインバックアップ作成、空き容量事前検査（4倍+1GiB）、zstd+AES256暗号化、復号ハッシュ照合、`PRAGMA quick_check` 検査を経て、単一暗号化ファイル（`*.zst.gpg`）および元暗号マニフェスト（`*.manifest.json`）を生成する。
2. 生成された単一暗号化ファイルとマニフェストを、rcloneを用いてGoogle Driveへストリームアップロードする。
3. `rclone cat` による全量読み戻しとSHA-256往復検証を実施する。
4. 往復検証の成功確認後、確定レシート（`cloud-receipts/`）をアトミックに記録する。
※本スクリプトは単一cipherファイルを直接扱う経路であり、chunk分割処理は含まれない。

#### 4.4.2 connector手動chunk分割保全経路（64MiB chunk分割・ストリーム往復検証）
Google Drive connectorの二段上限（512MiB/100MiB）を回避するため、専用スクリプトを用いて手動で実行する保全手順である。

1. **スナップショット作成と暗号化**:
   [nas_create_verified_backup.sh](../../scripts/nas_create_verified_backup.sh) を呼び出し、オンラインバックアップの作成、zstd+AES256暗号化、復号ハッシュ照合、`PRAGMA quick_check` 検査を経て、単一暗号化ファイルおよび元暗号マニフェストファイル（`*.manifest.json`）を生成する。
2. **チャンク分割（splitサブコマンド）**:
   Google Drive connectorの二段上限（512MiB/100MiB）を回避するため、[backup_chunks.py](../../scripts/backup_chunks.py) `split` により暗号文を既定64MiBのpartファイル群に分割する。
   ```bash
   python3 scripts/backup_chunks.py split \
     --ciphertext /path/to/backup.sqlite3.zst.gpg \
     --encrypted-manifest /path/to/backup.sqlite3.manifest.json \
     --output-dir /path/to/chunks_bundle \
     --chunk-size 67108864
   ```
   - 出力bundleディレクトリ内には、各part（`*.part-000000` 〜）、コピーされた元暗号マニフェスト、およびチャンクマニフェスト（`*.chunks.json`）が生成される。
3. **クラウドへの全partアップロード**:
   Google Driveのflat folderへ、各partファイル群を順次アップロードする（flat folder構造で元uuid付きbasenameを共有）。
4. **クラウド全量ストリーム復号検証 (`verify_cloud_chunks.py`)**:
   全partのアップロード完了後、確定前に [verify_cloud_chunks.py](../../scripts/verify_cloud_chunks.py) を実行してクラウド上の全partをストリーム読取・復号し、完全性を検証する。
   ```bash
   # シェルリダイレクト解釈を防止するためプレースホルダーは引用符で囲む
   python3 scripts/verify_cloud_chunks.py \
     --remote "gdrive_remote:ikaring-archive/backups" \
     --manifest "/path/to/chunks_bundle/<cipher_name>.chunks.json" \
     --passphrase-file /path/to/passphrase_file
   ```
   - `rclone cat` で各partをストリーム読取し、暗号文全体のSHA256および復号・伸長後のrawバイト数・raw SHA256を元暗号マニフェストと照合する。
   - `verify_cloud_chunks.py` は新規のSQLite `quick_check` は実行しない（元静止点スナップショット作成時の `quick_check=ok` と、復号rawバイト列およびSHA256ハッシュの完全一致が健全性の証拠となるためである）。
5. **2マニフェストのアップロードと読み戻しハッシュ記録**:
   全量ストリーム復号検証が成功した後にのみ、元暗号マニフェストとチャンクマニフェストの2ファイルをクラウドへアップロードし、読み戻しハッシュ一致を記録する。

### 4.5 チャンク分割バックアップからの障害復元手順
不測の事態によりクラウド上のチャンク分割バックアップからデータを復元する必要が生じた場合の安全手順を以下に示す。

#### 4.5.1 手順の全体要約と安全原則
- **検証中のコレクター稼働維持**: チャンクのダウンロード、結合、復号・伸長、ハッシュ照合、`PRAGMA quick_check` などの復元検証作業中は、コレクターを停止する必要はない。独立した安全な作業ディレクトリで全量検証を完遂させ、新DBの正常性が立証された「最後の正本切替直前」においてのみ対象コレクターを停止して排他状態を確保する。これにより無駄な収集停止と戦績流出を防ぐ。
- **空き容量の段階的事前検査**: リストア作業に必要な容量は平文rawだけでなく、取得part総量、結合後暗号文、平文raw、および十分な安全マージン（余裕）の合計である。作業を一括で確認するだけでなく、各工程（part取得前、join前、復号・伸長前、本番ステージング前）の直前に「現在の空き容量」と「その工程で必要となる追加容量」を逐次検査する。容量逼迫したローカルディスクや `/tmp` では作業を行わない。
- **安全なマニフェスト解析とファイル取得**: cipher basenameには `.zst.gpg` が含まれており、元暗号マニフェストは `<cipher_name>.manifest.json` ではない。まず正確なチャンクマニフェスト（`*.chunks.json`）を取得し、その中の `encrypted_manifest.basename` をPythonで安全basename検証（`safe_basename`）して元暗号マニフェスト名を特定する。全part名もチャンクマニフェストの `parts` 定義から取得する。シェルリダイレクト事故防止のため、コードプレースホルダーは必ず引用符で囲む。
- **同一ローカルディレクトリへの配置と既存上書き拒否**: 結合処理（[backup_chunks.py](../../scripts/backup_chunks.py) `join`）を安全に行うため、チャンクマニフェスト、元暗号マニフェスト、全partファイルを同一作業ディレクトリへ配置する。`backup_chunks.py join` および復号処理は既存ファイルが存在する場合に上書きを拒否する。
- **安全なパイプライン実行**: `set -euo pipefail` および `set -C`（既存ファイル保護）を有効化し、暗号鍵はパーミッション mode 600 の既存鍵ファイル（`passphrase-file`）を使用する。秘密値（パスフレーズ）をコマンド引数や標準入力へ直書きすることは厳禁とする。
- **新規復元DBの完全性検査**: 展開後、rawバイト数、raw SHA-256ハッシュ、およびSQLite `PRAGMA quick_check` を検証する。読み取り専用接続には `?mode=ro&immutable=1` を使用し、接続直後に `PRAGMA cache_size = -65536;`（64MiBキャッシュ）を設定して検査を行う。
- **切替専用手順の遵守（安易な2本mvの厳禁）**:
  - 旧DBの `archive.sqlite3-wal` や `archive.sqlite3-shm`、`archive.sqlite3-journal` が残っている状態で新DBを配置すると、古いWAL/SHMが新DBと混在し重大なDB破損を引き起こす。また異ファイルシステム間の `mv` はアトミックではない。
  - したがって安易な `mv` 2本による置換は厳禁とし、以下の切替専用手順を厳守する：
    1. コレクター停止
    2. 排他維持
    3. 旧DB本体、WAL、SHM、journalを一組でユニーク退避
    4. 同一ファイルシステムへ検証済み復元DBをステージングしてハッシュ再照合
    5. `os.replace` によるアトミック置換
    6. 失敗時の旧一組復元（ロールバック）
    7. 成功確認後のコレクター再開
- **元DB削除前のNAS最新点検証条件の保持**: 本復元手順は障害復旧または保全検証のための手順であり、元DBを削除する前にNAS最新静止点の検証条件およびクラウド保全の完全性検証条件を満たす安全原則は変更されない。

#### 4.5.2 詳細復元ステップ

1. **作業ディレクトリの準備と容量確認（工程A前検査）**:
   十分な空き容量のある独立した作業用ファイルシステム上に作業ディレクトリを作成する。
   復元作業全体では「取得part総量＋結合後暗号文joincipher＋平文raw＋安全余裕（5GB以上）」が必要となる。
   まず、チャンクマニフェストおよび全partを取得する前段階（工程A前）の空き容量を検査する。
   ```bash
   set -euo pipefail
   WORK_DIR="/path/to/large-disk/ikaring-restore-chunks"
   mkdir -m 700 -p "$WORK_DIR"

   # マニフェスト取得後、実サイズから全part取得の追加容量を検査する。
   ```

2. **正確なチャンクマニフェスト取得と取得対象リストの安全確定**:
   cipher basenameには `.zst.gpg` が含まれるため、元暗号マニフェストは `<cipher_name>.manifest.json` ではない。
   まずクラウドから復元対象のチャンクマニフェスト（`*.chunks.json`）を単独で取得し、その内容から元暗号マニフェスト名および全part名を安全に特定する。
   ```bash
   # シェルリダイレクト解釈を防止するため、プレースホルダーは引用符で囲む
   CHUNK_MANIFEST_NAME="<cipher_name>.chunks.json"

   # 1. チャンクマニフェストを先行取得
   rclone copy "gdrive_remote:ikaring-archive/backups/$CHUNK_MANIFEST_NAME" "$WORK_DIR"

   # 2. チャンクマニフェストを解析し、元暗号マニフェスト名と全part名を安全basename検証して抽出
   python3 - "$WORK_DIR/$CHUNK_MANIFEST_NAME" "$WORK_DIR/download-list.txt" <<'PY'
   import json, sys, shutil
   from pathlib import Path
   from scripts.backup_chunks import validate_chunk_manifest

   def safe_basename(name: str) -> str:
       if (not isinstance(name, str) or not name or name in {'.', '..'} or
               '/' in name or '\\' in name or '\x00' in name or Path(name).name != name):
           raise ValueError(f"UNSAFE_BASENAME: {name}")
       return name

   manifest_path = Path(sys.argv[1])
   output_list = Path(sys.argv[2])

   data = json.loads(manifest_path.read_text(encoding='utf-8'))
   cipher, reference, parts = validate_chunk_manifest(data)
   needed = cipher['bytes'] + reference['bytes'] + 1024**3
   assert shutil.disk_usage(manifest_path.parent).free >= needed, 'Insufficient space for parts'
   enc_manifest_name = safe_basename(data['encrypted_manifest']['basename'])
   part_names = [safe_basename(part['basename']) for part in data['parts']]

   with output_list.open('w', encoding='utf-8') as f:
       f.write(enc_manifest_name + '\n')
       for p in part_names:
           f.write(p + '\n')

   print(f"Validated manifest: {enc_manifest_name}")
   print(f"Validated parts count: {len(part_names)}")
   PY

   # 3. 確定したリストに基づき、元暗号マニフェストおよび全partを同一ディレクトリへ取得
   rclone copy "gdrive_remote:ikaring-archive/backups" "$WORK_DIR" \
     --files-from-raw "$WORK_DIR/download-list.txt"
   ```
   > [!IMPORTANT]
   > `backup_chunks.py join` はチャンクマニフェストと同じ親ディレクトリ内に元暗号マニフェストおよび全partが存在することを前提としているため、必ず同一作業ディレクトリに配置する。

3. **チャンク結合前の容量検査（工程B前）と暗号文復元 (`backup_chunks.py join`)**:
   結合後暗号文を出力するための追加空き容量を検査した上で、`backup_chunks.py join` を実行する。
   ```bash
   CHUNK_MANIFEST="$WORK_DIR/$CHUNK_MANIFEST_NAME"

   # 工程B前検査: 結合後暗号文のサイズ＋安全余裕（約1GiB）の空き容量があるか検査
   python3 - "$CHUNK_MANIFEST" "$WORK_DIR" <<'PY'
   import json, shutil, sys
   from pathlib import Path
   manifest = json.loads(Path(sys.argv[1]).read_text(encoding='utf-8'))
   cipher_bytes = manifest['ciphertext']['bytes']
   work_dir = Path(sys.argv[2])
   free_bytes = shutil.disk_usage(work_dir).free
   needed = cipher_bytes + 1024 * 1024 * 1024
   assert free_bytes >= needed, f"Insufficient disk space for join: free {free_bytes} < needed {needed}"
   print(f"Capacity check for join passed: {free_bytes // (1024**3)} GiB free.")
   PY

   # 暗号文の安全な結合と検証
   OUTPUT_CIPHER=$(python3 -c "import json, sys; print(json.load(open(sys.argv[1]))['ciphertext']['basename'])" "$CHUNK_MANIFEST")
   OUTPUT_CIPHER_PATH="$WORK_DIR/$OUTPUT_CIPHER"

   python3 scripts/backup_chunks.py join \
     --chunk-manifest "$CHUNK_MANIFEST" \
     --output "$OUTPUT_CIPHER_PATH"
   ```
   - **安全機構**:
     - 出力先（`--output`）が既に存在する場合は `DESTINATION_EXISTS` エラーとなり上書きを拒否する。
     - 各partの順序（インデックス）、サイズ、SHA-256ハッシュを全数検査する。
     - 結合された暗号文全体のサイズおよびSHA-256ハッシュを検査する。
     - 元暗号マニフェストのサイズ、SHA-256ハッシュ、および内部スキーマ（`raw_snapshot`, `encrypted_snapshot`, `verification`）を照合する。

4. **復号・伸長前の容量検査（工程C前）と安全な復号実行**:
   元暗号マニフェストから平文データベースの容量（`raw_snapshot.bytes`）を読み取り、平文raw＋安全余裕を展開できる空き容量があることを確認した上で、GPG復号とzstd伸長を実行する。
   ```bash
   ENCRYPTED_MANIFEST_NAME=$(python3 -c "import json, sys; print(json.load(open(sys.argv[1]))['encrypted_manifest']['basename'])" "$CHUNK_MANIFEST")
   ENCRYPTED_MANIFEST_PATH="$WORK_DIR/$ENCRYPTED_MANIFEST_NAME"

   # 工程C前検査: 平文rawサイズ＋安全余裕（3GiB）の空き容量があるか検査
   python3 - "$ENCRYPTED_MANIFEST_PATH" "$WORK_DIR" <<'PY'
   import json, shutil, sys
   from pathlib import Path
   manifest = json.loads(Path(sys.argv[1]).read_text(encoding='utf-8'))
   raw_bytes = manifest['raw_snapshot']['bytes']
   work_dir = Path(sys.argv[2])
   free_bytes = shutil.disk_usage(work_dir).free
   needed = raw_bytes + 3 * 1024 * 1024 * 1024
   assert free_bytes >= needed, f"Insufficient disk space for raw decompression: free {free_bytes} < needed {needed}"
   print(f"Capacity check for raw decompression passed: {free_bytes // (1024**3)} GiB free.")
   PY

   # 安全なパイプライン復号と伸長
   set -euo pipefail
   set -C

   RESTORED_DB="$WORK_DIR/restored-archive.sqlite3"
   PASSPHRASE_FILE="/path/to/keyfile"  # mode 600 で保全された鍵ファイル

   # 鍵ファイルのパーミッション確認（他ユーザー/グループアクセス禁止: mode 600）
   test "$(stat -f '%Lp' "$PASSPHRASE_FILE" 2>/dev/null || stat -c '%a' "$PASSPHRASE_FILE")" = "600"

   # パイプライン復号と伸長（既存ファイルの上書き禁止）
   gpg --batch --yes --no-tty --pinentry-mode loopback \
     --passphrase-file "$PASSPHRASE_FILE" \
     --decrypt "$OUTPUT_CIPHER_PATH" | zstd -d -c > "$RESTORED_DB"
   ```

5. **平文DBの完全性検査（raw SHA-256 / raw bytes / SQLite PRAGMA quick_check）**:
   展開されたデータベースファイルについて、サイズ、SHA-256ハッシュ、およびSQLite内部構造の完全性を検証する。
   SQLite接続には `mode=ro&immutable=1` を使用し、接続直後に `PRAGMA cache_size = -65536;`（64MiBキャッシュ）を設定する。
   ```bash
   python3 - "$ENCRYPTED_MANIFEST_PATH" "$RESTORED_DB" <<'PY'
   import hashlib, json, sqlite3, sys
   from pathlib import Path

   manifest_path = Path(sys.argv[1])
   db_path = Path(sys.argv[2])

   manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
   expected_raw = manifest['raw_snapshot']

   # 1. バイト数検査
   actual_size = db_path.stat().st_size
   assert actual_size == expected_raw['bytes'], f"Size mismatch: {actual_size} != {expected_raw['bytes']}"

   # 2. SHA-256ハッシュ検査
   digest = hashlib.sha256()
   with db_path.open('rb') as f:
       while chunk := f.read(1024 * 1024):
           digest.update(chunk)
   actual_sha = digest.hexdigest()
   assert actual_sha == expected_raw['sha256'], f"SHA256 mismatch: {actual_sha} != {expected_raw['sha256']}"

   # 3. SQLite PRAGMA quick_check 検査（immutable=1 および 64MiBキャッシュ指定）
   uri = f"{db_path.resolve().as_uri()}?mode=ro&immutable=1"
   con = sqlite3.connect(uri, uri=True)
   try:
       con.execute('PRAGMA cache_size = -65536;')
       res = con.execute('PRAGMA quick_check;').fetchall()
       assert res == [('ok',)], f"quick_check failed: {res}"
   finally:
       con.close()

   print("All restore verifications passed: bytes, SHA-256, and SQLite quick_check (ok).")
   PY
   ```
   > [!NOTE]
   > この検証が完了するまでの間、本番コレクターは稼働を維持している。次の切替直前に初めてコレクターを停止する。

6. **本番正本アトミック切替専用手順（スイッチオーバープロトコル）**:
   すべての検証が完了した後、本番DB配置先への切り替えを行う。
   > [!CAUTION]
   > **重大危険の回避**:
   > - 旧DB本体のみを `mv` して新DBを配置すると、旧DBの `archive.sqlite3-wal` や `archive.sqlite3-shm`、`archive.sqlite3-journal` の残骸が本番ディレクトリに残り、新DBと混在して重大なDB破損を引き起こす。
   > - 異なるファイルシステム間の `mv` はPOSIX上アトミックではなく、コピー途中の障害で破損ファイルが生じる。
   > - したがって安易な2本の `mv` コマンドは絶対に使用してはならない。以下の手順で旧版を保持しながら切り替える。

   切り替えは以下の順序で確実に実行する：
   1. **コレクター停止**: 対象コレクターを停止し、書き込みプロセスを遮断する。
      ```bash
      # Macローカルの場合
      launchctl bootout gui/$(id -u) ~/Library/LaunchAgents/local.ikaring3.archive.plist
      # NAS Dockerコンテナの場合
      docker stop ikaring-archive
      ```
   2. **排他維持**: プロセスが停止し、他プロセスがDBにアクセスしていない排他状態を確認・維持する。
   3. **旧DB一組のユニーク退避**:
      旧DB本体（`archive.sqlite3`）、および存在するすべての関連ファイル（`-wal`, `-shm`, `-journal`）を一組として、同一のユニークな退避ディレクトリ（例: `database/retired-<UUID>/`）へ退避する。各ファイルの移動を記録し、途中中断時はその記録から旧一組を復元する。
   4. **同一ファイルシステムへのステージングとハッシュ再照合（工程D）**:
      本番配置先のファイルシステムに空き容量があることを確認し、同一ディレクトリ内にステージングファイル（`.archive.sqlite3.staged.<UUID>`）として復元DBを配置。直後にSHA-256ハッシュを再計算して元マニフェストと完全一致することを再照合する。
   5. **アトミック置換 (`os.replace`)**:
      同一ファイルシステム内で `os.replace` を実行し、ステージングDBを正規パス（`archive.sqlite3`）へ瞬時にアトミック置換する。
   6. **失敗時のロールバック**:
      ステージングや置換、ハッシュ照合で異常が発生した場合は、退避した旧DB一組（本体、WAL、SHM、journal）を直ちに元のパスへ復元する。

   個別ファイルのrenameは一括トランザクションではないため、途中中断時も旧DB一組の配置を確認するまで収集を再開しない。

7. **コレクターの再開と稼働確認**:
   正本置換が成功したことを確認した後、コレクタープロセスを再開し、正常にデータ収集が継続されることをログおよびステータス（3.1節の各検証基準）で確認する。
   ```bash
   # Macローカルの場合
   launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/local.ikaring3.archive.plist
   # NAS Dockerコンテナの場合
   docker start ikaring-archive
   ```

---

### 4.6 単一暗号化ファイルからの復元手順（補足）
チャンク分割を行わない単一の暗号化スナップショット（`*.zst.gpg`）から直接復元する場合の手順を参考として以下に示す。手動チャンク分割経路と同様に、復元検証中はコレクターを停止せず、切替直前にのみ停止・排他確保を行う。

1. **容量確認と独立作業ディレクトリの準備**:
   復号・伸長先ファイルシステムにおいて、「平文rawサイズ＋安全余裕」の空き容量があることを確認し、独立した作業ディレクトリを用意する。
   ```bash
   WORK_DIR=$(mktemp -d /path/to/large-disk/ikaring-restore.XXXXXXXX)
   RESTORED_DB="$WORK_DIR/restore_test.sqlite3"
   ```
2. **暗号化バックアップの復号と伸長（検証中はコレクター稼働維持）**:
   パーミッション mode 600 の既存鍵ファイルを用いて、パイプライン復号と伸長を実行する。
   ```bash
   set -euo pipefail
   set -C
   PASSPHRASE_FILE="/path/to/keyfile"
   test "$(stat -f '%Lp' "$PASSPHRASE_FILE" 2>/dev/null || stat -c '%a' "$PASSPHRASE_FILE")" = "600"

   gpg --batch --pinentry-mode loopback --decrypt \
     --passphrase-file "$PASSPHRASE_FILE" \
     backup_target.sqlite3.zst.gpg | zstd -d -c > "$RESTORED_DB"
   ```
3. **ハッシュ照合と整合性検証**:
   - 伸長したファイルとマニフェスト内の `raw_snapshot.sha256` およびバイト数が完全一致することを確認。
   - SQLite接続に `?mode=ro&immutable=1` を使用し、`PRAGMA cache_size = -65536;` を実行した上で `PRAGMA quick_check;` を実行して `ok` を確認。
4. **切替直前のコレクター停止とアトミック置換手順**:
   - すべての検証が成功した段階で、コレクター（NASコンテナまたはMacプロセス）を停止して排他状態を確保する。
   - 安易な `mv` 2本による置換は行わず、切替専用手順（旧DB本体・`-wal`・`-shm`・`-journal` の一括ユニーク退避、同一ファイルシステムへのステージングとハッシュ再照合、`os.replace` によるアトミック置換、失敗時ロールバック）に従って本番DBを切り替える。
5. **コレクター再開と稼働実測**:
   置換完了を確認した後、コレクターを再開し正常収集を実測する。

---

### 4.7 NAS軽量生成物配信の運用手順
NAS上の `gui/index.html` だけを定期クラウド配信するGUI-only publisherについては、専用手順書である [nas-export-publisher.md](nas-export-publisher.md) に詳細な仕様と運用手順を定めている。全情報XLSXとSQLite全量・差分の配信は別pipelineであり、[full-data-sync worker](full-data-sync.md) と [cloud-data-map.md](cloud-data-map.md) を参照する。

#### 運用の要点
- **配信対象**: `exports/gui/index.html` 一つだけ。旧 `exports/分析.xlsx` は読まず、生成・更新・配信せず、既存ファイルがあれば履歴資産として残す。
- **receiptの範囲**: 成功receiptは `scope: gui_only`、`full_database_synchronized: false`。GUI一つの遠隔確認を示し、SQLite全量・全情報XLSX・継続同期は証明しない。
- **保存先**: ユーザーの戦績を含む派生出力であるため、事前に認可された非公開保存先（認可済みprivate Google Drive）のみに配信。DB正本、WAL、スプール、認証情報、収集プログラム等は完全除外。
- **実行形式**: Dockerコンテナ（`ikaring-archive-backup:current`）として最小権限（非root `1000:10`、read-only rootfs、`cap-drop ALL`、tmpfs 32m）で常駐運転または単発実行。
- **安全機構**:
  - リモート競合検知時は `.sync-conflicts/<UUID>/` へ退避保全後、`rclone cat` による全量読戻しSHA-256で退避完全性を検証してから配信先更新へ進む。
  - クラウドストレージの特性上、CAS（Compare-And-Swap）は提供されず微小な競合窓が存在する。
  - 配信対象はHTML一つなので複数ファイル間のtransactionはない。全量読戻し後にGUI専用状態とreceiptを確定する。全情報データpipelineの世代・receipt・checkpointとは独立して扱う。
