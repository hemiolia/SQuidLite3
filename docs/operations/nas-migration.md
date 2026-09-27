# NAS移行およびバックアップ運用手順書

本手順書は、ikaring-archiveにおけるMacローカル環境からNAS環境へのデータコレクター移行、定期バックアップ、クラウド保全、軽量ファイル同期、およびリストアに関する運用仕様と手順を定義する。

## 1. 基本方針とアーキテクチャ

### 1.1 NAS DBローカルディスクとコレクターの共置
- NAS上のデータコレクター（Dockerコンテナ）とSQLiteデータベースは、NASローカルのファイルシステム上に同一ホスト内で共置（co-located）して運用する。
- ネットワークマウント（SMBやNFS等）越しにSQLiteデータベースへ直接アクセス・書き込みを行う運用は行わない。ネットワークファイルシステム越しのロック制御不安定による破損を防止するためである。

### 1.2 稼働中DBの直接同期・コピー禁止
- 稼働中（書き込みプロセスが動作中）のSQLiteデータベースファイルを、SMB、Google Drive、rsync、双方向ファイル同期ツール等によって直接コピー・転送することは厳禁とする。
- 稼働中DBの転送はWALファイルとの不整合やページ破損、スプリットブレインの原因となる。バックアップや転送は、必ずSQLiteのオンラインバックアップAPIを用いて整合性のある静止点スナップショットを取得し、完全性検査を行った上で実施する。

### 1.3 軽量ファイルの双方向同期と競合旧版退避（導入検証中）
- `exports/` ディレクトリ下のレポート（HTML/XLSX等）など軽量ファイルの同期は、専用スクリプト（`sync_nas.py` および `sync_gdrive.py`）により制御する。
- **定期運転の状況**: 3way baselineと共通排他は実装・検証済みで、実NASとGoogle Driveの手動同期を2回完走した。LaunchAgentは登録済みだが、macOS側のファイルアクセス許可の確認待ちで自動実行は未成功である。
- **3way baselineと競合制御**:
  - 片側だけの更新は通常更新としてアトミックに反映する。
  - 両側で独立編集（競合）が発生した場合は、mtime（更新日時）を採用しつつ、上書きされる側の既存旧版を `.sync-conflicts/<UUID>/<相対パス>` へ安全に退避・保存した上でアトミックに新版へ置き換える。
  - プロセス間の共通排他ロック（lock）を適用して並行実行による不整合を防ぐ。
- **同期除外対象**:
  - データベースファイル（`*.sqlite*`）、秘密情報・認証情報（`secrets/`、`auth`、`token`、`credential`、`rclone.conf`、`rclone-*.conf` 等）、ランタイム一時スプール、およびNAS/Google Driveの同期状態管理ファイル（stateファイル）は同期対象から除外（ignore）される。
  - なお、`logs/` ディレクトリ全体は除外しない（通常のレシートJSONや運用ログ等は同期対象とし、秘密を含む個別設定・秘密ファイルのみを除外する）。
- **サービスインストーラー**: `python3 scripts/install_file_sync_service.py --print` で設定を確認し、`--install` で5分間隔のLaunchAgentを登録する。実行元はPythonの `scripts/run_file_sync.py`。Mac側のファイルアクセス許可と実行ログの成功を確認するまで、登録だけで稼働済みとはしない。

### 1.4 クラウド（Google Drive）への暗号スナップショット全量往復検証とチャンク分割設計
- クラウドへ保存するバックアップは、ローカルで生成した静止点スナップショットを圧縮（zstd）および対称暗号化（AES256）したファイルと、その整合性を記録したマニフェストファイルに限定する。
- **Google Drive connectorの二段上限と64MiBチャンク分割**:
  - Google Drive connectorにおいて、転送開始前のランタイム512MiB上限（`536,870,912 bytes`）に加え、後段のHTTP 413による100MiB上限の実測が確認された。
  - この二段上限を確実に回避するため、暗号化暗号文（cipher）を既定64MiB part（`scripts/backup_chunks.py`、最大256MiBまで対応可能だが二段上限回避のため既定64MiB）に分割して転送するchunk方式を採用する。
  - 暗号文cipher本体をpart分割（`"<cipher>.part-XXXXXX"`）、元暗号マニフェストの複製、およびチャンクマニフェスト（`"<cipher>.chunks.json"`）で構成し、クラウド上およびローカルのflat folderで同一の元uuid付きbasenameを共有して管理する。
- **全partアップロード後のストリーム全量往復検証**:
  - 全partのアップロード完了後、確定前に `scripts/verify_cloud_chunks.py` を用いて、クラウドから各partを `rclone cat` でストリーム読取し、GPG復号＋zstd伸長を行いながら、暗号文SHA256ハッシュおよび復号後rawサイズ・raw SHA256ハッシュが元マニフェストと完全一致することを検証する。
  - `verify_cloud_chunks.py` はストリーム復号によるハッシュ照合を実施するが、新規のSQLite `quick_check` は実行しない（元静止点スナップショット作成時の `quick_check=ok` と、復号rawバイト列およびSHA256ハッシュの完全一致が健全性の証拠となるためである）。
- **2マニフェストのアップロードと確定**:
  - 全partのストリーム往復検証が成功した後にのみ、2つのマニフェスト（元暗号マニフェストとチャンクマニフェスト）をアップロードし、クラウドからの読み戻しハッシュ一致を記録する。
  - `gdrive_upload_verified.sh` 単体は結果を標準出力のJSONで返し、レシートファイルは作らない。`nas_backup_cycle.py` 経由のサイクルだけが、成功後に `cloud-receipts/` へレシートをアトミックに記録する。
- **保全経路の峻別（定期単一cipher経路とconnector手動chunk経路）**:
  - 定期 `nas_backup_cycle.py` は、rcloneを用いた単一cipher（単一暗号化ファイル）の直接アップロード経路であり、chunk分割自動化は未実装である。
  - Google Drive connectorの二段上限（512MiB/100MiB）を回避するためのchunk分割経路は、`scripts/backup_chunks.py` および `scripts/verify_cloud_chunks.py` を用いる手動chunk経路である。
  - これら2つは独立した別経路であり、同一処理であると誤解してはならない。

### 1.5 安全原則: クラウド未検証時のローカル削除不可（安全原則の遵守とローカル削除完了）
- クラウドへの暗号化バックアップが全量往復検証を経て完全に保全されたことを確認するまでは、Macローカルの正本DBや既存のローカルバックアップファイルを絶対に削除してはならない。`nas_backup_cycle.py` を使う場合はレシート生成も確認する。
- **安全原則遵守の実績（2026-09-27 07:33削除完了）**:
  - 移行9/26 03:19静止点（raw 26,412,634,112 bytes, SHA 069290e0ae6f0982ac8dd0db67df31f53a015246d110970041a2a3f4ef29c6e5）、旧22GB、9/25 11:59、旧432MB、および最新daily静止点について、全37 cloud partsからの復号一致、両manifest full readback一致（status complete）を含むNAS保全およびクラウド全量raw一致を確認。
  - さらに、Mac collector停止（disabled/未登録）、NAS marker存在、ローカルhash検証時からのinode/mtime/size不変、WAL無し、writer lock下であることを再確認した上で、許可済みのローカルDB 4本＋暗号作業用複製65個（論理合計 83,729,271,876 bytes）を安全に削除完了。実空き容量は 3,237,855,232 bytes から 38,921,396,224 bytes へ大幅に回復した。
  - 元DB位置には `relocated.json` を保存し、NASおよびクラウド上の旧版は保持している（private証拠: `operations-20260926/local-cleanup-complete.json` および各 `cloud-complete.json`）。

### 1.6 移行マーカーとスプリットブレイン防止
- 移行マーカー（`config/storage-location.json`）は、コレクターの稼働主体がNASへ正常に切り替わったことを示す制御設定ファイルである。
- このマーカーが存在する場合、Macローカルの `archive.py` はローカル既定DBへのアクセスを遮断（`NAS_STORAGE_ACTIVE` 例外を発生）し、操作は `scripts/nas_archive.py` を経由してNAS側コンテナで実行することが強制される。
- **自動フォールバックの禁止**: NASとの接続切断や障害が発生した場合に、自動的にMacローカルDBへ書き込み先をフォールバックさせる処理は行わない。自動フォールバックを許容すると、NASとMacの双方に異なる新データが書き込まれる「スプリットブレイン」が生じ、データの整合性回復が困難になるためである。障害時は取得を停止し、手動で状態を確認して復旧する。
- **Start.command による安全なルーティング**: Macの起動スクリプト `Start.command`（`scripts/start.py`）は、起動時に `config/storage-location.json` のNASマーカーを検証し、正常なNASマーカーを検出した場合は自動的に `scripts/nas_archive.py gui` へ処理をルーティングする。これにより、Mac側でのNode.js/npm依存関係やニンテンドーアカウントへのログイン処理を一切不要とし、NAS側で生成された最新GUIを安全に表示する。マーカー不正時は即座に処理を拒否し、ローカルへの自動フォールバックは行わない。

### 1.7 復元手順の安全性
- バックアップからの復元（リストア）は、既存の本番稼働中DBへ直接上書きしてはならない。
- ダウンロード、チャンク結合、復号・伸長、ハッシュ検証、`PRAGMA quick_check` などの復元検証作業中からコレクターを止める必要はない。独立した安全な作業ディレクトリで検証を完了させた後、最後の正本切替直前に対象コレクターを停止して排他状態を確保する。
- 切替時は旧DB本体だけでなく関連ファイル（`-wal`, `-shm`, `-journal`）を一組でユニーク退避し、同一ファイルシステム上で検証済みDBをステージング・ハッシュ再照合した上で `os.replace` によりアトミックに差し替える。安易な `mv` 2本による置換はWAL混在や非アトミック破損を招くため厳禁とする。

### 1.8 暗号鍵の分離保全
- バックアップの暗号化および復号に使用する暗号鍵（パスフレーズ）は、単一障害点や環境侵害時の漏洩を防ぐため、NASローカルの制限領域（パーミッション0600）とMacのKeychain（`ikaring-archive-gdrive-backup`）にそれぞれ独立して別々に保全する。

### 1.9 ネットワークとポート設計
- NAS上で動作するDockerコンテナは、外部公開ポートを必要としない（`-p` によるポート開放は不要）。
- 外部やMacからの操作は、セキュアなSSH経由および `docker exec` による内部コマンド実行によって完結する。

### 1.10 ソース公開範囲の境界
- GitHub等のリポジトリへの公開は、コレクターおよび管理ツールのソースコードのみを対象とする。
- 実戦績データ、SQLiteデータベース、スプール、トークンや秘密情報、ローカルログ、個人を特定するデータは一切含めない。
- **GitHub暗号化DB保管役割の保留**: GitHubリポジトリへの暗号化DB自体の保管役割についてはユーザー未回答のまま保留中である。そのため、既存の公開リポジトリはソースコードと復元手順のみを保持し、暗号化DBファイル等は含めない。

---

## 2. 実装スクリプトの仕様と役割

| スクリプト / サービス | 実行場所 | 主な役割と安全機構 |
| :--- | :--- | :--- |
| `scripts/deploy_nas_container.sh` | Macから実行 | コレクターのソースコードのみを抽出し、tarストリームでSSH経由でNASへ転送。NAS側で排他ロック（flock）を取得しDockerイメージをビルド。旧版ソースを `runtime/releases/` に退避しアトミックに差し替え。 |
| `scripts/nas_create_verified_backup.sh` | NAS内 | SQLiteオンラインバックアップAPIによる静止点作成、空き容量事前検査（4倍+1GiB）、zstd圧縮+GPG AES256暗号化、復号+伸長ハッシュ往復検証、`quick_check` 検査、マニフェスト生成。全検証完了後に確定パスへアトミック移動。 |
| `scripts/run_nas_daily_backup.sh` | NASホスト内（手動補助） | 日次バックアップ実行用シェル。非ブロッキング排他ロック（`logs/backup/.daily.lock`）、日次ログ生成、バックアップコンテナ（`ikaring-archive-backup:current`）実行およびステータス管理。 |
| Docker schedulerコンテナ | NAS内（常駐サービス） | NASホストcron権限制限を回避し、日次バックアップを毎朝4:00 Asia/Tokyoに自動起動（UID 1000:10、`restart: unless-stopped`、失敗時15分retry、同日成功後重複なし）。 |
| `scripts/gdrive_upload_verified.sh` | Mac/NAS | NAS上の暗号化バックアップとマニフェストをGoogle Driveへストリームアップロード。アップロード中一時オブジェクトのサイズ/MD5検証、`rclone cat` による全量読み戻しとSHA256往復検証、確定名へのmoveto。成功結果は標準出力のJSONで返す。単体実行ではレシートファイルを作らない。失敗時は一時物のみ削除し既存保護。 |
| `scripts/nas_archive.py` | Macから実行 | `config/storage-location.json` を検証し、SSH経由でNASコンテナ内の `archive.py` コマンドを実行。エクスポート生成物（GUI HTMLや分析XLSX）を安全にローカルへ取得。未移行時やマーカー不正時は即時エラー。 |
| `scripts/start.py` / `Start.command` | Macから実行 | 起動時に `config/storage-location.json` のNAS markerを検証し、正常時は `scripts/nas_archive.py gui` へルーティング。MacローカルでのNode/npm/loginを不要化。マーカー不正時は拒否しlocalフォールバック禁止。 |
| `scripts/nas_backup_cycle.py` | NAS内（定期実行） | 排他ロック取得、バックアップ作成、成果物整合性検証、rcloneによるGDriveへのイミュータブルアップロード、全量往復検証、確定レシート（`cloud-receipts/`）の生成を一括調整するコーディネーター。単一暗号化ファイル（単一cipher）直接経路であり、chunk分割自動化は未実装（connector手動chunk経路と同一処理ではない）。 |
| `scripts/sync_nas.py` / `sync_gdrive.py` | Macから実行 | `exports/` 等の軽量ファイルを双方向同期。3way baseline実装・手動往復検証済み（片側更新は通常更新、両側独立編集はmtime採用＋旧版退避、共通ロック、NAS/GDrive state同期除外）。定期運転はOSファイルアクセス許可確認待ち。 |
| `scripts/install_file_sync_service.py` | Macから実行 | 軽量ファイル同期LaunchAgentサービス定義の生成（`--print`）および登録（`--install`）。コード検査済み・登録済み。OSファイルアクセス許可確認待ちで自動実行は未成功。 |
| `scripts/backup_chunks.py` | Mac/NAS | 暗号化暗号文を既定64MiB（最大256MiB）のチャンクに分割（`split`）およびアトミック再結合（`join`）。暗号文全体SHA256、各partのSHA256、元暗号マニフェストの参照整合性を二重検証。既存出力先の上書き拒否。 |
| `scripts/verify_cloud_chunks.py` | Mac/NAS | クラウド上のチャンク分割バックアップを `rclone cat` でストリーム読取し、暗号文結合・GPG復号・zstd伸長をパイプライン処理して暗号文SHA256および展開後rawバイト数・SHA256を元マニフェストと照合。ローカルに暗号文や平文ファイルを作らず完全性検証を実施（新規quick_checkは行わず元snapshotの証拠と照合）。 |

---

## 3. 実測境界と現在のステータス（2026-09-27 07:34 JST実測現在）

運用手順の実施にあたり、確認済みの事実と未完了の事項を厳密に区別する。過去の経緯・実績は経過記録として明記し、最新の実測状況と峻別する。

### 3.1 達成済みの実測事実（最新および経過実績）
- **NAS正本コレクターの安定稼働とSQL高速化（2026-09-27 12:34 JST更新）**:
  - NAS正本コレクターはイメージ `ddde94a6a8a8` で稼働中。販売ギア・リプレイ詳細の正常null再試行に上限24時間の待機を追加し、反映後の全7履歴更新と認証正常を確認。旧コンテナと変更前の整合性検査済みDBを保持。
  - Macローカルのコレクター（LaunchAgent）は無効化（disabled）済みであり、未登録を確認。二重コレクターは存在せず、NASが唯一の正本となっている。
  - 全7履歴current、認証正常、595試合・詳細待ち0件・取得不能詳細10件、画像4970件を実測。
  - 状態取得SQLにおいて、肥大化していたentitiesテーブルに対する細い `response_id` 索引（状態COUNT索引）を追加したことで、COUNT処理は約0.005秒に短縮され、`status` 全体の応答時間が数分から約1.6秒へ大幅に高速化。
  - Mac側の `Start.command`（`scripts/start.py`）は、`config/storage-location.json` のNASマーカー検証により自動的に `scripts/nas_archive.py gui` へルーティングされ、MacローカルでのNode/npm依存関係および本人ログインを不要化している。
- **日次バックアップDocker schedulerの定期自動実行と完走成功（経過と実績）**:
  - 【初回手動サイクル経過】2026-09-26 22:40にスクリプト手動実行による初回日次バックアップサイクル（NASローカル復元quick_check、クラウドcipher全量readback SHA-256一致）が完走・成功しレシートを生成（手動実行による初回サイクルの成功記録であり、定期実行の成功とは区別して整理）。
  - 【定期Docker scheduler設置】NASホストの一般ユーザーにcrontab実行権限がない制限を回避するため、Dockerコンテナによるスケジューラー方式（UID 1000:10、`restart: unless-stopped`、毎朝4:00 Asia/Tokyo、失敗時15分retry、同日成功後の重複実行なし）を実設置。
  - 【定期自動実行の初完走成功】2026-09-27 04:00に定期Docker schedulerが自動起動し、05:16に成功（クラウド全量cipher readback SHA一致、scheduler `last_success_due_day=2026-09-27`）。
  - 【最新世代のクラウド全量復号照合】さらに2026-09-27 07時台に、この最新daily世代（raw 26,697,261,056 bytes）をGoogle Driveから全量復号し、原点とraw SHA-256一致、マニフェスト（`quick_check: ok`）の原rawとbyte完全一致を確認。NASローカルでの平文復元およびquick_checkもbackup cycle内で実行済み。
- **移行静止点（2026-09-26 03:19点）のクラウド保全・全量復号検証完了**:
  - 移行静止点（2026-09-26 03:19時点のDB）: raw 26,412,634,112 bytes, SHA-256 `069290e0ae6f0982ac8dd0db67df31f53a015246d110970041a2a3f4ef29c6e5`。
  - クラウド上の全37 cloud partsからストリーム全量復号を実施し、rawバイト数およびSHA-256ハッシュが原点と完全一致することを確認。
  - 元暗号マニフェストおよびチャンクマニフェストの両manifestについても、クラウドからの全量読み戻し（full readback）ハッシュ一致を確認（status: complete）。
  - private証拠: `operations-20260926/migration-cloud-complete.json`。
- **過去全世代（旧22GB・9/25 11:59・旧432MB）のNAS保全およびクラウド全量raw一致確認**:
  - 旧22GB版スナップショット（全27片、22,264,725,504 bytes）: NAS保全およびクラウド全量復号raw一致確認済み（status: complete）。
  - 2026-09-25 11:59静止点スナップショット（全36片、26,287,431,680 bytes）: NAS保全（別名保持）およびクラウド全量復号raw一致確認済み（status: complete）。
  - 旧432MB版（432,381,952 bytes, SHA-256 `aff52aa81853165af8cd97e3e12fb805ca578df04595f23473e3b7a82ba48afa`）: NAS保全およびクラウドからの全量raw復号一致を確認。
  - NASおよびクラウド上の旧版暗号スナップショットはすべて破棄せず安全に保持。
- **保全済みローカル原本DBおよび作業用複製の削除完了（2026-09-27 07:33実測）**:
  - 移行静止点、旧22GB、9/25 11:59、旧432MB、最新daily静止点の全世代について、NAS保全証拠およびクラウド全量復号証拠を確認。
  - 削除直前の厳格な安全事前検査:
    1. Macコレクターがdisabled（未登録）であり二重コレクターが存在しないことの確認
    2. `config/storage-location.json` のNAS markerが存在することの確認
    3. NAS側保全物のファイルサイズ確認
    4. ローカルhash検証時からinode、mtime、sizeが一切変更されていないことの再確認
    5. WALファイルが存在しないことの確認
    6. writer lock下での作業であることの確認
  - 上記事前検査をすべて満たした上で、許可済みのローカル旧DB 4本＋暗号作業用複製65個（論理合計 83,729,271,876 bytes）を安全に削除完了。
  - Macローカルディスクの実空き容量が 3,237,855,232 bytes（約3.0GiB）から 38,921,396,224 bytes（約36.2GiB）へ大幅に回復。
  - 元DB配置場所には所在移転を示す `relocated.json` を保存。
  - NASおよびクラウド上の旧版データはすべて保持。
  - private証拠: `operations-20260926/local-cleanup-complete.json` および各 `cloud-complete.json`。
- **Google Drive書込みOAuth認可とrclone設定（2026-09-26実測）**:
  - 2026-09-26 21:19にwrite OAuth（Google Drive書込み権限スコープ）の認可を完了。専用の書き込み設定（`rclone-write.conf`）をNAS secrets配下へ権限600で配置済み。秘密情報は記録・公開しない。
- **Google Drive connector二段上限の実測と64MiBチャンク分割の実証（過去事実）**:
  - Google Drive connectorにおいて、転送開始前の上限（512MiB）および後段HTTP 413上限（100MiB）の二段上限が実測された。
  - 二段上限回避のための既定64MiB part分割（`scripts/backup_chunks.py`）およびストリーム復号・完全性往復検証（`scripts/verify_cloud_chunks.py`）の実装と単体・結合検証が完了している。

### 3.2 未完了項目と現在の制約事項（未着手・検証中タスク）
- **軽量ファイル同期の定期運転は未稼働（導入検証中）**:
  - `sync_nas.py` および `sync_gdrive.py` による軽量ファイル（`exports/` 等）の定期同期は、3way baseline実装・手動往復検証を完了。LaunchAgentを登録済みだが、OSファイルアクセス許可の確認待ちで定常運転には入っていない。
  - 検証済みの仕様:
    - 片側だけの更新は通常更新としてアトミックに同期
    - 両側独立編集（競合）はmtime採用＋旧版退避（`.sync-conflicts/`）
    - 共通排他ロック（lock）の適用
    - NASおよびGoogle Driveの同期状態ファイル（stateファイル）は同期対象から除外
  - `scripts/install_file_sync_service.py`（`--print`/`--install`）のコード検査と実登録は完了。登録時ログは `~/Library/Logs/ikaring-archive-file-sync/` にあり、OS許可後に終了コード0と転送結果を確認する。
- **GitHub暗号化DB保管役割の保留（未回答）**:
  - GitHubリポジトリへ暗号化DB自体を保管する役割を持たせるか、それとも公開ソースコードと復元手順のみとするかについては、ユーザー未回答のまま保留中である。
  - 既存公開リポジトリはソースコードと復元手順のみを保持し、実データや暗号化DBは含めない。
- **将来の統合GUIおよび追加承認済み開発タスク**:
  - 統合GUI等の既存承認済みタスクは未着手として保持。

> [!WARNING]
> **重要な安全注意事項**:
> - 軽量ファイル同期は手動往復成功・サービス登録済みだが、自動実行はOSファイルアクセス許可確認待ち。登録だけを運転成功とは扱わない。
> - GitHubへの暗号化DB保管役割は未回答であり、公開リポジトリへ実データや暗号化DBを含めてはならない。
> - 稼働中DBのネットワーク直接コピー禁止、暗号鍵の分離保全、およびアトミック置換手順等の安全原則を厳守する。

---

## 4. 運用手順

### 4.1 【参考手順】Mac正本から初回移行・再同期時の手順（初回切替用）

> [!NOTE]
> **運用状態との分離**: 現在はNASコレクターが正本として安定稼働しており、Mac側コレクターは無効化（disabled）、Macローカル原本DBは保全確認後に安全に削除完了している（詳細は3節参照）。
> 本節の手順は、今後Mac正本環境から初回移行を行う場合や、万一の緊急切戻し後に再度NASへ正本切替・同期を行う場合にのみ使用する参考手順であり、現在の定常稼働状態を示すものではない。

1. **Mac側コレクターの停止**:
   二重取得および転送中のDB更新を防ぐため、MacのLaunchAgentを停止する。
   ```bash
   launchctl bootout gui/$(id -u) ~/Library/LaunchAgents/local.ikaring3.archive.plist
   ```
2. **MacローカルDBの静止点検証**:
   書込みプロセスをすべて停止し、その排他状態を転送完了まで維持する。WALを `TRUNCATE` checkpoint して `busy=0` とWALが空であることを確認する。この条件を満たす場合だけDB本体単独のコピーを許可する。満たさなければDB本体だけをコピーせず、原因を解消するかSQLiteオンラインバックアップAPIで静止点を作る。
   ```bash
   DB="$HOME/Documents/イカリング3アーカイブ/database/archive.sqlite3"
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
   停止状態の最新DBをNASの作業ディレクトリへ安全にストリーム転送し、NAS側でSHA256ハッシュ検証および `quick_check` を実施する。
4. **NAS側正規配置への切替**:
   NAS側の既存DBを退避した上で、検証済み最新DBを `/home/Natsuki/ikaring-archive/database/archive.sqlite3` として配置する。

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
   - 再試行および重複防止: 失敗時は15分間隔で再試行（retry）、同日成功後の重複実行なし（`last_success_due_day` による整合性管理）。
   - 実行処理: 常駐backupコンテナ内の `scripts/nas_backup_scheduler.py` が `scripts/nas_backup_cycle.py` を直接実行する。Docker socketをマウントせず、コンテナ内から別コンテナを起動しない。`scripts/run_nas_daily_backup.sh` はNASホストから単発実行する補助手段である。

### 4.3 移行マーカーの配置とクライアント運用切替
NAS側のコンテナ収集が安定して稼働したことを実測した後、Astraが移行マーカーを作成する。

1. **マーカーファイルの作成** (`~/Documents/イカリング3アーカイブ/config/storage-location.json`):
   ```json
   {
     "schema_version": 1,
     "backend": "nas",
     "ssh_host": "nas",
     "container": "ikaring-archive",
     "database": "/data/database/archive.sqlite3"
   }
   ```
2. **Macクライアント操作の切り替えとStart.commandのルーティング**:
   - `Start.command`（`scripts/start.py`）を実行すると、Node.js/npmの検査や依存関係インストール、本人ログイン処理を行う前に `config/storage-location.json` を検証する。
   - 正常なNASマーカーが確認された場合は、自動的に `scripts/nas_archive.py gui` が呼び出され、MacローカルでのNode/npm/loginを一切不要としてNAS側で生成された最新GUI HTMLをブラウザで開く。マーカー不正時は即座に処理を拒否し、ローカルへの自動フォールバックは行わない。
   - CLI操作を行う場合は `scripts/nas_archive.py` を使用する。
     ```bash
     python3 scripts/nas_archive.py status
     python3 scripts/nas_archive.py gui
     python3 scripts/nas_archive.py export-xlsx
     ```
   - Macローカルの `archive.py` 直接実行は安全機構（`NAS_STORAGE_ACTIVE`）によりブロックされることを確認する。

### 4.4 バックアップおよびクラウド保全運用（定期単一cipher経路とconnector手動chunk分割経路）
クラウド保全には、rcloneによる定期単一cipher直接アップロード経路（`nas_backup_cycle.py`）と、Google Drive connector二段上限を回避するためのconnector手動chunk分割経路（`backup_chunks.py` / `verify_cloud_chunks.py`）の2つの運用経路が存在する。定期 `nas_backup_cycle.py` は単一cipher直接経路でありchunk自動化は未実装であるため、手動chunk経路と同一処理だと誤解してはならない。

#### 4.4.1 定期 nas_backup_cycle.py による単一cipher直接保全経路
NAS内で定期的に無人実行されるバックアップサイクルであり、以下の処理を一貫して実施する。
2026-09-26 22:40の手動初回完走（レシート生成）を経て、2026-09-27 04:00〜05:16にDocker schedulerによる定期自動実行の初完走・成功（cloud全量cipher readback SHA一致）を実測。さらに07時台に最新daily世代（raw 26,697,261,056 bytes）のクラウドからの全量復号rawバイト数・SHA-256一致、およびマニフェスト（`quick_check: ok`）との完全一致を確認している。

1. `scripts/nas_create_verified_backup.sh` を呼び出し、オンラインバックアップ作成、空き容量事前検査（4倍+1GiB）、zstd+AES256暗号化、復号ハッシュ照合、`quick_check` 検査を経て、単一暗号化ファイル（`*.zst.gpg`）および元暗号マニフェスト（`*.manifest.json`）を生成する。
2. 生成された単一暗号化ファイルとマニフェストを、rcloneを用いてGoogle Driveへストリームアップロードする。
3. `rclone cat` による全量読み戻しとSHA-256往復検証を実施する。
4. 往復検証の成功確認後、確定レシート（`cloud-receipts/`）をアトミックに記録する。
※本スクリプトは単一cipherファイルを直接扱う経路であり、chunk分割処理は含まれない。

#### 4.4.2 connector手動chunk分割保全経路（64MiB chunk分割・ストリーム往復検証）
Google Drive connectorの二段上限（512MiB/100MiB）を回避するため、専用スクリプトを用いて手動で実行する保全手順である。

1. **スナップショット作成と暗号化**:
   `scripts/nas_create_verified_backup.sh` を呼び出し、オンラインバックアップの作成、zstd+AES256暗号化、復号ハッシュ照合、`quick_check` 検査を経て、単一暗号化ファイルおよび元暗号マニフェストファイル（`*.manifest.json`）を生成する。
2. **チャンク分割（split）**:
   Google Drive connectorの二段上限（512MiB/100MiB）を回避するため、`scripts/backup_chunks.py split` により暗号文を既定64MiBのpartファイル群に分割する。
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
   全partのアップロード完了後、確定前に `scripts/verify_cloud_chunks.py` を実行してクラウド上の全partをストリーム読取・復号し、完全性を検証する。
   ```bash
   # シェルリダイレクト解釈を防止するためプレースホルダーは引用符で囲む
   python3 scripts/verify_cloud_chunks.py \
     --remote "gdrive_origin:イカリング3アーカイブ/backups" \
     --manifest "/path/to/chunks_bundle/<cipher_name>.chunks.json" \
     --passphrase-file /path/to/passphrase_file
   ```
   - `rclone cat` で各partをストリーム読取し、暗号文全体のSHA256および復号・伸長後のrawバイト数・raw SHA256を元暗号マニフェストと照合する。
   - `verify_cloud_chunks.py` は新規のSQLite `quick_check` は実行しない（元静止点スナップショット作成時の `quick_check=ok` と、復号rawバイト列およびSHA256ハッシュの完全一致が完全性の証拠となるためである）。
5. **2マニフェストのアップロードと読み戻しハッシュ記録**:
   全量ストリーム復号検証が成功した後にのみ、元暗号マニフェストとチャンクマニフェストの2ファイルをクラウドへアップロードし、読み戻しハッシュ一致を記録する。

### 4.5 チャンク分割バックアップからの障害復元（リストア）手順
不測の事態によりクラウド上のチャンク分割バックアップからデータを復元する必要が生じた場合の安全手順を以下に示す。

#### 4.5.1 手順の全体要約と安全原則
- **検証中のコレクター稼働維持**: チャンクのダウンロード、結合、復号・伸長、ハッシュ照合、`quick_check` などの復元検証作業中は、コレクターを停止する必要はない。独立した安全な作業ディレクトリで全量検証を完遂させ、新DBの正常性が立証された「最後の正本切替直前」においてのみ対象コレクターを停止して排他状態を確保する。これにより無駄な収集停止と戦績流出を防ぐ。
- **空き容量の段階的事前検査**: リストア作業に必要な容量は平文rawだけでなく、取得part総量、結合後暗号文（joincipher）、平文raw、および十分な安全マージン（余裕）の合計である。作業を一括で確認するだけでなく、各工程（part取得前、join前、復号・伸長前、本番ステージング前）の直前に「現在の空き容量」と「その工程で必要となる追加容量」を逐次検査する。容量逼迫したローカルディスクや `/tmp` では作業を行わない。
- **安全なマニフェスト解析とファイル取得**: cipher basenameには `.zst.gpg` が含まれており、元暗号マニフェストは `<cipher_name>.manifest.json` ではない。まず正確なチャンクマニフェスト（`*.chunks.json`）を取得し、その中の `encrypted_manifest.basename` をPythonで安全basename検証（`safe_basename`）して元暗号マニフェスト名を特定する。全part名もチャンクマニフェストの `parts` 定義から取得する。シェルリダイレクト事故防止のため、コードプレースホルダーは必ず引用符で囲む。
- **同一ローカルディレクトリへの配置と既存上書き拒否**: 結合処理（`backup_chunks.py join`）を安全に行うため、チャンクマニフェスト、元暗号マニフェスト、全partファイルを同一作業ディレクトリへ配置する。`backup_chunks.py join` および復号処理は既存ファイルが存在する場合に上書きを拒否する。
- **安全なパイプライン実行**: `set -euo pipefail` および `set -C`（既存ファイル保護）を有効化し、暗号鍵はパーミッション mode 600 の既存鍵ファイル（`passphrase-file`）を使用する。秘密値（パスフレーズ）をコマンド引数や標準入力へ直書きすることは厳禁とする。
- **新規復元DBの完全性検査**: 展開後、rawバイト数、raw SHA-256ハッシュ、およびSQLite `PRAGMA quick_check` を検証する。読み取り専用接続には `?mode=ro&immutable=1` を使用し、接続直後に `PRAGMA cache_size = -65536;`（64MiBキャッシュ）を設定して検査を行う。
- **切替専用手順の遵守（安易な2本mvの厳禁）**:
  - 旧DBの `archive.sqlite3-wal` や `archive.sqlite3-shm`、`archive.sqlite3-journal` が残っている状態で新DBを配置すると、古いWAL/SHMが新DBと混在（bind）し重大なDB破損を引き起こす。また異ファイルシステム間の `mv` はアトミックではない。
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
   復元作業全体では「取得part総量（約2.5GB）＋結合後暗号文joincipher（約2.5GB）＋平文raw（約26GB超）＋安全余裕（5GB以上）」が必要となる。
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
   rclone copy "gdrive_origin:イカリング3アーカイブ/backups/$CHUNK_MANIFEST_NAME" "$WORK_DIR"

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
   rclone copy "gdrive_origin:イカリング3アーカイブ/backups" "$WORK_DIR" \
     --files-from-raw "$WORK_DIR/download-list.txt"
   ```
   > [!IMPORTANT]
   > `backup_chunks.py join` はチャンクマニフェストと同じ親ディレクトリ内に元暗号マニフェストおよび全partが存在することを前提としているため、必ず同一作業ディレクトリに配置する。

3. **チャンク結合前の容量検査（工程B前）と暗号文復元 (`backup_chunks.py join`)**:
   結合後暗号文（joincipher）を出力するための追加空き容量を検査した上で、`backup_chunks.py join` を実行する。
   ```bash
   CHUNK_MANIFEST="$WORK_DIR/$CHUNK_MANIFEST_NAME"

   # 工程B前検査: 結合後暗号文のサイズ＋安全余裕（約3GiB分）の空き容量があるか検査
   python3 - "$CHUNK_MANIFEST" "$WORK_DIR" <<'PY'
   import json, shutil, sys
   from pathlib import Path
   manifest = json.loads(Path(sys.argv[1]).read_text(encoding='utf-8'))
   cipher_bytes = manifest['ciphertext']['bytes']
   work_dir = Path(sys.argv[2])
   free_bytes = shutil.disk_usage(work_dir).free
   # 暗号文サイズ + 1GiB余裕
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
   元暗号マニフェストから平文データベースの容量（`raw_snapshot.bytes`）を読み取り、平文raw（約26GB超）＋安全余裕を展開できる空き容量があることを確認した上で、GPG復号とzstd伸長を実行する。
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

5. **平文DBの完全性検査（raw SHA-256 / raw bytes / SQLite quick_check）**:
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
   > - 旧DB本体のみを `mv` して新DBを配置すると、旧DBの `archive.sqlite3-wal` や `archive.sqlite3-shm`、`archive.sqlite3-journal` の残骸が本番ディレクトリに残り、新DBと混在（bind）して重大なDB破損を引き起こす。
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

   切替用スクリプトはまだ整備されていない。上記の排他・退避・復帰を満たす実施手順を確認してから作業する。個別ファイルのrenameは一括トランザクションではないため、途中中断時も旧DB一組の配置を確認するまで収集を再開しない。

7. **コレクターの再開と稼働確認**:
   正本置換が成功したことを確認した後、コレクタープロセスを再開し、正常にデータ収集が継続されることをログおよびステータスで確認する。
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
   復号・伸長先ファイルシステムにおいて、「平文rawサイズ（約26GB超）＋安全余裕」の空き容量があることを確認し、独立した作業ディレクトリを用意する。
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
