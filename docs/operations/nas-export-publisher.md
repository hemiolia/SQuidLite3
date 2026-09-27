# NAS 軽量生成物配信 (Export Publisher) 運用手順

## 1. 概要と設計

本手順は、NAS 上で生成された軽量生成物（`gui/index.html` および `分析.xlsx`）を、事前に認可されたクラウドストレージ（Google Drive 等の rclone リモート）へ定期配信するための Docker コンテナ運用方法を定めたものである。

### 設計原則
- **派生生成物のみの配信**: 配信対象は `exports/gui/index.html` と `exports/分析.xlsx` の2ファイルに限定される。HTML/XLSX は本人戦績を含む派生出力であるため、事前に認可された非公開保存先だけに配信する。一方で SQLite データベース正本や WAL、スプール、認証情報、収集プログラムなどは一切含まれず配信対象外（非対象）とする。
- **一方向配信**: NAS からリモートへの一方通行の配信（プッシュ）であり、双方向同期ではない。本スクリプトは collector には一切触れない。
- **不可逆操作の排除**: リモート上の既存ファイルを無条件に上書き破壊せず、競合検知時は UUID 隔離プレフィックス配下へ退避保全する。
- **最小権限と隔離**: コンテナは非 root ユーザー（`1000:10`）、read-only rootfs、全ケーパビリティ破棄（`cap-drop ALL`）、no-new-privileges、一時領域 tmpfs（`size=32m`）、外部ポート開放なし、Docker ソケット非公開で動作する。

## 2. ディレクトリ構造と必要権限

NAS のアーカイブ root ディレクトリ配下に、以下の構造を配置する。シンボリックリンクは拒否される。

```
<ROOT>/
├── exports/                      # ro マウント (生成物配置ディレクトリ)
│   ├── gui/index.html
│   └── 分析.xlsx
├── runtime/
│   └── export-publisher/        # ro マウント (配信スクリプト配置)
│       └── nas_publish_exports.py
├── state/
│   └── export-publisher/        # rw マウント (状態・排他ロックディレクトリ, パーミッション 0700)
│       ├── .nas-publish-state.json
│       └── .nas-publish.lock
└── secrets/
    └── export-publisher/        # rw マウント (rclone 設定ファイル配置, パーミッション 0700)
        └── rclone.conf          # パーミッション 0600 必須
```

### マウントと権限の要件
- **exports (ro)**: 読み取り専用。配信対象の2ファイルが含まれる。
- **runtime/export-publisher (ro)**: 読み取り専用。`nas_publish_exports.py` が配置されていること。
- **state/export-publisher (rw)**: 読み書き可能。配信前後のハッシュや状態を記録する `.nas-publish-state.json` およびプロセス排他用 `.nas-publish.lock` を保持する。ディレクトリが存在しない場合は起動スクリプトがパーミッション `0700` で作成可能。
- **secrets/export-publisher (rw)**: 読み書き可能。rclone が Google Drive のアクセストークンを自動更新して書き戻すため rw マウントとする。中に配置する `rclone.conf` はパーミッション `0600` であること（取得失敗時も含め fail-closed で検証）。
  - **重要**: `backupkey` や `nxapi` の認証情報など、他の秘密情報は決してこのディレクトリに配置せず、マウントもしない。
- **パス検査制約**: スクリプトの root 引数および各マウントパスについて、`/` に至る全祖先ディレクトリでシンボリックリンクが拒否される（`state` 作成前にも検査）。`/..` や `..` コンポーネントを含むパスも拒否される。
- **コンテナ実行ユーザー**: コンテナは `--user 1000:10` で起動するため、`state/export-publisher` および `secrets/export-publisher/rclone.conf` は UID 1000 から書き込み可能である必要がある。

## 3. 運用手順

### Step 1: 単発検証実行 (`--once`)

常駐デーモンを起動する前に、必ず `--once` オプションを用いて手動で単発検証を実行し、全量読戻し（readback）検証が正常に完了することを確認する。

```bash
/path/to/scripts/run_nas_export_publisher.sh \
  --root /path/to/nas/root \
  --image ikaring-archive-backup:current \
  --remote exports_remote:exports \
  --once
```

※上記コマンド例の `--root /path/to/nas/root`、`--image ikaring-archive-backup:current`、`--remote exports_remote:exports` は実行可能な構文例である。シェルリダイレクトとして壊れる山括弧（`<...>`）は使用しない。実際の環境に合わせて、NAS 上のアーカイブ root パス、作成済みのバックアップ用 Docker イメージ名、設定済みの rclone リモート名と相対プレフィックス（個人識別子等を含まない値）に適切に差し替えて実行する。`--remote` 引数は `NAME:PATH` 形式（先頭英数アンダースコア、name英数._-、path相対で..component拒否、制御文字・バックスラッシュ拒否）で検証される。

- `--once` 実行時はコンテナが `--rm` でフォアグラウンド実行され、終了時にコンテナは破棄される。
- 既存のデーモンコンテナ（`ikaring-archive-export-publisher`）が既に稼働中の場合は、排他制御と状態整合性のため起動が拒否される（コンテナ状態取得失敗時も fail-closed で拒否）。
- 実行結果は標準出力に JSON レシート形式で出力される。
- 成功時のレシート例:
  ```json
  {
    "status": "success",
    "files": {
      "gui/index.html": {
        "status": "uploaded",
        "sha256": "...",
        "bytes": 12345
      },
      "分析.xlsx": {
        "status": "uploaded",
        "sha256": "...",
        "bytes": 67890
      }
    },
    "counts": {
      "total": 2,
      "uploaded": 2,
      "skipped": 0,
      "conflicts": 0
    }
  }
  ```
- レシートの `status` が `"success"` であり、エラーカテゴリ（`CONFIG_ERROR`, `PATH_SAFETY_ERROR`, `REMOTE_ERROR` 等）が標準エラーに出ていないことを確認する。

### Step 2: 常駐デーモンの起動

単発検証が正常に成功したことを確認した後、`--once` なしで起動スクリプトを実行して常駐デーモンを開始する。

```bash
/path/to/scripts/run_nas_export_publisher.sh \
  --root /path/to/nas/root \
  --image ikaring-archive-backup:current \
  --remote exports_remote:exports
```

- デーモンコンテナ名: `ikaring-archive-export-publisher`
- 再起動ポリシー: `--restart unless-stopped`
- 内部動作: 各サイクルの実行終了後、300秒待機してから次のサイクルを実行するループが起動する（周期はサイクル終了後300秒）。
- 既存の同名コンテナが存在する場合、自動停止や自動削除は行わず、安全のためスクリプトはエラー終了する。

### Step 3: 次の自然サイクルの状態確認

デーモン起動後、コンテナログを確認して前回のサイクル終了から300秒後の次回サイクルの動作を監視する。

```bash
docker logs -f ikaring-archive-export-publisher
```

- 生成物に変更がない場合は、各ファイルについて転送がスキップされ、レシートの `status` が `"skipped"` となる。
- 収集側により `exports` 配下のファイルが更新された場合は、次のサイクルで自動的に検知されて `"uploaded"` となる。
- 万が一ネットワーク障害等で一時的に失敗した場合でも、サイクル終了後300秒の待機を経て自動的に次回再検査が行われる。ログには標準化された安全なエラーカテゴリ名のみが出力される。

### Step 4: デーモンの停止手順

デーモンの停止は、手動で以下のコマンドを実行する。

```bash
docker stop ikaring-archive-export-publisher
```

- コンテナ内の起動シェルは `SIGTERM` および `SIGINT` をトラップし、実行中の Python プロセスまたは待機中の sleep プロセスを適切に kill/wait して正常終了（graceful shutdown）する。
- 配信処理は readonly でマウントされた NAS 側の元データ（source）を変更・削除しないため、停止によって NAS 側の元データが失われることはない。ただし、停止のタイミングによってはリモート（クラウド）側への転送途中や 2 ファイルのうち片方のみ更新された状態となりうる。次回起動・サイクル時に再検査が行われ、状態の検証と同期が試行される。

## 4. 制限事項と安全設計 (Limitations)

1. **2ファイル間の不可分トランザクションなし (No two-file atomic transaction)**
   - `gui/index.html` と `分析.xlsx` は個別に rclone 経由でアップロードされる。クラウドストレージプロバイダは複数ファイルにまたがる不可分なアトミックトランザクションを提供していない。
   - 1ファイル目のアップロード成功後、2ファイル目のアップロード前や転送途中に障害や停止が発生した場合、リモート側は一時的に1ファイルのみ更新された状態となる。
   - 状態ファイル（`.nas-publish-state.json`）のコミットは両方のファイルが検証に成功した後にのみ行われるため、次回サイクル時に再検査が行われ、未コミットの状態から再同期が試行される。
2. **クラウド CAS なし (No cloud Compare-And-Swap)**
   - アップロード直前にリモート側のメタデータ確認（stat）およびハッシュ比較を行うが、これは厳密な Compare-And-Swap (CAS) ではない。
   - 事前確認から実際の rclone コピー完了までの間にリモート側が変更された場合、微小な競合窓（race condition window）が存在する。
3. **派生生成物のみの配信と保護 (Derivative artifacts only)**
   - 本機能は本人戦績を含む派生出力である HTML/XLSX のみを対象とする。そのため配信先は認可済みの非公開保存先に限定する。SQLite データベース正本、WAL、スプール、認証ファイル、収集プログラムなどは一切扱わず、配信対象外である。
4. **競合保全 (Conflict preservation)**
   - リモート側でファイルが想定外に変更（競合）されていた場合、上書き消去せず退避保全する。
   - NAS上の退避先: `<ROOT>/state/export-publisher/conflicts/<UUID>/<relativepath>`（パーミッション 0600）
   - リモート退避先: `<REMOTE_PREFIX>/.sync-conflicts/<UUID>/<relativepath>`（`rclone copyto --immutable`）
   - リモート退避先へのアップロード後に全量読戻し（readback: `rclone cat` による SHA-256）で内容一致を確認した後にのみ本来の配信先更新へ進む。事前確認後の同時編集については、上記のクラウドCAS制約が残る。
