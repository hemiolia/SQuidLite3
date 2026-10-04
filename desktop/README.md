# SQuidLite3 デスクトップ窓（試作）

Electron の窓の中に、`scripts/desktop_app.py` の記録画面を出す。窓を開くと Python を子プロセスで起動し（`--no-open --port 0`）、標準出力の `http://127.0.0.1:<port>/#token=...` を窓に読み込む。窓を閉じると子プロセスを止める。起動に失敗した場合は「記録の画面を開けませんでした。」と理由を窓に出し、詳細を `~/Library/Logs/squidlite3-desktop/squidlite3.log` に書く。

パッケージングは @electron/packager を採った。出力が .app だけで設定が要らず、試作では署名・dmg・自動更新を扱わないため（electron-builder は第二段で配布形式を決めるときに再検討）。

## 開発時の起動

    cd desktop
    npm install
    npx electron .

開発時は PATH の `python3` と、リポジトリの `scripts/desktop_app.py` を使う。この Mac では NAS マーカーがあるため、記録画面は NAS から読む。

検証用に `SQUIDLITE3_PROOF=<出力.jpg>`（と待ち時間 `SQUIDLITE3_PROOF_WAIT` ミリ秒）を付けると、表示後に窓を画像保存して終了する。

## 配布物（.app）の作り方（macOS arm64）

    npm run pack       # Python 同梱なし。実行時は PATH の python3 を使う
    npm run pack:py    # out/pbs/python（下記）を resources/python として同梱

`tools/stage.js` が `scripts/*.py`、`archive.py`、`src`、`assets/app`、`assets/fonts` を `out/stage/repo` に集め、`resources/repo` として同梱する。出力は `dist/SQuidLite3-darwin-arm64/SQuidLite3.app`。

## python-build-standalone の同梱

実測済み（2026-10-05）。

    mkdir -p out/pbs && cd out/pbs
    curl -LO "https://github.com/astral-sh/python-build-standalone/releases/download/20261003/cpython-3.14.8+20261003-aarch64-apple-darwin-install_only_stripped.tar.gz"
    tar xzf cpython-*.tar.gz        # python/ ができる
    cd ../.. && npm run pack:py

`main.js` は `resources/python/bin/python3` があればそれを、無ければ PATH の `python3` を使う。

## 実測（2026-10-05、Mac arm64）

- 開発起動: 窓が開き、NAS の収集状態の画面が表示された（`proof/dev.jpg`）。
- Python 同梱の .app: PATH を `/usr/bin:/bin` に絞っても、同梱 Python で同じ画面が表示された（`proof/packaged.jpg`）。
- Python が見つからない場合の失敗表示: `proof/fail.jpg`。
- サイズ: Python なし 約 290MB、Python 同梱 約 359MB（Python は約 69MB）。zip 圧縮後（Python 同梱）は約 148MB（155,211,773 バイト）。

## 未実測・未対応

- Windows 版（同じ構成で python-build-standalone の Windows 版が要る）。
- コード署名・公証（無署名のため、他の Mac では初回に Gatekeeper の許可が要る）。
- nxapi（Node 側の取得）の同梱。今回は記録画面の閲覧のみ。
- NAS の無い利用者向けの保存先選択と初回案内。
