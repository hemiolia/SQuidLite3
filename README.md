# SQuidLite3

SQuidLite3（旧称: イカリング3アーカイブ）は、スプラトゥーン3のアプリ「イカリング3」に出る対戦とバイトの記録を保存するツールである。任天堂とは無関係の非公式ツールである。

> **分析時の注意**：旧 `analysis_slice`（OMITS付き）と従来の `exports/分析.xlsx`、`exports/analysis.xlsx` は、原文・全参加者・全WAVEなどを省いた版なので**分析資料として参照禁止**です。旧Macファイル同期はこの2つの相対パスだけを対象外とし、既存ファイルを削除・移動しません。ほかのパスは従来の除外規則に従います。全情報のbaseline・差分は専用workerで公開し、検証と全量読み戻しの状態を [クラウドデータ案内](docs/operations/cloud-data-map.md) で確認してください。現行の lossless 世代は分割しても元SQLiteの全表・全列・全型値・元行ID・schema・外部キーに加え、応答原文・取得状態・参照関係を保持する方式です。

イカリング3に出る対戦とバイトを、応答の原文を落とさず SQLite に貯める。

分析するときはジャンルを混ぜない。ナワバリ、オープン、チャレンジ、イベマ、Xマッチ、フェス、プラベ、バイト、ビッグラン、バイトチームコンテストは別のビューである。プラベだけ人数で分ける。公開戦の人数差は回線落ちとして、そのジャンルに残す。

## いま動かしている人

Python 3.10 以上と Node.js 22 以上が入っている必要がある。

- macOS は `Start.command` をダブルクリックする
- Windows は `Start.bat` をダブルクリックする
- Linux は端末で `sh scripts/start.sh` を実行する

パスワードは任天堂の画面にだけ入れる。認証リンクはチャットや GitHub に貼らない。

保存先の既定は `~/Documents/イカリング3アーカイブ` である。ソースとは分けてある。

NASへの移行を設定済みの場合、同じ起動ファイルがNASで最新の画面を生成し、手元で開く。手元ではPythonとSSHクライアントを使い、収集はNASで継続する。状態確認は `python3 scripts/nas_archive.py status` を使う。旧分析表は欠落があるため利用できず、全情報の書き出し機能は準備中である。

ターミナルを知らない人向けの導入は、まだ作っていない。

## 認証

再ログインが要るのは、セッショントークンが期限切れか失効したときだけである。2026-09-22 の実測では有効期間は 730 日だった。bullet トークンは約 2 時間、Nintendo Switch Online の資格情報は 7200 秒、短いアクセストークンは 900 秒で、いずれもセッショントークンから更新する。

認証で止まった取得は成功にしない。macOS では通知する。

## 保存するランキングと同期状態

オープンは本人の対戦・バンカラパワー・参加表彰記録を保存し、イベントのランキングは保存済みの本人の参加回だけを取得する。Xランキングは終了シーズンを取得する。対象外と未確認を別の状態にし、参加記録が後から追加された回は取得対象へ戻す。

同期中も `status` と `audit` で履歴7経路の確認時刻を確認できる。生成HTMLは同期中と同期終了時に自動更新する。旧分析.xlsxの自動生成・更新は停止しており、全情報の書き出し機能は準備中である。空き容量が2GiB未満なら最新履歴と試合詳細を優先し、512MiB未満では保存事故を避けるため取得を止める。空きが回復すると次の同期で再開する。

NASでの収集構成、平文バックアップ、および復元方法については [docs/operations/nas-migration.md](docs/operations/nas-migration.md) を参照。

Macを経由せず、NASの生成HTMLを非公開クラウドへ配信する手順は [NAS軽量生成物配信](docs/operations/nas-export-publisher.md) を参照。旧分析.xlsxは配信しない。

保存した対戦・バイトを読み取るCLIは `python3 scripts/nas_archive.py records list --account ACCOUNT`。単件は `records get --account ACCOUNT --kind vs --match-key KEY`（バイトは `--kind coop`）で取得できる。詳細の元HTTP本文は `source.body_base64`、派生JSON文字列は `detail_json` に分けて返す。ローカルDBを使う環境では `python3 archive.py --db PATH records ...` を使う。

## 検証

`npm test` は公開サンプルと人工データだけを見る。本人の全記録を取り終えたことにはならない。

ライセンスは AGPL-3.0-or-later。

`assets/fonts/Splatoon2-Unified.otf` はNintendoの第三者資産であり、AGPLの対象外である。
詳細は `THIRD_PARTY_NOTICES.md` を参照。
