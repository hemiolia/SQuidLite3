# Google Drive 専用の認証 ID を作る（前田さんの操作）

## なぜ要るか

NAS から Google Drive へ記録を送る仕組み（rclone）は、いま rclone が全利用者で共用している Google の認証 ID を使っている。rclone の案内によると、この共用 ID は 2026 年中に使えなくなる。使えなくなると、Drive への配信とバックアップがどちらも止まる。前田さん専用の ID を作れば止まらない。

所要時間はおよそ 10 分。Google アカウントは、Drive のアカウント（originnatsumikanf@gmail.com）でも別のアカウントでもよい。

## 手順

1. ブラウザで Google API Console（https://console.developers.google.com/ ）を開く。
2. 画面上部のプロジェクト選択から「新しいプロジェクト」を作る。名前は `SQuidLite3` などでよい。
3. 「API とサービスを有効にする」で `Google Drive API` を検索し、「有効にする」を押す。
4. 左の一覧の「認証情報」を開く（「認証情報を作成」の案内ではなく、左の一覧の項目）。
5. 「同意画面を構成」を押し、次を入れて保存する。
   - アプリ名: `SQuidLite3`
   - サポートのメール: 自分のメールアドレス
   - 対象: 「外部」
   - スコープ: `https://www.googleapis.com/auth/drive` と `https://www.googleapis.com/auth/drive.metadata.readonly` と `https://www.googleapis.com/auth/docs`
   - テストユーザー: Drive のアカウント（originnatsumikanf@gmail.com）
6. 「OAuth クライアントを作成」を押し、種類は「デスクトップ アプリ」を選ぶ。表示される「クライアント ID」と「クライアント シークレット」を控える。
7. 同意画面の公開状態を「アプリを公開」で本番にする。**テスト中のままだと認証が 1 週間で切れる。** 本番にすると、最初の認証のときに「確認されていないアプリ」という警告が出るが、自分で作ったアプリなので問題ない。個人用（100 人未満）は Google の審査は要らない。

## 終わったら

クライアント ID とクライアント シークレットを、チャットに貼らずに、Mac の次のファイルへ 2 行で保存して知らせてほしい（1 行目が ID、2 行目がシークレット）。

```
~/Library/Application Support/ikaring-archive/client/config/google-drive-client.txt
```

そのあと、NAS の rclone 設定（配信用とバックアップ用）にこの ID を入れ、ブラウザでの認証を一度だけお願いする。認証の手順はそのときに案内する。

出典: rclone 公式「Making your own client_id」https://rclone.org/drive/#making-your-own-client-id （2026-10-05 参照）
