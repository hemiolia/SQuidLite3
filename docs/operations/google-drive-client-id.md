# 前田さん個人の Google Drive 連携の認証 ID（版 2・2026-10-05）

この手順は、前田さんの NAS から前田さんの Google Drive へ記録を送る設定のためだけのもの。SQuidLite3 のほかの利用者は、Google の API 登録を一切しない（保存先にパソコンの Google ドライブ同期フォルダを選ぶだけで済む形にする。`docs/design/SQuidLite3_製品形態.md`）。

版 1 には誤りがあった（隠しフォルダを保存先に指定した。現在の Google の画面で確かめずに手順 5 以降を書いた）。版 1 と前田さんの書き込みは `.history/opus-20261005/google-drive-client-id.v1-with-maeda-note.md`、原文は開発ログにある。

## もう済んでいること

- Google Cloud のプロジェクト作成、Google Drive API の有効化、OAuth クライアント（デスクトップ アプリ）の作成。クライアント ID とシークレットは `~/Developer/ikaring-archive/config/Google Drive API Keys` に保存済み（公開リポジトリの作業フォルダ内なので、Git に入らない設定を入れた）。

## なぜまだ使えないか

いまのクライアントは「テスト中」なので、認証が 7 日で切れる。「本番」にするには、Google の決まりで、ホームページ・プライバシーポリシー・利用規約のリンクと、承認済みドメインが要る（Google 公式: 外部向けの本番アプリにはこれらのリンクが必須）。そのページを sql3.ink に置く（文案は `site/`、私が用意済み）。

## 残りの手順

1. 私が sql3.ink にページを公開する。前田さんにお願いするのは、運営者名と問い合わせ先を決めることと、Mac で一度だけ `npx wrangler login` を実行して Cloudflare にログインすることだけ。
2. 公開できたら、Google Cloud の「Google Auth Platform」の「ブランディング」で、次を入れる。画面の項目名が違っていたら、その画面を見せてもらえれば、その場で合わせる。
   - 承認済みドメイン: `sql3.ink`（先に入れる。Google の案内でも、リンクより先に入れることになっている）
   - アプリケーションのホームページ: `https://sql3.ink/`
   - プライバシーポリシーのリンク: `https://sql3.ink/privacy.html`
   - 利用規約のリンク: `https://sql3.ink/terms.html`
3. ドメインの所有確認を求められたら、Google Search Console で sql3.ink を確認する。sql3.ink は Cloudflare で管理しているので、Search Console の案内に従えば Cloudflare 経由で自動で済むことが多い。
4. 「対象」で「アプリを公開」を押して本番にする。
5. 最後に、NAS の rclone 設定にこのクライアントを入れ、ブラウザで一度だけ認証する。手順はそのときに案内する。
