# agri_db

沖縄協同青果の市況PDFを定期取得し、Supabase(Postgres)へ蓄積する最小構成です。

## 1. 事前準備

- GitHubアカウント
- Supabaseアカウント
- ローカルPCに `git` と `python 3.12+`

## 2. Supabase設定

1. Supabaseで新規Projectを作成
2. SQL Editorで `sql/init.sql` の内容を実行
3. Project Settings > Database で接続文字列を取得
4. 接続文字列を `SUPABASE_DB_URL` として控える

`SUPABASE_DB_URL` 例:
`postgresql://postgres.<project-ref>:<password>@<host>:5432/postgres?sslmode=require`

## 3. ローカル実行

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

`.env.example` を参考に環境変数を設定し、実行:

```bash
set SUPABASE_DB_URL=postgresql://...
python -m src.agri_db.main
```

## 4. GitHubリポジトリ作成

```bash
git init
git add .
git commit -m "initial ingest pipeline"
git branch -M main
git remote add origin <YOUR_REPO_URL>
git push -u origin main
```

## 5. GitHub Secrets設定

GitHubリポジトリ > Settings > Secrets and variables > Actions > New repository secret:

- `SUPABASE_DB_URL`: Supabaseの接続文字列

## 6. GitHub Actions実行

- `.github/workflows/daily_ingest.yml` は以下を設定済み
  - 手動実行 `workflow_dispatch`
  - 毎日JST 14:30 定期実行
  - 予備実行 JST 16:00

最初は Actions タブから手動実行し、`source_files` にデータが入ることを確認してください。

## 7. 現在の保存内容

- PDFメタ情報（URL、日付、サイズ、ハッシュ）
- PDFから抽出した全文テキスト（`raw_text`）
- 取得失敗時のエラー（`parse_status`, `error_message`）

品目別の構造化パースは、`raw_text` を元に次のステップで追加できます。
