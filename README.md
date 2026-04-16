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
- テーブルキャプション署名（`caption_signature`）
- フォーマット変化アラート（`format_alert`）
- 取得失敗時のエラー（`parse_status`, `error_message`）
- 行単位の構造化データ（`market_rows`）
  - `item_name`
  - `high_price`, `avg_price`, `low_price`, `quantity`
  - `raw_line`（元行を保持）
  - `parse_confidence`（暫定的な抽出信頼度）

## 8. 構造化抽出の現仕様

- `pdfplumber.find_tables()` で表抽出
- 欠損セルを `PyMuPDF` の単語座標で補完
- 1列目（品目名）は PyMuPDF の左列テキストから復元
- 各列は先頭数値を採用して `quantity / high_price / avg_price / low_price` に正規化
- 見出しキャプションが前回と変わった場合は `format_alert=true` と GitHub Actions warning を出力
- `format_alert=true` が1件でも出た実行は `::error::` を出してジョブ失敗
  - GitHub の標準通知（失敗通知メール/通知設定）で検知可能

PDFレイアウトの差異により誤抽出が混ざる可能性があるため、`raw_line` を見ながらルール改善する運用を想定しています。

## 9. ダッシュボード（ECharts）を表示する

`dashboard/` に1画面ダッシュボードを追加しています。

- 上段: KPIカード（本日の平均価格変化率の上位下位各3品目、前日比）
- 中段左: 品目別価格推移
- 中段右: 入荷量×価格（2軸）
- 下段: 品目間価格変動率の相関分析（上位/下位相関ペア Top20 + 選択品目との相関ランキング）
  - 相関計算は `log return` -> 日次横断中央値控除 -> 1%/99% winsorize -> MAD標準化 を適用
  - 表示は `相関スコア（-1〜+1）= 0.6×Spearman + 0.4×Pearson` に統合
  - 相関スコア列は `-0.19〜0.19` を中央帯とした9区画で色分け

### 9-1. Supabaseにダッシュボード用ビューを作成

- `sql/init.sql` を再実行する  
  または `python -m src.agri_db.main` を1回実行（`ensure_schema`が同じビューを作成）

作成されるビュー:

- `market_daily_item_stats`
  - `sale_date`
  - `item_name`
  - `quantity`
  - `high_price`
  - `avg_price`
  - `low_price`

### 9-2. anonでビューを読めるようにする

Supabase SQL Editorで以下を実行:

```sql
grant select on table market_daily_item_stats to anon, authenticated;
```

### 9-3. フロント設定

`dashboard/config.js` の値を更新:

- `supabaseUrl`: `https://<project-ref>.supabase.co`
- `supabaseAnonKey`: Supabaseの anon public key

### 9-4. GitHub Pagesで公開

`.github/workflows/deploy_dashboard.yml` を追加済みです。

1. GitHubの `Settings > Pages` で Build and deployment を `GitHub Actions` に設定
2. `main` へpush
3. Actions の `Deploy Dashboard (GitHub Pages)` 完了後、Pages URLで表示
