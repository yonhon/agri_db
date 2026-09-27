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

- 価格はすべて市況PDFの「中値」列（販売価格中値、DB列名は `avg_price`）を使用
- 表示期間: 7日 / 30日 / 90日 / 全期間 から選択（KPIと前週比マップは期間選択と連動せず、常に最新日基準）
- 上段: KPIカード（本日の販売価格中値の割高・割安 上位各3品目）
  - 本日の中値を、同じ品目の過去14日（本日を含まない）の中値の中央値と比べた乖離率
  - 過去14日の取引が7日以上あり、本日に入荷のある品目が対象
  - カードには本日の中値、14日中央値、前日比も表示
  - 日数などは `dashboard/config.js` の `kpiBaselineDays` / `kpiMinBaselineDays` で変更可
- 中段左: 品目別価格推移
  - 表示品目は選択中の品目をチップ表示（×で外す）。「＋品目を追加」から検索付きのチェックリストで追加（ひらがな入力でもカタカナ品目に一致）
  - 最低1品目は選択状態を維持
- 品目別価格推移・入荷量×価格（2軸）の選択候補はあいうえお順
  - 「その他◯◯」は通常の品目の後ろにまとめ、「その他」を除いた読みのあいうえお順で並べる
  - 漢字の品目名は `dashboard/app.js` の `KANJI_READINGS`（読みの対応表）で読みに変換して並べる。表にない漢字を含む品目は末尾に並ぶので、見つけたら追記する
  - 初期表示の品目は従来どおり最新日の入荷量が多い順
- 中段右: 入荷量×販売価格中値（2軸）
- 下段: 入荷量 × 販売価格中値 前週比マップ（散布図）
  - 直近7日と前の7日を比べ、横軸に入荷量の変化率、縦軸に中値（入荷量加重平均）の変化率をとる
  - 左上（品薄で値上がり）と右下（出回り増で値下がり）を色付け。点の大きさは14日間の入荷量
  - 14日間の入荷量上位20品目に加え、2軸グラフ・品目別価格推移で選択中の品目は順位に関係なく必ず表示
    - 2軸グラフの品目はオレンジ、品目別価格推移の品目は線と同じ色、それ以外は灰色。選択中の品目は常に名前を表示し、それ以外は変化の大きい6品目のみ名前を表示
    - 選択中でも、どちらかの7日間に入荷がない品目は表示できないため、その旨をグラフ上部に表示
  - 点をタップすると2軸グラフの対象品目が切り替わる
  - 品目数などは `dashboard/config.js` の `weeklyMapTopItems` / `weeklyMapLabelItems` で変更可
- 下段: 品目間価格変動率の相関分析（上位/下位相関ペア Top20 + 選択品目との相関ランキング）
  - 現在は非表示（`index.html` の `#corrSection` の `hidden` 属性を外すと再表示され、計算も再開されます）
  - 相関計算は `log return` -> 日次横断中央値控除 -> 1%/99% winsorize -> MAD標準化 を適用
  - 表示は `相関スコア（-1〜+1）= 0.6×Spearman + 0.4×Pearson` に統合
  - 相関スコア列は `-0.19〜0.19` を中央帯とした9区画で色分け
- 最下段: 販売価格中値ロリポップ（直近7日の中値を入荷量で加重平均、高い順）

補足:

- 品目の選択状態と表示期間は `localStorage` に保存され、次回アクセス時にも復元されます。
  - 対象: `品目別価格推移`、`入荷量×価格（2軸）`、`相関分析の対象品目`
- タイトル下の「直近一週間のアップデート」は `dashboard/updates.json` を参照します。
  - `posted_at`（`YYYY-MM-DD`）が当日から7日以内の項目のみ表示
  - 該当項目が0件の場合はセクション自体を非表示

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

### 9-5. 直近アップデート表示の運用

`dashboard/updates.json` を編集して、表示内容を更新します。

```json
[
  {
    "posted_at": "2026-04-22",
    "message": "更新内容..."
  }
]
```

- `posted_at`: 投稿日（ローカル日付、`YYYY-MM-DD`）
- `message`: 表示テキスト
- 配列順のまま表示されます

## 10. 利用状況ログ（日次/月次UU・PV・エラー）を確認する

`localStorage` に `visitor_id` を保存し、ページ表示時に `page_view`、JS例外時に `error` を記録します。

### 10-1. スキーマ反映

- `sql/init.sql` を再実行する  
  または `python -m src.agri_db.main` を1回実行（`ensure_schema` が同じ定義を作成）

追加される主なオブジェクト:

- `usage_events`（生ログ）
- `usage_daily_metrics_jst`（日次UU/PV/error）
- `usage_monthly_metrics_jst`（月次UU/PV/error）
- `usage_daily_user_pv_jst`（日次ユーザー別PV）
- `usage_monthly_user_pv_jst`（月次ユーザー別PV）
- `usage_error_latest_7d_jst`（直近7日エラー上位）

### 10-2. 閲覧ページ

- 既存ページ: `dashboard/index.html`
  - ヘッダーの `利用ログ` ボタンから遷移
- 新規ページ: `dashboard/usage-admin.html`
  - 日次トレンド（UU/PV/上位5ユーザーPV、30/90日切替）
  - 月次トレンド（UU/PV/上位5ユーザーPV、直近12か月）
  - エラー監視（日次件数 + 直近7日エラー上位）

### 10-3. 注意点

- `visitor_id` はブラウザのデータ削除や環境差分で変わるため、UUは推定値です。
- `usage_events` は anon insert を許可しています。必要に応じて Supabase 側で追加制限（レート制限・WAF等）を設定してください。
