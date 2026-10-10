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
- 下段: 販売価格中値の相図と、入荷量の相図（どちらも 水準 × 7日前からの変化）
  - 対象は2軸グラフ・品目別価格推移で選択中の品目。表示期間と連動
  - ならし方は「7日 → 3日」の二段の移動平均（9日間で重み 1・2・3・3・3・3・3・2・1）
    - 1段目: 各日について「その日までの暦の7日間」の入荷量（1取引日あたりの平均。祝日などで取引日数が変わっても揺れないよう合計ではなく平均）と中値（入荷量加重平均）を計算。暦の7日間には各曜日が1回ずつ入るので曜日の偏りが消える。7日間の取引が3日未満の日は線を途切れさせる（`config.js` の `rollingMinTradingDays`）
    - 2段目: 1段目の値を過去3日間で平均（中値は7日間の入荷量合計で重み付け）。突出した入荷の日が窓に出入りするときの跳ねを抑える
  - 横軸は水準、縦軸は7日前からの変化（中央値比モードでは%、Zスコアモードでは0を中心に標準化）。7日前との比較のため、表示期間の7日前から計算する
  - 縦軸の「7日前からの変化」は区間の真ん中（約3.5日前）の速さを表すため、横軸の水準も同じ時点（t−4日と t−3日の移動平均の平均）にそろえる。最新日の水準を使うと時刻がずれ、周期的な動きの輪が斜めに歪み、偶然の揺れが斜めの帯になる。二段の移動平均（9日窓）の値は計算上の日付より約4日前を中心とするので、最新の点はデータの最新日の約7.5日前（約1週間前）の状態を表す。見出しの下の説明とツールチップには、この実際の時点を表示する
  - 周期的な値動きは時計回りの渦になる。入荷量が価格に先行する品目では、入荷量の相図の動きが価格の相図に先立って現れる
  - 線は過去ほど薄く細く描き、点は最新値のみ（大きさは入荷量、色は前週比マップと同じ）
  - 象限の背景色は価格への影響の向き（赤: 値上がり方向、青: 値下がり方向。色は前週比マップと同じ）。グラフ上部のキャプションの色と対応させる
    - 価格の相図: 左上（安値圏から回復）を赤、右下（高値圏から下落）を青
    - 入荷量の相図: 左下（品薄が深まる）を赤、右上（出回りのピークへ）を青。入荷量の水準と変化が同じ向きにそろい、価格への圧力が最も強い局面に色を付ける
  - 正規化は「中央値比（対数軸、原点 = 表示期間の中央値）」と「Zスコア（中央値・MAD基準）」を切り替え可能（2つの相図で共通、選択は保存される）
    - Zスコアは、値がほとんど動かない期間が長いとMADが0に近づくため、ばらつきに下限（水準は中央値の2%、変化率は1ポイント）を設けている
  - 線や点をタップすると2軸グラフの対象品目が切り替わる
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

## 11. Claude から市況データを使う（MCPサーバー）

`supabase/functions/agri-mcp/` に、市況データを Claude から使うための MCP（Model Context Protocol）サーバーがあります。Supabase Edge Function として動き、claude.ai の「カスタムコネクタ」に登録すると、会話の中で Claude がデータを取得して答えます。

- 読み取り専用（anon キーで `market_daily_item_stats` ビューを読むだけ）。ログイン不要
- 計算はダッシュボードと同じ（KPI・前週比・相図）。`analytics.js` を変更するときは `dashboard/app.js` も合わせる
- 取得したデータは10分間キャッシュする

提供するツール:

| ツール | 内容 |
| --- | --- |
| `list_items` | 品目一覧（最新の中値・入荷量・取引日数）。品目名の検索（ひらがな・部分一致可） |
| `get_item_history` | 1品目の日次の入荷量・中値・高値・安値 |
| `get_price_ranking` | 最新日の中値の、過去14日中央値からの乖離ランキング（割高・割安） |
| `get_weekly_change` | 直近7日と前の7日の、入荷量・中値の変化率（品薄で値上がり 等の判定付き） |
| `get_phase_state` | 価格・入荷量の相図の局面（例: 高値圏から下落、出回りのピークへ）と過去の軌跡 |

### 配備（初回のみ設定が必要）

1. Supabase のダッシュボードで Personal Access Token を発行する（Account → Access Tokens）
2. GitHub リポジトリの Settings → Secrets and variables → Actions に `SUPABASE_ACCESS_TOKEN` として登録する
3. `main` に `supabase/**` の変更が入ると `.github/workflows/deploy_mcp.yml` がテスト → 配備 → 動作確認を行う（Actions 画面から手動実行も可）

配備先の URL:

```
https://byhthaiwbutqstrqbwwc.supabase.co/functions/v1/agri-mcp
```

### claude.ai に登録する

1. claude.ai の設定 → コネクタ →「カスタムコネクタを追加」
2. 名前（例: 沖縄市況）と上記の URL を入力して追加（認証の設定は不要）
3. 会話の入力欄のツール設定でコネクタを有効にして質問する

質問の例:

- 「今日いちばん割安な野菜は？」
- 「レタスとズッキーニは今、値動きのどの局面？入荷量の動きと合わせて教えて」
- 「島バナナの直近30日の価格推移を表にして」

### ローカルでのテスト

```bash
node --test tests/agri-mcp.test.mjs
```

注意:

- URL を知っていれば誰でも呼び出せます（データは市況PDFの公開情報）。Supabase の Edge Functions の無料枠（呼び出し回数）を超える使われ方をしていないか、Supabase のダッシュボードで確認してください
- 相図（`get_phase_state`）は移動平均のため、最新の点はデータ最新日の約1週間前の状態です
