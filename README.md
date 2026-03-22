# Databricks GitHub Activity Dashboard

GitHub のコミット履歴を Databricks 上で取り込み、整形、集計、可視化する学習用サンプルです。

このリポジトリでは、以下の流れを実装するNotebooksやQueryを管理しています。

- GitHub API からコミット情報を取得
- Bronze / Silver / Gold の形でデータを段階管理
- Databricks SQL Dashboard で可視化
- Databricks Job で日次実行

## 構成

- `databricks/notebooks/`
  - `ingest_github_commits.py`
  - `transform_to_silver.py`
  - `aggregate_to_gold.py`
- `databricks/sql/`
  - `01_create_tables.sql`
  - `02_dashboard_queries.sql`
- `docs/`
  - `system-overview.md`
  - `quickstart.md`
  - `databricks-learning-report.html`

## 使い方

1. Databricks で schema とテーブルを作成
2. GitHub PAT を Secret に登録
3. 3 つの Notebook を実行
4. Dashboard を作成
5. 必要に応じて Job を設定

詳細は [quickstart.md](/Users/miuratouya/Develop/Project/databricks-playground/docs/quickstart.md) を参照してください。
