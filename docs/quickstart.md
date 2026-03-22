# Quickstart (1アカウント)

## 1. 事前に用意するもの

- GitHub Personal Access Token（repo 読み取り可能）
- Databricks の `catalog` と `schema` 名
- 取り込み対象のリポジトリ名（`owner/repo1,owner/repo2` 形式）
- Databricks Secret Scope と Secret Key

## 2. Databricks 側で最初にやること

1. Secret を作成する  
GitHub PAT を Databricks Secret に登録する  
例: `scope=github`, `key=pat`

2. テーブルを作成する  
`databricks/sql/01_create_tables.sql` の `__CATALOG__` と `__SCHEMA__` を置換して実行

3. ノートブックをアップロードする  
以下 3 ファイルを Workspace に配置
- `databricks/notebooks/ingest_github_commits.py`
- `databricks/notebooks/transform_to_silver.py`
- `databricks/notebooks/aggregate_to_gold.py`

## 3. 動作確認

1. `ingest_github_commits` を手動実行  
`repo_list` は `owner/repo1,owner/repo2` 形式
`github_token` は空のまま  
`github_secret_scope` / `github_secret_key` に Secret 名を設定
2. `transform_to_silver` を手動実行  
3. `aggregate_to_gold` を手動実行  
4. Gold テーブルにデータがあることを確認

## 4. ダッシュボード作成

- `databricks/sql/02_dashboard_queries.sql` のプレースホルダーを置換してクエリ作成
- 3 クエリを Databricks SQL ダッシュボードに配置

## 5. 自動化

- `Jobs & Pipelines` から Job を作成し、以下 3 タスクを接続する
- `ingest_github_commits`
- `transform_to_silver`
- `aggregate_to_gold`
- Compute は Serverless を利用する
- 毎日 09:00 JST 実行で保存する
