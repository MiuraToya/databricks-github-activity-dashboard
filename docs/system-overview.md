# Git User Analytics システム概要（個人サンプル）

## 1. 目的

このシステムは、複数リポジトリの Git 履歴を `ユーザー単位` で集約し、日々の開発活動を可視化するための個人用サンプルです。  
Databricks 上でデータ収集、整形、集計、ダッシュボード表示までを一貫して実行します。

## 2. 何を解決するか

- 「最近どれくらいコミットしているか」を日別で把握できる
- 「何時帯に開発しているか」をユーザー別に把握できる
- 「どのディレクトリをよく触っているか」を横断的に把握できる

## 3. スコープ

### 対象

- Push 済みコミット（GitHub API から取得可能な履歴）
- ユーザー識別は `author_email` を主キーとして扱う
- Databricks SQL ダッシュボードでの可視化
- 個人利用を前提に最小構成で作る

## 4. 全体アーキテクチャ

1. Databricks ノートブックで GitHub API からコミット情報を取得する
2. 取得した JSON を `Bronze` に保存する
3. 正規化と重複除去を行い `Silver` に保存する
4. ユーザー分析向けに日次集計し `Gold` に保存する
5. Databricks SQL ダッシュボードで `Gold` テーブルを表示する

## 5. データフロー

1. 収集  
Databricks ノートブック（Python）が GitHub API を呼び出し、コミット情報を取得します。

2. 生データ格納（Bronze）  
取得した JSON をそのまま保持します。

3. 整形（Silver）  
`repo_name`、`commit_hash`、`author_email`、`commit_ts` など分析に必要な列へ正規化します。  
キーは `repo_name + commit_hash` を採用し、`MERGE` で重複を排除します。

4. 集計（Gold）  
ユーザー単位で日次・時間帯・ディレクトリ別の集計テーブルを作成します。

## 6. テーブル設計（MVP）

### Bronze

`bronze_git_raw`

- `ingested_at` TIMESTAMP
- `source_repo` STRING
- `raw_json` STRING

### Silver

`silver_user_commits`

- `repo_name` STRING
- `commit_hash` STRING
- `author_name` STRING
- `author_email` STRING
- `commit_ts` TIMESTAMP
- `message` STRING
- `insertions` INT
- `deletions` INT

`silver_user_changed_files`

- `repo_name` STRING
- `commit_hash` STRING
- `author_email` STRING
- `commit_ts` TIMESTAMP
- `file_path` STRING
- `directory` STRING

### Gold

`gold_user_activity_daily`

- `author_email` STRING
- `commit_date` DATE
- `commit_count` BIGINT
- `changed_files` BIGINT
- `insertions_sum` BIGINT
- `deletions_sum` BIGINT

`gold_user_activity_hourly`

- `author_email` STRING
- `commit_hour` INT
- `commit_count` BIGINT

`gold_user_top_directories_30d`

- `author_email` STRING
- `directory` STRING
- `commit_count` BIGINT
- `file_touch_count` BIGINT

## 7. 実行方法

現状は Databricks 上で以下の 3 ノートブックを手動実行でき、さらに同じ順序で Job による日次実行を設定済みです。

1. `ingest_github_commits`  
GitHub API からコミット詳細を取得し、`bronze_git_raw` に保存

2. `transform_to_silver`  
JSON を分析用の列に整形し、`silver_user_commits` と `silver_user_changed_files` を更新

3. `aggregate_to_gold`  
ダッシュボード向けの `gold_*` テーブルを更新

日次実行では、上記 3 タスクを `ingest -> transform -> aggregate` の順で実行します。

## 8. 可視化（Databricks SQL / Dashboard）

初期ダッシュボードは以下の 3 画面を提供します。

- 日別コミット数（ユーザー単位）
- 時間帯別コミット数（棒グラフ）
- 直近 30 日のディレクトリ TOP10

## 9. 最初の完了条件（MVP Done）

- 3 つのノートブックが手動実行で成功する
- 3 タスクの日次 Job が設定されている
- 3 つの Gold テーブルが更新される
- ダッシュボードでユーザー単位の活動が閲覧できる
- GitHub API からデータ取得できる
