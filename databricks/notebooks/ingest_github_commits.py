# Databricks notebook source
# MAGIC %md
# MAGIC # ingest_github_commits
# MAGIC
# MAGIC GitHub API からコミット詳細を取得し、`bronze_git_raw` に append するノートブックです。

# COMMAND ----------

def create_widget_if_missing(name: str, default_value: str) -> None:
    try:
        dbutils.widgets.get(name)
    except Exception:
        dbutils.widgets.text(name, default_value)


create_widget_if_missing("catalog", "workspace")
create_widget_if_missing("schema", "git_user_analytics")
create_widget_if_missing("repo_list", "MiuraToya/pythaw")
create_widget_if_missing("days_back", "7")
create_widget_if_missing("github_token", "")
create_widget_if_missing("github_secret_scope", "github")
create_widget_if_missing("github_secret_key", "pat")

catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
repo_list_raw = dbutils.widgets.get("repo_list").strip()
days_back = int(dbutils.widgets.get("days_back").strip())
github_token_widget = dbutils.widgets.get("github_token").strip()
secret_scope = dbutils.widgets.get("github_secret_scope").strip()
secret_key = dbutils.widgets.get("github_secret_key").strip()

full_table = f"{catalog}.{schema}.bronze_git_raw"
repos = [r.strip() for r in repo_list_raw.split(",") if r.strip()]
if not repos:
    raise ValueError("repo_list is empty. Set comma-separated repos like owner/repo1,owner/repo2")

# COMMAND ----------

import json
from datetime import datetime, timedelta, timezone

import requests
from pyspark.sql import Row

if github_token_widget:
    token = github_token_widget
else:
    token = dbutils.secrets.get(secret_scope, secret_key)
headers = {
    "Accept": "application/vnd.github+json",
    "Authorization": f"Bearer {token}",
    "X-GitHub-Api-Version": "2022-11-28",
}

since_dt = datetime.now(timezone.utc) - timedelta(days=days_back)
since_str = since_dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")
print(f"repo_list={repos}, days_back={days_back}, since={since_str}")


def github_get(url: str, params: dict | None = None) -> requests.Response:
    resp = requests.get(url, headers=headers, params=params, timeout=60)
    resp.raise_for_status()
    return resp


rows = []
for repo in repos:
    page = 1
    while True:
        list_url = f"https://api.github.com/repos/{repo}/commits"
        params = {"since": since_str, "per_page": 100, "page": page}
        commit_list = github_get(list_url, params=params).json()
        if page == 1:
            print(f"repo={repo}, page={page}, commits_in_page={len(commit_list)}")
        if not commit_list:
            break

        for c in commit_list:
            sha = c["sha"]
            detail_url = f"https://api.github.com/repos/{repo}/commits/{sha}"
            detail = github_get(detail_url).json()
            rows.append(
                Row(
                    ingested_at=datetime.now(timezone.utc),
                    source_repo=repo,
                    raw_json=json.dumps(detail, ensure_ascii=False),
                )
            )
        page += 1

if rows:
    spark.createDataFrame(rows).write.mode("append").saveAsTable(full_table)
    print(f"Inserted {len(rows)} rows into {full_table}")
else:
    print(f"No new commits found since {since_str}")
