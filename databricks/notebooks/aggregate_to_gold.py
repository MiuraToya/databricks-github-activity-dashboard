# Databricks notebook source
# MAGIC %md
# MAGIC # aggregate_to_gold
# MAGIC
# MAGIC `silver_*` から `gold_*` 集計テーブルを更新します。

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "git_user_analytics")

catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

spark.sql(
    """
    CREATE OR REPLACE TABLE gold_user_activity_daily
    USING DELTA
    AS
    WITH base AS (
      SELECT
        author_email,
        to_date(commit_ts) AS commit_date,
        count(*) AS commit_count,
        sum(coalesce(insertions, 0)) AS insertions_sum,
        sum(coalesce(deletions, 0)) AS deletions_sum
      FROM silver_user_commits
      GROUP BY author_email, to_date(commit_ts)
    ),
    files AS (
      SELECT
        author_email,
        to_date(commit_ts) AS commit_date,
        count(*) AS changed_files
      FROM silver_user_changed_files
      GROUP BY author_email, to_date(commit_ts)
    )
    SELECT
      b.author_email,
      b.commit_date,
      b.commit_count,
      coalesce(f.changed_files, 0) AS changed_files,
      b.insertions_sum,
      b.deletions_sum,
      current_timestamp() AS last_updated_at
    FROM base b
    LEFT JOIN files f
      ON b.author_email = f.author_email
     AND b.commit_date = f.commit_date
    """
)

spark.sql(
    """
    CREATE OR REPLACE TABLE gold_user_activity_hourly
    USING DELTA
    AS
    SELECT
      author_email,
      hour(commit_ts) AS commit_hour,
      count(*) AS commit_count,
      current_timestamp() AS last_updated_at
    FROM silver_user_commits
    GROUP BY author_email, hour(commit_ts)
    """
)

spark.sql(
    """
    CREATE OR REPLACE TABLE gold_user_top_directories_30d
    USING DELTA
    AS
    WITH filtered AS (
      SELECT *
      FROM silver_user_changed_files
      WHERE commit_ts >= current_timestamp() - INTERVAL 30 DAYS
    ),
    rollup AS (
      SELECT
        author_email,
        directory,
        count(DISTINCT concat(repo_name, ':', commit_hash)) AS commit_count,
        count(*) AS file_touch_count
      FROM filtered
      GROUP BY author_email, directory
    )
    SELECT
      author_email,
      directory,
      commit_count,
      file_touch_count,
      current_timestamp() AS last_updated_at
    FROM rollup
    """
)

print("Gold tables updated")

