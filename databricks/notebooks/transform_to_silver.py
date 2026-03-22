# Databricks notebook source
# MAGIC %md
# MAGIC # transform_to_silver
# MAGIC
# MAGIC `bronze_git_raw` から `silver_user_commits` / `silver_user_changed_files` を更新します。

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
    CREATE OR REPLACE TEMP VIEW v_commits_src AS
    SELECT
      source_repo AS repo_name,
      get_json_object(raw_json, '$.sha') AS commit_hash,
      get_json_object(raw_json, '$.commit.author.name') AS author_name,
      lower(get_json_object(raw_json, '$.commit.author.email')) AS author_email,
      to_timestamp(get_json_object(raw_json, '$.commit.author.date')) AS commit_ts,
      get_json_object(raw_json, '$.commit.message') AS message,
      cast(get_json_object(raw_json, '$.stats.additions') AS INT) AS insertions,
      cast(get_json_object(raw_json, '$.stats.deletions') AS INT) AS deletions,
      ingested_at AS source_ingested_at
    FROM bronze_git_raw
    WHERE get_json_object(raw_json, '$.sha') IS NOT NULL
    """
)

spark.sql(
    """
    MERGE INTO silver_user_commits AS t
    USING (
      SELECT
        repo_name,
        commit_hash,
        max_by(author_name, source_ingested_at) AS author_name,
        max_by(author_email, source_ingested_at) AS author_email,
        max_by(commit_ts, source_ingested_at) AS commit_ts,
        max_by(message, source_ingested_at) AS message,
        max_by(insertions, source_ingested_at) AS insertions,
        max_by(deletions, source_ingested_at) AS deletions,
        max(source_ingested_at) AS source_ingested_at
      FROM v_commits_src
      GROUP BY repo_name, commit_hash
    ) AS s
    ON t.repo_name = s.repo_name AND t.commit_hash = s.commit_hash
    WHEN MATCHED THEN UPDATE SET
      t.author_name = s.author_name,
      t.author_email = s.author_email,
      t.commit_ts = s.commit_ts,
      t.message = s.message,
      t.insertions = s.insertions,
      t.deletions = s.deletions,
      t.source_ingested_at = s.source_ingested_at
    WHEN NOT MATCHED THEN INSERT *
    """
)

# COMMAND ----------

spark.sql(
    """
    CREATE OR REPLACE TEMP VIEW v_files_src AS
    SELECT
      source_repo AS repo_name,
      get_json_object(raw_json, '$.sha') AS commit_hash,
      lower(get_json_object(raw_json, '$.commit.author.email')) AS author_email,
      to_timestamp(get_json_object(raw_json, '$.commit.author.date')) AS commit_ts,
      f.filename AS file_path,
      CASE
        WHEN instr(f.filename, '/') > 0 THEN regexp_extract(f.filename, '^(.*)/[^/]+$', 1)
        ELSE '.'
      END AS directory,
      ingested_at AS source_ingested_at
    FROM bronze_git_raw
    LATERAL VIEW explode(
      from_json(get_json_object(raw_json, '$.files'), 'array<struct<filename:string>>')
    ) ex AS f
    WHERE get_json_object(raw_json, '$.sha') IS NOT NULL
    """
)

spark.sql(
    """
    MERGE INTO silver_user_changed_files AS t
    USING (
      SELECT
        repo_name,
        commit_hash,
        file_path,
        max_by(author_email, source_ingested_at) AS author_email,
        max_by(commit_ts, source_ingested_at) AS commit_ts,
        max_by(directory, source_ingested_at) AS directory,
        max(source_ingested_at) AS source_ingested_at
      FROM v_files_src
      GROUP BY repo_name, commit_hash, file_path
    ) AS s
    ON t.repo_name = s.repo_name
      AND t.commit_hash = s.commit_hash
      AND t.file_path = s.file_path
    WHEN MATCHED THEN UPDATE SET
      t.author_email = s.author_email,
      t.commit_ts = s.commit_ts,
      t.directory = s.directory,
      t.source_ingested_at = s.source_ingested_at
    WHEN NOT MATCHED THEN INSERT *
    """
)

print("Silver tables updated")

