-- Replace __CATALOG__ and __SCHEMA__ before running.
-- Example:
--   __CATALOG__ = your current catalog (check with: SELECT current_catalog();)
--   __SCHEMA__  = git_user_analytics

CREATE SCHEMA IF NOT EXISTS __CATALOG__.__SCHEMA__;

USE CATALOG __CATALOG__;
USE SCHEMA __SCHEMA__;

CREATE TABLE IF NOT EXISTS bronze_git_raw (
  ingested_at TIMESTAMP,
  source_repo STRING,
  raw_json STRING
)
USING DELTA;

CREATE TABLE IF NOT EXISTS silver_user_commits (
  repo_name STRING,
  commit_hash STRING,
  author_name STRING,
  author_email STRING,
  commit_ts TIMESTAMP,
  message STRING,
  insertions INT,
  deletions INT,
  source_ingested_at TIMESTAMP
)
USING DELTA;

CREATE TABLE IF NOT EXISTS silver_user_changed_files (
  repo_name STRING,
  commit_hash STRING,
  author_email STRING,
  commit_ts TIMESTAMP,
  file_path STRING,
  directory STRING,
  source_ingested_at TIMESTAMP
)
USING DELTA;

CREATE TABLE IF NOT EXISTS gold_user_activity_daily (
  author_email STRING,
  commit_date DATE,
  commit_count BIGINT,
  changed_files BIGINT,
  insertions_sum BIGINT,
  deletions_sum BIGINT,
  last_updated_at TIMESTAMP
)
USING DELTA;

CREATE TABLE IF NOT EXISTS gold_user_activity_hourly (
  author_email STRING,
  commit_hour INT,
  commit_count BIGINT,
  last_updated_at TIMESTAMP
)
USING DELTA;

CREATE TABLE IF NOT EXISTS gold_user_top_directories_30d (
  author_email STRING,
  directory STRING,
  commit_count BIGINT,
  file_touch_count BIGINT,
  last_updated_at TIMESTAMP
)
USING DELTA;
