-- Replace placeholders:
--   __CATALOG__, __SCHEMA__, __AUTHOR_EMAIL__

-- 1) 日別コミット数
SELECT
  commit_date,
  commit_count,
  changed_files,
  insertions_sum,
  deletions_sum
FROM __CATALOG__.__SCHEMA__.gold_user_activity_daily
WHERE author_email = '__AUTHOR_EMAIL__'
ORDER BY commit_date DESC;

-- 2) 時間帯別コミット数
SELECT
  commit_hour,
  commit_count
FROM __CATALOG__.__SCHEMA__.gold_user_activity_hourly
WHERE author_email = '__AUTHOR_EMAIL__'
ORDER BY commit_hour ASC;

-- 3) 直近 30 日のよく触るディレクトリ
SELECT
  directory,
  commit_count,
  file_touch_count
FROM __CATALOG__.__SCHEMA__.gold_user_top_directories_30d
WHERE author_email = '__AUTHOR_EMAIL__'
ORDER BY file_touch_count DESC
LIMIT 10;
