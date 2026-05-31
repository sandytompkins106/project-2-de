-- Flink DDL: define github.trending_repos
-- ─────────────────────────────────────────────────────────────────────────────
-- Without a Schema Registry schema the topic appears as [key: BYTES, val: BYTES]
-- and INSERT INTO fails with "Different number of columns".
-- Running this CREATE TABLE registers the column schema so the INSERT can match.
-- ─────────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE TABLE `github.trending_repos` (
  repo_id           BIGINT,
  full_name         STRING,
  owner_login       STRING,
  `language`        STRING,
  stargazers_count  BIGINT,
  forks_count       BIGINT,
  open_issues_count BIGINT,
  star_tier         STRING,
  event_time        TIMESTAMP_LTZ(3)
) DISTRIBUTED BY HASH(repo_id) INTO 1 BUCKETS;
