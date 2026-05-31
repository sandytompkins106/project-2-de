-- Flink DDL: define github.developer_activity
-- ─────────────────────────────────────────────────────────────────────────────
-- Without a Schema Registry schema the topic appears as [key: BYTES, val: BYTES]
-- and INSERT INTO fails with "Different number of columns".
-- Running this CREATE TABLE registers the column schema so the INSERT can match.
-- window_end from TUMBLE TVF produces TIMESTAMP(3), NOT TIMESTAMP_LTZ(3).
-- ─────────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE TABLE `github.developer_activity` (
  owner_login      STRING,
  owner_type       STRING,
  repos_count      BIGINT,
  total_stars      BIGINT,
  total_forks      BIGINT,
  primary_language STRING,
  event_time       TIMESTAMP(3)
) DISTRIBUTED BY HASH(owner_login) INTO 1 BUCKETS;
