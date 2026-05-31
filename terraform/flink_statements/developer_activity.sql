-- Flink SQL: github-developer-activity
-- ─────────────────────────────────────────────────────────────────────────────
-- Row-by-row INSERT: one record per repo event with owner fields.
-- Aggregation (repos_count, total_stars, etc.) happens at query time in
-- ClickHouse using GROUP BY owner_login.
-- ─────────────────────────────────────────────────────────────────────────────

INSERT INTO `github.developer_activity`
SELECT
    CAST(NULL AS BYTES)                  AS `key`,
    owner_login,
    owner_type,
    CAST(1 AS BIGINT)                    AS repos_count,
    stargazers_count                     AS total_stars,
    forks_count                          AS total_forks,
    primary_language,
    CAST(`$rowtime` AS TIMESTAMP_LTZ(3)) AS event_time
FROM github_repos_parsed
WHERE owner_login IS NOT NULL;
