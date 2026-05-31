-- Flink SQL: github-repos-parsed (persistent view)
-- ─────────────────────────────────────────────────────────────────────────────
-- Parses the raw JSON `value` BYTES column from github.repos.raw into typed
-- columns.  Used by github-developer-activity for windowed aggregation via
-- TUMBLE TVF (which requires a TABLE reference, not a subquery).
--
-- github.repos.raw has no Schema Registry schema (output.data.format=JSON),
-- so Confluent Flink exposes each Kafka message as a raw `value` BYTES column.
-- Fields are extracted via JSON_VALUE on CAST(`value` AS STRING).
--
-- `$rowtime` is included so the downstream TUMBLE TVF can use event-time
-- windowing keyed on the Kafka message timestamp watermark.
-- ─────────────────────────────────────────────────────────────────────────────

CREATE VIEW IF NOT EXISTS github_repos_parsed AS
SELECT
    JSON_VALUE(CAST(val AS STRING), '$.payload.owner.login')                                     AS owner_login,
    JSON_VALUE(CAST(val AS STRING), '$.payload.owner.type')                                      AS owner_type,
    CAST(COALESCE(JSON_VALUE(CAST(val AS STRING), '$.payload.stargazers_count'), '0') AS BIGINT) AS stargazers_count,
    CAST(COALESCE(JSON_VALUE(CAST(val AS STRING), '$.payload.forks_count'),      '0') AS BIGINT) AS forks_count,
    JSON_VALUE(CAST(val AS STRING), '$.payload.language')                                        AS primary_language,
    `$rowtime`
FROM `github.repos.raw` /*+ OPTIONS('scan.startup.mode'='earliest-offset') */;
