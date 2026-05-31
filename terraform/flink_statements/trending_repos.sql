-- Flink SQL: github-trending-repos
-- ─────────────────────────────────────────────────────────────────────────────
-- Reads a continuous stream from github.repos.raw, classifies each repo by
-- star tier, and writes typed columns (Avro schema registered in SR) into
-- github.trending_repos.
-- ─────────────────────────────────────────────────────────────────────────────

INSERT INTO `github.trending_repos`
SELECT
    CAST(NULL AS BYTES)         AS `key`,
    CAST(repo_id AS BIGINT)     AS repo_id,
    full_name,
    owner_login,
    lang                        AS `language`,
    CAST(stars       AS BIGINT) AS stargazers_count,
    CAST(forks       AS BIGINT) AS forks_count,
    CAST(issues      AS BIGINT) AS open_issues_count,
    CASE
        WHEN CAST(stars AS BIGINT) >= 1000 THEN 'high'
        WHEN CAST(stars AS BIGINT) >= 100  THEN 'medium'
        WHEN CAST(stars AS BIGINT) >= 10   THEN 'low'
        ELSE                                    'minimal'
    END                         AS star_tier,
    CURRENT_TIMESTAMP           AS event_time
FROM (
    SELECT
        JSON_VALUE(CAST(val AS STRING), '$.payload.id')                                       AS repo_id,
        JSON_VALUE(CAST(val AS STRING), '$.payload.full_name')                                AS full_name,
        JSON_VALUE(CAST(val AS STRING), '$.payload.owner.login')                              AS owner_login,
        JSON_VALUE(CAST(val AS STRING), '$.payload.language')                                 AS lang,
        COALESCE(JSON_VALUE(CAST(val AS STRING), '$.payload.stargazers_count'), '0')          AS stars,
        COALESCE(JSON_VALUE(CAST(val AS STRING), '$.payload.forks_count'),      '0')          AS forks,
        COALESCE(JSON_VALUE(CAST(val AS STRING), '$.payload.open_issues_count'), '0')         AS issues
    FROM `github.repos.raw` /*+ OPTIONS('scan.startup.mode'='earliest-offset') */
) t;
