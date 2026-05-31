-- ClickHouse setup script — run once against your ClickHouse Cloud service.
-- Execute via the ClickHouse Cloud UI SQL console or:
--   curl -u default:<password> \
--     "https://<host>:8443/?database=default" \
--     --data-binary @clickhouse_setup.sql
--
-- Terraform no longer manages these resources because the ClickHouse/clickhouse
-- Terraform provider v3.x is a Cloud management API (it cannot create tables).
-- Schema management via SQL is the standard ClickHouse practice.
-- ─────────────────────────────────────────────────────────────────────────────

CREATE DATABASE IF NOT EXISTS github_streaming;

-- ── trending_repos ────────────────────────────────────────────────────────────
-- ReplacingMergeTree deduplicates on repo_id so re-delivered Kafka messages
-- are idempotent.  Use SELECT ... FINAL or OPTIMIZE TABLE ... FINAL for
-- exact dedup at query time.

CREATE TABLE IF NOT EXISTS github_streaming.trending_repos
(
    repo_id           Int64,
    full_name         String,
    owner_login       String,
    language          String,
    stargazers_count  Int64,
    forks_count       Int64,
    open_issues_count Int64,
    star_tier         String,
    event_time        DateTime
)
ENGINE = ReplacingMergeTree
ORDER BY (repo_id);

-- ── developer_activity ────────────────────────────────────────────────────────
-- ReplacingMergeTree deduplicates on owner_login — each Flink window close
-- overwrites the previous aggregate for that developer.

CREATE TABLE IF NOT EXISTS github_streaming.developer_activity
(
    owner_login      String,
    owner_type       String,
    repos_count      Int64,
    total_stars      Int64,
    total_forks      Int64,
    primary_language String,
    event_time       DateTime
)
ENGINE = ReplacingMergeTree
ORDER BY (owner_login);
