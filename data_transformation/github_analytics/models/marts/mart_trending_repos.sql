-- Trending repositories ranked by stars
-- Analytics question: Which repos are gaining traction? What languages dominate?
-- Deduplicates to latest snapshot per repo across all extraction runs
select
    repo_id,
    full_name,
    repo_name,
    owner_login,
    language,
    description,
    homepage,
    topics,
    visibility,
    license_key,
    default_branch,

    -- engagement metrics
    stargazers_count,
    watchers_count,
    forks_count,
    open_issues_count,
    size_kb,

    -- flags
    is_fork,
    is_archived,
    has_issues,
    has_wiki,
    has_discussions,

    -- time dimensions
    created_at,
    updated_at,
    pushed_at,
    DATE_TRUNC('day', created_at)   as created_date,
    DATE_TRUNC('week', created_at)  as created_week,

    -- derived
    DATEDIFF('day', created_at, updated_at) as days_since_creation,
    CASE
        WHEN stargazers_count >= 1000 THEN 'high'
        WHEN stargazers_count >= 100  THEN 'medium'
        WHEN stargazers_count >= 10   THEN 'low'
        ELSE 'minimal'
    END as star_tier

from {{ ref('stg_github__repositories') }}
where not is_archived
  and not is_disabled
QUALIFY ROW_NUMBER() OVER (PARTITION BY repo_id ORDER BY updated_at DESC) = 1
