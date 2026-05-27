-- Community health per repository
-- Analytics question: How healthy are repos? Are issues being engaged with?
with repos as (
    select *
    from {{ ref('dim_repos_snapshot') }}
    where dbt_valid_to is null
),

issue_counts as (
    select
        repo_full_name,
        COUNT(*)                                            as total_issues,
        COUNT(CASE WHEN state = 'open' THEN 1 END)          as open_issues,
        COUNT(CASE WHEN state = 'closed' THEN 1 END)        as closed_issues,
        AVG(comments_count)                                 as avg_issue_comments
    from {{ ref('stg_github__issues') }}
    group by repo_full_name
),

pr_counts as (
    select
        repo_full_name,
        COUNT(*)                                            as total_prs,
        COUNT(CASE WHEN is_merged THEN 1 END)               as merged_prs,
        COUNT(CASE WHEN state = 'open' THEN 1 END)          as open_prs,
        AVG(comments_count)                                 as avg_pr_comments
    from {{ ref('stg_github__pull_requests') }}
    group by repo_full_name
)

select
    r.repo_id,
    r.full_name,
    r.owner_login,
    r.language,
    r.stargazers_count,
    r.forks_count,
    r.open_issues_count,
    r.has_issues,
    r.has_wiki,
    r.has_discussions,
    r.license_key,
    r.created_at,

    -- issue health
    COALESCE(i.total_issues, 0)         as tracked_issues,
    COALESCE(i.open_issues, 0)          as tracked_open_issues,
    COALESCE(i.closed_issues, 0)        as tracked_closed_issues,
    COALESCE(i.avg_issue_comments, 0)   as avg_issue_comments,

    -- pr health
    COALESCE(p.total_prs, 0)            as tracked_prs,
    COALESCE(p.merged_prs, 0)           as merged_prs,
    COALESCE(p.open_prs, 0)             as open_prs,
    COALESCE(p.avg_pr_comments, 0)      as avg_pr_comments,

    -- derived ratios
    CASE
        WHEN COALESCE(i.total_issues, 0) > 0
        THEN ROUND(i.closed_issues / i.total_issues * 100, 1)
        ELSE NULL
    END as issue_close_rate_pct,

    CASE
        WHEN COALESCE(p.total_prs, 0) > 0
        THEN ROUND(p.merged_prs / p.total_prs * 100, 1)
        ELSE NULL
    END as pr_merge_rate_pct,

    -- overall health score (0–100)
    LEAST(100, (
        COALESCE(CASE WHEN r.has_issues     THEN 10 ELSE 0 END, 0) +
        COALESCE(CASE WHEN r.has_wiki       THEN 10 ELSE 0 END, 0) +
        COALESCE(CASE WHEN r.has_discussions THEN 10 ELSE 0 END, 0) +
        COALESCE(CASE WHEN r.license_key IS NOT NULL THEN 20 ELSE 0 END, 0) +
        COALESCE(CASE WHEN r.stargazers_count > 0 THEN 10 ELSE 0 END, 0) +
        COALESCE(CASE WHEN COALESCE(i.total_issues, 0) > 0 THEN 20 ELSE 0 END, 0) +
        COALESCE(CASE WHEN COALESCE(p.total_prs, 0) > 0 THEN 20 ELSE 0 END, 0)
    ))                                  as health_score

from repos r
left join issue_counts i on LOWER(r.full_name) = LOWER(i.repo_full_name)
left join pr_counts p    on LOWER(r.full_name) = LOWER(p.repo_full_name)
