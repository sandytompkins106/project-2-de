-- Developer activity: repo creation patterns and engagement per owner
-- Analytics question: Who are the most active developers? What do they build?

with latest_repos as (
    select *
    from {{ ref('dim_repos_snapshot') }}
    where owner_login is not null
      and valid_to is null
)

select
    owner_login,
    owner_id,
    owner_type,

    -- volume
    COUNT(*)                                        as repos_created,
    COUNT(CASE WHEN NOT is_fork THEN 1 END)         as original_repos,
    COUNT(CASE WHEN is_fork THEN 1 END)             as forked_repos,

    -- engagement totals
    SUM(stargazers_count)                           as total_stars,
    SUM(forks_count)                                as total_forks,
    SUM(open_issues_count)                          as total_open_issues,
    AVG(stargazers_count)                           as avg_stars_per_repo,

    -- language diversity
    COUNT(DISTINCT language)                        as distinct_languages,
    ARRAY_AGG(DISTINCT language)
        WITHIN GROUP (ORDER BY language)            as languages_used,

    -- most used language
    MODE(language)                                  as primary_language,

    -- time range
    MIN(created_at)                                 as first_repo_at,
    MAX(created_at)                                 as latest_repo_at,
    DATEDIFF('day', MIN(created_at), MAX(created_at)) as active_days_span

from latest_repos
group by owner_login, owner_id, owner_type
