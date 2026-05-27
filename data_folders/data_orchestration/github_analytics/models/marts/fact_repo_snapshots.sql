-- One row per repo per extraction run — tracks metric changes over time
select
    {{ dbt_utils.generate_surrogate_key(['repo_id', 'run_id']) }}   as snapshot_id,

    -- FK surrogate keys → dim tables
    {{ dbt_utils.generate_surrogate_key(['repo_id']) }}             as repo_key,
    {{ dbt_utils.generate_surrogate_key(['owner_id']) }}            as developer_key,
    extracted_at::DATE                                              as date_key,

    -- natural keys
    repo_id,
    run_id,
    full_name,
    owner_id,
    extracted_at,
    extracted_at::DATE                                              as snapshot_date,

    -- point-in-time metrics
    stargazers_count,
    watchers_count,
    forks_count,
    open_issues_count,
    size_kb,
    search_score
from {{ ref('dim_repos_snapshot') }}
where valid_to is null
QUALIFY ROW_NUMBER() OVER (PARTITION BY repo_id, run_id ORDER BY extracted_at DESC) = 1
