-- One row per issue
select
    {{ dbt_utils.generate_surrogate_key(['issue_id', 'run_id']) }}  as issue_snapshot_id,

    -- FK surrogate keys → dim tables
    {{ dbt_utils.generate_surrogate_key(['author_id']) }}           as developer_key,
    created_at::DATE                                                as date_key,
    -- note: repo_key joins via repo_full_name = dim_repos.full_name (no numeric repo_id on issue records)

    -- natural keys
    issue_id,
    issue_number,
    node_id,
    run_id,
    repo_full_name,
    author_login,
    author_id,
    title,
    state,
    comments_count,
    search_score,
    created_at,
    created_at::DATE            as created_date,
    updated_at,
    closed_at,
    closed_at::DATE             as closed_date,
    CASE
        WHEN closed_at IS NOT NULL AND created_at IS NOT NULL
        THEN DATEDIFF('hour', created_at, closed_at)
        ELSE NULL
    END                         as hours_to_close
from {{ ref('stg_github__issues') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY issue_id, run_id ORDER BY created_at DESC) = 1
