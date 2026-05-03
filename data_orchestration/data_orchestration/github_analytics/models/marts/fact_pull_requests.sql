-- One row per pull request
select
    {{ dbt_utils.generate_surrogate_key(['pr_id', 'run_id']) }}  as pr_snapshot_id,

    -- FK surrogate keys → dim tables
    {{ dbt_utils.generate_surrogate_key(['author_id']) }}        as developer_key,
    created_at::DATE                                             as date_key,
    -- note: repo_key joins via repo_full_name = dim_repos.full_name (no numeric repo_id on PR records)

    -- natural keys
    pr_id,
    pr_number,
    node_id,
    run_id,
    repo_full_name,
    author_login,
    author_id,
    title,
    state,
    is_merged,
    comments_count,
    search_score,
    created_at,
    created_at::DATE            as created_date,
    updated_at,
    closed_at,
    closed_at::DATE             as closed_date,
    merged_at,
    merged_at::DATE             as merged_date,
    CASE
        WHEN closed_at IS NOT NULL AND created_at IS NOT NULL
        THEN DATEDIFF('hour', created_at, closed_at)
        ELSE NULL
    END                         as hours_to_close
from {{ ref('stg_github__pull_requests') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY pr_id, run_id ORDER BY created_at DESC) = 1
