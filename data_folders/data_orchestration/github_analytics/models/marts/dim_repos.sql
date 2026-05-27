-- One row per unique repository (latest snapshot by valid_from)
with latest_snapshot as (
    select *,
        ROW_NUMBER() OVER (PARTITION BY repo_id ORDER BY valid_from DESC) as rn
    from {{ ref('dim_repos_snapshot') }}
    where valid_to is null
)

select
    {{ dbt_utils.generate_surrogate_key(['repo_id']) }}  as repo_key,
    repo_id,
    node_id,
    full_name,
    repo_name,
    owner_login,
    owner_id,
    owner_type,
    language,
    description,
    homepage,
    topics,
    visibility,
    default_branch,
    license_key,
    license_name,
    is_fork,
    is_archived,
    is_disabled,
    is_private,
    has_issues,
    has_wiki,
    has_discussions,
    has_pages,
    created_at,
    updated_at,
    pushed_at
from latest_snapshot
where rn = 1
