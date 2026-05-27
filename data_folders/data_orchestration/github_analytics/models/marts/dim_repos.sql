-- One row per unique repository — powered by the SCD2 dim_repos_snapshot.
-- Only the currently-active row is returned (dbt_valid_to IS NULL).
-- Historical rows remain in the snapshot table for time-travel analysis.
with current_repos as (
    select *
    from {{ ref('dim_repos_snapshot') }}
    where dbt_valid_to is null
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
from current_repos
