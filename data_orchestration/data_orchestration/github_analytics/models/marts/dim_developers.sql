-- All unique developers: repo owners + PR authors + issue authors
with repo_owners as (
    select
        owner_id        as developer_id,
        owner_login     as developer_login,
        owner_type      as developer_type
    from {{ ref('stg_github__repositories') }}
    where owner_id is not null
),

pr_authors as (
    select
        author_id       as developer_id,
        author_login    as developer_login,
        'User'          as developer_type
    from {{ ref('stg_github__pull_requests') }}
    where author_id is not null
),

issue_authors as (
    select
        author_id       as developer_id,
        author_login    as developer_login,
        'User'          as developer_type
    from {{ ref('stg_github__issues') }}
    where author_id is not null
),

all_developers as (
    select * from repo_owners
    union all
    select * from pr_authors
    union all
    select * from issue_authors
)

select distinct
    {{ dbt_utils.generate_surrogate_key(['developer_id']) }}  as developer_key,
    developer_id,
    developer_login,
    developer_type
from all_developers
