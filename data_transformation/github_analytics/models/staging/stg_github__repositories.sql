with source as (
    select * from {{ source('raw', 'repositories') }}
)

select
    _AIRBYTE_RAW_ID                             as airbyte_raw_id,
    _AIRBYTE_EXTRACTED_AT                       as airbyte_extracted_at,
    _RUN_ID                                     as run_id,
    _EXTRACTED_AT::TIMESTAMP_TZ                 as extracted_at,

    -- identifiers
    PAYLOAD:id::NUMBER                          as repo_id,
    PAYLOAD:node_id::VARCHAR                    as node_id,
    PAYLOAD:name::VARCHAR                       as repo_name,
    PAYLOAD:full_name::VARCHAR                  as full_name,

    -- owner
    PAYLOAD:owner:login::VARCHAR                as owner_login,
    PAYLOAD:owner:id::NUMBER                    as owner_id,
    PAYLOAD:owner:type::VARCHAR                 as owner_type,

    -- metadata
    PAYLOAD:description::VARCHAR                as description,
    PAYLOAD:html_url::VARCHAR                   as html_url,
    PAYLOAD:homepage::VARCHAR                   as homepage,
    PAYLOAD:language::VARCHAR                   as language,
    PAYLOAD:topics                              as topics,
    PAYLOAD:visibility::VARCHAR                 as visibility,
    PAYLOAD:default_branch::VARCHAR             as default_branch,
    PAYLOAD:license:key::VARCHAR                as license_key,
    PAYLOAD:license:name::VARCHAR               as license_name,

    -- booleans
    PAYLOAD:fork::BOOLEAN                       as is_fork,
    PAYLOAD:archived::BOOLEAN                   as is_archived,
    PAYLOAD:disabled::BOOLEAN                   as is_disabled,
    PAYLOAD:private::BOOLEAN                    as is_private,
    PAYLOAD:has_issues::BOOLEAN                 as has_issues,
    PAYLOAD:has_wiki::BOOLEAN                   as has_wiki,
    PAYLOAD:has_discussions::BOOLEAN            as has_discussions,
    PAYLOAD:has_pages::BOOLEAN                  as has_pages,

    -- metrics
    PAYLOAD:stargazers_count::NUMBER            as stargazers_count,
    PAYLOAD:watchers_count::NUMBER              as watchers_count,
    PAYLOAD:forks_count::NUMBER                 as forks_count,
    PAYLOAD:open_issues_count::NUMBER           as open_issues_count,
    PAYLOAD:size::NUMBER                        as size_kb,
    PAYLOAD:score::FLOAT                        as search_score,

    -- timestamps
    PAYLOAD:created_at::TIMESTAMP_TZ            as created_at,
    PAYLOAD:updated_at::TIMESTAMP_TZ            as updated_at,
    PAYLOAD:pushed_at::TIMESTAMP_TZ             as pushed_at

from source
