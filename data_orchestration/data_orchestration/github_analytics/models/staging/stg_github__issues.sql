with source as (
    select * from {{ source('raw', 'issues') }}
)

select
    _AIRBYTE_RAW_ID                                     as airbyte_raw_id,
    _AIRBYTE_EXTRACTED_AT                               as airbyte_extracted_at,
    _RUN_ID                                             as run_id,
    _EXTRACTED_AT::TIMESTAMP_TZ                         as extracted_at,

    -- identifiers
    PAYLOAD:id::NUMBER                                  as issue_id,
    PAYLOAD:number::NUMBER                              as issue_number,
    PAYLOAD:node_id::VARCHAR                            as node_id,

    -- content
    PAYLOAD:title::VARCHAR                              as title,
    PAYLOAD:state::VARCHAR                              as state,
    PAYLOAD:repository_url::VARCHAR                     as repository_url,

    -- derived repo full_name from repository_url
    REGEXP_SUBSTR(PAYLOAD:repository_url::VARCHAR, '[^/]+/[^/]+$') as repo_full_name,

    -- author
    PAYLOAD:user:login::VARCHAR                         as author_login,
    PAYLOAD:user:id::NUMBER                             as author_id,

    -- metrics
    PAYLOAD:comments::NUMBER                            as comments_count,
    PAYLOAD:score::FLOAT                                as search_score,

    -- timestamps
    PAYLOAD:created_at::TIMESTAMP_TZ                    as created_at,
    PAYLOAD:updated_at::TIMESTAMP_TZ                    as updated_at,
    PAYLOAD:closed_at::TIMESTAMP_TZ                     as closed_at

from source
