{% snapshot dim_repos_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='repo_id',
        strategy='check',
        check_cols=[
            'stargazers_count',
            'forks_count',
            'open_issues_count',
            'language',
            'description',
            'topics',
            'is_archived',
            'is_disabled',
            'has_issues',
            'has_wiki',
            'has_discussions',
        ],
        updated_at='updated_at',
        invalidate_hard_deletes=True,
    )
}}

-- Source is the staging view so all type-casting and field aliasing is
-- already applied before we snapshot.  dbt will add dbt_scd_id,
-- dbt_updated_at, dbt_valid_from, and dbt_valid_to automatically.
select * from {{ ref('stg_github__repositories') }}

{% endsnapshot %}
