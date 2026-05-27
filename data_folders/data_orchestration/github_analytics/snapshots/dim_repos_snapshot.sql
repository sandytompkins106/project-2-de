{% snapshot dim_repos_snapshot %}
{{
    config(
      target_schema='marts',
      unique_key='repo_id',
      strategy='check',
      check_cols=['stargazers_count', 'language', 'description', 'homepage', 'topics', 'visibility', 'default_branch', 'license_key', 'license_name', 'is_fork', 'is_archived', 'is_disabled', 'is_private', 'has_issues', 'has_wiki', 'has_discussions', 'has_pages', 'created_at', 'updated_at', 'pushed_at']
    )
}}
select * from {{ ref('stg_github__repositories') }}
{% endsnapshot %}
