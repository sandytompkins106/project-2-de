from dagster import DailyPartitionsDefinition
from data_orchestration.assets import (
    airbyte_assets,
    github_analytics_dbt_assets,
    github_extraction,
)


# ---------------------------------------------------------------------------
# github_extraction
# ---------------------------------------------------------------------------
class TestGithubExtractionAsset:
    def test_has_daily_partitions(self):
        partitions = github_extraction.partitions_def
        assert isinstance(partitions, DailyPartitionsDefinition)

    def test_partition_start_date(self):
        partitions = github_extraction.partitions_def
        assert str(partitions.start) == "2026-05-17"

    def test_end_offset_excludes_today(self):
        partitions = github_extraction.partitions_def
        assert partitions.end_offset == 1

    def test_group_name(self):
        assert github_extraction.group_names_by_key[github_extraction.key] == "github_pipeline"

    def test_has_automation_condition(self):
        assert (
            github_extraction.auto_materialize_policy is not None or github_extraction.automation_condition is not None
        )


# ---------------------------------------------------------------------------
# Airbyte assets
# ---------------------------------------------------------------------------
class TestAirbyteAssets:
    def test_produces_three_streams(self):
        keys = [spec.key.path[-1] for defn in airbyte_assets for spec in defn.specs]
        assert set(keys) == {"repositories", "pull_requests", "issues"}

    def test_all_in_github_pipeline_group(self):
        for defn in airbyte_assets:
            for spec in defn.specs:
                assert spec.group_name == "github_pipeline"

    def test_all_depend_on_github_extraction(self):
        from dagster import AssetKey

        for defn in airbyte_assets:
            for spec in defn.specs:
                dep_keys = {dep.asset_key for dep in spec.deps}
                assert AssetKey("github_extraction") in dep_keys


# ---------------------------------------------------------------------------
# dbt assets
# ---------------------------------------------------------------------------
class TestDbtAssets:
    def test_dbt_assets_defined(self):
        assert github_analytics_dbt_assets is not None

    def test_expected_models_present(self):
        expected = {
            "stg_github__repositories",
            "stg_github__pull_requests",
            "stg_github__issues",
            "dim_repos",
            "dim_developers",
            "fact_repo_snapshots",
            "fact_pull_requests",
            "fact_issues",
            "mart_trending_repos",
            "mart_developer_activity",
            "mart_community_health",
        }
        asset_names = {key.path[-1] for key in github_analytics_dbt_assets.keys}
        assert expected.issubset(asset_names)
