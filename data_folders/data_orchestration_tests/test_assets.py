"""Tests for Dagster asset definitions in data_orchestration.assets.

Verifies structural properties — partition definitions, automation conditions,
and translator behaviour — without executing any dbt or Airbyte commands.
All external I/O (dbt CLI, manifest parsing) is mocked at module-import time.
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from dagster import AssetKey, AutomationCondition, DailyPartitionsDefinition

# ─── helpers ─────────────────────────────────────────────────────────────────


def _make_mock_dbt_resource():
    """Minimal mock that satisfies the two module-level dbt CLI calls in assets.py."""
    invocation = MagicMock()
    invocation.wait.return_value = invocation
    invocation.target_path.joinpath.return_value = Path("/tmp/mock_manifest.json")
    resource = MagicMock()
    resource.cli.return_value = invocation
    return resource


# ─── module fixture ───────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def assets_mod():
    """Import data_orchestration.assets with all external side-effects disabled.

    Patches applied:
      * dagster_dbt.DbtCliResource  — prevents real dbt deps / dbt parse runs
      * dagster_dbt.dbt_assets      — no-op decorator, skips manifest.json reading
    """
    # Remove any cached import so our patches apply cleanly on re-import.
    sys.modules.pop("data_orchestration.assets", None)
    mock_dbt = _make_mock_dbt_resource()

    with (
        patch("dagster_dbt.DbtCliResource", return_value=mock_dbt),
        patch("dagster_dbt.dbt_assets", lambda **kw: (lambda fn: fn)),
    ):
        import data_orchestration.assets as mod  # noqa: PLC0415

        yield mod


# ─── github_extraction: partition definition ─────────────────────────────────


def test_github_extraction_uses_daily_partitions(assets_mod):
    """github_extraction must use a DailyPartitionsDefinition."""
    assert isinstance(
        assets_mod.github_extraction.partitions_def,
        DailyPartitionsDefinition,
    )


def test_github_extraction_partition_start_date(assets_mod):
    """Partition window must start on the agreed project start date (2026-04-25)."""
    start = assets_mod.github_extraction.partitions_def.start.date().isoformat()
    assert start == "2026-05-17"


# ─── github_extraction: automation condition ─────────────────────────────────


def test_github_extraction_has_automation_condition(assets_mod):
    """github_extraction must declare an automation condition (cron-based)."""
    specs = list(assets_mod.github_extraction.specs)
    assert len(specs) == 1
    assert specs[0].automation_condition is not None


def test_github_extraction_automation_condition_is_not_eager(assets_mod):
    """github_extraction must be cron-driven, not eager like the dbt assets."""
    specs = list(assets_mod.github_extraction.specs)
    assert specs[0].automation_condition != AutomationCondition.eager()


# ─── github_extraction: group membership ─────────────────────────────────────


def test_github_extraction_group_name(assets_mod):
    """github_extraction must belong to the github_pipeline asset group."""
    specs = list(assets_mod.github_extraction.specs)
    assert specs[0].group_name == "github_pipeline"


# ─── _EagerDbtTranslator ─────────────────────────────────────────────────────


def test_eager_dbt_translator_returns_eager_condition(assets_mod):
    """_EagerDbtTranslator.get_automation_condition must return AutomationCondition.eager()."""
    translator = assets_mod._EagerDbtTranslator()
    condition = translator.get_automation_condition({})
    assert condition == AutomationCondition.eager()


def test_eager_dbt_translator_any_resource_props(assets_mod):
    """Result must be eager() regardless of what dbt_resource_props dict contains."""
    translator = assets_mod._EagerDbtTranslator()
    for props in [{}, {"name": "stg_github__repositories"}, {"config": {"materialized": "table"}}]:
        assert translator.get_automation_condition(props) == AutomationCondition.eager()


# ─── _GithubAirbyteTranslator ────────────────────────────────────────────────


def _call_translator(assets_mod, table_name: str):
    """Helper: call _GithubAirbyteTranslator.get_asset_spec with a mocked props object."""
    translator = assets_mod._GithubAirbyteTranslator()
    mock_props = MagicMock()
    mock_props.table_name = table_name

    mock_spec = MagicMock()
    mock_spec.replace_attributes.return_value = mock_spec

    parent_cls = type(translator).__bases__[0]
    with patch.object(parent_cls, "get_asset_spec", return_value=mock_spec):
        translator.get_asset_spec(mock_props)

    return mock_spec.replace_attributes.call_args.kwargs


def test_airbyte_translator_maps_to_raw_asset_key(assets_mod):
    """Every Airbyte table must map to AssetKey(['raw', <table_name>])."""
    for table in ("repositories", "pull_requests", "issues"):
        kwargs = _call_translator(assets_mod, table)
        assert kwargs["key"] == AssetKey(["raw", table]), f"wrong key for {table}"


def test_airbyte_translator_group_name(assets_mod):
    """Airbyte assets must be assigned to the github_pipeline group."""
    kwargs = _call_translator(assets_mod, "repositories")
    assert kwargs["group_name"] == "github_pipeline"


def test_airbyte_translator_depends_on_extraction(assets_mod):
    """Airbyte assets must declare github_extraction as an upstream dependency."""
    kwargs = _call_translator(assets_mod, "repositories")
    assert AssetKey("github_extraction") in kwargs["deps"]
