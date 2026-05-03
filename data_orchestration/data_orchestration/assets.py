import os
import sys
from pathlib import Path

from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetSpec,
    AutomationCondition,
    DailyPartitionsDefinition,
    EnvVar,
    asset,
)
from dagster_airbyte import (
    AirbyteCloudWorkspace,
    AirbyteConnectionTableProps,
    DagsterAirbyteTranslator,
    build_airbyte_assets_definitions,
)
from dagster_dbt import DagsterDbtTranslator as _DbtTranslatorBase
from dagster_dbt import DbtCliResource, dbt_assets
from dotenv import load_dotenv

# Load env vars (local only — Dagster Cloud uses UI env vars)
_REPO_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(_REPO_ROOT / "data_integration" / ".env")
load_dotenv(_REPO_ROOT / "data_orchestration" / ".env")

# dbt project is embedded inside this package so __file__-relative path works
# in both local dev (source tree) and Dagster+ cloud (installed pex venv).
_DBT_PROJECT_DIR = (Path(__file__).parent / "github_analytics").resolve()


_AIRBYTE_CONNECTION_ID = os.getenv("AIRBYTE_CONNECTION_ID", "")
_AIRBYTE_CLIENT_ID = os.getenv("AIRBYTE_CLIENT_ID", "")
_AIRBYTE_CLIENT_SECRET = os.getenv("AIRBYTE_CLIENT_SECRET", "")


# ---------------------------------------------------------------------------
# Airbyte workspace + assets (auto-discovers synced tables from the connection)
# ---------------------------------------------------------------------------
class _GithubAirbyteTranslator(DagsterAirbyteTranslator):
    """Map Airbyte sync tables to AssetKey(["raw", table_name]) to match dbt sources."""

    def get_asset_spec(self, props: AirbyteConnectionTableProps) -> AssetSpec:
        default_spec = super().get_asset_spec(props)
        return default_spec.replace_attributes(
            key=AssetKey(["raw", props.table_name]),
            group_name="github_pipeline",
            deps=[AssetKey("github_extraction")],
            automation_condition=AutomationCondition.on_cron("30 6 * * *"),
        )


airbyte_workspace = AirbyteCloudWorkspace(
    workspace_id=EnvVar("AIRBYTE_WORKSPACE_ID"),
    client_id=EnvVar("AIRBYTE_CLIENT_ID"),
    client_secret=EnvVar("AIRBYTE_CLIENT_SECRET"),
)

airbyte_assets = build_airbyte_assets_definitions(
    workspace=airbyte_workspace,
    dagster_airbyte_translator=_GithubAirbyteTranslator(),
)

# ---------------------------------------------------------------------------
# dbt resource + manifest
# ---------------------------------------------------------------------------
dbt_project_dir = _DBT_PROJECT_DIR
dbt_resource = DbtCliResource(project_dir=os.fspath(dbt_project_dir))

dbt_manifest_path = (
    dbt_resource.cli(["--quiet", "parse"], target_path=Path("target")).wait().target_path.joinpath("manifest.json")
)


class _EagerDbtTranslator(_DbtTranslatorBase):
    """dbt translator that applies AutomationCondition.eager() to every dbt asset."""

    def get_automation_condition(self, dbt_resource_props):
        return AutomationCondition.eager()


@dbt_assets(manifest=dbt_manifest_path, dagster_dbt_translator=_EagerDbtTranslator())
def github_analytics_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    """Build all dbt models and run tests in the github_analytics project."""
    yield from dbt.cli(["build"], context=context).stream()


# ---------------------------------------------------------------------------
# Extraction asset
# ---------------------------------------------------------------------------
@asset(
    partitions_def=DailyPartitionsDefinition(start_date="2026-04-25", end_offset=1),
    group_name="github_pipeline",
    automation_condition=AutomationCondition.on_cron("0 6 * * *"),
    description="Extract GitHub repos, pull requests, and issues via the Search API and upload to S3.",
)
def github_extraction(context: AssetExecutionContext) -> None:
    partition_date = context.partition_key  # "YYYY-MM-DD"
    # Re-insert path in case this step runs in a Dagster subprocess
    _di = str(_REPO_ROOT / "data_integration")
    if _di not in sys.path:
        sys.path.insert(0, _di)

    from src.config import get_settings
    from src.pipeline import run_phase1_extraction

    settings = get_settings()
    summary = run_phase1_extraction(
        github_token=settings.github_token,
        github_api_url=settings.github_api_url,
        resources=["repositories", "pull_requests", "issues"],
        date=partition_date,
        per_page=settings.per_page,
        max_pages=settings.max_pages,
        output_dir=settings.output_dir,
        s3_bucket=settings.s3_bucket,
        s3_prefix=settings.s3_prefix,
        aws_region=settings.aws_region,
    )
    for resource, info in summary["resources"].items():
        context.log.info(f"{resource}: {info['records']} records → {info.get('s3_data_uri', 'local only')}")
