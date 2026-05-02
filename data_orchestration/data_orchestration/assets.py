import os
import sys
from pathlib import Path

from dagster import AssetExecutionContext, AssetKey, AssetSpec, AutomationCondition, EnvVar, asset
from dagster_airbyte import AirbyteCloudWorkspace, AirbyteConnectionTableProps, DagsterAirbyteTranslator, build_airbyte_assets_definitions
from dagster_dbt import DagsterDbtTranslator as _DbtTranslatorBase, DbtCliResource, dbt_assets
from dotenv import load_dotenv

# Load env vars (local only — Dagster Cloud uses UI env vars set in the Dagster+ UI)
load_dotenv(Path("data_integration") / ".env")
load_dotenv(Path("data_orchestration") / ".env")

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
            automation_condition=AutomationCondition.eager(),
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
dbt_project_dir = Path("data_transformation") / "github_analytics"
dbt_resource = DbtCliResource(project_dir=os.fspath(dbt_project_dir))

dbt_manifest_path = (
    dbt_resource.cli(["--quiet", "parse"], target_path=Path("target"))
    .wait()
    .target_path.joinpath("manifest.json")
)


class _EagerDbtTranslator(_DbtTranslatorBase):
    def get_automation_condition(self, dbt_resource_props):
        return AutomationCondition.eager()


@dbt_assets(manifest=dbt_manifest_path, dagster_dbt_translator=_EagerDbtTranslator())
def github_analytics_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["run"], context=context).stream()


# ---------------------------------------------------------------------------
# Extraction asset
# ---------------------------------------------------------------------------
@asset(
    group_name="github_pipeline",
    description="Extract GitHub repos, pull requests, and issues via the Search API and upload to S3.",
)
def github_extraction(context: AssetExecutionContext) -> None:
    # Re-insert path in case this step runs in a Dagster subprocess
    _di = str(Path("data_integration").resolve())
    if _di not in sys.path:
        sys.path.insert(0, _di)

    from src.config import get_settings
    from src.pipeline import run_phase1_extraction

    settings = get_settings()
    summary = run_phase1_extraction(
        github_token=settings.github_token,
        github_api_url=settings.github_api_url,
        resources=["repositories", "pull_requests", "issues"],
        since=settings.since,
        per_page=settings.per_page,
        max_pages=settings.max_pages,
        output_dir=settings.output_dir,
        s3_bucket=settings.s3_bucket,
        s3_prefix=settings.s3_prefix,
        aws_region=settings.aws_region,
    )
    for resource, info in summary["resources"].items():
        context.log.info(
            f"{resource}: {info['records']} records → {info.get('s3_data_uri', 'local only')}"
        )


