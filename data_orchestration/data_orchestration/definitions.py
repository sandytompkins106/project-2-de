from dagster import (
    AssetSelection,
    Definitions,
    ScheduleDefinition,
    load_assets_from_modules,
)

from data_orchestration import assets  # noqa: TID252
from data_orchestration.assets import airbyte_workspace, dbt_resource

all_assets = load_assets_from_modules([assets])

# Fires extraction daily at 06:00 UTC
# Airbyte raw table assets auto-materialize eagerly after github_extraction (AutomationCondition)
# dbt models auto-materialize eagerly after raw tables (AutomationCondition)
daily_extraction_schedule = ScheduleDefinition(
    name="daily_github_extraction",
    cron_schedule="0 6 * * *",
    target=AssetSelection.assets(assets.github_extraction),
)

defs = Definitions(
    assets=all_assets,
    schedules=[daily_extraction_schedule],
    resources={"dbt": dbt_resource, "airbyte": airbyte_workspace},
)

