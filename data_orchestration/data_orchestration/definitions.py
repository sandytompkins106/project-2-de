"""Dagster Definitions for the GitHub analytics pipeline.

Wires together all assets (extraction, Airbyte sync, dbt models), the
automation sensor that drives them, and the dbt + Airbyte resources.
"""
from dagster import (
    AutomationConditionSensorDefinition,
    DefaultSensorStatus,
    Definitions,
    load_assets_from_modules,
)

from data_orchestration import assets  # noqa: TID252
from data_orchestration.assets import airbyte_workspace, dbt_resource

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    sensors=[AutomationConditionSensorDefinition("automation_sensor", target="*", default_status=DefaultSensorStatus.RUNNING)],
    resources={"dbt": dbt_resource, "airbyte": airbyte_workspace},
)

