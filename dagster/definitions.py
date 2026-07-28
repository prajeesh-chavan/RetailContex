from pathlib import Path

from dagster import (
    AssetExecutionContext,
    Definitions,
    ScheduleDefinition,
)
from dagster_dbt import DbtCliResource, dbt_assets

PROJECT = Path(__file__).resolve().parent.parent
DBT_DIR = PROJECT / "dbt_retail"
MANIFEST = DBT_DIR / "target" / "manifest.json"


def _dbt_binary():
    candidates = [
        PROJECT / "env" / "Scripts" / "dbt.exe",
        PROJECT / "env" / "bin" / "dbt",
    ]
    for c in candidates:
        if c.exists():
            return str(c)
    return "dbt"


@dbt_assets(manifest=str(MANIFEST))
def dbt_retail(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["run"], context=context).stream()


daily_schedule = ScheduleDefinition(
    name="daily_pipeline",
    target=dbt_retail,
    cron_schedule="0 6 * * *",
)

defs = Definitions(
    assets=[dbt_retail],
    schedules=[daily_schedule],
    resources={"dbt": DbtCliResource(project_dir=str(DBT_DIR), dbt_executable=_dbt_binary())},
)