"""Configuration for the Zerobus demo setup CLI.

Defaults match the original setup/00_env.sh. Override per-field via env vars
(ZB_CATALOG, ZB_SCHEMA, ZB_TABLE, ZB_SP_DISPLAY_NAME, ZB_WAREHOUSE_NAME,
DATABRICKS_PROFILE) or via CLI flags on `python -m setup.cli`.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import dotenv_values

REPO_ROOT = Path(__file__).resolve().parent.parent
ENV_FILE = REPO_ROOT / ".env"


@dataclass
class Config:
    profile: str
    catalog: str
    schema: str
    table: str
    sp_display_name: str
    warehouse_name: str

    @property
    def fqn(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.table}"


def load_config(
    profile: str | None = None,
    catalog: str | None = None,
    schema: str | None = None,
    table: str | None = None,
    sp_display_name: str | None = None,
    warehouse_name: str | None = None,
) -> Config:
    return Config(
        profile=profile or os.environ.get("DATABRICKS_PROFILE", "e2-fe"),
        catalog=catalog or os.environ.get("ZB_CATALOG", "karthik_auto_validator_1"),
        schema=schema or os.environ.get("ZB_SCHEMA", "iot"),
        table=table or os.environ.get("ZB_TABLE", "sensor_readings"),
        sp_display_name=sp_display_name
        or os.environ.get("ZB_SP_DISPLAY_NAME", "zerobus-demo-sp"),
        warehouse_name=warehouse_name
        or os.environ.get("ZB_WAREHOUSE_NAME", "Shared endpoint"),
    )


def load_dotenv() -> dict[str, str]:
    """Read .env without exporting to os.environ — used for cleanup/grant lookups."""
    if not ENV_FILE.exists():
        return {}
    return {k: v for k, v in dotenv_values(ENV_FILE).items() if v is not None}
