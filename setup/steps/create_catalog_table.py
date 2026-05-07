"""Create catalog, schema, and bronze table from setup/01_create_catalog_table.sql."""

from __future__ import annotations

from pathlib import Path
from string import Template

from setup.client import execute_sql, find_warehouse_id, workspace_client
from setup.config import Config

SQL_FILE = Path(__file__).resolve().parent.parent / "01_create_catalog_table.sql"


def _split_statements(rendered: str) -> list[str]:
    return [s.strip() for s in rendered.split(";") if s.strip()]


def run(cfg: Config) -> None:
    w = workspace_client(cfg.profile)
    wh_id = find_warehouse_id(w, cfg.warehouse_name)
    print(f">> using warehouse {wh_id}")

    template = Template(SQL_FILE.read_text())
    rendered = template.safe_substitute(
        ZB_CATALOG=cfg.catalog,
        ZB_SCHEMA=cfg.schema,
        ZB_TABLE=cfg.table,
    )

    for stmt in _split_statements(rendered):
        execute_sql(w, wh_id, stmt)

    print(">> all statements applied")
