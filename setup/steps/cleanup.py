"""Tear down the demo: stop+delete the pipeline, drop the catalog, delete the SP, remove .env."""

from __future__ import annotations

from databricks.sdk.errors import NotFound

from setup.client import execute_sql, find_warehouse_id, workspace_client
from setup.config import ENV_FILE, Config, load_dotenv

PIPELINE_NAME = "zerobus_iot_demo"


def _delete_pipeline(w) -> None:
    pipeline_id = None
    for p in w.pipelines.list_pipelines(filter=f"name LIKE '{PIPELINE_NAME}'"):
        if p.name == PIPELINE_NAME:
            pipeline_id = p.pipeline_id
            break

    if pipeline_id is None:
        print(f">> pipeline {PIPELINE_NAME} not found; skipping")
        return

    print(f">> stopping pipeline {pipeline_id}")
    try:
        w.pipelines.stop(pipeline_id)
    except NotFound:
        pass
    print(f">> deleting pipeline {pipeline_id}")
    w.pipelines.delete(pipeline_id)


def _drop_catalog(w, cfg: Config) -> None:
    wh_id = find_warehouse_id(w, cfg.warehouse_name)
    print(f">> dropping catalog {cfg.catalog} (CASCADE)")
    execute_sql(w, wh_id, f"DROP CATALOG IF EXISTS {cfg.catalog} CASCADE")


def _delete_service_principal(w) -> None:
    env = load_dotenv()
    sp_id = env.get("ZB_SP_ID")
    if not sp_id:
        print(">> ZB_SP_ID not in .env; skipping SP deletion")
        return
    print(f">> deleting service principal {sp_id}")
    try:
        w.service_principals.delete(id=sp_id)
    except NotFound:
        print(">> service principal already gone")


def run(cfg: Config, drop_data: bool = True, delete_sp: bool = True) -> None:
    w = workspace_client(cfg.profile)

    _delete_pipeline(w)
    if drop_data:
        _drop_catalog(w, cfg)
    if delete_sp:
        _delete_service_principal(w)

    if ENV_FILE.exists():
        ENV_FILE.unlink()
        print(f">> removed {ENV_FILE}")

    print(">> cleanup done")
