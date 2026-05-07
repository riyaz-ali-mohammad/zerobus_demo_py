"""Create the Zerobus IoT Lakeview dashboard."""

from __future__ import annotations

import json

from databricks.sdk.service.dashboards import Dashboard

from setup.client import find_warehouse_id, workspace_client
from setup.config import REPO_ROOT, Config

DASHBOARD_FILE = REPO_ROOT / "dashboard" / "iot_sensors.lvdash.json"
PARENT_PATH = "/Workspace/Shared/zerobus_demo"
DISPLAY_NAME = "Zerobus IoT Demo"


def run(cfg: Config) -> str:
    w = workspace_client(cfg.profile)
    wh_id = find_warehouse_id(w, cfg.warehouse_name)

    serialized = json.dumps(json.loads(DASHBOARD_FILE.read_text()), separators=(",", ":"))

    resp = w.lakeview.create(
        dashboard=Dashboard(
            display_name=DISPLAY_NAME,
            warehouse_id=wh_id,
            parent_path=PARENT_PATH,
            serialized_dashboard=serialized,
        )
    )
    print(f">> dashboard created under {PARENT_PATH} (id={resp.dashboard_id})")
    return resp.dashboard_id
