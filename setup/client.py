"""Shared SDK client + warehouse-lookup helper."""

from __future__ import annotations

import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState


def workspace_client(profile: str) -> WorkspaceClient:
    return WorkspaceClient(profile=profile)


def find_warehouse_id(w: WorkspaceClient, name: str) -> str:
    for wh in w.warehouses.list():
        if wh.name == name:
            return wh.id
    raise RuntimeError(
        f"warehouse not found: {name!r}. "
        f"List with: databricks warehouses list --profile {w.config.profile}"
    )


def execute_sql(w: WorkspaceClient, warehouse_id: str, statement: str) -> None:
    """Run one SQL statement, polling until SUCCEEDED or raising on failure."""
    print(f">> running: {statement[:80].strip()}...")
    resp = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=statement,
        wait_timeout="30s",
    )
    state = resp.status.state
    while state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(1)
        resp = w.statement_execution.get_statement(resp.statement_id)
        state = resp.status.state

    if state != StatementState.SUCCEEDED:
        raise RuntimeError(f"statement failed ({state}): {resp.status}")
    print(f"   {state.value}")
