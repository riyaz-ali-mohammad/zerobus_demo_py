"""Grant USE + SELECT/MODIFY on the bronze table to the demo SP via the UC Grants API."""

from __future__ import annotations

from databricks.sdk.service.catalog import PermissionsChange, Privilege

from setup.client import workspace_client
from setup.config import Config, load_dotenv


def run(cfg: Config) -> None:
    env = load_dotenv()
    client_id = env.get("ZB_CLIENT_ID")
    if not client_id:
        raise RuntimeError(
            "missing ZB_CLIENT_ID in .env — run `python -m setup.cli register-sp` first"
        )

    w = workspace_client(cfg.profile)

    # securable_type goes into the URL path; pass the string value rather than
    # the SecurableType enum because the SDK formats path params via str(enum),
    # which yields 'SECURABLETYPE.CATALOG' and gets rejected by the API.
    grants = [
        ("CATALOG", cfg.catalog, [Privilege.USE_CATALOG]),
        ("SCHEMA", f"{cfg.catalog}.{cfg.schema}", [Privilege.USE_SCHEMA]),
        ("TABLE", cfg.fqn, [Privilege.SELECT, Privilege.MODIFY]),
    ]

    for securable_type, full_name, privileges in grants:
        names = ", ".join(p.value for p in privileges)
        print(f">> GRANT {names} ON {securable_type} {full_name} TO `{client_id}`")
        w.grants.update(
            securable_type=securable_type,
            full_name=full_name,
            changes=[PermissionsChange(add=privileges, principal=client_id)],
        )

    print(">> grants applied")
