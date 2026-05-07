"""Zerobus demo setup CLI.

Replaces the old bash setup scripts. Talks to Databricks via databricks-sdk.

  python -m setup.cli create-table
  python -m setup.cli register-sp --client-id ... --client-secret ... [--sp-id ...]
  python -m setup.cli grant
  python -m setup.cli deploy-pipeline [--start]
  python -m setup.cli deploy-dashboard
  python -m setup.cli all [--client-id ...] [--client-secret ...] [--sp-id ...] [--start-pipeline]
  python -m setup.cli cleanup [--keep-sp] [--keep-data]

All subcommands accept --profile / --catalog / --schema / --table /
--sp-display-name / --warehouse to override the env-var defaults.
"""

from __future__ import annotations

import argparse
import sys

from setup.config import load_config
from setup.steps import (
    cleanup,
    create_catalog_table,
    deploy_dashboard,
    deploy_pipeline,
    grant_permissions,
    register_service_principal,
)


def _add_common_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--profile", help="Databricks CLI profile name")
    p.add_argument("--catalog", help="UC catalog name")
    p.add_argument("--schema", help="UC schema name")
    p.add_argument("--table", help="Bronze table name")
    p.add_argument("--sp-display-name", dest="sp_display_name", help="Service principal display name")
    p.add_argument("--warehouse", dest="warehouse_name", help="SQL warehouse name")


def _add_sp_credential_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--client-id", dest="client_id", help="OAuth client_id (SP applicationId)")
    p.add_argument("--client-secret", dest="client_secret", help="OAuth client_secret")
    p.add_argument("--sp-id", dest="sp_id", help="SP numeric id (used by cleanup to delete the SP)")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m setup.cli")
    sub = parser.add_subparsers(dest="cmd", required=True)

    for name in ("create-table", "grant", "deploy-dashboard"):
        sp = sub.add_parser(name)
        _add_common_args(sp)

    sp_register = sub.add_parser("register-sp", help="Record an externally-created SP's credentials in .env")
    _add_common_args(sp_register)
    _add_sp_credential_args(sp_register)

    sp_pipe = sub.add_parser("deploy-pipeline")
    _add_common_args(sp_pipe)
    sp_pipe.add_argument("--start", action="store_true", help="Kick off an update after create")

    sp_all = sub.add_parser("all", help="Run every setup step in order")
    _add_common_args(sp_all)
    _add_sp_credential_args(sp_all)
    sp_all.add_argument("--start-pipeline", action="store_true")

    sp_clean = sub.add_parser("cleanup", help="Tear down everything created by `all`")
    _add_common_args(sp_clean)
    sp_clean.add_argument("--keep-data", action="store_true", help="Don't drop the catalog")
    sp_clean.add_argument("--keep-sp", action="store_true", help="Don't delete the service principal")

    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    cfg = load_config(
        profile=args.profile,
        catalog=args.catalog,
        schema=args.schema,
        table=args.table,
        sp_display_name=args.sp_display_name,
        warehouse_name=args.warehouse_name,
    )
    print(f"profile={cfg.profile}  target={cfg.fqn}  sp={cfg.sp_display_name}")

    if args.cmd == "create-table":
        create_catalog_table.run(cfg)
    elif args.cmd == "register-sp":
        register_service_principal.run(
            cfg,
            client_id=args.client_id,
            client_secret=args.client_secret,
            sp_id=args.sp_id,
        )
    elif args.cmd == "grant":
        grant_permissions.run(cfg)
    elif args.cmd == "deploy-pipeline":
        deploy_pipeline.run(cfg, start=args.start)
    elif args.cmd == "deploy-dashboard":
        deploy_dashboard.run(cfg)
    elif args.cmd == "all":
        create_catalog_table.run(cfg)
        register_service_principal.run(
            cfg,
            client_id=args.client_id,
            client_secret=args.client_secret,
            sp_id=args.sp_id,
        )
        grant_permissions.run(cfg)
        deploy_pipeline.run(cfg, start=args.start_pipeline)
        deploy_dashboard.run(cfg)
        print(">> all setup steps complete")
    elif args.cmd == "cleanup":
        cleanup.run(cfg, drop_data=not args.keep_data, delete_sp=not args.keep_sp)

    return 0


if __name__ == "__main__":
    sys.exit(main())
