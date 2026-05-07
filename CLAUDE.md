# CLAUDE.md

Guidance for Claude Code when working in this repo. Humans should read `README.md`
and `docs/` first — this file is for the agent.

## What this project is

End-to-end live demo of **Zerobus Ingest** (Databricks serverless push API, GA
Feb 2026). A laptop-run Python simulator pushes synthetic IoT sensor readings
over gRPC directly into a Unity Catalog Delta bronze table — no Kafka, no
Kinesis, no Connect cluster. A Lakeflow Declarative Pipeline produces silver and
gold tables, and a Lakeview dashboard + SQL queries are used during the demo.

Primary audience of the repo is a Databricks SA running the demo live.

## Authoritative docs

Before changing anything non-trivial, read the relevant doc:

- `docs/ARCHITECTURE.md` — data flow, schema, component responsibilities, non-goals.
- `docs/SETUP.md` — step-by-step setup with expected output and verification.
- `docs/DEMO_GUIDE.md` — presenter talk track; changes to the flow must stay in sync.
- `docs/TROUBLESHOOTING.md` — known failure modes (auth, ingest, pipeline, dashboard).

When you change behavior, update the affected doc in the same change.

## Environment and config

- Target workspace: `e2-demo-west` (profile in `~/.databrickscfg`). The setup
  CLI authenticates via `WorkspaceClient(profile=...)`. Override per-invocation
  with `--profile` or the `DATABRICKS_PROFILE` env var.
- Setup is driven by `python -m setup.cli` (see "Running things" below). It
  reads defaults from env vars (`ZB_CATALOG`, `ZB_SCHEMA`, `ZB_TABLE`,
  `ZB_SP_DISPLAY_NAME`, `ZB_WAREHOUSE_NAME`) or accepts CLI flags. Current
  defaults: `karthik_auto_validator_1.iot.sensor_readings`, SP
  `zerobus-demo-sp`, warehouse `Shared endpoint`.
- Simulator secrets live in `.env` at repo root (gitignored). The SP and its
  OAuth secret are created **out-of-band** in the workspace UI (programmatic
  secret minting needs elevated privileges the demo presenter doesn't have).
  `python -m setup.cli register-sp` records them into `.env`; `.env.example`
  documents the keys: `DATABRICKS_HOST`, `ZB_CLIENT_ID`, `ZB_CLIENT_SECRET`,
  `ZB_TABLE_FQN`, `ZB_SP_ID`. `ZB_SP_ID` is optional and only used by
  `cleanup` to delete the SP.
- Python venv is `.venv/` at repo root. Setup deps in `requirements.txt`
  (`databricks-sdk`, `python-dotenv`); simulator deps in
  `simulator/requirements.txt` (key one: `databricks-zerobus-ingest-sdk`).

## Repo layout (and who owns what)

| Path | Purpose | Notes for edits |
|------|---------|-----------------|
| `setup/cli.py` | argparse entrypoint for all setup steps | `python -m setup.cli <subcommand>` |
| `setup/config.py` | `Config` dataclass + env-var loading | Defaults must match `.env.example` and the table below |
| `setup/client.py` | `WorkspaceClient` + warehouse-lookup + SQL exec helpers | Shared by every step |
| `setup/steps/` | One module per subcommand (create-table, register-sp, grant, deploy-pipeline, deploy-dashboard, cleanup) | Each must be idempotent — re-running a step should be a no-op or replace |
| `setup/01_create_catalog_table.sql` | Bronze schema source-of-truth | DDL only; rendered by `create_catalog_table.py` via `string.Template` |
| `simulator/sensor_simulator.py` | Local gRPC producer | Single file, no helper modules |
| `pipeline/sensor_pipeline.py` | `@dlt.table` × 3 (silver + 2 gold) | Serverless continuous |
| `pipeline/pipeline.json` | Lakeflow pipeline create spec | `continuous: true`, `serverless: true` |
| `dashboard/iot_sensors.lvdash.json` | Lakeview dashboard definition | Hand-authored JSON |
| `queries/` | Demo SQL, one file per talk-track moment | Keep fast — last 5–30 min only |
| `docs/` | Architecture, setup, demo guide, troubleshooting | Update alongside code |
| `requirements.txt` | Setup CLI deps | `databricks-sdk`, `python-dotenv` |

## Schema invariants — don't drift

Zerobus **has no schema evolution on ingest**. The bronze schema, the simulator
record dict, and the pipeline casts must match exactly. Column order + types
pinned in `setup/01_create_catalog_table.sql` are source of truth.

Columns: `device_id STRING`, `site STRING`, `sensor_type STRING`,
`reading_value DOUBLE`, `unit STRING`, `status STRING`, `event_time` (DOUBLE
epoch seconds as written by the simulator; silver casts to TIMESTAMP).

If you add a column, update in this order: bronze DDL → simulator `make_reading`
→ silver `sensor_readings_clean` projection → dashboard datasets → docs.

The bronze DDL uses `CREATE TABLE IF NOT EXISTS` (not `CREATE OR REPLACE`) so
re-running setup preserves the underlying Delta table id — replacing the table
breaks the LDP streaming checkpoint with `DIFFERENT_DELTA_TABLE_READ_BY_STREAMING_SOURCE`
and forces a full refresh. To intentionally change the schema, drop the table
manually (or run `cleanup`) and re-run setup.

Sensor set and thresholds are duplicated in two places (intentionally, to keep
each file self-contained): `simulator/sensor_simulator.py:SENSORS` and
`pipeline/sensor_pipeline.py:SENSOR_THRESHOLDS`. Keep them in sync.

## Conventions

- Setup CLI subcommands: idempotent (reuse-if-exists, create-if-missing), fail
  fast on missing config, print what they did. New subcommands go in
  `setup/steps/<name>.py` with a `run(cfg: Config, ...)` entrypoint and a wire-up
  in `setup/cli.py`.
- Setup uses `databricks-sdk` only — no shelling out to the `databricks` CLI,
  no `jq`, no `envsubst`. Use the structured APIs (`w.grants.update`,
  `w.catalogs.create`, etc.) where available; fall back to
  `w.statement_execution.execute_statement` for DDL the SDK doesn't model.
- Python: no frameworks beyond the SDK and `python-dotenv` in the simulator; no
  shared helper modules across `simulator/` and `pipeline/`.
- SQL in `queries/` is copy-paste-friendly for the SQL editor — no templating,
  bounded time ranges, runs fast on a serverless warehouse.
- The simulator uses `stream.ingest_record(record)` and **discards the returned
  `RecordAcknowledgment`** — at-least-once, fire-and-forget. Don't add a
  `.wait()` on the ack: that would convert the call to sync per-record and
  collapse throughput. The Zerobus SDK 0.2.0 collapsed the old `_nowait` /
  `ingest_record` pair into one method where waiting on the ack is opt-in;
  not waiting preserves the original demo semantic.
- The simulator calls `stream.flush()` on a **time-based cadence from a
  background daemon thread** per worker (default every 2 s, via
  `--flush-interval`). This is intentional: mid-loop flushes consumed 76% of
  runtime in profiling (`flush()` is a blocking network call), and
  shutdown-only flushing left records invisible for over a minute. The
  threaded design decouples flush cadence from record cadence. Don't move
  `flush()` back into the producer loop. `--flush-interval 0` disables
  periodic flushing (max throughput, bad freshness — useful only for
  benchmarking).
- High throughput is achieved via `--workers N` (multi-processing), not
  threading — each worker is its own process with its own Zerobus stream.
  Tune `N` to physical cores; more hurts. 15k events/s has been measured
  with 4 workers.

## Things to avoid

- Don't add Kafka, Kinesis, Structured Streaming jobs, or Connect clusters — the
  whole point of the demo is that none of those are needed.
- Don't move the simulator into a notebook. It must run on the laptop during the
  demo so the "push from anywhere" story is visible.
- Don't add schema evolution, CDC consumers, or late-arrival handling beyond the
  existing 2-minute watermark — listed as explicit non-goals.
- Don't hardcode workspace-specific identifiers in new code — read from
  `setup/config.py` (env-var-backed defaults) or `.env`. Note:
  `sensor_simulator.py` and `sensor_pipeline.py` currently contain some
  hardcoded values (the Zerobus endpoint host, workspace URL, and bronze FQN)
  — preserve those if editing nearby but don't add more.

## Running things (quick reference)

```bash
# one-time
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt -r simulator/requirements.txt

# setup — happy path (creds come from UI; see register-sp below for how)
python -m setup.cli all \
  --client-id <applicationId> --client-secret <secret> --sp-id <numeric-id> \
  --start-pipeline

# or step by step
python -m setup.cli create-table        # catalog + schema + bronze table
python -m setup.cli register-sp \       # record creds you generated in the workspace UI
  --client-id <applicationId> --client-secret <secret> --sp-id <numeric-id>
python -m setup.cli grant               # UC grants on catalog/schema/table
python -m setup.cli deploy-pipeline --start
python -m setup.cli deploy-dashboard

# simulator
python simulator/sensor_simulator.py --rate 150 --duration 120 --anomaly-rate 0.15

# high throughput
python simulator/sensor_simulator.py --rate 15000 --workers 4 --duration 120

# tear down
python -m setup.cli cleanup
```

## When in doubt

Ask before: dropping the catalog, deleting the SP, force-pushing, or changing
the bronze schema. These either cost a demo reset or require re-deploying the
pipeline.
