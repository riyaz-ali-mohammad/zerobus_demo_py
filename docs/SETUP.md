# Setup — step by step

Walks from a fresh checkout to a running demo. Target workspace is
**`e2-demo-west`**; the CLI profile of the same name (or whatever you set as
`DATABRICKS_PROFILE` / `--profile`) must exist in `~/.databrickscfg`.

All setup is driven by `python -m setup.cli` — no shell scripts, no `databricks`
CLI required.

## 0. Prerequisites

| Requirement | How to check | How to install |
|-------------|--------------|----------------|
| Python 3.9+ | `python3 --version` | `brew install python` |
| Workspace profile in `~/.databrickscfg` | the file lists `[e2-demo-west]` (or your chosen profile) with a `host` and OAuth/PAT auth | `databricks auth login --profile e2-demo-west --host https://...` (the CLI is only used for one-time profile creation; the demo no longer needs it) |
| Admin-ish permissions | Ability to create catalogs + service principals on the target workspace | Ask the workspace admin |
| Running serverless warehouse | The named warehouse exists and is `RUNNING` | Start one in the SQL UI (`Shared endpoint` is the default) |

A running warehouse is required for `create-table` (Statement Execution API),
`grant` (UC grants are workspace-API not warehouse-bound, so technically not
needed there), and `deploy-dashboard` (Lakeview pins the dataset to a warehouse).

## 1. Clone / open the repo

```bash
cd path/to/kar_zerobus_demo
```

## 2. Install dependencies

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt        # SDK + python-dotenv (setup deps)
pip install -r simulator/requirements.txt   # zerobus ingest SDK (simulator deps)
```

## 3. Run setup

Happy path — runs every step in order. You'll need an SP + secret you created
in the workspace UI (see step 3b for how); pass them in here:

```bash
python -m setup.cli all \
  --client-id <applicationId> \
  --client-secret <secret> \
  --sp-id <numeric-id> \
  --start-pipeline
```

(Or pre-populate `.env` and omit the credential flags.)

…or do it step by step (each step is idempotent):

### 3a. Catalog, schema, bronze table

```bash
python -m setup.cli create-table
```

The script renders `${ZB_CATALOG}`/`${ZB_SCHEMA}`/`${ZB_TABLE}` from your config
into `setup/01_create_catalog_table.sql` and submits each statement through the
Statement Execution API on the configured warehouse.

Expected output:
```
profile=e2-demo-west  target=karthik_auto_validator_1.iot.sensor_readings  sp=zerobus-demo-sp
>> using warehouse 0123abc...
>> running: CREATE CATALOG IF NOT EXISTS karthik_auto_validator_1 ...
   SUCCEEDED
>> running: CREATE SCHEMA IF NOT EXISTS karthik_auto_validator_1.iot ...
   SUCCEEDED
>> running: CREATE OR REPLACE TABLE karthik_auto_validator_1.iot.sensor_readings ...
   SUCCEEDED
>> all statements applied
```

### 3b. Service principal + OAuth secret

The demo doesn't programmatically mint the OAuth secret (that endpoint requires
elevated privileges most demo presenters don't have). Instead, **create the SP
and secret yourself in the workspace UI** and tell the CLI to record them:

1. Workspace UI > **Settings** > **Identity and access** > **Service principals**
2. **Add service principal** (display name `zerobus-demo-sp` matches the CLI default)
3. Open the SP, click **Generate secret**, copy the secret immediately — it's
   shown once
4. Note the SP's **applicationId** (UUID, used as `client_id`) and the
   **numeric id** (used by `cleanup` to delete the SP)

Then register them:

```bash
python -m setup.cli register-sp \
  --client-id <applicationId> \
  --client-secret <secret> \
  --sp-id <numeric-id>
```

This writes `.env` (umask 600) with:

- `DATABRICKS_HOST` (resolved from your CLI profile)
- `ZB_CLIENT_ID` (the SP's `applicationId`)
- `ZB_CLIENT_SECRET`
- `ZB_TABLE_FQN`
- `ZB_SP_ID` (numeric id; only present if you passed `--sp-id`)

**Alternative inputs** (any of these work; flags > env vars > existing `.env`):

```bash
# via env vars
ZB_CLIENT_ID=... ZB_CLIENT_SECRET=... ZB_SP_ID=... python -m setup.cli register-sp

# via pre-populated .env
cp .env.example .env
# edit .env to fill in ZB_CLIENT_ID / ZB_CLIENT_SECRET / ZB_SP_ID
python -m setup.cli register-sp     # picks values up from .env, refreshes ZB_TABLE_FQN
```

**Without `--sp-id`** the registration still works, but `cleanup` won't be able
to delete the SP — it'll print a skip notice and leave the SP intact. Pass
`--sp-id` if you want full teardown.

### 3c. Grants

```bash
python -m setup.cli grant
```

Uses the **UC Grants API** (`w.grants.update`) — no SQL. Reads `ZB_CLIENT_ID`
from `.env` and applies:

- `USE_CATALOG` on the catalog
- `USE_SCHEMA` on the schema
- `SELECT, MODIFY` on the bronze table

### 3d. Pipeline

```bash
python -m setup.cli deploy-pipeline --start
```

Uploads `pipeline/sensor_pipeline.py` to `/Workspace/Shared/zerobus_demo/` and
creates the `zerobus_iot_demo` Lakeflow Declarative Pipeline from
`pipeline/pipeline.json`. With `--start`, kicks off the first update; otherwise
start it from the Lakeflow UI. If a pipeline with the same name already exists,
the create is skipped (still safe to re-run).

### 3e. Dashboard

```bash
python -m setup.cli deploy-dashboard
```

Creates the `Zerobus IoT Demo` Lakeview dashboard under
`/Workspace/Shared/zerobus_demo/`. Widgets show "no data" until the simulator
starts pushing rows.

## 4. Smoke test

Short 10-second burst to verify the whole path:

```bash
python simulator/sensor_simulator.py --rate 50 --duration 10 --anomaly-rate 0.1
```

Expected output:
```
zerobus simulator: 200 virtual sensors across 5 sites -> karthik_auto_validator_1.iot.sensor_readings
target rate: 50 events/s  anomaly rate: 10.00%  duration: 10s
  sent=     100  rate=   50.0/s  elapsed=   2.0s
  ...
flushing final batch...
done: sent=500 in 10.0s (avg 50/s)
```

Confirm rows landed by pasting `queries/01_freshness.sql` into the SQL editor —
expect `rows_total ≈ 500` and `e2e_latency_p95_s < 5`.

## 5. Ready to demo

Jump to `docs/DEMO_GUIDE.md`.

## Cleanup

```bash
python -m setup.cli cleanup
```

Stops + deletes the pipeline, drops the catalog (CASCADE), deletes the SP, and
removes `.env`. `--keep-data` skips the drop; `--keep-sp` skips the delete.
