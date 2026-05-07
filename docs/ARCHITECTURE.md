# Architecture & Implementation Details

## What Zerobus is, in one paragraph

Zerobus Ingest is a fully managed, serverless push API from Databricks (GA Feb 2026)
that writes directly into a Unity Catalog Delta table. A client app opens a gRPC
stream, authenticates with OAuth client credentials, and calls `ingest_record` per
event. Databricks handles buffering, exactly-once semantics, and the commit into
Delta — **no Kafka, no Kinesis, no Connect cluster, no Structured Streaming job**
sits between the producer and the lakehouse. End-to-end p95 latency is typically
under 5 seconds; a single stream handles 100 MB/s, and tables can absorb 10+ GB/s
aggregate.

## End-to-end data flow

```
┌────────────────────┐
│ sensor_simulator.py│   (local laptop; optionally N worker
│  - 200 virtual     │    processes for high-rate runs)
│    devices         │
│  - gaussian + drift│
│  - ALERT injection │
└─────────┬──────────┘
          │ gRPC + OAuth (client_credentials)
          │ ingest_record_nowait(record)   — one stream per worker
          ▼
┌─────────────────────────────────┐
│  Zerobus Ingest API             │  Serverless, managed by Databricks
│  (direct writer to Delta)       │
└─────────────┬───────────────────┘
              │  commits to Delta
              ▼
┌─────────────────────────────────────────────┐
│ BRONZE: zerobus_demo.iot.sensor_readings    │  schema fixed at CREATE TABLE
└─────────────┬───────────────────────────────┘
              │  streaming read
              ▼
┌─────────────────────────────────────────────┐
│         Lakeflow Declarative Pipeline        │  serverless, continuous
│                                              │
│  silver: sensor_readings_clean               │  dedup + type enforce + lag column
│     │                                        │
│     ├── gold: sensor_minute_agg              │  1-min tumbling, per site+sensor
│     │                                        │
│     └── gold: sensor_anomalies               │  status='ALERT' OR threshold bust
└─────────────┬───────────────────────────────┘
              │
              ▼
┌──────────────────────────┐      ┌────────────────────┐
│  Lakeview dashboard      │      │  SQL editor queries│
│  (auto-refresh, 10 s)    │      │  (freshness, etc.) │
└──────────────────────────┘      └────────────────────┘
```

## Component breakdown

### 1. Bronze table (`setup/01_create_catalog_table.sql`)

The bronze table is **the Zerobus write target**. Zerobus requires a pre-existing
Delta table with a fixed schema — there is no schema evolution on ingest, so we
pin every column up front.

| Column | Type | Notes |
|--------|------|-------|
| `device_id` | STRING NOT NULL | Composite of site + sensor prefix + index |
| `site` | STRING NOT NULL | Logical grouping for gold aggregation |
| `sensor_type` | STRING NOT NULL | `temperature` / `humidity` / `vibration` / `pressure` |
| `reading_value` | DOUBLE NOT NULL | Numeric measurement |
| `unit` | STRING | Human-readable unit (`C`, `%`, `mm/s`, `kPa`) |
| `status` | STRING | `OK` / `WARN` / `ALERT` — emitted by the simulator |
| `event_time` | TIMESTAMP NOT NULL | Client-side event time |
| `ingestion_time` | TIMESTAMP NOT NULL | Client-side capture time (≈ send time) |

`delta.enableChangeDataFeed = 'true'` is set so downstream streaming reads in the
declarative pipeline are efficient. CDF is the mechanism LDP uses when reading
from a Delta table as a streaming source.

### 2. Service principal + OAuth (`setup/steps/register_service_principal.py`)

Zerobus authenticates via OAuth 2.0 client credentials. Minting an OAuth
secret programmatically (`w.service_principal_secrets_proxy.create`) requires
elevated workspace privileges most demo presenters don't have, so the demo
intentionally **doesn't** create the SP or its secret in code. Instead the
presenter:

1. Creates the SP in the workspace UI (Settings > Identity and access >
   Service principals).
2. Generates a secret on the SP page (one-time view).
3. Notes the SP's `applicationId` (used as `client_id`) and numeric `id`
   (used later by `cleanup`).

`register-sp` then **records** those credentials. It resolves
`client_id` / `client_secret` / `sp_id` from (in priority order) explicit
flags → `ZB_*` env vars → pre-populated `.env`, then writes `.env` at the
repo root with `umask 600`. `DATABRICKS_HOST` is pulled from
`w.config.host` (no extra config needed). `.env` is gitignored.

The numeric `sp_id` is optional — it's only used by `cleanup` to delete the
SP. Without it, the SP is left intact during teardown.

### 3. Grants (`setup/steps/grant_permissions.py`)

The SP needs:

- `USE CATALOG` on the catalog
- `USE SCHEMA` on the catalog's schema
- `SELECT` + `MODIFY` on the bronze table

`MODIFY` is what lets Zerobus write. Applied via the **UC Grants API**
(`w.grants.update(securable_type, full_name, changes=[PermissionsChange(add=[...], principal=...)])`)
— no SQL, no warehouse needed. The principal is the SP's `applicationId`.

### 4. Local simulator (`simulator/sensor_simulator.py`)

Pure-Python, ~170 lines, one dependency: `databricks-zerobus-ingest-sdk`.
Design notes:

- **Fleet shape**: `sites × devices_per_site × sensor_types` → by default
  `5 × 10 × 4 = 200` virtual sensors. Device IDs are stable (e.g.
  `plant-01-tem-007`) so gold aggregations are consistent across runs.
- **Value model**: `baseline + amplitude·sin(phase + 2πt/300s) + N(0, σ)`.
  Each device has a per-device phase offset so readings don't move in lockstep.
  This makes charts look like real telemetry.
- **Status logic**:
  - With probability `--anomaly-rate`, override to a clearly over-threshold
    value and stamp `status='ALERT'`.
  - Otherwise, values beyond 2.5σ are stamped `WARN`.
  - Everything else is `OK`.
- **Throughput & pacing**: loop pre-computes an interval `1/rate`. Uses a
  monotonic "next tick" target so it holds steady. Falls through (no sleep) if
  it's behind schedule. Single-process ceiling is low thousands of events/s;
  `--workers N` spawns N independent producer processes and divides the
  aggregate `--rate` across them. 15k events/s is reachable with 4–6 workers
  on a modern laptop; the limit is laptop CPU, not the ingest backend.
- **SDK calls**:
  - `stream = ZerobusSdk(...).create_stream(client_id, client_secret, table_properties, options)`
    at startup — once per worker process.
  - `stream.ingest_record_nowait(record)` per event — fire-and-forget,
    enqueues into the SDK's internal buffer.
  - `stream.flush()` on a **time-based cadence from a background daemon thread**
    (default every 2 s, via `--flush-interval`). `flush()` is a blocking
    network round-trip; calling it inside the hot loop consumed 76% of
    runtime in profiling, and not calling it at all left records buffered
    for over a minute before the SDK's own flush kicked in. Running it from a
    separate thread decouples flush cadence from record cadence — the producer
    loop stays unblocked while data freshness is bounded to roughly
    `flush-interval + 1 s`. Pass `--flush-interval 0` to disable (max
    throughput, unbounded freshness).
  - A final `stream.flush()` in the `finally` block drains everything before
    `stream.close()`. On long, high-rate runs this final flush can take
    several seconds — don't kill the process mid-drain.
- **Signal handling**: SIGINT sets a flag; the main loop drains and flushes
  cleanly, then reports total sent + average rate.

### 5. Declarative pipeline (`pipeline/sensor_pipeline.py`)

Three `@dlt.table`s, deployed as a **serverless continuous** pipeline:

| Table | Kind | Source | Logic |
|-------|------|--------|-------|
| `sensor_readings_clean` | Silver | `spark.readStream.table(BRONZE)` | Cast value to DOUBLE; compute `ingest_lag_ms`; 2-min watermark; dedup on `(device_id, event_time)`. Expectations drop rows with null IDs. |
| `sensor_minute_agg` | Gold | `dlt.readStream("sensor_readings_clean")` | Stateful streaming aggregation: 1-minute tumbling window, grouped by `(site, sensor_type)`. Emits avg/min/max/count and alert count. |
| `sensor_anomalies` | Gold | `dlt.readStream("sensor_readings_clean")` | Streaming filter: `status='ALERT'` OR `reading_value > threshold(sensor_type)`. |

Deployed via `python -m setup.cli deploy-pipeline`
(`setup/steps/deploy_pipeline.py`):
1. `w.workspace.mkdirs("/Workspace/Shared/zerobus_demo")`
2. `w.workspace.upload(...)` for `sensor_pipeline.py`
3. `w.pipelines.create(...)` from the spec in `pipeline/pipeline.json`
4. Pass `--start` to also call `w.pipelines.start_update(...)`; otherwise the
   operator starts it from the Lakeflow UI.

`pipeline.json` uses `"continuous": true` and `"serverless": true` — the right
knobs for a live demo where you want updates to land within seconds of a bronze
write rather than waiting for a batched trigger.

### 6. Lakeview dashboard (`dashboard/iot_sensors.lvdash.json`)

Five datasets, seven widgets on one page:

| Widget | Type | Dataset | Purpose |
|--------|------|---------|---------|
| Rows ingested (5m) | Counter | `freshness` | Visible growth during the demo |
| Events / second (1m) | Counter | `throughput` | Real-time rate proof |
| E2E latency p95 (s) | Counter | `freshness` | The money metric — < 5s |
| Sec since last row | Counter | `freshness` | Liveness indicator |
| Avg reading / minute by sensor | Line | `minute_trends` | Gold is flowing |
| Anomalies by sensor (30m) | Bar | `anomalies_by_sensor` | Reacts to `--anomaly-rate` |
| Recent anomalies | Table | `anomalies_recent` | Drill-in moment |

Dashboards in Databricks use the Lakeview serialized JSON format.
`python -m setup.cli deploy-dashboard` (`setup/steps/deploy_dashboard.py`)
calls `w.lakeview.create(Dashboard(...))` with the serialized JSON, the
resolved warehouse id, and the parent workspace path.

### 7. Demo SQL (`queries/`)

Four tight queries mapped to specific moments in the talk track:

- `01_freshness.sql` — "how fresh" + p50/p95 latency
- `02_throughput.sql` — rolling 1m/5m/60m counts
- `03_trends.sql` — reads gold minute agg
- `04_anomalies.sql` — reads gold anomalies table

Each query targets the last 5–30 minutes so results are bounded and fast on a
serverless warehouse.

## Why this design

- **Push > pull for the demo**: the point of Zerobus is "your code calls the
  lakehouse directly," so the simulator runs on the laptop, not in a notebook.
- **Silver + gold via Lakeflow Declarative Pipelines**: zero glue code, and
  visibly "managed" — the customer sees decorators, not Structured Streaming
  boilerplate.
- **Separate bronze and gold anomaly tables**: reveals the separation between
  raw ingest (Zerobus's job) and downstream enrichment (LDP's job), which is
  the real architectural story.
- **Serverless warehouse + serverless pipeline**: no cluster startup during the
  demo.

## Non-goals / explicit trade-offs

- No CDC, no late-arriving data handling beyond the 2-minute watermark.
- No Change Data Feed consumer downstream — bronze CDF is enabled only to make
  the silver streaming read efficient.
- The dashboard JSON is hand-authored for clarity; production dashboards would
  typically be built in the Lakeview UI and exported.
- The simulator uses `ingest_record_nowait` (at-least-once by default). A
  production ingest path wanting exactly-once would use the sync `ingest_record`
  with an idempotency key.

## File-by-file map

```
zerobus_test/
├── README.md                             # Operator-facing quickstart
├── .env.example                          # Template for .env (gitignored)
├── .gitignore
├── docs/
│   ├── ARCHITECTURE.md                   # This document
│   ├── SETUP.md                          # Detailed setup with expected output
│   ├── DEMO_GUIDE.md                     # Presenter talk track + timing
│   └── TROUBLESHOOTING.md                # Errors → fixes
├── requirements.txt                      # Setup CLI deps (databricks-sdk, python-dotenv)
├── setup/
│   ├── cli.py                            # `python -m setup.cli <subcommand>`
│   ├── config.py                         # Config dataclass + env-var defaults
│   ├── client.py                         # Shared SDK client + warehouse / SQL helpers
│   ├── 01_create_catalog_table.sql       # Catalog, schema, bronze DDL (rendered by Python)
│   └── steps/
│       ├── create_catalog_table.py       # Render + execute the SQL via SDK
│       ├── register_service_principal.py # Record externally-created SP creds → .env
│       ├── grant_permissions.py          # Apply UC grants via Grants API
│       ├── deploy_pipeline.py            # Upload + create pipeline
│       ├── deploy_dashboard.py           # Create Lakeview dashboard
│       └── cleanup.py                    # Tear down pipeline / catalog / SP / .env
├── simulator/
│   ├── sensor_simulator.py               # Local gRPC producer
│   └── requirements.txt
├── pipeline/
│   ├── sensor_pipeline.py                # @dlt.table × 3
│   └── pipeline.json                     # Pipeline create spec
├── dashboard/
│   └── iot_sensors.lvdash.json           # Lakeview dashboard definition
└── queries/
    ├── 01_freshness.sql
    ├── 02_throughput.sql
    ├── 03_trends.sql
    └── 04_anomalies.sql
```
