# Databricks Zerobus — End-to-End Demo

Live demo of **Zerobus Ingest** (GA Feb 2026): a serverless push API that streams
directly into Unity Catalog Delta tables — no Kafka, no Kinesis, no Connect
cluster. The simulator runs from your laptop over gRPC.

Target workspace: **`e2-demo-west`** (profile in `~/.databrickscfg`).

```
[laptop: sensor_simulator.py] --gRPC--> [Zerobus] --> bronze: sensor_readings
                                                           |
                                         Lakeflow Declarative Pipeline
                                             |                   |
                                  silver: _clean       gold: _minute_agg, _anomalies
                                                           |
                                                Lakeview dashboard + SQL
```

## Documentation

| Doc | Read when |
|-----|-----------|
| [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) | You want to understand how the pieces fit together, the data model, and why each component exists. |
| [`docs/SETUP.md`](docs/SETUP.md) | First-time install — prerequisites, step-by-step with expected output, verification, cleanup. |
| [`docs/DEMO_GUIDE.md`](docs/DEMO_GUIDE.md) | Running the demo live — pre-show checklist, act-by-act talk track, objection prep, reset between demos. |
| [`docs/TROUBLESHOOTING.md`](docs/TROUBLESHOOTING.md) | Something broke. Errors grouped setup → auth → ingest → pipeline → dashboard. |

## Layout

| Path | Purpose |
|------|---------|
| `setup/` | Catalog, table, service principal, grants |
| `simulator/` | Local Python generator using `databricks-zerobus-ingest-sdk` |
| `pipeline/` | Lakeflow Declarative Pipeline (silver + gold) |
| `dashboard/` | Lakeview dashboard definition |
| `queries/` | Copy-pasteable SQL for live-demo moments |
| `docs/` | Architecture, setup, demo guide, troubleshooting |

## One-time setup

All setup runs through a single Python CLI that talks to Databricks via
`databricks-sdk` (no `databricks` CLI / `jq` / `envsubst` required).

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 0. (one-time, in the Databricks workspace UI)
#    Settings > Identity and access > Service principals > Add
#    Open the SP, click "Generate secret", copy the secret immediately.
#    Note the SP's applicationId (UUID) — that's your client_id.
#    Optionally note the numeric id too — needed if you want `cleanup` to delete the SP.

# Happy path — runs every step below in order. Pass the SP creds you got in step 0.
python -m setup.cli all \
  --client-id <applicationId> \
  --client-secret <secret> \
  --sp-id <numeric-id> \
  --start-pipeline

# Or run them individually:
python -m setup.cli create-table                     # 1. catalog + bronze table (SQL via Statement Execution API)
python -m setup.cli register-sp \                    # 2. write .env from creds you generated in the UI
  --client-id <applicationId> --client-secret <secret> --sp-id <numeric-id>
python -m setup.cli grant                            # 3. UC grants (USE_CATALOG, USE_SCHEMA, SELECT, MODIFY)
python -m setup.cli deploy-pipeline --start          # 4. uploads sensor_pipeline.py + creates the LDP
python -m setup.cli deploy-dashboard                 # 5. creates the Lakeview dashboard
```

You can also pre-populate `.env` (copy `.env.example` → `.env`, fill in
`ZB_CLIENT_ID` / `ZB_CLIENT_SECRET` / `ZB_SP_ID`) and run `register-sp` /
`all` with no flags — values are picked up from `.env` automatically.

Override defaults with `--profile`, `--catalog`, `--schema`, `--table`,
`--sp-display-name`, `--warehouse` (or the matching `ZB_*` env vars). Defaults
match the demo: profile `e2-demo-field-engg`, catalog `karthik_auto_validator_1`,
schema `iot`, table `sensor_readings`, SP `zerobus-demo-sp`, warehouse `Shared endpoint`.

## Simulator — local laptop

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r simulator/requirements.txt

# default: 100 events/s, single process, infinite, 1% ALERTs
python simulator/sensor_simulator.py

# demo-friendly, short burst with loud anomalies
python simulator/sensor_simulator.py --rate 200 --duration 90 --anomaly-rate 0.15

# high-throughput: 15k events/s across 4 producer processes
python simulator/sensor_simulator.py --rate 15000 --workers 4 --duration 120
```

The script loads `.env` from the repo root (written by `python -m setup.cli register-sp`).

**`--workers N`** runs N parallel producer processes (each with its own Zerobus
stream) and divides the aggregate `--rate` across them. Single-threaded Python
tops out in the low thousands/s because of SDK-side work; splitting across
processes is how we reach 10k+ events/s. Tune `N` to your physical core count
— more workers than cores hurts throughput.

**`--flush-interval S`** (default `2.0`) sets how often a background daemon
thread inside each worker calls `stream.flush()`. `flush()` is what makes the
buffered records visible in the Delta table; without it, `ingest_record_nowait`
can sit in the SDK's queue for over a minute. The timer thread keeps the hot
producer loop unblocked while bounding data freshness to roughly `flush-interval
+ 1s`. Set to `0` to disable background flushing (max throughput, unbounded
freshness — records only flush at shutdown). Lower values give fresher data at
a small throughput cost.

## Live demo flow (~6 minutes)

1. **Open SQL editor**, run `queries/01_freshness.sql` — `rows_total = 0`. Set the stage:
   "no message bus, nothing streaming yet."
2. **Start simulator locally** from a terminal the customer can see:
   ```
   python simulator/sensor_simulator.py --rate 150 --duration 120
   ```
   Narrate the laptop → Unity Catalog path. No cluster, no broker.
3. **Re-run `01_freshness.sql`** every ~10s. Counters climb, `seconds_since_last_row` stays
   < 5s, `e2e_latency_p95_s` stays under ~5s — **this is the story**.
4. **Open the Lakeview dashboard** (`/Workspace/Shared/zerobus_demo/Zerobus IoT Demo`).
   Counters, throughput, per-minute trends all live.
5. **Trigger anomalies**: Ctrl-C the simulator, restart with:
   ```
   python simulator/sensor_simulator.py --rate 150 --anomaly-rate 0.25
   ```
   Run `queries/04_anomalies.sql` — gold anomaly rows appear within ~seconds of the spike
   (bronze -> silver -> gold all continuous).
6. **Wrap**: run `queries/02_throughput.sql` for the totals. Point out that this whole path
   is one SDK call + one declarative pipeline — the customer's code stays on their edge.

## Cleanup

```bash
# Stop the simulator first (Ctrl-C), then:
python -m setup.cli cleanup
```

Stops + deletes the `zerobus_iot_demo` pipeline, drops the catalog (CASCADE),
deletes the service principal, and removes `.env`. Pass `--keep-data` or
`--keep-sp` to skip either side.

## Talk-track cheat sheet

| Moment | Point to make |
|--------|----------------|
| Simulator starts | No Kafka / Kinesis / Connect cluster. Just SDK + OAuth. |
| Freshness query | Single-digit seconds end-to-end, from laptop to UC. |
| Dashboard | Same Delta table is queryable immediately by any UC-aware tool. |
| Anomaly flip | Bronze, silver, gold are a continuous declarative pipeline — no glue code. |
| Cleanup | Zerobus is billed as Lakeflow Jobs Serverless — scales to zero. |
