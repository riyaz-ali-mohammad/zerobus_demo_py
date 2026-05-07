# Troubleshooting

Ordered roughly by where you'd hit the problem: setup → auth → ingest → pipeline → dashboard.

## Setup

### `ModuleNotFoundError: No module named 'databricks.sdk'`
Setup deps aren't installed. `pip install -r requirements.txt` from the repo
root with `.venv` active.

### `databricks.sdk.errors.platform.PermissionDenied` / 401 from any subcommand
Profile auth is broken. Verify `~/.databrickscfg` has the profile you're using
and that its credentials still work:
```bash
databricks auth env --profile <profile>   # only used for this check
```
Re-login if needed: `databricks auth login --profile <profile> --host https://...`.

### Wrong catalog/profile/etc.
The CLI reads from `ZB_CATALOG`/`ZB_SCHEMA`/`ZB_TABLE`/`ZB_SP_DISPLAY_NAME`/
`ZB_WAREHOUSE_NAME`/`DATABRICKS_PROFILE` env vars or matching `--catalog` /
`--schema` / `--table` / `--sp-display-name` / `--warehouse` / `--profile`
flags. Inline override:
```bash
ZB_CATALOG=my_cat python -m setup.cli create-table
# or
python -m setup.cli create-table --catalog my_cat
```

### Non-default catalog/schema names
If you override `ZB_CATALOG`/`ZB_SCHEMA`/`ZB_TABLE`, the setup CLI handles the
substitutions for `01_create_catalog_table.sql` and grants automatically.
These files still reference the defaults and need updating manually:

- `pipeline/sensor_pipeline.py` (the `BRONZE` constant)
- `pipeline/pipeline.json` (`catalog` + `target`)
- `dashboard/iot_sensors.lvdash.json`
- `queries/*.sql`

One-liner to rename everything:
```bash
OLD=karthik_auto_validator_1.iot
NEW="${ZB_CATALOG}.${ZB_SCHEMA}"
grep -rl "$OLD" pipeline/ dashboard/ queries/ \
  | xargs sed -i '' "s|$OLD|$NEW|g"

# pipeline.json splits catalog/target — fix separately:
sed -i '' "s|\"catalog\": \"karthik_auto_validator_1\"|\"catalog\": \"${ZB_CATALOG}\"|" pipeline/pipeline.json
sed -i '' "s|\"target\": \"iot\"|\"target\": \"${ZB_SCHEMA}\"|" pipeline/pipeline.json
```

### `CREATE CATALOG` fails with permission denied
Your user doesn't have `CREATE CATALOG` on the metastore. Options:
1. Ask a workspace admin to create the catalog and grant you `MANAGE`.
2. Use an existing catalog you have rights on — set `ZB_CATALOG` (or pass
   `--catalog`) and apply the rename one-liner above.

## Auth & service principal

### `register-sp`: `missing client_id, client_secret`
You haven't supplied the credentials yet. The CLI doesn't create the SP or
mint its secret — you do that in the workspace UI (Settings > Identity and
access > Service principals > Generate secret) and pass the values in via:
1. `--client-id`, `--client-secret` (and optional `--sp-id`) flags, or
2. `ZB_CLIENT_ID` / `ZB_CLIENT_SECRET` / `ZB_SP_ID` env vars, or
3. a pre-populated `.env` file (copy `.env.example` first).

### `register-sp`: 401 / wrong workspace host
`register-sp` writes `DATABRICKS_HOST` from the SDK's resolved profile host. If
you pre-populated `.env` with the wrong host, re-run `register-sp` and it
overwrites the line. To force a different profile: `--profile <name>` or
`export DATABRICKS_PROFILE=<name>`.

### Simulator: `PERMISSION_DENIED: user does not have MODIFY on table`
Grants didn't apply, or applied to the wrong principal. Inspect via the SDK:
```bash
python -c "from databricks.sdk import WorkspaceClient; from databricks.sdk.service.catalog import SecurableType; w=WorkspaceClient(profile='e2-demo-west'); print(w.grants.get(SecurableType.TABLE, 'karthik_auto_validator_1.iot.sensor_readings'))"
```
The grantee should be the SP's `applicationId` (UUID), not its integer id.
Re-run `python -m setup.cli grant`.

### Simulator: `UNAUTHENTICATED` / `invalid_client`
The OAuth secret is stale or mistyped. Generate a fresh one in the workspace
UI (Service principal page > Generate secret), update `ZB_CLIENT_SECRET` in
`.env`, and re-run `python -m setup.cli register-sp` to refresh the file.

## Simulator

### `ImportError: databricks.zerobus.ingest.api.v1`
Venv isn't active or SDK not installed:
```bash
source .venv/bin/activate
pip install -r simulator/requirements.txt
```

### `AttributeError: type object 'ZerobusClient' has no attribute 'create_stream'`
The GA SDK's entrypoint may differ slightly by version. Check the installed
version and its public API:
```bash
python -c "import databricks.zerobus.ingest.api.v1 as z; print(dir(z))"
```
Adjust the import and `create_stream` call in `simulator/sensor_simulator.py`
to match. The kwargs (`host`, `client_id`, `client_secret`, `table`) are the
stable contract.

### Simulator runs but no rows in bronze
1. Is the simulator's `ZB_TABLE_FQN` matching the actual table?
   ```bash
   grep ZB_TABLE_FQN .env
   ```
2. Are events silently dropping? Add a quick verbose-mode print of the first
   record in `make_reading`.
3. Check the SP still has `MODIFY` (see grants check above).

### Throughput lower than `--rate` target
Single-process Python tops out in the low thousands/s. Use `--workers N` to
spawn N parallel producer processes; each gets its own Zerobus stream and
handles `rate/N` events/s. Rule of thumb: match `N` to physical CPU cores —
more workers than cores thrashes and *lowers* throughput. 15k/s is reachable
with 4 workers on a modern laptop.

If a single worker is slower than expected, the bottleneck is usually one of:
- **Flush cadence too aggressive**. `flush()` is a blocking network round-trip
  that scales with buffered batch size. The default `--flush-interval 2.0`
  is a good balance. If you set it very low (e.g. 0.2s) throughput will drop
  noticeably; if you set it to `0` it goes away entirely but data freshness
  collapses — see the next entry.
- **Network latency to the workspace**. Laptop → us-west ~20 ms is fine;
  hotel wifi may cap you.
- **Laptop CPU saturation**. Watch `top` — if each worker is near 100% CPU,
  you're at the producer-side ceiling. If they're low-utilization, the SDK is
  backpressuring; profile with `python3 -m cProfile -o prof.out simulator/sensor_simulator.py --rate 5000 --duration 30 --workers 1 --flush-interval 0`
  and inspect with `python3 -c "import pstats; pstats.Stats('prof.out').sort_stats('tottime').print_stats(25)"`.

### `seconds_since_last_row` stays tens of seconds or minutes behind
Records are enqueued by `ingest_record_nowait` but only become queryable after
`flush()`. The simulator flushes every `--flush-interval` seconds from a
background daemon thread (default 2 s). If freshness is much worse than that:
- You passed `--flush-interval 0`, which disables periodic flushing entirely
  — records will only flush at shutdown. Drop that flag, or set a small value
  like `1.0`.
- The background flush thread is hitting errors. Look for
  `[wN] background flush error: ...` lines in the simulator output — most
  commonly an auth or network issue that also blocks the foreground flush.
- The pipeline is lagging, not Zerobus. Check the Lakeflow UI for the
  `zerobus_iot_demo` pipeline — if silver/gold are backed up, the bronze rows
  are landing fine but the trend/anomaly dashboards will look stale.

## Declarative pipeline

### Pipeline fails: `Path does not exist: /Workspace/Shared/zerobus_demo/sensor_pipeline.py`
The pipeline-deploy step failed to upload. Re-run:
```bash
python -m setup.cli deploy-pipeline
```

### Pipeline fails: `Table ... not found`
Bronze table wasn't created. Re-run `python -m setup.cli create-table`.

### Pipeline fails: `Delta source requires ChangeDataFeed` or similar
The bronze table was created without CDF. Run:
```sql
ALTER TABLE zerobus_demo.iot.sensor_readings
SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');
```
Or drop & recreate using `python -m setup.cli create-table` (the SQL file sets CDF).

### Gold tables stay empty but silver has rows
The watermark may not have advanced yet. Silver uses a 2-minute watermark;
first minute-agg row lands ~1 minute after the pipeline catches its first
event. Wait ~2 min after starting the simulator.

### `dlt.readStream("sensor_readings_clean")` errors with "not a table"
LDP scopes `dlt.readStream` within the pipeline's live schema. If you see
this, confirm all three `@dlt.table` functions are in the same Python file
(`pipeline/sensor_pipeline.py`) — they are by default.

## Dashboard

### `deploy-dashboard`: `warehouse not found`
`ZB_WAREHOUSE_NAME` doesn't match any warehouse on the workspace. List:
```bash
python -c "from databricks.sdk import WorkspaceClient; w=WorkspaceClient(profile='e2-demo-west'); [print(x.name) for x in w.warehouses.list()]"
```
Set the right name via `ZB_WAREHOUSE_NAME=...` or `--warehouse "..."`.

### Widgets show "no data"
- Bronze is genuinely empty — run the simulator.
- Warehouse is stopped — start it.
- Dashboard is caching — use the refresh button (top-right of the dashboard).
- The dataset query in the JSON hardcodes `karthik_auto_validator_1.iot.*`. If
  you used a different catalog/schema, run the rename one-liner from the
  "Non-default catalog/schema names" section above.

### Dashboard auto-refresh isn't firing
Auto-refresh is a per-dashboard UI setting (not in the JSON). Open the
dashboard, click the clock icon, set "Auto-refresh every 10s."

## Queries

### `Table or view 'sensor_minute_agg' cannot be found`
The pipeline hasn't produced any gold rows yet. Wait 1–2 minutes after
starting the simulator, or check the pipeline UI for errors.

### Latency query reports huge `e2e_latency_p95_s`
You're looking at old data. The query window is `last 5 minutes` — if the
simulator was off for longer than that you'll get zero rows. If you just ran
it, wait ~5s and re-run; Zerobus commits are fast but the first batch after a
dry period can cold-start to ~1–2s.

## Clean-state reset

When all else fails:

```bash
python -m setup.cli cleanup        # stops + deletes pipeline, drops catalog, deletes SP, removes .env
python -m setup.cli all --start-pipeline   # re-create everything
```

Pass `--keep-data` or `--keep-sp` to `cleanup` if you want to preserve either.
