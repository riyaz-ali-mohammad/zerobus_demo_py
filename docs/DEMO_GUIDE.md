# Demo Guide — presenter talk track

**Audience**: customer stakeholders evaluating streaming ingest options.
**Runtime**: ~8 minutes end to end. ~4 minutes if you skip the anomaly segment.
**Prereq**: everything in `docs/SETUP.md` already done, pipeline is `RUNNING`,
dashboard is open in a browser tab, bronze table is empty.

## List of Databricks Resources already deployed:

- [DLT pipeline](https://e2-demo-field-eng.cloud.databricks.com/pipelines/97ba8148-2c55-48da-bbf0-c9f0bdc3f8fc/updates/03f4cc4f-afaf-47d8-97e6-9887ce359f44?o=1444828305810485)
- [AIBI Dashboard](https://e2-demo-field-eng.cloud.databricks.com/dashboardsv3/01f13cb61ef41f3194578b863c1539d4/published?o=1444828305810485)
- [Unity Catalog Schema](https://e2-demo-field-eng.cloud.databricks.com/explore/data/karthik_auto_validator_1/iot?o=1444828305810485&activeListType=TABLE)
- [Helpful Queries](https://e2-demo-field-eng.cloud.databricks.com/editor/queries/2447239766225356?o=1444828305810485)

## Pre-show checklist (do these 5 minutes before the call)

- [ ] `source .venv/bin/activate`  (Python env active; deps from `requirements.txt` and `simulator/requirements.txt` installed)
- [ ] Pipeline state is `RUNNING` (check the Lakeflow UI, or via SDK: `python -c "from databricks.sdk import WorkspaceClient; w=WorkspaceClient(profile='e2-demo-west'); [print(p.name, p.state) for p in w.pipelines.list_pipelines() if p.name=='zerobus_iot_demo']"`)
- [ ] Dashboard open: `/Workspace/Shared/zerobus_demo/Zerobus IoT Demo`
- [ ] SQL editor open on `e2-demo-west`, warehouse running
- [ ] `queries/01_freshness.sql` pasted in one SQL tab
- [ ] `queries/04_anomalies.sql` pasted in a second SQL tab
- [ ] Terminal window with `python simulator/sensor_simulator.py --help` visible
- [ ] Bronze is empty:
  ```sql
  SELECT COUNT(*) FROM karthik_auto_validator_1.iot.sensor_readings;
  -- expect 0
  ```
- [ ] Browser zoom bumped so numbers are readable on screen share

## Act 1 — the problem (1 min, no tools)

Say something like:

> "Typical streaming architecture: source → agent → Kafka/Kinesis → Structured
> Streaming job → Delta. Four systems, four sets of retries, four places for
> schema drift. Zerobus collapses that: your app code writes directly to a UC
> Delta table. No bus, no Connect cluster, no managed service in the middle
> that you have to operate."

## Act 2 — empty state (30 s)

Show the SQL editor tab with `01_freshness.sql`. Run.

> "Zero rows. This is just a Delta table in Unity Catalog — the catalog is
> `zerobus_demo`, schema `iot`, table `sensor_readings`. Same table any
> notebook, dashboard, or SQL warehouse can already query."

## Act 3 — start the stream (2 min)

Switch to the terminal window. Run:

```bash
python simulator/sensor_simulator.py --rate 150 --duration 180
```

For a high-throughput variant (when the customer asks "but can it handle *real*
volume?"), run:

```bash
python simulator/sensor_simulator.py --rate 15000 --workers 4 --duration 180
```

This spawns 4 producer processes, each with its own Zerobus stream, and sustains
15k events/s — the ingest backend absorbs it comfortably; the laptop is the
limit, not Databricks. Skip this if you don't have screen-share real estate for
multiple log streams.

While it's ramping:

> "This is my laptop. No Kafka running anywhere. No broker. The script
> authenticates to the workspace with an OAuth client secret — same way any
> app would — and calls `ingest_record_nowait` per event. The Zerobus SDK
> handles the gRPC stream; Databricks handles the Delta commit."

After ~10 seconds, switch back to the SQL editor and re-run `01_freshness.sql`.

Point at the output:
- `rows_total` is now ~1500 and climbing.
- `seconds_since_last_row` is under 5.
- `e2e_latency_p95_s` is under 5.

> "The p95 latency from laptop to UC — over the public internet — is single-digit
> seconds. That's the whole story. No tuning, no micro-batch interval to set."

## Act 4 — the dashboard (1.5 min)

Switch to the browser dashboard tab. It should be auto-refreshing.

- The four counters across the top move every 10 seconds.
- The trend chart fills out — each sensor is on its own line, drift is visible.
- The anomalies widgets are still empty (anomaly rate is low by default).

> "Everything on this dashboard is querying the exact same UC table that was
> just getting written. There's no secondary pipeline to materialize — this is
> one table, live."

Run `queries/03_trends.sql` in the SQL editor.

> "And this is the gold aggregation — 1-minute averages, per site, per sensor.
> It's produced by a Lakeflow Declarative Pipeline that reads the bronze as a
> streaming source. Three `@dlt.table` decorators total, no orchestration to
> write."

## Act 5 — trigger anomalies (2 min)

Ctrl-C the simulator. Restart with:

```bash
python simulator/sensor_simulator.py --rate 150 --duration 90 --anomaly-rate 0.25
```

Switch to the SQL editor, run `queries/04_anomalies.sql`.

> "25% anomaly rate — one in four readings is over threshold. Pipeline sees it
> in bronze, silver filters, gold materializes. Watch the anomaly table fill."

Re-run the query every 5–10 seconds. Rows appear within seconds of the
simulator bursts. Flip to the dashboard — the "Anomalies by sensor" bar chart
and the recent-anomalies table are populating.

> "Bronze → silver → gold, all continuous, all declarative. If I change the
> anomaly rule, I change the one `@dlt.table` filter — no re-deployment of
> producer code."

## Act 6 — wrap (1 min)

Stop the simulator (Ctrl-C). Show totals:

```bash
# from the simulator's own final line
done: sent=13500 in 90.0s (avg 150/s)
```

Run `queries/02_throughput.sql`.

> "13k events, serverless pipeline, nothing to scale up or down, no cluster
> to keep warm. The billing mode is Lakeflow Jobs Serverless — you pay for
> compute only while it's committing. Scales to zero between events."

## Objection prep — likely questions

| Q | A |
|---|---|
| "How does this compare to Structured Streaming from Kafka?" | You skip Kafka entirely. Structured Streaming stays as an option downstream (our silver/gold here). Zerobus replaces the producer→bus→consumer hop. |
| "What's the maximum throughput?" | Single stream: 100 MB/s. Single table: 10+ GB/s aggregate. Thousands of concurrent streams per workspace. The laptop simulator in this demo can sustain 15k events/s with `--workers 4`; the ingest backend isn't the limiter at that scale, laptop CPU is. |
| "Delivery guarantees?" | `ingest_record_nowait` is at-least-once. Sync `ingest_record` with idempotency keys gives exactly-once. We're using nowait here for throughput. |
| "Schema evolution?" | Not on the bronze ingest side — schema is pinned at `CREATE TABLE`. If you need evolution, you pivot via silver (`ALTER TABLE` on silver is fine). |
| "Auth model?" | OAuth 2.0 client credentials via a service principal. No PATs. Secrets are one-time-viewable. |
| "What about AT&T's region / data residency?" | Same as the workspace. Zerobus runs co-located with the UC metastore; no cross-region hops. |
| "Cost?" | Volume-based under the Lakeflow Jobs Serverless SKU. We can get a sizing estimate via Quicksizer. |
| "What if my app is on-prem / no egress?" | Zerobus needs outbound gRPC to the workspace. We support AWS/Azure PrivateLink for workspaces that require it. |
| "Python only?" | SDKs: Python, Java, Rust, Go, TypeScript. REST API (beta) for anything else. |

## Fallback: "I can't run the simulator" path

If the laptop path fails mid-call:
- The same `sensor_simulator.py` runs inside a Databricks notebook — attach to
  any compute, `%pip install databricks-zerobus-ingest-sdk`, copy the body of
  `main()`, paste credentials from `.env`.
- Or demo against a pre-loaded table: run the simulator yourself 10 minutes
  before and just walk through the queries/dashboard on static-but-recent data.

## Reset between demos

For a fast reset (keeps pipeline + SP, just clears data) paste these into the
SQL editor on the demo warehouse:

```sql
TRUNCATE TABLE karthik_auto_validator_1.iot.sensor_readings;
TRUNCATE TABLE karthik_auto_validator_1.iot.sensor_readings_clean;
TRUNCATE TABLE karthik_auto_validator_1.iot.sensor_minute_agg;
TRUNCATE TABLE karthik_auto_validator_1.iot.sensor_anomalies;
```

For a full reset:

```bash
python -m setup.cli cleanup
python -m setup.cli all --start-pipeline
```

Re-run Act 2 onwards.
