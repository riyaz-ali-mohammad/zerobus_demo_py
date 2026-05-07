#!/usr/bin/env python3
"""Zerobus IoT sensor simulator.

Streams synthetic sensor readings into a Unity Catalog Delta table via the
Databricks Zerobus Ingest API. Intended to be run from a laptop during a live
demo so the "no message bus, no broker, just push" story is visible.

Example:
    pip install -r simulator/requirements.txt
    python simulator/sensor_simulator.py --rate 100 --duration 120 --anomaly-rate 0.02
"""

from __future__ import annotations

import argparse
import math
import multiprocessing
import os
import random
import signal
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import logging

from dotenv import load_dotenv

try:
    from zerobus.sdk.sync import ZerobusSdk
    from zerobus.sdk.shared import RecordType, StreamConfigurationOptions, TableProperties

except ImportError:
    sys.stderr.write(
        "error: databricks-zerobus-ingest-sdk not installed. "
        "Run: pip install -r simulator/requirements.txt\n"
    )
    raise

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)


SENSORS = {
    # sensor_type -> (unit, baseline, amplitude, noise_sigma, alert_abs_threshold)
    "temperature": ("C",   65.0, 8.0, 0.8, 90.0),
    "humidity":    ("%",   45.0, 12.0, 1.5, 85.0),
    "vibration":   ("mm/s", 2.5, 1.2, 0.3, 8.0),
    "pressure":    ("kPa", 101.3, 0.8, 0.2, 110.0),
}

DEFAULT_SITES = ["plant-01", "plant-02", "warehouse-a", "warehouse-b", "lab-03"]


@dataclass
class Device:
    device_id: str
    site: str
    sensor_type: str
    phase: float  # per-device phase offset so readings don't all move together


def build_fleet(sites: list[str], devices_per_site: int) -> list[Device]:
    fleet: list[Device] = []
    for site in sites:
        for i in range(devices_per_site):
            for sensor in SENSORS:
                fleet.append(
                    Device(
                        device_id=f"{site}-{sensor[:3]}-{i:03d}",
                        site=site,
                        sensor_type=sensor,
                        phase=random.random() * 2 * math.pi,
                    )
                )
    return fleet


def _utc_epoch() -> float:
    """UTC instant as Unix epoch seconds."""
    return time.time()


def make_reading(dev: Device, t: float, anomaly_rate: float) -> dict:
    unit, baseline, amp, sigma, threshold = SENSORS[dev.sensor_type]
    # baseline + slow sinusoidal drift (period ~5 min) + gaussian noise
    drift = amp * math.sin(dev.phase + (2 * math.pi * t / 300.0))
    value = baseline + drift + random.gauss(0.0, sigma)

    status = "OK"
    if random.random() < anomaly_rate:
        # inject a clearly anomalous spike above threshold
        value = threshold + abs(random.gauss(5.0, 2.0))
        status = "ALERT"
    elif abs(value - baseline) > 2.5 * sigma:
        status = "WARN"

    ts = _utc_epoch()
    return {
        "device_id": dev.device_id,
        "site": dev.site,
        "sensor_type": dev.sensor_type,
        "reading_value": round(value, 3),
        "unit": unit,
        "status": status,
        "event_time": ts,
    }


def _run_producer(
    worker_id: int,
    fleet: list[Device],
    rate: float,
    duration: float,
    anomaly_rate: float,
    flush_interval: float,
    client_id: str,
    client_secret: str,
    table_fqn: str,
    host: str = "https://e2-demo-field-eng.cloud.databricks.com",
    zb_endpoint: str = "1444828305810485.zerobus.us-west-2.cloud.databricks.com",
) -> None:
    """Single-process producer loop. Creates its own Zerobus stream.

    A background daemon thread calls stream.flush() every `flush_interval`
    seconds so records become queryable with bounded latency without blocking
    the hot producer loop. Set flush_interval=0 to disable (records will only
    be flushed at shutdown — max throughput, unbounded freshness).
    """
    tag = f"[w{worker_id}]"
    sdk = ZerobusSdk(
        zb_endpoint,
        host,
    )
    table_properties = TableProperties(table_name=table_fqn)
    options = StreamConfigurationOptions(record_type=RecordType.JSON)
    stream = sdk.create_stream(client_id, client_secret, table_properties, options)

    stop_event = threading.Event()

    def _handle_sigint(signum, frame):
        stop_event.set()
        print(f"\n{tag} ^C received, draining...", flush=True)

    signal.signal(signal.SIGINT, _handle_sigint)

    def _flush_loop():
        # wait() returns True if stop_event was set (shutdown), False on timeout (normal cadence).
        while not stop_event.wait(flush_interval):
            try:
                stream.flush()
            except Exception as e:
                print(f"{tag} background flush error: {e}", flush=True)

    flusher: threading.Thread | None = None
    if flush_interval > 0:
        flusher = threading.Thread(target=_flush_loop, name=f"zerobus-flusher-{worker_id}", daemon=True)
        flusher.start()

    started = time.monotonic()
    next_tick = started
    interval = 1.0 / rate if rate > 0 else 0.0
    sent = 0

    try:
        while not stop_event.is_set():
            if duration and (time.monotonic() - started) >= duration:
                break

            dev = fleet[sent % len(fleet)]
            record = make_reading(dev, time.monotonic() - started, anomaly_rate)
            stream.ingest_record(record)  # returned ack is intentionally discarded — at-least-once, fire-and-forget
            sent += 1

            next_tick += interval
            sleep_for = next_tick - time.monotonic()
            if sleep_for > 0:
                time.sleep(sleep_for)
            else:
                # falling behind target rate — skip pacing and continue
                next_tick = time.monotonic()
    finally:
        stop_event.set()
        if flusher is not None:
            flusher.join(timeout=max(flush_interval * 2, 5.0))
        print(f"{tag} flushing final batch...", flush=True)
        stream.flush()
        stream.close()
        elapsed = time.monotonic() - started
        avg = sent / elapsed if elapsed else 0
        print(f"{tag} done: sent={sent} in {elapsed:.1f}s (avg {avg:.0f}/s)", flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rate", type=float, default=100.0,
                        help="target events per second, aggregated across workers (default: 100)")
    parser.add_argument("--duration", type=float, default=0.0,
                        help="run for N seconds, 0 = run until Ctrl-C (default: 0)")
    parser.add_argument("--anomaly-rate", type=float, default=0.01,
                        help="fraction of records flagged as ALERT (default: 0.01)")
    parser.add_argument("--devices-per-site", type=int, default=10,
                        help="devices per site per sensor type (default: 10)")
    parser.add_argument("--sites", nargs="+", default=DEFAULT_SITES,
                        help=f"site names (default: {' '.join(DEFAULT_SITES)})")
    parser.add_argument("--flush-interval", type=float, default=2.0,
                        help="seconds between background flush() calls (default: 2.0). "
                             "Lower = fresher data, slightly less throughput. "
                             "Set to 0 to disable (flush only at shutdown; max throughput).")
    parser.add_argument("--workers", type=int, default=1,
                        help="parallel producer processes; aggregate --rate is divided "
                             "evenly across them (default: 1). Use 4+ for rates above ~5000/s.")
    parser.add_argument("--env-file", default=None,
                        help="path to .env (default: repo root .env)")
    args = parser.parse_args()

    if args.workers < 1:
        sys.stderr.write("error: --workers must be >= 1\n")
        return 2

    repo_root = Path(__file__).resolve().parent.parent
    load_dotenv(args.env_file or repo_root / ".env")

    host = os.environ.get("DATABRICKS_HOST")
    client_id = os.environ.get("ZB_CLIENT_ID")
    client_secret = os.environ.get("ZB_CLIENT_SECRET")
    table_fqn = os.environ.get("ZB_TABLE_FQN")
    zb_endpoint = os.environ.get("ZB_ENDPOINT", "1444828305810485.zerobus.us-west-2.cloud.databricks.com")
    missing = [k for k, v in (
        ("DATABRICKS_HOST", host),
        ("ZB_CLIENT_ID", client_id),
        ("ZB_CLIENT_SECRET", client_secret),
        ("ZB_TABLE_FQN", table_fqn),
    ) if not v]
    if missing:
        sys.stderr.write(f"error: missing env vars: {', '.join(missing)}\n")
        return 2

    fleet = build_fleet(args.sites, args.devices_per_site)
    print(f"zerobus simulator: {len(fleet)} virtual sensors across "
          f"{len(args.sites)} sites -> {table_fqn}")
    print(f"target rate: {args.rate:.0f} events/s  workers: {args.workers}  "
          f"anomaly rate: {args.anomaly_rate:.2%}  "
          f"duration: {'infinite' if args.duration == 0 else f'{args.duration:.0f}s'}")

    if args.workers == 1:
        _run_producer(
            0, fleet, args.rate, args.duration, args.anomaly_rate,
            args.flush_interval, client_id, client_secret, table_fqn, host, zb_endpoint,
        )
        return 0

    per_worker_rate = args.rate / args.workers
    shards = [fleet[i::args.workers] for i in range(args.workers)]
    procs: list[multiprocessing.Process] = []
    for i in range(args.workers):
        p = multiprocessing.Process(
            target=_run_producer,
            args=(i, shards[i], per_worker_rate, args.duration, args.anomaly_rate,
                  args.flush_interval, client_id, client_secret, table_fqn, host, zb_endpoint),
            name=f"zerobus-worker-{i}",
        )
        p.start()
        procs.append(p)

    # Ctrl-C in the foreground terminal is delivered to the whole process
    # group; each child installs its own SIGINT handler and drains.
    try:
        for p in procs:
            p.join()
    except KeyboardInterrupt:
        for p in procs:
            p.join()

    return 0


if __name__ == "__main__":
    sys.exit(main())
