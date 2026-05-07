"""Lakeflow Declarative Pipeline: Zerobus bronze -> silver -> gold.

Deployed as a continuous, serverless pipeline (see pipeline/pipeline.json). The
bronze table `karthik_auto_validator_1.iot.sensor_readings` is written by the Zerobus
Ingest API; this pipeline cleans it, produces per-minute aggregations, and
surfaces anomalies.
"""

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, TimestampType


BRONZE = "karthik_auto_validator_1.iot.sensor_readings"


SENSOR_THRESHOLDS = {
    "temperature": 90.0,
    "humidity":    85.0,
    "vibration":    8.0,
    "pressure":   110.0,
}


@dlt.table(
    name="sensor_readings_clean",
    comment="Silver: deduplicated, typed sensor readings from Zerobus bronze.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)
@dlt.expect_or_drop("device_id_not_null", "device_id IS NOT NULL")
@dlt.expect_or_drop("event_time_not_null", "event_time IS NOT NULL")
@dlt.expect("known_sensor_type",
            "sensor_type IN ('temperature','humidity','vibration','pressure')")
def sensor_readings_clean():
    return (
        spark.readStream.table(BRONZE)
        .withColumn("event_time", F.col("event_time").cast(TimestampType()))
        .withColumn("ingestion_time", F.current_timestamp())
        .withColumn("reading_value", F.col("reading_value").cast(DoubleType()))
        .withColumn("ingest_lag_ms",
                    (F.col("ingestion_time").cast("double") -
                     F.col("event_time").cast("double")) * 1000)
        .withWatermark("event_time", "2 minutes")
        .dropDuplicates(["device_id", "event_time"])
    )


@dlt.table(
    name="sensor_minute_agg",
    comment="Gold: 1-minute tumbling aggregation per (site, sensor_type).",
    table_properties={"quality": "gold"},
)
def sensor_minute_agg():
    return (
        dlt.readStream("sensor_readings_clean")
        .withWatermark("event_time", "2 minutes")
        .groupBy(
            F.window("event_time", "1 minute").alias("w"),
            F.col("site"),
            F.col("sensor_type"),
        )
        .agg(
            F.avg("reading_value").alias("avg_value"),
            F.min("reading_value").alias("min_value"),
            F.max("reading_value").alias("max_value"),
            F.count(F.lit(1)).alias("n_readings"),
            F.sum(F.when(F.col("status") == "ALERT", 1).otherwise(0)).alias("n_alerts"),
            F.avg("ingest_lag_ms").alias("avg_ingest_lag_ms"),
        )
        .select(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            "site", "sensor_type",
            "avg_value", "min_value", "max_value",
            "n_readings", "n_alerts", "avg_ingest_lag_ms",
        )
    )


def _threshold_predicate():
    expr = None
    for sensor, limit in SENSOR_THRESHOLDS.items():
        cond = (F.col("sensor_type") == sensor) & (F.col("reading_value") > F.lit(limit))
        expr = cond if expr is None else (expr | cond)
    return expr


@dlt.table(
    name="sensor_anomalies",
    comment="Gold: rows flagged ALERT or exceeding per-sensor hard thresholds.",
    table_properties={"quality": "gold"},
)
def sensor_anomalies():
    return (
        dlt.readStream("sensor_readings_clean")
        .where((F.col("status") == "ALERT") | _threshold_predicate())
        .select(
            "event_time", "ingestion_time", "ingest_lag_ms",
            "device_id", "site", "sensor_type",
            "reading_value", "unit", "status",
        )
    )
