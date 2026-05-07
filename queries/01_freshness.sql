-- Demo moment: "look how fresh this is" — max ingestion timestamp vs now, and end-to-end latency.
SELECT
  COUNT(*)                                                            AS rows_total,
  MAX(ingestion_time)                                                 AS last_ingested_at,
  CAST(current_timestamp() AS TIMESTAMP)                              AS clock_now,
  ROUND(
    (unix_timestamp(current_timestamp()) - unix_timestamp(MAX(ingestion_time))),
    2
  )                                                                   AS seconds_since_last_row,
  ROUND(percentile_approx(
    unix_timestamp(ingestion_time) - unix_timestamp(event_time), 0.50), 3) AS e2e_latency_p50_s,
  ROUND(percentile_approx(
    unix_timestamp(ingestion_time) - unix_timestamp(event_time), 0.95), 3) AS e2e_latency_p95_s
FROM karthik_auto_validator_1.iot.sensor_readings
WHERE ingestion_time >= current_timestamp() - INTERVAL 5 MINUTES;
