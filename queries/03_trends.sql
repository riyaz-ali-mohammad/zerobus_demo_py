-- Demo moment: "gold table already populated" — per-minute trends produced by the LDP pipeline.
SELECT
  window_start,
  site,
  sensor_type,
  ROUND(avg_value, 2) AS avg_value,
  n_readings,
  n_alerts,
  ROUND(avg_ingest_lag_ms, 1) AS avg_ingest_lag_ms
FROM zerobus_demo.iot.sensor_minute_agg
WHERE window_start >= current_timestamp() - INTERVAL 15 MINUTES
ORDER BY window_start DESC, site, sensor_type
LIMIT 50;
