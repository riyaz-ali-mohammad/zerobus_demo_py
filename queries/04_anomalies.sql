-- Demo moment: trigger with `--anomaly-rate 0.2` and watch this populate in real time.
SELECT
  event_time,
  device_id,
  site,
  sensor_type,
  reading_value,
  unit,
  status,
  ROUND(ingest_lag_ms, 1) AS ingest_lag_ms
FROM zerobus_demo.iot.sensor_anomalies
WHERE event_time >= current_timestamp() - INTERVAL 10 MINUTES
ORDER BY event_time DESC
LIMIT 50;
