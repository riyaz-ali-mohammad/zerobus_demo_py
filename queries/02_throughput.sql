-- Demo moment: "how much volume are we pushing?" — rolling throughput windows.
SELECT
  COUNT(CASE WHEN ingestion_time >= current_timestamp() - INTERVAL 1  MINUTES THEN 1 END) AS rows_1m,
  COUNT(CASE WHEN ingestion_time >= current_timestamp() - INTERVAL 5  MINUTES THEN 1 END) AS rows_5m,
  COUNT(CASE WHEN ingestion_time >= current_timestamp() - INTERVAL 60 MINUTES THEN 1 END) AS rows_60m,
  ROUND(
    COUNT(CASE WHEN ingestion_time >= current_timestamp() - INTERVAL 1 MINUTES THEN 1 END) / 60.0,
    1
  ) AS events_per_second_1m
FROM zerobus_demo.iot.sensor_readings;
