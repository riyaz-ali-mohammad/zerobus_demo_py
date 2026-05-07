-- Run:
--   source setup/00_env.sh
--   databricks sql-queries ... OR paste into the SQL editor on $DATABRICKS_PROFILE
-- Substitute ${ZB_CATALOG}/${ZB_SCHEMA}/${ZB_TABLE} with values from 00_env.sh if not using envsubst.

CREATE CATALOG IF NOT EXISTS ${ZB_CATALOG}
  COMMENT 'Zerobus IoT ingestion demo';

CREATE SCHEMA IF NOT EXISTS ${ZB_CATALOG}.${ZB_SCHEMA}
  COMMENT 'Bronze/silver/gold IoT sensor data for the Zerobus demo';

-- IF NOT EXISTS rather than OR REPLACE: a replace would create a new Delta
-- table id and break the LDP's streaming checkpoint (DIFFERENT_DELTA_TABLE_READ
-- _BY_STREAMING_SOURCE). To intentionally change the bronze schema, drop the
-- table by hand first or run `python -m setup.cli cleanup` and re-run.
CREATE TABLE IF NOT EXISTS ${ZB_CATALOG}.${ZB_SCHEMA}.${ZB_TABLE} (
  device_id      STRING   NOT NULL,
  site           STRING   NOT NULL,
  sensor_type    STRING   NOT NULL,
  reading_value  DOUBLE   NOT NULL,
  unit           STRING,
  status         STRING,
  event_time     DOUBLE   NOT NULL
)
COMMENT 'Zerobus bronze: raw sensor records pushed directly via the Zerobus Ingest API';
