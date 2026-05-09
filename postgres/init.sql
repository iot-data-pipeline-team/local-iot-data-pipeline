CREATE TABLE IF NOT EXISTS iot_data (
    device_id TEXT,
    device_type TEXT,
    location TEXT,
    technician TEXT,
    temperature DOUBLE PRECISION,
    humidity DOUBLE PRECISION,
    status TEXT,
    timestamp TIMESTAMPTZ,
    anomaly_flag INTEGER
);

CREATE TABLE IF NOT EXISTS iot_aggregates (
    device_id TEXT,
    window_start TIMESTAMPTZ,
    window_end TIMESTAMPTZ,
    avg_temp DOUBLE PRECISION,
    avg_humidity DOUBLE PRECISION
);

CREATE OR REPLACE VIEW iot_data_view AS
SELECT
    device_id,
    device_type,
    location,
    technician,
    temperature,
    humidity,
    status,
    timestamp,
    timestamp AT TIME ZONE 'Africa/Cairo' AS cairo_time,
    anomaly_flag,
    CASE 
        WHEN timestamp >= NOW() - INTERVAL '60 seconds'
        THEN 1
        ELSE 0
    END AS is_recent
FROM iot_data;


CREATE VIEW iot_aggregates_view AS
SELECT
    device_id,

    window_start,

    window_end,

    window_start AT TIME ZONE 'Africa/Cairo' AS cairo_window_start,

    window_end AT TIME ZONE 'Africa/Cairo' AS cairo_window_end,

    avg_temp,
    avg_humidity

FROM iot_aggregates;