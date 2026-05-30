-- IoT pipeline PostgreSQL schema
-- Run: docker exec -i data-postgres1 psql -U admin -d data_hub < schema.sql

CREATE TABLE IF NOT EXISTS sensor_aggregates (
    id               SERIAL PRIMARY KEY,
    window_start     TIMESTAMPTZ NOT NULL,
    window_end       TIMESTAMPTZ NOT NULL,
    machine_id       VARCHAR(32)  NOT NULL,
    machine_type     VARCHAR(64),
    floor            VARCHAR(8),
    shift            VARCHAR(16),
    avg_temperature  DOUBLE PRECISION,
    avg_vibration    DOUBLE PRECISION,
    avg_rpm          DOUBLE PRECISION,
    avg_power_kw     DOUBLE PRECISION,
    avg_health_score DOUBLE PRECISION,
    avg_efficiency   DOUBLE PRECISION,
    event_count      INTEGER      NOT NULL DEFAULT 0,
    fault_count      INTEGER      NOT NULL DEFAULT 0,
    anomaly_count    INTEGER      NOT NULL DEFAULT 0,
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sensor_agg_machine_window
    ON sensor_aggregates (machine_id, window_start DESC);

CREATE INDEX IF NOT EXISTS idx_sensor_agg_window_end
    ON sensor_aggregates (window_end DESC);

COMMENT ON TABLE sensor_aggregates IS
    '1-minute windowed aggregates written by Spark Structured Streaming (JDBC sink)';
