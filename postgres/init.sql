CREATE TABLE worker_events_bronze (

    worker_id VARCHAR(50),

    timestamp TIMESTAMPTZ,

    floor VARCHAR(10),

    helmet_on BOOLEAN,

    danger_zone BOOLEAN,

    fatigue_score DOUBLE PRECISION
);

CREATE TABLE worker_events_silver (

    worker_id VARCHAR(50),

    timestamp TIMESTAMPTZ,

    floor VARCHAR(10),

    helmet_on BOOLEAN,

    danger_zone BOOLEAN,

    fatigue_score DOUBLE PRECISION,

    safety_violation_flag INTEGER,

    fatigue_status VARCHAR(20),

    worker_risk_level VARCHAR(20)
);

CREATE TABLE worker_safety_gold (
    worker_id VARCHAR(50),

    window_start TIMESTAMPTZ,

    window_end TIMESTAMPTZ,

    violations_per_window BIGINT,

    workers_in_danger_zone BIGINT,

    avg_fatigue_score DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS machine_events_quarantine (

    event_id VARCHAR(100),

    timestamp TIMESTAMPTZ,

    machine_id VARCHAR(50),

    machine_type VARCHAR(50),

    floor VARCHAR(10),
    shift VARCHAR(20),

    status VARCHAR(20),
    error_code VARCHAR(20),

    is_fault BOOLEAN,

    temperature DOUBLE PRECISION,
    vibration DOUBLE PRECISION,
    rpm DOUBLE PRECISION,
    power_kw DOUBLE PRECISION,

    cnc_oil DOUBLE PRECISION,
    coolant_pressure DOUBLE PRECISION,

    joint_torque DOUBLE PRECISION,
    force DOUBLE PRECISION,

    belt_tension DOUBLE PRECISION,
    load_weight DOUBLE PRECISION,

    flow_rate DOUBLE PRECISION,
    inlet_pressure DOUBLE PRECISION,

    validation_reason VARCHAR(255)
);



CREATE TABLE IF NOT EXISTS machine_events_bronze (
    event_id VARCHAR(100),
    timestamp TIMESTAMPTZ,

    machine_id VARCHAR(50),
    machine_type VARCHAR(50),

    floor VARCHAR(10),
    shift VARCHAR(20),

    status VARCHAR(20),
    error_code VARCHAR(20),

    is_fault BOOLEAN,

    temperature DOUBLE PRECISION,
    vibration DOUBLE PRECISION,
    rpm DOUBLE PRECISION,
    power_kw DOUBLE PRECISION,

    cnc_oil DOUBLE PRECISION,
    coolant_pressure DOUBLE PRECISION,

    joint_torque DOUBLE PRECISION,
    force DOUBLE PRECISION,

    belt_tension DOUBLE PRECISION,
    load_weight DOUBLE PRECISION,

    flow_rate DOUBLE PRECISION,
    inlet_pressure DOUBLE PRECISION
);


CREATE TABLE IF NOT EXISTS machine_events_silver (

    event_id VARCHAR(100) PRIMARY KEY,
    timestamp TIMESTAMPTZ,

    machine_id VARCHAR(50),
    machine_type VARCHAR(50),

    floor VARCHAR(20),
    shift VARCHAR(20),

    status VARCHAR(20),
    error_code VARCHAR(20),

    is_fault BOOLEAN,

    temperature DOUBLE PRECISION,
    vibration DOUBLE PRECISION,
    rpm DOUBLE PRECISION,
    power_kw DOUBLE PRECISION,

    cnc_oil DOUBLE PRECISION,
    coolant_pressure DOUBLE PRECISION,

    joint_torque DOUBLE PRECISION,
    force DOUBLE PRECISION,

    belt_tension DOUBLE PRECISION,
    load_weight DOUBLE PRECISION,

    flow_rate DOUBLE PRECISION,
    inlet_pressure DOUBLE PRECISION,

    temperature_status VARCHAR(20),
    vibration_status VARCHAR(20),

    fault_flag INTEGER,

    event_date DATE,
    event_hour INTEGER,

    health_score DOUBLE PRECISION,

    risk_score DOUBLE PRECISION,

    running_flag INTEGER,

    fault_category VARCHAR(50),

    power_status VARCHAR(20),

    time_bucket VARCHAR(20),

    anomaly_flag INTEGER
);
CREATE TABLE IF NOT EXISTS machine_aggregates_gold (

    machine_id VARCHAR(50),

    window_start TIMESTAMPTZ,
    window_end TIMESTAMPTZ,

    avg_temp DOUBLE PRECISION,
    max_temp DOUBLE PRECISION,

    avg_rpm DOUBLE PRECISION,

    avg_vibration DOUBLE PRECISION,
    max_vibration DOUBLE PRECISION,

    avg_power DOUBLE PRECISION,
    peak_power DOUBLE PRECISION,

    avg_health_score DOUBLE PRECISION,
    min_health_score DOUBLE PRECISION,

    avg_risk_score DOUBLE PRECISION,

    uptime_percentage DOUBLE PRECISION,

    fault_count BIGINT,
    total_events BIGINT,

    fault_percentage DOUBLE PRECISION
);



CREATE OR REPLACE VIEW machine_events_bronze_view AS
SELECT *
FROM machine_events_bronze;


CREATE OR REPLACE VIEW machine_events_silver_view AS
SELECT
    event_id,
    timestamp,
    timestamp AT TIME ZONE 'Africa/Cairo' AS cairo_time,

    machine_id,
    machine_type,

    floor,
    shift,

    status,
    error_code,

    is_fault,

    temperature,
    vibration,
    rpm,
    power_kw,

    cnc_oil,
    coolant_pressure,

    joint_torque,
    force,

    belt_tension,
    load_weight,

    flow_rate,
    inlet_pressure,

    temperature_status,
    vibration_status,

    fault_flag,

    event_date,
    event_hour,

    health_score,
    risk_score,

    running_flag,

    fault_category,

    power_status,

    time_bucket,

    anomaly_flag

FROM machine_events_silver;


CREATE OR REPLACE VIEW machine_aggregates_gold_view AS
SELECT
    machine_id,

    window_start,
    window_end,

    window_start AT TIME ZONE 'Africa/Cairo'
        AS cairo_window_start,

    window_end AT TIME ZONE 'Africa/Cairo'
        AS cairo_window_end,

    avg_temp,
    max_temp,

    avg_rpm,

    avg_vibration,
    max_vibration,

    avg_power,
    peak_power,

    avg_health_score,
    min_health_score,

    avg_risk_score,

    uptime_percentage,

    fault_count,
    fault_percentage,

    total_events

FROM machine_aggregates_gold;

