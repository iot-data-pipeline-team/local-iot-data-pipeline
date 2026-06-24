# CNC Factory — Real-Time IoT Streaming Pipeline

A real-time data engineering pipeline for a CNC manufacturing factory. The system simulates an industrial IoT gateway that reads sensor data from 4 CNC machines and 4 workers, streams the data through Kafka, processes it with PySpark Structured Streaming using a Medallion architecture (Bronze → Silver → Gold), and writes results to Elasticsearch, MinIO, and PostgreSQL for visualization and analysis.

```
IoT Machines (CNC_01, ROB_01, CNV_01, PMP_01)
        ↓  Simulated sensor readings
ahmed_producer.py  ──────────────────────────────┐
                                                  ↓
Workers (W001, W002, W003, W004)             Kafka Cluster (3 brokers)
        ↓  Simulated wearable sensors             ↓
worker_producer.py ──────────────────────────┘   ↓
                                            streaming_job.py (PySpark)
                                                  ↓
                    ┌─────────────────────────────┤
                    ↓                             ↓                     ↓
             Elasticsearch                      MinIO                PostgreSQL
             (Kibana dashboards)         (Parquet archive)       (Power BI / SQL)
```

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Project Structure](#project-structure)
- [Kafka Cluster](#kafka-cluster)
- [Producers](#producers)
  - [Machine Producer (ahmed_producer.py)](#machine-producer-ahmed_producerpy)
  - [Worker Producer (worker_producer.py)](#worker-producer-worker_producerpy)
- [PySpark Streaming Job (streaming_job.py)](#pyspark-streaming-job-streaming_jobpy)
  - [Machine Pipeline](#machine-pipeline)
  - [Worker Pipeline](#worker-pipeline)
  - [Sink Summary](#sink-summary)
- [Data Quality — Quarantine Layer](#data-quality--quarantine-layer)
- [Schema Reference](#schema-reference)
  - [Machine Event Schema](#machine-event-schema)
  - [Worker Event Schema](#worker-event-schema)
- [PostgreSQL Tables](#postgresql-tables)
- [Elasticsearch Indices & Templates](#elasticsearch-indices--templates)
- [MinIO Storage Layout](#minio-storage-layout)
- [Computed Fields Reference](#computed-fields-reference)
- [Error Codes](#error-codes)
- [Resetting the Project](#resetting-the-project)

---

## Architecture Overview

The pipeline follows the **Medallion architecture** with three layers:

| Layer | Description | What happens |
|---|---|---|
| **Bronze** | Raw ingested data | Kafka JSON is parsed and flattened. Nested sensor structs are promoted to top-level columns. No transformations, no filtering — this is the raw record as received. |
| **Quarantine** | Invalid / dirty records | Any record that fails validation rules is diverted here with a `validation_reason` explaining why it was rejected. These records are stored in PostgreSQL for investigation. |
| **Silver** | Cleaned and enriched data | Validated records with computed business fields added: `health_score`, `risk_score`, `temperature_status`, `fault_category`, `alert_level`, etc. |
| **Gold** | Aggregated KPIs | 1-minute windowed aggregations per machine (avg/max temp, fault %, uptime %). 5-minute windowed aggregations per worker (violations, danger zone count, avg fatigue). |

Both the **machine** and **worker** event streams go through this same 4-layer process independently inside the same `streaming_job.py`.

---

## Prerequisites

**Infrastructure (Docker):**

- Docker Desktop (or Docker Engine + Docker Compose)
- At least 8 GB RAM available for Docker (Kafka × 3 + Spark + Elasticsearch + PostgreSQL + MinIO + Jupyter)

**Python (for the producers, run locally outside Docker):**

```bash
pip install -r requirements.txt
```

`requirements.txt` contains:
```
kafka-python==2.3.1
tzdata==2026.2
```

**Spark JAR dependencies** (already referenced in `run_project.bat`):

| JAR | Purpose |
|---|---|
| `spark-sql-kafka-0-10_2.12:3.5.0` | Kafka source/sink for PySpark |
| `elasticsearch-spark-30_2.12:8.13.0` | Elasticsearch connector for PySpark |
| `hadoop-aws:3.3.4` | S3A filesystem for MinIO access |
| `postgresql-42.6.2.jar` | JDBC driver for PostgreSQL writes |

The `.jar` files are mounted into the Jupyter container from `./jars/`. You must download them manually and place them in the `jars/` folder before starting.

---

## Quick Start

```bat
# Windows — runs the full stack automatically
run_project.bat
```

What `run_project.bat` does step by step:

1. Starts infrastructure: Zookeeper, 3 Kafka brokers, Elasticsearch, PostgreSQL, MinIO, Kibana
2. Runs `kafka-init` — creates the 3 Kafka topics (`sensor-events`, `sensor-processed`, `worker-events`)
3. Runs `elasticsearch-init` — applies all 4 index templates and creates the 4 indices
4. Runs `kibana-init` — imports pre-built Kibana dashboards from `kibana/exports/export.ndjson`
5. Runs `minio-init` — creates the `iot-data` bucket
6. Starts Kafka UI, Spark master/worker, and Jupyter
7. Waits 15 seconds for Jupyter to be ready
8. Opens a new terminal and launches the PySpark streaming job via `spark-submit`
9. Opens two more terminals for the machine producer and the worker producer

**Service URLs after startup:**

| Service | URL |
|---|---|
| Kafka UI | http://localhost:12000 |
| Jupyter | http://localhost:8888 |
| Spark UI | http://localhost:8081 |
| Elasticsearch | http://localhost:9200 |
| Kibana | http://localhost:5601 |
| MinIO Console | http://localhost:9001 |

---

## Project Structure

```
├── ahmed_producer.py          # Machine sensor producer (4 machines, round-robin)
├── worker_producer.py         # Worker safety producer (4 workers, round-robin)
├── streaming_job.py           # PySpark Structured Streaming — full pipeline
├── docker-compose.yml         # Full infrastructure definition
├── run_project.bat            # One-command Windows startup script
├── reset.sh                   # Tears down Docker volumes + clears checkpoints
├── requirements.txt           # Python dependencies for the producers
│
├── postgres/
│   └── init.sql               # Creates all PostgreSQL tables and views on first start
│
├── elasticsearch/
│   └── templates/
│       ├── machine_template.json           # Index template for machine-events
│       ├── machine_aggregates_template.json # Index template for machine-aggregates
│       ├── worker_template.json            # Index template for worker-events
│       └── worker_safety_template.json     # Index template for worker-safety
│
├── kibana/
│   └── exports/
│       └── export.ndjson      # Pre-built Kibana dashboards (auto-imported on startup)
│
├── jars/                      # Spark JARs (you must download these manually)
│   └── postgresql-42.6.2.jar
│
└── minio_data/                # MinIO local data directory (auto-created)
```

---

## Kafka Cluster

### Configuration

| Parameter | Value | Reason |
|---|---|---|
| Mode | Zookeeper (cp-kafka 7.6.0) | Classic mode — 3 brokers, Zookeeper for metadata |
| Brokers | 3 | High availability; any 1 broker can fail |
| Internal ports | `kafka1:9092`, `kafka2:9093`, `kafka3:9094` | Used inside Docker network by PySpark |
| External ports | `localhost:19092`, `localhost:19093`, `localhost:19094` | Used by producers running on the host machine |
| `KAFKA_MIN_INSYNC_REPLICAS` | 2 | A write is only confirmed when at least 2 replicas have it |
| `acks` (producer) | `all` | Producer waits for all in-sync replicas to acknowledge |
| Compression | gzip | Reduces network and storage usage |

### Topics

| Topic | Partitions | Replication | Producer | Consumer |
|---|---|---|---|---|
| `sensor-events` | 6 | 3 | `ahmed_producer.py` | PySpark streaming job |
| `sensor-processed` | 6 | 3 | PySpark streaming job (silver output) | Downstream consumers |
| `worker-events` | 6 | 3 | `worker_producer.py` | PySpark streaming job |

### Why separate internal and external listeners?

Kafka brokers need to advertise different addresses depending on who is connecting:

- **Internal** (`PLAINTEXT://kafka1:9092`): used by PySpark running inside the Docker network. The hostname `kafka1` resolves inside Docker.
- **External** (`EXTERNAL://localhost:19092`): used by the Python producers running on your local machine. They cannot reach `kafka1` by that name, so they connect via `localhost` on the mapped port.

If you only had one listener, either the producers or PySpark would fail to connect.

---

## Producers

### Machine Producer (`ahmed_producer.py`)

Simulates 4 CNC factory machines sending sensor readings to the `sensor-events` Kafka topic. Machines are polled in round-robin order (CNC_01 → ROB_01 → CNV_01 → PMP_01 → repeat) with a 1-second pause between each event.

**Machines:**

| Machine ID | Type | Floor | Specific Sensors |
|---|---|---|---|
| `CNC_01` | CNC Milling Machine | A | `oil_level_pct`, `coolant_pressure_bar` |
| `ROB_01` | Robot Arm | B | `joint_torque_nm`, `end_effector_force_n` |
| `CNV_01` | Conveyor Belt | C | `belt_tension_n`, `load_weight_kg` |
| `PMP_01` | Hydraulic Pump | C | `oil_level_pct`, `flow_rate_lpm`, `inlet_pressure_bar` |

**Realistic sensor simulation:**

The producer simulates physical degradation, not random noise:

- **Oil drain**: `CNC_01` and `PMP_01` have an `oil_level` that decreases every reading (`oil_drain` = 0.006% and 0.010% per reading respectively). As oil drops below 30%, a penalty is added to the fault probability — low oil causes friction → temperature rises → vibration increases. This creates correlated faults rather than independent random failures.
- **Fault streaks**: When a fault fires, a `fault_streak` counter (1–4 readings) is set. During a streak, fault probability is tripled. This simulates how real machine faults don't appear for a single reading and then disappear — they persist for several readings.
- **Shift-based load**: The sensor readings are scaled by a `shift_load` multiplier. Morning shift (06–14h) = 1.0 (full load), evening = 0.85, night = 0.60. This means temperature, vibration, and RPM are higher during morning shift.
- **Ambient temperature**: A sinusoidal function adds ambient heat based on the hour of day, peaking in the early afternoon. This affects base temperature readings.

**Fault types by machine:**

| Machine | Fault Type | What happens |
|---|---|---|
| `CNC_01` | `overheat` | Temperature jumps +14–24°C, RPM drops 5–15% |
| `ROB_01` | `rpm_drop` | RPM drops to 35–55% of normal, vibration spikes |
| `CNV_01` | `vibration` | Vibration jumps +3.5–6.0 mm/s |
| `PMP_01` | `overheat` | Temperature jumps +14–24°C, RPM drops 5–15% |

**Intentional dirty data injection** (for testing the quarantine layer):

| Condition | Probability | What it produces |
|---|---|---|
| Missing temperature | 3% | `temperature: null` |
| Impossible temperature | 2% | `temperature: 999` |
| Negative RPM | 1% | `rpm: -500` |
| Empty machine ID | 1% | `machine_id: ""` |
| Null timestamp | 1% | `timestamp: null` |
| Negative power | 1% | `power_kw: -5` |
| Invalid status | 1% | `status: "BROKEN_STATUS"` |

**Run options:**

```bash
python ahmed_producer.py                        # Normal mode, runs forever
python ahmed_producer.py --interval 0           # As fast as possible
python ahmed_producer.py --count 100            # Send exactly 100 events then stop
python ahmed_producer.py --bootstrap localhost:19092  # Override broker address
```

---

### Worker Producer (`worker_producer.py`)

Simulates 4 factory workers with wearable IoT sensors sending safety data to the `worker-events` topic. Workers are also sent in round-robin order with a 1-second interval.

**Workers:**

| Worker ID | Floor | Zone |
|---|---|---|
| `W001` | A | ZONE_1 |
| `W002` | B | ZONE_2 |
| `W003` | C | ZONE_3 |
| `W004` | A | ZONE_1 |

**Sensor logic:**

- `fatigue_score` is random between 10–100. When fatigue > 80, the probability of the worker entering the danger zone increases from 10% to 30%.
- `helmet_on` is `True` 95% of the time (5% chance of a violation).
- `safety_vest_on` is `True` 92% of the time (8% chance of a violation).

**Intentional dirty data injection:**

| Condition | Probability | What it produces |
|---|---|---|
| Negative heart rate | 1% | `heart_rate: -20` |
| Invalid movement status | 1% | `movement_status: "FLYING"` |
| Null timestamp | 1% | `timestamp: null` |
| Empty worker ID | 1% | `worker_id: ""` |

---

## PySpark Streaming Job (`streaming_job.py`)

The streaming job runs two independent pipelines in parallel inside the same Spark session — one for machine events and one for worker events. Both use `foreachBatch` to write to all sinks from a single Kafka read, ensuring all sinks stay in sync on the same offset.

### Machine Pipeline

#### Step 1 — Read from Kafka

```python
df = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka1:9092,kafka2:9093,kafka3:9094")
    .option("subscribe", "sensor-events")
    .option("startingOffsets", "latest")
    .load()
```

Kafka delivers messages as binary. The raw `value` column is cast to String, then `from_json` parses it using the defined schema.

#### Step 2 — Bronze Layer (parsing + flattening)

The JSON has nested objects (`metrics`, `cnc_sensors`, `robot_sensors`, etc.). Bronze unpacks them into flat columns:

```
metrics.temperature   → temperature
metrics.vibration     → vibration
cnc_sensors.oil_level_pct  → cnc_oil
robot_sensors.joint_torque_nm → joint_torque
... etc.
```

For machine types that don't have a particular sensor (e.g. `ROB_01` has no `cnc_sensors`), the column will be `null`. This is correct — null here means "not applicable", not "missing data". The quarantine layer handles actually missing required data.

Bronze is written to:
- **MinIO**: `s3a://iot-data/bronze/machine_bronze_data/` (Parquet, append)
- **PostgreSQL**: `machine_events_bronze` table

#### Step 3 — Quarantine Layer (data quality)

Before building Silver, the pipeline creates two separate DataFrames from Bronze:

- `invalid_df`: records that fail any validation rule, with a `validation_reason` column explaining the failure
- `valid_df`: records that pass all rules

Validation rules checked (in order — first match wins):

| Rule | Reason written |
|---|---|
| `timestamp IS NULL` | `NULL_TIMESTAMP` |
| `temperature IS NULL` | `NULL_TEMPERATURE` |
| `temperature NOT BETWEEN -20 AND 150` | `TEMPERATURE_OUT_OF_RANGE` |
| `rpm < 0` | `INVALID_RPM` |
| `power_kw < 0` | `INVALID_POWER` |
| `status NOT IN ('running', 'idle', 'fault')` | `INVALID_STATUS` |
| `machine_id = ''` | `EMPTY_MACHINE_ID` |

Invalid records are written to **PostgreSQL**: `machine_events_quarantine` table.

#### Step 4 — Silver Layer (enrichment)

Valid records go through a `.select()` that adds computed columns using PySpark `when/otherwise` logic:

| New Column | Logic |
|---|---|
| `temperature_status` | `> 90` → Critical, `> 80` → Warning, else Normal |
| `vibration_status` | `> 5` → Critical, `> 3` → Warning, else Normal |
| `fault_flag` | `1` if `is_fault=True`, else `0` (integer, for aggregation) |
| `event_date` | Date extracted from timestamp |
| `event_hour` | Hour (0–23) extracted from timestamp |
| `health_score` | `0` if fault, else `max(0, 100 - temperature*0.3 - vibration*5)` |
| `risk_score` | `temperature * 0.4 + vibration * 10` |
| `running_flag` | `1` if status = 'running', else `0` |
| `fault_category` | E001/E003 → Overheat, E002/E004 → Vibration, E005 → RPM Drop, else None |
| `power_status` | `> 5kW` → High, `> 3kW` → Normal, else Low |
| `time_bucket` | Hour 6–13 → Morning, 14–21 → Evening, else Night |
| `anomaly_flag` | `1` if temperature > 90, else `0` |

Silver is written to:
- **Elasticsearch**: `machine-events` index
- **MinIO**: `s3a://iot-data/silver/machine_silver_data/` (Parquet)
- **PostgreSQL**: `machine_events_silver` table
- **Kafka**: `sensor-processed` topic (re-published as JSON for downstream consumers)

#### Step 5 — Gold Layer (aggregations)

Gold groups Silver by `machine_id` and a **1-minute tumbling window** with a **1-minute watermark**. The watermark tells Spark to wait up to 1 minute for late-arriving data before finalizing a window.

Aggregations computed per machine per minute:

| Column | Calculation |
|---|---|
| `avg_temp` / `max_temp` | Average and peak temperature in the window |
| `avg_rpm` | Average RPM |
| `avg_vibration` / `max_vibration` | Average and peak vibration |
| `avg_power` / `peak_power` | Average and peak power consumption |
| `avg_health_score` / `min_health_score` | Health score trend (min shows worst moment) |
| `avg_risk_score` | Average composite risk |
| `uptime_percentage` | `sum(running_flag) / count(*) * 100` |
| `fault_count` | Total fault events in the window |
| `total_events` | Total events (running + idle + fault) |
| `fault_percentage` | `fault_count / total_events * 100` |

Gold is written to:
- **Elasticsearch**: `machine-aggregates` index
- **MinIO**: `s3a://iot-data/gold/machine_gold_data/` (Parquet)
- **PostgreSQL**: `machine_aggregates_gold` table

---

### Worker Pipeline

The worker pipeline follows the same Bronze → Quarantine → Silver → Gold structure, running on the `worker-events` Kafka topic in parallel.

#### Worker Bronze

Parses the flat worker JSON schema (no nested structs) and converts `timestamp` from string to timestamp type.

Written to:
- **MinIO**: `s3a://iot-data/bronze/worker_bronze_data/`
- **PostgreSQL**: `worker_events_bronze`

#### Worker Quarantine

Validation rules:

| Rule | Reason written |
|---|---|
| `timestamp IS NULL` | `NULL_TIMESTAMP` |
| `worker_id = ''` | `EMPTY_WORKER_ID` |
| `heart_rate < 0` | `INVALID_HEART_RATE` |
| `movement_status NOT IN ('ACTIVE', 'IDLE', 'WALKING')` | `INVALID_MOVEMENT_STATUS` |

Written to **PostgreSQL**: `worker_events_quarantine`

#### Worker Silver

Added computed columns:

| New Column | Logic |
|---|---|
| `safety_violation_flag` | `1` if helmet OR vest is missing, else `0` |
| `fatigue_status` | `> 80` → High, `> 50` → Medium, else Low |
| `worker_risk_level` | danger_zone AND fatigue > 80 → Critical; danger_zone → High; else Normal |
| `alert_level` | danger_zone AND fatigue > 80 → CRITICAL; safety violation → WARNING; else NORMAL |
| `heart_rate_status` | `> 120` → High, `> 90` → Elevated, else Normal |

Written to:
- **Elasticsearch**: `worker-events` index
- **MinIO**: `s3a://iot-data/silver/worker_silver_data/`
- **PostgreSQL**: `worker_events_silver`

#### Worker Gold

Groups by `worker_id` with a **5-minute tumbling window** and **1-minute watermark**:

| Column | Calculation |
|---|---|
| `violations_per_window` | `sum(safety_violation_flag)` |
| `workers_in_danger_zone` | `sum(1 if danger_zone else 0)` |
| `avg_fatigue_score` | Average fatigue score over the window |

Written to:
- **Elasticsearch**: `worker-safety` index
- **MinIO**: `s3a://iot-data/gold/worker_gold_data/`
- **PostgreSQL**: `worker_safety_gold`

---

### Sink Summary

| DataFrame | Elasticsearch | MinIO (Parquet) | PostgreSQL | Kafka |
|---|---|---|---|---|
| Machine Bronze | — | `bronze/machine_bronze_data/` | `machine_events_bronze` | — |
| Machine Quarantine | — | — | `machine_events_quarantine` | — |
| Machine Silver | `machine-events` | `silver/machine_silver_data/` | `machine_events_silver` | `sensor-processed` |
| Machine Gold | `machine-aggregates` | `gold/machine_gold_data/` | `machine_aggregates_gold` | — |
| Worker Bronze | — | `bronze/worker_bronze_data/` | `worker_events_bronze` | — |
| Worker Quarantine | — | — | `worker_events_quarantine` | — |
| Worker Silver | `worker-events` | `silver/worker_silver_data/` | `worker_events_silver` | — |
| Worker Gold | `worker-safety` | `gold/worker_gold_data/` | `worker_safety_gold` | — |

---

## Data Quality — Quarantine Layer

Every invalid record is stored with a `validation_reason` string so the team can audit what went wrong. PostgreSQL views with a Cairo timezone conversion are created automatically by `init.sql` for easier debugging:

```sql
-- See all quarantined machine events with Cairo time
SELECT * FROM machine_events_quarantine_view;

-- See all quarantined worker events
SELECT * FROM worker_events_quarantine_view;
```

The intentional dirty data in the producers ensures this layer is always exercised during development.

---

## Schema Reference

### Machine Event Schema

This is the raw JSON structure published to `sensor-events`:

```json
{
  "event_id":     "CNC_01_1717236000000",
  "timestamp":    "2026-06-09T10:00:00+00:00",
  "machine_id":   "CNC_01",
  "machine_type": "cnc_machine",
  "floor":        "A",
  "shift":        "morning",
  "status":       "running",
  "error_code":   null,
  "is_fault":     false,
  "metrics": {
    "temperature": 74.3,
    "vibration":   2.61,
    "rpm":         1498.0,
    "power_kw":    4.4
  },
  "cnc_sensors": {
    "oil_level_pct":        78.0,
    "coolant_pressure_bar": 2.1
  }
}
```

Only the machine-type-specific sensor block is present — `cnc_sensors` for CNC_01, `robot_sensors` for ROB_01, etc. The others are absent (not null).

**Sensor thresholds:**

| Sensor | Normal | Warning | Critical |
|---|---|---|---|
| Temperature | 65–80°C | 80–90°C | > 90°C |
| Vibration | 1.5–3.5 mm/s | 3.5–5.0 mm/s | > 5.0 mm/s |
| RPM | 1400–1550 | 1200–1400 | < 1200 |
| Power | 3.5–5.5 kW | 5.5–7.0 kW | > 7.0 kW |
| Oil Level | 40–100% | 20–40% | < 20% |
| Coolant Pressure | 1.8–3.0 bar | 1.2–1.8 bar | < 1.2 bar |

**Shift schedule:**

| Shift | Hours |
|---|---|
| `morning` | 06:00 – 14:00 |
| `evening` | 14:00 – 22:00 |
| `night` | 22:00 – 06:00 |

---

### Worker Event Schema

```json
{
  "worker_id":      "W001",
  "timestamp":      "2026-06-09T10:00:00+00:00",
  "floor":          "A",
  "zone_id":        "ZONE_1",
  "helmet_on":      true,
  "safety_vest_on": true,
  "heart_rate":     88,
  "movement_status": "ACTIVE",
  "danger_zone":    false,
  "fatigue_score":  45
}
```

---

## PostgreSQL Tables

All tables and views are created automatically when the `postgres` container starts by mounting `init.sql` into `/docker-entrypoint-initdb.d/`.

| Table | Layer | Content |
|---|---|---|
| `machine_events_bronze` | Bronze | Raw parsed machine events |
| `machine_events_quarantine` | Quarantine | Invalid machine records with reason |
| `machine_events_silver` | Silver | Enriched machine events with computed fields |
| `machine_aggregates_gold` | Gold | 1-minute per-machine KPI aggregations |
| `worker_events_bronze` | Bronze | Raw parsed worker events |
| `worker_events_quarantine` | Quarantine | Invalid worker records with reason |
| `worker_events_silver` | Silver | Enriched worker events with computed fields |
| `worker_safety_gold` | Gold | 5-minute per-worker safety aggregations |

**Views** (all include a `cairo_time` column — `timestamp AT TIME ZONE 'Africa/Cairo'`):

`machine_events_quarantine_view`, `machine_events_bronze_view` (via `worker_events_bronze_view`), `worker_events_silver_view`, `worker_safety_gold_view`

---

## Elasticsearch Indices & Templates

Index templates are applied by the `elasticsearch-init` container on startup using curl against the `_index_template` API.

| Template file | Applies to | Description |
|---|---|---|
| `machine_template.json` | `machine-events*` | Silver machine events — all sensor fields, status, health/risk scores |
| `machine_aggregates_template.json` | `machine-aggregates*` | Gold machine KPIs — window aggregations |
| `worker_template.json` | `worker-events*` | Silver worker events — safety fields, alert levels |
| `worker_safety_template.json` | `worker-safety*` | Gold worker aggregations — violations per window |

All templates use `"number_of_shards": 1, "number_of_replicas": 0` — appropriate for a single-node development Elasticsearch instance.

---

## MinIO Storage Layout

MinIO acts as an S3-compatible object store, accessed by PySpark via the S3A filesystem (`s3a://`). All data is stored as Parquet files under the `iot-data` bucket:

```
iot-data/
├── bronze/
│   ├── machine_bronze_data/     # Raw machine events (Parquet)
│   └── worker_bronze_data/      # Raw worker events (Parquet)
├── silver/
│   ├── machine_silver_data/     # Enriched machine events (Parquet)
│   └── worker_silver_data/      # Enriched worker events (Parquet)
└── gold/
    ├── machine_gold_data/       # Machine KPI aggregations (Parquet)
    └── worker_gold_data/        # Worker safety aggregations (Parquet)
```

MinIO credentials (set via environment variables in `docker-compose.yml`): `MINIO_USER` / `MINIO_PASSWORD`.

---

## Computed Fields Reference

### Machine Silver Fields

| Field | Formula | Purpose |
|---|---|---|
| `health_score` | `max(0, 100 - temp*0.3 - vibration*5)` if no fault, else `0` | Single score summarizing machine condition; 0 = fault, ~75 = healthy |
| `risk_score` | `temp * 0.4 + vibration * 10` | Composite leading indicator of potential failure |
| `fault_flag` | `1` if `is_fault` else `0` | Integer version of `is_fault` for aggregation with `sum()` |
| `running_flag` | `1` if status = 'running' else `0` | Used to calculate uptime percentage in Gold |
| `anomaly_flag` | `1` if `temperature > 90` else `0` | Separate flag for temperature-specific anomalies (independent of `is_fault`) |

### Worker Silver Fields

| Field | Formula | Purpose |
|---|---|---|
| `safety_violation_flag` | `1` if helmet OR vest missing else `0` | PPE compliance tracking |
| `fatigue_status` | Score > 80 → High, > 50 → Medium, else Low | Human-readable fatigue classification |
| `worker_risk_level` | danger_zone + fatigue > 80 → Critical; danger_zone → High; else Normal | Composite risk considering both location and physical state |
| `alert_level` | Critical/WARNING/NORMAL based on combined conditions | Final alerting output for dashboards |

---

## Error Codes

| Code | Sensor Threshold | Machines |
|---|---|---|
| `E001` | Temperature > 90°C | All |
| `E002` | Vibration > 5.0 mm/s | All |
| `E003` | RPM < 1200 | All |
| `E004` | Coolant pressure < 1.2 bar | CNC_01 |
| `E005` | Oil level < 20% | CNC_01, PMP_01 |
| `E006` | Joint torque > 100 Nm | ROB_01 |
| `E007` | Belt tension > 800 N | CNV_01 |
| `E008` | Flow rate < 10 lpm | PMP_01 |

In `streaming_job.py`, error codes E001/E003 are mapped to `fault_category = "Overheat"`, E002/E004 to `"Vibration"`, and E005 to `"RPM Drop"`. E006, E007, and E008 are not currently mapped to a category in Silver (they resolve to `"None"`).

---

## Resetting the Project

To completely reset all data and start fresh:

```bash
bash reset.sh
```

This runs:

```bash
docker compose down -v           # Stops all containers and removes all volumes
rm -rf /home/jovyan/data/checkpoints/*   # Clears PySpark streaming checkpoints
rm -rf parquet_data/*            # Clears any local Parquet output
```

> **Important:** Clearing checkpoints means PySpark will restart reading from `latest` offset on next run. Any unprocessed Kafka messages between the last checkpoint and now will be skipped. If you need to reprocess from the beginning, change `startingOffsets` to `"earliest"` temporarily.
