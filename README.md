# CNC Factory — IoT Gateway & Kafka Pipeline

Real-time IoT data pipeline for a CNC factory. Simulates an industrial gateway that bridges CNC machine sensors to a Kafka message broker, consumed by PySpark Structured Streaming.

```
IoT Machines
    ↓  Modbus TCP / OPC-UA  (simulated)
Gateway API  (gateway_api.py)
    ↓  HTTP GET /machines/{id}
Kafka Producer  (kafka_producer.py)
    ↓  Kafka binary protocol / TCP
Kafka Cluster  (3 brokers)
    ↓
PySpark Streaming → Elasticsearch → Grafana      (streaming path)
                  → MinIO                         (hourly archive)
                  → PostgreSQL → Power BI         (hourly KPIs)
```

---

## Table of Contents

- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Kafka Cluster](#kafka-cluster)
- [Gateway API](#gateway-api)
- [Kafka Producer](#kafka-producer)
- [Kafka Consumer](#kafka-consumer)
- [Schema Reference](#schema-reference)
- [Error Codes](#error-codes)

---

## Prerequisites

```bash
pip install fastapi uvicorn aiohttp kafka-python
```

---

## Quick Start

```bash
# Terminal 1 — start Docker stack (Kafka + all services)
docker-compose up -d

# Terminal 2 — start gateway
python gateway_api.py

# Terminal 3 — start producer
python kafka_producer.py

# Terminal 4 — monitor events (optional)
python kafka_consumer.py --dry-run
```

Open `http://localhost:8000` to see the live gateway dashboard.

---

## Kafka Cluster

### Configuration

| Parameter | Value |
|---|---|
| Mode | KRaft (no Zookeeper — Kafka manages its own metadata) |
| Brokers | 3 brokers |
| Ports | `localhost:9094`, `localhost:9095`, `localhost:9096` |
| Topic | `iot-sensors` |
| Partitions | 4 (one per machine) |
| Replication factor | 3 |
| Retention | 7 days |
| Compression | gzip |
| Acks | `all` (wait for all replicas) |

### Partitioning Strategy

Events are keyed by `machine_id` so the same machine always goes to the same partition. This guarantees ordering per machine — essential for detecting fault sequences (oil drops → temperature rises → E001 fires).

```
Partition 0 → CNC_01
Partition 1 → ROB_01
Partition 2 → CNV_01
Partition 3 → PMP_01
```

### Kafka Services in Docker

| Service | Port | Purpose |
|---|---|---|
| Kafka Broker 1 | `9094` | Message broker |
| Kafka Broker 2 | `9095` | Message broker |
| Kafka Broker 3 | `9096` | Message broker |
| Kafka UI | `12000` | Web UI to inspect topics and messages |

---

## Gateway API

**File:** `gateway_api.py`

Simulates an industrial gateway that would normally read CNC PLCs via **Modbus TCP** or **OPC-UA** and expose sensor data over REST. In production, this layer would be an edge device physically close to the machines.

### Start

```bash
python gateway_api.py
```

Runs on `http://localhost:8000`

### Machines

| Machine ID | Type | Floor | Model |
|---|---|---|---|
| `CNC_01` | CNC Milling Machine | A | Haas VF-2 |
| `ROB_01` | Robot Arm | B | KUKA KR 10 R1100 |
| `CNV_01` | Conveyor Belt | C | FlexLink X65 |
| `PMP_01` | Hydraulic Pump | C | Bosch Rexroth A10VSO |

### Endpoints

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/` | Live dashboard — all 4 machines, auto-refreshes every second |
| `GET` | `/machines/CNC_01` | Latest sensor reading for CNC_01 |
| `GET` | `/machines/ROB_01` | Latest sensor reading for ROB_01 |
| `GET` | `/machines/CNV_01` | Latest sensor reading for CNV_01 |
| `GET` | `/machines/PMP_01` | Latest sensor reading for PMP_01 |

### Sensor Ranges

| Sensor | Normal | Warning | Critical |
|---|---|---|---|
| Temperature | 65–80°C | 80–90°C | > 90°C |
| Vibration | 1.5–3.5 mm/s | 3.5–5.0 mm/s | > 5.0 mm/s |
| RPM | 1400–1550 | 1200–1400 | < 1200 |
| Power | 3.5–5.5 kW | 5.5–7.0 kW | > 7.0 kW |
| Oil Level | 40–100% | 20–40% | < 20% |
| Coolant Pressure | 1.8–3.0 bar | 1.2–1.8 bar | < 1.2 bar |

### Realistic Sensor Behaviour

The gateway simulates real physical degradation:

- **Oil drain** — oil level drops slowly over time. Low oil → temperature rises (friction heat) → vibration increases
- **Bearing wear** — vibration increases gradually as bearing degrades
- **Causal chain** — `oil drops → temperature rises → vibration increases` — faults are correlated, not independent

---

## Kafka Producer

**File:** `kafka_producer.py`

Polls all 4 machines **simultaneously** every second using async parallel HTTP calls and publishes 4 events to Kafka — one reading per machine per second.

### Start

```bash
# Normal mode (Kafka must be running)
python kafka_producer.py

# Dry-run mode (prints to console, no Kafka needed)
python kafka_producer.py --dry-run
```

### How It Works

Uses `asyncio` + `aiohttp` to fire all 4 HTTP calls at the same time:

```
Sequential (old):  CNC_01 → sleep → ROB_01 → sleep → ...  = 1 event/sec total
Parallel  (now):   CNC_01 ┐
                   ROB_01 ├→ all fire simultaneously = 4 events/sec
                   CNV_01 ┘
                   PMP_01 ┘
```

Each machine gets **1 reading per second** instead of 1 reading every 4 seconds.

### Throughput

| | Value |
|---|---|
| Poll interval | 1 second per cycle |
| Events per cycle | 4 (one per machine) |
| Events per minute | 240 |
| Events per hour | 14,400 |
| Events per day | 345,600 |

### Producer Config

| Parameter | Value | Reason |
|---|---|---|
| `acks` | `all` | Wait for all replicas — no data loss |
| `compression_type` | `gzip` | Reduce network usage |
| `retries` | `3` | Auto-retry on transient failures |
| `linger_ms` | `5` | Small batching window for efficiency |
| `key` | `machine_id` | Same machine → same partition → ordered |


---

## Kafka Consumer

**File:** `kafka_consumer.py`

Reads events from the `iot-sensors` topic and prints to console. Useful for debugging and verifying the pipeline end-to-end.

### Start

```bash
# Read new messages only (default)
python kafka_consumer.py

# Read all stored messages from beginning
python kafka_consumer.py --from-beginning

# Show fault events only
python kafka_consumer.py --faults-only

# Filter one machine
python kafka_consumer.py --machine CNC_01

# Print full JSON instead of summary line
python kafka_consumer.py --raw

# Combine filters
python kafka_consumer.py --faults-only --machine CNC_01 --from-beginning
```

### Consumer Config

| Parameter | Value | Reason |
|---|---|---|
| `group_id` | `iot-console-consumer` | Separate from PySpark consumer group |
| `auto_offset_reset` | `latest` | Only new messages by default |
| `session_timeout_ms` | `30000` | 30s before broker thinks consumer died |
| `enable_auto_commit` | `True` | Auto-commit offsets |

> **Note:** The consumer uses a different `group_id` than PySpark. This means both the console consumer and PySpark can read the same topic independently without interfering with each other's offsets.

---

## Schema Reference

Every event published to Kafka has this structure:

```json
{
  "event_id":    "CNC_01_1715000400",
  "timestamp":   "2026-06-09T10:00:00Z",
  "machine_id":  "CNC_01",
  "machine_type": "cnc_machine",
  "floor":       "A",
  "shift":       "morning",
  "status":      "running",
  "error_code":  null,
  "is_fault":    false,
  "metrics": {
    "temperature": 74.3,
    "vibration":   2.61,
    "rpm":         1498,
    "power_kw":    4.4
  },
  "cnc_sensors": {
    "oil_level_pct":        78.0,
    "coolant_pressure_bar": 2.1
  }
}
```

### Field Reference

| Field | Type | Description |
|---|---|---|
| `event_id` | string | Unique ID: `{machine_id}_{unix_timestamp}` |
| `timestamp` | string | ISO 8601 UTC timestamp |
| `machine_id` | string | `CNC_01`, `ROB_01`, `CNV_01`, `PMP_01` |
| `machine_type` | string | `cnc_machine`, `robot_arm`, `conveyor_belt`, `pump` |
| `floor` | string | `A`, `B`, or `C` |
| `shift` | string | `morning` (06–14h), `evening` (14–22h), `night` (22–06h) |
| `status` | string | `running`, `warning`, `fault` |
| `error_code` | string \| null | Active error code, `null` when normal |
| `is_fault` | boolean | `true` when `error_code` is set |
| `metrics` | object | Shared sensors — all machines |
| `{type}_sensors` | object | Machine-specific sensors |

### Machine-Specific Sensor Keys

| Machine Type | Sensor Key | Fields |
|---|---|---|
| `cnc_machine` | `cnc_sensors` | `oil_level_pct`, `coolant_pressure_bar` |
| `robot_arm` | `robot_sensors` | `joint_torque_nm`, `end_effector_force_n` |
| `conveyor_belt` | `conveyor_sensors` | `belt_tension_n`, `load_weight_kg` |
| `pump` | `pump_sensors` | `oil_level_pct`, `flow_rate_lpm`, `inlet_pressure_bar` |

### Shift Schedule

| Shift | Hours |
|---|---|
| `morning` | 06:00 – 14:00 |
| `evening` | 14:00 – 22:00 |
| `night` | 22:00 – 06:00 |

---

## Error Codes

| Code | Sensor | Threshold | Machines |
|---|---|---|---|
| `E001` | Temperature | > 90°C | All |
| `E002` | Vibration | > 5.0 mm/s | All |
| `E003` | RPM | < 1200 | All |
| `E004` | Coolant pressure | < 1.2 bar | CNC_01 |
| `E005` | Oil level | < 20% | CNC_01, PMP_01 |
| `E006` | Joint torque | > 100 Nm | ROB_01 |
| `E007` | Belt tension | > 800 N | CNV_01 |
| `E008` | Flow rate | < 10 lpm | PMP_01 |

When `error_code` is `null` and `is_fault` is `false` — machine is operating normally.

---

## Architecture Notes

### Why One Topic for All Machines

At 4 machines all sharing the same schema, one topic with 4 partitions (keyed by `machine_id`) is the correct approach. At 100+ machines with diverging schemas, topics per machine type would be considered.

### Why Async Parallel Polling

The producer uses `asyncio` + `aiohttp` to fire all 4 HTTP calls simultaneously. This gives each machine 1 reading per second rather than 1 reading every 4 seconds (which would be the case with sequential round-robin polling).

### Why foreachBatch in PySpark

PySpark uses `foreachBatch` to write to all 3 sinks (Elasticsearch, MinIO, PostgreSQL) from a single Kafka read. This ensures all sinks are always in sync on the same offset. Separate `writeStream` calls would create independent offsets that drift apart on failure.

### Real Protocol Context

In production, this gateway API would be replaced by an edge device running:
- **Modbus TCP** reads against CNC PLC registers (most common for CNCs)
- **OPC-UA** subscriptions for modern machines
- **Embedded Kafka producer** — no HTTP hop between gateway and Kafka

The HTTP layer exists here for development clarity and testability.
