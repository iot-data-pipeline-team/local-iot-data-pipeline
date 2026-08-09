# Consumers 
# `machines_consumer.py`

## Overview

`machines_consumer.py` is the **main streaming consumer for machine sensor events** in the IoT data pipeline.

It uses **Apache Spark Structured Streaming** to continuously consume machine events from Kafka, parse and flatten the JSON messages, validate the incoming data, and process each micro-batch through the machine EDA, cleaning, and enhancement stages.

The consumer acts as the main integration point between the **Kafka streaming layer** and the machine data processing pipeline.

The processing flow is:

- Read machine events from Kafka.
- Parse the incoming JSON messages using the machine schema.
- Flatten nested machine and sensor structures.
- Validate machine records.
- Separate valid and invalid records.
- Run machine EDA on the incoming batch.
- Clean valid machine records.
- Enhance cleaned records with derived features.
- Display validation, cleaning, and enhancement results.
- Maintain Spark Structured Streaming checkpoints.

---

## Processing Architecture

```text
Kafka
  │
  │  sensor-events
  ▼
┌───────────────────────────────┐
│      Spark Structured         │
│          Streaming            │
└───────────────────────────────┘
  │
  ▼
Read Machine Events
  │
  ├── Kafka Bootstrap Servers
  └── Subscribe to sensor-events
  │
  ▼
Parse JSON Messages
  │
  ├── Cast Kafka value to STRING
  └── Apply machine_schema
  │
  ▼
Expand JSON Structure
  │
  └── data.*
  │
  ▼
Flatten Nested Sensors
  │
  ├── Event Information
  ├── Common Machine Metrics
  ├── CNC Sensors
  ├── Robot Sensors
  ├── Conveyor Sensors
  └── Pump Sensors
  │
  ▼
Validate Machine Data
  │
  └── validate_machine_data()
  │
  ├── Valid Records
  │
  │   ▼
  │   clean_machine_data()
  │   │
  │   ▼
  │   enhance_machine_data()
  │   │
  │   ▼
  │   Enhanced Machine Data
  │
  └── Invalid Records
      │
      ▼
      Display Invalid Records
  │
  ▼
foreachBatch(process_batch)
  │
  ├── Batch Statistics
  ├── Machine EDA
  ├── Cleaning
  └── Enhancement
  │
  ▼
Checkpoint
  │
  └── checkpoints/machine_consumer
``` 
# Main Components

## 1. Kafka Configuration

The consumer connects to the Kafka cluster using the configured bootstrap servers and subscribes to the machine sensor topic.

```text
Kafka Brokers:
localhost:9094
localhost:9095
localhost:9096

Topic:
sensor-events
```
---
## 2. Spark Session

A Spark session is created for the streaming application.

The consumer loads the required dependencies for:

- Kafka integration with Spark Structured Streaming.
- PostgreSQL connectivity for downstream processing.

Spark logging is also reduced to `ERROR` to keep the console output focused on the actual processing results.

---

## 3. Reading Machine Events

The consumer uses **Spark Structured Streaming** to continuously read machine sensor events from Kafka.

```text
Kafka
   │
   ▼
sensor-events
   │
   ▼
spark.readStream
   │
   ▼
machine_kafka_df
```
Each Kafka message contains a machine sensor event encoded as JSON.

---

### 4. JSON Parsing

The Kafka `value` is first converted from binary data into a string.

The machine schema is then applied using `from_json()`.

```text
Kafka value
    │
    ▼
STRING
    │
    ▼
machine_schema
    │
    ▼
Structured JSON
```
---

## 5. Flattening Machine Events

The nested JSON structure is expanded into individual columns.

### Common Event Information

- `event_id`
- `event_time`
- `machine_id`
- `machine_type`
- `floor`
- `shift`
- `status`
- `error_code`
- `is_fault`

### Common Machine Metrics

- `temperature`
- `vibration`
- `rpm`
- `power_kw`

### Machine-Specific Sensors

Sensors are also extracted for:

- CNC machines
- Robot arms
- Conveyor belts
- Pumps

This produces a flattened machine event DataFrame that can be processed by the downstream validation and transformation stages.

---

## 6. Validation

The flattened machine events are passed to:

```python
validate_machine_data()
```
The validation layer adds validation results and produces the final:

```text
is_valid
```
The records are then separated into:
```text 
Valid Records
     │
     └── is_valid = true

Invalid Records
     │
     └── is_valid = false
``` 
The consumer displays the number of valid and invalid records for every micro-batch.

--- 

## 7. Batch Processing

The `process_batch()` function is executed for every Spark micro-batch through `foreachBatch()`.

For each batch, the consumer:

1. Caches the incoming batch.
2. Separates valid and invalid records.
3. Displays validation statistics.
4. Displays invalid records.
5. Runs machine EDA.
6. Cleans valid records.
7. Enhances the cleaned data.
8. Displays before/after cleaning results.
9. Displays enhanced machine features.
10. Releases cached DataFrames.

---

## 8. Machine EDA

The consumer runs:

```python
run_machine_eda(batch_df)
```
The EDA stage provides:

- Schema exploration
- Data quality analysis
- Exploratory statistics
- Frequency distributions
- Fault analysis
- Sensor statistics

EDA is performed on the incoming batch to monitor the quality and characteristics of the streaming machine data.

---
## 9. Data Cleaning

Only valid records are passed to:

```python
clean_machine_data(valid_df)
```
The cleaning stage performs:

- String trimming
- Case normalization
- Empty-string cleaning
- Numeric rounding
- Duplicate event removal
```text
Valid Data
    │
    ▼
clean_machine_data()
    │
    ▼
Clean Machine Data
```

---
## 10. Data Enhancement

The cleaned data is then passed to:

```python
enhance_machine_data(cleaned_df)
```

The enhancement stage creates additional analytical features such as:

* **Event Date**
* **Event Hour**
* **Time Bucket**
* **Weekend Status**
* **Temperature Status**
* **Vibration Status**
* **Power Status**
* **RPM Status**
* **Fault Flag**
* **Running Flag**
* **Fault Category**
* **Machine Group**
* **Health Score**
* **Risk Score**

### Data Flow

```text
Clean Machine Data
        │
        ▼
enhance_machine_data()
        │
        ▼
Enhanced Machine Data
```
--- 

# 11. Console Monitoring

The consumer displays important information from every batch.

### Batch Information

```text
Processing Batch 0

Valid rows   : 95
Invalid rows : 5
```

### Invalid Records

```text
+--------+----------+------------+--------+
|event_id|machine_id|machine_type|is_valid|
+--------+----------+------------+--------+
|EVT102  |CNC_01    |cnc_machine |false   |
|EVT118  |ROB_01    |robot_arm   |false   |
+--------+----------+------------+--------+
```

### Before Cleaning

```text
+------------+-------+-----+----------+-----------+
|machine_type|status |floor|error_code|temperature|
+------------+-------+-----+----------+-----------+
| CNC_MACHINE|RUNNING| a   | e001     |75.456     |
+------------+-------+-----+----------+-----------+
```

### After Cleaning

```text
+------------+-------+-----+----------+-----------+
|machine_type|status |floor|error_code|temperature|
+------------+-------+-----+----------+-----------+
|cnc_machine |running|A    |E001      |75.46      |
+------------+-------+-----+----------+-----------+
```

### Enhanced Data

```text
+----------+-------------+------------------+-----------------+----------+------------+-------------+------------+----------+
|machine_id|machine_group|temperature_status|vibration_status|rpm_status|power_status|running_flag|health_score|risk_score|
+----------+-------------+------------------+-----------------+----------+------------+-------------+------------+----------+
|CNC_01    |Production   |Normal            |Normal           |Normal    |Normal      |1            |100         |53.18     |
+----------+-------------+------------------+-----------------+----------+------------+-------------+------------+----------+
```

---

# 12. Structured Streaming Checkpoint

The streaming query uses:

```text
checkpoints/machine_consumer
```

as its checkpoint location.

Checkpoints allow Spark Structured Streaming to maintain processing progress and recover from interruptions without unnecessarily reprocessing previously committed streaming data.

---

# End-to-End Flow

```text
Kafka: sensor-events
        │
        ▼
Spark Structured Streaming
        │
        ▼
Parse JSON
        │
        ▼
Flatten Machine Sensors
        │
        ▼
Validate Machine Data
        │
        ├───────────────┐
        │               │
        ▼               ▼
     Valid           Invalid
        │               │
        │               └──► Display
        ▼
Machine EDA
        │
        ▼
Machine Cleaning
        │
        ▼
Machine Enhancement
        │
        ▼
Enhanced Machine Data
        │
        ▼
Console / Downstream Processing
```

---

# Role in the IoT Pipeline

`machines_consumer.py` acts as the **machine-data processing orchestrator**. It connects the streaming ingestion layer with the validation, EDA, cleaning, and enhancement components.

```text
Machine Producer
       │
       ▼
     Kafka
       │
       ▼
machines_consumer.py
       │
       ├── Validation
       ├── EDA
       ├── Cleaning
       └── Enhancement
       │
       ▼
Enhanced Machine Data
```

This design keeps the individual processing functions modular while allowing the consumer to coordinate the complete machine-data processing workflow.
