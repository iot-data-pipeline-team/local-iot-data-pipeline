# Overview

The `sparkjobs` directory contains the Apache Spark Structured Streaming components responsible for consuming real-time IoT events from Apache Kafka, validating and transforming the incoming data, and generating analytical datasets using the Medallion Architecture.

The Spark layer is the main **processing engine** of the project.

It receives events from the Kafka topics:

```text
sensor-events
worker-events
```

and processes them through multiple stages:

```text
Kafka
  │
  ▼
Spark Structured Streaming
  │
  ├── Schema & Parsing
  │
  ├── Bronze Processing
  │
  ├── Silver Transformations
  │
  └── Gold Aggregations
```

The Spark jobs are designed to process events continuously as they arrive rather than processing a fixed batch of data.

---

# Spark Layer Responsibilities

The Spark processing layer is responsible for:

- Connecting to Kafka
- Reading streaming events
- Parsing JSON messages
- Applying explicit schemas
- Validating incoming records
- Handling malformed data
- Flattening nested structures
- Applying business rules
- Creating derived columns
- Calculating health and risk indicators
- Processing event timestamps
- Performing time-based aggregations
- Calculating machine KPIs
- Supporting worker-related analytics
- Applying watermarks
- Maintaining streaming state
- Managing checkpoints
- Producing Bronze, Silver, and Gold datasets

---

# Spark Architecture

The Spark processing architecture can be represented as:

```text
                Apache Kafka
                     │
          ┌──────────┴──────────┐
          │                     │
   sensor-events          worker-events
          │                     │
          └──────────┬──────────┘
                     │
                     ▼
           Spark Structured
              Streaming
                     │
                     ▼
              JSON Parsing
                     │
                     ▼
               Schema Layer
                     │
                     ▼
              Data Validation
                     │
                     ▼
              Bronze Layer
                     │
                     ▼
            Transformations
                     │
                     ▼
              Silver Layer
                     │
                     ▼
               Aggregations
                     │
                     ▼
               Gold Layer
```
--- 

# Spark Jobs Structure

The `sparkjobs` directory contains the different components used by the Spark processing pipeline.

Current structure:

```text
sparkjobs/
│
├── aggregations/
│   ├── __init__.py
│   └── machines_aggregations.py
│
├── checkpoints/
│
├── consumers/
│   ├── __init__.py
│   ├── kafka_consumer.py
│   ├── machines_consumer.py
│   ├── schema_df.py
│   └── workers_consumer.py
│
├── eda/
│   └── machines/
│       ├── __init__.py
│       ├── machine_data_quality.py
│       ├── machine_eda.py
│       ├── machine_schema_exploration.py
│       └── machine_statistics.py
│
├── transformations/
│   ├── __init__.py
│   ├── machines_cleaning.py
│   └── machines_enhancement.py
│
├── validations/
│   ├── __init__.py
│   └── machine_validation.py
│
└── __init__.py
```
Each module has a specific responsibility.