# 🚀 Real-Time Industrial IoT Analytics Platform

A complete real-time Industrial IoT Analytics Platform built using Apache Kafka, Spark Structured Streaming, PostgreSQL, Elasticsearch, MinIO, Kibana, and Power BI-ready data models.

The platform simulates both industrial machines and worker safety events, processes them through Bronze/Silver/Gold data layers, performs real-time data quality validation, stores clean and quarantined records, and delivers analytics-ready datasets for operational monitoring and business intelligence.

---

# 🎥 Demo Video

[Demo Link Here]

---

# 📸 Screenshots

## Kibana Dashboard

<img width="1920" height="1080" alt="Screenshot (1883)" src="https://github.com/user-attachments/assets/075fb464-fd22-49c1-a029-0819ea0ec344" />

## Kafka

<img width="1920" height="1080" alt="Screenshot (1882)" src="https://github.com/user-attachments/assets/62424989-1ef5-4ed8-b7f0-1f17b0083f6c" />
<img width="1920" height="1080" alt="Screenshot (1881)" src="https://github.com/user-attachments/assets/6127fe44-6b71-460b-8ada-12f932a92d0a" />

## Spark Structured Streaming

<img width="1920" height="1080" alt="Screenshot (1880)" src="https://github.com/user-attachments/assets/51177dd9-ffe5-4fce-a54e-9ef0f9761b1c" />

## MinIO Data Lake

<img width="1920" height="1080" alt="Screenshot (1878)" src="https://github.com/user-attachments/assets/ef0a869f-d6b8-426d-b99c-34aa653e00ee" />

## PostgreSQL Tables

<img width="1920" height="1080" alt="Screenshot (1885)" src="https://github.com/user-attachments/assets/1a07e916-5567-4114-92c2-dcec114941bd" />

## Power BI Dashboard

(Add Screenshot)

---

# 🏗️ Architecture

```text
Machine Producer                 Worker Producer
       │                                 │
       └──────────────┬──────────────────┘
                      │
                   Kafka
                      │
            Spark Structured Streaming
                      │
      ┌───────────────┼────────────────┐
      │               │                │
      ▼               ▼                ▼

 PostgreSQL      Elasticsearch      MinIO
 Bronze/Silver   Real-Time Search   Data Lake
 Gold Layers     & Analytics        Parquet Files

      │               │
      ▼               ▼

  Power BI        Kibana
```

---

# ⚙️ Technology Stack

## Streaming

* Apache Kafka
* Apache Spark Structured Streaming
* Python

## Storage

* PostgreSQL
* Elasticsearch
* MinIO (S3-Compatible Data Lake)

## Visualization

* Kibana
* Kafka UI
* Power BI

## Infrastructure

* Docker
* Docker Compose

---

# 📊 Data Pipelines

## Machine Monitoring Pipeline

### Bronze Layer

Raw machine telemetry:

* Temperature
* RPM
* Vibration
* Power Consumption
* Machine Status
* Fault Information

### Silver Layer

Business transformations:

* Health Score
* Risk Score
* Temperature Status
* Vibration Status
* Fault Categories
* Power Status
* Time Buckets
* Anomaly Detection

### Gold Layer

Windowed aggregations:

* Average Temperature
* Peak Temperature
* Average RPM
* Average Vibration
* Fault Percentage
* Uptime Percentage
* Health Metrics

---

## Worker Safety Pipeline

### Bronze Layer

Raw worker events:

* Worker ID
* Floor
* Zone
* Heart Rate
* Helmet Detection
* Safety Vest Detection
* Fatigue Score
* Movement Status

### Silver Layer

Safety analytics:

* Safety Violation Flag
* Fatigue Status
* Worker Risk Level
* Heart Rate Status
* Alert Level

### Gold Layer

5-minute window aggregations:

* Violations Per Window
* Danger Zone Counts
* Average Fatigue Score

---

# 🛡️ Data Quality & Quarantine Layer

The platform performs real-time validation and routes invalid records into dedicated quarantine tables.

## Machine Validation Rules

* Missing Timestamp
* Missing Temperature
* Invalid Temperature Range
* Negative RPM
* Negative Power
* Empty Machine ID
* Invalid Status

## Worker Validation Rules

* Missing Timestamp
* Empty Worker ID
* Invalid Heart Rate
* Invalid Movement Status

## Quarantine Tables

* machine_events_quarantine
* worker_events_quarantine

Each record includes:

* Original data
* Validation reason
* Timestamp

---

# 🗄️ Storage Layers

## PostgreSQL

Stores:

* Machine Bronze
* Machine Silver
* Machine Gold
* Worker Bronze
* Worker Silver
* Worker Gold
* Machine Quarantine
* Worker Quarantine

---

## Elasticsearch

Indexes:

### machine-events

Real-time machine analytics.

### machine-aggregates

Machine KPI aggregations.

### worker-events

Real-time worker safety analytics.

### worker-safety

Worker safety KPI aggregations.

---

## MinIO Data Lake

Stores Parquet files for:

```text
bronze/
silver/
gold/
```

for both Machine and Worker pipelines.

---

# 📈 Analytics Features

## Machine Analytics

* Health Monitoring
* Fault Detection
* Predictive Indicators
* Uptime Analysis
* Risk Scoring

## Worker Analytics

* PPE Compliance Monitoring
* Fatigue Tracking
* Danger Zone Monitoring
* Worker Risk Assessment
* Safety Alerts

---

# 📊 Power BI Integration

The platform provides PostgreSQL views specifically designed for BI tools.

### Machine Views

* machine_events_bronze_view
* machine_events_silver_view
* machine_aggregates_gold_view
* machine_events_quarantine_view

### Worker Views

* worker_events_bronze_view
* worker_events_silver_view
* worker_safety_gold_view
* worker_events_quarantine_view

All views include Cairo timezone conversion columns for reporting.

---

# ✨ Key Features

* Real-Time Streaming Architecture
* Bronze/Silver/Gold Data Lake Design
* Worker Safety Monitoring
* Industrial Machine Monitoring
* Real-Time Data Validation
* Quarantine Layer
* Elasticsearch Analytics
* Kibana Dashboards
* Power BI Reporting
* Kafka UI Monitoring
* Automated Infrastructure Startup
* Dockerized Deployment
* Window-Based Aggregations

---

# 📦 Quick Start

## Clone Repository

```bash
git clone -b baseline-pipeline-malek https://github.com/iot-data-pipeline-team/local-iot-data-pipeline.git

cd local-iot-data-pipeline
```

---

## Create Virtual Environment

```bash
python -m venv venv

venv\Scripts\activate
```

---

## Install Dependencies

```bash
pip install -r requirements.txt
```

---

## Run Entire Platform

```bash
.\scripts\run_project.bat
```

This automatically:

* Starts Docker Services
* Creates Kafka Topics
* Applies Elasticsearch Templates
* Creates MinIO Buckets
* Imports Kibana Dashboards
* Starts Spark Cluster
* Launches Streaming Jobs
* Starts Machine Producer
* Starts Worker Producer

---

# 🌐 Service URLs

| Service       | URL                    |
| ------------- | ---------------------- |
| Kibana        | http://localhost:5601  |
| Kafka UI      | http://localhost:12000 |
| MinIO Console | http://localhost:9001  |
| Elasticsearch | http://localhost:9200  |
| Jupyter       | http://localhost:8888  |

---

# 🔐 Default Credentials

## MinIO

```text
Username: admin
Password: password123
```

## PostgreSQL

```text
Database: db
Username: user
Password: password
```

---

# 🧹 Reset Project

## Soft Reset

```bash
docker compose down
```

## Full Reset

```bash
docker compose down -v
```

---

# 🚀 Future Enhancements

* Apache Airflow Orchestration
* dbt Transformations
* Great Expectations Data Quality
* Kubernetes Deployment
* CI/CD Pipelines
* Cloud Deployment (AWS/GCP/Azure)
* Grafana Monitoring
* ML-Based Predictive Maintenance

---

# 👨‍💻 Author

Mostafa Abdelazeem, Ahmed Mahmoud, Dina Mostafa, Abdelrahman Malek, Mostafa Fahmi

Industrial IoT Streaming & Analytics Platform
