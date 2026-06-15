# 🚀 Real-Time Industrial IoT Streaming Platform

A real-time industrial IoT data engineering platform built using Docker, Kafka, Spark Structured Streaming, Elasticsearch, PostgreSQL, MinIO, and Kibana.

The project simulates industrial IoT devices generating telemetry data and processes it through a modern streaming architecture with automated orchestration and dashboard provisioning.

---

# 🎥 Demo Video

[Demo Link Here]

---

# 📸 Screenshots

## Kibana Dashboard
<img width="1920" height="1080" alt="image" src="https://github.com/user-attachments/assets/d1e7ecbf-653e-4242-a6e2-8b276f6e5eca" />


## Spark Streaming
<img width="1920" height="1080" alt="image" src="https://github.com/user-attachments/assets/42bd90cf-dd6e-49f9-a53c-c5d1d028f4c0" />


## MinIO Storage
<img width="1920" height="1080" alt="image" src="https://github.com/user-attachments/assets/88b842e8-c537-4a28-a2bf-7f1fb005bd32" />


---

# 🧠 Architecture

```text
Python IoT Producer
        ↓
Kafka + ZooKeeper
        ↓
Spark Structured Streaming
   ↙         ↓          ↘
PostgreSQL  Elasticsearch  MinIO (Parquet)
                  ↓
               Kibana
```

---

# ⚙️ Tech Stack

## Streaming & Processing
- Apache Kafka
- Apache Spark Structured Streaming
- Python

## Storage & Analytics
- PostgreSQL
- Elasticsearch
- MinIO (S3-compatible object storage)

## Visualization
- Kibana
- Kafka UI

## Infrastructure
- Docker
- Docker Compose

---

# ✨ Key Features

- Real-time IoT streaming pipeline
- Automated Kafka topic creation
- Elasticsearch template-first ingestion
- Automated Kibana dashboard import
- Real-time anomaly detection
- Data lake storage using Parquet
- Window-based aggregations
- Automated MinIO bucket provisioning
- Dockerized distributed architecture
- Healthchecks and readiness orchestration

---

# 🧠 Engineering Concepts Demonstrated

- Distributed systems orchestration
- Readiness vs healthchecks
- Real-time stream processing
- Schema evolution & mapping management
- Data lake architecture
- Search and analytics pipelines
- Infrastructure automation
- Fault-tolerant initialization

---

# 📦 Quick Start

## 1️⃣ Clone Repository

```bash
git clone -b baseline-pipeline-malek https://github.com/iot-data-pipeline-team/local-iot-data-pipeline.git

cd local-iot-data-pipeline
```

---

## 2️⃣ Create Virtual Environment

```bash
python -m venv venv

venv\Scripts\activate
```

---

## 3️⃣ Install Dependencies

```bash
python -m pip install -r requirements.txt
```

---

## 4️⃣ Run Entire Platform

```
 .\scripts\run_project.bat 
```

This automatically:

- Starts infrastructure services
- Creates Kafka topic
- Applies Elasticsearch index template
- Imports Kibana dashboards
- Creates MinIO bucket
- Starts Spark cluster
- Runs Spark streaming job
- Starts IoT producer

---
# 🌐 Service URLs

| Service | URL |
|---|---|
| Kibana | http://localhost:5601 |
| MinIO Console | http://localhost:9001 |
| Elasticsearch | http://localhost:9200 |

---

## 🔐 MinIO Login Credentials

Use the following credentials to access the MinIO console:

```text
Username: admin
Password: password123
```

---

## 📊 Kibana Dashboard

After startup:

1. Open Kibana:
   http://localhost:5601

2. Navigate to:
   Dashboards → My Dashboards

3. Open the imported IoT dashboard.

4. To enable real-time updates:
   - Click the time filter in the top-right corner
   - Enable auto-refresh
   - Set refresh interval to 1 second

The dashboard will now update in near real-time as new IoT events arrive.

# 🗂️ Data Outputs

## PostgreSQL
- Raw IoT events
- Aggregated metrics

## Elasticsearch
- Searchable real-time analytics
- Kibana visualizations

## MinIO
- Parquet files
- Data lake storage

---

# ⚠️ Common Issues

| Issue | Solution |
|---|---|
| Kafka startup failure | `docker compose down -v` |
| Kibana import retrying | Wait for Kibana initialization |
| Spark checkpoint conflicts | Delete checkpoints |
| Elasticsearch unhealthy | Wait for JVM startup |

---

# 🧹 Reset Project

## Soft Reset

```bash
docker compose down
```

---

## Full Reset

```bash
docker compose down -v
```

---

# 🚀 Future Improvements

- Kubernetes deployment
- Airflow orchestration
- Grafana monitoring
- CI/CD pipeline
- Cloud deployment (AWS/Azure)

---

# 👨‍💻 Author

Abdelrahman Malek

ITI Data Management Graduation Project
