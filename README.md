# 🚀 Real-Time IoT Data Pipeline

This project implements a real-time data engineering pipeline using Kafka, Spark Structured Streaming, PostgreSQL, and Power BI.

---

## 🎥 Demo Video

Watch the full pipeline walkthrough here:  
https://drive.google.com/your-link

---

## 📸 Screenshots

---


## 🧠 Architecture

```
IoT Producer → Kafka → Spark Structured Streaming → PostgreSQL → Power BI
                           ↓
                        Parquet (Data Lake)
```

---

## ⚙️ Tech Stack

* Python (IoT Producer)
* Apache Kafka
* Apache Spark (Structured Streaming)
* PostgreSQL
* Docker & Docker Compose
* Power BI (DirectQuery)

---

## 📦 Project Setup (Step-by-Step)

### 1️⃣ Clone Repository

```bash
git clone -b baseline-pipeline-malek https://github.com/iot-data-pipeline-team/local-iot-data-pipeline.git
cd local-iot-data-pipeline
```

---

### 2️⃣ Create Virtual Environment

```bash
python -m venv venv
venv\Scripts\activate   # Windows
```

---

### 3️⃣ Install Dependencies

```bash
pip install -r requirements.txt
```

---

### 4️⃣ Start Docker Services

```bash
docker-compose up -d
```

Check containers:

```bash
docker ps
```

---

### 5️⃣ Create Kafka Topic

```bash
docker exec -it kafka kafka-topics \
--create \
--topic iot-data \
--bootstrap-server kafka:9092 \
--partitions 1 \
--replication-factor 1
```

---

### 6️⃣ Run IoT Producer

```bash
python producer/iot_producer.py
```

---

### 7️⃣ Run Spark Streaming Job

Enter container:

```bash
docker exec -it jupyter bash
cd /home/jovyan/work
```

Run Spark:

```bash
spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
--jars /home/jovyan/jars/postgresql-42.6.2.jar \
streaming_job.py
```

---

### 8️⃣ Verify Data in PostgreSQL

```bash
docker exec -it postgres psql -U user -d db
```

```sql
SELECT count(*) FROM iot_data;
SELECT count(*) FROM iot_aggregates;
```

---

## 🧹 Reset Pipeline (if needed)

```bash
rm -rf /home/jovyan/data/output/*
rm -rf /home/jovyan/data/checkpoints/*
```

---

## 📊 PostgreSQL Views (for Power BI)

### 🔹 Raw Data View

```sql
CREATE OR REPLACE VIEW iot_data_view AS
SELECT
    device_id,
    device_type,
    location,
    technician,
    temperature,
    humidity,
    status,
    timestamp AT TIME ZONE 'UTC' AS event_time,
    anomaly_flag,
    CASE 
        WHEN timestamp >= NOW() - INTERVAL '60 seconds' THEN 1 
        ELSE 0
    END AS is_recent
FROM iot_data;
```

---

## ⚡ Performance Notes

* Spark uses **micro-batch processing**
* Use `.trigger(processingTime="2 seconds")` for near real-time updates
* Use `.option("maxOffsetsPerTrigger", 10)` to reduce latency
* JDBC writes are batched → slight delay is expected

---

## 🎯 Key Features

* Real-time data ingestion from IoT devices
* Data cleaning and validation
* Window-based aggregation
* Anomaly detection
* Dual pipeline:

  * Real-time → PostgreSQL
  * Storage → Parquet
* Power BI integration via DirectQuery

---

## ⚠️ Common Issues

| Issue                 | Solution           |
| --------------------- | ------------------ |
| No data in aggregates | Wait for window    |
| Spark crashes         | Delete checkpoints |
| PostgreSQL type error | Fix schema         |
| Kafka not working     | Check Docker       |

---

## 🧠 Learning Outcomes

* Real-time streaming with Spark
* Kafka as buffering layer
* Micro-batch processing
* Data pipeline design
* Debugging pipelines

---

## 🚀 Future Improvements

* Deploy on AWS (Kinesis, S3, Redshift)
* Add monitoring
* Improve schema (star schema)
* Reduce latency further

---

## 👨‍💻 Author

Abdelrahman Malek
