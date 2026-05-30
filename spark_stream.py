"""
IoT Sensor Stream Processing (Spark Structured Streaming)

Reads:  sensor-events      (raw producer JSON)
Writes: sensor-processed   (enriched per-event records)
        sensor-alerts       (critical/warning alerts)
        sensor_aggregates   (PostgreSQL via JDBC, 1-minute windows)
        sensor-aggregates   (Elasticsearch, windowed — Kibana dashboards)
        sensor-raw-events   (Elasticsearch, every event — Kibana Discover)
        Console             (dev/testing)
        hdfs://namenode:9000/data_lake/... (Parquet data lake, optional)

Run inside Jupyter PySpark container:

  docker cp spark_stream.py data-jupyter1:/home/jovyan/spark_stream.py
  docker exec -it data-jupyter1 spark-submit \\
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,\\
org.postgresql:postgresql:42.6.0,\\
org.elasticsearch:elasticsearch-spark-30_2.12:7.17.21 \\
    /home/jovyan/spark_stream.py
"""

import os
from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BROKERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka1:9092,kafka2:9093,kafka3:9094",
)
INPUT_TOPIC = "sensor-events"
OUTPUT_TOPIC = "sensor-processed"
ALERTS_TOPIC = "sensor-alerts"

POSTGRES_URL = "jdbc:postgresql://postgres:5432/data_hub"
POSTGRES_TABLE = "sensor_aggregates"
POSTGRES_PROPS = {
    "user": "admin",
    "password": "password123",
    "driver": "org.postgresql.Driver",
}

ES_HOST = os.getenv("ES_HOST", "elasticsearch")
ES_PORT = os.getenv("ES_PORT", "9200")
ES_AGG_INDEX = "sensor-aggregates"
ES_RAW_INDEX = "sensor-raw-events"

CHECKPOINT_BASE = os.getenv("SPARK_CHECKPOINT", "/home/jovyan/checkpoints")
DATA_LAKE_PATH = os.getenv("DATA_LAKE_PATH", "hdfs://namenode:9000/data_lake/sensor-events")
ENABLE_DATA_LAKE = os.getenv("ENABLE_DATA_LAKE", "false").lower() == "true"

# ── Schemas ───────────────────────────────────────────────────────────────────
METRICS_SCHEMA = StructType([
    StructField("temperature", DoubleType()),
    StructField("vibration", DoubleType()),
    StructField("rpm", DoubleType()),
    StructField("power_kw", DoubleType()),
])

CNC_SENSORS = StructType([
    StructField("oil_level_pct", DoubleType()),
    StructField("coolant_pressure_bar", DoubleType()),
])

ROBOT_SENSORS = StructType([
    StructField("joint_torque_nm", DoubleType()),
    StructField("end_effector_force_n", DoubleType()),
])

CONVEYOR_SENSORS = StructType([
    StructField("belt_tension_n", DoubleType()),
    StructField("load_weight_kg", DoubleType()),
])

PUMP_SENSORS = StructType([
    StructField("oil_level_pct", DoubleType()),
    StructField("flow_rate_lpm", DoubleType()),
    StructField("inlet_pressure_bar", DoubleType()),
])

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("machine_id", StringType()),
    StructField("machine_type", StringType()),
    StructField("floor", StringType()),
    StructField("shift", StringType()),
    StructField("status", StringType()),
    StructField("error_code", StringType()),
    StructField("is_fault", BooleanType()),
    StructField("metrics", METRICS_SCHEMA),
    StructField("cnc_sensors", CNC_SENSORS),
    StructField("robot_sensors", ROBOT_SENSORS),
    StructField("conveyor_sensors", CONVEYOR_SENSORS),
    StructField("pump_sensors", PUMP_SENSORS),
])

MACHINE_METADATA = [
    Row(machine_id="CNC_01", location="Floor A - CNC Bay", department="Machining"),
    Row(machine_id="ROB_01", location="Floor B - Assembly Line", department="Robotics"),
    Row(machine_id="CNV_01", location="Floor C - Logistics", department="Material Handling"),
    Row(machine_id="PMP_01", location="Floor C - Pump Room", department="Utilities"),
]


def build_spark():
    spark = (
        SparkSession.builder
        .appName("iot-sensor-stream")
        .config("spark.sql.shuffle.partitions", "6")
        .config("spark.sql.streaming.schemaInference", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_kafka_raw(spark):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("subscribe", INPUT_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_events(raw_df):
    return (
        raw_df
        .select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"))
        .select("e.*")
        .filter(F.col("machine_id").isNotNull())
    )


def clean_and_enrich(df, metadata_df):
    """Real-time cleaning, health score, KPIs, alerts, anomaly flags."""
    df = (
        df
        .withColumn("event_time", F.to_timestamp("timestamp"))
        .withColumn("temperature", F.col("metrics.temperature"))
        .withColumn("vibration", F.col("metrics.vibration"))
        .withColumn("rpm", F.col("metrics.rpm"))
        .withColumn("power_kw", F.col("metrics.power_kw"))
    )

    # 1. Data cleaning — drop physically impossible readings
    df = df.filter(
        F.col("temperature").isNotNull()
        & (F.col("temperature") > 0)
        & (F.col("temperature") < 150)
        & F.col("rpm").isNotNull()
        & (F.col("rpm") >= 0)
    )

    oil_level = F.coalesce(
        F.col("cnc_sensors.oil_level_pct"),
        F.col("pump_sensors.oil_level_pct"),
        F.lit(70.0),
    )
    flow_rate = F.col("pump_sensors.flow_rate_lpm")
    torque = F.col("robot_sensors.joint_torque_nm")

    vib_baseline = (
        F.when(F.col("machine_type") == "cnc_machine", F.lit(2.5))
        .when(F.col("machine_type") == "robot_arm", F.lit(2.2))
        .when(F.col("machine_type") == "conveyor_belt", F.lit(1.8))
        .when(F.col("machine_type") == "pump", F.lit(3.1))
        .otherwise(F.lit(2.5))
    )

    df = (
        df
        .withColumn("oil_level_pct", oil_level)
        # 2. Machine health score
        .withColumn(
            "health_score",
            F.round(100 - F.col("temperature") * 0.3 - F.col("vibration") * 10 + oil_level * 0.2, 1),
        )
        # 6. Real-time KPI — pump efficiency = flow / power
        .withColumn(
            "efficiency",
            F.when(
                flow_rate.isNotNull() & (F.col("power_kw") > 0),
                F.round(flow_rate / F.col("power_kw"), 2),
            ),
        )
        # 3. Fault / alert detection
        .withColumn(
            "alert",
            F.when(F.col("temperature") > 90, F.lit("Pump overheating"))
            .when(F.col("temperature") > 80, F.lit("High Temperature"))
            .when(F.col("vibration") > 6, F.lit("High Vibration"))
            .when(F.col("is_fault") == True, F.lit("Equipment Fault"))
            .otherwise(F.lit(None).cast("string")),
        )
        .withColumn(
            "alert_severity",
            F.when(F.col("temperature") > 90, F.lit("critical"))
            .when(F.col("alert").isNotNull(), F.lit("warning"))
            .otherwise(F.lit(None).cast("string")),
        )
        # 4. Predictive maintenance — rising robot torque
        .withColumn(
            "maintenance_risk",
            F.when(
                (F.col("machine_type") == "robot_arm") & torque.isNotNull() & (torque > 160),
                F.lit("high"),
            )
            .when(
                (F.col("machine_type") == "robot_arm") & torque.isNotNull() & (torque > 145),
                F.lit("medium"),
            )
            .otherwise(F.lit("low")),
        )
        # 7. Anomaly detection — vibration > 3× machine baseline
        .withColumn("anomaly", F.col("vibration") > (vib_baseline * 3))
        # Data lake partition columns
        .withColumn("year", F.year("event_time"))
        .withColumn("month", F.month("event_time"))
        .withColumn("day", F.dayofmonth("event_time"))
    )

    # 8. Join machine metadata (location enrichment)
    return df.join(F.broadcast(metadata_df), "machine_id", "left")


def build_processed(df):
    return df.select(
        "event_id",
        "timestamp",
        "event_time",
        "machine_id",
        "machine_type",
        "floor",
        "shift",
        "status",
        "error_code",
        "is_fault",
        "metrics",
        "cnc_sensors",
        "robot_sensors",
        "conveyor_sensors",
        "pump_sensors",
        "location",
        "department",
        "oil_level_pct",
        "health_score",
        "efficiency",
        "alert",
        "alert_severity",
        "maintenance_risk",
        "anomaly",
        "temperature",
        "vibration",
        "rpm",
        "power_kw",
        "year",
        "month",
        "day",
    )


def write_kafka_processed(processed_df):
    payload = processed_df.select(
        F.col("machine_id").cast("string").alias("key"),
        F.to_json(
            F.struct(
                "event_id", "timestamp", "machine_id", "machine_type", "floor", "shift",
                "status", "error_code", "is_fault", "metrics",
                "cnc_sensors", "robot_sensors", "conveyor_sensors", "pump_sensors",
                "location", "department", "health_score", "efficiency",
                "alert", "alert_severity", "maintenance_risk", "anomaly", "oil_level_pct",
            )
        ).alias("value"),
    )
    return (
        payload.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("topic", OUTPUT_TOPIC)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/kafka-processed")
        .outputMode("append")
        .start()
    )


def write_kafka_alerts(processed_df):
    alerts = (
        processed_df
        .filter(F.col("alert").isNotNull())
        .select(
            F.col("machine_id").cast("string").alias("key"),
            F.to_json(
                F.struct(
                    F.col("alert_severity").alias("severity"),
                    "machine_id",
                    F.col("alert").alias("message"),
                    "timestamp",
                    "health_score",
                    "temperature",
                    "vibration",
                )
            ).alias("value"),
        )
    )
    return (
        alerts.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("topic", ALERTS_TOPIC)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/kafka-alerts")
        .outputMode("append")
        .start()
    )


def write_console(processed_df):
    return (
        processed_df
        .select(
            "timestamp", "machine_id", "status", "temperature", "vibration",
            "health_score", "efficiency", "alert", "anomaly", "maintenance_risk",
        )
        .writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", "false")
        .trigger(processingTime="10 seconds")
        .queryName("processed-console")
        .start()
    )


def write_raw_to_elasticsearch(processed_df):
    def _write_batch(batch_df, _batch_id):
        if batch_df.isEmpty():
            return
        (
            batch_df
            .write
            .format("org.elasticsearch.spark.sql")
            .option("es.nodes", ES_HOST)
            .option("es.port", ES_PORT)
            .option("es.resource", ES_RAW_INDEX)
            .option("es.mapping.id", "event_id")
            .option("es.nodes.wan.only", "true")
            .mode("append")
            .save()
        )

    return (
        processed_df.writeStream
        .foreachBatch(_write_batch)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/es-raw")
        .outputMode("append")
        .trigger(processingTime="15 seconds")
        .start()
    )


def write_windowed_aggregates(processed_df):
    windowed = (
        processed_df
        .withWatermark("event_time", "2 minutes")
        .groupBy(
            F.window("event_time", "1 minute"),
            "machine_id",
            "machine_type",
            "floor",
            "shift",
        )
        .agg(
            F.round(F.avg("temperature"), 2).alias("avg_temperature"),
            F.round(F.avg("vibration"), 3).alias("avg_vibration"),
            F.round(F.avg("rpm"), 1).alias("avg_rpm"),
            F.round(F.avg("power_kw"), 3).alias("avg_power_kw"),
            F.round(F.avg("health_score"), 1).alias("avg_health_score"),
            F.round(F.avg("efficiency"), 2).alias("avg_efficiency"),
            F.count("*").cast("int").alias("event_count"),
            F.sum(F.when(F.col("is_fault"), 1).otherwise(0)).cast("int").alias("fault_count"),
            F.sum(F.when(F.col("anomaly"), 1).otherwise(0)).cast("int").alias("anomaly_count"),
        )
    )

    def _write_batch(batch_df, _batch_id):
        if batch_df.isEmpty():
            return

        flat = batch_df.select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "machine_id",
            "machine_type",
            "floor",
            "shift",
            "avg_temperature",
            "avg_vibration",
            "avg_rpm",
            "avg_power_kw",
            "avg_health_score",
            "avg_efficiency",
            "event_count",
            "fault_count",
            "anomaly_count",
        )

        flat.write.jdbc(
            POSTGRES_URL,
            POSTGRES_TABLE,
            mode="append",
            properties=POSTGRES_PROPS,
        )

        (
            flat.write
            .format("org.elasticsearch.spark.sql")
            .option("es.nodes", ES_HOST)
            .option("es.port", ES_PORT)
            .option("es.resource", ES_AGG_INDEX)
            .option("es.nodes.wan.only", "true")
            .mode("append")
            .save()
        )

    return (
        windowed.writeStream
        .foreachBatch(_write_batch)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/aggregates")
        .outputMode("update")
        .trigger(processingTime="1 minute")
        .start()
    )


def write_data_lake(processed_df):
    return (
        processed_df
        .writeStream
        .format("parquet")
        .option("path", DATA_LAKE_PATH)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/data-lake")
        .partitionBy("year", "month", "day", "machine_id")
        .outputMode("append")
        .trigger(processingTime="1 minute")
        .start()
    )


def main():
    spark = build_spark()
    metadata_df = spark.createDataFrame(MACHINE_METADATA)

    raw_kafka = read_kafka_raw(spark)
    events = parse_events(raw_kafka)
    enriched = clean_and_enrich(events, metadata_df)
    processed = build_processed(enriched)

    queries = [
        write_kafka_processed(processed),
        write_kafka_alerts(processed),
        write_console(processed),
        write_raw_to_elasticsearch(processed),
        write_windowed_aggregates(processed),
    ]

    if ENABLE_DATA_LAKE:
        queries.append(write_data_lake(processed))

    print("Spark streaming started.")
    print(f"  Input:       {INPUT_TOPIC}")
    print(f"  Processed:   {OUTPUT_TOPIC}")
    print(f"  Alerts:      {ALERTS_TOPIC}")
    print(f"  PostgreSQL:  {POSTGRES_TABLE}")
    print(f"  ES raw:      {ES_RAW_INDEX}")
    print(f"  ES agg:      {ES_AGG_INDEX}")
    if ENABLE_DATA_LAKE:
        print(f"  Data lake:   {DATA_LAKE_PATH}")
    print("Press Ctrl+C to stop.\n")

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
