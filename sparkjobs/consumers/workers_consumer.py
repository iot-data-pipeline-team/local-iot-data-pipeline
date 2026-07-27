from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp

from sparkjobs.consumers.schema_df import worker_schema

# ======================================================
# Configuration
# ======================================================

KAFKA_BOOTSTRAP = "localhost:9094,localhost:9095,localhost:9096"
WORKER_TOPIC = "worker-events"

# ======================================================
# Create Spark Session
# ======================================================

spark = (
    SparkSession.builder
    .appName("WorkerSafetyConsumer")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
        "org.postgresql:postgresql:42.7.3"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# ======================================================
# Read Worker Events from Kafka
# ======================================================

worker_kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", WORKER_TOPIC)
    .load()
)

# ======================================================
# Parse JSON Messages
# ======================================================

worker_json_df = worker_kafka_df.selectExpr(
    "CAST(value AS STRING) AS value"
)

worker_parsed_df = worker_json_df.select(
    from_json(
        col("value"),
        worker_schema
    ).alias("data")
)

# ======================================================
# Expand JSON Structure
# ======================================================

worker_events_df = worker_parsed_df.select(
    "data.*"
)

# ======================================================
# Flatten Worker Events
# ======================================================

worker_flattened_df = worker_events_df.select(

    to_timestamp("timestamp").alias("event_time"),

    "worker_id",
    "floor",
    "zone_id",

    "helmet_on",
    "safety_vest_on",

    "heart_rate",

    "movement_status",

    "danger_zone",

    "fatigue_score"
)

# ======================================================
# Display Stream (EDA)
# ======================================================

query = (
    worker_flattened_df.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", False)
    .start()
)

query.awaitTermination()