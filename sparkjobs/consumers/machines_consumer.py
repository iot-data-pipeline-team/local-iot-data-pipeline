import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from sparkjobs.consumers.schema_df import machine_schema
from sparkjobs.eda.machines.machine_eda import run_machine_eda
from sparkjobs.validations.machine_validation import validate_machine_data

# ======================================================
# Configuration
# ======================================================

KAFKA_BOOTSTRAP = "localhost:9094,localhost:9095,localhost:9096"
MACHINE_TOPIC = "sensor-events"

# ======================================================
# Create Spark Session
# ======================================================

spark = (
    SparkSession.builder
    .appName("MachineSensorConsumer")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
        "org.postgresql:postgresql:42.7.3"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# ======================================================
# Read Machine Events from Kafka
# ======================================================

machine_kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", MACHINE_TOPIC)
    .load()
)

# ======================================================
# Parse JSON Messages
# ======================================================

machine_json_df = machine_kafka_df.selectExpr(
    "CAST(value AS STRING) AS value"
)

machine_parsed_df = machine_json_df.select(
    from_json(
        col("value"),
        machine_schema
    ).alias("data")
)

# ======================================================
# Expand JSON Structure
# ======================================================

machine_events_df = machine_parsed_df.select(
    "data.*"
)

# ======================================================
# Flatten Nested Structure
# ======================================================

machine_flattened_df = machine_events_df.select(

    # Event information
    "event_id",
    to_timestamp("timestamp").alias("event_time"),
    "machine_id",
    "machine_type",
    "floor",
    "shift",
    "status",
    "error_code",
    "is_fault",

    # Common machine metrics
    col("metrics.temperature").alias("temperature"),
    col("metrics.vibration").alias("vibration"),
    col("metrics.rpm").alias("rpm"),
    col("metrics.power_kw").alias("power_kw"),

    # CNC sensors
    col("cnc_sensors.oil_level_pct").alias("cnc_oil"),
    col("cnc_sensors.coolant_pressure_bar").alias("coolant_pressure"),

    # Robot sensors
    col("robot_sensors.joint_torque_nm").alias("joint_torque"),
    col("robot_sensors.end_effector_force_n").alias("force"),

    # Conveyor sensors
    col("conveyor_sensors.belt_tension_n").alias("belt_tension"),
    col("conveyor_sensors.load_weight_kg").alias("load_weight"),

    # Pump sensors
    col("pump_sensors.oil_level_pct").alias("pump_oil"),
    col("pump_sensors.flow_rate_lpm").alias("flow_rate"),
    col("pump_sensors.inlet_pressure_bar").alias("inlet_pressure")
)

# ======================================================
# Display Stream (EDA)
# ======================================================

# query = (
#     machine_flattened_df.writeStream
#     .format("console")
#     .outputMode("append")
#     .option("truncate", False)
#     .start()
# )

# query.awaitTermination()

# ===================================
# run validations
# validated_df = validate_machine_data(machine_flattened_df)
# query = (
#     validated_df.writeStream
#     .format("console")
#     .outputMode("append")
#     .option("truncate", False)
#     .start()
# )

# =================================================
# run eda 
validated_df = validate_machine_data(machine_flattened_df)

def run_eda(batch_df, batch_id):

    print(f"\nProcessing Batch {batch_id}")

    batch_df.cache()

    # ---------------------------
    # valid records
    # ---------------------------
    valid_df = batch_df.filter(col("is_valid"))

    # ---------------------------
    # invalid records
    # ---------------------------
    invalid_df = batch_df.filter(~col("is_valid"))

    print(f"Valid rows   : {valid_df.count()}")
    print(f"Invalid rows : {invalid_df.count()}")
    print("\nInvalid Records")

    invalid_df.select(
        "event_id",
        "machine_id",
        "machine_type",
        "is_valid"
    ).show(truncate=False)

    # ---------------------------
    # EDA
    # ---------------------------
    run_machine_eda(valid_df)

    # later
    # write_bronze(valid_df)
    # write_invalid(invalid_df)

    batch_df.unpersist()

    time.sleep(120)

# query = (
#     machine_flattened_df.writeStream
#     .foreachBatch(run_eda)
#     .outputMode("append")
#     .start()
# )




query = (
    validated_df.writeStream
    .foreachBatch(run_eda)
    .outputMode("append")
    .option(
        "checkpointLocation",
        "checkpoints/machine_consumer"
    )
    .start()
)

query.awaitTermination()

