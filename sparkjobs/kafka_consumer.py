from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from schema_df import iot_schema
from transformations import apply_transformations
from aggregations import machine_summary
from WriteToPostgrSQL import write_to_postgres_bronze, write_to_postgres_silver, write_to_postgres_gold

# =============================================
spark = (
    SparkSession.builder
    .appName("KafkaReader")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.3"
    )
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
# ========================================

df = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        "localhost:9094,localhost:9095,localhost:9096"
    )
    .option("subscribe", "iot-sensors")
    .load()
)

# df.printSchema()

# query = (
#     df.writeStream
#     .format("console")
#     .start()
# )


# ==================================================

# parsed_df = (
#     df.selectExpr("CAST(value AS STRING)")
#       .select(
#           from_json(
#               col("value"),
#               iot_schema
#           ).alias("data")
#       )
#       .select("data.*")
# )

json_df = df.selectExpr(
    "CAST(value AS STRING)"
)

parsed_df = json_df.select(
    from_json(
        col("value"),
        iot_schema
    ).alias("data")
)
final_df = parsed_df.select(
    "data.*"
)

flat_df = final_df.select(
    "event_id",
    to_timestamp("timestamp").alias("event_time"),
    "machine_id",
    "machine_type",
    "floor",
    "shift",
    "status",
    "error_code",
    "is_fault",

    col("metrics.temperature").alias("temperature"),
    col("metrics.vibration").alias("vibration"),
    col("metrics.rpm").alias("rpm"),
    col("metrics.power_kw").alias("power_kw"),

    col("cnc_sensors.oil_level_pct").alias("cnc_oil"),
    col("cnc_sensors.coolant_pressure_bar").alias("coolant_pressure"),

    col("robot_sensors.joint_torque_nm").alias("joint_torque"),
    col("robot_sensors.end_effector_force_n").alias("force"),

    col("conveyor_sensors.belt_tension_n").alias("belt_tension"),
    col("conveyor_sensors.load_weight_kg").alias("load_weight"),

    col("pump_sensors.flow_rate_lpm").alias("flow_rate"),
    col("pump_sensors.inlet_pressure_bar").alias("inlet_pressure")
)

# ==========================================================
# apply transformations

enhanced_df = flat_df
enhanced_df = apply_transformations(enhanced_df)


# ==========================================================
# apply aggregations

aggregated_df = enhanced_df
aggregated_df = machine_summary(aggregated_df)


# query = (
#     flat_df
#     .writeStream
#     .format("console") # write to console
#     .outputMode("append")
#     .start()
# )

query_bronze = (
    flat_df
    .writeStream
    .foreachBatch(write_to_postgres_bronze)# write to postgres
    .outputMode("append")
    .start()
)


query_silver = (
    enhanced_df
    .writeStream
    .foreachBatch(write_to_postgres_silver)# write to postgres
    .outputMode("append")
    .start()
)

query_gold = (
    aggregated_df
    .writeStream
    .foreachBatch(write_to_postgres_gold)# write to postgres
    .outputMode("complete")
    .start()
)




# query_bronze.awaitTermination()
# query_silver.awaitTermination()
# query_gold.awaitTermination()

spark.streams.awaitAnyTermination()