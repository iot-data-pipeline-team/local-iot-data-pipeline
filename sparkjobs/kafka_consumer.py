from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from PostgreSQLConnection import write_to_postgres
from schema_df import iot_schema
from transformations import apply_transformations

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


enhanced_df = flat_df


enhanced_df = apply_transformations(flat_df)


# query = (
#     flat_df
#     .writeStream
#     .format("console") # write to console
#     .outputMode("append")
#     .start()
# )

query = (
    flat_df
    .writeStream
    .foreachBatch(write_to_postgres)# write to postgres
    .outputMode("append")
    .start()
)




query.awaitTermination()