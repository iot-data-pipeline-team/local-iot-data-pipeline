from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

from schema_df import iot_schema

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
    col("event_id"),

    to_timestamp("timestamp").alias("event_time"),

    col("machine_id"),
    col("machine_type"),
    col("floor"),
    col("shift"),
    col("status"),
    col("error_code"),
    col("is_fault"),

    col("metrics.temperature").alias("temperature"),
    col("metrics.vibration").alias("vibration"),
    col("metrics.rpm").alias("rpm"),
    col("metrics.power_kw").alias("power_kw")
)

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
    .format("console") # write to console
    .outputMode("append")
    .start()
)



query.awaitTermination()