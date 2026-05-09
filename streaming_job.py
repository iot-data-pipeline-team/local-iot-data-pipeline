from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType
from pyspark.sql.functions import to_timestamp
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import when
from pyspark.sql.functions import window, avg

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("KafkaTest") \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "iot-data") \
    .option("startingOffsets", "latest") \
    .load()


schema = StructType() \
    .add("device_id", StringType()) \
    .add("device_type", StringType()) \
    .add("location", StringType()) \
    .add("technician", StringType()) \
    .add("temperature", DoubleType()) \
    .add("humidity", DoubleType()) \
    .add("status", StringType()) \
    .add("timestamp", StringType())

parsed_df = df.selectExpr("CAST (value AS String)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

parsed_df = parsed_df.withColumn(
    "timestamp",
    to_timestamp(col("timestamp"))
)

clean_df = parsed_df \
    .filter(col("device_id").isNotNull())\
    .filter(col("timestamp").isNotNull())\
    .filter(col("temperature").between(-20, 80))\
    .filter(col("humidity").between(0, 100))


clean_df = clean_df.withColumn(
    "anomaly_flag",
    when(col("temperature") > 50, 1).otherwise(0)
)


query_parquet = clean_df.writeStream \
    .format("parquet") \
    .trigger(processingTime="2 seconds") \
    .option("path", "s3a://iot-data/raw/") \
    .option("checkpointLocation", "/home/jovyan/data/checkpoints/parquet") \
    .partitionBy("device_id") \
    .outputMode("append") \
    .start()

debug_query = clean_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .trigger(processingTime="2 seconds") \
    .start()

schema_parquet = StructType() \
    .add("device_id", StringType()) \
    .add("device_type", StringType()) \
    .add("location", StringType()) \
    .add("technician", StringType()) \
    .add("temperature", DoubleType()) \
    .add("humidity", DoubleType()) \
    .add("status", StringType()) \
    .add("timestamp", TimestampType())\
    .add("anomaly_flag", IntegerType())






agg_df = clean_df \
    .withWatermark("timestamp", "1 minute")\
    .groupBy(
        col("device_id"),
        window(col("timestamp"), "1 minute")
    ) \
    .agg(
        avg("temperature").alias("avg_temp"),
        avg("humidity").alias("avg_humidity")
    )


def write_raw(batch_df, batch_id):
    print(f"[PARQUET → POSTGRES] Batch {batch_id}, rows: {batch_df.count()}")

    batch_df.select(
        "device_id",
        "device_type",
        "location",
        "technician",
        "temperature",
        "humidity",
        "status",
        "timestamp",
        "anomaly_flag"
    ).write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/db") \
        .option("dbtable", "iot_data") \
        .option("user", "user") \
        .option("password", "password") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()
    
def write_agg(batch_df, batch_id):

    count = batch_df.count()

    print(f"[AGG] Batch {batch_id}, rows: {count}")

    if count > 0:
        batch_df.show(truncate=False)

    batch_df = batch_df.select(
        col("device_id"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("avg_temp"),
        col("avg_humidity")
    )

    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/db") \
        .option("dbtable", "iot_aggregates") \
        .option("user", "user") \
        .option("password", "password") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

def write_to_es(batch_df, batch_id):
    batch_df.write \
        .format("org.elasticsearch.spark.sql") \
        .option("es.nodes", "elasticsearch") \
        .option("es.port", "9200") \
        .mode("append") \
        .save("iot_index")

raw_query = clean_df.writeStream \
    .foreachBatch(write_raw) \
    .trigger(processingTime="2 seconds") \
    .option("checkpointLocation", "/home/jovyan/data/checkpoints/raw_data") \
    .start()

agg_query = agg_df.writeStream \
    .foreachBatch(write_agg) \
    .trigger(processingTime="2 seconds") \
    .option("checkpointLocation", "/home/jovyan/data/checkpoints/agg_data") \
    .start()

es_query = clean_df.writeStream \
    .foreachBatch(write_to_es) \
    .trigger(processingTime="2 seconds") \
    .option("checkpointLocation", "/home/jovyan/data/checkpoints/es_data") \
    .start()



spark.streams.awaitAnyTermination()
