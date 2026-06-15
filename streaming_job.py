from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, hour, lit, sum, count, min, greatest
from pyspark.sql.functions import when, window, avg, to_timestamp, from_json, col
from pyspark.sql.functions import to_json, struct
from pyspark.sql.types import *

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
    .option("kafka.bootstrap.servers", "kafka1:9092,kafka2:9093,kafka3:9094") \
    .option("subscribe", "sensor-events") \
    .option("startingOffsets", "latest") \
    .load()




schema = StructType([
    StructField("event_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("machine_id", StringType()),
    StructField("machine_type", StringType()),
    StructField("floor", StringType()),
    StructField("shift", StringType()),
    StructField("status", StringType()),
    StructField("error_code", StringType()),
    StructField("is_fault", BooleanType()),

    StructField(
        "metrics",
        StructType([
            StructField("temperature", DoubleType()),
            StructField("vibration", DoubleType()),
            StructField("rpm", DoubleType()),
            StructField("power_kw", DoubleType())
        ])
    ),
    StructField(
        "cnc_sensors",
        StructType([
            StructField("oil_level_pct", DoubleType()),
            StructField("coolant_pressure_bar", DoubleType())
        ])
    ),

    StructField(
        "robot_sensors",
        StructType([
            StructField("joint_torque_nm", DoubleType()),
            StructField("end_effector_force_n", DoubleType())
        ])
    ),

    StructField(
        "conveyor_sensors",
        StructType([
            StructField("belt_tension_n", DoubleType()),
            StructField("load_weight_kg", DoubleType())
        ])
    ),

    StructField(
        "pump_sensors",
        StructType([
            StructField("oil_level_pct", DoubleType()),
            StructField("flow_rate_lpm", DoubleType()),
            StructField("inlet_pressure_bar", DoubleType())
        ])
    )    
])

bronze_df = df.selectExpr("CAST (value AS String)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

bronze_df = bronze_df.withColumn(
    "timestamp",
    to_timestamp(col("timestamp"))
)

bronze_df = bronze_df.select(
    "event_id",
    "timestamp",
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

silver_df = bronze_df \
    .filter(col("machine_id").isNotNull()) \
    .filter(col("timestamp").isNotNull()) \
    .filter(col("temperature").between(-20, 150)) \
    .select(
        "*",

        when(col("temperature") > 90, "Critical")
        .when(col("temperature") > 80, "Warning")
        .otherwise("Normal")
        .alias("temperature_status"),

        when(col("vibration") > 5, "Critical")
        .when(col("vibration") > 3, "Warning")
        .otherwise("Normal")
        .alias("vibration_status"),

        when(col("is_fault"), 1)
        .otherwise(0)
        .alias("fault_flag"),

        to_date(col("timestamp"))
        .alias("event_date"),

        hour(col("timestamp"))
        .alias("event_hour"),

        when(col("is_fault"), 0)
        .otherwise(
            greatest(
                lit(0),
                lit(100)
                - col("temperature") * 0.3
                - col("vibration") * 5
            )
        )
        .alias("health_score")
    )


silver_df = silver_df.withColumn(
    "anomaly_flag",
    when(col("temperature") > 90, 1).otherwise(0)
)

    
gold_df = silver_df \
    .withWatermark("timestamp", "1 minute") \
    .groupBy(
        col("machine_id"),
        window(col("timestamp"), "1 minute")
    ) \
    .agg(
        avg("temperature").alias("avg_temp"),
        avg("rpm").alias("avg_rpm"),
        avg("vibration").alias("avg_vibration"),
        avg("power_kw").alias("avg_power"),
        avg("health_score").alias("avg_health_score"),
        min("health_score").alias("min_health_score"),
        sum("fault_flag").alias("fault_count"),
        count("*").alias("total_events"),
        (
            sum("fault_flag") * 100.0 / count("*")
        ).alias("fault_percentage")
    )

gold_df = gold_df.select(
    col("machine_id"),
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("avg_temp"),
    col("avg_rpm"),
    col("avg_vibration"),
    col("avg_power"),
    col("avg_health_score"),
    col("min_health_score"),
    col("fault_count"),
    col("fault_percentage"),
    col("total_events")
)


silver_kafka_df = silver_df.select(
    to_json(
        struct(*silver_df.columns)
    ).alias("value")
)

silver_kafka_query = silver_kafka_df.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka1:9092,kafka2:9093,kafka3:9094") \
    .option("topic", "sensor-processed") \
    .option("checkpointLocation", "/home/jovyan/data/checkpoints/sensor_processed_kafka") \
    .outputMode("append") \
    .start()

# debug_query = silver_df.writeStream \
#     .format("console") \
#     .outputMode("append") \
#     .trigger(processingTime="2 seconds") \
#     .start()

def write_bronze_to_minio(batch_df, batch_id):

    batch_df.write \
        .mode("append") \
        .parquet(
            "s3a://iot-data/bronze/machine_bronze_data/"
        )
    
bronze_minio_query = bronze_df.writeStream \
    .foreachBatch(write_bronze_to_minio) \
    .option(
        "checkpointLocation",
        "/home/jovyan/data/checkpoints/machine_bronze_minio"
    ) \
    .start()

def write_silver_to_minio(batch_df, batch_id):

    batch_df.write \
        .mode("append") \
        .parquet(
            "s3a://iot-data/silver/machine_silver_data/"
        )
    
silver_minio_query = silver_df.writeStream \
    .foreachBatch(write_silver_to_minio) \
    .option(
        "checkpointLocation",
        "/home/jovyan/data/checkpoints/machine_silver_minio"
    ) \
    .start()

def write_gold_to_minio(batch_df, batch_id):

    batch_df.select(
        col("machine_id"),
        col("window_start"),
        col("window_end"),
        col("avg_temp"),
        col("avg_rpm"),
        col("avg_vibration"),
        col("avg_power"),
        col("avg_health_score"),
        col("min_health_score"),
        col("fault_count"),
        col("fault_percentage"),
        col("total_events")
    ).write \
     .mode("append") \
     .parquet(
         "s3a://iot-data/gold/machine_gold_data/"
     )
    





gold_minio_query = gold_df.writeStream \
    .foreachBatch(write_gold_to_minio) \
    .outputMode("update") \
    .option(
        "checkpointLocation",
        "/home/jovyan/data/checkpoints/machine_gold_minio"
    ) \
    .start()

def write_bronze_to_postgres(batch_df, batch_id):

    print(f"[BRONZE] Batch {batch_id}")

    batch_df.select(
        "event_id",
        "timestamp",
        "machine_id",
        "machine_type",
        "floor",
        "shift",
        "status",
        "error_code",
        "is_fault",

        "temperature",
        "vibration",
        "rpm",
        "power_kw",

        "cnc_oil",
        "coolant_pressure",

        "joint_torque",
        "force",

        "belt_tension",
        "load_weight",

        "flow_rate",
        "inlet_pressure"
    ).write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/db") \
        .option("dbtable", "machine_events_bronze") \
        .option("user", "user") \
        .option("password", "password") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()
    

bronze_postgres_query = bronze_df.writeStream \
    .foreachBatch(write_bronze_to_postgres) \
    .trigger(processingTime="2 seconds") \
    .option("checkpointLocation", "/home/jovyan/data/checkpoints/machine_bronze_postgres") \
    .start()

def write_silver_to_postgres(batch_df, batch_id):

    print(f"[POSTGRES] Batch {batch_id}")

    batch_df.select(
        "event_id",
        "timestamp",
        "machine_id",
        "machine_type",
        "floor",
        "shift",
        "status",
        "error_code",
        "is_fault",

        "temperature",
        "vibration",
        "rpm",
        "power_kw",

        "cnc_oil",
        "coolant_pressure",

        "joint_torque",
        "force",

        "belt_tension",
        "load_weight",

        "flow_rate",
        "inlet_pressure",

        "temperature_status",
        "vibration_status",

        "fault_flag",

        "event_date",
        "event_hour",

        "health_score",

        "anomaly_flag"
        
    ).write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/db") \
        .option("dbtable", "machine_events_silver") \
        .option("user", "user") \
        .option("password", "password") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()



silver_postgres_query = silver_df.writeStream \
    .foreachBatch(write_silver_to_postgres) \
    .trigger(processingTime="2 seconds") \
    .option("checkpointLocation", "/home/jovyan/data/checkpoints/machine_silver_postgres") \
    .start()








def write_gold_to_postgres(batch_df, batch_id):


    print(f"[AGG] Batch {batch_id}")



    batch_df = batch_df.select(
        col("machine_id"),
        col("window_start"),
        col("window_end"),
        col("avg_temp"),
        col("avg_rpm"),
        col("avg_vibration"),
        col("avg_power"),
        col("avg_health_score"),
        col("min_health_score"),
        col("fault_count"),
        col("fault_percentage"),
        col("total_events")

    )

    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/db") \
        .option("dbtable", "machine_aggregates_gold") \
        .option("user", "user") \
        .option("password", "password") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()


gold_postgres_query = gold_df.writeStream \
    .outputMode("update") \
    .foreachBatch(write_gold_to_postgres) \
    .trigger(processingTime="2 seconds") \
    .option("checkpointLocation", "/home/jovyan/data/checkpoints/machine_gold_postgres") \
    .start()








def write_silver_to_es(batch_df, batch_id):
    try:    
        batch_df.write \
            .format("org.elasticsearch.spark.sql") \
            .option("es.nodes", "elasticsearch") \
            .option("es.port", "9200") \
            .option("es.index.auto.create", "true") \
            .mode("append") \
            .save("machine-events")
    except Exception as e:
        print(f"[FATAL] Elasticsearch write failed: {e}")
        raise e    



silver_elastic_query = silver_df.writeStream \
    .foreachBatch(write_silver_to_es) \
    .trigger(processingTime="2 seconds") \
    .option("checkpointLocation", "/home/jovyan/data/checkpoints/machine_silver_elastic") \
    .start()


def write_gold_to_es(batch_df, batch_id):
    try:
        batch_df.select(
            col("machine_id"),
            col("window_start"),
            col("window_end"),
            col("avg_temp"),
            col("avg_rpm"),
            col("avg_vibration"),
            col("avg_power"),
            col("avg_health_score"),
            col("min_health_score"),
            col("fault_count"),
            col("fault_percentage"),
            col("total_events")
        ).write \
        .format("org.elasticsearch.spark.sql") \
        .option("es.nodes", "elasticsearch") \
        .option("es.port", "9200") \
        .option("es.index.auto.create", "true") \
        .mode("append") \
        .save("machine-aggregates")
    except Exception as e:
        print(f"[FATAL] Elasticsearch write failed: {e}")
        raise e




gold_elastic_query = gold_df.writeStream \
    .foreachBatch(write_gold_to_es) \
    .outputMode("update") \
    .option(
        "checkpointLocation",
        "/home/jovyan/data/checkpoints/machine_gold_elastic"
    ) \
    .start()











spark.streams.awaitAnyTermination()
