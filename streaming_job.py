from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, hour, lit, sum, count, min, greatest
from pyspark.sql.functions import when, window, avg, to_timestamp, from_json, col
from pyspark.sql.functions import to_json, struct, max
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

invalid_df = bronze_df.withColumn(
    "validation_reason",

    when(
        col("temperature").isNull(),
        "NULL_TEMPERATURE"
    )

    .when(
        ~col("temperature").between(-20,150),
        "TEMPERATURE_OUT_OF_RANGE"
    )

    .when(
        col("rpm") < 0,
        "INVALID_RPM"
    )

    .when(
        col("machine_id") == "",
        "EMPTY_MACHINE_ID"
    )

).filter(
    col("validation_reason").isNotNull()
)


valid_df = bronze_df.filter(
    col("temperature").isNotNull()
).filter(
    col("temperature").between(-20,150)
).filter(
    col("rpm") >= 0
).filter(
    col("machine_id") != ""
)

silver_df = valid_df  \
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
        .alias("health_score"),
        (col("temperature") * 0.4 + col("vibration") * 10).alias("risk_score"),
        when(col("status") == "running", 1).otherwise(0).alias("running_flag"),

        when(col("error_code").isin("E001", "E003"), "Overheat")
        .when(col("error_code").isin("E002", "E004"), "Vibration")
        .when(col("error_code") == "E005", "RPM Drop")
        .otherwise("None").alias("fault_category"),

        when(col("power_kw") > 5, "High")
        .when(col("power_kw") > 3, "Normal")
        .otherwise("Low")
        .alias("power_status"),

        when(hour(col("timestamp")).between(6, 13), "Morning")
        .when(hour(col("timestamp")).between(14, 21), "Evening")
        .otherwise("Night")
        .alias("time_bucket"),



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
        max("temperature").alias("max_temp"),
        avg("rpm").alias("avg_rpm"),
        avg("vibration").alias("avg_vibration"),
        max("vibration").alias("max_vibration"),
        avg("power_kw").alias("avg_power"),
        max("power_kw").alias("peak_power"),
        avg("health_score").alias("avg_health_score"),
        min("health_score").alias("min_health_score"),
        avg("risk_score").alias("avg_risk_score"),
        (sum("running_flag")/count("*")* 100).alias("uptime_percentage"),
        sum("fault_flag").alias("fault_count"),
        count("*").alias("total_events"),
        (sum("fault_flag") * 100.0 / count("*")).alias("fault_percentage")
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
    col("total_events"),
    col("max_temp"),
    col("max_vibration"),
    col("peak_power"),
    col("avg_risk_score"),
    col("uptime_percentage")    
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
        col("total_events"),
        col("max_temp"),
        col("max_vibration"),
        col("peak_power"),
        col("avg_risk_score"),
        col("uptime_percentage"),
                                      
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




def write_quarantine_to_postgres(
    batch_df,
    batch_id
):
    try:

        missing = []

        for c in [
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
            "validation_reason"
        ]:
            if c not in batch_df.columns:
                missing.append(c)

        print("MISSING COLUMNS:", missing)    
        print("===== QUARANTINE BATCH =====")
        print(batch_df.columns)
        # invalid_count = batch_df.count()

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
            "validation_reason"
            ).write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/db") \
            .option("dbtable", "machine_events_quarantine") \
            .option("user", "user") \
            .option("password", "password") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()

        # print(
        #     f"[QUALITY] Batch {batch_id}"
        #     f" Invalid Records = {invalid_count}"
        # )
    except Exception as e:
        print("QUARANTINE ERROR:")
        print(str(e))
        raise        

invalid_query = (
    invalid_df.writeStream
    .foreachBatch(write_quarantine_to_postgres)
    .option(
        "checkpointLocation",
        "/home/jovyan/data/checkpoints/quarantine"
    )
    .start()
)    





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
        "power_status",
        

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

        "running_flag",
        
        "fault_flag",
        "fault_category",
        "event_date",
        "event_hour",
        "time_bucket",
        

        "health_score",
        
        "risk_score",

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
        col("total_events"),
        col("max_temp"),
        col("max_vibration"),
        col("peak_power"),
        col("avg_risk_score"),
        col("uptime_percentage"),        

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
            col("total_events"),
            col("max_temp"),
            col("max_vibration"),
            col("peak_power"),
            col("avg_risk_score"),
            col("uptime_percentage")          
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










##----------------------------- Workers Code -----------------

worker_schema = StructType([
    StructField(
        "worker_id",
        StringType()
    ),

    StructField(
        "timestamp",
        StringType()
    ),

    StructField(
        "floor",
        StringType()
    ),

    StructField(
        "helmet_on",
        BooleanType()
    ),

    StructField(
        "danger_zone",
        BooleanType()
    ),

    StructField(
        "fatigue_score",
        IntegerType()
    )
])


worker_raw_df = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        "kafka1:9092,kafka2:9093,kafka3:9094"
    )
    .option(
        "subscribe",
        "worker-events"
    )
    .load()
)

worker_bronze_df = (
    worker_raw_df
    .selectExpr("CAST(value AS STRING)")
    .select(
        from_json(
            col("value"),
            worker_schema
        ).alias("data")
    )
    .select("data.*")
)

worker_bronze_df = (
    worker_bronze_df
    .withColumn(
        "timestamp",
        to_timestamp(col("timestamp"))
    )
)



worker_silver_df = (
    worker_bronze_df

    .withColumn(
        "safety_violation_flag",

        when(
            col("helmet_on") == False,
            1
        ).otherwise(0)
    )

    .withColumn(
        "fatigue_status",

        when(
            col("fatigue_score") > 80,
            "High"
        )
        .when(
            col("fatigue_score") > 50,
            "Medium"
        )
        .otherwise("Low")
    )

    .withColumn(
        "worker_risk_level",

        when(
            (col("danger_zone") == True)
            &
            (col("fatigue_score") > 80),
            "Critical"
        )
        .when(
            col("danger_zone") == True,
            "High"
        )
        .otherwise("Normal")
    )
)


worker_gold_df = (
    worker_silver_df

    .withWatermark(
        "timestamp",
        "1 minute"
    )

    .groupBy(
        window(
            col("timestamp"),
            "5 minutes"
        )
    )

    .agg(

        sum(
            "safety_violation_flag"
        ).alias(
            "violations_per_hour"
        ),

        sum(
            when(
                col("danger_zone"),
                1
            ).otherwise(0)
        ).alias(
            "workers_in_danger_zone"
        ),

        avg(
            "fatigue_score"
        ).alias(
            "avg_fatigue_score"
        )
    )
)

worker_gold_df = (
    worker_gold_df

    .select(
        col("window.start")
        .alias("window_start"),

        col("window.end")
        .alias("window_end"),

        col("violations_per_hour"),

        col("workers_in_danger_zone"),

        col("avg_fatigue_score")
    )
)

def write_worker_bronze_to_postgres(
    batch_df,
    batch_id
):

    print(
        f"[WORKER BRONZE] Batch {batch_id}"
    )

    batch_df.select(
        "worker_id",
        "timestamp",
        "floor",
        "helmet_on",
        "danger_zone",
        "fatigue_score"
    ).write \
        .format("jdbc") \
        .option(
            "url",
            "jdbc:postgresql://postgres:5432/db"
        ) \
        .option(
            "dbtable",
            "worker_events_bronze"
        ) \
        .option("user", "user") \
        .option("password", "password") \
        .option(
            "driver",
            "org.postgresql.Driver"
        ) \
        .mode("append") \
        .save()
    

worker_bronze_postgres_query = (
    worker_bronze_df
    .writeStream
    .foreachBatch(
        write_worker_bronze_to_postgres
    )
    .trigger(
        processingTime="2 seconds"
    )
    .option(
        "checkpointLocation",
        "/home/jovyan/data/checkpoints/worker_bronze_postgres"
    )
    .start()
)


def write_worker_silver_to_postgres(
    batch_df,
    batch_id
):

    print(
        f"[WORKER SILVER] Batch {batch_id}"
    )

    batch_df.select(
        "worker_id",
        "timestamp",
        "floor",
        "helmet_on",
        "danger_zone",
        "fatigue_score",
        "safety_violation_flag",
        "fatigue_status",
        "worker_risk_level"
    ).write \
        .format("jdbc") \
        .option(
            "url",
            "jdbc:postgresql://postgres:5432/db"
        ) \
        .option(
            "dbtable",
            "worker_events_silver"
        ) \
        .option("user", "user") \
        .option("password", "password") \
        .option(
            "driver",
            "org.postgresql.Driver"
        ) \
        .mode("append") \
        .save()
    

worker_silver_postgres_query = (
    worker_silver_df
    .writeStream
    .foreachBatch(
        write_worker_silver_to_postgres
    )
    .trigger(
        processingTime="2 seconds"
    )
    .option(
        "checkpointLocation",
        "/home/jovyan/data/checkpoints/worker_silver_postgres"
    )
    .start()
)




def write_worker_gold_to_postgres(
    batch_df,
    batch_id
):

    print(
        f"[WORKER GOLD] Batch {batch_id}"
    )

    batch_df.write \
        .format("jdbc") \
        .option(
            "url",
            "jdbc:postgresql://postgres:5432/db"
        ) \
        .option(
            "dbtable",
            "worker_safety_gold"
        ) \
        .option("user", "user") \
        .option("password", "password") \
        .option(
            "driver",
            "org.postgresql.Driver"
        ) \
        .mode("append") \
        .save()
    
worker_gold_postgres_query = (
    worker_gold_df
    .writeStream
    .outputMode("update")
    .foreachBatch(
        write_worker_gold_to_postgres
    )
    .option(
        "checkpointLocation",
        "/home/jovyan/data/checkpoints/worker_gold_postgres"
    )
    .start()
)

def write_worker_silver_to_es(
    batch_df,
    batch_id
):
    try:

        batch_df.select(
            "worker_id",
            "timestamp",
            "floor",
            "helmet_on",
            "danger_zone",
            "fatigue_score",
            "safety_violation_flag",
            "fatigue_status",
            "worker_risk_level"
        ).write \
        .format(
            "org.elasticsearch.spark.sql"
        ) \
        .option(
            "es.nodes",
            "elasticsearch"
        ) \
        .option(
            "es.port",
            "9200"
        ) \
        .option(
            "es.index.auto.create",
            "true"
        ) \
        .mode("append") \
        .save(
            "worker-events"
        )

    except Exception as e:

        print(
            f"[FATAL] Worker ES write failed: {e}"
        )

        raise e
    

worker_silver_elastic_query = (
    worker_silver_df
    .writeStream
    .foreachBatch(
        write_worker_silver_to_es
    )
    .trigger(
        processingTime="2 seconds"
    )
    .option(
        "checkpointLocation",
        "/home/jovyan/data/checkpoints/worker_silver_elastic"
    )
    .start()
)


def write_worker_gold_to_es(
    batch_df,
    batch_id
):
    try:

        batch_df.select(
            "window_start",
            "window_end",
            "violations_per_hour",
            "workers_in_danger_zone",
            "avg_fatigue_score"
        ).write \
        .format(
            "org.elasticsearch.spark.sql"
        ) \
        .option(
            "es.nodes",
            "elasticsearch"
        ) \
        .option(
            "es.port",
            "9200"
        ) \
        .option(
            "es.index.auto.create",
            "true"
        ) \
        .mode("append") \
        .save(
            "worker-safety"
        )

    except Exception as e:

        print(
            f"[FATAL] Worker Gold ES write failed: {e}"
        )

        raise e
    
worker_gold_elastic_query = (
    worker_gold_df
    .writeStream
    .outputMode("update")
    .foreachBatch(
        write_worker_gold_to_es
    )
    .option(
        "checkpointLocation",
        "/home/jovyan/data/checkpoints/worker_gold_elastic"
    )
    .start()
)    



spark.streams.awaitAnyTermination()
