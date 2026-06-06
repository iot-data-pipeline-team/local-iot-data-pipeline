from pyspark.sql.functions import avg, sum, count

def machine_summary(df):

    return (
        df.groupBy("machine_id")
        .agg(
            avg("temperature").alias("avg_temp"),
            avg("vibration").alias("avg_vibration"),
            avg("rpm").alias("avg_rpm"),
            avg("power_kw").alias("avg_power"),
            avg("health_score").alias("avg_health_score"),
            sum("fault_flag").alias("fault_count"),
            (
                sum("fault_flag") / count("*") * 100
            ).alias("fault_percentage")
        )
    )