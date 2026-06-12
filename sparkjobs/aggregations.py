from pyspark.sql.functions import avg, sum, count

from pyspark.sql.functions import *

def machine_summary(df):

    return (
        df.groupBy("machine_id")
        .agg(

            count("*").alias("total_events"),

            avg("temperature").alias("avg_temp"),
            max("temperature").alias("max_temp"),

            avg("vibration").alias("avg_vibration"),
            max("vibration").alias("max_vibration"),

            avg("rpm").alias("avg_rpm"),

            avg("power_kw").alias("avg_power"),
            max("power_kw").alias("peak_power"),

            avg("health_score").alias("avg_health_score"),

            avg("risk_score").alias("avg_risk_score"),

            sum("fault_flag").alias("fault_count"),

            (
                sum("fault_flag") / count("*") * 100
            ).alias("fault_percentage"),

            (
                sum("running_flag") / count("*") * 100
            ).alias("uptime_percentage")
        )
    )


def hourly_summary(df):

    return (
        df.groupBy(
            "event_date",
            "event_hour",
            "machine_id"
        )
        .agg(
            avg("temperature").alias("avg_temp"),
            avg("power_kw").alias("avg_power"),
            avg("health_score").alias("avg_health_score"),
            sum("fault_flag").alias("fault_count")
        )
    )

def shift_summary(df):

    return (
        df.groupBy(
            "event_date",
            "time_bucket",
            "floor"
        )
        .agg(
            count("*").alias("total_events"),
            avg("temperature").alias("avg_temp"),
            avg("power_kw").alias("avg_power"),
            sum("fault_flag").alias("fault_count"),
            avg("health_score").alias("avg_health_score")
        )
    )