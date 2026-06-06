from pyspark.sql.functions import *


def machine_summary(df):

    return (
        df.groupBy("machine_id")
        .agg(
            avg("temperature").alias("avg_temp"),
            avg("vibration").alias("avg_vibration"),
            avg("rpm").alias("avg_rpm"),
            avg("power_kw").alias("avg_power"),
            sum("fault_flag").alias("fault_count")
        )
    )


def average_temperature(df):

    return (
        df.groupBy("machine_id")
        .agg(
            avg("temperature").alias("avg_temperature")
        )
    )


def fault_count(df):

    return (
        df.groupBy("machine_id")
        .agg(
            sum("fault_flag").alias("fault_count")
        )
    )


def fault_percentage(df):

    return (
        df.groupBy("machine_id")
        .agg(
            (
                sum("fault_flag") / count("*") * 100
            ).alias("fault_percentage")
        )
    )

def health_summary(df):

    return (
        df.groupBy("machine_id")
        .agg(
            avg("health_score").alias("avg_health_score")
        )
    )

def shift_summary(df):

    return (
        df.groupBy("shift")
        .agg(
            avg("temperature").alias("avg_temp"),
            avg("power_kw").alias("avg_power"),
            sum("fault_flag").alias("fault_count")
        )
    )