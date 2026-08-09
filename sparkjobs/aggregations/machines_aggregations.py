from pyspark.sql.functions import *
from pyspark.sql.functions import countDistinct
# ======================================================
# Machine Summary
# One record per machine
# ======================================================

def machine_summary(df):

    return (
        df.groupBy("machine_id")
        .agg(

            first("machine_type").alias("machine_type"),
            first("machine_group").alias("machine_group"),

            count("*").alias("total_events"),

            round(avg("temperature"), 2).alias("avg_temp"),
            round(max("temperature"), 2).alias("max_temp"),

            round(avg("vibration"), 2).alias("avg_vibration"),
            round(max("vibration"), 2).alias("max_vibration"),

            round(avg("rpm"), 2).alias("avg_rpm"),

            round(avg("power_kw"), 2).alias("avg_power"),
            round(max("power_kw"), 2).alias("peak_power"),

            round(avg("health_score"), 2).alias("avg_health_score"),
            round(avg("risk_score"), 2).alias("avg_risk_score"),

            sum("fault_flag").alias("fault_count"),

            round(
                sum("fault_flag") * 100 / count("*"),
                2
            ).alias("fault_percentage"),

            round(
                sum("running_flag") * 100 / count("*"),
                2
            ).alias("uptime_percentage")
        )
    )


# ======================================================
# Hourly Summary
# One record per machine per hour
# ======================================================

def hourly_summary(df):

    return (
        df.groupBy(
            "event_date",
            "event_hour",
            "machine_id"
        )
        .agg(

            round(avg("temperature"), 2).alias("avg_temp"),

            round(avg("vibration"), 2).alias("avg_vibration"),

            round(avg("rpm"), 2).alias("avg_rpm"),

            round(avg("power_kw"), 2).alias("avg_power"),

            round(avg("health_score"), 2).alias("avg_health_score"),

            round(avg("risk_score"), 2).alias("avg_risk_score"),

            sum("fault_flag").alias("fault_count"),

            sum("running_flag").alias("running_events")
        )
    )


# ======================================================
# Shift Summary
# One record per floor and shift
# ======================================================

def shift_summary(df):

    return (
        df.groupBy(
            "event_date",
            "time_bucket",
            "floor"
        )
        .agg(

            count("*").alias("total_events"),

            round(avg("temperature"), 2).alias("avg_temp"),

            round(avg("vibration"), 2).alias("avg_vibration"),

            round(avg("rpm"), 2).alias("avg_rpm"),

            round(avg("power_kw"), 2).alias("avg_power"),

            round(avg("health_score"), 2).alias("avg_health_score"),

            round(avg("risk_score"), 2).alias("avg_risk_score"),

            sum("fault_flag").alias("fault_count")
        )
    )


# ======================================================
# Machine Type Summary
# ======================================================

def machine_type_summary(df):

    return (
        df.groupBy(
            "machine_type",
            "machine_group"
        )
        .agg(

            count("*").alias("total_events"),

            round(avg("temperature"), 2).alias("avg_temp"),

            round(avg("vibration"), 2).alias("avg_vibration"),

            round(avg("power_kw"), 2).alias("avg_power"),

            round(avg("health_score"), 2).alias("avg_health_score"),

            round(avg("risk_score"), 2).alias("avg_risk_score"),

            sum("fault_flag").alias("fault_count")
        )
    )


# ======================================================
# Floor Summary
# ======================================================

def floor_summary(df):

    return (
        df.groupBy("floor")
        .agg(

            count("*").alias("total_events"),

            countDistinct("machine_id").alias("machines"),

            round(avg("temperature"), 2).alias("avg_temp"),

            round(avg("power_kw"), 2).alias("avg_power"),

            round(avg("health_score"), 2).alias("avg_health_score"),

            sum("fault_flag").alias("fault_count")
        )
    )


# ======================================================
# Fault Summary
# ======================================================

def fault_summary(df):

    return (
        df.filter(col("fault_flag") == 1)
        .groupBy("fault_category")
        .agg(

            count("*").alias("fault_count"),

            round(avg("temperature"), 2).alias("avg_temp"),

            round(avg("vibration"), 2).alias("avg_vibration"),

            round(avg("risk_score"), 2).alias("avg_risk_score")
        )
    )

