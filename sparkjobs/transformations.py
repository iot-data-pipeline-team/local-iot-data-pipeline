from pyspark.sql.functions import *

def add_temperature_status(df):
    return df.withColumn(
        "temperature_status",
        when(col("temperature") > 90, "Critical")
        .when(col("temperature") > 80, "Warning")
        .otherwise("Normal")
    )


def add_vibration_status(df):
    return df.withColumn(
        "vibration_status",
        when(col("vibration") > 5, "Critical")
        .when(col("vibration") > 3, "Warning")
        .otherwise("Normal")
    )


def add_fault_flag(df):
    return df.withColumn(
        "fault_flag",
        when(col("is_fault"), 1)
        .otherwise(0)
    )


def add_event_date(df):
    return df.withColumn(
        "event_date",
        to_date(col("event_time"))
    )


def add_event_hour(df):
    return df.withColumn(
        "event_hour",
        hour(col("event_time"))
    )


def add_health_score(df):
    return df.withColumn(
        "health_score",
        when(col("is_fault"), 0)
        .otherwise(
            lit(100)
            - col("temperature") * 0.3
            - col("vibration") * 5
        )
    )

def apply_transformations(df):

    df = add_temperature_status(df)
    df = add_vibration_status(df)
    df = add_fault_flag(df)
    df = add_event_date(df)
    df = add_event_hour(df)
    df = add_health_score(df)

    return df