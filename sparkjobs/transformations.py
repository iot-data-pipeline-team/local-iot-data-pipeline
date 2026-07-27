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
def add_time_bucket(df):
    return df.withColumn(
        "time_bucket",
        when(col("event_hour").between(6,13), "Morning")
        .when(col("event_hour").between(14,21), "Evening")
        .otherwise("Night")
    )



def add_health_score(df):
    return df.withColumn(
        "health_score",
        when(col("is_fault"), 0)
        .otherwise(
            greatest(
                lit(0),
                lit(100)
                - col("temperature") * 0.3
                - col("vibration") * 5
            )
        )
    )
def add_risk_score(df):
    return df.withColumn(
        "risk_score",
        col("temperature") * 0.4 +
        col("vibration") * 10
    )
def add_fault_category(df):
    return df.withColumn(
        "fault_category",
        when(col("error_code").isin("E001", "E003"), "Overheat")
        .when(col("error_code").isin("E002", "E004"), "Vibration")
        .when(col("error_code") == "E005", "RPM Drop")
        .otherwise("None")
    )
def add_power_status(df):
    return df.withColumn(
        "power_status",
        when(col("power_kw") > 5, "High")
        .when(col("power_kw") > 3, "Normal")
        .otherwise("Low")
    )
def add_running_flag(df):
    return df.withColumn(
        "running_flag",
        when(col("status") == "running", 1)
        .otherwise(0)
    )



def apply_transformations(df):

    df = add_temperature_status(df)
    df = add_vibration_status(df)
    df = add_fault_flag(df)
    df = add_event_date(df)
    df = add_event_hour(df)
    df = add_health_score(df)
    df = add_fault_category(df)
    df = add_power_status(df)
    df = add_running_flag(df)
    df = add_time_bucket(df)
    df = add_risk_score(df)


    return df

