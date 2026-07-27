from pyspark.sql.functions import *

# ==========================================================
# Basic Statistics
# ==========================================================

def sensor_statistics(df):
    """
    Descriptive statistics for machine sensors.
    """

    return df.select(

        min("temperature").alias("temperature_min"),
        max("temperature").alias("temperature_max"),
        avg("temperature").alias("temperature_avg"),
        stddev("temperature").alias("temperature_std"),

        min("rpm").alias("rpm_min"),
        max("rpm").alias("rpm_max"),
        avg("rpm").alias("rpm_avg"),
        stddev("rpm").alias("rpm_std"),

        min("vibration").alias("vibration_min"),
        max("vibration").alias("vibration_max"),
        avg("vibration").alias("vibration_avg"),
        stddev("vibration").alias("vibration_std"),

        min("power_kw").alias("power_min"),
        max("power_kw").alias("power_max"),
        avg("power_kw").alias("power_avg"),
        stddev("power_kw").alias("power_std")
    )


# ==========================================================
# Frequency Distributions
# ==========================================================

def machine_distribution(df):

    return (
        df.groupBy("machine_type")
          .count()
          .orderBy(desc("count"))
    )


def machine_id_distribution(df):

    return (
        df.groupBy("machine_id")
          .count()
          .orderBy(desc("count"))
    )


def status_distribution(df):

    return (
        df.groupBy("status")
          .count()
          .orderBy(desc("count"))
    )


def shift_distribution(df):

    return (
        df.groupBy("shift")
          .count()
          .orderBy(desc("count"))
    )


def floor_distribution(df):

    return (
        df.groupBy("floor")
          .count()
          .orderBy(desc("count"))
    )


def fault_distribution(df):

    return (
        df.groupBy("is_fault")
          .count()
          .orderBy(desc("count"))
    )


def error_code_distribution(df):

    return (
        df.groupBy("error_code")
          .count()
          .orderBy(desc("count"))
    )


# ==========================================================
# Numeric Summaries
# ==========================================================

def machine_numeric_summary(df):
    """
    Spark summary() for all numeric columns.
    """

    return df.select(
        "temperature",
        "vibration",
        "rpm",
        "power_kw"
    ).summary()


# ==========================================================
# Fault Statistics
# ==========================================================

def fault_rate_by_machine(df):

    return (
        df.groupBy("machine_type")
          .agg(
              count("*").alias("total_events"),
              sum(col("is_fault").cast("int")).alias("fault_events")
          )
          .withColumn(
              "fault_rate_percent",
              round(col("fault_events") * 100 / col("total_events"), 2)
          )
          .orderBy(desc("fault_rate_percent"))
    )


def fault_rate_by_shift(df):

    return (
        df.groupBy("shift")
          .agg(
              count("*").alias("total_events"),
              sum(col("is_fault").cast("int")).alias("fault_events")
          )
          .withColumn(
              "fault_rate_percent",
              round(col("fault_events") * 100 / col("total_events"), 2)
          )
          .orderBy("shift")
    )


# ==========================================================
# Sensor Statistics by Machine Type
# ==========================================================

def average_sensor_values(df):

    return (
        df.groupBy("machine_type")
          .agg(

              round(avg("temperature"),2).alias("avg_temperature"),

              round(avg("vibration"),2).alias("avg_vibration"),
              

              round(avg("rpm"),2).alias("avg_rpm"),

              round(avg("power_kw"),2).alias("avg_power")

          )
          .orderBy("machine_type")
    )