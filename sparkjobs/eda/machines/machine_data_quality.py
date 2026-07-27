from pyspark.sql.functions import *


# ==========================================================
# Missing Values
# ==========================================================

def check_nulls(df):

    return df.select(

        count("*").alias("total_rows"),

        sum(col("event_id").isNull().cast("int")).alias("event_id_null"),
        sum(col("event_time").isNull().cast("int")).alias("event_time_null"),
        sum(col("machine_id").isNull().cast("int")).alias("machine_id_null"),
        sum(col("machine_type").isNull().cast("int")).alias("machine_type_null"),
        sum(col("floor").isNull().cast("int")).alias("floor_null"),
        sum(col("shift").isNull().cast("int")).alias("shift_null"),
        sum(col("status").isNull().cast("int")).alias("status_null"),
        sum(col("error_code").isNull().cast("int")).alias("error_code_null"),

        sum(col("temperature").isNull().cast("int")).alias("temperature_null"),
        sum(col("vibration").isNull().cast("int")).alias("vibration_null"),
        sum(col("rpm").isNull().cast("int")).alias("rpm_null"),
        sum(col("power_kw").isNull().cast("int")).alias("power_kw_null"),

        sum(col("cnc_oil").isNull().cast("int")).alias("cnc_oil_null"),
        sum(col("coolant_pressure").isNull().cast("int")).alias("coolant_pressure_null"),

        sum(col("joint_torque").isNull().cast("int")).alias("joint_torque_null"),
        sum(col("force").isNull().cast("int")).alias("force_null"),

        sum(col("belt_tension").isNull().cast("int")).alias("belt_tension_null"),
        sum(col("load_weight").isNull().cast("int")).alias("load_weight_null"),

        sum(col("pump_oil").isNull().cast("int")).alias("pump_oil_null"),
        sum(col("flow_rate").isNull().cast("int")).alias("flow_rate_null"),
        sum(col("inlet_pressure").isNull().cast("int")).alias("inlet_pressure_null")

    )


# ==========================================================
# Empty Strings
# ==========================================================

def check_empty_strings(df):

    return df.select(

        sum((trim(col("event_id")) == "").cast("int")).alias("event_id_empty"),
        sum((trim(col("machine_id")) == "").cast("int")).alias("machine_id_empty"),
        sum((trim(col("machine_type")) == "").cast("int")).alias("machine_type_empty"),
        sum((trim(col("floor")) == "").cast("int")).alias("floor_empty"),
        sum((trim(col("shift")) == "").cast("int")).alias("shift_empty"),
        sum((trim(col("status")) == "").cast("int")).alias("status_empty")

    )


# ==========================================================
# Duplicate Records
# ==========================================================

def check_duplicate_event_ids(df):

    return (
        df.groupBy("event_id")
          .count()
          .filter(col("count") > 1)
    )


def check_duplicate_machine_timestamp(df):

    return (
        df.groupBy("machine_id", "event_time")
          .count()
          .filter(col("count") > 1)
    )


# ==========================================================
# Category Validation
# ==========================================================

def check_invalid_categories(df):

    valid_status = ["running", "idle", "fault"]
    valid_shift = ["morning", "evening", "night"]
    valid_floor = ["A", "B", "C"]
    valid_machine_types = [
        "cnc_machine",
        "robot_arm",
        "conveyor_belt",
        "pump"
    ]

    return df.select(

        sum((~col("status").isin(valid_status)).cast("int"))
            .alias("invalid_status"),

        sum((~col("shift").isin(valid_shift)).cast("int"))
            .alias("invalid_shift"),

        sum((~col("floor").isin(valid_floor)).cast("int"))
            .alias("invalid_floor"),

        sum((~col("machine_type").isin(valid_machine_types)).cast("int"))
            .alias("invalid_machine_type")

    )


# ==========================================================
# Numeric Validation
# ==========================================================

def check_numeric_ranges(df):

    return df.select(

        sum((col("temperature") > 150).cast("int"))
            .alias("temperature_too_high"),

        sum((col("temperature") < -20).cast("int"))
            .alias("temperature_too_low"),

        sum((col("rpm") < 0).cast("int"))
            .alias("negative_rpm"),

        sum((col("power_kw") < 0).cast("int"))
            .alias("negative_power"),

        sum((col("vibration") < 0).cast("int"))
            .alias("negative_vibration")

    )


# ==========================================================
# Outlier Detection
# ==========================================================

def detect_outliers(df):

    return df.select(

        sum((col("temperature") > 120).cast("int"))
            .alias("high_temperature"),

        sum((col("rpm") > 5000).cast("int"))
            .alias("high_rpm"),

        sum((col("vibration") > 15).cast("int"))
            .alias("high_vibration")

    )


# ==========================================================
# Business Rules
# ==========================================================

def check_fault_consistency(df):

    return df.select(

        sum(
            (
                (col("is_fault") == False) &
                col("error_code").isNotNull()
            ).cast("int")
        ).alias("error_code_without_fault"),

        sum(
            (
                (col("is_fault") == True) &
                col("error_code").isNull()
            ).cast("int")
        ).alias("fault_without_error_code")

    )


def check_machine_specific_sensors(df):

    return df.select(

        sum(
            (
                (col("machine_type") == "cnc_machine") &
                col("cnc_oil").isNull()
            ).cast("int")
        ).alias("missing_cnc_oil"),

        sum(
            (
                (col("machine_type") == "cnc_machine") &
                col("coolant_pressure").isNull()
            ).cast("int")
        ).alias("missing_coolant_pressure"),

        sum(
            (
                (col("machine_type") == "robot_arm") &
                col("joint_torque").isNull()
            ).cast("int")
        ).alias("missing_joint_torque"),

        sum(
            (
                (col("machine_type") == "robot_arm") &
                col("force").isNull()
            ).cast("int")
        ).alias("missing_force"),

        sum(
            (
                (col("machine_type") == "conveyor_belt") &
                col("belt_tension").isNull()
            ).cast("int")
        ).alias("missing_belt_tension"),

        sum(
            (
                (col("machine_type") == "conveyor_belt") &
                col("load_weight").isNull()
            ).cast("int")
        ).alias("missing_load_weight"),

        sum(
            (
                (col("machine_type") == "pump") &
                col("pump_oil").isNull()
            ).cast("int")
        ).alias("missing_pump_oil"),

        sum(
            (
                (col("machine_type") == "pump") &
                col("flow_rate").isNull()
            ).cast("int")
        ).alias("missing_flow_rate"),

        sum(
            (
                (col("machine_type") == "pump") &
                col("inlet_pressure").isNull()
            ).cast("int")
        ).alias("missing_inlet_pressure")

    )


# ==========================================================
# Timestamp Validation
# ==========================================================

def check_invalid_timestamps(df):

    return df.select(

        sum(col("event_time").isNull().cast("int"))
            .alias("invalid_timestamp")

    )


