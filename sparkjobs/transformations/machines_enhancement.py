from pyspark.sql.functions import *

CNC_HIGH = 1700
CNC_LOW = 1300

ROBOT_HIGH = 1700
ROBOT_LOW = 1300

CONVEYOR_HIGH = 1100
CONVEYOR_LOW = 700

PUMP_HIGH = 1900
PUMP_LOW = 1500

def add_temperature_status(df):
    return df.withColumn(
        "temperature_status",
        when(col("temperature") > 90, lit("Critical"))
        .when(col("temperature") > 80, lit("Warning"))
        .otherwise(lit("Normal"))
    )


def add_vibration_status(df):
    return df.withColumn(
        "vibration_status",
        when(col("vibration") > 5, lit("Critical"))
        .when(col("vibration") > 3, lit("Warning"))
        .otherwise(lit("Normal"))
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
        when(col("event_hour").between(6,13), lit("Morning"))
        .when(col("event_hour").between(14,21), lit("Evening"))
        .otherwise(lit("Night"))
    )



def add_health_score(df):

    return (
        df.withColumn(

            # Overall machine health score (0 - 100)
            "health_score",

            greatest(

                lit(0),

                # Start with a perfect score
                lit(100)

                # -----------------------------
                # Temperature penalty
                # -----------------------------
                - when(
                    col("temperature_status") == "Critical",
                    30
                )
                .when(
                    col("temperature_status") == "Warning",
                    15
                )
                .otherwise(0)

                # -----------------------------
                # Vibration penalty
                # -----------------------------
                - when(
                    col("vibration_status") == "Critical",
                    30
                )
                .when(
                    col("vibration_status") == "Warning",
                    15
                )
                .otherwise(0)

                # -----------------------------
                # RPM penalty
                # -----------------------------
                - when(
                    col("rpm_status") == "High",
                    10
                )
                .when(
                    col("rpm_status") == "Low",
                    10
                )
                .otherwise(0)

                # -----------------------------
                # Power penalty
                # -----------------------------
                - when(
                    col("power_status") == "High",
                    10
                )
                .otherwise(0)

                # -----------------------------
                # Machine fault penalty
                # -----------------------------
                - when(
                    col("is_fault"),
                    40
                )
                .otherwise(0)

            )
        )
    )


def add_risk_score(df):

    return df.withColumn(
        "risk_score",
        round(
            col("temperature") * 0.4 +
            col("vibration") * 10,
            2
        )
    )

def add_fault_category(df):
    return df.withColumn(
        "fault_category",
        when(col("error_code").isin("E001", "E003"), lit("Overheat"))
        .when(col("error_code").isin("E002", "E004"), lit("Vibration"))
        .when(col("error_code") == "E005", lit("RPM Drop"))
        .otherwise(lit("None"))
    )
def add_power_status(df):
    return df.withColumn(
        "power_status",
        when(col("power_kw") > 5, lit("High"))
        .when(col("power_kw") > 3, lit("Normal"))
        .otherwise(lit("Low"))
    )
def add_running_flag(df):
    return df.withColumn(
        "running_flag",
        when(col("status") == "running", 1)
        .otherwise(0)
    )


def add_machine_group(df):

    return (
        df.withColumn(
            "machine_group",
            when(
                col("machine_type") == "cnc_machine",
                lit("Production")
            )
            .when(
                col("machine_type") == "robot_arm",
                lit("Automation")
            )
            .when(
                col("machine_type") == "conveyor_belt",
                lit("Material Handling")
            )
            .when(
                col("machine_type") == "pump",
                lit("Utilities")
            )
            .otherwise(lit("Unknown"))
        )
    )


def add_rpm_status(df):

    return (
        df.withColumn(
            "rpm_status",
            when(
                (col("machine_type") == "cnc_machine") &
                (col("rpm") > CNC_HIGH),
                lit("High")
            )
            .when(
                (col("machine_type") == "cnc_machine") &
                (col("rpm") < CNC_LOW),
                lit("Low")
            )

            .when(
                (col("machine_type") == "robot_arm") &
                (col("rpm") > ROBOT_HIGH),
                lit("High")
            )
            .when(
                (col("machine_type") == "robot_arm") &
                (col("rpm") < ROBOT_LOW),
                lit("Low")
            )

            .when(
                (col("machine_type") == "conveyor_belt") &
                (col("rpm") > CONVEYOR_HIGH),
                lit("High")
            )
            .when(
                (col("machine_type") == "conveyor_belt") &
                (col("rpm") < CONVEYOR_LOW),
                lit("Low")
            )

            .when(
                (col("machine_type") == "pump") &
                (col("rpm") > PUMP_HIGH),
                lit("High")
            )
            .when(
                (col("machine_type") == "pump") &
                (col("rpm") < PUMP_LOW),
                lit("Low")
            )

            .otherwise(lit("Normal"))
        )
    )

def add_weekend_status(df):

    return (
        df.withColumn(
            "weekend_status",
            when(
                dayofweek(col("event_time")).isin(1, 7),
                lit("Weekend")
            )
            .otherwise(lit("Weekday"))
        )
    )


def enhance_machine_data(df):

    # Time
    df = add_event_date(df)
    df = add_event_hour(df)
    df = add_time_bucket(df)
    df = add_weekend_status(df)

    # Status
    df = add_temperature_status(df)
    df = add_vibration_status(df)
    df = add_power_status(df)
    df = add_rpm_status(df)

    # Business
    df = add_fault_flag(df)
    df = add_running_flag(df)
    df = add_fault_category(df)
    df = add_machine_group(df)

    # Scores
    df = add_health_score(df)
    df = add_risk_score(df)

    return df