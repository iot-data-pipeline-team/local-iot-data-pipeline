from pyspark.sql.functions import *


# fields validation
def validate_required_fields(df):

    return (
        df
        .withColumn(
            "valid_event_id",
            col("event_id").isNotNull() &
            (trim(col("event_id")) != "")
        )
        .withColumn(
            "valid_machine_id",
            col("machine_id").isNotNull() &
            (trim(col("machine_id")) != "")
        )
        .withColumn(
            "valid_timestamp",
            col("event_time").isNotNull()
        )
        .withColumn(
            "valid_machine_type",
            col("machine_type").isNotNull() &
            (trim(col("machine_type")) != "")
        )
        .withColumn(
            "valid_shift",
            col("shift").isNotNull()
        )
        .withColumn(
            "valid_floor",
            col("floor").isNotNull()
        )
        .withColumn(
            "valid_status",
            col("status").isNotNull()
        )
    )

# category validation 
def validate_categories(df):

    valid_status = ["running", "idle", "fault"]

    valid_shift = [
        "morning",
        "evening",
        "night"
    ]

    valid_floor = [
        "A",
        "B",
        "C"
    ]

    valid_machine_types = [
        "cnc_machine",
        "robot_arm",
        "conveyor_belt",
        "pump"
    ]

    return (
        df
        .withColumn(
            "status_valid",
            col("status").isin(valid_status)
        )
        .withColumn(
            "shift_valid",
            col("shift").isin(valid_shift)
        )
        .withColumn(
            "floor_valid",
            col("floor").isin(valid_floor)
        )
        .withColumn(
            "machine_type_valid",
            col("machine_type").isin(valid_machine_types)
        )
    )


# numeric ranges validations
def validate_numeric_ranges(df):

    return (
        df
        .withColumn(
            "temperature_valid",
            col("temperature").between(-20,150)
        )
        .withColumn(
            "rpm_valid",
            col("rpm") >= 0
        )
        .withColumn(
            "power_valid",
            col("power_kw") >= 0
        )
        .withColumn(
            "vibration_valid",
            col("vibration") >= 0
        )
    )

# timestamp validation 
def validate_timestamp(df):

    return (
        df
        .withColumn(
            "future_timestamp",
            col("event_time") > current_timestamp()
        )
    )


# machines specific sensors validations 
def validate_machine_sensors(df):

    return (
        df

        .withColumn(
            "cnc_sensor_valid",
            when(
                col("machine_type") == "cnc_machine",
                col("cnc_oil").isNotNull() &
                col("coolant_pressure").isNotNull()
            ).otherwise(True)
        )

        .withColumn(
            "robot_sensor_valid",
            when(
                col("machine_type") == "robot_arm",
                col("joint_torque").isNotNull() &
                col("force").isNotNull()
            ).otherwise(True)
        )

        .withColumn(
            "conveyor_sensor_valid",
            when(
                col("machine_type") == "conveyor_belt",
                col("belt_tension").isNotNull() &
                col("load_weight").isNotNull()
            ).otherwise(True)
        )

        .withColumn(
            "pump_sensor_valid",
            when(
                col("machine_type") == "pump",
                col("pump_oil").isNotNull() &
                col("flow_rate").isNotNull() &
                col("inlet_pressure").isNotNull()
            ).otherwise(True)
        )
    )

# overall validation 
def validate_machine_data(df):

    df = validate_required_fields(df)

    df = validate_categories(df)

    df = validate_numeric_ranges(df)

    df = validate_timestamp(df)

   

    df = validate_machine_sensors(df)

    validation_columns = [

        "valid_event_id",
        "valid_machine_id",
        "valid_timestamp",
        "valid_machine_type",
        "valid_shift",
        "valid_floor",
        "valid_status",

        "status_valid",
        "shift_valid",
        "floor_valid",
        "machine_type_valid",

        "temperature_valid",
        "rpm_valid",
        "power_valid",
        "vibration_valid",

       

        "cnc_sensor_valid",
        "robot_sensor_valid",
        "conveyor_sensor_valid",
        "pump_sensor_valid"

    ]

    expression = " AND ".join(validation_columns)

    return (
        df
        .withColumn(
            "is_valid",
            expr(expression)
        )
        .withColumn(
            "validation_time",
            current_timestamp()
        )
    )