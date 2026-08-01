from pyspark.sql.functions import *

def trim_strings(df):

    string_columns = [
        "event_id",
        "machine_id",
        "machine_type",
        "floor",
        "shift",
        "status",
        "error_code"
    ]

    for c in string_columns:
        df = df.withColumn(c, trim(col(c)))

    return df

def normalize_case(df):

    return (
        df
        .withColumn("machine_type", lower(col("machine_type")))
        .withColumn("status", lower(col("status")))
        .withColumn("shift", lower(col("shift")))
        .withColumn("floor", upper(col("floor")))
        .withColumn("error_code", upper(col("error_code")))
    )

def clean_empty_strings(df):

    string_columns = [
        "event_id",
        "machine_id",
        "machine_type",
        "floor",
        "shift",
        "status",
        "error_code"
    ]

    for c in string_columns:

        df = df.withColumn(
            c,
            when(trim(col(c)) == "", None)
            .otherwise(col(c))
        )

    return df





def round_numeric_values(df):

    numeric_columns = [

        "temperature",
        "vibration",
        "rpm",
        "power_kw",

        "cnc_oil",
        "coolant_pressure",

        "joint_torque",
        "force",

        "belt_tension",
        "load_weight",

        "pump_oil",
        "flow_rate",
        "inlet_pressure"
    ]

    for c in numeric_columns:

        df = df.withColumn(
            c,
            round(col(c), 2)
        )

    return df

def remove_duplicate_events(df):

    return df.dropDuplicates(["event_id"])


def clean_machine_data(df):

    df = trim_strings(df)

    df = normalize_case(df)

    df = clean_empty_strings(df)

    df = round_numeric_values(df)

    df = remove_duplicate_events(df)

    return df

