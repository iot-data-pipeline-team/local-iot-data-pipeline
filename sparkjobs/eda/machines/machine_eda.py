from sparkjobs.eda.machines.machine_schema_exploration import *
from sparkjobs.eda.machines.machine_data_quality import *
from sparkjobs.eda.machines.machine_statistics import *


# ==========================================================
# Helper Functions
# ==========================================================

def print_header(title):
    print("\n" + "=" * 80)
    print(f" {title}")
    print("=" * 80)


def print_subheader(title):
    print("\n" + "-" * 80)
    print(f" {title}")
    print("-" * 80)


# ==========================================================
# Schema Exploration
# ==========================================================

def schema_exploration(df):

    print_header("SCHEMA EXPLORATION")

    print_subheader("Dataset Summary")
    print(f"Total Records : {df.count():,}")
    print(f"Total Columns : {len(df.columns)}")

    explore_summary(df)

    print_subheader("Schema")
    explore_schema(df)

    print_subheader("Columns")
    explore_columns(df)

    print_subheader("Data Types")
    explore_data_types(df)

    print_subheader("Nullable Columns")
    explore_nullable_columns(df)

    print_subheader("Column Categories")
    explore_column_categories(df)

    print_subheader("Sample Records")
    df.show(5, truncate=False)


# ==========================================================
# Data Quality
# ==========================================================

def data_quality(df):

    print_header("DATA QUALITY")

    print_subheader("Null Value Analysis")
    check_nulls(df).show(truncate=False)

    print_subheader("Empty String Analysis")
    check_empty_strings(df).show(truncate=False)

    print_subheader("Category Validation")
    check_invalid_categories(df).show(truncate=False)

    print_subheader("Numeric Range Validation")
    check_numeric_ranges(df).show(truncate=False)

    print_subheader("Business Rule Validation")
    check_fault_consistency(df).show(truncate=False)

    print_subheader("Machine Specific Sensor Validation")
    check_machine_specific_sensors(df).show(truncate=False)

    print_subheader("Timestamp Validation")
    check_invalid_timestamps(df).show(truncate=False)

   

    print_subheader("Duplicate Event IDs")
    check_duplicate_event_ids(df).show(truncate=False)

    print_subheader("Duplicate Machine + Timestamp")
    check_duplicate_machine_timestamp(df).show(truncate=False)

    print_subheader("Potential Outliers")
    detect_outliers(df).show(truncate=False)


# ==========================================================
# Exploratory Statistics
# ==========================================================

def exploratory_statistics(df):

    print_header("EXPLORATORY STATISTICS")

    print_subheader("Sensor Statistics")
    sensor_statistics(df).show(truncate=False)

    print_subheader("Machine Type Distribution")
    machine_distribution(df).show(truncate=False)

    print_subheader("Status Distribution")
    status_distribution(df).show(truncate=False)

    print_subheader("Shift Distribution")
    shift_distribution(df).show(truncate=False)

    print_subheader("Fault Distribution")
    fault_distribution(df).show(truncate=False)

    print_subheader("Error Code Distribution")
    error_code_distribution(df).show(truncate=False)


# ==========================================================
# Main EDA Pipeline
# ==========================================================

def run_machine_eda(df):
    """
    Complete exploratory data analysis for machine sensor data.
    """

    print("\n")
    print("=" * 80)
    print("           MACHINE SENSOR EXPLORATORY DATA ANALYSIS")
    print("=" * 80)

    # ------------------------------------------------------
    # 1. Schema Exploration
    # ------------------------------------------------------
    schema_exploration(df)

    # ------------------------------------------------------
    # 2. Data Quality Assessment
    # ------------------------------------------------------
    data_quality(df)

    # ------------------------------------------------------
    # 3. Exploratory Statistics
    # ------------------------------------------------------
    exploratory_statistics(df)

    # ------------------------------------------------------
    # Finished
    # ------------------------------------------------------
    print("\n")
    print("=" * 80)
    print("               MACHINE EDA COMPLETED SUCCESSFULLY")
    print("=" * 80)
    print(f"Records Analyzed : {df.count():,}")
    print(f"Columns          : {len(df.columns)}")
    print("=" * 80)