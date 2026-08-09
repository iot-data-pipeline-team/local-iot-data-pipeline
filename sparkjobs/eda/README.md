# Exploratory Data Analysis (EDA)

## Overview

The `eda` folder contains PySpark modules for exploring and assessing machine sensor data before downstream processing.

```text
eda/
└── machines/
    ├── machine_eda.py                     # Orchestrates the complete machine EDA workflow
    ├── machine_schema_exploration.py      # Explores dataset schema, columns, data types, and structure
    ├── machine_data_quality.py             # Validates data quality, consistency, ranges, duplicates, and business rules
    └── machine_statistics.py               # Calculates descriptive statistics, distributions, and fault metrics

The EDA workflow is:
Schema Exploration → Data Quality → Exploratory Statistics
```
---

## Machine Schema Exploration

`machine_schema_exploration.py` provides reusable PySpark functions for inspecting the structure and schema of the machine sensor DataFrame before data quality and statistical analysis.

## Functions

### `explore_schema(df)`

Prints the complete DataFrame schema using:

```python
df.printSchema()
``` 
This shows:

- Column names
- Data types
- Nullable status
- Nested structure, if present

---

### `explore_columns(df)`

Prints all column names with their corresponding position in the DataFrame.

#### Example:

```text
COLUMNS (10)

 1. event_id
 2. event_time
 3. machine_id
 4. machine_type
 ...
```

This provides a quick overview of the available fields.

---

### `explore_summary(df)`

Provides a quick overview of the DataFrame by displaying:

- Total number of columns
- Column names

#### Example:
```text
======================================================================
DATAFRAME SUMMARY
======================================================================
Number of Columns : 21
Column Names      : event_id, event_time, machine_id, machine_type, floor, shift, status, ...
```

---

### `explore_column_categories(df)`

Classifies the DataFrame columns into four categories based on their Spark data types:

- Numeric
- Categorical
- Boolean
- Timestamp

#### Example:

```text
COLUMN CATEGORIES

Numeric (4): ['temperature', 'vibration', 'rpm', 'power_kw']
Categorical (6): ['event_id', 'machine_id', 'machine_type', 'floor', 'shift', 'status']
Boolean (1): ['is_fault']
Timestamp (1): ['event_time']
```

--- 
# Machine Data Quality

`machine_data_quality.py` contains reusable PySpark functions for identifying data quality issues in the machine sensor DataFrame.

The module validates missing values, empty strings, duplicates, categorical values, numeric ranges, outliers, business rules, machine-specific sensors, and timestamps.

## Functions

### `check_nulls(df)`

Checks for NULL values across event fields, machine attributes, common sensors, and machine-specific sensors.


---

### `check_empty_strings(df)`

Checks important string columns for empty or whitespace-only values using `trim()`.

---

### `check_duplicate_event_ids(df)`

Identifies duplicate records based on `event_id` by grouping event IDs and finding those that occur more than once.

---

### `check_duplicate_machine_timestamp(df)`

Identifies potential duplicate sensor readings by checking repeated combinations of `machine_id` and `event_time`.


### `check_invalid_categories(df)`

Validates categorical columns against the expected machine domain values.

Validated fields:

- `status`
- `shift`
- `floor`
- `machine_type`

---

### `check_numeric_ranges(df)`

Checks sensor values for logically invalid numeric ranges.

Validated conditions include:

- Temperature above `150`
- Temperature below `-20`
- Negative RPM
- Negative power consumption
- Negative vibration


---

### `detect_outliers(df)`

Identifies potential extreme sensor values that may require further investigation.

Current thresholds include:

- Temperature above `120`
- RPM above `5000`
- Vibration above `15`


---

### `check_fault_consistency(df)`

Validates the relationship between `is_fault` and `error_code`.

It identifies:

- Error codes without a fault
- Faults without an error code

---

### `check_machine_specific_sensors(df)`

Validates that machine-specific sensor values are available for the corresponding machine type.

Expected sensors:

```text
CNC Machine
    ├── cnc_oil
    └── coolant_pressure

Robot Arm
    ├── joint_torque
    └── force

Conveyor Belt
    ├── belt_tension
    └── load_weight

Pump
    ├── pump_oil
    ├── flow_rate
    └── inlet_pressure
```
---
