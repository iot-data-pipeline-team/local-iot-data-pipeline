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
# Machine Statistics 
Generates descriptive statistics, distributions, and machine-level analysis.

It includes:

- Basic sensor statistics
- Numeric summaries
- Machine type distribution
- Machine ID distribution
- Status distribution
- Shift distribution
- Floor distribution
- Fault distribution
- Error code distribution
- Fault rate by machine type
- Fault rate by shift
- Average sensor values by machine type

---

### `sensor_statistics(df)`

Calculates descriptive statistics for the main machine sensors:

- Temperature
- RPM
- Vibration
- Power consumption

For each sensor, it calculates:

- Minimum value
- Maximum value
- Average value
- Standard deviation

---

### `machine_distribution(df)`

Counts the number of records for each machine type and sorts the results by count in descending order.

### Example:
```text
##### MACHINE TYPE DISTRIBUTION

machine_type      count
--------------    -----
cnc_machine        300
robot_arm          280
pump               220
conveyor_belt      200
```
---

### `machine_id_distribution(df)`

Counts the number of records for each individual machine and sorts the results by count in descending order.

# Example:

# MACHINE ID DISTRIBUTION

machine_id    count
----------    -----
CNC_01         300
ROB_01         280
PMP_01         220
CNV_01         200

---

### `status_distribution(df)`

Counts the number of records for each machine status and sorts the results by count in descending order.

# Example:

# STATUS DISTRIBUTION

status      count
---------   -----
running      700
idle         200
fault        100

---

### `shift_distribution(df)`

Counts the number of records for each shift and sorts the results by count in descending order.

# Example:

# SHIFT DISTRIBUTION

shift       count
---------   -----
morning      400
evening      350
night        250

---

### `floor_distribution(df)`

Counts the number of records for each floor and sorts the results by count in descending order.

# Example:

# FLOOR DISTRIBUTION

floor       count
---------   -----
A            400
B            350
C            250

---

### `fault_distribution(df)`

Counts the number of fault and non-fault events and sorts the results by count in descending order.

# Example:

# FAULT DISTRIBUTION

is_fault    count
---------   -----
false        900
true         100

---

### `error_code_distribution(df)`

Counts the number of records for each error code and sorts the results by count in descending order.

# Example:

# ERROR CODE DISTRIBUTION

error_code    count
----------   -----
NULL           900
E001            40
E002            35
E003            25

---
### `machine_numeric_summary(df)`

Generates a Spark statistical summary for the main numeric sensor columns:

- Temperature
- Vibration
- RPM
- Power consumption

Uses Spark's `summary()` function to provide common descriptive statistics such as:

- Count
- Mean
- Standard deviation
- Minimum
- Maximum

### Example:

#### MACHINE NUMERIC SUMMARY
```text
summary    temperature    vibration    rpm       power_kw
-------    -----------    ---------    -------   --------
count      1000           1000         1000      1000
mean       68.75          2.45         1450.25   3.72
stddev     8.32           1.12         325.40    0.85
min        52.10          0.80         850.00    1.50
max        98.40          8.50         3200.00   6.80
```
---

### `fault_rate_by_machine(df)`

Calculates the fault rate for each machine type.

For each machine type, it calculates:

- Total number of events
- Total number of fault events
- Fault rate percentage

The fault rate is calculated as:

```text
Fault Rate % = (Fault Events / Total Events) × 100
```

The results are sorted by `fault_rate_percent` in descending order, so machine types with the highest fault rates appear first.

### Example

```text
FAULT RATE BY MACHINE

machine_type    total_events    fault_events    fault_rate_percent
------------    ------------    ------------    ------------------
robot_arm           280              35                12.50
cnc_machine         300              30                10.00
pump                220              15                 6.82
```
---
### `fault_rate_by_shift(df)`

Calculates the fault rate for each shift.

For each shift, it calculates:

- Total number of events
- Total number of fault events
- Fault rate percentage

The fault rate is calculated as:

```text
Fault Rate % = (Fault Events / Total Events) × 100
The results are ordered by `shift`.

### Example

```text
FAULT RATE BY SHIFT

shift       total_events    fault_events    fault_rate_percent
---------   ------------    ------------    ------------------
morning          400             25                 6.25
evening          350             35                10.00
night            250             30                12.00
```
---
### `average_sensor_values(df)`

Calculates the average sensor values for each machine type.

The function groups the data by `machine_type` and calculates the average of:

- Temperature
- Vibration
- RPM
- Power consumption

All average values are rounded to **2 decimal places**.

The results are ordered by `machine_type`.

### Example

```text
AVERAGE SENSOR VALUES BY MACHINE TYPE

machine_type    avg_temperature    avg_vibration    avg_rpm    avg_power
------------    ---------------    --------------   ---------  ---------
cnc_machine          74.25              2.51         1498.32      4.42
conveyor_belt        55.18              1.82          901.45      2.10
pump                 62.43              2.15         1205.76      3.25
robot_arm            65.72              2.24         1501.87      4.68


