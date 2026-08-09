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
```
---

# Machine EDA
### Machine EDA Architecture

The `machine_eda.py` module orchestrates the complete exploratory data analysis workflow for machine sensor data.

```text
run_machine_eda(df)
│
├── 1. schema_exploration(df)
│   │
│   ├── Dataset Summary
│   │   ├── df.count()
│   │   └── len(df.columns)
│   │
│   ├── explore_summary()
│   ├── explore_schema()
│   ├── explore_columns()
│   ├── explore_column_categories()
│   └── df.show(5)
│
├── 2. data_quality(df)
│   │
│   ├── Null Value Analysis
│   │   └── check_nulls()
│   │
│   ├── Empty String Analysis
│   │   └── check_empty_strings()
│   │
│   ├── Category Validation
│   │   └── check_invalid_categories()
│   │
│   ├── Numeric Range Validation
│   │   └── check_numeric_ranges()
│   │
│   ├── Business Rule Validation
│   │   └── check_fault_consistency()
│   │
│   ├── Machine-Specific Sensor Validation
│   │   └── check_machine_specific_sensors()
│   │
│   ├── Timestamp Validation
│   │   └── check_invalid_timestamps()
│   │
│   ├── Duplicate Event IDs
│   │   └── check_duplicate_event_ids()
│   │
│   ├── Duplicate Machine + Timestamp
│   │   └── check_duplicate_machine_timestamp()
│   │
│   └── Potential Outliers
│       └── detect_outliers()
│
└── 3. exploratory_statistics(df)
    │
    ├── Basic Sensor Statistics
    │   ├── sensor_statistics()
    │   └── machine_numeric_summary()
    │
    ├── Frequency Distributions
    │   ├── machine_distribution()
    │   ├── machine_id_distribution()
    │   ├── status_distribution()
    │   ├── shift_distribution()
    │   ├── floor_distribution()
    │   ├── fault_distribution()
    │   └── error_code_distribution()
    │
    ├── Fault Analysis
    │   ├── fault_rate_by_machine()
    │   └── fault_rate_by_shift()
    │
    └── Sensor Analysis by Machine Type
        └── average_sensor_values()
```
---

# Module Dependencies

```text
machine_eda.py
│
├── machine_schema_exploration.py
│   ├── explore_summary()
│   ├── explore_schema()
│   ├── explore_columns()
│   └── explore_column_categories()
│
├── machine_data_quality.py
│   ├── check_nulls()
│   ├── check_empty_strings()
│   ├── check_invalid_categories()
│   ├── check_numeric_ranges()
│   ├── check_fault_consistency()
│   ├── check_machine_specific_sensors()
│   ├── check_invalid_timestamps()
│   ├── check_duplicate_event_ids()
│   ├── check_duplicate_machine_timestamp()
│   └── detect_outliers()
│
└── machine_statistics.py
    ├── sensor_statistics()
    ├── machine_numeric_summary()
    ├── machine_distribution()
    ├── machine_id_distribution()
    ├── status_distribution()
    ├── shift_distribution()
    ├── floor_distribution()
    ├── fault_distribution()
    ├── error_code_distribution()
    ├── fault_rate_by_machine()
    ├── fault_rate_by_shift()
    └── average_sensor_values()
```
--- 

# Execution Flow

```text
Input Machine DataFrame
        │
        ▼
Schema Exploration
        │
        ▼
Data Quality Assessment
        │
        ▼
Exploratory Statistics
        │
        ▼
EDA Results
        │
        ▼
EDA Completed
```
---

### `schema_exploration(df)`

Runs the schema exploration functions and displays information about the dataset structure.

### Example Output

```text
SCHEMA EXPLORATION

Dataset Summary
Total Records : 1,000
Total Columns : 21

Schema
root
 |-- event_id: string (nullable = true)
 |-- event_time: timestamp (nullable = true)
 |-- machine_id: string (nullable = true)
 |-- machine_type: string (nullable = true)
 |-- floor: string (nullable = true)
 |-- shift: string (nullable = true)
 |-- status: string (nullable = true)
 |-- error_code: string (nullable = true)
 |-- temperature: double (nullable = true)
 |-- vibration: double (nullable = true)
 |-- rpm: double (nullable = true)
 |-- power_kw: double (nullable = true)
 |-- is_fault: boolean (nullable = true)
 |-- cnc_oil: double (nullable = true)
 |-- coolant_pressure: double (nullable = true)
 |-- joint_torque: double (nullable = true)
 |-- force: double (nullable = true)
 |-- belt_tension: double (nullable = true)
 |-- load_weight: double (nullable = true)
 |-- pump_oil: double (nullable = true)
 |-- flow_rate: double (nullable = true)

Columns
 1. event_id
 2. event_time
 3. machine_id
 4. machine_type
 5. floor
 6. shift
 7. status
 8. error_code
 9. temperature
10. vibration
11. rpm
12. power_kw
13. is_fault
14. cnc_oil
15. coolant_pressure
16. joint_torque
17. force
18. belt_tension
19. load_weight
20. pump_oil
21. flow_rate

Column Categories
Numeric (13): [temperature, vibration, rpm, power_kw, cnc_oil,
               coolant_pressure, joint_torque, force, belt_tension,
               load_weight, pump_oil, flow_rate]

Categorical (7): [event_id, machine_id, machine_type, floor,
                  shift, status, error_code]

Boolean (1): [is_fault]

Timestamp (1): [event_time]

Sample Records
+--------+-------------------+----------+-------------+-----+-------+--------+
|event_id|event_time         |machine_id|machine_type |floor|shift  |status  |
+--------+-------------------+----------+-------------+-----+-------+--------+
|EVT001  |2026-08-01 08:00:01|CNC_01    |cnc_machine  |A    |morning|running |
|EVT002  |2026-08-01 08:00:02|ROB_01    |robot_arm    |B    |morning|running |
|EVT003  |2026-08-01 08:00:03|CNV_01    |conveyor_belt|C    |morning|idle    |
|EVT004  |2026-08-01 08:00:04|PMP_01    |pump         |C    |morning|running |
|EVT005  |2026-08-01 08:00:05|CNC_01    |cnc_machine  |A    |morning|fault   |
+--------+-------------------+----------+-------------+-----+-------+--------+
only showing top 5 rows
```
---

### `data_quality(df)`

Runs the complete machine data quality assessment by checking:

- Missing values
- Empty strings
- Invalid categorical values
- Invalid numeric ranges
- Fault and error-code consistency
- Machine-specific sensor values
- Invalid timestamps
- Duplicate event IDs
- Duplicate machine and timestamp combinations
- Potential outliers

### Example Output

```text
DATA QUALITY

Null Value Analysis
+----------+-------------+----------------+---------------+------------------+
|total_rows|event_id_null|event_time_null |machine_id_null|machine_type_null |
+----------+-------------+----------------+---------------+------------------+
|1000      |0            |0               |0              |0                 |
+----------+-------------+----------------+---------------+------------------+

Empty String Analysis
+----------------+----------------+--------------------+------------+-----------+------------+
|event_id_empty  |machine_id_empty|machine_type_empty  |floor_empty |shift_empty |status_empty|
+----------------+----------------+--------------------+------------+-----------+------------+
|0               |0               |0                   |0           |0          |0           |
+----------------+----------------+--------------------+------------+-----------+------------+

Category Validation
+--------------+-------------+-------------+--------------------+
|invalid_status|invalid_shift|invalid_floor|invalid_machine_type|
+--------------+-------------+-------------+--------------------+
|0             |0            |0            |0                   |
+--------------+-------------+-------------+--------------------+

Numeric Range Validation
+--------------------+-------------------+-------------+--------------+------------------+
|temperature_too_high|temperature_too_low|negative_rpm |negative_power|negative_vibration|
+--------------------+-------------------+-------------+--------------+------------------+
|0                   |0                  |0            |0             |0                 |
+--------------------+-------------------+-------------+--------------+------------------+

Business Rule Validation
+-------------------------+----------------------+
|error_code_without_fault|fault_without_error_code|
+-------------------------+----------------------+
|0                        |0                     |
+-------------------------+----------------------+

Machine Specific Sensor Validation
+-----------------+-----------------------+--------------------+-------------+-------------------------+
|missing_cnc_oil  |missing_coolant_pressure|missing_joint_torque|missing_force|missing_belt_tension     |
+-----------------+-----------------------+--------------------+-------------+-------------------------+
|0                |0                      |0                   |0            |0                        |
+-----------------+-----------------------+--------------------+-------------+-------------------------+

Timestamp Validation
+-----------------+
|invalid_timestamp|
+-----------------+
|0                |
+-----------------+

Duplicate Event IDs
+---------+-----+
|event_id |count|
+---------+-----+
+---------+-----+

Duplicate Machine + Timestamp
+----------+-------------------+-----+
|machine_id|event_time         |count|
+----------+-------------------+-----+
+----------+-------------------+-----+

Potential Outliers
+---------------+---------+--------------+
|high_temperature|high_rpm|high_vibration|
+---------------+---------+--------------+
|12             |3        |5             |
+---------------+---------+--------------+
```
---

### `exploratory_statistics(df)`

Runs the complete exploratory statistical analysis for machine sensor data.

The function analyzes:

- Basic sensor statistics
- Numeric summaries
- Machine type distribution
- Machine ID distribution
- Status distribution
- Shift distribution
- Floor distribution
- Fault distribution
- Error code distribution

### Example Output

```text
EXPLORATORY STATISTICS

Sensor Statistics
+---------------+---------------+----------------+----------------+
|temperature_min|temperature_max|temperature_avg |temperature_std |
+---------------+---------------+----------------+----------------+
|48.32          |128.74         |72.56           |14.82           |
+---------------+---------------+----------------+----------------+

Machine Numeric Summary
+-------+------------------+------------------+------------------+------------------+
|summary|temperature       |vibration         |rpm               |power_kw          |
+-------+------------------+------------------+------------------+------------------+
|count  |1000              |1000              |1000              |1000              |
|mean   |72.56             |3.42              |1254.73            |3.84              |
|stddev |14.82             |1.87              |342.51             |1.21              |
|min    |48.32             |0.52              |850.00             |1.42              |
|max    |128.74            |16.42             |4820.00            |8.91              |
+-------+------------------+------------------+------------------+------------------+

Machine Type Distribution
+-------------+-----+
|machine_type |count|
+-------------+-----+
|cnc_machine  |280  |
|robot_arm    |260  |
|conveyor_belt|240  |
|pump         |220  |
+-------------+-----+

Machine ID Distribution
+----------+-----+
|machine_id|count|
+----------+-----+
|CNC_01    |280  |
|ROB_01    |260  |
|CNV_01    |240  |
|PMP_01    |220  |
+----------+-----+

Status Distribution
+-------+-----+
|status |count|
+-------+-----+
|running|720  |
|idle   |180  |
|fault  |100  |
+-------+-----+

Shift Distribution
+-------+-----+
|shift  |count|
+-------+-----+
|morning|400  |
|evening|350  |
|night  |250  |
+-------+-----+

Floor Distribution
+-----+-----+
|floor|count|
+-----+-----+
|A    |300  |
|B    |280  |
|C    |420  |
+-----+-----+

Fault Distribution
+--------+-----+
|is_fault|count|
+--------+-----+
|false   |900  |
|true    |100  |
+--------+-----+

Error Code Distribution
+----------+-----+
|error_code|count|
+----------+-----+
|null      |900  |
|E001      |35   |
|E002      |25   |
|E003      |20   |
|E004      |20   |
+----------+-----+
```
---
### `run_machine_eda(df)`

Executes the complete Machine EDA workflow in three stages:

1. **Schema Exploration**
2. **Data Quality Assessment**
3. **Exploratory Statistics**

After completing all stages, it displays a summary of the analyzed dataset.

### Example Output

```text
================================================================================
           MACHINE SENSOR EXPLORATORY DATA ANALYSIS
================================================================================

--------------------------------------------------------------------------------
 SCHEMA EXPLORATION
--------------------------------------------------------------------------------

Dataset Summary
Total Records : 1,000
Total Columns : 21

Schema
root
 |-- event_id: string (nullable = true)
 |-- event_time: timestamp (nullable = true)
 |-- machine_id: string (nullable = true)
 |-- machine_type: string (nullable = true)
 ...

--------------------------------------------------------------------------------
 DATA QUALITY
--------------------------------------------------------------------------------

Null Value Analysis
+----------+-------------+----------------+
|total_rows|event_id_null|event_time_null |
+----------+-------------+----------------+
|1000      |0            |0               |
+----------+-------------+----------------+

Category Validation
...

Numeric Range Validation
...

Duplicate Event IDs
...

Potential Outliers
...

--------------------------------------------------------------------------------
 EXPLORATORY STATISTICS
--------------------------------------------------------------------------------

Sensor Statistics
...

Machine Type Distribution
...

Status Distribution
...

Fault Rate by Machine Type
...

Fault Rate by Shift
...

Average Sensor Values by Machine Type
...

================================================================================
               MACHINE EDA COMPLETED SUCCESSFULLY
================================================================================
Records Analyzed : 1,000
Columns          : 21
================================================================================



