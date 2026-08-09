# Machine Data Transformations
### Overview

The transformation layer prepares the validated machine sensor data for downstream processing and analysis.

It is divided into two main stages:

1. **Data Cleaning** — standardizes and cleans the existing machine sensor data.
2. **Data Enhancement** — derives new business, operational, time-based, and machine-health attributes from the cleaned data.

The transformation functions are designed as reusable PySpark functions. Each function receives a DataFrame and returns a transformed DataFrame, allowing the functions to be chained together into a complete transformation pipeline.

---

## Transformation Architecture

```text
Machine Sensor Data
        │
        ▼
clean_machine_data(df)
│
├── trim_strings(df)
│   └── Clean whitespace from string columns
│
├── normalize_case(df)
│   ├── machine_type
│   ├── status
│   ├── shift
│   ├── floor
│   └── error_code
│
├── clean_empty_strings(df)
│   └── Convert empty strings to NULL
│
├── round_numeric_values(df)
│   ├── temperature
│   ├── vibration
│   ├── rpm
│   ├── power_kw
│   ├── cnc_oil
│   ├── coolant_pressure
│   ├── joint_torque
│   ├── force
│   ├── belt_tension
│   ├── load_weight
│   ├── pump_oil
│   ├── flow_rate
│   └── inlet_pressure
│
└── remove_duplicate_events(df)
    └── Remove duplicate records based on event_id
        │
        ▼
   Clean Machine Data
        │
        ▼
enhance_machine_data(df)
│
├── Time Features
│   ├── add_event_date(df)
│   │   └── event_date
│   │
│   ├── add_event_hour(df)
│   │   └── event_hour
│   │
│   ├── add_time_bucket(df)
│   │   └── time_bucket
│   │
│   └── add_weekend_status(df)
│       └── weekend_status
│
├── Sensor Status
│   ├── add_temperature_status(df)
│   │   └── temperature_status
│   │
│   ├── add_vibration_status(df)
│   │   └── vibration_status
│   │
│   ├── add_power_status(df)
│   │   └── power_status
│   │
│   └── add_rpm_status(df)
│       └── rpm_status
│
├── Business Features
│   ├── add_fault_flag(df)
│   │   └── fault_flag
│   │
│   ├── add_running_flag(df)
│   │   └── running_flag
│   │
│   ├── add_fault_category(df)
│   │   └── fault_category
│   │
│   └── add_machine_group(df)
│       └── machine_group
│
└── Machine Health & Risk
    ├── add_health_score(df)
    │   └── health_score
    │
    └── add_risk_score(df)
        └── risk_score
        │
        ▼
 Enhanced Machine Data
```
---

# `machines_cleaning.py`

## Overview

`machines_cleaning.py` contains reusable PySpark functions for **cleaning and standardizing machine sensor data** before applying business transformations or creating analytical features.

The cleaning process focuses on:

- Removing unnecessary whitespace from string fields.
- Standardizing the format and case of categorical values.
- Converting empty strings into `NULL`.
- Rounding numeric sensor measurements to two decimal places.
- Removing duplicate machine events based on `event_id`.

The functions are designed to be reusable and are combined by the main `clean_machine_data(df)` function.

### Execution Flow
```text

clean_machine_data(df)
    │
    ├── trim_strings(df)
    │
    ├── normalize_case(df)
    │
    ├── clean_empty_strings(df)
    │
    ├── round_numeric_values(df)
    │
    └── remove_duplicate_events(df)
                │
                ▼
        Clean Machine Data

This means clean_machine_data(df) is the main entry point for the machine data cleaning stage, while the individual functions perform specific cleaning tasks.
```
---

### `trim_strings(df)`
Removes leading and trailing whitespace from important string columns such as `event_id`, `machine_id`, `machine_type`, `floor`, `shift`, `status`, and `error_code`.

The function uses PySpark's `trim()` to clean each column while preserving the original columns.

This prevents inconsistencies caused by values such as `" CNC_01 "` instead of `"CNC_01"`.

### Example Input

```text
+----------+----------+-------------+-----+-------+-------+----------+
|event_id  |machine_id|machine_type |floor|shift  |status |error_code|
+----------+----------+-------------+-----+-------+-------+----------+
| EVT001   | CNC_01   | cnc_machine | A   | morning|running| E001     |
|EVT002    | ROB_01   | robot_arm   | B   | evening| idle | E002     |
+----------+----------+-------------+-----+-------+-------+----------+
```

### Example Output

```text
+--------+----------+------------+-----+-------+-------+----------+
|event_id|machine_id|machine_type|floor|shift  |status |error_code|
+--------+----------+------------+-----+-------+-------+----------+
|EVT001  |CNC_01    |cnc_machine |A    |morning|running|E001      |
|EVT002  |ROB_01    |robot_arm   |B    |evening|idle   |E002      |
+--------+----------+------------+-----+-------+-------+----------+
```

For example:

- The first record has spaces around `event_id`, `machine_id`, `machine_type`, and other string values. These spaces are removed.
- The second record also has unnecessary spaces removed.
- Values such as `CNC_01` and `cnc_machine` remain unchanged apart from the removed whitespace.
- Numeric and timestamp columns are not affected by this function.

---

### `normalize_case(df)`

Standardizes the letter case of categorical string columns to keep values consistent across the dataset.

The function:

- Converts `machine_type`, `status`, and `shift` to **lowercase**.
- Converts `floor` and `error_code` to **uppercase**.

This prevents inconsistent values such as `"Running"` and `"running"` from being treated as different categories.

### Example

```text
Before:
machine_type = "CNC_MACHINE"
status        = "Running"
shift         = "Morning"
floor         = "a"
error_code    = "e001"

After:
machine_type = "cnc_machine"
status        = "running"
shift         = "morning"
floor         = "A"
error_code    = "E001"
```
---
### `clean_empty_strings(df)`

Converts empty or whitespace-only string values into `NULL` for important string columns.

The function checks columns such as `event_id`, `machine_id`, `machine_type`, `floor`, `shift`, `status`, and `error_code`.

This helps standardize missing values so that empty strings are treated consistently as `NULL`.

### Example

```text
Before:
+--------+----------+------------+-----+-------+
|event_id|machine_id|machine_type|floor|status |
+--------+----------+------------+-----+-------+
| EVT001 | CNC_01   | cnc_machine| A   |running|
|        | ROB_01   | robot_arm  | B   |idle   |
| EVT003 |          | pump       | C   |       |
+--------+----------+------------+-----+-------+

After:
+--------+----------+------------+-----+-------+
|event_id|machine_id|machine_type|floor|status |
+--------+----------+------------+-----+-------+
|EVT001  |CNC_01    |cnc_machine |A    |running|
|NULL    |ROB_01    |robot_arm   |B    |idle   |
|EVT003  |NULL      |pump        |C    |NULL   |
+--------+----------+------------+-----+-------+
```

Empty strings and strings containing only whitespace are converted to `NULL`, while valid values remain unchanged.

---

### `round_numeric_values(df)`

Rounds machine sensor and measurement columns to **two decimal places** using PySpark's `round()` function.

The function applies rounding to numeric fields such as `temperature`, `vibration`, `rpm`, `power_kw`, and machine-specific sensor values.

This standardizes numeric precision and makes sensor values easier to read and analyze.

### Example

```text
Before:
+-----------+---------+------+--------+--------+
|temperature|vibration|rpm   |power_kw|cnc_oil |
+-----------+---------+------+--------+--------+
|75.4387    |2.34567  |1500.789|4.45678|82.3456 |
|68.1234    |1.87654  |1499.456|3.98765|80.1234 |
+-----------+---------+------+--------+--------+

After:
+-----------+---------+------+--------+-------+
|temperature|vibration|rpm   |power_kw|cnc_oil|
+-----------+---------+------+--------+-------+
|75.44      |2.35     |1500.79|4.46    |82.35  |
|68.12      |1.88     |1499.46|3.99    |80.12  |
+-----------+---------+------+--------+-------+
```

The original numeric columns are preserved, but their values are rounded to two decimal places.

---

### `clean_machine_data(df)`

Main cleaning function that orchestrates all machine data cleaning steps in a defined sequence.

The function applies:

1. `trim_strings(df)` — removes unnecessary whitespace from string columns.
2. `normalize_case(df)` — standardizes the case of categorical values.
3. `clean_empty_strings(df)` — converts empty strings to `NULL`.
4. `round_numeric_values(df)` — rounds numeric sensor values to two decimal places.
5. `remove_duplicate_events(df)` — removes duplicate records based on `event_id`.

The function returns the final **clean machine DataFrame**, ready for the enhancement and transformation stage.


### Example

```text
Before Cleaning
+----------+----------+-------------+-----+---------+-------+--------+
|event_id  |machine_id|machine_type |floor|status   |rpm    |power_kw|
+----------+----------+-------------+-----+---------+-------+--------+
| EVT001   | CNC_01   | CNC_MACHINE | a   | Running |1500.789|4.5678 |
| EVT002   | ROB_01   | Robot_Arm   | b   | IDLE    |1499.456|3.9876 |
| EVT001   | CNC_01   | CNC_MACHINE | a   | Running |1500.789|4.5678 |
+----------+----------+-------------+-----+---------+-------+--------+

After Cleaning
+--------+----------+------------+-----+-------+-------+--------+
|event_id|machine_id|machine_type|floor|status |rpm    |power_kw|
+--------+----------+------------+-----+-------+-------+--------+
|EVT001  |CNC_01    |cnc_machine |A    |running|1500.79|4.57    |
|EVT002  |ROB_01    |robot_arm   |B    |idle   |1499.46|3.99    |
+--------+----------+------------+-----+-------+-------+--------+
```

The final output contains standardized values, cleaned strings, rounded numeric measurements, and no duplicate `event_id` records.
