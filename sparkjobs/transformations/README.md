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

---

# `machines_enhancement.py`

## Overview

`machines_enhancement.py` contains reusable PySpark functions for **enriching cleaned machine sensor data with derived features, business classifications, time attributes, and machine health indicators**.

The enhancement stage does not replace the original sensor data. Instead, it adds new columns that make the data more useful for:

- Machine monitoring
- Fault analysis
- Operational reporting
- Machine health assessment
- Risk analysis
- Power BI dashboards
- Downstream analytical models

The functions are grouped into five main areas:

- **Time Features** — derive date, hour, time bucket, and weekday/weekend information.
- **Sensor Status** — classify temperature, vibration, power, and RPM conditions.
- **Business Features** — create fault, running, fault-category, and machine-group indicators.
- **Machine Health & Risk** — calculate overall health and risk scores.
- **Main Enhancement Pipeline** — orchestrate all enhancement functions through `enhance_machine_data(df)`.

## Main Execution Flow

The `enhance_machine_data(df)` function is the main entry point for the machine enhancement process.

It executes all enhancement functions in a specific order and returns the final **Enhanced Machine Data**.

### Execution Architecture

```text
enhance_machine_data(df)
        │
        ├── Time Features
        │   ├── add_event_date()
        │   ├── add_event_hour()
        │   ├── add_time_bucket()
        │   └── add_weekend_status()
        │
        ├── Sensor Status
        │   ├── add_temperature_status()
        │   ├── add_vibration_status()
        │   ├── add_power_status()
        │   └── add_rpm_status()
        │
        ├── Business Features
        │   ├── add_fault_flag()
        │   ├── add_running_flag()
        │   ├── add_fault_category()
        │   └── add_machine_group()
        │
        └── Scores
            ├── add_health_score()
            └── add_risk_score()
        │
        ▼
Enhanced Machine Data
```

## Important Dependency

Some enhancement functions depend on columns created by earlier functions.

Therefore, the execution order inside `enhance_machine_data(df)` is important and should be maintained.

### Example 1: Time Feature Dependency

`add_time_bucket()` depends on the `event_hour` column created by `add_event_hour()`.

```text
add_event_hour()
      │
      ▼
event_hour
      │
      ▼
add_time_bucket()
      │
      ▼
time_bucket
```

Therefore, `add_event_hour()` must run before `add_time_bucket()`.

### Example 2: Health Score Dependency

`add_health_score()` depends on the status columns created by the sensor-status functions.

```text
add_temperature_status()
add_vibration_status()
add_power_status()
add_rpm_status()
        │
        ▼
   Status Columns
        │
        ▼
add_health_score()
        │
        ▼
health_score
```

The health score uses:

- `temperature_status`
- `vibration_status`
- `rpm_status`
- `power_status`
- `is_fault`

Therefore, the required status and fault columns must exist before `add_health_score()` is executed.

### Execution Order

The complete execution order is:

```text
1. add_event_date()
2. add_event_hour()
3. add_time_bucket()
4. add_weekend_status()

5. add_temperature_status()
6. add_vibration_status()
7. add_power_status()
8. add_rpm_status()

9. add_fault_flag()
10. add_running_flag()
11. add_fault_category()
12. add_machine_group()

13. add_health_score()
14. add_risk_score()
```

This ordering ensures that every function has access to the columns it needs.

---

### `add_temperature_status(df)`

Adds a `temperature_status` column that classifies the machine's temperature based on predefined thresholds.

The classification is:

- `Critical` → temperature is greater than `90`.
- `Warning` → temperature is greater than `80` and up to `90`.
- `Normal` → temperature is `80` or below.

The original `temperature` column is preserved, and `temperature_status` is added as a new column.

### Example Output

```text
+----------+-----------+------------------+
|machine_id|temperature|temperature_status|
+----------+-----------+------------------+
|CNC_01    |75.40      |Normal            |
|CNC_01    |85.20      |Warning           |
|ROB_01    |92.50      |Critical          |
|PMP_01    |78.30      |Normal            |
+----------+-----------+------------------+
```

For example:

- `CNC_01` has a temperature of `75.40`, so its status is `Normal`.
- `CNC_01` has a temperature of `85.20`, so its status is `Warning`.
- `ROB_01` has a temperature of `92.50`, so its status is `Critical`.

---

### `add_vibration_status(df)`

Adds a `vibration_status` column that classifies the machine's vibration level based on predefined thresholds.

The classification is:

- `Critical` → vibration is greater than `5`.
- `Warning` → vibration is greater than `3` and up to `5`.
- `Normal` → vibration is `3` or below.

The original `vibration` column is preserved, and `vibration_status` is added as a new column.

### Example Output

```text
+----------+---------+----------------+
|machine_id|vibration|vibration_status|
+----------+---------+----------------+
|CNC_01    |2.40     |Normal          |
|CNC_01    |4.20     |Warning         |
|ROB_01    |5.80     |Critical        |
|PMP_01    |2.90     |Normal          |
+----------+---------+----------------+
```

For example:

- `CNC_01` has a vibration of `2.40`, so its status is `Normal`.
- `CNC_01` has a vibration of `4.20`, so its status is `Warning`.
- `ROB_01` has a vibration of `5.80`, so its status is `Critical`.

---

### `add_vibration_status(df)`

Adds a `vibration_status` column that classifies the machine's vibration level based on predefined thresholds.

The classification is:

- `Critical` → vibration is greater than `5`.
- `Warning` → vibration is greater than `3` and up to `5`.
- `Normal` → vibration is `3` or below.

The original `vibration` column is preserved, and `vibration_status` is added as a new column.

### Example Output

```text
+----------+---------+----------------+
|machine_id|vibration|vibration_status|
+----------+---------+----------------+
|CNC_01    |2.40     |Normal          |
|CNC_01    |4.20     |Warning         |
|ROB_01    |5.80     |Critical        |
|PMP_01    |2.90     |Normal          |
+----------+---------+----------------+
```

For example:

- `CNC_01` has a vibration of `2.40`, so its status is `Normal`.
- `CNC_01` has a vibration of `4.20`, so its status is `Warning`.
- `ROB_01` has a vibration of `5.80`, so its status is `Critical`.

---

### `add_fault_flag(df)`

Adds a `fault_flag` column that converts the boolean `is_fault` value into a numeric flag.

- `1` → the machine event represents a fault.
- `0` → the machine event does not represent a fault.

The original `is_fault` column is preserved, and `fault_flag` is added as a new column.

### Example Output

```text
+----------+--------+----------+
|machine_id|is_fault|fault_flag|
+----------+--------+----------+
|CNC_01    |false   |0         |
|CNC_01    |true    |1         |
|ROB_01    |false   |0         |
|PMP_01    |true    |1         |
+----------+--------+----------+
```

For example:

- The first record has `is_fault = false`, so `fault_flag` is `0`.
- The second record has `is_fault = true`, so `fault_flag` is `1`.

---

### `add_event_date(df)`

Adds an `event_date` column by extracting the date portion from the `event_time` timestamp.

This makes it easier to perform **daily aggregations and time-based analysis** without the time component.

The original `event_time` column is preserved, and `event_date` is added as a new column.

### Example Output

```text
+----------+-------------------+----------+
|machine_id|event_time         |event_date|
+----------+-------------------+----------+
|CNC_01    |2026-08-09 08:15:32|2026-08-09|
|CNC_01    |2026-08-09 14:22:10|2026-08-09|
|ROB_01    |2026-08-10 09:05:45|2026-08-10|
|PMP_01    |2026-08-10 21:40:18|2026-08-10|
+----------+-------------------+----------+
```

For example:

- The first event occurs at `2026-08-09 08:15:32`, so `event_date` is `2026-08-09`.
- The second event occurs on the same day, so it also has `event_date = 2026-08-09`.
- The third and fourth events occur on `2026-08-10`, so their `event_date` is `2026-08-10`.

---

## `add_event_hour(df)` and `add_time_bucket(df)`

### `add_event_hour(df)`

Extracts the **hour** from `event_time` and creates a new `event_hour` column.

- `event_hour` contains values from `0` to `23`.
- The original `event_time` column is preserved.
- This feature is useful for analyzing machine activity by hour.

### Example Output

```text
+-------------------+----------+
|event_time         |event_hour|
+-------------------+----------+
|2026-08-09 05:30:00|5         |
|2026-08-09 08:15:00|8         |
|2026-08-09 14:20:00|14        |
|2026-08-09 22:45:00|22        |
+-------------------+----------+
```

---

### `add_time_bucket(df)`

Classifies each event into a **time-of-day bucket** based on `event_hour`.

The classification rules are:

- `06–13` → `Morning`
- `14–21` → `Evening`
- `22–05` → `Night`

The function adds a new `time_bucket` column.

### Example Output

```text
+----------+-----------+
|event_hour|time_bucket|
+----------+-----------+
|5         |Night      |
|8         |Morning    |
|13        |Morning    |
|14        |Evening    |
|20        |Evening    |
|22        |Night      |
+----------+-----------+
```


The two functions work together: add_event_hour() extracts the hour first, then add_time_bucket() uses that hour to classify the event into Morning, Evening, or Night.

---

### `add_health_score(df)`

Calculates an overall **machine health score from 0 to 100** based on the machine's sensor conditions and fault status.

The function starts with a perfect score of `100` and subtracts penalties when abnormal conditions are detected.

The calculated score is stored in a new column called `health_score`.

### Health Score Calculation

The function applies the following penalties:

| Condition | Penalty |
|---|---:|
| Critical temperature | -30 |
| Warning temperature | -15 |
| Critical vibration | -30 |
| Warning vibration | -15 |
| High RPM | -10 |
| Low RPM | -10 |
| High power | -10 |
| Machine fault (`is_fault = true`) | -40 |

The final score is calculated as:

```text
Health Score =
100
- Temperature Penalty
- Vibration Penalty
- RPM Penalty
- Power Penalty
- Fault Penalty
```

The `greatest()` function ensures that the health score **cannot be less than 0**.

### Required Columns

The function depends on the following columns created by earlier enhancement functions:

- `temperature_status`
- `vibration_status`
- `rpm_status`
- `power_status`
- `is_fault`

### Example

Suppose a machine has:

```text
temperature_status = Critical
vibration_status   = Warning
rpm_status         = Normal
power_status       = High
is_fault            = true
```

The calculation would be:

```text
100
- 30   → Critical temperature
- 15   → Warning vibration
- 0    → Normal RPM
- 10   → High power
- 40   → Machine fault
----------------
= 5
```

### Example Output

```text
+----------+------------------+----------------+----------+--------------+--------+------------+
|machine_id|temperature_status|vibration_status|rpm_status|power_status  |is_fault|health_score|
+----------+------------------+----------------+----------+--------------+--------+------------+
|CNC_01    |Normal            |Normal          |Normal    |Normal        |false   |100         |
|CNC_02    |Warning           |Normal          |Normal    |Normal        |false   |85          |
|ROB_01    |Critical          |Warning         |High      |High          |false   |40          |
|PMP_01    |Critical          |Warning         |Normal    |High          |true    |5           |
+----------+------------------+----------------+----------+--------------+--------+------------+
```

The original DataFrame columns are preserved, and `health_score` is added as a new derived column.

---

### `add_risk_score(df)`

Calculates a **risk score** for each machine event based on temperature and vibration levels.

The function adds a new column called `risk_score` using the following formula:

```text
risk_score =
(temperature × 0.4) +
(vibration × 10)
```

The result is rounded to **two decimal places**.

### Calculation Components

- `temperature × 0.4` — contributes 40% of the temperature value to the score.
- `vibration × 10` — gives vibration a higher impact because vibration can be an important indicator of machine instability.

### Example

For a machine with:

```text
temperature = 75
vibration   = 2.5
```

The calculation is:

```text
risk_score =
(75 × 0.4) + (2.5 × 10)

= 30 + 25

= 55.00
```

### Example Output

```text
+----------+-----------+---------+----------+
|machine_id|temperature|vibration|risk_score|
+----------+-----------+---------+----------+
|CNC_01    |75.00      |2.50     |55.00     |
|CNC_02    |85.00      |3.20     |66.00     |
|ROB_01    |70.00      |1.80     |46.00     |
|PMP_01    |95.00      |5.50     |93.00     |
+----------+-----------+---------+----------+
```

The original sensor columns are preserved, and `risk_score` is added as a new derived feature.

---
### `add_fault_category(df)`

Classifies machine faults into meaningful categories based on the `error_code`.

The function adds a new column called `fault_category`.

### Fault Classification Rules

| Error Code | Fault Category |
|---|---|
| `E001`, `E003` | `Overheat` |
| `E002`, `E004` | `Vibration` |
| `E005` | `RPM Drop` |
| Any other value | `None` |

The function uses PySpark's `when()` and `otherwise()` to assign the appropriate category.

### Example Output

```text
+----------+----------+--------------+
|machine_id|error_code|fault_category|
+----------+----------+--------------+
|CNC_01    |E001      |Overheat      |
|CNC_02    |E003      |Overheat      |
|ROB_01    |E002      |Vibration     |
|ROB_02    |E004      |Vibration     |
|PMP_01    |E005      |RPM Drop      |
|PMP_02    |NULL      |None          |
+----------+----------+--------------+
```

For example:

- `E001` and `E003` are classified as **Overheat** faults.
- `E002` and `E004` are classified as **Vibration** faults.
- `E005` is classified as an **RPM Drop** fault.
- Any other error code, including `NULL`, is classified as **None**.

The original `error_code` column is preserved, while `fault_category` is added as a new derived column.

---
### `add_power_status(df)`

Classifies machine power consumption into three operational status levels based on the `power_kw` value.

The function adds a new `power_status` column:

- `High` → when `power_kw` is greater than `5`.
- `Normal` → when `power_kw` is greater than `3` and less than or equal to `5`.
- `Low` → when `power_kw` is less than or equal to `3`.

### Example Output

```text
+--------+------------+
|power_kw|power_status|
+--------+------------+
|6.20    |High        |
|5.10    |High        |
|4.50    |Normal      |
|3.50    |Normal      |
|2.80    |Low         |
+--------+------------+
```
For example:

- A machine with `power_kw = 6.20` is classified as `High`.
- A machine with `power_kw = 4.50` is classified as `Normal`.
- A machine with `power_kw = 2.80` is classified as `Low`.

The original `power_kw` column is preserved, while `power_status` is added as a new derived column.

---
### `add_running_flag(df)`

Creates a numeric flag that indicates whether a machine is currently running based on the `status` column.

The function adds a new `running_flag` column:

- `1` → when `status` is `"running"`.
- `0` → for all other status values, such as `"idle"` or `"fault"`.

### Example Output

```text
+--------+------------+
|status  |running_flag|
+--------+------------+
|running |1           |
|idle    |0           |
|fault   |0           |
|running |1           |
+--------+------------+
```
For example:

- A machine with `status = "running"` receives `running_flag = 1`.
- A machine with `status = "idle"` receives `running_flag = 0`.
- A machine with `status = "fault"` receives `running_flag = 0`.

The original `status` column is preserved, while `running_flag` is added as a new derived column.

---

### `add_machine_group(df)`

Classifies each machine into a higher-level business group based on its `machine_type`.

The function adds a new `machine_group` column:

- `cnc_machine` → `Production`
- `robot_arm` → `Automation`
- `conveyor_belt` → `Material Handling`
- `pump` → `Utilities`
- Any other machine type → `Unknown`

### Example Output

```text
+--------------+-----------------+
|machine_type  |machine_group    |
+--------------+-----------------+
|cnc_machine   |Production       |
|robot_arm     |Automation       |
|conveyor_belt |Material Handling|
|pump          |Utilities        |
|unknown_type  |Unknown          |
+--------------+-----------------+
```
For example:

- A `cnc_machine` is classified as `Production`.
- A `robot_arm` is classified as `Automation`.
- A `conveyor_belt` is classified as `Material Handling`.
- A `pump` is classified as `Utilities`.
- Any unrecognized machine type is classified as `Unknown`.

The original `machine_type` column is preserved, while `machine_group` is added as a new derived business classification.

--- 

### `add_rpm_status(df)`

Classifies the RPM condition of each machine as `High`, `Low`, or `Normal` using machine-specific RPM thresholds.

Different machine types have different acceptable RPM ranges:

| Machine Type | Low Limit | High Limit |
|---|---:|---:|
| `cnc_machine` | 1300 | 1700 |
| `robot_arm` | 1300 | 1700 |
| `conveyor_belt` | 700 | 1100 |
| `pump` | 1500 | 1900 |

The function adds a new `rpm_status` column while preserving the original `machine_type` and `rpm` columns.

### Example Output

```text
+--------------+----+----------+
|machine_type  |rpm |rpm_status|
+--------------+----+----------+
|cnc_machine   |1200|Low       |
|cnc_machine   |1500|Normal    |
|cnc_machine   |1800|High      |
|robot_arm     |1250|Low       |
|robot_arm     |1500|Normal    |
|robot_arm     |1800|High      |
|conveyor_belt |600 |Low       |
|conveyor_belt |900 |Normal    |
|conveyor_belt |1200|High      |
|pump          |1400|Low       |
|pump          |1700|Normal    |
|pump          |2000|High      |
+--------------+----+----------+
```

For example:

- A `cnc_machine` with `rpm = 1200` is classified as `Low`.
- A `cnc_machine` with `rpm = 1500` is classified as `Normal`.
- A `cnc_machine` with `rpm = 1800` is classified as `High`.
- A `conveyor_belt` with `rpm = 600` is classified as `Low`.
- A `conveyor_belt` with `rpm = 900` is classified as `Normal`.
- A `conveyor_belt` with `rpm = 1200` is classified as `High`.
- A `pump` with `rpm = 1400` is classified as `Low`.
- A `pump` with `rpm = 1700` is classified as `Normal`.
- A `pump` with `rpm = 2000` is classified as `High`.

The boundary values themselves are considered `Normal` because the function uses `>` for the high condition and `<` for the low condition.

For example, for a `pump`:

```text
1500 RPM → Normal
1900 RPM → Normal
1499 RPM → Low
1901 RPM → High
```
---
### `add_weekend_status(df)`

Determines whether each machine event occurred on a **weekend or weekday** based on the `event_time` column.

The function adds a `weekend_status` column:

- `Weekend` → Sunday or Saturday.
- `Weekday` → Monday to Friday.

The original `event_time` column is preserved.

### Example Output

```text
+-------------------+--------------+
|event_time         |weekend_status|
+-------------------+--------------+
|2026-08-03 08:00:01|Weekday       |
|2026-08-07 14:30:10|Weekday       |
|2026-08-08 09:45:30|Weekend       |
|2026-08-09 16:20:45|Weekend       |
+-------------------+--------------+
```

For example:

- Monday → `Weekday`
- Friday → `Weekday`
- Saturday → `Weekend`
- Sunday → `Weekend`

---

### `enhance_machine_data(df)`

Main orchestration function for the machine data enhancement stage.

It applies all enhancement functions in a specific order and returns the **Enhanced Machine DataFrame**.

The transformations are organized into four groups:

- **Time Features** — adds event date, hour, time bucket, and weekend status.
- **Sensor Status** — classifies temperature, vibration, power, and RPM conditions.
- **Business Features** — adds fault, running, fault category, and machine group information.
- **Scores** — calculates the overall machine health and risk scores.

### Main Execution Flow

```text
enhance_machine_data(df)
        │
        ├── Time Features
        │   ├── add_event_date()
        │   ├── add_event_hour()
        │   ├── add_time_bucket()
        │   └── add_weekend_status()
        │
        ├── Sensor Status
        │   ├── add_temperature_status()
        │   ├── add_vibration_status()
        │   ├── add_power_status()
        │   └── add_rpm_status()
        │
        ├── Business Features
        │   ├── add_fault_flag()
        │   ├── add_running_flag()
        │   ├── add_fault_category()
        │   └── add_machine_group()
        │
        └── Scores
            ├── add_health_score()
            └── add_risk_score()
        │
        ▼
Enhanced Machine Data
```

### Important Dependency

The order of execution is important because some functions depend on columns created by previous functions.

For example:

```text
add_event_hour()
       │
       ▼
   event_hour
       │
       ▼
add_time_bucket()
       │
       ▼
  time_bucket
```

Similarly, `add_health_score()` depends on several status columns that must already exist:

```text
add_temperature_status()
add_vibration_status()
add_power_status()
add_rpm_status()
       │
       ▼
add_fault_flag()
       │
       ▼
add_health_score()
       │
       ▼
health_score
```

Therefore, the transformation order inside `enhance_machine_data(df)` should be maintained.