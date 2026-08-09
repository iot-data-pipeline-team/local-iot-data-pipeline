# Machine Data Validation

`machine_validation.py` contains PySpark validation functions used to validate machine sensor records before they continue to downstream processing.

The validation logic creates boolean validation columns for different groups of rules and finally combines all rules into a single `is_valid` column.

## Validation Flow

```text
validate_machine_data(df)
│
├── validate_required_fields(df)
│   ├── valid_event_id
│   ├── valid_machine_id
│   ├── valid_timestamp
│   ├── valid_machine_type
│   ├── valid_shift
│   ├── valid_floor
│   └── valid_status
│
├── validate_categories(df)
│   ├── status_valid
│   ├── shift_valid
│   ├── floor_valid
│   └── machine_type_valid
│
├── validate_numeric_ranges(df)
│   ├── temperature_valid
│   ├── rpm_valid
│   ├── power_valid
│   └── vibration_valid
│
├── validate_timestamp(df)
│   └── future_timestamp
│
├── validate_machine_sensors(df)
│   ├── cnc_sensor_valid
│   ├── robot_sensor_valid
│   ├── conveyor_sensor_valid
│   └── pump_sensor_valid
│
└── Overall Validation
    ├── Combine all validation rules
    ├── is_valid
    └── validation_time

```
---
### `validate_required_fields(df)`

Validates that all required machine event fields are present and not empty.

The function adds a validation column for each required field:

- `valid_event_id` — checks that `event_id` is not `NULL` and not an empty string.
- `valid_machine_id` — checks that `machine_id` is not `NULL` and not an empty string.
- `valid_timestamp` — checks that `event_time` is not `NULL`.
- `valid_machine_type` — checks that `machine_type` is not `NULL` and not an empty string.
- `valid_shift` — checks that `shift` is not `NULL`.
- `valid_floor` — checks that `floor` is not `NULL`.
- `valid_status` — checks that `status` is not `NULL`.

Each validation column contains:

- `true` → the field is valid.
- `false` → the field is missing or empty.

The function keeps the original columns and adds the validation columns to the DataFrame.

### Example Output

```text
+--------+----------+-------------------+------------+-------+-----+------+----------------+-----------------+----------------+------------------+------------+------------+-------------+
|event_id|machine_id|event_time         |machine_type|shift  |floor|status|valid_event_id  |valid_machine_id |valid_timestamp |valid_machine_type|valid_shift |valid_floor |valid_status |
+--------+----------+-------------------+------------+-------+-----+------+----------------+-----------------+----------------+------------------+------------+------------+-------------+
|EVT001  |CNC_01    |2026-08-01 08:00:01|cnc_machine |morning|A    |running|true            |true             |true            |true              |true        |true        |true         |
|EVT002  |          |2026-08-01 08:00:02|robot_arm   |morning|B    |running|true            |false            |true            |true              |true        |true        |true         |
|EVT003  |PMP_01    |NULL               |pump        |night  |C    |idle  |true            |true             |false           |true              |true        |true        |true         |
|EVT004  |CNV_01    |2026-08-01 08:00:04|NULL        |evening|C    |fault |true            |true             |true            |false             |true        |true        |true         |
+--------+----------+-------------------+------------+-------+-----+------+----------------+-----------------+----------------+------------------+------------+------------+-------------+
```

#### For example:

- The second record has an empty `machine_id`, so `valid_machine_id` is `false`.
- The third record has a missing `event_time`, so `valid_timestamp` is `false`.
- The fourth record has a missing `machine_type`, so `valid_machine_type` is `false`.

---

### `validate_categories(df)`

Validates categorical fields against predefined valid values.

The function adds four validation columns:

- `status_valid` — checks whether `status` is `running`, `idle`, or `fault`.
- `shift_valid` — checks whether `shift` is `morning`, `evening`, or `night`.
- `floor_valid` — checks whether `floor` is `A`, `B`, or `C`.
- `machine_type_valid` — checks whether `machine_type` is one of:
  - `cnc_machine`
  - `robot_arm`
  - `conveyor_belt`
  - `pump`

Each validation column contains:

- `true` → the category is valid.
- `false` → the category is not in the allowed list.

The original columns are preserved, and the validation results are added as new columns.

### Example Output

```text
+----------+-------+-----+------------+------------+-----------+----------+------------------+
|machine_id|status |shift|floor       |machine_type|status_valid|shift_valid|floor_valid|machine_type_valid|
+----------+-------+-----+------------+------------+------------+-----------+-----------+------------------+
|CNC_01    |running|morning|A          |cnc_machine |true        |true       |true       |true              |
|ROB_01    |fault  |night  |B          |robot_arm   |true        |true       |true       |true              |
|CNV_01    |working|morning|C          |conveyor_belt|false      |true       |true       |true              |
|PMP_01    |idle   |afternoon|C        |pump        |true        |false      |true       |true              |
|CNC_02    |running|night  |D          |cnc_machine |true        |true       |false      |true              |
|ROB_02    |idle   |evening|A          |robot       |true        |true       |true       |false             |
+----------+-------+--------+------------+------------+------------+-----------+-----------+------------------+
```
#### For example:

- The third record has `status = "working"`, which is not allowed, so `status_valid` is `false`.
- The fourth record has `shift = "afternoon"`, which is not allowed, so `shift_valid` is `false`.
- The fifth record has `floor = "D"`, which is not allowed, so `floor_valid` is `false`.
- The sixth record has `machine_type = "robot"`, which is not allowed, so `machine_type_valid` is `false`.

---

### `validate_numeric_ranges(df)`

Validates numeric sensor values against predefined acceptable ranges.

The function adds four validation columns:

- `temperature_valid` — checks that `temperature` is between `-20` and `150`.
- `rpm_valid` — checks that `rpm` is greater than or equal to `0`.
- `power_valid` — checks that `power_kw` is greater than or equal to `0`.
- `vibration_valid` — checks that `vibration` is greater than or equal to `0`.

Each validation column contains:

- `true` → the value is within the expected range.
- `false` → the value is outside the expected range.

The original sensor columns are preserved, and the validation results are added as new columns.

### Example Output

```text
+-----------+------+--------+---------+-----------------+---------+-----------+---------------+
|temperature|rpm   |power_kw|vibration|temperature_valid|rpm_valid|power_valid|vibration_valid|
+-----------+------+--------+---------+-----------------+---------+-----------+---------------+
|75.4       |1500  |4.5     |2.3      |true             |true     |true       |true           |
|165.2      |1500  |4.7     |2.1      |false            |true     |true       |true           |
|68.7       |-100  |4.2     |2.5      |true             |false    |true       |true           |
|70.1       |1450  |-2.0    |2.4      |true             |true     |false      |true           |
|72.5       |1500  |4.3     |-1.5     |true             |true     |true       |false          |
+-----------+------+--------+---------+-----------------+---------+-----------+---------------+
```
### For example:

- The second record has `temperature = 165.2`, which exceeds the maximum of `150`, so `temperature_valid` is `false`.
- The third record has a negative `rpm`, so `rpm_valid` is `false`.
- The fourth record has a negative `power_kw`, so `power_valid` is `false`.
- The fifth record has a negative `vibration`, so `vibration_valid` is `false`.

---

### `validate_timestamp(df)`

Validates event timestamps by checking whether an event occurs in the future.

The function adds one validation column:

- `future_timestamp` — compares `event_time` with the current Spark timestamp using `current_timestamp()`.

The result is:

- `true` → the event timestamp is in the future.
- `false` → the event timestamp is current or in the past.

### Example Output

```text
+-------------------+-----------------+
|event_time         |future_timestamp |
+-------------------+-----------------+
|2026-08-01 08:00:01|false            |
|2026-08-01 08:05:12|false            |
|2026-08-09 15:00:00|true             |
+-------------------+-----------------+
```
### For example:

- The first record has a timestamp in the past, so `future_timestamp` is `false`.
- The second record also has a valid past timestamp, so `future_timestamp` is `false`.
- The third record has a timestamp later than the current time, so `future_timestamp` is `true`.

---

### `validate_machine_sensors(df)`

Validates that each machine has all of its required machine-specific sensor values.

The function adds four validation columns:

- `cnc_sensor_valid` — for `cnc_machine`, checks that `cnc_oil` and `coolant_pressure` are not null.
- `robot_sensor_valid` — for `robot_arm`, checks that `joint_torque` and `force` are not null.
- `conveyor_sensor_valid` — for `conveyor_belt`, checks that `belt_tension` and `load_weight` are not null.
- `pump_sensor_valid` — for `pump`, checks that `pump_oil`, `flow_rate`, and `inlet_pressure` are not null.

For each validation:

- `true` → all required sensors for that machine type are present.
- `false` → at least one required sensor is missing.
- For other machine types, the validation is `true` because the check does not apply.

### Example Output

```text
+-------------+--------+-----------------+------------------+---------------------+----------------+
|machine_type |cnc_oil |coolant_pressure |cnc_sensor_valid  |robot_sensor_valid   |...             |
+-------------+--------+-----------------+------------------+---------------------+----------------+
|cnc_machine  |82.5    |3.2              |true              |true                 |...             |
|cnc_machine  |null    |3.1              |false             |true                 |...             |
|robot_arm    |null    |null             |true              |true                 |...             |
|robot_arm    |null    |null             |true              |false                |...             |
|pump         |78.4    |null             |true              |true                 |...             |
+-------------+--------+-----------------+------------------+---------------------+----------------+
```
### For example:

- The first `cnc_machine` has both `cnc_oil` and `coolant_pressure`, so `cnc_sensor_valid` is `true`.
- The second `cnc_machine` has a missing `cnc_oil`, so `cnc_sensor_valid` is `false`.
- The `robot_arm` records are checked using `joint_torque` and `force`.
- The `pump` records are checked using `pump_oil`, `flow_rate`, and `inlet_pressure`.

---

### `validate_machine_data(df)`

Performs the **complete validation process** for the machine sensor DataFrame.

The function applies all previously defined validation functions:

1. `validate_required_fields(df)` — validates required fields.
2. `validate_categories(df)` — validates categorical values.
3. `validate_numeric_ranges(df)` — validates numeric sensor ranges.
4. `validate_timestamp(df)` — checks for future timestamps.
5. `validate_machine_sensors(df)` — validates machine-specific sensors.

After applying these checks, the function collects all validation columns into a single expression using:

```python
expression = " AND ".join(validation_columns)
```
This means that a record is considered valid **only if all validation checks are `true`**.

The function then adds two final columns:

- `is_valid`
  - `true` → all validation checks passed.
  - `false` → at least one validation check failed.
- `validation_time`
  - Stores the timestamp when the validation was performed.

### Example Output

The function preserves all original DataFrame columns and adds the validation columns.

For readability, the following example shows only selected columns from the resulting DataFrame:

```text
+------------+-------+----------------+-----------------+-----------------+-----+
|machine_type|cnc_oil|coolant_pressure|cnc_sensor_valid |robot_sensor_valid| ... |
+------------+-------+----------------+-----------------+-----------------+-----+
|cnc_machine |82.5   |3.2             |true             |true             | ... |
|cnc_machine |null   |3.1             |false            |true             | ... |
|robot_arm   |null   |null            |true             |true             | ... |
|robot_arm   |null   |null            |true             |false            | ... |
|pump        |78.4   |null            |true             |true             | ... |
+------------+-------+----------------+-----------------+-----------------+-----+
```
### For example:

- The first record passes all validation checks, so `is_valid` is `true`.
- The second record has an invalid `temperature` value, so `temperature_valid` is `false` and `is_valid` is `false`.
- The third record has an invalid `rpm` value, so `rpm_valid` is `false` and `is_valid` is `false`.
- The fourth record passes all validation checks, so `is_valid` is `true`.

The original DataFrame columns are preserved, while the individual validation results, `is_valid`, and `validation_time` are added as new columns.