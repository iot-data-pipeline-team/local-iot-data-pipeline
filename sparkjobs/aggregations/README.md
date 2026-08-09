# `machine_aggregation.py` — Overview

`machine_aggregation.py` contains reusable PySpark functions for **aggregating enhanced machine sensor data into different analytical levels**.

The aggregation stage converts detailed event-level data into summarized datasets that can be used for:

- Machine performance monitoring
- Hourly trend analysis
- Shift and floor analysis
- Machine type comparison
- Fault analysis
- Machine health and risk reporting
- Power BI dashboards and reports

The functions use PySpark aggregation operations such as `count()`, `avg()`, `max()`, `sum()`, `first()`, and `countDistinct()`.

### Aggregation Functions

- `machine_summary(df)` — one record per machine.
- `hourly_summary(df)` — one record per machine per hour.
- `shift_summary(df)` — one record per date, time bucket, and floor.
- `machine_type_summary(df)` — one record per machine type and machine group.
- `floor_summary(df)` — one record per floor.
- `fault_summary(df)` — one record per fault category containing only fault events.

## Aggregation Architecture

```text
Enhanced Machine Data
        │
        ├── machine_summary()
        │       └── One record per machine
        │
        ├── hourly_summary()
        │       └── One record per machine per hour
        │
        ├── shift_summary()
        │       └── One record per date + time bucket + floor
        │
        ├── machine_type_summary()
        │       └── One record per machine type + group
        │
        ├── floor_summary()
        │       └── One record per floor
        │
        └── fault_summary()
                └── One record per fault category
        │
        ▼
Aggregated Machine Data
```

## Aggregation Levels

| Function | Grouping Level | Main Purpose |
|---|---|---|
| `machine_summary()` | Machine | Overall machine performance |
| `hourly_summary()` | Date + Hour + Machine | Hourly machine monitoring |
| `shift_summary()` | Date + Time Bucket + Floor | Shift/floor performance |
| `machine_type_summary()` | Machine Type + Group | Compare machine types |
| `floor_summary()` | Floor | Compare operational floors |
| `fault_summary()` | Fault Category | Analyze machine faults |

Each function produces a **new aggregated DataFrame** and does not modify the original event-level DataFrame.

---

### `machine_summary(df)`

Creates a **machine-level summary** by grouping all machine events by `machine_id`.

The function produces **one record per machine** and calculates overall performance, sensor, health, risk, fault, and uptime metrics.

It includes:

- `machine_type` and `machine_group` — identify the machine and its business category.
- `total_events` — total number of events recorded for the machine.
- `avg_temp` and `max_temp` — average and maximum temperature.
- `avg_vibration` and `max_vibration` — average and maximum vibration.
- `avg_rpm` — average RPM.
- `avg_power` and `peak_power` — average and maximum power consumption.
- `avg_health_score` — average machine health score.
- `avg_risk_score` — average machine risk score.
- `fault_count` — total number of fault events.
- `fault_percentage` — percentage of events that were faults.
- `uptime_percentage` — percentage of events where the machine was running.

The original event-level DataFrame is not modified. Instead, the function returns a new **machine-level aggregated DataFrame**.

### How It Works

```text
Machine Sensor Events
        │
        ▼
groupBy("machine_id")
        │
        ├── Machine Information
        │   ├── machine_type
        │   └── machine_group
        │
        ├── Event Metrics
        │   └── total_events
        │
        ├── Sensor Metrics
        │   ├── avg_temp
        │   ├── max_temp
        │   ├── avg_vibration
        │   ├── max_vibration
        │   ├── avg_rpm
        │   ├── avg_power
        │   └── peak_power
        │
        ├── Health & Risk
        │   ├── avg_health_score
        │   └── avg_risk_score
        │
        └── Operational Metrics
            ├── fault_count
            ├── fault_percentage
            └── uptime_percentage
        │
        ▼
One Record Per Machine 

### Example Output

The following example shows the type of output produced by Spark's `show()`:

```text
+----------+-------------+-----------------+------------+--------+--------+-------------+--------------+-------+---------+----------+----------------+---------------+-----------+----------------+-----------------+
|machine_id|machine_type |machine_group    |total_events|avg_temp|max_temp|avg_vibration|max_vibration|avg_rpm|avg_power|peak_power|avg_health_score|avg_risk_score |fault_count|fault_percentage|uptime_percentage|
+----------+-------------+-----------------+------------+--------+--------+-------------+--------------+-------+---------+----------+----------------+---------------+-----------+----------------+-----------------+
|CNC_01    |cnc_machine  |Production       |100         |76.42   |94.80   |2.84         |5.90          |1512.35|4.62     |6.80      |82.45           |35.72          |8          |8.00            |91.00            |
|ROB_01    |robot_arm    |Automation       |100         |68.31   |87.20   |2.41         |4.80          |1498.62|4.51     |6.20      |86.17           |29.85          |5          |5.00            |94.00            |
|CNV_01    |conveyor_belt|Material Handling|100         |57.84   |81.50   |2.12         |4.30          |902.45 |2.38     |4.10      |90.32           |25.64          |3          |3.00            |97.00            |
|PMP_01    |pump         |Utilities       |100         |63.75   |89.40   |2.67         |5.20          |1698.21|3.42     |5.70      |84.91           |31.25          |6          |6.00            |95.00            |
+----------+-------------+-----------------+------------+--------+--------+-------------+--------------+-------+---------+----------+----------------+---------------+-----------+----------------+-----------------+
```

### Example Interpretation

For `CNC_01`:

- `total_events = 100` means the machine generated 100 sensor events.
- `avg_temp = 76.42` means its average recorded temperature was 76.42.
- `max_temp = 94.80` is the highest recorded temperature.
- `fault_count = 8` means 8 of the 100 events were fault events.
- `fault_percentage = 8.00` means 8% of the machine's events were faults.
- `uptime_percentage = 91.00` means the machine was running during 91% of its recorded events.
- `avg_health_score = 82.45` represents its average machine health.
- `avg_risk_score = 35.72` represents its average calculated risk.

Therefore, instead of analyzing hundreds or thousands of individual sensor events, this function provides **one concise performance record for each machine**.


--- 

### `hourly_summary(df)`

Creates an **hourly summary of machine activity**, producing one record for each combination of `event_date`, `event_hour`, and `machine_id`.

The function calculates:

- `avg_temp` — average machine temperature during the hour.
- `avg_vibration` — average vibration during the hour.
- `avg_rpm` — average RPM during the hour.
- `avg_power` — average power consumption during the hour.
- `avg_health_score` — average machine health score during the hour.
- `avg_risk_score` — average machine risk score during the hour.
- `fault_count` — total number of fault events during the hour.
- `running_events` — total number of events where the machine was running.

The sensor averages and health/risk scores are rounded to **two decimal places**.

### Example Output

```text
+----------+----------+----------+--------+-------------+-------+---------+----------------+--------------+-----------+--------------+
|event_date|event_hour|machine_id|avg_temp|avg_vibration|avg_rpm|avg_power|avg_health_score|avg_risk_score|fault_count|running_events|
+----------+----------+----------+--------+-------------+-------+---------+----------------+--------------+-----------+--------------+
|2026-08-09|8         |CNC_01    |76.45   |2.34         |1502.50|4.52     |91.50           |53.60         |1          |45            |
|2026-08-09|8         |ROB_01    |68.21   |2.18         |1500.20|4.71     |95.00           |48.20         |0          |52            |
|2026-08-09|9         |CNC_01    |78.12   |2.51         |1498.30|4.68     |88.75           |56.42         |2          |48            |
|2026-08-09|9         |PMP_01    |64.30   |1.92         |1701.40|3.85     |90.25           |45.16         |1          |50            |
+----------+----------+----------+--------+-------------+-------+---------+----------------+--------------+-----------+--------------+
```
Each row represents **one machine during one specific hour**. This makes the output useful for monitoring machine behavior over time and identifying hourly changes in **performance, faults, health, and risk**.

--- 

### `shift_summary(df)`

Creates a **shift-level summary** of machine activity, producing one record for each combination of `event_date`, `time_bucket`, and `floor`.

The function calculates:

- `total_events` — total number of machine events during the shift.
- `avg_temp` — average machine temperature.
- `avg_vibration` — average vibration level.
- `avg_rpm` — average RPM.
- `avg_power` — average power consumption.
- `avg_health_score` — average machine health score.
- `avg_risk_score` — average machine risk score.
- `fault_count` — total number of fault events during the shift.

All average values are rounded to **two decimal places**.

### Example Output

```text
+----------+-----------+-----+------------+--------+-------------+-------+---------+----------------+--------------+-----------+
|event_date|time_bucket|floor|total_events|avg_temp|avg_vibration|avg_rpm|avg_power|avg_health_score|avg_risk_score|fault_count|
+----------+-----------+-----+------------+--------+-------------+-------+---------+----------------+--------------+-----------+
|2026-08-09|Morning    |A    |150         |76.42   |2.35         |1498.50|4.52     |91.25           |52.84         |5          |
|2026-08-09|Morning    |B    |135         |68.75   |2.18         |1501.20|4.68     |94.10           |48.32         |3          |
|2026-08-09|Evening    |A    |160         |79.35   |2.67         |1510.40|4.75     |88.60           |55.21         |8          |
|2026-08-09|Evening    |C    |142         |65.82   |1.95         |1702.30|3.91     |92.45           |46.73         |4          |
+----------+-----------+-----+------------+--------+-------------+-------+---------+----------------+--------------+-----------+
```
Each row represents one floor during one specific time bucket on one specific date. This makes the output useful for comparing operational performance, machine health, risk, and fault activity across different floors and shifts.

---

### `machine_type_summary(df)`

Aggregates machine sensor data by `machine_type` and `machine_group`.

The function calculates:

- `total_events` — total number of sensor events for each machine type.
- `avg_temp` — average machine temperature.
- `avg_vibration` — average machine vibration.
- `avg_power` — average power consumption in kW.
- `avg_health_score` — average machine health score.
- `avg_risk_score` — average machine risk score.
- `fault_count` — total number of fault events.

Each row represents **one machine type and its corresponding machine group**. This makes the output useful for comparing sensor performance, machine health, risk levels, and fault activity across different machine types.

### Example Output

```text
+--------------+-----------------+------------+--------+-------------+---------+----------------+--------------+-----------+
|machine_type  |machine_group    |total_events|avg_temp|avg_vibration|avg_power|avg_health_score|avg_risk_score|fault_count|
+--------------+-----------------+------------+--------+-------------+---------+----------------+--------------+-----------+
|cnc_machine   |Production       |400         |75.42   |2.51         |4.42     |91.35           |32.18         |18         |
|robot_arm     |Automation       |300         |65.18   |2.21         |4.71     |94.62           |28.45         |12         |
|conveyor_belt |Material Handling|200         |55.73   |1.82         |2.14     |97.18           |24.63         |6          |
|pump          |Utilities        |100         |62.35   |2.05         |3.28     |95.41           |26.72         |4          |
+--------------+-----------------+------------+--------+-------------+---------+----------------+--------------+-----------+
```
The output provides a high-level performance summary for each machine type, allowing analysts to identify which machine types have higher fault counts, power consumption, risk levels, or lower health scores.

---

### `floor_summary(df)`

Aggregates machine sensor data by `floor` to provide a high-level view of operational activity and machine performance across different floors.

The function calculates:

- `total_events` — total number of sensor events recorded on each floor.
- `machines` — number of distinct machines operating on each floor.
- `avg_temp` — average machine temperature.
- `avg_power` — average power consumption in kW.
- `avg_health_score` — average machine health score.
- `fault_count` — total number of fault events recorded on the floor.

Each row represents **one floor**. This makes the output useful for comparing machine activity, power consumption, health, and fault levels across different floors.

### Example Output

```text
+-----+------------+--------+--------+---------+----------------+-----------+
|floor|total_events|machines|avg_temp|avg_power|avg_health_score|fault_count|
+-----+------------+--------+--------+---------+----------------+-----------+
|A    |420         |2       |74.85   |4.52     |91.42           |20         |
|B    |310         |1       |65.37   |4.68     |94.15           |12         |
|C    |270         |2       |58.42   |2.61     |96.73           |8          |
+-----+------------+--------+--------+---------+----------------+-----------+
```

The output provides a floor-level operational summary, making it easier to identify floors with higher machine activity, power consumption, fault counts, or lower average health scores.

---

### `fault_summary(df)`

Creates a summary of **machine fault events grouped by fault category**.

The function first filters the DataFrame to include only records where `fault_flag = 1`.

It then groups the fault events by `fault_category` and calculates:

- `fault_count` — total number of fault events in each category.
- `avg_temp` — average temperature during the fault events.
- `avg_vibration` — average vibration during the fault events.
- `avg_risk_score` — average risk score during the fault events.

The average values are rounded to **two decimal places**.

### Example Output

```text
+--------------+-----------+--------+-------------+--------------+
|fault_category|fault_count|avg_temp|avg_vibration|avg_risk_score|
+--------------+-----------+--------+-------------+--------------+
|Overheat      |35         |94.52   |4.21         |76.35         |
|Vibration     |28         |82.16   |5.73         |71.42         |
|RPM Drop      |17         |78.45   |3.18         |62.87         |
+--------------+-----------+--------+-------------+--------------+
``` 
Each row represents one fault category and summarizes how frequently that fault occurred and the average machine conditions associated with it.
