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