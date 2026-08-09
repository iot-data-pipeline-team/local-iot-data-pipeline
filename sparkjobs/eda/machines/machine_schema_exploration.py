from pyspark.sql.functions import *

# ==========================================
# Schema Exploration
# ==========================================

def explore_schema(df):
    """
    Print the complete DataFrame schema.
    """
    print("\n" + "=" * 70)
    print("SCHEMA")
    print("=" * 70)
    df.printSchema()


def explore_columns(df):
    """
    Print all column names.
    """
    print("\n" + "=" * 70)
    print(f"COLUMNS ({len(df.columns)})")
    print("=" * 70)

    for i, column in enumerate(df.columns, start=1):
        print(f"{i:2}. {column}")





def explore_summary(df):
    """
    Print a quick summary of the DataFrame.
    """
    print("\n" + "=" * 70)
    print("DATAFRAME SUMMARY")
    print("=" * 70)

    print(f"Number of Columns : {len(df.columns)}")
    print(f"Column Names      : {', '.join(df.columns)}")


def explore_column_categories(df):
    """
    Classify columns into numeric, categorical, boolean, and timestamp.
    """
    numeric = []
    categorical = []
    boolean = []
    timestamp = []

    for field in df.schema.fields:
        dtype = field.dataType.simpleString()

        if dtype in ("double", "float", "int", "bigint", "long", "short"):
            numeric.append(field.name)
        elif dtype == "boolean":
            boolean.append(field.name)
        elif dtype == "timestamp":
            timestamp.append(field.name)
        else:
            categorical.append(field.name)

    print("\n" + "=" * 70)
    print("COLUMN CATEGORIES")
    print("=" * 70)

    print(f"Numeric ({len(numeric)}): {numeric}")
    print(f"Categorical ({len(categorical)}): {categorical}")
    print(f"Boolean ({len(boolean)}): {boolean}")
    print(f"Timestamp ({len(timestamp)}): {timestamp}")
