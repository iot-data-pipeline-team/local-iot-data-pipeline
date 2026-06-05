from pyspark.sql.functions import *
from pyspark.sql.types import *

iot_schema = StructType([

    StructField("event_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("machine_id", StringType()),
    StructField("machine_type", StringType()),
    StructField("floor", StringType()),
    StructField("shift", StringType()),
    StructField("status", StringType()),
    StructField("error_code", StringType()),
    StructField("is_fault", BooleanType()),

    StructField(
        "metrics",
        StructType([
            StructField("temperature", DoubleType()),
            StructField("vibration", DoubleType()),
            StructField("rpm", DoubleType()),
            StructField("power_kw", DoubleType())
        ])
    ),

     StructField(
        "cnc_sensors",
        StructType([
            StructField("oil_level_pct", DoubleType()),
            StructField("coolant_pressure_bar", DoubleType())
        ])
    ),

    StructField(
        "robot_sensors",
        StructType([
            StructField("joint_torque_nm", DoubleType()),
            StructField("end_effector_force_n", DoubleType())
        ])
    ),

    StructField(
        "conveyor_sensors",
        StructType([
            StructField("belt_tension_n", DoubleType()),
            StructField("load_weight_kg", DoubleType())
        ])
    ),

    StructField(
        "pump_sensors",
        StructType([
            StructField("oil_level_pct", DoubleType()),
            StructField("flow_rate_lpm", DoubleType()),
            StructField("inlet_pressure_bar", DoubleType())
        ])
    )
])
