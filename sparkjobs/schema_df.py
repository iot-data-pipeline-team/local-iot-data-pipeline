
from pyspark.sql.types import *
#####################
# machine schema
####################
machine_schema = StructType([

    StructField("event_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("machine_id", StringType(), True),
    StructField("machine_type", StringType(), True),
    StructField("floor", StringType(), True),
    StructField("shift", StringType()),
    StructField("status", StringType(), True),
    StructField("error_code", StringType(), True),
    StructField("is_fault", BooleanType(), True),

    StructField(
        "metrics",
        StructType([
            StructField("temperature", DoubleType(), True),
            StructField("vibration", DoubleType(), True),
            StructField("rpm", DoubleType(), True),
            StructField("power_kw", DoubleType(), True)
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
            StructField("joint_torque_nm", DoubleType(), True),
            StructField("end_effector_force_n", DoubleType(), True)
        ])
    ),

    StructField(
        "conveyor_sensors",
        StructType([
            StructField("belt_tension_n", DoubleType(), True),
            StructField("load_weight_kg", DoubleType())
        ])
    ),

    StructField(
        "pump_sensors",
        StructType([
            StructField("oil_level_pct", DoubleType(), True),
            StructField("flow_rate_lpm", DoubleType(),True),
            StructField("inlet_pressure_bar", DoubleType(), True)
        ])
    )
])

################################
# worker schema
################################
worker_schema = StructType([

    StructField("worker_id", StringType(), True),

    StructField("timestamp", StringType(), True),

    StructField("floor", StringType(), True),

    StructField("zone_id", StringType(), True),

    StructField("helmet_on", BooleanType(), True),

    StructField("safety_vest_on", BooleanType(), True),

    StructField("heart_rate", IntegerType(), True),

    StructField("movement_status", StringType(), True),

    StructField("danger_zone", BooleanType(), True),

    StructField("fatigue_score", IntegerType(), True)

])