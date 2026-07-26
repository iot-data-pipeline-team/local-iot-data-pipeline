POSTGRES_URL = (
    "jdbc:postgresql://localhost:5433/data_hub"
)

POSTGRES_PROPERTIES = {
    "user": "admin",
    "password": "password123",
    "driver": "org.postgresql.Driver"
}

# =========================
# load row data to bronze table

# def write_to_postgres_bronze(batch_df, batch_id):

#     print(f"Writing BRONZE batch {batch_id}")

#     (
#         batch_df.write
#         .mode("append")
#         .jdbc(
#             url=POSTGRES_URL,
#             table="iot_events",
#             properties=POSTGRES_PROPERTIES
#         )
#     )


# # =========================
# # load enhanced data to silver table
# def write_to_postgres_silver(batch_df, batch_id):

#     print(f"Writing SILVER batch {batch_id}")

#     (
#         batch_df.write
#         .mode("append")
#         .jdbc(
#             url=POSTGRES_URL,
#             table="iot_events_enhanced",
#             properties=POSTGRES_PROPERTIES
#         )
#     )

# =========================
# load macihne summary data to gold  table
def write_to_postgres_machine_summary(batch_df, batch_id):
    
    try:
        batch_df.printSchema()
    except Exception as e:
        print("ERROR IN MACHINE SUMMARY SCHEMA")
        print(str(e))
        raise


    try:
        print(f"Writing Machine Summary batch {batch_id}")

        batch_df.show(truncate=False)

        (
            batch_df.write
            .mode("overwrite")
            .option("truncate", "true")
            .jdbc(
                url=POSTGRES_URL,
                table="machine_summary",
                properties=POSTGRES_PROPERTIES
            )
        )

        print("SUCCESS LOAD MACHINE SUMMARY")

    except Exception as e:
        print("ERROR IN MACHINE SUMMARY")
        print(str(e))
        raise
# ========================
# load machine hourly summary to gold table
def write_to_postgres_hourly_summary(batch_df, batch_id):

    try:
        batch_df.printSchema()
    except Exception as e:
        print("ERROR IN HOURLY MACHINE SUMMARY SCHEMA")
        print(str(e))
        raise

    try:
        print(f"Writing Hourly Summary batch {batch_id}")
        batch_df.show(truncate=False)

        (
            batch_df.write
            .mode("overwrite")
            .option("truncate", "true")
            .jdbc(
                url=POSTGRES_URL,
                table="hourly_summary",
                properties=POSTGRES_PROPERTIES
            )
        )

        print("SUCCESS LOAD MACHINE HOURLY SUMMARY")
    except Exception as e:
        print("ERROR IN HOURLY SUMMARY")
        print(str(e))
        raise
# ============

# ========================
# load shift summary to gold table
def write_to_postgres_shift_summary(batch_df, batch_id):

    try:
        batch_df.printSchema()
    except Exception as e:
        print("ERROR IN SHIFT SUMMARY SCHEMA")
        print(str(e))
        raise

    try:
        print(f"Writing Shift Summary batch {batch_id}")
        batch_df.show(truncate=False)


        (
            batch_df.write
            .mode("overwrite")
            .option("truncate", "true")
            .jdbc(
                url=POSTGRES_URL,
                table="shift_summary",
                properties=POSTGRES_PROPERTIES
            )
        )
        print('SUCESS LOAD SHIFT SUMMARY')
    except Exception as e:
        print("ERROR IN SHIFT SUMMARY")
        print(str(e))
        raise


