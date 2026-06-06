POSTGRES_URL = (
    "jdbc:postgresql://localhost:5433/data_hub"
)

POSTGRES_PROPERTIES = {
    "user": "admin",
    "password": "password123",
    "driver": "org.postgresql.Driver"
}



def write_to_postgres_bronze(batch_df, batch_id):

    print(f"Writing batch {batch_id}")

    (
        batch_df.write
        .mode("append")
        .jdbc(
            url=POSTGRES_URL,
            table="iot_events",
            properties=POSTGRES_PROPERTIES
        )
    )



def write_to_postgres_silver(batch_df, batch_id):

    print(f"Writing batch {batch_id}")

    (
        batch_df.write
        .mode("append")
        .jdbc(
            url=POSTGRES_URL,
            table="iot_events_enhanced",
            properties=POSTGRES_PROPERTIES
        )
    )

def write_to_postgres_gold(batch_df, batch_id):

    print(f"Writing batch {batch_id}")

    (
        batch_df.write
        .mode("overwrite")
        .jdbc(
            url=POSTGRES_URL,
            table="machine_summary",
            properties=POSTGRES_PROPERTIES
        )
    )