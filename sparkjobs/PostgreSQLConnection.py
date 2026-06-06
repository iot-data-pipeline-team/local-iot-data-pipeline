POSTGRES_URL = (
    "jdbc:postgresql://localhost:5433/data_hub"
)

POSTGRES_PROPERTIES = {
    "user": "admin",
    "password": "password123",
    "driver": "org.postgresql.Driver"
}



def write_to_postgres(batch_df, batch_id):

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