import os

import psycopg2


POSTGRES_JDBC_URL = (
    f"jdbc:postgresql://{os.getenv('POSTGRES_HOST', 'postgres')}:"
    f"{os.getenv('POSTGRES_PORT', '5432')}/{os.getenv('POSTGRES_DB', 'bigdataspark')}"
)
POSTGRES_PROPERTIES = {
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
    "driver": "org.postgresql.Driver",
}

CLICKHOUSE_JDBC_URL = (
    f"jdbc:clickhouse://{os.getenv('CLICKHOUSE_HOST', 'clickhouse')}:"
    f"{os.getenv('CLICKHOUSE_PORT', '8123')}/{os.getenv('CLICKHOUSE_DB', 'reports')}"
)
CLICKHOUSE_PROPERTIES = {
    "user": os.getenv("CLICKHOUSE_USER", "default"),
    "password": os.getenv("CLICKHOUSE_PASSWORD", "clickhouse"),
    "driver": "com.clickhouse.jdbc.ClickHouseDriver",
}


def table_writer(df, table_name, jdbc_url, properties, mode="overwrite", create_table_options=None):
    writer = df.write.mode(mode).option("driver", properties["driver"])
    if create_table_options:
        writer = writer.option("createTableOptions", create_table_options)
    writer.jdbc(url=jdbc_url, table=table_name, properties=properties)


def truncate_postgres_tables(table_names):
    connection = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "bigdataspark"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres"),
    )
    connection.autocommit = True
    try:
        with connection.cursor() as cursor:
            cursor.execute("TRUNCATE TABLE {} RESTART IDENTITY CASCADE".format(", ".join(table_names)))
    finally:
        connection.close()
