from utilities.logger import log

def create_iceberg_table(spark, catalog, database, table, df, partition_cols):
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{database}")

    cols_expr = []
    for field in df.schema.fields:
        cols_expr.append(f"{field.name} {field.dataType.simpleString()}")

    schema_sql = ",\n".join(cols_expr)

    part = ""
    if partition_cols:
        part = f"PARTITIONED BY ({', '.join(partition_cols)})"

    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{database}.{table} (
        {schema_sql}
    )
    USING iceberg
    {part}
    """

    spark.sql(create_sql)
    log("info", "iceberg_create",
        f"Iceberg table created or already exists: {catalog}.{database}.{table}")


def merge_into_iceberg(spark, catalog, database, table, df, id_cols, upd_cols):
    target = f"{catalog}.{database}.{table}"
    df.createOrReplaceTempView("incoming_records")

    merge_condition = " AND ".join([f"t.{c} = s.{c}" for c in id_cols])

    order_expr = ", ".join([f"s.{c} DESC" for c in upd_cols]) if upd_cols else ""

    if order_expr:
        df.createOrReplaceTempView("incoming_records_ordered")
        df = spark.sql(f"""
            SELECT *
            FROM incoming_records
            QUALIFY ROW_NUMBER() OVER (PARTITION BY {','.join(id_cols)} ORDER BY {order_expr}) = 1
        """)

    df.createOrReplaceTempView("incoming_dedup")

    merge_sql = f"""
    MERGE INTO {target} t
    USING incoming_dedup s
    ON {merge_condition}
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """

    spark.sql(merge_sql)
    log("info", "iceberg_merge", f"Merged records into Iceberg: {target}")
