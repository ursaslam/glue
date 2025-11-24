from utilities.logger import log
import pyspark.sql.functions as F


# -------------------------------------------------------------
# 1. ICEBERG TABLE CREATION
# -------------------------------------------------------------
def create_iceberg_table(spark, catalog, database, table, df, partition_cols):
    """
    Creates Iceberg table automatically using DF schema + metadata partitions.
    """

    # Create namespace (DB)
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{database}")

    # Generate schema based on DataFrame fields
    columns_sql = ",\n".join(
        [f"{field.name} {field.dataType.simpleString()}" 
         for field in df.schema.fields]
    )

    partition_sql = (
        f"PARTITIONED BY ({', '.join(partition_cols)})"
        if partition_cols else ""
    )

    create_sql = f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{database}.{table} (
            {columns_sql}
        )
        USING iceberg
        {partition_sql}
    """

    spark.sql(create_sql)

    log("info", "iceberg_create",
        f"Iceberg table created (or already exists): {catalog}.{database}.{table}",
        partitions=partition_cols)
    

# -------------------------------------------------------------
# 2. ICEBERG MERGE USING ONLY ID_COLS FROM METADATA
# -------------------------------------------------------------
def merge_into_iceberg(
    spark,
    df,
    catalog,
    database,
    table,
    id_cols
):
    """
    Incremental MERGE INTO Iceberg using only ID_COLS from metadata JSON.
    """

    # Load target table name
    target = f"{catalog}.{database}.{table}"

    # Register incoming dataframe
    df.createOrReplaceTempView("incoming_records")

    # Build MERGE condition: id1 AND id2 AND id3 ...
    merge_condition = " AND ".join([f"t.{col} = s.{col}" for col in id_cols])

    # MERGE INTO Iceberg table
    sql = f"""
        MERGE INTO {target} t
        USING incoming_records s
        ON {merge_condition}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """

    spark.sql(sql)

    log("info", "iceberg_merge",
        f"Iceberg MERGE completed for {target}",
        merge_condition=merge_condition)
