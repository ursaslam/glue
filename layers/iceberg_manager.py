from typing import List

import pyspark.sql.functions as F

from app.custom.utilities.logger import log


def _ensure_domicile_column(df, domicile_value: str):
    """
    Guarantee 'domicile' column exists and has a non-null value.
    """
    if "domicile" not in df.columns:
        df = df.withColumn("domicile", F.lit(domicile_value))
    else:
        df = df.withColumn(
            "domicile",
            F.when(
                (F.col("domicile").isNull())
                | (F.col("domicile") == "")
                | (F.lower(F.col("domicile")).isin("null", "none")),
                domicile_value,
            ).otherwise(F.col("domicile")),
        )
    return df


def create_iceberg_table(spark, catalog: str, database: str, table: str, df, partition_cols: List[str]):
    """
    Create Iceberg table if it does not exist, using DF schema.
    """
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{database}")

    cols_expr = []
    for field in df.schema.fields:
        cols_expr.append(f"{field.name} {field.dataType.simpleString()}")

    schema_sql = ",\n        ".join(cols_expr)

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

    log("info", "iceberg_create", f"Creating Iceberg table (if not exists): {catalog}.{database}.{table}")
    spark.sql(create_sql)


def overwrite_iceberg_table(
    spark,
    df,
    catalog: str,
    database: str,
    table: str,
    domicile_value: str,
):
    """
    Full load: TRUNCATE Iceberg table and INSERT all rows.
    """
    target = f"{catalog}.{database}.{table}"

    df = _ensure_domicile_column(df, domicile_value)
    df.createOrReplaceTempView("incoming_full")

    log("info", "iceberg_overwrite", f"Truncating Iceberg table before full load: {target}")
    spark.sql(f"TRUNCATE TABLE {target}")

    insert_sql = f"INSERT INTO {target} SELECT * FROM incoming_full"
    spark.sql(insert_sql)

    log("info", "iceberg_overwrite", "Full load completed", table=target, rows=df.count())


def merge_into_iceberg(
    spark,
    df,
    catalog: str,
    database: str,
    table: str,
    id_cols: List[str],
    domicile_value: str,
):
    """
    Incremental load: MERGE INTO Iceberg using id_cols + domicile.
    """
    target = f"{catalog}.{database}.{table}"

    df = _ensure_domicile_column(df, domicile_value)
    df.createOrReplaceTempView("incoming_inc")

    # Join keys = ID columns + domicile
    merge_keys = list(id_cols or [])
    if "domicile" not in merge_keys:
        merge_keys.append("domicile")

    if not merge_keys:
        raise ValueError("Iceberg merge requires at least one id column or domicile key")

    merge_condition = " AND ".join([f"t.{c} = s.{c}" for c in merge_keys])

    merge_sql = f"""
    MERGE INTO {target} t
    USING incoming_inc s
    ON {merge_condition}
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """

    log(
        "info",
        "iceberg_merge",
        "Executing Iceberg MERGE",
        table=target,
        merge_keys=merge_keys,
    )
    spark.sql(merge_sql)

    log("info", "iceberg_merge", "MERGE completed", table=target)
