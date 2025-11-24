from utilities.logger import log
import pyspark.sql.functions as F


def create_iceberg_table(spark, catalog, database, table, df, partition_cols):
    """
    Create Iceberg table if not exists, using the DF schema and optional
    partition columns (e.g. ["domicile"]).
    """
    # Make sure namespace exists
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{database}")

    cols_expr = []
    for field in df.schema.fields:
        # Spark's simpleString gives types like "decimal(22,0)", "string", etc.
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
    log(
        "info",
        "iceberg_create",
        f"Table created or already exists: {catalog}.{database}.{table}",
        partition_cols=partition_cols,
    )


def merge_into_iceberg(
    spark,
    df,
    catalog,
    database,
    table,
    merge_cols,
    partition_cols,
    id_cols,
    domicile_value=None,
):
    """
    Incremental MERGE INTO Iceberg table driven by metadata JSON.

    Parameters
    ----------
    spark : SparkSession
    df : DataFrame
        Curated, valid dataframe ready to be merged.
    catalog : str
        Iceberg catalog name (e.g. "glue_catalog").
    database : str
        Target database.
    table : str
        Target table.
    merge_cols : list[str]
        Columns to use in the MERGE ON clause. Comes from metadata JSON
        (e.g. "merge_cols") and may typically include ID columns and
        domiciled/partition columns.
    partition_cols : list[str]
        Partition columns for the table, usually ["domicile"] or
        ["domicile", "load_dt"].
    id_cols : list[str]
        ID columns that should not be null; nulls will be replaced with
        the literal string "EMPTY" as part of DQ enforcement.
    domicile_value : str | None
        If provided and the DF does NOT already have a "domicile" column,
        a new column "domicile" will be added with this constant value.
    """

    target = f"{catalog}.{database}.{table}"

    # --------------------------------------------------------------
    # 1) Ensure domicile column exists if domicile_value is provided
    # --------------------------------------------------------------
    if domicile_value is not None and "domicile" not in df.columns:
        df = df.withColumn("domicile", F.lit(domicile_value))
        log(
            "info",
            "iceberg_merge",
            "Added domicile column to DF",
            domicile_value=domicile_value,
        )

    # --------------------------------------------------------------
    # 2) Enforce non-null ID columns by replacing nulls with "EMPTY"
    # --------------------------------------------------------------
    for c in id_cols or []:
        if c in df.columns:
            df = df.withColumn(
                c,
                F.when(F.col(c).isNull(), F.lit("EMPTY")).otherwise(F.col(c)),
            )

    # --------------------------------------------------------------
    # 3) Register temp view for MERGE
    # --------------------------------------------------------------
    df.createOrReplaceTempView("incoming_data")

    # --------------------------------------------------------------
    # 4) Build MERGE condition based only on JSON-defined columns
    # --------------------------------------------------------------
    if not merge_cols:
        raise ValueError("merge_into_iceberg: merge_cols cannot be empty")

    merge_condition = " AND ".join([f"t.{c} = s.{c}" for c in merge_cols])

    merge_sql = f"""
    MERGE INTO {target} t
    USING incoming_data s
    ON {merge_condition}
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """

    spark.sql(merge_sql)

    log(
        "info",
        "iceberg_merge",
        f"Merged incremental data into {target}",
        merge_cols=merge_cols,
        partition_cols=partition_cols,
    )
