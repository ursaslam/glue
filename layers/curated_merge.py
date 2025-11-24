from utilities.logger import log
from layers.iceberg_manager import merge_into_iceberg, create_iceberg_table
import pyspark.sql.functions as F


def write_curated(df, curated_path, full_load, iceberg_conf, spark):
    """
    Handles writing to curated layer.

    LOGIC:
      - If Iceberg enabled:
            FULL_LOAD = True  → OVERWRITE entire Iceberg table
            FULL_LOAD = False → MERGE INTO using id_cols
      - If Iceberg disabled:
            FULL_LOAD = True  → Parquet overwrite
            FULL_LOAD = False → Parquet append
    """

    iceberg_enabled = iceberg_conf.get("enabled", False)

    # -------------------------------------------------------
    # Iceberg mode
    # -------------------------------------------------------
    if iceberg_enabled:
        catalog = iceberg_conf["catalog"]
        database = iceberg_conf["database"]
        table = iceberg_conf["table"]

        id_cols = iceberg_conf.get("id_cols", [])
        partition_cols = iceberg_conf.get("partition_cols", [])

        # Create Iceberg table if not exists
        create_iceberg_table(
            spark=spark,
            catalog=catalog,
            database=database,
            table=table,
            df=df,
            partition_cols=partition_cols
        )

        # FULL LOAD → drop & recreate behaviour
        if full_load:
            spark.sql(f"DROP TABLE IF EXISTS {catalog}.{database}.{table}")
            create_iceberg_table(
                spark=spark,
                catalog=catalog,
                database=database,
                table=table,
                df=df,
                partition_cols=partition_cols
            )
            df.write.format("iceberg").mode("overwrite").saveAsTable(
                f"{catalog}.{database}.{table}"
            )
            log("info", "curated_iceberg_full_load",
                f"Overwrote Iceberg table: {catalog}.{database}.{table}")
            return

        # INCREMENTAL LOAD → merge into Iceberg table
        merge_into_iceberg(
            spark=spark,
            df=df,
            catalog=catalog,
            database=database,
            table=table,
            id_cols=id_cols,                 # MERGE ON id_cols only
            partition_cols=partition_cols    # Not used for merge, informational
        )

        log("info", "curated_iceberg_merge",
            f"Incremental MERGE completed → {catalog}.{database}.{table}")
        return

    # -------------------------------------------------------
    # PARQUET MODE
    # -------------------------------------------------------
    mode = "overwrite" if full_load else "append"

    df.write.mode(mode).parquet(curated_path)

    log("info", "curated_parquet",
        "Parquet write complete",
        mode=mode,
        path=curated_path)

