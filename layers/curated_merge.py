import pyspark.sql.functions as F

from app.custom.utilities.logger import log
from app.custom.layers.iceberg_manager import (
    create_iceberg_table,
    overwrite_iceberg_table,
    merge_into_iceberg,
)


def write_curated(
    spark,
    df_curated,
    curated_path: str,
    full_load: bool,
    iceberg_conf: dict,
    domicile_value: str,
    id_cols,
):
    """
    Write curated data.

    - If iceberg_conf["enabled"] == True:
        * create table if not exists
        * full load: truncate + insert
        * incremental: MERGE into
    - Else:
        * Write Parquet to curated_path
        * full load: overwrite, incremental: append
    """
    iceberg_enabled = bool(iceberg_conf.get("enabled", False))

    if iceberg_enabled:
        catalog = iceberg_conf["catalog"]
        database = iceberg_conf["database"]
        table = iceberg_conf["table"]
        partition_cols = iceberg_conf.get("partition_cols", ["domicile"])

        # Ensure domicile column is present before table creation
        if "domicile" not in df_curated.columns:
            df_curated = df_curated.withColumn("domicile", F.lit(domicile_value))

        create_iceberg_table(
            spark=spark,
            catalog=catalog,
            database=database,
            table=table,
            df=df_curated,
            partition_cols=partition_cols,
        )

        if full_load:
            overwrite_iceberg_table(
                spark=spark,
                df=df_curated,
                catalog=catalog,
                database=database,
                table=table,
                domicile_value=domicile_value,
            )
        else:
            merge_into_iceberg(
                spark=spark,
                df=df_curated,
                catalog=catalog,
                database=database,
                table=table,
                id_cols=id_cols,
                domicile_value=domicile_value,
            )

    else:
        # Fallback: just write Parquet
        mode = "overwrite" if full_load else "append"

        df_out = df_curated
        if "domicile" not in df_out.columns:
            df_out = df_out.withColumn("domicile", F.lit(domicile_value))

        df_out.write.mode(mode).partitionBy("domicile").parquet(curated_path)

        log(
            "info",
            "curated_parquet",
            "Curated Parquet write completed",
            path=curated_path,
            mode=mode,
            rows=df_out.count(),
        )
