import pyspark.sql.functions as F

from app.custom.utilities.logger import log


def write_staging(df, staging_path: str, full_load: bool):
    """
    Write to staging/raw layer with load_dt + load_hr partitions.
    """
    mode = "overwrite" if full_load else "append"

    df_out = (
        df.withColumn("load_dt", F.current_date())
          .withColumn("load_hr", F.hour(F.current_timestamp()))
    )

    df_out.write.mode(mode).partitionBy("load_dt", "load_hr").parquet(staging_path)

    log(
        "info",
        "staging",
        "Staging write completed",
        path=staging_path,
        mode=mode,
        rows=df_out.count(),
    )
