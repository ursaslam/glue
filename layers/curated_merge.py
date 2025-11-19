from utilities.logger import log
from layers.iceberg_manager import merge_into_iceberg

def write_curated(df, curated_path, full_load, iceberg_conf, spark):
    if iceberg_conf and iceberg_conf.get("enabled", False):
        merge_into_iceberg(
            spark=spark,
            catalog=iceberg_conf["catalog"],
            database=iceberg_conf["database"],
            table=iceberg_conf["table"],
            df=df,
            id_cols=iceberg_conf.get("id_cols", []),
            upd_cols=iceberg_conf.get("upd_cols", [])
        )
        return

    mode = "overwrite" if full_load else "append"
    df.write.mode(mode).parquet(curated_path)

    log("info", "curated_layer",
        "Curated layer written (Parquet mode)",
        path=curated_path,
        mode=mode)
