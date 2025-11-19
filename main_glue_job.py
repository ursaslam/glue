from layers.iceberg_manager import create_iceberg_table
from layers.curated_merge import write_curated

# inside main():
iceberg_conf = ds.get("iceberg", {})

# After DQ validation / before curated write
if iceberg_conf.get("enabled", False):
    create_iceberg_table(
        spark=spark,
        catalog=iceberg_conf["catalog"],
        database=iceberg_conf["database"],
        table=iceberg_conf["table"],
        df=df_valid,
        partition_cols=iceberg_conf.get("partition_cols", [])
    )

# Curated write (Iceberg-aware)
write_curated(
    df=df_valid,
    curated_path=curated_path,
    full_load=full_load,
    iceberg_conf=iceberg_conf,
    spark=spark
)
