import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

from utilities.logger import log
from utilities.metadata_loader import load_metadata
from utilities.schema_validator import validate_schema
from utilities.id_dedupe import dedupe
from utilities.rowcount_validator import validate_rowcount

from transformations.column_transformations import apply_column_transformations
from transformations.dq_checks import apply_dq_rules

from layers.staging_layer import write_staging
from layers.curated_merge import write_curated
from layers.audit_writer import write_audit
from layers.iceberg_manager import create_iceberg_table


def main():
    # ---------------------------------------------------------
    # Glue Arguments
    # ---------------------------------------------------------
    args = getResolvedOptions(sys.argv,
                              ["SYS_LEVEL", "DATASET", "FULL_LOAD"])

    sys_level = args["SYS_LEVEL"]
    dataset = args["DATASET"]
    full_load = args["FULL_LOAD"].lower() == "true"

    # ---------------------------------------------------------
    # Build bucket paths using your naming pattern
    # ---------------------------------------------------------
    base = f"s3://bi-efs-{sys_level}-us-east-1-dna-raw-sf-ans"

    staging_path = f"{base}/adw/ERP/Spen/data/staging/{dataset}"
    audit_path   = f"{base}/adw/ERP/Spen/data/audit/"
    curated_path = f"{base}/curated/adw/ERP/Spen/{dataset}/"

    log("info", "paths", "Resolved S3 paths",
        sys_level=sys_level,
        dataset=dataset,
        staging_path=staging_path,
        audit_path=audit_path,
        curated_path=curated_path)

    # ---------------------------------------------------------
    # Spark Init
    # ---------------------------------------------------------
    sc = SparkContext()
    glue_ctx = GlueContext(sc)
    spark = glue_ctx.spark_session

    log("info", "init", "Glue job initialized")

    # ---------------------------------------------------------
    # Load metadata JSON from local config
    # ---------------------------------------------------------
    ds = load_metadata("config/sample_metadata.json")

    src_prefix = ds["src_prefix"]
    fmt = ds["format"]
    cols = ds["cols"]
    id_cols = ds.get("id_cols", [])
    upd_cols = ds.get("upd_cols", [])
    dq_rules = ds.get("dq_rules", {})
    iceberg_conf = ds.get("iceberg", {})

    log("info", "metadata", "Metadata loaded",
        dataset_name=ds.get("nm"),
        src_prefix=src_prefix,
        format=fmt,
        id_cols=id_cols,
        upd_cols=upd_cols,
        dq_rules=dq_rules,
        iceberg_enabled=iceberg_conf.get("enabled", False))

    # ---------------------------------------------------------
    # 1) Read input (from src_prefix in metadata)
    # ---------------------------------------------------------
    df_src = spark.read.format(fmt).load(src_prefix)
    src_count = df_src.count()

    log("info", "read_input", "Source read complete",
        rows=src_count,
        src_prefix=src_prefix)

    # ---------------------------------------------------------
    # 2) Write to STAGING
    # ---------------------------------------------------------
    write_staging(df_src, staging_path, full_load)

    # We continue with df_src for transformations
    df_work = df_src

    # ---------------------------------------------------------
    # 3) Schema Validation
    # ---------------------------------------------------------
    validate_schema(df_work, cols, dq_rules)

    # ---------------------------------------------------------
    # 4) Column Transformations (rename + type enforcement)
    # ---------------------------------------------------------
    df_tx = apply_column_transformations(df_work, cols)

    # ---------------------------------------------------------
    # 5) DEDUPE (id columns + update timestamp columns)
    # ---------------------------------------------------------
    df_dedup = dedupe(df_tx, id_cols, upd_cols)

    # ---------------------------------------------------------
    # 6) DQ Checks → valid + reject sets
    # ---------------------------------------------------------
    df_valid, df_reject = apply_dq_rules(df_dedup, cols)

    curated_count = df_valid.count()
    rejected_count = df_reject.count() if df_reject is not None else 0

    # ---------------------------------------------------------
    # 7) Row Count Validation
    # ---------------------------------------------------------
    validate_rowcount(src_count, curated_count, dq_rules)

    # ---------------------------------------------------------
    # 8) If Iceberg is enabled, create table before merge
    # ---------------------------------------------------------
    if iceberg_conf.get("enabled", False):
        create_iceberg_table(
            spark=spark,
            catalog=iceberg_conf["catalog"],
            database=iceberg_conf["database"],
            table=iceberg_conf["table"],
            df=df_valid,
            partition_cols=iceberg_conf.get("partition_cols", [])
        )

    # ---------------------------------------------------------
    # 9) Write curated layer (Iceberg or Parquet)
    # ---------------------------------------------------------
    write_curated(
        df=df_valid,
        curated_path=curated_path,
        full_load=_
