import sys

from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
import pyspark.sql.functions as F

from app.custom.utilities.logger import log
from app.custom.utilities.metadata_loader import load_metadata
from app.custom.utilities.schema_validator import validate_schema
from app.custom.utilities.id_dedupe import dedupe
from app.custom.utilities.rowcount_validator import validate_rowcount

from app.custom.transformations.column_transformations import apply_column_transformations
from app.custom.transformations.dq_checks import apply_dq_rules

from app.custom.layers.staging_layer import write_staging
from app.custom.layers.curated_merge import write_curated
from app.custom.layers.audit_writer import write_audit


def main():
    # ---------------------------------------------------------
    # Glue arguments
    # ---------------------------------------------------------
    args = getResolvedOptions(
        sys.argv,
        ["SYS_LEVEL", "DATASET", "FULL_LOAD"],
    )

    sys_level = args["SYS_LEVEL"]
    dataset_nm = args["DATASET"]
    full_load = args["FULL_LOAD"].lower() == "true"

    # ---------------------------------------------------------
    # S3 bucket layout (from your notes)
    # ---------------------------------------------------------
    base = f"s3://bi-efs-{sys_level}-us-east-1-dna-raw-sf-ans"

    staging_path = f"{base}/adw/ERP/Spen/data/staging/{dataset_nm}"
    audit_path = f"{base}/adw/ERP/Spen/data/audit/"
    curated_path = f"{base}/curated/adw/ERP/Spen/{dataset_nm}/"

    log(
        "info",
        "paths",
        "Resolved S3 paths",
        sys_level=sys_level,
        dataset=dataset_nm,
        staging_path=staging_path,
        audit_path=audit_path,
        curated_path=curated_path,
    )

    # ---------------------------------------------------------
    # Spark / Glue init
    # ---------------------------------------------------------
    sc = SparkContext()
    glue_ctx = GlueContext(sc)
    spark = glue_ctx.spark_session

    log("info", "init", "Glue job initialized")

    # ---------------------------------------------------------
    # Load metadata JSON for this dataset
    # e.g. config/VGI_FIN_GL_LOB_DH_V.json
    # ---------------------------------------------------------
    metadata_path = f"config/{dataset_nm}.json"
    ds = load_metadata(metadata_path)

    src_prefix = ds["src_prefix"]
    fmt = ds["format"]              # "csv" for Oracle extracts
    cols = ds["cols"]
    id_cols = ds.get("id_cols", [])
    upd_cols = ds.get("upd_cols", [])
    dq_rules = ds.get("dq_rules", {})
    iceberg_conf = ds.get("iceberg", {})
    domicile_value = ds["domicile"]
    custom_sql_stmt = (ds.get("custom_sql_stmt") or "").strip()

    # Log column sensitivities (if present)
    for c in cols:
        sensitivity = (c.get("sensitivity") or "").strip()
        if sensitivity:
            log(
                "info",
                "column_sensitivity",
                "Sensitive column detected",
                column=c.get("tgt_nm") or c.get("src_nm"),
                sensitivity=sensitivity,
            )

    log(
        "info",
        "metadata_parsed",
        "Metadata parsed",
        dataset_nm=ds.get("nm"),
        src_prefix=src_prefix,
        format=fmt,
        id_cols=id_cols,
        upd_cols=upd_cols,
        domicile=domicile_value,
        iceberg_enabled=iceberg_conf.get("enabled", False),
    )

    # ---------------------------------------------------------
    # 1) READ SOURCE (CSV from Oracle on S3)
    # ---------------------------------------------------------
    reader = spark.read.format(fmt)

    if fmt.lower() == "csv":
        reader = (
            reader.option("header", "true")
                  .option("inferSchema", "true")
        )

    log("info", "read_input", f"Reading source from {src_prefix}")
    df_src = reader.load(src_prefix)
    src_count_initial = df_src.count()

    log(
        "info",
        "read_input",
        "Source read completed",
        rows=src_count_initial,
        src_prefix=src_prefix,
    )

    # ---------------------------------------------------------
    # 1.1) Apply custom SQL filter (dataset-level)
    # custom_sql_stmt is expected to be a WHERE expression, e.g.:
    #   "DATASOURCE_NUM_ID IS NOT NULL"
    # ---------------------------------------------------------
    if custom_sql_stmt:
        log(
            "info",
            "custom_sql",
            "Applying custom SQL filter",
            where_expr=custom_sql_stmt,
        )
        df_src = df_src.filter(custom_sql_stmt)
        src_count_after_sql = df_src.count()
        log(
            "info",
            "custom_sql",
            "Custom SQL filter applied",
            rows_before=src_count_initial,
            rows_after=src_count_after_sql,
        )
        src_count_initial = src_count_after_sql

    # ---------------------------------------------------------
    # 1.2) Ensure domicile column at row level
    # ---------------------------------------------------------
    if "domicile" not in df_src.columns:
        df_src = df_src.withColumn("domicile", F.lit(domicile_value))
    else:
        df_src = df_src.withColumn(
            "domicile",
            F.when(
                (F.col("domicile").isNull())
                | (F.col("domicile") == "")
                | (F.lower(F.col("domicile")).isin("null", "none")),
                domicile_value,
            ).otherwise(F.col("domicile")),
        )

    # ---------------------------------------------------------
    # 2) WRITE TO STAGING (raw copy with load_dt/load_hr)
    # ---------------------------------------------------------
    write_staging(df_src, staging_path, full_load=full_load)

    # We'll continue using df_src for ETL
    df_work = df_src

    # ---------------------------------------------------------
    # 3) SCHEMA VALIDATION
    # ---------------------------------------------------------
    validate_schema(df_work, cols, dq_rules)

    # ---------------------------------------------------------
    # 4) COLUMN TRANSFORMATIONS (rename + type enforcement)
    # ---------------------------------------------------------
    df_tx = apply_column_transformations(df_work, cols)

    # ---------------------------------------------------------
    # 5) ID-BASED DEDUPE
    # ---------------------------------------------------------
    df_dedup = dedupe(df_tx, id_cols=id_cols, upd_cols=upd_cols)

    # ---------------------------------------------------------
    # 6) COLUMN-LEVEL DQ (nullable + regex)
    # ---------------------------------------------------------
    df_valid, df_reject = apply_dq_rules(df_dedup, cols)

    curated_count = df_valid.count()
    rejected_count = df_reject.count() if df_reject is not None else 0

    # ---------------------------------------------------------
    # 7) ROW COUNT VALIDATION
    # ---------------------------------------------------------
    validate_rowcount(src_count_initial, curated_count, dq_rules)

    # ---------------------------------------------------------
    # 8) WRITE CURATED (Iceberg ≫ overwrite / merge OR Parquet)
    # ---------------------------------------------------------
    write_curated(
        spark=spark,
        df_curated=df_valid,
        curated_path=curated_path,
        full_load=full_load,
        iceberg_conf=iceberg_conf,
        domicile_value=domicile_value,
        id_cols=id_cols,
    )

    # Rejects always as Parquet side-path
    if df_reject is not None and rejected_count > 0:
        reject_path = curated_path.rstrip("/") + "_rejects/"
        df_reject.write.mode("overwrite").parquet(reject_path)
        log(
            "info",
            "rejects",
            "Reject records written",
            path=reject_path,
            rows=rejected_count,
        )

    # ---------------------------------------------------------
    # 9) AUDIT RECORD
    # ---------------------------------------------------------
    write_audit(
        spark=spark,
        audit_path=audit_path,
        dataset_nm=dataset_nm,
        sys_level=sys_level,
        src_count=src_count_initial,
        curated_count=curated_count,
        rejected_count=rejected_count,
        status="SUCCESS",
    )

    log(
        "info",
        "complete",
        "ETL pipeline completed successfully",
        source_rows=src_count_initial,
        curated_rows=curated_count,
        rejected_rows=rejected_count,
    )


if __name__ == "__main__":
    main()
