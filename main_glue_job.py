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


# ----------------------------
# Glue Args
# ----------------------------
args = getResolvedOptions(sys.argv,
                          ["SYS_LEVEL", "DATASET", "FULL_LOAD"])

sys_level = args["SYS_LEVEL"]
dataset = args["DATASET"]
full_load = args["FULL_LOAD"].lower() == "true"


# ----------------------------
# Path Derivation From Screenshot
# ----------------------------
base = f"s3://bi-efs-{sys_level}-us-east-1-dna-raw-sf-ans"

# Source is from metadata JSON
# RAW INPUT = metadata["src_prefix"]

staging_path = f"{base}/adw/ERP/Spen/data/staging/{dataset}"
audit_path   = f"{base}/adw/ERP/Spen/data/audit/"
curated_path = f"{base}/curated/adw/ERP/Spen/{dataset}/"


# ----------------------------
# Spark Init
# ----------------------------
sc = SparkContext()
glue = GlueContext(sc)
spark = glue.spark_session

log("info", "init", "Glue job started")


# ----------------------------
# Load Metadata JSON
# ----------------------------
meta = load_metadata("config/sample_metadata.json")

src_prefix = meta["src_prefix"]
fmt = meta["format"]
cols = meta["cols"]
id_cols = meta["id_cols"]
upd_cols = meta["upd_cols"]
dq_rules = meta["dq_rules"]


# ----------------------------
# Read Source (from metadata JSON)
# ----------------------------
df_src = spark.read.format(fmt).load(src_prefix)
src_count = df_src.count()

log("info", "read", "Source dataset loaded",
    src_prefix=src_prefix, rows=src_count)


# ----------------------------
# STAGING
# ----------------------------
write_staging(df_src, staging_path, full_load)


# ----------------------------
# SCHEMA VALIDATION
# ----------------------------
validate_schema(df_src, cols, dq_rules)


# ----------------------------
# TRANSFORMATIONS
# ----------------------------
df_tx = apply_column_transformations(df_src, cols)


# ----------------------------
# DEDUPE
# ----------------------------
df_dedup = dedupe(df_tx, id_cols, upd_cols)


# ----------------------------
# DQ CHECKS
# ----------------------------
df_valid, df_reject = apply_dq_rules(df_dedup, cols)

curated_count = df_valid.count()
rejected_count = df_reject.count() if df_reject else 0


# ----------------------------
# ROW COUNT VALIDATION
# ----------------------------
validate_rowcount(src_count, curated_count, dq_rules)


# ----------------------------
# CURATED WRITE
# ----------------------------
write_curated(df_valid, curated_path, full_load)

if df_reject:
    reject_path = curated_path.rstrip("/") + "_rejects/"
    df_reject.write.mode("overwrite").parquet(reject_path)
    log("info", "rejects", "Rejects written", path=reject_path)


# ----------------------------
# AUDIT WRITE
# ----------------------------
write_audit(spark, audit_path, dataset, sys_level,
            src_count, curated_count, rejected_count, "SUCCESS")


log("info", "complete", "ETL Pipeline Completed Successfully")
