from datetime import datetime
from utilities.logger import log

def write_audit(spark, audit_path, dataset, sys_level,
                src_count, curated_count, rejected_count, status):
    df = spark.createDataFrame(
        [(dataset, sys_level, datetime.utcnow().isoformat(),
          src_count, curated_count, rejected_count, status)],
        ["dataset", "sys_level", "timestamp_utc",
         "src_count", "curated_count", "rejected_count", "status"]
    )

    df.write.mode("append").parquet(audit_path)

    log("info", "audit", "Audit record written", path=audit_path)
