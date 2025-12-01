# ------------------------------------------------------------
# rejects_manager.py
# ------------------------------------------------------------
from typing import Any, List
import boto3
from pyspark.sql import DataFrame, SparkSession
from app.custom.utilities.logger import log_event


def write_rejects(
    spark: SparkSession,
    df_reject: DataFrame,
    raw_bucket: str,
    data_domain: str,
    table_name: str,
    glue_database: str,
    process_dt: str,
    process_hr: str
) -> None:
    """
    Handles writing rejects to RAW bucket and creating Glue table.

    Steps:
      1. Write rejects to Parquet
      2. Create Glue table if not exists
      3. Add partition (dt, hr)
    """

    if df_reject is None or df_reject.count() == 0:
        log_event("rejects", "No rejects found. Skipping write.")
        return

    # ------------------------------------------------------------
    # Build reject S3 path
    # ------------------------------------------------------------
    reject_path = (
        f"s3://{raw_bucket}/{data_domain}/rejects/{table_name}/"
        f"dt={process_dt}/hr={process_hr}/"
    )

    # ------------------------------------------------------------
    # Write rejects to Parquet
    # ------------------------------------------------------------
    df_reject.write.mode("overwrite").parquet(reject_path)

    log_event(
        "rejects_write",
        "Reject records written to RAW bucket",
        path=reject_path,
        rows=df_reject.count()
    )

    # ------------------------------------------------------------
    # Create Glue table
    # ------------------------------------------------------------
    glue = boto3.client("glue")
    reject_table = f"{table_name}_rejects"

    # Columns for Glue table
    columns = [
        {"Name": f.name, "Type": f.dataType.simpleString()}
        for f in df_reject.schema.fields
    ]

    try:
        glue.create_table(
            DatabaseName=glue_database,
            TableInput={
                "Name": reject_table,
                "TableType": "EXTERNAL_TABLE",
                "Parameters": {
                    "classification": "parquet",
                    "EXTERNAL": "TRUE"
                },
                "StorageDescriptor": {
                    "Columns": columns,
                    "Location": f"s3://{raw_bucket}/{data_domain}/rejects/{table_name}/",
                    "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                    "SerdeInfo": {
                        "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
                    }
                },
                "PartitionKeys": [
                    {"Name": "dt", "Type": "string"},
                    {"Name": "hr", "Type": "string"}
                ]
            }
        )

        log_event(
            "rejects_table",
            f"Created Glue table {reject_table}",
            location=reject_path
        )

    except glue.exceptions.AlreadyExistsException:

        log_event(
            "rejects_table",
            f"Glue table {reject_table} already exists — adding partition",
        )

        # ------------------------------------------------------------
        # Add partition for dt/hr
        # ------------------------------------------------------------
        glue.create_partition(
            DatabaseName=glue_database,
            TableName=reject_table,
            PartitionInput={
                "Values": [process_dt, process_hr],
                "StorageDescriptor": {
                    "Columns": columns,
                    "Location": reject_path,
                    "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                    "SerdeInfo": {
                        "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
                    }
                }
            }
        )

        log_event(
            "rejects_partition",
            f"Partition dt={process_dt}, hr={process_hr} added to {reject_table}",
        )
