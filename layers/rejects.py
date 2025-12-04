import boto3
from botocore.exceptions import ClientError
from app.custom.utilities.logger import log_event


# -------------------------------------------------------------------
# Convert Spark datatype → Glue datatype
# -------------------------------------------------------------------
def spark_to_glue_type(dt):
    from pyspark.sql.types import (
        StringType, IntegerType, LongType, DoubleType, FloatType,
        DecimalType, BooleanType, DateType, TimestampType
    )

    if isinstance(dt, StringType):
        return "string"
    if isinstance(dt, IntegerType):
        return "int"
    if isinstance(dt, LongType):
        return "bigint"
    if isinstance(dt, DoubleType):
        return "double"
    if isinstance(dt, FloatType):
        return "float"
    if isinstance(dt, BooleanType):
        return "boolean"
    if isinstance(dt, DecimalType):
        return f"decimal({dt.precision},{dt.scale})"
    if isinstance(dt, DateType):
        return "date"
    if isinstance(dt, TimestampType):
        return "timestamp"

    return "string"   # fallback


# -------------------------------------------------------------------
# DROP + CREATE Glue Table for Rejects (with logging)
# -------------------------------------------------------------------
def create_rejects_glue_table(database: str, table_name: str, location: str, df):
    glue = boto3.client("glue")

    log_event(
        "rejects_table",
        "Starting Glue table creation for rejects",
        database=database,
        table=table_name,
        location=location
    )

    # -------------------------
    # 1) Ensure database exists
    # -------------------------
    try:
        glue.create_database(DatabaseInput={"Name": database})
        log_event("rejects_table", "Created Glue database", database=database)
    except glue.exceptions.AlreadyExistsException:
        log_event("rejects_table", "Glue database already exists", database=database)

    # -------------------------
    # 2) Drop table if it already exists
    # -------------------------
    try:
        glue.delete_table(DatabaseName=database, Name=table_name)
        log_event(
            "rejects_table",
            "Dropped existing Glue rejects table",
            database=database,
            table=table_name
        )
    except glue.exceptions.EntityNotFoundException:
        log_event(
            "rejects_table",
            "Rejects table does not exist — nothing to drop",
            database=database,
            table=table_name
        )

    # -------------------------
    # 3) Build Glue schema from Spark schema
    # -------------------------
    glue_cols = [
        {"Name": f.name, "Type": spark_to_glue_type(f.dataType)}
        for f in df.schema.fields
    ]

    # -------------------------
    # 4) Create Glue table
    # -------------------------
    try:
        glue.create_table(
            DatabaseName=database,
            TableInput={
                "Name": table_name,
                "TableType": "EXTERNAL_TABLE",
                "Parameters": {"classification": "parquet"},
                "StorageDescriptor": {
                    "Columns": glue_cols,
                    "Location": location,
                    "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                    "SerdeInfo": {
                        "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                        "Parameters": {"serialization.format": "1"}
                    }
                }
            }
        )

        log_event(
            "rejects_table",
            "Created Glue rejects table successfully",
            database=database,
            table=table_name,
            location=location
        )

    except Exception as e:
        log_event(
            "error",
            "Failed to create Glue rejects table",
            database=database,
            table=table_name,
            error=str(e)
        )
        raise

    log_event(
        "rejects_table",
        "Glue rejects table creation completed",
        database=database,
        table=table_name,
        location=location
    )
