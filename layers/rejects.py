import boto3
from botocore.exceptions import ClientError


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
# Drop + Create Glue Table for Rejects
# -------------------------------------------------------------------
def create_rejects_glue_table(database: str, table_name: str, location: str, df):
    glue = boto3.client("glue")

    # -------------------------
    # 1) Ensure database exists
    # -------------------------
    try:
        glue.create_database(DatabaseInput={"Name": database})
    except glue.exceptions.AlreadyExistsException:
        pass

    # -------------------------
    # 2) Drop table if exists
    # -------------------------
    try:
        glue.delete_table(DatabaseName=database, Name=table_name)
        print(f"Dropped existing rejects table: {database}.{table_name}")
    except glue.exceptions.EntityNotFoundException:
        pass

    # -------------------------
    # 3) Build Glue columns from Spark schema
    # -------------------------
    glue_cols = [
        {"Name": f.name, "Type": spark_to_glue_type(f.dataType)}
        for f in df.schema.fields
    ]

    # -------------------------
    # 4) Create Glue table
    # -------------------------
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

    print(f"Created rejects Glue table: {database}.{table_name}")
    print(f"Location: {location}")
