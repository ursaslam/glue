import pyspark.sql.functions as F
from utilities.logger import log

def write_staging(df, staging_path, full_load):
    mode = "overwrite" if full_load else "append"

    df2 = df.withColumn("load_dt", F.current_date()) \
            .withColumn("load_hr", F.hour(F.current_timestamp()))

    df2.write.mode(mode).partitionBy("load_dt", "load_hr").parquet(staging_path)

    log("info", "staging", "Data written to staging", path=staging_path)
