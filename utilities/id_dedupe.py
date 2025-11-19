import pyspark.sql.functions as F
from pyspark.sql.window import Window
from utilities.logger import log

def dedupe(df, id_cols, upd_cols):
    if not id_cols or not upd_cols:
        log("warn", "dedupe", "Skipping dedupe due to missing keys")
        return df

    log("info", "dedupe", "Running dedupe")

    w = Window.partitionBy(*id_cols).orderBy(
        *[F.col(c).desc() for c in upd_cols]
    )

    return df.withColumn("rn", F.row_number().over(w)) \
             .filter("rn = 1") \
             .drop("rn")
