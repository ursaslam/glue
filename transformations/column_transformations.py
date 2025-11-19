import pyspark.sql.functions as F
from utilities.logger import log

def apply_column_transformations(df, cols):
    out = df

    for c in cols:
        src = c["src_nm"]
        tgt = c["tgt_nm"]

        if src != tgt:
            out = out.withColumnRenamed(src, tgt)
            log("info", "transform", f"Renamed {src} -> {tgt}")

        if c["src_dtype"] != c["tgt_dtype"]:
            out = out.withColumn(tgt, F.col(tgt).cast(c["tgt_dtype"]))
            log("info", "datatype_cast",
                f"Casted {tgt}", new_type=c["tgt_dtype"])

    return out
