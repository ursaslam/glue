import pyspark.sql.functions as F
from utilities.logger import log

def apply_sensitivity(df, cols):
    """
    Apply masking based on column-level sensitivity.
    """
    out = df

    for col in cols:
        colname = col["tgt_nm"]
        sens = col.get("sensitivity", "none").lower()

        if colname not in out.columns:
            continue

        if sens == "none":
            continue

        if sens == "pii":
            out = out.withColumn(colname, F.lit("********"))
        elif sens == "phi":
            out = out.withColumn(colname, F.lit("#####"))
        elif sens == "confidential":
            out = out.withColumn(colname, F.lit("XXX-CONFIDENTIAL-XXX"))

        log("info", "sensitivity", f"Applied sensitivity mask to {colname}", sensitivity=sens)

    return out
