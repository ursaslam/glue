import pyspark.sql.functions as F
from app.custom.utilities.logger import log_event


def apply_dq_rules(df, cols):
    """
    SAFE DQ CHECKS:
    - No cascading filters
    - All fails flagged first
    - Valid + reject separated at the end
    """

    df2 = df

    # add per-column flag fields
    for c in cols:
        col = c["tgt_nm"]
        nullable = c.get("nullable", True)
        regex = (c.get("regex") or "").strip()

        # NULL FAIL
        df2 = df2.withColumn(
            f"{col}_null_fail",
            F.when((~nullable) & F.col(col).isNull(), 1).otherwise(0)
        )

        # REGEX FAIL
        df2 = df2.withColumn(
            f"{col}_regex_fail",
            F.when((regex != "") & (~F.col(col).rlike(regex)), 1).otherwise(0)
        )

    # Combine into single FAIL flag
    fail_expr = " OR ".join(
        [
            f"{c['tgt_nm']}_null_fail = 1 OR {c['tgt_nm']}_regex_fail = 1"
            for c in cols
        ]
    )

    df2 = df2.withColumn("dq_fail", F.expr(fail_expr))

    df_valid = df2.filter("dq_fail = 0")
    df_reject = df2.filter("dq_fail = 1")

    # Log summary
    log_event("dq_summary",
              f"DQ VALID: {df_valid.count()} | REJECT: {df_reject.count()}")

    # Cleanup flags
    drop_cols = []
    for c in cols:
        drop_cols.append(f"{c['tgt_nm']}_null_fail")
        drop_cols.append(f"{c['tgt_nm']}_regex_fail")

    df_valid = df_valid.drop("dq_fail", *drop_cols)
    df_reject = df_reject.drop("dq_fail", *drop_cols)

    return df_valid, df_reject
