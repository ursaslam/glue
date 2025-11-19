import pyspark.sql.functions as F
from utilities.logger import log

def apply_dq_rules(df, cols):
    valid = df
    reject = None

    for c in cols:
        col = c["tgt_nm"]
        nullable = c.get("nullable", True)
        regex = c.get("regex") or ""

        # Null checks
        if not nullable:
            bad = valid.filter(F.col(col).isNull())
            if bad.count() > 0:
                log("warn", "dq_null", f"{col} has nulls", rows=bad.count())
                reject = bad if reject is None else reject.union(bad)
            valid = valid.filter(F.col(col).isNotNull())

        # Regex
        if regex:
            bad = valid.filter(~F.col(col).rlike(regex))
            if bad.count() > 0:
                log("warn", "dq_regex", f"{col} regex failed", rows=bad.count())
                reject = bad if reject is None else reject.union(bad)
            valid = valid.filter(F.col(col).rlike(regex))

    return valid, reject
