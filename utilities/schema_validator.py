from utilities.logger import log

def validate_schema(df, cols, dq_rules):
    if not dq_rules.get("check_schema", True):
        log("info", "schema_validation", "Schema validation disabled")
        return

    allow_extra = dq_rules.get("allow_extra_columns", True)
    allow_missing = dq_rules.get("allow_missing_columns", False)

    src_cols = set(df.columns)
    expected = set(c["src_nm"] for c in cols)

    missing = expected - src_cols
    extra = src_cols - expected

    if missing:
        log("warn" if allow_missing else "error",
            "schema_validation",
            "Missing columns", missing=list(missing))

    if extra:
        log("info" if allow_extra else "warn",
            "schema_validation",
            "Extra columns", extra=list(extra))

    log("info", "schema_validation", "Schema validation done")
