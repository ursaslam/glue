

def merge_into_iceberg(
    spark,
    df: DataFrame,
    catalog: str,
    database: str,
    table: str,
    id_cols: List[str],
    upd_cols: List[str],
    domicile_value: str
) -> None:
    """
    Incremental Load:
    1. If Iceberg table does NOT exist → CREATE automatically.
    2. Then perform MERGE using id_cols + domicile.
    """

    target = f"{catalog}.{database}.{table}"

    # ==========================================================
    # STEP 1 — CREATE TABLE IF NOT EXISTS
    # ==========================================================
    log_event("iceberg_create_check", f"Checking if table exists: {target}")

    if not spark.catalog.tableExists(f"{database}.{table}"):
        log_event("iceberg_create_missing", f"Table missing — creating: {target}")

        # Ensure domicile column exists in DF
        df = _ensure_domicile_column(df, domicile_value)

        df.createOrReplaceTempView("incoming_inc")

        # ---------------------------
        # CREATE ICEBERG TABLE
        # ---------------------------
        create_sql = f"""
        CREATE TABLE {target}
        USING iceberg
        PARTITIONED BY (domicile)
        AS SELECT * FROM incoming_inc
        """

        spark.sql(create_sql)

        log_event("iceberg_create_done", f"Created Iceberg table: {target}")
        return
    else:
        log_event("iceberg_exists", f"Table already exists: {target}")

    # ==========================================================
    # STEP 2 — MERGE LOGIC (only if table exists)
    # ==========================================================

    df = _ensure_domicile_column(df, domicile_value)
    df.createOrReplaceTempView("incoming_inc")

    # Build merge keys
    merge_keys = list(id_cols or [])
    if "domicile" not in [k.lower() for k in merge_keys]:
        merge_keys.append("domicile")

    if not merge_keys:
        raise ValueError("Iceberg merge requires at least one id column")

    merge_condition = " AND ".join([f"t.{c} = s.{c}" for c in merge_keys])

    # Compare update timestamp columns to determine if newer record should update
    if upd_cols:
        update_condition = " OR ".join([f"s.{c} > t.{c}" for c in upd_cols])

        merge_sql = f"""
        MERGE INTO {target} t
        USING incoming_inc s
        ON {merge_condition}
        WHEN MATCHED AND ({update_condition}) THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """

    else:
        merge_sql = f"""
        MERGE INTO {target} t
        USING incoming_inc s
        ON {merge_condition}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """

    log_event("iceberg_merge_sql", merge_sql)
    spark.sql(merge_sql)

    log_event("iceberg_merge_done", f"Merge completed for {target}")