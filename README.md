

from app.custom.layers.rejects_table_manager import create_rejects_glue_table

if df_reject is not None and rejected_count > 0:

    reject_path = f"s3://{raw_bucket}/{data_domain}/rejects/{table_name}/"

    # ---------------------------------------------------------
    # Log start of reject write
    # ---------------------------------------------------------
    log_event(
        "rejects",
        "Starting reject write",
        path=reject_path,
        rows=rejected_count,
        table=table_name
    )

    try:
        # ---------------------------------------------------------
        # Write reject dataframe to Parquet
        # ---------------------------------------------------------
        df_reject.write.mode("overwrite").parquet(reject_path)

        log_event(
            "rejects",
            "Reject records written to S3",
            path=reject_path,
            rows=rejected_count,
        )

        # ---------------------------------------------------------
        # Drop + Create Glue table on top of reject path
        # ---------------------------------------------------------
        create_rejects_glue_table(
            database=f"{data_domain}_rejects",
            table_name=f"{table_name}_rejects",
            location=reject_path,
            df=df_reject
        )

        log_event(
            "rejects",
            "Rejects Glue table dropped and created",
            database=f"{data_domain}_rejects",
            table=f"{table_name}_rejects",
            path=reject_path
        )

    except Exception as e:
        # ---------------------------------------------------------
        # Log any failure during reject write
        # ---------------------------------------------------------
        log_event(
            "error",
            "Reject write failed",
            error=str(e),
            path=reject_path,
            table=table_name,
        )
        raise
