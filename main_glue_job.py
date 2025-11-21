def dedupe(df, id_cols, upd_cols):
    # Clean out empty strings
    clean_id_cols = [c for c in id_cols if c and c.strip()]
    clean_upd_cols = [c for c in upd_cols if c and c.strip()]

    # No dedupe needed
    if not clean_id_cols or not clean_upd_cols:
        log("warn", "dedupe", "Skipping dedupe due to missing id/upd cols")
        return df

    log("info", "dedupe", f"Running dedupe using id_cols={clean_id_cols}, upd_cols={clean_upd_cols}")

    w = Window.partitionBy(*clean_id_cols).orderBy(
            *[F.col(c).desc() for c in clean_upd_cols]
        )

    return (
        df.withColumn("rn", F.row_number().over(w))
          .filter("rn = 1")
          .drop("rn")
    )