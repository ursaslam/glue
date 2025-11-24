from typing import List

import pyspark.sql.functions as F
from pyspark.sql.window import Window

from app.custom.utilities.logger import log


def dedupe(df, id_cols: List[str], upd_cols: List[str]):
    """
    Dedupe rows using ROW_NUMBER over id_cols ordered by upd_cols DESC.

    If either id_cols or upd_cols is empty → returns df unchanged.
    """
    if not id_cols or not upd_cols:
        log(
            "warn",
            "dedupe",
            "Skipping dedupe – id_cols or upd_cols empty",
            id_cols=id_cols,
            upd_cols=upd_cols,
        )
        return df

    log("info", "dedupe", "Running id-based dedupe", id_cols=id_cols, upd_cols=upd_cols)

    w = Window.partitionBy(*id_cols).orderBy(*[F.col(c).desc() for c in upd_cols])

    out = (
        df.withColumn("rn", F.row_number().over(w))
        .filter("rn = 1")
        .drop("rn")
    )

    log("info", "dedupe", "Dedupe completed", rows=out.count())
    return out
