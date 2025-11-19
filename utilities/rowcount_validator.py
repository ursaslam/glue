from utilities.logger import log

def validate_rowcount(src_count, curated_count, dq_rules):
    if not dq_rules.get("check_row_count", True):
        log("info", "row_count", "Row count validation disabled")
        return

    rejected = src_count - curated_count

    log("info", "row_count",
        "Validation",
        source=src_count,
        curated=curated_count,
        rejected=rejected)

    if src_count != curated_count:
        log("warn", "row_count",
            "Mismatch detected",
            loss_pct=round((rejected/src_count)*100,2) if src_count else 0)
