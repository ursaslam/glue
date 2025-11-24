import json
import os
import boto3
from app.custom.utilities.logger import log     # Keep your current import structure


def normalize_value(val):
    """
    Converts blank/null/None/"none"/"null" into empty string.
    """
    if val is None:
        return ""
    if isinstance(val, str) and val.strip().lower() in ["", "none", "null", "na"]:
        return ""
    return val


def load_metadata(path, sys_level=None):
    """
    Loads metadata JSON from either local path or S3.
    Also applies defaulting rules:
      - domicile default = "global"
      - replaces ${SYS_LEVEL} inside prefixes (src_prefix, tgt_prefix)
    """

    try:
        # ------------------------------------------------------------------
        # 1. READ METADATA (local or S3)
        # ------------------------------------------------------------------
        if path.lower().startswith("s3://"):
            bucket, key = path.replace("s3://", "").split("/", 1)
            s3 = boto3.client("s3")

            log("info", "metadata", f"Loading metadata from S3: {bucket}/{key}")
            response = s3.get_object(Bucket=bucket, Key=key)
            meta = json.loads(response["Body"].read().decode("utf-8"))
        else:
            # Local file load
            here = os.path.dirname(os.path.abspath(__file__))
            metadata_path = os.path.abspath(os.path.join(here, "..", path))

            log("info", "metadata", f"Loading metadata from local: {metadata_path}")
            with open(metadata_path, "r") as f:
                meta = json.load(f)

        ds = meta["ds"]

        # ------------------------------------------------------------------
        # 2. NORMALIZE INPUTS
        # ------------------------------------------------------------------
        domicile_raw = ds.get("domicile")
        domicile_norm = normalize_value(domicile_raw)

        # ------------------------------------------------------------------
        # 3. DOMICILE LOGIC
        # ------------------------------------------------------------------
        if domicile_norm in ["", None]:
            domicile_norm = "global"

        ds["domicile"] = domicile_norm  # set final value

        log("info", "metadata", f"Domicile normalized to: {ds['domicile']}")

        # ------------------------------------------------------------------
        # 4. SYS_LEVEL REPLACEMENT IN PREFIXES
        # ------------------------------------------------------------------
        if sys_level:
            for prefix_field in ["src_prefix", "tgt_prefix"]:
                if prefix_field in ds and isinstance(ds[prefix_field], str):
                    ds[prefix_field] = (
                        ds[prefix_field]
                        .replace("${SYS_LEVEL}", sys_level)
                        .replace("<sysLevel>", sys_level)
                    )

            log("info", "metadata", f"SYS_LEVEL applied to prefixes: {sys_level}")

        # ------------------------------------------------------------------
        # 5. NORMALIZE sensitivity for each column
        # ------------------------------------------------------------------
        columns = ds.get("cols", [])
        for col in columns:
            raw_sens = col.get("sensitivity")
            normalized_sens = normalize_value(raw_sens)

            # If blank → default sensitivity
            col["sensitivity"] = normalized_sens if normalized_sens else "none"

        log("info", "metadata", "Sensitivity normalized for all columns")

        return ds

    except Exception as e:
        log("error", "metadata", f"Metadata load failed: {e}")
        raise
