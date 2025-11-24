import json
import os
import boto3

from app.custom.utilities.logger import log  # adjust if your package root is different


def load_metadata(path: str, sys_level: str | None = None) -> dict:
    """
    Load metadata from either local file or S3 path.

    :param path:      Local path OR S3 URI (e.g. "config/FIN_GL.json"
                      or "s3://bucket/app/custom/config/FIN_GL.json")
    :param sys_level: Optional environment value ("eng", "test", "prod" etc.)
                      used to replace <sysLevel> placeholders in prefixes.
    :return:          The `ds` dictionary from the metadata JSON with
                      normalized domicile and optional prefix substitution.
    """
    try:
        # ----------------------------------------------------------
        # 1) Load raw JSON (S3 or local)
        # ----------------------------------------------------------
        if path.lower().startswith("s3://"):
            # ----- Read from S3 -----
            bucket, key = path.replace("s3://", "", 1).split("/", 1)
            s3 = boto3.client("s3")
            log("info", "metadata", f"Loading metadata from S3: {bucket}/{key}")
            response = s3.get_object(Bucket=bucket, Key=key)
            meta = json.loads(response["Body"].read().decode("utf-8"))
        else:
            # ----- Read from local file -----
            here = os.path.dirname(os.path.abspath(__file__))
            metadata_path = os.path.abspath(os.path.join(here, "..", path))
            log("info", "metadata", f"Loading metadata from {metadata_path}")
            with open(metadata_path, "r") as f:
                meta = json.load(f)

        ds = meta["ds"]

        # ----------------------------------------------------------
        # 2) Normalize domicile at dataset level
        # ----------------------------------------------------------
        domicile_raw = ds.get("domicile", "")
        # Handle empty / null-like values
        dom_norm = str(domicile_raw).strip().lower() if domicile_raw is not None else ""
        if dom_norm in ("", "null", "none"):
            domicile = "global"
        else:
            # keep original (but as string)
            domicile = str(domicile_raw).strip()

        ds["domicile"] = domicile
        log(
            "info",
            "metadata",
            "Domicile normalized",
            raw_value=domicile_raw,
            normalized_value=domicile,
        )

        # ----------------------------------------------------------
        # 3) Replace <sysLevel> placeholders in prefixes (if provided)
        # ----------------------------------------------------------
        if sys_level:
            for key in ("src_prefix", "tgt_prefix"):
                val = ds.get(key)
                if isinstance(val, str) and "<sysLevel>" in val:
                    new_val = val.replace("<sysLevel>", sys_level)
                    ds[key] = new_val
                    log(
                        "info",
                        "metadata",
                        f"Replaced <sysLevel> in {key}",
                        old=val,
                        new=new_val,
                        sys_level=sys_level,
                    )

        # ----------------------------------------------------------
        # 4) Normalize sensitivity at column level (empty or value)
        #     - If missing/empty -> "NONE"
        #     - Else keep as-is (upper-cased for consistency)
        # ----------------------------------------------------------
        cols = ds.get("cols", [])
        for col_meta in cols:
            raw_sens = col_meta.get("sensitivity", "")
            if raw_sens is None or str(raw_sens).strip() == "":
                norm_sens = "NONE"
            else:
                norm_sens = str(raw_sens).strip().upper()
            col_meta["sensitivity"] = norm_sens

        return ds

    except Exception as e:
        log("error", "metadata", f"Metadata load failed: {e}")
        raise
