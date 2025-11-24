import json
import os
import boto3

from app.custom.utilities.logger import log


def load_metadata(path: str) -> dict:
    """
    Loads metadata from either local file or S3 path.

    :param path: Local path or S3 URI
                 e.g. "config/VGI_FIN_GL_LOB_DH_V.json"
                 or   "s3://bucket/app/custom/config/file.json"
    :return: metadata["ds"] dictionary with normalized domicile
    """
    try:
        # ---------------------------------------------------------
        # S3 path
        # ---------------------------------------------------------
        if path.lower().startswith("s3://"):
            bucket, key = path.replace("s3://", "").split("/", 1)
            s3 = boto3.client("s3")
            log("info", "metadata", f"Loading metadata from S3: {bucket}/{key}")
            response = s3.get_object(Bucket=bucket, Key=key)
            meta = json.loads(response["Body"].read().decode("utf-8"))

        # ---------------------------------------------------------
        # Local file packaged in the ZIP
        # ---------------------------------------------------------
        else:
            here = os.path.dirname(os.path.abspath(__file__))
            metadata_path = os.path.abspath(os.path.join(here, "..", path))
            log("info", "metadata", f"Loading metadata from {metadata_path}")
            with open(metadata_path, "r") as f:
                meta = json.load(f)

        ds = meta["ds"]

        # ---------------------------------------------------------
        # Normalize domicile at dataset level
        # default → "global"
        # ---------------------------------------------------------
        domicile = (ds.get("domicile") or "").strip().lower()
        if domicile in ["", "null", "none"]:
            domicile = "global"

        ds["domicile"] = domicile

        log(
            "info",
            "metadata",
            "Metadata loaded",
            dataset=ds.get("nm"),
            version=ds.get("ver"),
            domicile=domicile,
        )

        return ds

    except Exception as e:
        log("error", "metadata", f"Metadata load failed: {e}")
        raise
