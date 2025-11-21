
import json
import boto3
from urllib.parse import urlparse
from utilities.logger import log

def load_metadata(path):
    """
    Loads metadata either from local file or S3.
    Example accepted paths:
      - "custom/config/N.json"
      - "s3://bucket/config/N.json"
    """

    # Case 1: S3 path
    if path.startswith("s3://"):
        try:
            log("info", "metadata", f"Loading metadata from S3 → {path}")

            parsed = urlparse(path)
            bucket = parsed.netloc
            key = parsed.path.lstrip("/")

            s3 = boto3.client("s3")
            obj = s3.get_object(Bucket=bucket, Key=key)
            meta = json.loads(obj['Body'].read())

            return meta["ds"]

        except Exception as e:
            log("error", "metadata", f"S3 metadata load failed: {e}")
            raise

    # Case 2: Local file (Glue ZIP)
    else:
        try:
            log("info", "metadata", f"Loading local metadata file → {path}")

            with open(path, "r") as f:
                meta = json.load(f)

            return meta["ds"]

        except Exception as e:
            log("error", "metadata", f"Local metadata load failed: {e}")
            raise
