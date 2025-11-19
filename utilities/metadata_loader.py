import json
import os
from utilities.logger import log

def load_metadata(local_file="config/sample_metadata.json"):
    try:
        here = os.path.dirname(os.path.abspath(__file__))
        metadata_path = os.path.abspath(os.path.join(here, "..", local_file))

        log("info", "metadata", f"Loading metadata from {metadata_path}")

        with open(metadata_path, "r") as f:
            meta = json.load(f)

        return meta["ds"]

    except Exception as e:
        log("error", "metadata", f"Metadata load failed: {e}")
        raise
