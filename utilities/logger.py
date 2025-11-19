import json
from datetime import datetime

def log(level, stage, message, **details):
    entry = {
        "timestamp": datetime.utcnow().isoformat(),
        "level": level.upper(),
        "stage": stage,
        "message": message,
        "details": details
    }
    print(json.dumps(entry))
