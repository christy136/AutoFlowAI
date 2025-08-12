import json, os
from datetime import datetime

def log_error(reason, error_type, data):
    os.makedirs("logs", exist_ok=True)
    log_file = "logs/error_log.jsonl"
    log_entry = {
        "timestamp": datetime.utcnow().isoformat(),
        "reason": reason,
        "type": error_type,
        "data": data
    }
    with open(log_file, "a") as f:
        f.write(json.dumps(log_entry) + "\n")
