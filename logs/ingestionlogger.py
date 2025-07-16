import os
import json

def create_ingestion_log(log_file_path, log_data):
    os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
    with open(log_file_path, 'w') as f:
        json.dump(log_data, f, indent=2)
    print(f"Log saved to {log_file_path}")

def update_incremental_log(log_file_path, latest_modified, count):
    if os.path.exists(log_file_path):
        with open(log_file_path, 'r') as f:
            log_data = json.load(f)
    else:
        log_data = {}

    history_entry = {
        "timestamp": latest_modified,
        "extracted_count": count
    }

    log_data["last_ingested"] = latest_modified
    if "history" not in log_data:
        log_data["history"] = []
    log_data["history"].append(history_entry)

    with open(log_file_path, 'w') as f:
        json.dump(log_data, f, indent=2)
    print(f"Updated log with new last_ingested: {latest_modified}")
