import os
import json

def ensure_log_dir():
    os.makedirs("logs/extraction/", exist_ok=True)

def write_initial_log(cos_filename, ingestion_date, count, status_code, reason):
    ensure_log_dir()
    log_data = {
        "ingestion_date": ingestion_date,
        "count": count,
        "response status": status_code,
        "response mesage": reason,
        "last_ingested": ingestion_date,
        "history": [
            {
                "timestamp": ingestion_date,
                "extracted_count": count
            }
        ]
    }
    with open(f"logs/extraction/{cos_filename}.json", "w") as f:
        json.dump(log_data, f, indent=2)

def update_incremental_log(cos_filename, new_last_ingested, count):
    log_file = f"logs/extraction/{cos_filename}.json"
    with open(log_file, 'r') as f:
        log_data = json.load(f)
    ingestion_record = {
        "timestamp": new_last_ingested,
        "extracted_count": count
    }
    if "history" not in log_data:
        log_data["history"] = []
    log_data["history"].append(ingestion_record)
    log_data["last_ingested"] = new_last_ingested
    with open(log_file, 'w') as f:
        json.dump(log_data, f, indent=2)