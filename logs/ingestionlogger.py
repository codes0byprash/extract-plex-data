# from datetime import datetime
# import os, json

# def log_ingestion_status(log_file_path, timestamp, status_code, error_message=None):
#     dir_path = os.path.dirname(log_file_path)
#     if not dir_path:
#         raise ValueError("log_file_path must include a directory path.")

#     os.makedirs(dir_path, exist_ok=True)
#     try:
#         if os.path.exists(log_file_path):
#             with open(log_file_path, 'r') as f:
#                 log_data = json.load(f)
#         else:
#                 log_data = {}
#     except json.JSONDecodeError:
#             log_data = {}
#     if "history" not in log_data or not isinstance(log_data["history"], list):
#         log_data["history"] = []
#     if isinstance(timestamp, datetime):
#             timestamp = timestamp.isoformat()

#     log_entry = {
#             "timestamp": timestamp,
#             "status_code": status_code
#         }

#     if error_message:
#         log_entry["error_message"] = error_message
#     log_data["history"].append(log_entry)
#     with open(log_file_path, 'w') as f:
#         json.dump(log_data, f, indent=4)

#     return log_data

import os
import json
import datetime

def log_ingestion_status(log_file_path, timestamp, status_code, error_message=None):
    dir_path = os.path.dirname(log_file_path)
    os.makedirs(dir_path, exist_ok=True)

    if os.path.exists(log_file_path):
        try:
            with open(log_file_path, 'r') as f:
                log_data = json.load(f)
        except json.JSONDecodeError:
            log_data = {}
    else:
        log_data = {}

    if "history" not in log_data or not isinstance(log_data["history"], list):
        log_data["history"] = []

    if isinstance(timestamp, datetime.datetime):
        timestamp = timestamp.isoformat()

    log_entry = {
        "timestamp": timestamp,
        "status_code": status_code
    }
    if error_message:
        log_entry["error_message"] = error_message

    log_data["history"].append(log_entry)

    with open(log_file_path, 'w') as f:
        json.dump(log_data, f, indent=4)

    print(f"Appended log to {log_file_path}:")
    print(json.dumps(log_entry, indent=2))

    return log_data
