import os
import logging
import json
from datetime import datetime
from urllib.parse import urlparse

def setup_logging_for_url(url):
    """
    Creates log directory with timestamp and sets up logging inside it.
    Returns the full directory path.
    """
    parsed_url = urlparse(url)
    resource_name = parsed_url.path.strip("/").split("/")[-1] or "default"
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    logs_dir = os.path.join("logs", "extraction", resource_name, timestamp)
    os.makedirs(logs_dir, exist_ok=True)
    log_file_path = os.path.join(logs_dir, 'extraction.log')
    logging.basicConfig(
        filename=log_file_path,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)

    logging.info(f"Logging initialized in {logs_dir}")
    return logs_dir

def save_extraction_metadata(logs_dir, resource_name, total_count, extraction_date):
    """
    Saves metadata about the extraction to a JSON file with historical record.
    """
    metadata_path = os.path.join(logs_dir, f"{resource_name}_metadata.json")
    metadata = {
        "extraction_date": extraction_date,
        "count": total_count,
        "history": [],
        "last_extracted": extraction_date
    }

    # Try to load existing metadata to append to history
    try:
        if os.path.exists(metadata_path):
            with open(metadata_path, "r", encoding="utf-8") as f:
                existing_metadata = json.load(f)
                history = existing_metadata.get("history", [])
                if "extraction_date" in existing_metadata and "count" in existing_metadata:
                    history.append({
                        "extraction_date": existing_metadata["extraction_date"],
                        "count": existing_metadata["count"]
                    })
                metadata["history"] = history
    except Exception as e:
        logging.warning(f"Failed to read existing metadata to preserve history: {e}")

    try:
        with open(metadata_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, ensure_ascii=False, indent=4)
        logging.info(f"Metadata saved to {metadata_path}")
    except Exception as e:
        logging.error(f"Error while saving metadata: {e}")


def save_extraction_outputs(data, logs_dir, resource_name):
    """
    Saves data to .json files, sorted files, and count to txt.
    """
    try:
        all_path = os.path.join(logs_dir, f"{resource_name}.json")
        with open(all_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        logging.info(f"Saved all invoices to {all_path}")

        sorted_by_modified = sorted(data, key=lambda x: x.get('modifiedDateEnd', ''))
        mod_path = os.path.join(logs_dir, f"{resource_name}modifieddate.json")
        with open(mod_path, "w", encoding="utf-8") as f:
            json.dump(sorted_by_modified, f, ensure_ascii=False, indent=4)
        logging.info(f"Saved sorted by modifiedDateEnd to {mod_path}")

        sorted_by_invoice = sorted(data, key=lambda x: x.get('invoiceNumber', ''))
        inv_path = os.path.join(logs_dir, f"{resource_name}sorted.json")
        with open(inv_path, "w", encoding="utf-8") as f:
            json.dump(sorted_by_invoice, f, ensure_ascii=False, indent=4)
        logging.info(f"Saved sorted by invoiceNumber to {inv_path}")

        count_path = os.path.join(logs_dir, f"{resource_name}_count.txt")
        with open(count_path, "w") as f:
            f.write(f"Total invoices fetched: {len(data)}\n")
        logging.info(f"Wrote count to {count_path}")

    except Exception as e:
        logging.error(f"Error while saving outputs: {e}")


def load_last_extraction_time(log_dir, resource_name):
    """
    Loads the last extraction timestamp from the metadata JSON file if available.
    """
    metadata_file = os.path.join(log_dir, f"{resource_name}_metadata.json")
    if os.path.exists(metadata_file):
        try:
            with open(metadata_file, 'r') as f:
                metadata = json.load(f)
            return metadata.get("last_extracted")
        except Exception as e:
            logging.warning(f"Failed to read last_extracted from metadata: {e}")
    return None


def save_last_extraction_time(log_dir, end_time_str):
    """
    Saves the timestamp of the last successful extraction to a JSON file.
    """
    metadata_file = os.path.join(log_dir, "last_successful_extraction.json")
    with open(metadata_file, 'w') as f:
        json.dump({"last_extracted_end": end_time_str}, f)
