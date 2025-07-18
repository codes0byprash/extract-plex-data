from src.extractors.accounts.Accounts_Payable_Invoices_API.List_AP_Invoices import extract_list_ap_invoices
# from src.extractors.accounts.Accounts_Payable_Payments_API.List_AP_Payments import extract_list_ap_payments
# from src.extractors.accounts.Accounts_Receivable_Deposits_API.List_AR_Deposits import extract_list_ar_deposits
# from src.extractors.accounts.Accounts_Receivable_Invoices_API.LIST_AR_Invoice import extract_list_ar_invoices
import os
from datetime import datetime, timezone
from logs.ingestionlogger import log_ingestion_status
from src.s3uploader.upload_to_s3 import logs_upload_to_s3, fetch_and_upload
import config
import concurrent.futures
import json,os
from urllib.parse import urlparse

def get_common_headers():
    return {
        'X-Plex-Connect-Api-Key': config.X_PLEX_CONNECT_API_KEY,
        'X-Plex-Connect-Api-Secret': config.X_PLEX_CONNECT_API_SECRET,
        'X-Plex-Connect-Customer-Id': config.X_PLEX_CONNECT_CUSTOMER_ID,
        'X-Plex-Connect-Tenant-Id': config.X_PLEX_CONNECT_TENANT_ID,
    }

def run_all_extractions():
    headers = get_common_headers()
    print("Starting extraction of AP Invoices, Payments, AR Deposits, and AR Invoices...")
    print(f"path of the file",os.path.join(os.path.dirname(__file__), 'src','s3uploader'))

    # Define tasks as tuples of (function, url)
    tasks = [
        (extract_list_ap_invoices, 'https://connect.plex.com/accounting/v1/ap-invoices')
        #(extract_list_ap_payments, 'https://connect.plex.com/accounting/v1/ap-payments'),
       # (extract_list_ar_deposits, 'https://connect.plex.com/accounting/v1/ar-deposits'),
        #(extract_list_ar_invoices, 'https://connect.plex.com/accounting/v1/ar-invoices')
    ]

    results = {}
    output_dir = "extraction_json_outputs"
    os.makedirs(output_dir, exist_ok=True)

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        future_to_url = {
            executor.submit(func, headers=headers, url=url): url
            for func, url in tasks
        }

        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
            try:
                data = future.result()
                parsed_url = urlparse(url)
                resource_name = parsed_url.path.strip("/").split("/")[-1] or "default"
                results[resource_name] = data
            except Exception as e:
                print(f"Error running extraction for {url}: {e}")
    # print(" Seen results:",results)
    return results

if __name__ == "__main__":
    results = run_all_extractions()
    print("Extraction completed. Results:")

    for resource_name, data in results.items():
        print(f"{resource_name}: {len(data)} records")
        log_file_path = os.path.join("logs", "ingestion_logs", resource_name, f"{resource_name}.json")

        try:
            fetch_and_upload(data, resource_name, log_file_path)
        except Exception as e:
            print(f"Plex data upload failed for {resource_name}. Skipping log upload to COS.")
            log_ingestion_status(log_file_path, datetime.now(timezone.utc), "400", str(e))
            continue

        try:
            logs_upload_to_s3( resource_name, log_file_path)
        except Exception as e:
            print(f"Log upload to COS failed for {resource_name}: {e}")
            # log_ingestion_status(log_file_path, datetime.now(timezone.utc), "400", str(e))

    print("All extractions completed.")
