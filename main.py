from src.extractors.accounts.Accounts_Payable_Invoices_API.List_AP_Invoices import extract_list_ap_invoices
from src.extractors.accounts.Accounts_Payable_Payments_API.List_AP_Payments import extract_list_ap_payments
from src.extractors.accounts.Accounts_Receivable_Deposits_API.List_AR_Deposits import extract_list_ar_deposits
from src.extractors.accounts.Accounts_Receivable_Invoices_API.LIST_AR_Invoice import extract_list_ar_invoices
import config
import concurrent.futures
import json,os

def get_common_headers():
    return {
        'X-Plex-Connect-Api-Key': config.PLEX_API_KEY,
        'X-Plex-Connect-Api-Secret': config.X_PLEX_CONNECT_API_SECRET,
        'X-Plex-Connect-Customer-Id': config.X_PlEX_XPLEX_CONNECT_CUSTOMER_ID,
        'X-Plex-Connect-Tenant-Id': config.X_PlEX_XPLEX_CONNECT_CUSTOMER_ID,
    }

def run_all_extractions():
    headers = get_common_headers()
    print("Starting extraction of AP Invoices, Payments, AR Deposits, and AR Invoices...")

    # Define tasks as tuples of (function, url)
    tasks = [
        (extract_list_ap_invoices, 'https://connect.plex.com/accounting/v1/ap-invoices'),
        (extract_list_ap_payments, 'https://connect.plex.com/accounting/v1/ap-payments'),
        (extract_list_ar_deposits, 'https://connect.plex.com/accounting/v1/ar-deposits'),
        (extract_list_ar_invoices, 'https://connect.plex.com/accounting/v1/ar-invoices')
    ]

    results = {}
    output_dir = "extraction_json_outputs"
    os.makedirs(output_dir, exist_ok=True)

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        future_to_task = {
            executor.submit(func, headers=headers, url=url): (func.__name__, url)
            for func, url in tasks
        }

        for future in concurrent.futures.as_completed(future_to_task):
            func_name, url = future_to_task[future]
            try:
                data = future.result()
                results[func_name] = data
                print(f"{func_name} extracted {len(data)} records.")

                # Save each result to its own JSON file
                filename = os.path.join(output_dir, f"{func_name}.json")
                with open(filename, "w") as f:
                    json.dump(data, f, indent=4)
                print(f"Saved results to {filename}")
            except Exception as e:
                print(f"Error running {func_name}: {e}")
    return results


if __name__ == "__main__":
    run_all_extractions()

    