from src.extractors.accounts.Accounts_Payable_Invoices_API.List_AP_Invoices import extract_list_ap_invoices
from src.extractors.accounts.Accounts_Payable_Payments_API.List_AP_Payments import extract_list_ap_payments
from src.extractors.accounts.Accounts_Receivable_Deposits_API.List_AR_Deposits import extract_list_ar_deposits
from src.extractors.accounts.Accounts_Receivable_Invoices_API.LIST_AR_Invoice import extract_list_ar_invoices
import config
import concurrent.futures

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
        # (extract_list_ap_invoices, 'https://connect.plex.com/accounting/v1/ap-invoices'),
        # (extract_list_ap_payments, 'https://connect.plex.com/accounting/v1/ap-payments'),
        (extract_list_ar_deposits, 'https://connect.plex.com/accounting/v1/ar-deposits')
        # (extract_list_ar_invoices, 'https://connect.plex.com/accounting/v1/ar-invoices')
    ]

    results = {}

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        # Submit all tasks to the executor
        future_to_task = {
            executor.submit(func, headers=headers, url=url): func.__name__
            for func, url in tasks
        }

        # As each task completes, gather the result or exception
        for future in concurrent.futures.as_completed(future_to_task):
            func_name = future_to_task[future]
            try:
                data = future.result()
                results[func_name] = data
                print(f"{func_name} extracted {len(data)} records.")
            except Exception as e:
                print(f"Error running {func_name}: {e}")

    # upload or further process the data here
    # upload_to_s3(results['extract_list_ap_invoices'], "ap-invoices.json")

    return results


if __name__ == "__main__":
    run_all_extractions()

    