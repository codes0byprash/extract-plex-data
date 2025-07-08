from src.extractors.Accounts_Payable_Invoices_API.List_AP_Invoices import extract_list_ap_invoices
from src.extractors.Accounts_Payable_Payments_API.List_AP_Payments import extract_list_ap_payments
# from src.uploaders.s3_uploader import upload_to_s3
import config

def get_common_headers():
    return {
        'X-Plex-Connect-Api-Key': config.PLEX_API_KEY,
        'X-Plex-Connect-Api-Secret': config.X_PLEX_CONNECT_API_SECRET,
        'X-Plex-Connect-Customer-Id': config.X_PlEX_XPLEX_CONNECT_CUSTOMER_ID,
        'X-Plex-Connect-Tenant-Id': config.X_PlEX_XPLEX_CONNECT_CUSTOMER_ID,
    }

def run_all_extractions():
    # Extract data from Get_AP_Invoices (existing)
    headers = get_common_headers()
    ap_invoices_data = extract_list_ap_invoices(headers=headers,url = 'https://connect.plex.com/accounting/v1/ap-invoices')
    # print("Extracted AP Invoices Data:", ap_invoices_data)  # Debugging line
    ap_payemnts_data = extract_list_ap_payments(headers=headers,url = 'https://connect.plex.com/accounting/v1/ap-payments')
    #count the number of invoices
    # print("Extracted AP Payments Data:", ap_payemnts_data)  # Debugging line
    print(f"Number of AP Invoices: {len(ap_invoices_data)}")
    print(f"Number of AP Payments: {len(ap_payemnts_data)}")    # # # Upload to S3
    
    # upload_to_s3(ap_invoices_data, "ap-invoices.json")

if __name__ == "__main__":
    get_common_headers()
    run_all_extractions()