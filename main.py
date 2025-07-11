# from src.extractors.Accounts_Payable_Invoices_API.List_AP_Invoices import extract_list_ap_invoices
# from src.extractors.Accounts_Payable_Payments_API.List_AP_Payments import extract_list_ap_payments
from src.extractors.supplier_API.list_suppliers import fetch_and_upload_if_changed
# from src.uploaders.s3_uploader import upload_to_s3
# import config
import  os
from dotenv import load_dotenv
load_dotenv()
def get_common_headers():
    return {
        'X-Plex-Connect-Api-Key': os.environ.get("X_PLEX_CONNECT_API_KEY"),
        'X-Plex-Connect-Api-Secret': os.environ.get("X_PLEX_CONNECT_API_SECRET"),
        'X-Plex-Connect-Customer-Id': os.environ.get("X_PLEX_CONNECT_CUSTOMER_ID")
        # 'X-Plex-Connect-Tenant-Id': os.environ.get("X_PLEX_CONNECT_TENANT_ID"),
    }

def run_all_extractions():
    # Extract data from Get_AP_Invoices (existing)
    headers = get_common_headers()
    print(f"header being used {headers}")
    # ap_invoices_data = extract_list_ap_invoices(headers=headers,url = 'https://connect.plex.com/accounting/v1/ap-invoices')
    # print("Extracted AP Invoices Data:", ap_invoices_data)  # Debugging line
    # ap_payemnts_data = fetch_and_upload_if_changed(headers=headers,api_url = 'https://connect.plex.com/accounting/v1/ap-payments',cos_filename="ap-payments")
    suppliers_data=fetch_and_upload_if_changed(api_url="https://connect.plex.com/mdm/v1/suppliers",cos_filename="suppliers",headers=headers)
    #count the number of invoices
    # print("Extracted AP Payments Data:", ap_payemnts_data)  # Debugging line
    # print(f"Number of AP Invoices: {len(ap_invoices_data)}")
    # print(f"Number of AP Payments: {len(ap_payemnts_data)}")   
    if  suppliers_data: 
        print(f"Number of suppliers: {len(suppliers_data)}")
    else:
        print(f"****Error Occur vayo!!!! Problem Problem****")
    
    # upload_to_s3(ap_invoices_data, "ap-invoices.json")

if __name__ == "__main__":
    # get_common_headers()
    run_all_extractions()