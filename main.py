import os
from dotenv import load_dotenv
from src.extractors.supplier_API.list_suppliers import fetch_and_upload_if_changed

load_dotenv()

def get_common_headers():
    return {
        'X-Plex-Connect-Api-Key': os.environ.get("X_PLEX_CONNECT_API_KEY"),
        'X-Plex-Connect-Api-Secret': os.environ.get("X_PLEX_CONNECT_API_SECRET"),
        'X-Plex-Connect-Customer-Id': os.environ.get("X_PLEX_CONNECT_CUSTOMER_ID"),
        # 'X-Plex-Connect-Tenant-Id': os.environ.get("X_PLEX_CONNECT_TENANT_ID"),
    }
# def get_part_id():
#     parts_data=fetch_and_upload_if_changed(
#         api_url="https://connect.plex.com/mdm/v1/parts",
#         cos_filename="parts"
#     )
#     part_ids = [part['id'] for part in parts_data] if parts_data else []
#     bom_urls=[f"https://connect.plex.com/mdm/v1/boms?partId={part_id}" for part_id in part_ids]
#     return bom_urls
def get_bom_urls(parts_data):
    return [f"https://connect.plex.com/mdm/v1/boms?partId={part['id']}" for part in parts_data] if parts_data else []
DATA_SOURCES = [
    ("suppliers", "https://connect.plex.com/mdm/v1/suppliers"),
    ("customers", "https://connect.plex.com/mdm/v1/customers"),
    ("edi-units", "https://connect.plex.com/edi/v1/units"),
    ("buildings", "https://connect.plex.com/mdm/v1/buildings"),
    ("contacts", "https://connect.plex.com/mdm/v1/contacts"),
    ("employees", "https://connect.plex.com/mdm/v1/employees"),
    ("charts-of-accounts", "https://connect.plex.com/accounting/v1/charts-of-accounts"),
    ("Fiscal-Periods-Statuses","https://connect.plex.com/accounting/v1/fiscal-period-statuses"),
    ("Charts-of-Accounts","https://connect.plex.com/mdm/v1/chart-of-accounts"),
    ("parts", "https://connect.plex.com/mdm/v1/parts"),
    ("BOM",'{bom_urls}')
]

def run_all_extractions():
    headers = get_common_headers()
    print(f"Headers being used: {headers}\n")
    results = {}
    parts_data=None
    for name, url in DATA_SOURCES:
        #skipping Bom for now
        if name == "BOM":
            continue
        try:
            data = fetch_and_upload_if_changed(api_url=url, cos_filename=name,headers=headers)
            results[name] = data
            if name=="parts":
                parts_data = data
        except Exception as e:
            print(f"Error processing {name}: {e}")
            results[name] = None
    if parts_data:
        bom_urls= get_bom_urls(parts_data)
        for bom_url in bom_urls:
            fetch_and_upload_if_changed(api_url=bom_url,cos_filename="bom")
    any_data = False
    for name, data in results.items():
        if data:
            print(f"Number of {name}: {len(data)}")
            any_data = True
    if not any_data:
        print("****Up-to-date! No new data found.****")

if __name__ == "__main__":
    run_all_extractions()

    