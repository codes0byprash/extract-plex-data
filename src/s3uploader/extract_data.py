import json
import os
import time
# from dotenv import load_dotenv
import requests,datetime
from datetime import datetime, timezone
from logs.ingestionlogger import create_ingestion_log, update_incremental_log
import config

# Load environment variables from .env
# load_dotenv()
API_KEY = config.X_PLEX_CONNECT_API_KEY
X_PLEX_CONNECT_API_SECRET = config.X_PLEX_CONNECT_API_SECRET
X_PLEX_CONNECT_CUSTOMER_ID = config.X_PLEX_CONNECT_CUSTOMER_ID
X_PLEX_CONNECT_TENANT_ID = config.X_PLEX_CONNECT_TENANT_ID
headers = {
            'X-Plex-Connect-Api-Key': API_KEY,
            'X-Plex-Connect-Api-Secret': X_PLEX_CONNECT_API_SECRET,
            'X-Plex-Connect-Customer-Id': X_PLEX_CONNECT_CUSTOMER_ID,
            'X-Plex-Connect-Tenant-Id': X_PLEX_CONNECT_TENANT_ID
}
# Rate limiting setup (200 requests/minute = 1 request every 0.3s)
_last_request_time = 0
_min_interval = 0.3  
def throttle():
    global _last_request_time
    elapsed = time.time() - _last_request_time
    if elapsed < _min_interval:
        time.sleep(_min_interval - elapsed)
    _last_request_time = time.time()
def check_for_changes(api_url,cos_filename):
    """Check if there are any changes since last ingestion"""
    log_file = f"logs/ingestion_logs/{cos_filename}/{cos_filename}.json"
    if not os.path.exists(log_file):
        print(f"No previous ingestion log found for {cos_filename}. Will perform initial ingestion.")
        return True
    try:
        with open (log_file,'r') as f:
            log_data = json.load(f)
            last_ingested = log_data.get( "ingestion_date") or log_data.get("last_ingested")
            if not last_ingested:
                print(f"No last ingestion date found for {cos_filename}. Will perform full ingestion.")
                return True
            params={"modifiedDateBegin": last_ingested}
            print(f"checking for changes in {cos_filename} since {last_ingested}...")
            throttle()
            response= requests.get(api_url,headers =headers, params=params)
            response.raise_for_status()
            data = response.json()
            print(f"data: {data}")
            change_count = len(data) if data else 0
            if change_count> 0 :
                print(f"Found {change_count} changed records for {cos_filename}")
                return True
            else:
                print(f"No changes found for {cos_filename}")
                return False
    except Exception as e :
        print(f"Error checking for changes in {cos_filename}:{e}")
        print(f"Will proceed with ingestion as a safety measure.")
        return True 

def call_plex_api(api_url, cos_filename):
    throttle()
    log_file = f"logs/ingestion_logs/{cos_filename}/{cos_filename}.json"
    if not os.path.exists(log_file):
        return call_first_call(api_url, cos_filename,headers)
    else:
        return incremental_call(api_url, cos_filename,headers)

def call_first_call(api_url, cos_filename, headers):
    try:
        response = requests.get(api_url, headers=headers)
        response.raise_for_status()
        data = response.json()
        count = len(data)
        print(f"Initial ingestion returned {count} records.")

        if count > 0:
            ingestion_date = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
            log_data = {
                "ingestion_date": ingestion_date,
                "count": count,
                "response status": response.status_code,
                "response message": response.reason,
            }
            log_file = f"logs/ingestion_logs/{cos_filename}/{cos_filename}.json"
            create_ingestion_log(log_file, log_data)
            return data
        else:
            print(f"No data ingested from {api_url}")
    except Exception as e:
        print(f"Error calling Plex API: {e}")


def incremental_call(api_url, cos_filename, headers):
    try:
        log_file = f"logs/ingestion_logs/{cos_filename}/{cos_filename}.json"
        if os.path.exists(log_file):
            with open(log_file, 'r') as f:
                log_data = json.load(f)
                last_ingested = log_data.get("ingestion_date") or log_data.get("last_ingested")

            if last_ingested:
                params = {"modifiedDateBegin": last_ingested}
                print(f"Performing incremental ingestion from {last_ingested}")
                throttle()
                response = requests.get(api_url, headers=headers, params=params)
                response.raise_for_status()
                data = response.json()
                count = len(data)
                print(f"Incremental ingestion returned {count} records.")

                if count > 0:
                    latest_modified = max(item["modifiedDate"] for item in data)
                    update_incremental_log(log_file, latest_modified, count)
                    return data
                else:
                    print("No new records found since last_ingested.")
    except Exception as e:
        print(f"Error during incremental ingestion: {e}")