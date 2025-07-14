import json
import os
import time
from dotenv import load_dotenv
import requests,datetime
from datetime import datetime, timezone
# Load environment variables from .env
load_dotenv()
API_KEY = os.getenv("X_PLEX_CONNECT_API_KEY")
X_PLEX_CONNECT_API_SECRET = os.getenv("X_PLEX_CONNECT_API_SECRET")
X_PLEX_CONNECT_CUSTOMER_ID = os.getenv("X_PLEX_CONNECT_CUSTOMER_ID")
headers = {
            'X-Plex-Connect-Api-Key': API_KEY,
            'X-Plex-Connect-Api-Secret': X_PLEX_CONNECT_API_SECRET,
            'X-Plex-Connect-Customer-Id': X_PLEX_CONNECT_CUSTOMER_ID
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
    """Check if there are any changes since last extraction"""
    log_file = f"logs/extraction/{cos_filename}.json"
    if not os.path.exists(log_file):
        print(f"No previous extraction log found for {cos_filename}. Will perform initial extraction.")
        return True
    try:
        with open (log_file,'r') as f:
            log_data = json.load(f)
            last_ingested = log_data.get( "ingestion_date") or log_data.get("last_ingested")
            if not last_ingested:
                print(f"No last ingestion date found for {cos_filename}. Will perform full extraction.")
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
        print(f"Will proceed with extraction as a safety measure.")
        return True 


def call_plex_api(api_url, cos_filename):
    throttle()
    log_file = f"logs/extraction/{cos_filename}.json"
    if not os.path.exists(log_file):
        return call_first_call(api_url, cos_filename,headers)
    else:
        return incremental_call(api_url, cos_filename,headers)
def call_first_call(api_url, cos_filename,headers):
    try:
        response = requests.get(api_url, headers=headers)
        response.raise_for_status()
        data = response.json()
        count = len(data)
        print(f"Initial extraction returned {count} records.")
        # count = len(response)
        # print(count)
        print(f"Initial extraction returned {count} records.")
        if count > 0:
            ingestion_date  = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
            log_data = {
                "ingestion_date": ingestion_date,
                "count": count,
                "response status": response.status_code,
                "response mesage": response.reason,
            }
            # cos_filename  = cos_filename
            #create a folder logs/extraction if not exists
            os.makedirs("logs/extraction/", exist_ok=True)
            with open(f"logs/extraction/{cos_filename}.json", "w") as f:
                json.dump(log_data, f, indent=2)
            print(f"Data logs to logs/extraction/{cos_filename}.json")
            return data
        else:
            print(f"No data ingested from {api_url}")
    except Exception as e:
        print(f"Error calling Plex API: {e}")    

def incremental_call(api_url, cos_filename,headers):
    try:
        log_file = f"logs/extraction/{cos_filename}.json"
        if os.path.exists(log_file):
            with open(log_file, 'r') as f:
                log_data = json.load(f)
                last_ingested = log_data.get("ingestion_date")
                if last_ingested:
                    params = {
                        "modifiedDateBegin": last_ingested
                    }
                    print(f"Performing incremental extraction from {last_ingested}")
                    throttle()
                    response = requests.get(api_url, headers=headers, params=params)
                    response.raise_for_status()
                    data = response.json()
                    # print(data)
                    count = len(data)
                    print(f"Incremental extraction returned {count} records.")
                    if count > 0:
                        # Handle response and update log file as needed
                        print("New records found. Proceeding with update.")
                        # Update log file with new last_ingested date
                        # new_last_ingested = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
                        # Find the max modifiedDate from the returned records
                        latest_modified = max(item["modifiedDate"] for item in data)
                        log_data["last_ingested"] = latest_modified
                        log_data["history"].append({
                            "timestamp": latest_modified,
                            "extracted_count": count
                        })
                        print(f"New last_ingested date: {latest_modified}")
                        # Add to history
                        ingestion_record = {
                            "timestamp": latest_modified,
                            "extracted_count": count
                        }
                        if "history" not in log_data:
                            log_data["history"] = []
                        log_data["history"].append(ingestion_record)
                        # Update last_ingested at root
                        log_data["last_ingested"] = latest_modified
                        with open(log_file, 'w') as f:
                            json.dump(log_data, f, indent=2)
                        print (data)
                        return data
                    else:
                        print("No new records found since last_ingested date.")
                else:
                    print("No last_ingested date found in log file. Skipping incremental extraction.")
        else:
            print(f"Log file {log_file} does not exist. Skipping incremental extraction.")
    except Exception as e:
        print(f"Error during incremental extraction: {e}")
    
