from src.s3uploader.connect_to_cos import connect_to_cos
import json
import datetime
from src.s3uploader.extract_data import call_plex_api,check_for_changes

def fetch_and_upload_if_changed(api_url, cos_filename,headers):
    """used if there are actual changes in the data"""
    changes=check_for_changes(api_url,cos_filename)
    if not changes:
        print(f"No changes detected for {cos_filename}. Skip uploading \n")
        return None
    print(f"Changes detected for : {cos_filename}\n...Proceeding with extraction...")
    cos, bucket_name = connect_to_cos()
    data = call_plex_api(api_url, cos_filename)
    if data :
        json_bytes = json.dumps(data, indent=2).encode("utf-8")
        # Create date-based folder structure with timestamp
        today = datetime.datetime.now()
        date_path = today.strftime("%Y/%m/%d")
        timestamp = today.strftime("%Y%m%dT%H%M%S")
        object_key = (
            f"cuisine-solution-datalake/sample_data/"
            f"{cos_filename}/{date_path}/data_{timestamp}.json"
        )
        cos.put_object(Bucket=bucket_name, Key=object_key, Body=json_bytes)
        print(f"Uploaded Plex data to '{object_key}' in bucket '{bucket_name}'.")
        return data
    else:
        print("No data fetched from Plex API.")
        return False
