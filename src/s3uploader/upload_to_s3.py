import s3uploader.connect as connect_to_cos
import json
import datetime
import logging
def initialize_ingestion_logging():
    return logging.getLogger("ingestion_logger")

def call_plex_api(api_url, cos_filename):
    retunbrn_data = []
    

def fetch_and_upload(api_url, cos_filename):
    cos, bucket_name = connect_to_cos()
    data = call_plex_api(api_url, cos_filename)
    if data:
        json_bytes = json.dumps(data, indent=2).encode("utf-8")
        # Create date-based folder structure with timestamp
        today = datetime.now()
        date_path = today.strftime("year=%Y/month=%m/day=%d")
        timestamp = today.strftime("%Y%m%dT%H%M%S")
        object_key = (
            f"cuisine-solution-datalake/sample_data/"
            f"{cos_filename}/{date_path}/data_{timestamp}.json"
        )
        cos.put_object(Bucket=bucket_name, Key=object_key, Body=json_bytes)
        print(f"Uploaded Plex data to '{object_key}' in bucket '{bucket_name}'.")
    else:
        print("No data fetched from Plex API.")