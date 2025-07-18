import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from s3uploader.connect import connect_to_cos

from logs.ingestionlogger import log_ingestion_status
from botocore.exceptions import ClientError
import json, datetime

def fetch_and_upload(data, resource_name, log_file_path):
    cos, bucket_name = connect_to_cos()
    timestamp = datetime.datetime.now(datetime.timezone.utc)

    if data:
        try:
            json_bytes = json.dumps(data, indent=2).encode("utf-8")
            today = datetime.datetime.utcnow()
            date_path = today.strftime("%Y/%m/%d")
            timestamp_str = today.strftime("%Y%m%dT%H%M%S")
            object_key = (
                f"cuisine-solution-datalake/sample_data/"
                f"{resource_name}/{date_path}/data_{timestamp_str}.json"
            )

            cos.put_object(Bucket=bucket_name, Key=object_key, Body=json_bytes)
            print(f"Uploaded Plex data to '{object_key}' in bucket '{bucket_name}'.")
            log_ingestion_status(log_file_path, timestamp, "200")

        except ClientError as ce:
            error_msg = ce.response["Error"]["Message"]
            print(f"ClientError during Plex data upload: {error_msg}")
            log_ingestion_status(log_file_path, timestamp, "400", error_msg)
            raise

        except Exception as e:
            error_msg = str(e)
            print(f"Unexpected error during Plex data upload: {error_msg}")
            log_ingestion_status(log_file_path, timestamp, "400", error_msg)
            raise
    else:
        print("No data fetched from Plex API.")

def logs_upload_to_s3(resource_name, log_file_path):
    cos, bucket_name = connect_to_cos()
    try:
        with open(log_file_path, "rb") as f:
            # today = datetime.datetime.utcnow()
            # date_path = today.strftime("%Y/%m/%d")
            # timestamp_str = today.strftime("%Y%m%dT%H%M%S")
            object_key = (
                f"cuisine-solution-datalake/sample_data/{resource_name}/{resource_name}_logs.json"
            )
            cos.put_object(Bucket=bucket_name, Key=object_key, Body=f)
            print(f"Uploaded log file to '{object_key}' in bucket '{bucket_name}'.")
    except Exception as e:
        print(f"Failed to upload log file to S3: {e}")