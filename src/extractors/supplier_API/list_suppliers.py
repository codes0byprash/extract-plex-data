import os
import ibm_boto3
import json
from ibm_botocore.client import Config
from src.extractors.supplier_API.extract_data import call_plex_api,check_for_changes
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()
def connect_to_cos():
    access_key = os.environ.get("IBM_ACCESS_KEY")
    secret_key = os.environ.get("IBM_SECRET_ACCESS_KEY") 
    endpoint_url = os.environ.get("IBM_COS_ENDPOINT")
    bucket_name = os.environ.get("BUCKET_NAME")
    if not all([access_key, secret_key, endpoint_url, bucket_name]):
        raise EnvironmentError("One or more IBM COS environment variables are missing.")
    cos = ibm_boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4"),
        endpoint_url=endpoint_url
    )
    #create sample_data folder if it doesn't exist
    try:
        cos.head_object(Bucket=bucket_name, Key="cuisine-solution-datalake/sample_data/")
    except cos.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            cos.put_object(Bucket=bucket_name, Key="cuisine-solution-datalake/sample_data/")
            print("Created folder 'cuisine-solution-datalake/sample_data/' in bucket.")
        else:
            raise
    return cos, bucket_name


def fetch_and_upload_if_changed(api_url, cos_filename,headers):
    """used if there are actual changes in the data"""
    changes=check_for_changes(api_url,cos_filename)
    if not changes:
        print(f"No changes detected for {cos_filename}. Skip uploading \n")
        return None
    print(f"Changes detected for : {cos_filename}\n...Proceeding with extraction...")
    cos, bucket_name = connect_to_cos()
    data = call_plex_api(api_url, cos_filename,headers=headers)
    if data :
        json_bytes = json.dumps(data, indent=2).encode("utf-8")
        # Create date-based folder structure with timestamp
        today = datetime.now()
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
def fetch_and_upload(api_url,cos_filename):
    """ Always fetch and upload"""
    return fetch_and_upload_if_changed(api_url,cos_filename)