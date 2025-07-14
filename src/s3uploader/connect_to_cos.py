import os 
import ibm_boto3
# import json
from ibm_botocore.client import Config
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
