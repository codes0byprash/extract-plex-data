# from s3uploader import connect_to_cos
from src.s3uploader.upload_to_s3 import fetch_and_upload_if_changed
def fetch_and_upload(api_url,cos_filename):
    """ Always fetch and upload"""
    return fetch_and_upload_if_changed(api_url,cos_filename)