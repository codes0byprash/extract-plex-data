# from src.s3uploader.upload_to_s3 import fetch_and_upload_if_changed
# def fetch_and_upload(api_url,cos_filename):
#     """ Always fetch and upload"""
#     return fetch_and_upload_if_changed(api_url,cos_filename)
from src.s3uploader.upload_to_s3 import call_plex_api
def tenant_directory(headers,url):
    """ Always fetch and upload"""
    cos_filename = "Tenant_Directory"  # or however you want to name it
    return call_plex_api(url,cos_filename)