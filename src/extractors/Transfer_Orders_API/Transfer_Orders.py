from src.s3uploader.upload_to_s3 import call_plex_api
def transfer_orders(headers,url):
    """ Always fetch and upload"""
    cos_filename = "transfer_orders"  # or however you want to name it
    return call_plex_api(url,cos_filename)