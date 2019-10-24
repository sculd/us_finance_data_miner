import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), 'credential.json')
from google.cloud import storage
from google.cloud.exceptions import NotFound

_client = None

_BUCKET_NAME = 'stock_daily_data'
_BLOB_NAME_US = 'us.daily.csv'
DEST_FILENAME_US = 'data/daily_last_record/combined.us.csv'

def _get_client():
    global _client
    if _client is None:
        _client = storage.Client()
    return _client

def download():
    try:
        client = _get_client()
        bucket = client.get_bucket(_BUCKET_NAME)
        blob_name = _BLOB_NAME_US
        blob = bucket.blob(blob_name)

        deat_filename = DEST_FILENAME_US
        blob.download_to_filename(deat_filename)
        print('Blob {} downloaded to {}.'.format(blob_name, deat_filename))
    except NotFound:
        print("Sorry, that bucket {} does not exist!".format(_BUCKET_NAME))


