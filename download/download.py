import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), 'credential.json')
from google.cloud import storage
from google.cloud.exceptions import NotFound

_client = None

_BUCKET_NAME = 'stock_daily_data'

_BLOB_NAME_US = 'us.daily.csv'
_BLOB_NAME_US_POLYGON = 'us.daily.polygon.csv'

DEST_DIR_DAILY = 'data/daily_last_record/'
DEST_DIR_DAILY_LAST_RECORD  = 'data/daily_last_record/'
DEST_FILENAME_US = _DEST_DIR + 'combined.us.csv'


def _get_client():
    global _client
    if _client is None:
        _client = storage.Client()
    return _client

def _download(dest_dir, blob_name):
    try:
        client = _get_client()
        bucket = client.get_bucket(_BUCKET_NAME)
        blob = bucket.blob(blob_name)

        if not os.path.exists(dest_dir):
            os.mkdir(dest_dir)

        dest_filename = dest_dir + 'combined.us.csv'
        blob.download_to_filename(dest_filename)
        print('Blob {} downloaded to {}.'.format(blob_name, dest_filename))
    except NotFound:
        print("Sorry, that bucket {} does not exist!".format(_BUCKET_NAME))

def download_polygon(dest_dir):
    _download(dest_dir, _BLOB_NAME_US_POLYGON)

def download(dest_dir):
    _download(dest_dir, _BLOB_NAME_US)

