import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), 'credential.json')

from google.cloud import storage
from google.cloud.exceptions import NotFound

_client = None

_BUCKET_NAME = 'stock_daily_data'
_BLOB_NAME = 'kor.daily.csv'

def _get_client():
    global _client
    if _client is None:
        _client = storage.Client()
    return _client

def get_latest_source_filename():
    '''
    give the filename most recently written.

    :return: e.g. combined_2019_09_24.csv
    '''
    base_dir = 'data'
    latest = ''
    for filename in os.listdir(base_dir):
        if os.path.isdir(os.path.join(base_dir, filename)): continue
        if not filename.endswith('.csv'): continue
        if filename > latest:
            latest = filename
    return os.path.join(base_dir, latest)

def upload():
    try:
        client = _get_client()
        bucket = client.get_bucket(_BUCKET_NAME)
        blob = bucket.blob(_BLOB_NAME)
        source_file = get_latest_source_filename()
        blob.upload_from_filename(source_file)
        print('File {} uploaded to {}.'.format(source_file, _BLOB_NAME))
    except NotFound:
        print("Sorry, that bucket {} does not exist!".format(_BUCKET_NAME))


