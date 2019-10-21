import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), 'credential.json')

from google.cloud import storage
from google.cloud.exceptions import NotFound
from ingest.intraday.iex import INTRADAY_MODE

_client = None

_BUCKET_NAME = 'stock_daily_data'
_BLOB_NAME_ALL_MINUTES = 'us.intraday.all.csv'
_BLOB_NAME_LAST_RECORD = 'us.intraday.last.csv'

def _get_client():
    global _client
    if _client is None:
        _client = storage.Client()
    return _client

def get_latest_source_filename(intraday_mode):
    '''
    data/daily/us.combined.csv
    '''
    base_dir = 'data/intraday'
    filename = None
    if intraday_mode is INTRADAY_MODE.ALL_MINUTES:
        filename = 'us.intraday.all.csv'
    elif intraday_mode is INTRADAY_MODE.LAST_RECORD:
        filename = 'us.intraday.last.csv'

    return os.path.join(base_dir, filename)

def upload(intraday_mode):
    try:
        client = _get_client()
        bucket = client.get_bucket(_BUCKET_NAME)
        blob_name = None
        if intraday_mode is INTRADAY_MODE.ALL_MINUTES:
            blob_name = _BLOB_NAME_ALL_MINUTES
        elif intraday_mode is INTRADAY_MODE.LAST_RECORD:
            blob_name = _BLOB_NAME_LAST_RECORD

        blob = bucket.blob(blob_name)

        source_file = get_latest_source_filename(intraday_mode)
        blob.upload_from_filename(source_file)
        print('File {} uploaded to {}.'.format(source_file, blob_name))
    except NotFound:
        print("Sorry, that bucket {} does not exist!".format(_BUCKET_NAME))


