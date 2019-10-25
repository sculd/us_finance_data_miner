import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), 'credential.json')
import config

from google.cloud import storage
from google.cloud.exceptions import NotFound

_client = None

_BUCKET_NAME = 'stock_daily_data'
_BLOB_NAME = 'us.daily.csv'
_BLOB_NAME_LAST_RECORD = 'us.daily.last.record.csv'

def _get_client():
    global _client
    if _client is None:
        _client = storage.Client()
    return _client

def get_latest_source_filename():
    '''
    data/daily/us.combined.csv
    '''
    base_dir = 'data/daily'
    return os.path.join(base_dir, 'us.combined.csv')

def upload(cfg):
    try:
        client = _get_client()
        bucket = client.get_bucket(_BUCKET_NAME)
        blob_name = config.get_uploadname(cfg)
        blob = bucket.blob(blob_name)
        source_file = get_latest_source_filename()
        blob.upload_from_filename(source_file)
        print('File {} uploaded to {}.'.format(source_file, blob_name))
    except NotFound:
        print("Sorry, that bucket {} does not exist!".format(_BUCKET_NAME))


def get_latest_source_filename_last_record():
    '''
    data/daily/us.combined.csv
    '''
    base_dir = 'data/daily_last_record'
    return os.path.join(base_dir, 'us.daily.polygon.last.record.csv')

def upload_last_record():
    try:
        client = _get_client()
        bucket = client.get_bucket(_BUCKET_NAME)
        blob_name = _BLOB_NAME_LAST_RECORD
        blob = bucket.blob(blob_name)
        source_file = get_latest_source_filename_last_record()
        blob.upload_from_filename(source_file)
        print('File {} uploaded to {}.'.format(source_file, blob_name))
    except NotFound:
        print("Sorry, that bucket {} does not exist!".format(_BUCKET_NAME))



