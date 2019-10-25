import requests, os, pickle, time, datetime
import util.symbols
from pytz import timezone

_TZ_US_EAST = timezone('US/EASTERN')
_URL_BASE = 'https://api.polygon.io/v1'
_QUERY_PATH_INTRADAY_PRICE  = '/last/stocks/{symbol}?apiKey={apiKey}'
_API_KEY = os.environ['API_KEY_POLYGON']
_DIR_BASE = 'data/daily_last_record/'

from enum import Enum

class INTRADAY_MODE(Enum):
    ALL_MINUTES = 1
    LAST_RECORD = 2

from requests_throttler import BaseThrottler

def _get_request(symbol):
    param_option = {
        'symbol': symbol,
        'apiKey': _API_KEY,
    }
    url = _URL_BASE + _QUERY_PATH_INTRADAY_PRICE.format(**param_option)
    return requests.Request(method='GET', url=url)

def _get_requests():
    res = []
    symbols = util.symbols.get_symbols()
    for symbol in symbols:
        res.append(_get_request(symbol))
    return res

def _run_requests_return_rows(request_list):
    bt = BaseThrottler(name='base-throttler', delay=0.04)
    bt.start()
    throttled_requests = bt.multi_submit(request_list)

    print('shutting down the throttler')
    bt.shutdown()
    print('waiting for the requests to be done')
    bt.wait_end()
    print('run_done')
    responses = [tr.response for tr in throttled_requests]

    rows = []
    for cnt, res in enumerate(responses):
        if not res:
            print('The response is invalid: %s' % (res))
            continue

        if res.status_code != 200:
            continue

        if not res:
            print('The response does not have contents: %s' % (res))
            continue

        js = res.json()
        if 'status' not in js or js['status'] != 'success':
            print('The response does not have proper status: %s' % (js))
            continue

        if 'last' not in js:
            print('The response does not have results: %s' % (js))
            continue

        symbol = js['symbol']
        print('{cnt}th {symbol}'.format(cnt=cnt, symbol=symbol))
        blob = js['last']
        epoch = int(blob['timestamp']) // 1000
        t = datetime.datetime.fromtimestamp(epoch).astimezone(_TZ_US_EAST)
        date_str = t.strftime('%Y-%m-%d')
        price = blob['price']
        volume = blob['size']
        close, open_, high, low = price, price, price, price
        rows.append('{date_str},{close},{open},{high},{low},{volume},{symbol}\n'.format(
            date_str=date_str, close=close, open=open_, high=high, low=low, volume=volume,
            symbol=symbol))

    return rows


def download_histories_csv():
    if not os.path.exists(_DIR_BASE):
        os.mkdir(_DIR_BASE)
        
    filename = _DIR_BASE + 'us.daily.polygon.last.record.csv'

    request_list = _get_requests()

    batch_size = 100
    i_batch_start = 0
    rows = []
    while i_batch_start < len(request_list):
        print('i_batch_start: {i_batch_start} begin'.format(i_batch_start=i_batch_start))
        rows += _run_requests_return_rows(request_list[i_batch_start:i_batch_start+batch_size])
        print('i_batch_start: {i_batch_start} is done'.format(i_batch_start=i_batch_start))
        i_batch_start += batch_size

    with open(filename, 'w') as outfile:
        outfile.write('date,close,open,high,low,volume,symbol\n')
        for row in rows:
            outfile.writelines(row)
