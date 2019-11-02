import requests, os, pickle, time, datetime
import util.symbols
from pytz import timezone

_TZ_US_EAST = timezone('US/EASTERN')
_URL_BASE = 'https://api.polygon.io/v1'
_QUERY_PATH_INTRADAY_PRICE  = '/last/stocks/{symbol}?apiKey={apiKey}'
_API_KEY = os.environ['API_KEY_POLYGON']


from enum import Enum

class INTRADAY_MODE(Enum):
    ALL_MINUTES = 1
    LAST_RECORD = 2

from requests_throttler import BaseThrottler

def _get_request(date, symbol):
    param_option = {
        'symbol': symbol,
        'apiKey': _API_KEY,
    }
    url = _URL_BASE + _QUERY_PATH_INTRADAY_PRICE.format(**param_option)
    return requests.Request(method='GET', url=url)

def _get_requests(date):
    res = []
    symbols = util.symbols.get_symbols_nasdaq()
    for symbol in symbols:
        res.append(_get_request(date, symbol))
    return res

def download_histories_csv(date_str):
    filename = 'data/intraday/us.intraday.polygon.last.csv'

    request_list = _get_requests(date_str)
    # request_list = request_list[:10]
    bt = BaseThrottler(name='base-throttler', delay=0.04)
    bt.start()
    throttled_requests = bt.multi_submit(request_list)

    print('shutting down the throttler')
    bt.shutdown()
    print('waiting for the requests to be done')
    bt.wait_end()
    print('run_done')
    responses = [tr.response for tr in throttled_requests]

    with open(filename, 'w') as outfile:
        outfile.write('date,time,close,open,high,low,volume,symbol\n')
        for cnt, res in enumerate(responses):

            if not res:
                print('The response is invalid: %s' % (res))
                continue

            if res.status_code != 200:
                continue

            js = res.json()
            if 'results' not in js:
                print('The response does not have results: %s' % (js))
                continue

            symbol = js['symbol']
            print('{cnt}th {symbol}'.format(cnt=cnt, symbol=symbol))
            out_lines = []
            blob = js['last']
            epoch = int(blob['t']) // 1000
            t = datetime.datetime.fromtimestamp(epoch).astimezone(_TZ_US_EAST)
            date_str = t.strftime('%Y-%m-%d')
            time_str = t.strftime('%H:%M:%S')
            price = blob['price']
            close, open_, high, low, volume = price, price, price, price, blob['"size"']
            out_lines.append('{date_str},{time_str},{close},{open},{high},{low},{volume},{symbol}\n'.format(
                date_str=date_str, time_str=time_str,
                close=close, open=open_, high=high, low=low, volume=volume, symbol=symbol))
            outfile.writelines(out_lines)

