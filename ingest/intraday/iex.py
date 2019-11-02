import requests, os, re, pickle, time, datetime
import util.symbols

_URL_BASE = 'https://sandbox.iexapis.com/stable'
_QUERY_PATH  = '/stock/{symbol}/chart/date/{date}?token={token}'
_API_KEY = os.environ['API_KEY_IEX_SANDBOX']

from enum import Enum

class INTRADAY_MODE(Enum):
    ALL_MINUTES = 1
    LAST_RECORD = 2

from requests_throttler import BaseThrottler

def _get_request(date, symbol):
    param_option = {
        'symbol': symbol,
        'date': date, # YYYYMMDD
        'token': _API_KEY,
    }
    url = _URL_BASE + _QUERY_PATH.format(**param_option)
    return requests.Request(method='GET', url=url)

def _get_requests(date):
    res = []
    symbols = util.symbols.get_symbols_nasdaq()
    for symbol in symbols:
        res.append(_get_request(date, symbol))
    return res

def _run_requests_return_rows(request_list, intraday_mode):
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
            print('response status code is not 200 OK: {code}'.format(code=res.status_code))
            continue

        js = res.json()
        req = request_list[cnt]
        m = re.search(r'stock/([^/]+)', req.url)
        if not m:
            continue

        if not m.groups():
            continue

        symbol = m.groups()[0]

        if not js:
            continue

        print('{cnt}th {symbol}, blobs: {l}'.format(cnt=cnt, symbol=symbol, l=len(js)))
        prev_close = None

        if intraday_mode is INTRADAY_MODE.LAST_RECORD:
            js = js[-5:][::-1]

        for blob in js:
            keys = ['date', 'minute', 'close', 'open', 'high', 'low', 'volume']
            is_blob_compromised = False
            for k in keys:
                if k not in blob:
                    print('blob: {blob} does not have all the expected keys, missing key: {key}'.format(blob=str(blob), key=k))
                    is_blob_compromised = True
                    break
            if is_blob_compromised:
                continue
            date_str = blob['date']
            time_str = blob['minute']
            close, open_, high, low, volume = blob['close'], blob['open'], blob['high'], blob['low'], blob['volume']
            if volume == '0' or volume == 0 or close is None:
                close, open_, high, low = prev_close, prev_close, prev_close, prev_close

            if close is None:
                continue

            rows.append('{date_str},{time_str},{close},{open},{high},{low},{volume},{symbol}\n'.format(
                date_str=date_str, time_str=time_str, close=close, open=open_, high=high, low=low, volume=volume,
                symbol=symbol))

            prev_close = close

            if intraday_mode is INTRADAY_MODE.LAST_RECORD:
                break
            
    return rows

def download_histories_csv(date, intraday_mode=INTRADAY_MODE.LAST_RECORD):
    filename = None
    if intraday_mode is INTRADAY_MODE.ALL_MINUTES:
        filename = 'data/intraday/us.intraday.iex.all.csv'.format(date=date)
    elif intraday_mode is INTRADAY_MODE.LAST_RECORD:
        filename = 'data/intraday/us.intraday.iex.last.csv'.format(date=date)

    request_list = _get_requests(date)

    batch_size = 100
    i_batch_start = 0
    rows = []
    while i_batch_start < len(request_list):
        print('i_batch_start: {i_batch_start} begin'.format(i_batch_start=i_batch_start))
        rows += _run_requests_return_rows(request_list[i_batch_start:i_batch_start+batch_size], intraday_mode)
        print('i_batch_start: {i_batch_start} is done'.format(i_batch_start=i_batch_start))
        i_batch_start += batch_size

    with open(filename, 'w') as outfile:
        outfile.write('date,time,close,open,high,low,volume,symbol\n')
        for row in rows:
            outfile.writelines(row)
