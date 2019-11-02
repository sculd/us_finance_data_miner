import requests, os, re, pickle, time, datetime
import util.symbols
import util.time

_URL_BASE = 'https://sandbox.iexapis.com/stable'
_QUERY_PATH = '/stock/{symbol}/chart/{range}?token={token}'
_API_KEY = os.environ['API_KEY_IEX_SANDBOX']

_RANGE_5D = '5d'

from requests_throttler import BaseThrottler

def test_request():
    symbol = 'TWMC'
    param_option = {
        'symbol': symbol,
        'range': _RANGE_5D,
        'token': _API_KEY,
    }
    url = _URL_BASE + _QUERY_PATH.format(**param_option)
    res = requests.get(url)
    return res.json()

def _get_request(symbol):
    param_option = {
        'symbol': symbol,
        'range': _RANGE_5D,
        'token': _API_KEY,
    }
    url = _URL_BASE + _QUERY_PATH.format(**param_option)
    return requests.Request(method='GET', url=url)

def _get_requests():
    res = []
    symbols = util.symbols.get_symbols_nasdaq()
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
        for blob in js:
            keys = ['date', 'close', 'open', 'high', 'low', 'volume']
            is_blob_compromised = False
            for k in keys:
                if k not in blob:
                    print('blob: {blob} does not have all the expected keys, missing key: {key}'.format(blob=str(blob), key=k))
                    is_blob_compromised = True
                    break
            if is_blob_compromised:
                continue
            date_str = blob['date']
            close, open_, high, low, volume = blob['close'], blob['open'], blob['high'], blob['low'], blob['volume']
            if volume == '0' or volume == 0 or close is None:
                close, open_, high, low = prev_close, prev_close, prev_close, prev_close

            if close is None:
                continue

            rows.append('{date_str},{close},{open},{high},{low},{volume},{symbol}\n'.format(
                date_str=date_str, close=close, open=open_, high=high, low=low, volume=volume,
                symbol=symbol))

            prev_close = close
    return rows

def download_histories_csv():
    filename = 'data/daily/us.daily.iex.{range}.csv'.format(range=_RANGE_5D)

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
