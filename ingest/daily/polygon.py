import requests, os, re, pickle, time, datetime
import util.symbols
import util.time

_URL_BASE = 'https://api.polygon.io/v1'
#_QUERY_PATH = '/aggs/grouped/locale/US/market/STOCKS/{date}?apiKey={apiKey}'
_QUERY_PATH = '/open-close/{symbol}/{date}?apiKey={apiKey}'
_API_KEY = os.environ['API_KEY_POLYGON']
_DIR_BASE = 'data/daily_polygon/'

from pytz import timezone
_TZ_US_EAST = timezone('US/EASTERN')


from requests_throttler import BaseThrottler

def _get_request(symbol, date_str):
    param_option = {
        'symbol': symbol,
        'date': date_str,
        'apiKey': _API_KEY
    }
    url = _URL_BASE + _QUERY_PATH.format(**param_option)
    return requests.Request(method='GET', url=url)

def _get_requests(date_str):
    res = []
    symbols = util.symbols.get_symbols()
    for symbol in symbols:
        res.append(_get_request(symbol, date_str))
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

        if not res:
            continue

        js = res.json()

        if 'status' not in js or (js['status'] != 'OK' and js['status'] != 'success'):
            print('The response does not have proper status: %s' % (js))
            continue

        keys = ['open', 'afterHours', 'high', 'low', 'volume', 'from']
        is_blob_compromised = False
        for k in keys:
            if k not in js:
                print('blob: {blob} does not have all the expected keys, missing key: {key}'.format(blob=str(blob),
                                                                                                    key=k))
                is_blob_compromised = True
                break
        if is_blob_compromised:
            continue

        symbol = js['symbol']

        close, open_, high, low, volume = js['afterHours'], js['open'], js['high'], js['low'], js['volume']
        print('{symbol}'.format(symbol=symbol))
        close_v = float(close)
        if close_v < 1.0 or close_v > 10000:
            continue

        date_str = datetime.datetime.strptime(js['from'], "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d")

        rows.append('{date_str},{close},{open},{high},{low},{volume},{symbol}\n'.format(
            date_str=date_str, close=close, open=open_, high=high, low=low, volume=volume,
            symbol=symbol))

    return rows

def download_histories_csv_range(past_days):
    filename = _DIR_BASE + 'us.daily.{past_days}d.polygon.csv'.format(past_days=past_days)

    request_list = []
    d_today = util.time.get_today_tz()
    for i in range(past_days):
        td = datetime.timedelta(days=i)
        d = d_today - td
        date_str = str(d)
        print('{date_str}'.format(date_str=date_str))
        request_list += _get_requests(date_str)

    batch_size = 100
    i_batch_start = 0
    rows = []
    while i_batch_start < len(request_list):
        print('i_batch_start: {i_batch_start} begin'.format(i_batch_start=i_batch_start))
        rows += _run_requests_return_rows(request_list[i_batch_start:i_batch_start + batch_size])
        print('i_batch_start: {i_batch_start} is done'.format(i_batch_start=i_batch_start))
        i_batch_start += batch_size

    with open(filename, 'w') as outfile:
        outfile.write('date,close,open,high,low,volume,symbol\n')
        for row in rows:
            outfile.writelines(row)

def download_histories_csv(date_str):
    filename = _DIR_BASE + 'us.daily.polygon.csv'

    request_list = _get_requests(date_str)

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

