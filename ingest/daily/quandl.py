import requests, os, pickle, time, datetime
import util.symbols, util.time

_USL_BASE_QUANDL = 'https://www.quandl.com/api'
_QUERY_PATH_QUANDL_DAILY_QUOTE  = '/v3/datasets/EOD/{symbol}?start_date={start_date}&end_date={end_date}&api_key={api_key}'
_API_KEY_QUANDL = os.environ['API_KEY_QUANDL']

from requests_throttler import BaseThrottler

def _get_request(symbol, start_date, end_date):
    param_option = {
        'symbol': symbol,
        'start_date': start_date,
        'end_date': end_date,
        'api_key': _API_KEY_QUANDL,
    }
    url = _USL_BASE_QUANDL + _QUERY_PATH_QUANDL_DAILY_QUOTE.format(**param_option)
    return requests.Request(method='GET', url=url)

def _get_requests(start_date, end_date):
    res = []
    symbols = util.symbols.get_symbols()
    for symbol in symbols:
        res.append(_get_request(symbol, start_date, end_date))
    return res

def _run_requests_return_rows(request_list):
    bt = BaseThrottler(name='base-throttler', delay=0.1)
    bt.start()
    throttled_requests = bt.multi_submit(request_list)

    print('shutting down the throttler')
    bt.shutdown()
    print('waiting for the requests to be done')
    bt.wait_end()
    print('run_done')
    responses = [tr.response for tr in throttled_requests]

    rows = []
    for cnt, response in enumerate(responses):
        if not response:
            print('The response is invalid: %s' % (response))
            continue

        if response.status_code != 200:
            print('response status code is not 200 OK: {code}'.format(code=response.status_code))
            continue

        if not response:
            continue

        js = response.json()
        if not js:
            print('The response is invalid: %s' % (js))
            continue

        if 'dataset' not in js:
            print('The response does not have dataset: %s' % (js))
            continue

        if 'data' not in js['dataset']:
            print('The response data does not have data: %s' % (js))
            continue

        symbol = js['dataset']['dataset_code']
        data = js['dataset']['data']
        for data_for_date in data:
            date_str = data_for_date[0]

            close, open_, high, low, volume = data_for_date[4], data_for_date[1], data_for_date[2], data_for_date[3], data_for_date[5]
            rows.append(
                '{date_str},{close},{open},{high},{low},{volume},{symbol}\n'.format(
                    date_str=date_str, close=close, open=open_, high=high, low=low, volume=volume, symbol=symbol))
    return rows

def _download_histories_csv(past_days, start_date, end_date):
    request_list = _get_requests(start_date, end_date)

    batch_size = 100
    i_batch_start = 0
    rows = []
    while i_batch_start < len(request_list):
        print('i_batch_start: {i_batch_start} begin'.format(i_batch_start=i_batch_start))
        rows += _run_requests_return_rows(request_list[i_batch_start:i_batch_start + batch_size])
        print('i_batch_start: {i_batch_start} is done'.format(i_batch_start=i_batch_start))
        i_batch_start += batch_size

    filename = 'data/daily/us.history.quandl.{past_days}.csv'.format(past_days=past_days)
    with open(filename, 'w') as outfile:
        outfile.write('date,close,open,high,low,volume,symbol\n')
        for row in rows:
            outfile.writelines(row)

def download_histories_csv(past_days):
    d_today = util.time.get_today_tz()
    end_date_str = d_today
    start_date_str = None
    for i in range(past_days):
        td = datetime.timedelta(days=i)
        d = d_today - td
        start_date_str = str(d)

    return _download_histories_csv(past_days, start_date_str, end_date_str)
