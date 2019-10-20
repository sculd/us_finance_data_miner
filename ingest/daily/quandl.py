import requests, os, pickle, time, datetime
import util.symbols

_USL_BASE_QUANDL = 'https://www.quandl.com/api'
_QUERY_PATH_QUANDL_DAILY_QUOTE  = '/v3/datasets/EOD/{symbol}?start_date={start_date}&end_date={end_date}&api_key={api_key}'
_API_KEY_QUANDL = os.environ['API_KEY_QUANDL']

def download_histories_csv(start_date, end_date):
    date_str = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d')
    filename = 'data/us.history_{date_str}.csv'.format(date_str=date_str)

    with open(filename, 'w') as outfile:
        outfile.write('date,close,open,high,low,volume,symbol\n')

        symbols = util.symbols.get_symbols()
        for cnt, symbol in enumerate(symbols):
            print('processing {cnt}th symbol: {symbol}'.format(symbol=symbol, cnt=cnt))

            param_option = {
                'symbol': symbol,
                'start_date': start_date,
                'end_date': end_date,
                'api_key': _API_KEY_QUANDL,
            }

            response = requests.get(
                _USL_BASE_QUANDL + _QUERY_PATH_QUANDL_DAILY_QUOTE.format(**param_option),
                data={}
                )

            res = response.json()
            if not res:
                print('The response is invalid: %s' % (res))
                continue

            if 'dataset' not in res:
                print('The response does not have dataset: %s' % (res))
                continue

            if 'data' not in res['dataset']:
                print('The response data does not have data: %s' % (res))
                continue

            data = res['dataset']['data']
            out_lines = []
            for data_for_date in data:
                date_str, close, open_, high, low, volume = data_for_date[0], data_for_date[4], data_for_date[1], data_for_date[2], data_for_date[3], data_for_date[5]
                out_lines.append('{date_str},{close},{open},{high},{low},{volume},{symbol}\n'.format(date_str=date_str, close=close, open=open_, high=high, low=low, volume=volume, symbol=symbol))
            outfile.writelines(out_lines)


