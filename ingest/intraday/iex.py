import requests, os, pickle, time, datetime
import util.symbols

_URL_BASE = 'https://cloud.iexapis.com/stable'
_QUERY_PATH_INTRADAY_PRICE  = '/stock/{symbol}/intraday-prices?token={token}&exactDate={date}'
_API_KEY = os.environ['API_KEY_IEX']


def download_histories_csv(date):
    filename = 'data/us.intraday.history_{date}.csv'.format(date=date)

    with open(filename, 'w') as outfile:
        outfile.write('date,close,open,high,low,volume,symbol\n')

        symbols = util.symbols.get_symbols()
        for cnt, symbol in enumerate(symbols):
            print('processing {cnt}th symbol: {symbol}'.format(symbol=symbol, cnt=cnt))

            param_option = {
                'symbol': symbol,
                'date': date, #YYYYMMDD
                'token': _API_KEY,
            }

            url = _URL_BASE + _QUERY_PATH_INTRADAY_PRICE.format(**param_option)
            response = requests.get(
                url,
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
                out_lines.append('{date},{close},{open},{high},{low},{volume},{symbol}\n'.format(date=date, close=close, open=open_, high=high, low=low, volume=volume, symbol=symbol))
            outfile.writelines(out_lines)


'''
{ 
      "date":"2019-10-18",
      "minute":"13:55",
      "label":"1:55 PM",
      "high":236.13,
      "low":236.095,
      "open":236.13,
      "close":236.095,
      "average":236.112,
      "volume":450,
      "notional":106250.5,
      "numberOfTrades":5
   }
'''