import os, time, datetime
import util.symbols, util.time

def extract_histories_csv(past_days):
    d_today = util.time.get_today_tz()
    start_date_str = None
    for i in range(past_days):
        td = datetime.timedelta(days=i)
        d = d_today - td
        start_date_str = str(d)

    source_filename = 'data/history_dump/EOD_20191101.csv'
    filename = 'data/history_dump/us.history.quandl.{past_days}csv'.format(past_days=past_days)
    with open(filename, 'w') as outfile:
        outfile.write('date,close,open,high,low,volume,symbol\n')
        for line in open(source_filename, 'r'):
            tokens = line.split(',')
            date_str = tokens[1]
            if date_str < start_date_str:
                continue

            symbol, open_, high, low, close, volume = tokens[0], tokens[2], tokens[3], tokens[4], tokens[5], tokens[6]
            row = '{date_str},{close},{open},{high},{low},{volume},{symbol}\n'.format(
                    date_str=date_str, close=close, open=open_, high=high, low=low, volume=volume, symbol=symbol)
            outfile.write(row)
