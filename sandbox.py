import pprint
import ingest.intraday.iex
import ingest.intraday.polygon

#ingest.intraday.iex.download_histories_csv(20191016)

#ingest.intraday.iex.download_histories_csv('20191016')

#ingest.intraday.polygon.download_histories_csv('2019-10-16')

import ingest.daily.last.polygon

#ingest.daily.last.polygon.download_histories_csv()


import ingest.daily.snapshot.polygon

#ingest.daily.snapshot.polygon.download_histories_csv()


import ingest.daily.quandl

#ingest.daily.quandl.download_histories_csv(2)

import ingest.history_dump.quandl
ingest.history_dump.quandl.extract_histories_csv(700)

import ingest.daily.polygon

#ingest.daily.polygon.download_histories_csv('2019-10-21')

#ingest.daily.polygon.download_histories_csv_range(355)


#ingest.daily.polygon.download_histories_csv_range(355)

import ingest.daily.iex
#pprint.pprint(ingest.daily.iex.test_request())


'''
import util.auth_etrade

session = util.auth_etrade.get_session()


import ingest.intraday.etrade

market = ingest.intraday.etrade.Market(session)
market.quotes()
'''
