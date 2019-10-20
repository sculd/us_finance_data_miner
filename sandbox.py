import ingest.daily.iex

ingest.daily.iex.download_histories_csv()


'''
import ingest.intraday.iex

ingest.intraday.iex.download_histories_csv('20191016')
'''


'''
import ingest.intraday.polygon

ingest.intraday.polygon.download_histories_csv('2019-10-16')
'''





'''
import util.auth_etrade

session = util.auth_etrade.get_session()


import ingest.intraday.etrade

market = ingest.intraday.etrade.Market(session)
market.quotes()
'''
