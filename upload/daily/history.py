import util.time
import config
import logging, sys

_run_dates = set()
logging.basicConfig(stream=sys.stdout, level=logging.INFO)


def _did_upload_today_run_dates():
    cfg = config.load('config.us.yaml')
    tz = config.get_tz(cfg)
    dt_str = str(util.time.get_utcnow().astimezone(tz).date())
    if dt_str in _run_dates:
        return True
    return False


def did_upload_today():
    '''
    Tells if upload already happened for today.

    :return: boolean
    '''
    return _did_upload_today_run_dates()


def on_upload():
    global _run_dates
    _run_dates.add(util.time.get_today_str_tz())
