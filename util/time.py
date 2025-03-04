import datetime
import pytz
import config

def get_utcnow():
    tz_utc = pytz.utc
    return tz_utc.localize(datetime.datetime.utcnow())

def get_now_tz():
    cfg = config.load('config.us.yaml')
    tz = config.get_tz(cfg)
    return get_utcnow().astimezone(tz)

def get_now_time_tz():
    '''
    get datetime.time of now in korean time zone.
    :return:
    '''
    cfg = config.load('config.us.yaml')
    tz = config.get_tz(cfg)
    now_tz = get_utcnow().astimezone(tz)
    return datetime.time(now_tz.hour, now_tz.minute, now_tz.second, tzinfo=tz)

def get_today_tz():
    '''
    :return: e.g: 2019-12-25
    '''
    cfg = config.load('config.us.yaml')
    tz = config.get_tz(cfg)
    return get_utcnow().astimezone(tz).date()

def get_today_str_tz():
    '''
    :return: e.g: 2019-12-25
    '''
    return str(get_today_tz())

def get_today_v_tz():
    '''
    :return: e.g: 2019-12-25
    '''
    cfg = config.load('config.us.yaml')
    tz = config.get_tz(cfg)
    return get_utcnow().astimezone(tz).strftime('%Y%m%d')

def time_diff_seconds(t1, t2):
    '''
    Get datetime.timedelta between two datetime.time

    :param t1:
    :param t2:
    :return:
    '''
    today = datetime.date.today()
    dt1, dt2 = datetime.datetime.combine(today, t1), datetime.datetime.combine(today, t2)
    tf = dt1 - dt2
    return tf.days * 24 * 3600 + tf.seconds
