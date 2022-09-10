
import re
from tool_kit import db_zcs, pd, datetime, timedelta


def gen_trade_date(s_date='', e_date='', days=0, freq='d'):
    """
    生成一段时间的交易日列表，方式有三种：指定开始日期和结束日期，指定结束日期和向前追溯的天数，指定开始日期和向后追溯的天数
    :param s_date: 开始日期，str，"%Y-%m-%d"
    :param e_date: 结束日期，str，"%Y-%m-%d"
    :param days: 期数，int
    :param freq: 日期频率，str，'d'是日频，'w'是周频，'m'是月频
    :return: 交易日列表，list
    """
    database = db_zcs

    if days == 0:#追溯天数为0
        if freq == 'd':
            # s_data<=trade_date <=e_date ,只取data这一列。
            trade_date = database.wind_trade_day.find({'date': {'$gte': s_date, '$lte': e_date}}, {'_id': 0, 'date': 1})
        else:#s_data<=trade_date <=e_date and 月或周的_last_trade_day ==1
            trade_date = database.wind_trade_day.find({'date': {'$gte': s_date, '$lte': e_date}, '%s_last_trade_day' % freq: 1},
                                                      {'_id': 0, 'date': 1})
    elif s_date == '':
        if freq == 'd':
            trade_date = database.wind_trade_day.find({'date': {'$lte': e_date}}, {'_id': 0, 'date': 1}).sort('date', -1).limit(days)
        else:
            trade_date = database.wind_trade_day.find({'date': {'$lte': e_date}, '%s_last_trade_day' % freq: 1},
                                                      {'_id': 0, 'date': 1}).sort('date', -1).limit(days)
    elif e_date == '':
        if freq == 'd':
            trade_date = database.wind_trade_day.find({'date': {'$gte': s_date}}, {'_id': 0, 'date': 1}).limit(days)
        else:
            trade_date = database.wind_trade_day.find({'date': {'$gte': s_date}, '%s_last_trade_day' % freq: 1},
                                                      {'_id': 0, 'date': 1}).limit(days)
    # trade_date形如[{'date':value},{},{}..],故在这里提取value;
    date_ls = [date['date'] for date in trade_date]
    date_ls.sort()
    return date_ls


def shift_date(t_date='', days=None, direction='pre'):
    """
    日期追溯，生成由给定的日期向前或者向后追溯一定数量的交易日后得到的日期，步长包含当天
    :param t_date: 给定日期，str，"%Y-%m-%d"
    :param days: 追溯交易日数量，int表示追溯多少天，str表示追溯至邻近的时间段结尾日期，w是周最后一个交易日，
                  m是月最后一个交易日，可配合整数使用，表示几周或者几月
    :param direction: 追溯方向，str，pre是向过去追溯，post是向未来追溯
    :return: 得到的追溯后目标日期，str，%Y-%m-%d"
    """
    database = db_zcs
    if days == 0:
        target_date = t_date
    elif isinstance(days, int):
        if direction == 'pre':#sort('date',-1)为降序排序
            cursor = database.wind_trade_day.find({'date': {'$lte': t_date}}, {'_id': 0, 'date': 1}
                                                  ).sort('date', -1).limit(days)
        elif direction == 'post':
            cursor = database.wind_trade_day.find({'date': {'$gte': t_date}}, {'_id': 0, 'date': 1}).limit(days)
        target_date = [date['date'] for date in cursor][-1]

    elif isinstance(days, str):
        n = re.match(r'(\d+)(.*)', days)
        if n:#匹配成功，表示存在数字+str
            if direction == 'pre':#group（0）是匹配正则表达式整体结果 group(1)是（\d+）意为一个或多个数字
                cursor = database.wind_trade_day.find({'date': {'$lt': t_date}, '%s_last_trade_day' % n.group(2): 1},
                                                      {'_id': 0, 'date': 1}).sort('date', -1).limit(int(n.group(1)))
            elif direction == 'post':
                cursor = database.wind_trade_day.find({'date': {'$gt': t_date}, '%s_last_trade_day' % n.group(2): 1},
                                                      {'_id': 0, 'date': 1}).limit(int(n.group(1)))
        else:#表示不存在数字，默认为最近一个。
            if direction == 'pre':
                cursor = database.wind_trade_day.find({'date': {'$lt': t_date}, '%s_last_trade_day' % days: 1},
                                                      {'_id': 0, 'date': 1}).sort('date', -1).limit(1)
            elif direction == 'post':
                cursor = database.wind_trade_day.find({'date': {'$gt': t_date}, '%s_last_trade_day' % days: 1},
                                                      {'_id': 0, 'date': 1}).limit(1)
        target_date = [date['date'] for date in cursor][-1]
    return target_date


def get_next_date(t_date, freq='d'):
    """
    计算下一期的日期
    :param t_date: 当期的日期，str，"%Y-%m-%d"
    :param freq: 频率，str，'d'是日频，'m'是月频
    :return: 下一期的日期，str，"%Y-%m-%d"
    """
    database = db_zcs
    if freq == 'd':
        cursor = database.wind_trade_day.find({'date': {'$gt': t_date}}, {'_id': 0, 'date': 1}).limit(1)
        next_date = [it for it in cursor][0]['date']
    elif freq == 'm':
        cursor = database.wind_trade_day.find({'date': {'$gt': t_date}, 'm_last_trade_day': 1}, {'_id': 0, 'date': 1}).limit(1)
        next_date = [it for it in cursor][0]['date']
    return next_date


def util_get_trade_calendar():
    """
    获取交易日历
    :return: 交易日序列，list
    """
    database = db_zcs
    recs = database.ts_trade_cal.find({"is_open": 1})
    df_tmp = pd.DataFrame([rec for rec in recs])
    df_tmp = df_tmp.sort_values(by="cal_date", ascending=True)
    trade_calendar = df_tmp["cal_date"].tolist()
    return trade_calendar


trade_date_sse = util_get_trade_calendar()


def util_get_real_date(date, towards=-1, trade_list=trade_date_sse):
    """
    获取指定迭代方向上最近的一个交易日
    :param date: 迭代开始的日期，"%Y-%m-%d"
    :param towards: 迭代方向，int，1是向后迭代，-1是向前迭代
    :param trade_list: 交易日序列，list
    :return: 从迭代开始日期沿迭代方向开始迭代所得到的第一个交易日，str，"%Y-%m-%d"
    @ yutiansut
    """
    if towards == 1:
        while date not in trade_list:
            date = str(datetime.strptime(
                str(date)[0:10], '%Y-%m-%d') + timedelta(days=1))[0:10]
        else:
            return str(date)[0:10]
    elif towards == -1:
        while date not in trade_list:
            date = str(datetime.strptime(
                str(date)[0:10], '%Y-%m-%d') - timedelta(days=1))[0:10]
        else:
            return str(date)[0:10]


def util_get_closed_month_end(date):
    """
    获取离指定日期最近的上一月最后一个交易日
    :param date: 指定日期，str，"%Y-%m-%d"
    :return: 离指定日期最近的上一月最后一个交易日，str，"%Y-%m-%d"
    """
    ts = pd.Timestamp(date)
    if pd.offsets.BMonthEnd().is_on_offset(ts):
        ed = str(ts.date())
        ed = util_get_real_date(ed)
    else:
        ed = str((ts - pd.offsets.BMonthEnd()).date())
        ed = util_get_real_date(ed)
    return ed


def gen_last_trade_date():
    """
    获取当前时间之前的最近一个交易日
    :return: 当前时间之前的最近一个交易日，str，'%Y-%m-%d'
    """
    current_date = str(datetime.now().date())
    cursor = db_zcs.wind_trade_day.find({'date': {'$lte': current_date}}, {'_id': 0, 'date': 1}).sort('date', 1)
    lte_current_date_ls = [i['date'] for i in cursor]
    if current_date in lte_current_date_ls:
        return lte_current_date_ls[-2]
    else:
        return lte_current_date_ls[-1]


def is_window_start(date, window_type='m'):
    """
    判断指定日期是否为窗口期的开始日期
    :param date: 指定日期，str，"%Y-%m-%d"
    :param window_type: 窗口期类型，str，w表示周，m表示月，q表示季度
    :return: 判定结果，bool
    """
    last_date = shift_date(date, 2, 'pre')
    cursor = db_zcs.wind_trade_day.find({'date': last_date}, {'_id': 0, '%s_last_trade_day' % window_type: 1})
    data = [it for it in cursor][0]['%s_last_trade_day' % window_type]
    if data == 1:
        return True
    else:
        return False


def is_window_end(date, window_type='m'):
    """
    判断指定日期是否为窗口期的结束日期
    :param date: 指定日期，str，"%Y-%m-%d"
    :param window_type: 窗口期类型，str，w表示周，m表示月，q表示季度
    :return: 判定结果，bool
    """
    cursor = db_zcs.wind_trade_day.find({'date': date}, {'_id': 0, '%s_last_trade_day' % window_type: 1})
    data = [it for it in cursor][0]['%s_last_trade_day' % window_type]
    if data == 1:
        return True
    else:
        return False


def is_trade_date(date):
    """
    判断指定日期是否为交易日
    :param date: 指定日期，str，'%Y-%m-%d'
    :return: 判定结果，bool
    """
    cursor = db_zcs.wind_trade_day.find({'date': date})
    if len(list(cursor)) == 0:
        return False
    else:
        return True