
import smtplib
import statsmodels.api as sm
from tool_kit import db_zcs, pd, np, datetime
from tool_kit.base_datastruct import block_data, basic_codes
from tool_kit.date_N_time import gen_trade_date, shift_date
from scipy import stats
from email.mime.text import MIMEText
from email.header import Header
from functools import wraps


def del_ST(s_date, e_date, initial_universe):
    """
    删除ST股票
    :param s_date: 开始日期，str，"%Y-%m-%d"
    :param e_date: 结束日期，str，"%Y-%m-%d"
    :param initial_universe: 初始股票池列表，list
    :return: 删除ST股票后的股票池列表，list
    """
    database = db_zcs
    st_stock = database.wind_block_2014.find(
        {'date': {'$gte': s_date, '$lte': e_date}, 'code': {'$in': initial_universe}, 'ST': 1},
        {'_id': 0, 'code': 1})
    st_stock_ls = list(set([stock['code'] for stock in st_stock]))
    for stock in st_stock_ls:
        initial_universe.remove(stock)
    return initial_universe


def del_suspended(s_date, e_date, initial_universe):
    """
    删除停牌股票
    :param s_date: 开始日期，str，"%Y-%m-%d"
    :param e_date: 结束日期，str，"%Y-%m-%d"
    :param initial_universe: 初始股票池列表，list
    :return:删除停牌股票后的股票池列表，list
    """
    database = db_zcs
    sus_stock = database.ts_daily_adj_factor.find(
        {'date': {'$gte': s_date, '$lte': e_date}, 'code': {'$in': initial_universe}, 'volume': 0},
        {'_id': 0, 'code': 1})
    sus_stock_ls = list(set([stock['code'] for stock in sus_stock]))
    for stock in sus_stock_ls:
        initial_universe.remove(stock)
    return initial_universe


def del_newlist(s_date, e_date, initial_universe, new_days=60):
    """
    剔除上市不足60日的股票
    :param s_date: 开始日期，str，"%Y-%m-%d"
    :param e_date: 结束日期，str，"%Y-%m-%d"
    :param initial_universe: 初始股票池列表，list
    :param new_days: 新股上市时间，int
    :return: 剔除上市不足new_days日的股票后的股票池列表，list
    """
    database = db_zcs
    cursor = database.ts_stock_basic.find({'code': {'$in': initial_universe}}, {'_id': 0, 'code': 1, 'list_date': 1})
    list_date_df = pd.DataFrame(it for it in cursor).set_index('code')
    pre_s_date = shift_date(t_date=s_date, days=new_days, direction='pre')
    new_stock_ls = list_date_df[(list_date_df['list_date'] >= pre_s_date) & (list_date_df['list_date'] <= e_date)].index.tolist()
    for stock in new_stock_ls:
        initial_universe.remove(stock)
    return initial_universe


def do_del_extremum(one_factor, multi=3.0):
    """
    对一日多股票单因子向量去极值函数
    :param one_factor: 一日多股票单因子向量，Series，index是股票代码
    :param multi：偏离倍数，int/float
    :return: 去极值后的一日多股票单因子向量，Series，index是股票代码
    """
    media = one_factor.median()
    deviation = abs(one_factor - media)
    mad = deviation.median()
    return one_factor


def do_fill_nan(one_factor, ind, ind_mean):
    """
    对一日多股票单因子向量填空值函数
    :param one_factor: 一日多股票单因子向量，Series，index是股票代码
    :param ind: 行业代码，Series，index是股票代码
    :param ind_mean:因子行业均值矩阵，pandas.DataFrame，index是行业代码，columns是因子名称
    :return:填空值后的一日多股票单因子向量，Series，index是股票代码
    """
    non_series = one_factor[one_factor.isna()]
    for stock in non_series.index:
        if ind[stock] is None:  # if isinstance(ind[stock], float) and np.isnan(ind[stock]):
            continue
        else:
            non_series[stock] = ind_mean.at[ind[stock], one_factor.name]
    one_factor.update(non_series)
    return one_factor


def do_neutralize(one_factor, cap_indus_factor=None):
    """
    对一日多股票单因子向量进行市值和行业中性化函数
    :param one_factor: 一日多股票单因子向量，pandas.Series，index是股票代码，name是因子名称
    :param cap_indus_factor: 市值因子和行业因子，pandas.DataFrame，index是股票代码，columns是[free_mkt，行业代码]
    :return: 中性化后的一日多股票单因子向量，pandas.Series，index是股票代码，name是因子名称
    """
    cap_indus_factor.loc[:, 'free_mkt'] = np.log(cap_indus_factor['free_mkt'])
    # 原算法
    # cap_indus_factor.loc[:, 'free_mkt'] = standardize(cap_indus_factor['free_mkt'], None)
    ols = sm.OLS(endog=one_factor, exog=cap_indus_factor, hasconst=False).fit()
    result_factor = ols.resid.rename(one_factor.name)
    return result_factor


def do_standardize(onefactor, cap_weight):
    """
    对一日多股票单因子向量标准化函数
    :param onefactor: 一日多股票单因子向量，Series，index是股票代码
    :param cap_weight: 市值权重，Series，index是股票代码，若为None则采用等权重均值做标准化
    :return: 标准化后的一日多股票单因子向量，Series，index是股票代码
    """
    if cap_weight is not None:
        onefactor = (onefactor - (onefactor*cap_weight).sum())/onefactor.std()
    else:
        onefactor = (onefactor - onefactor.mean()) / onefactor.std()
    return onefactor


def do_orth(one_factor, independent_factor, weight_factor):
    """
    对一日多股票单因子向量正交化函数
    :param one_factor: 一日多股票单因子向量，Series，index是股票代码
    :param independent_factor: 作为正交化回归解释变量的因子，pandas.DataFrame/pandas.Series，index是股票代码，columns是因子名
    :param weight_factor: 市值权重，Series，index是股票代码
    :return: 正交化后的一日多股票单因子向量，Series，index是股票代码
    """
    y = one_factor.values
    x = independent_factor.values
    wls = sm.WLS(y, x, weights=weight_factor**2)
    reg_result = wls.fit()
    resid_factor = pd.Series(reg_result.resid, index=one_factor.index)
    std_resid_factor = do_standardize(resid_factor, weight_factor)
    return std_resid_factor


def half_decay_weight(t_start=0, t_end=2, half_period=1, descend=True):
    """
    生成半衰权重序列
    :param t_start: 周期的开始时点，int
    :param t_end: 周期的结束时点，int
    :param half_period: 半衰期长度，int
    :param descend: 权重衰减方向，bool，True，大->小，False，小->大
    :return: 半衰权重序列，list
    """
    if half_period == 0:
        half_period = 1
    weight_vector = []
    for t in range(t_start, t_end+1):
        weight_vector.append(0.5 ** (t / half_period))
    weight_sum = sum(weight_vector)
    weight_vector = [i/weight_sum for i in weight_vector]
    weight_vector.sort(reverse=descend)
    return weight_vector


def send_email(sender='xxx', sender_name='', sender_ip='xxx', sender_port=465,
               sender_pwd='xxx', receiver='xxx', receiver_name='', title='', content=''):
    """
    发送邮件
    :param sender: 发件人地址，str
    :param sender_name: 发件人名称，str
    :param sender_ip: 发件人邮箱服务器IP地址，可在邮箱帮助-SMTP中查看，str
    :param sender_port: 发件人邮箱服务器端口，可在邮箱帮助-SMTP中查看，int
    :param sender_pwd: 发件人邮箱授权码，str
    :param receiver: 收件人地址，str
    :param receiver_name: 收件人名称，str
    :param title: 邮件主题，str
    :param content: 邮件内容，str
    :return: 无返回值，直接发送邮件至收件人邮箱
    """
    server = smtplib.SMTP_SSL(host=sender_ip, port=sender_port)
    server.login(user=sender, password=sender_pwd)
    message = MIMEText(content, 'plain', 'utf-8')
    message['From'] = Header(sender_name, 'utf-8')
    message['To'] = Header(receiver_name, 'utf-8')
    message['Subject'] = Header(title, 'utf-8')
    server.sendmail(from_addr=sender, to_addrs=receiver, msg=message.as_string())


def gen_universe(s_date, e_date, freq='d', index='A'):
    """
    生成一段时间内的指数成分股
    :param s_date: 开始日期，str，"%Y-%m-%d"
    :param e_date: 结束日期，str，"%Y-%m-%d"
    :param freq: 日期频率，str，'d'是日频，'w'是周频，'m'是月频
    :param index: 指数名称，str，sz50/zz500/zz800/hs300/A
    :return: 指数成分股列表，list
    """
    stock_universe = []
    if index == 'A':
        cursor0 = db_zcs.ts_daily_adj_factor.find({'date': {'$gte': s_date, '$lte': e_date}, 'code': {'$in': basic_codes}},
                                                  {'_id': 0, 'code': 1})
        stock_universe = [data['code'] for data in cursor0]
    else:
        trade_date_ls = gen_trade_date(s_date, e_date, freq=freq)
        for date in trade_date_ls:
            if index == 'A':
                index = 'a_share'
            stock_universe = stock_universe + eval("block_data(date='%s').%s()" % (date, index))
    stock_universe = list(set(stock_universe))
    stock_universe.sort()
    return stock_universe


def get_mkt_group(group_standard=None, stock_universe=None, date='', group_nums=10):
    """
    生成市值分组序列，若股票池中的股票不在基准股票池中，则用先ffill再bfill的方法填充
    :param group_standard: 分组基准股票池，可选A，hs300，zz500或传入股票池
    :param stock_universe: 目标股票池，list
    :param date: 分组日期，str
    :param group_nums: 分组数量，int
    :return: 市值分组序列，pandas.Series，index是股票代码
    """
    db = db_zcs
    if isinstance(group_standard, str):
        if group_standard == 'A':
            group_standard = 'a_share'
        standard_universe = eval("block_data(date='%s').%s()" % (date, group_standard))
    else:
        standard_universe = group_standard
    all_universe = list(set(stock_universe+standard_universe))
    all_universe.sort()
    price_cursor = db.ts_daily_adj_factor.find({'date': date, 'code': {'$in': all_universe}},
                                               {'_id': 0, 'code': 1, 'close': 1})
    tprice_df = pd.DataFrame(it for it in price_cursor).set_index('code')
    share_cursor = db.wind_financial_2014.find({'date': date, 'code': {'$in': all_universe}},
                                               {'_id': 0, 'code': 1, 'free_float_shares': 1})
    share_df = pd.DataFrame(it for it in share_cursor).set_index('code')
    cap_df = pd.concat([tprice_df, share_df], join='inner', axis=1)
    cap_df['free_mkt'] = cap_df['close'] * cap_df['free_float_shares']
    group_series = pd.qcut(x=cap_df[cap_df.index.isin(standard_universe)]['free_mkt'], q=group_nums, labels=False
                           ).rename('mkt_group')
    cap_df = pd.concat([cap_df, group_series], join='outer', axis=1, sort=False).sort_values(by='free_mkt')
    cap_df['mkt_group'] = cap_df['mkt_group'].ffill()
    cap_df['mkt_group'] = cap_df['mkt_group'].bfill()
    result_df = cap_df[cap_df.index.isin(stock_universe)]
    return result_df['mkt_group']


def back_fill(df, col):
    """
    用backfill的方法填充空值，配合apply使用
    :param df: 原始DataFrame变量，pandas.DataFrame，index是股票代码，columns是因子名称
    :param col:列名，str
    :return:col列填充后的df，pandas.DataFrame，index是股票代码，columns是因子名称
    """
    df[col] = df[col].bfill()
    return df


def boxcox_normal(onefactor):
    """
    对一日多股票单因子向量正态化函数
    :param onefactor: 一日多股票单因子向量，Series，index是股票代码
    :return: 正态化后的一日多股票单因子向量，Series，index是股票代码
    """
    positive_factor = onefactor - onefactor.min() + 0.1*(onefactor.max()-onefactor.min())
    if positive_factor.median() < 1:
        positive_factor = positive_factor*100
    elif positive_factor.median() > 10000:
        positive_factor = positive_factor / 100
    normal_factor = stats.boxcox(positive_factor)[0]
    return normal_factor


def symmetric_orth(fct_df):
    """
    因子对称正交化函数
    :param fct_df: 数据预处理后的因子矩阵，pandas.DataFrame，index是股票名称，columns是因子名称
    :return: 对称正交化后的因子矩阵，pandas.DataFrame，index是股票名称，columns是因子名称
    """
    F = fct_df.values
    fct_overlap = F.T @ F
    eigenvalue, eigenvector = np.linalg.eig(fct_overlap)
    diag_matrix = np.diag(eigenvalue ** (-0.5))
    transition_matrix = eigenvector @ diag_matrix @ eigenvector.T
    F_orth = F @ transition_matrix
    orth_fct_df = pd.DataFrame(F_orth, columns=fct_df.columns, index=fct_df.index)
    return orth_fct_df


# def get_direction(onefactor_ic=None):
#     positive_len = len(onefactor_ic[onefactor_ic>0])
#     negative_len = len(onefactor_ic[onefactor_ic<0])
#     if positive_len > negative_len:
#         return 1
#     elif positive_len < negative_len:
#         return -1
#     else:
#         return np.nan


def get_cap(universe, s_date, e_date):
    """
    获取流通市值
    :param universe: 股票池，list
    :param s_date: 开始日期，str，'%Y-%m-%d'
    :param e_date: 结束日期，str，'%Y-%m-%d'
    :return: 股票池中的股票在这段时间的流通市值，pandas.DataFrame，index是[股票代码，日期]，columns是[free_mkt]
    """
    db = db_zcs
    cursor1 = db.ts_daily_adj_factor.find({'code': {'$in': universe}, 'date': {'$gte': s_date, '$lte': e_date}},
                                          {'_id': 0, 'code': 1, 'date': 1, 'close': 1})
    price_df = pd.DataFrame(list(cursor1)).set_index(['code', 'date'])
    cursor2 = db.wind_financial_2014.find({'code': {'$in': universe}, 'date': {'$gte': s_date, '$lte': e_date}},
                                          {'_id': 0, 'code': 1, 'date': 1, 'free_float_shares': 1})
    share_df = pd.DataFrame(list(cursor2)).set_index(['code', 'date'])
    data_df = pd.concat([price_df, share_df], axis=1, join='inner', sort=True)
    data_df['free_mkt'] = data_df['close'] * data_df['free_float_shares']
    return data_df[['free_mkt']]


def get_industry_dummies(universe, s_date, e_date):
    """
    获取行业虚拟变量
    :param universe: 股票池，list
    :param s_date: 开始日期，str，'%Y-%m-%d'
    :param e_date: 结束日期，str，'%Y-%m-%d'
    :return: 股票池中的股票在这段时间的行业虚拟变量，pandas.DataFrame，index是[股票代码，日期]，columns是[行业代码]
    """
    date_ls = gen_trade_date(s_date, e_date, freq='d')
    industry_dummy_df_ls = []
    for date in date_ls:
        industry_df = block_data(date=date).CS().to_frame()
        industry_dummy_df = pd.get_dummies(industry_df['CS'], prefix_sep='')
        industry_dummy_df = industry_dummy_df[industry_dummy_df.index.isin(universe)]
        industry_dummy_df['date'] = date
        industry_dummy_df.set_index('date', append=True, inplace=True)
        industry_dummy_df_ls.append(industry_dummy_df)
    all_industry_df = pd.concat(industry_dummy_df_ls, axis=0, join='outer')
    return all_industry_df


# def cal_rho(gama, alpha=None, beta=None):
#     """
#     顶端优化算法中计算rho的函数
#     :param gama: 标量，float
#     :param alpha: numpy.ndarray，(m, )
#     :param beta: numpy.ndarray，(n, )
#     :return: rho值，float
#     """
#     sum_a = 0
#     sum_b = 0
#     for a in alpha:
#         sum_a = sum_a + max(a-gama, 0)
#     for b in beta:
#         sum_b = sum_b + max(b+gama, 0)
#     return sum_a - sum_b


# def top_rank(positive_instance_df=None, negative_instance_df=None, lamb=0.02, epsilon=1):
#     """
#     利用顶端优化算法，优化因子组合权重
#     :param positive_instance_df: 正例样本，pandas.DataFrame，index是股票代码，columns是因子名称
#     :param negative_instance_df: 负例样本，pandas.DataFrame，index是股票代码，columns是因子名称
#     :param lamb: 模型参数，float
#     :param epsilon: 模型精度，float
#     :return: 优化出的因子权重向量，pandas.Series，index是因子名称
#     """
#     m = len(positive_instance_df)
#     n = len(negative_instance_df)
#     X_p = positive_instance_df.values
#     X_n = negative_instance_df.values
#
#     def g(alpha=None, beta=None):
#         v = np.dot(alpha, X_p) - np.dot(beta, X_n)
#         l_star = -1 * alpha + (alpha ** 2) / 4
#         g = np.dot(v, v) / (2 * lamb * m) + sum(l_star)
#         return g
#
#     def g_ab_kplus1(L_k, s_a_k=None, s_b_k=None, g_a=None, g_b=None):
#         alpha_kplus1_pi = s_a_k - g_a / L_k
#         beta_kplus1_pi = s_b_k - g_b / L_k
#         gama = fsolve(cal_rho, x0=np.array([0]), args=(alpha_kplus1_pi, beta_kplus1_pi))[0]
#         alpha_kplus1 = np.array([a - gama if a - gama > 0 else 0 for a in alpha_kplus1_pi])
#         beta_kplus1 = np.array([b + gama if b + gama > 0 else 0 for b in beta_kplus1_pi])
#         g_result = g(alpha_kplus1, beta_kplus1)
#         return g_result, alpha_kplus1, beta_kplus1
#
#     alpha_ls = [np.zeros(m), np.zeros(m)]
#     beta_ls = [np.zeros(n), np.zeros(n)]
#     t_ls = [1]
#     L_ls = [1 / (m + n)]
#     k = 1
#     while True:
#         # print('k:%s' % k)
#         if k == 1:
#             w_k = (0 - 1) / t_ls[k - 1]
#         else:
#             w_k = (t_ls[k - 2] - 1) / t_ls[k - 1]
#         L_k = L_ls[k-1]
#         s_alpha_k = alpha_ls[k] + w_k * (alpha_ls[k] - alpha_ls[k - 1])
#         s_beta_k = beta_ls[k] + w_k * (beta_ls[k] - beta_ls[k - 1])
#         v = np.dot(s_alpha_k, X_p) - np.dot(s_beta_k, X_n)
#         g_alpha = np.dot(X_p, v) / (lamb * m) - 1 + 0.5 * s_alpha_k
#         g_beta = -1 * np.dot(X_n, v) / (lamb * m)
#
#         # # 修改后算法
#         # def obj_func(L_k):
#         #     return L_k
#         #
#         # cons = ({'type': 'ineq', 'fun': lambda x: x - L_ls[k - 1]},
#         #         {'type': 'ineq',
#         #          'fun': lambda x: g(s_alpha_k, s_beta_k) + (np.dot(g_alpha, g_alpha) + np.dot(g_beta, g_beta)) / (
#         #                      2 * x) - g_ab_kplus1(x, s_alpha_k, s_beta_k, g_alpha, g_beta)[0]}
#         #         )
#         #
#         # ret = minimize(obj_func, x0=np.array([1/(m+n)]), constraints=cons, method='SLSQP', options={'disp': True})
#         # if ret.success:
#         #     L_k = ret.x[0]
#         #     g_result_ls = g_ab_kplus1(L_k, s_alpha_k, s_beta_k, g_alpha, g_beta)
#         #     alpha_kplus1 = g_result_ls[1]
#         #     beta_kplus1 = g_result_ls[2]
#         #     alpha_ls.append(alpha_kplus1)
#         #     beta_ls.append(beta_kplus1)
#         #     L_ls.append(L_k)
#         # else:
#         #     print('优化失败')
#         #     L_k = L_ls[k-1]
#         #     gama = fsolve(cal_rho, x0=np.array([0]), args=(s_alpha_k, s_beta_k))[0]
#         #     alpha_kplus1 = np.array([a - gama if a - gama > 0 else 0 for a in s_alpha_k])
#         #     beta_kplus1 = np.array([b + gama if b + gama > 0 else 0 for b in s_beta_k])
#         #     alpha_ls.append(alpha_kplus1)
#         #     beta_ls.append(beta_kplus1)
#         #     L_ls.append(L_k)
#         # 原算法
#         while True:
#             # print('L_k:%s' % L_k)
#             alpha_kplus1_pi = s_alpha_k - g_alpha / L_k
#             beta_kplus1_pi = s_beta_k - g_beta / L_k
#             if np.absolute(max(g_alpha / L_k)) <= 1.0e-16:
#                 print('无限循环')
#                 return
#             gama = fsolve(cal_rho, x0=np.array([0]), args=(alpha_kplus1_pi, beta_kplus1_pi))[0]
#             # # 原算法
#             # U_alpha = [i for i in alpha_kplus1_pi]
#             # U_beta = [-1*i for i in beta_kplus1_pi]
#             # U = U_alpha + U_beta
#             # s_alpha, s_beta, n_alpha, n_beta = 0, 0, 0, 0
#             # while len(U) != 0:
#             #     # print('len(U): %s' % len(U))
#             #     i = random.randint(0, len(U)-1)
#             #     u = U[i]
#             #     big_g_alpha = []
#             #     big_l_alpha = []
#             #     for a in U_alpha:
#             #         if a > u:
#             #             big_g_alpha.append(a)
#             #         else:
#             #             big_l_alpha.append(a)
#             #     big_g_beta = []
#             #     big_l_beta = []
#             #     for b in U_beta:
#             #         if b >= u:
#             #             big_g_beta.append(b)
#             #         else:
#             #             big_l_beta.append(b)
#             #     delta_n_alpha = len(big_g_alpha)
#             #     delta_s_alpha = sum(big_g_alpha)
#             #     delta_n_beta = len(big_l_beta)
#             #     delta_s_beta = sum(big_l_beta)
#             #     s_pi = s_alpha+delta_s_alpha+s_beta+delta_s_beta
#             #     n_pi = n_alpha+delta_n_alpha+n_beta+delta_n_beta
#             #     if s_pi < n_pi*u:
#             #         U_alpha = [i for i in big_l_alpha if i != u]
#             #         U_beta = big_l_beta
#             #         s_alpha = s_alpha + delta_s_alpha
#             #         n_alpha = n_alpha + delta_n_alpha
#             #     else:
#             #         U_alpha = [i for i in big_g_alpha if i != u]
#             #         U_beta = big_g_beta
#             #         s_beta = s_beta + delta_s_beta
#             #         n_beta = n_beta + delta_n_beta
#             #     U = U_alpha + U_beta
#             #     # U = [i for i in U if i != u]
#             #     print(1)
#             #     # del U[i]
#             # gama = (s_alpha + s_beta)/(n_alpha + n_beta)
#             alpha_kplus1 = np.array([a - gama if a - gama > 0 else 0 for a in alpha_kplus1_pi])
#             beta_kplus1 = np.array([b + gama if b + gama > 0 else 0 for b in beta_kplus1_pi])
#             cha = g(alpha=alpha_kplus1, beta=beta_kplus1) - g(alpha=s_alpha_k, beta=s_beta_k) - (
#                         np.dot(g_alpha, g_alpha) + np.dot(g_beta, g_beta)) / (2 * L_k)
#             # print('cha: %s' % cha)
#             if cha <= 0:
#                 break
#             L_k = 2 * L_k
#         alpha_ls.append(alpha_kplus1)
#         beta_ls.append(beta_kplus1)
#         L_ls.append(L_k)
#         t_k = (1 + np.sqrt(1 + 4 * (t_ls[k - 1] ** 2))) / 2
#         t_ls.append(t_k)
#         # print('eps: %s' % np.absolute(g(alpha_kplus1, beta_kplus1) - g(alpha_ls[k], beta_ls[k])))
#         # print('g: %s' % g(alpha_kplus1, beta_kplus1))
#         if np.absolute(g(alpha_kplus1, beta_kplus1) - g(alpha_ls[k], beta_ls[k])) < epsilon:
#             w = (np.dot(alpha_ls[k], X_p) - np.dot(beta_ls[k], X_n)) / (lamb * m)
#             break
#         k += 1
#     weight_df = pd.Series(data=w, index=positive_instance_df.columns)
#     return weight_df
def cal_period_return(s_date='', e_date='', stock_universe=None):
    """
    计算一段时间的收益率
    :param s_date: 开始日期，str，"%Y-%m-%d"
    :param e_date: 结束日期，str，"%Y-%m-%d"
    :param stock_universe: 股票池，list
    :return: 股票池中的股票在s_date和e_date之间
    """
    db = db_zcs
    cursor = db.ts_daily_adj_factor.find(
        {'date': {'$in': [s_date, e_date]}, 'code': {'$in': stock_universe}},
        {'_id': 0, 'code': 1, 'date': 1, 'close': 1, 'adj_factor': 1})
    price_df = pd.DataFrame(it for it in cursor).sort_values(['code', 'date']).set_index('code')
    price_df['post_close'] = price_df['close'] * price_df['adj_factor']
    return_series = price_df['post_close'].groupby(level=0, group_keys=False).apply(lambda x: x.pct_change().tail(1))
    return_series.rename('return', inplace=True)
    return return_series


def gen_daily_return_matrix(s_date='', e_date='', stock_universe=None):
    """
    生成一段时间的日频股票收益率矩阵
    :param s_date: 开始日期，str，"%Y-%m-%d"
    :param e_date: 结束日期，str，"%Y-%m-%d"
    :param stock_universe: 股票池，list
    :return: 日频股票收益率矩阵，index是日期，columns是股票代码
    """
    db = db_zcs
    cursor = db.ts_daily_adj_factor.find({'date': {'$gte': s_date, '$lte': e_date}, 'code': {'$in': stock_universe}},
                                         {'_id': 0, 'code': 1, 'date': 1, 'close': 1, 'adj_factor': 1})
    price_df = pd.DataFrame(it for it in cursor).set_index(['code', 'date'])
    price_df['post_close'] = price_df['close'] * price_df['adj_factor']
    price_df['return'] = price_df['post_close'].groupby(level=0).pct_change()
    return_df = price_df['return'].unstack(level=0).dropna(axis=0)
    return return_df


def gen_continuous_position(discrete_position_df=None, end_date=''):
    """
    生成连续持仓，每个交易日都会对持仓股票的权重进行调整
    :param discrete_position_df: 非连续持仓数据，pandas.DataFrame，index是股票代码，columns是日期
    :param end_date: 结束持仓日期，str，"%Y-%m-%d"
    :return: 连续持仓数据，pandas.DataFrame，index是股票代码，columns是连续持仓日期
    """
    db = db_zcs
    universe = discrete_position_df.index.tolist()
    date_ls = gen_trade_date(discrete_position_df.columns[0], end_date)
    cursor = db.ts_daily_adj_factor.find({'date': {'$gte': discrete_position_df.columns[0],
                                                   '$lte': end_date},
                                          'code': {'$in': universe}},
                                         {'_id': 0, 'code': 1, 'date': 1, 'close': 1, 'adj_factor': 1})
    price_df = pd.DataFrame(list(cursor)).set_index(['code', 'date'])
    price_df['post_close'] = price_df['close'] * price_df['adj_factor']
    return_df = price_df['post_close'].unstack(level=1)
    return_df = return_df.pct_change(axis=1)

    for trade_date in discrete_position_df.columns:
        position_series = discrete_position_df.loc[:, trade_date]
        position_series = position_series.loc[
            del_suspended(trade_date, trade_date, discrete_position_df.index.tolist())]
        position_series = position_series / position_series.sum()
        discrete_position_df.update(position_series)

    position_ls = [discrete_position_df.iloc[:, 0].dropna()]
    for date in date_ls[1:]:
        if date in discrete_position_df.columns.tolist():
            temp_df1 = pd.concat([position_ls[-1].rename('weight'), return_df.loc[:, date].rename('return')],
                                 join='inner',
                                 axis=1)
            temp_df1['new_weight'] = temp_df1['weight'] * (1 + temp_df1['return'])
            pre_suspended = list(set(temp_df1.index.tolist()) - set(del_suspended(date, date, temp_df1.index.tolist())))
            position_series = discrete_position_df.loc[:, date].dropna()
            valid_weight = temp_df1.loc[del_suspended(date, date, temp_df1.index.tolist()), 'new_weight'].sum()
            position_series = position_series * valid_weight
            universe = list(set(position_series.index.tolist() + pre_suspended))
            universe.sort()
            position_series = position_series.reindex(index=universe)
            position_series.loc[pre_suspended] = temp_df1.loc[pre_suspended, 'new_weight'].copy()
            position_series = position_series/position_series.sum()
            position_ls.append(position_series)
        else:
            position_series = position_ls[-1]
            temp_df1 = pd.concat([position_series.rename('weight'), return_df.loc[:, date].rename('return')],
                                 join='inner',
                                 axis=1)
            temp_df1['new_weight'] = temp_df1['weight'] * (1 + temp_df1['return'])
            temp_df1['new_weight'] = temp_df1['new_weight'] / temp_df1['new_weight'].sum()
            position_ls.append(temp_df1['new_weight'].rename(date))
    position_df = pd.concat(position_ls, axis=1, join='outer')
    return position_df


def select_prefix_by_type(ins_type):
    """
    获取指定证券类型的代码前缀, 主要为了区分股票、债券、基金
    :param ins_type: 证券类型，str，Stock/CBond/ETF/Fund/Option
    :return: 前缀列表，list，元素是前缀，str
    """
    if ins_type == 'Stock':
        prefix = ['60', '68'] + ['00', '30']
    elif ins_type == 'CBond':
        prefix = ['110', '113', '132'] + ['123', '127', '128']
    elif ins_type in ['ETF', 'Fund']:
        prefix = ['510', '511', '512', '513', '515', '518'] + ['159']
    elif ins_type == 'Option':
        prefix = ['100'] + ['900']
    else:
        raise RuntimeError('invalid type!')
    return prefix


def update_zcs_update(update_date, update_part):
    """
    更新_zcs_update中的数据，数据库更新完成后添加成功标签和更新完成时间到对应数据库区块
    :param update_date: 数据更新日期，str，%Y-%m-%d
    :param update_part: 数据库区块名称，str，part1/part1/part1/part1/sql_t/sql_m/bundle
    :return: 无返回值，直接执行update操作
    """
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    if len(list(db_zcs['_zcs_update'].find({'date': update_date}))) == 0:
        db_zcs['_zcs_update'].insert_many([{'date': update_date,
                                            'part1': False, 'part1_dt': None,
                                            'part2': False, 'part2_dt': None,
                                            'part3': False, 'part3_dt': None,
                                            'part4': False, 'part4_dt': None,
                                            'part5': False, 'part5_dt': None,
                                            'part6': False, 'part6_dt': None,
                                            'sql_t': False, 'sql_t_dt': None,
                                            'sql_m': False, 'sql_m_dt': None,
                                            'bundle': False, 'bundle_dt': None,
                                            'bundle110': False, 'bundle110_dt': None,
                                            'bundle119_2': False, 'bundle119_2_dt': None,
                                            'mint_ic': False, 'mint_ic_dt': None}])
    db_zcs['_zcs_update'].update_one({'date': update_date},
                                     {'$set': {update_part: True, update_part+'_dt': current_time}},
                                     upsert=True)


def is_mongo_finished(update_date, part):
    """
    判断当天是否完成数据落地
    :param update_date: 更新日期，str，%Y-%m-%d
    :param part: 指定数据库区域名称，str，part1/part2/part3/part4/...
    :return: 返回指定区域是否更新完成的bool变量，bool
    """
    status_coll = db_zcs['_zcs_update']
    ref = status_coll.find({"date": update_date})
    if ref.count() > 0:
        rec = ref[0]
        if rec.get(part):
            return True
    else:
        return False


def update_from_df(raw_data, table_name=''):
    """
    从DataFrame中获取数据更新到database
    :param raw_data: 原始数据，pandas.DataFrame，index是(股票代码，日期)，columns是字段名称
    :param table_name: 数据表名称，str
    :return: 无返回值，更新至数据库.zcs指定数据表中
    """
    data_ls = raw_data.to_dict(orient='records')
    length = len(raw_data)
    for i in range(length):
        print(i / length)
        ind = raw_data.index[i]
        db_zcs[table_name].update_one({'code': [e1 for e1 in ind if '.' in e1][0],
                                       'date': [e2 for e2 in ind if '-' in e2][0]},
                                      {'$set': data_ls[i]}, upsert=True)


def match_code(raw_df, table=''):
    """
    将DataFrame的index由数字代码替换为wind代码
    :param raw_df: 原始DataFrame，pandas.DataFrame，index是数字代码
    :param table: 查询wind代码的数据表，str
    :return: index替换为wind代码的DataFrame，pandas.DataFrame，index是wind代码
    """
    cursor = db_zcs[table].find({}, {'_id': 0, 'code': 1})
    standard_df = pd.DataFrame(list(cursor))
    standard_df['代码'] = standard_df['code'].str[:6]
    standard_df.set_index('代码', inplace=True)
    raw_df.index = raw_df.index.astype(str)
    raw_df = pd.merge(raw_df, standard_df, how='left', on='代码')
    raw_df = raw_df.reset_index('代码', drop=True).set_index('code')
    return raw_df


def cal_indicator(net_value_df):
    """
    根据净值序列计算annualized_returns、max_drawdown、sharpe三种回测指标
    :param net_value_df: 净值序列，pandas.DataFrame，index是日期，columns是策略名称
    :return: 回测指标，pandas.DataFrame，index是策略名称，columns是回测指标名称
    """
    annual_return = net_value_df.iloc[-1, :] ** (252.0 / len(net_value_df)) - 1
    drawdown_df = net_value_df.apply(lambda x: x / net_value_df.loc[:x.name, :].max() - 1, axis=1)
    max_drawdown = drawdown_df.min() * -1
    daily_ret_df = net_value_df.pct_change().fillna(0)
    sharpe_ratio = daily_ret_df.mean() / daily_ret_df.std() * (252 ** 0.5)
    indicator_df = pd.concat([annual_return.rename('annualized_returns'), max_drawdown.rename('max_drawdown'),
                              sharpe_ratio.rename('sharpe')], axis=1, join='outer')
    return indicator_df

