import statsmodels.api as sm
from scipy import stats
from tool_kit import pd, np, db_zcs
from tool_kit.date_N_time import shift_date
from tool_kit.base_datastruct import block_data
from tool_kit.utility_tool import del_ST, del_suspended, del_newlist, do_orth, do_del_extremum, do_fill_nan, \
    do_neutralize, do_standardize, half_decay_weight


class FactorCal(object):
    """
    固定时间节点后，在横截面维度上完成因子数据获取，因子数据预处理，因子收益率计算，特异性收益率计算
    date：时间节点T期，str
    universe：股票池，A、hs300、zz500、zz800，str，若为list表示指定股票池
    freq：是计算频率，str，'d'是日频，'m'是月频
    tom_date：时间节点T+1期，str
    cal_return: 未来收益的计算方法，str，None是不匹配未来收益，standard是从数据库获取的next_date的动量数据，custom是计算date和next_date之间的真实价格变动
    style_factor_ls：风格因子名称列表，list
    industry_standard：行业分类标准，CS，SW，str，若为None则表示不获取行业代码
    d_ST：是否剔除ST股票，bool
    d_suspended：是否剔除suspended股票，bool
    d_newlist：是否剔除新股，bool
    del_extremum：是否去极值，bool
    fill_nan：是否填空值，bool
    neutralize：是否中性化，bool
    standardize：是否标准化，bool
    orth：是否正交化，bool
    """
    def __init__(self, date, universe, freq='', tom_date='', cal_return='standard', style_factor_ls=None,
                 industry_standard='CS', d_ST=True, d_suspended=True, d_newlist=True, del_extremum=True, fill_nan=True,
                 neutralize=True, standardize=True, orth=True):
        self.date = date
        self.universe = universe
        self.freq = freq
        self.tom_date = tom_date
        self.cal_return = cal_return
        self.style_factor_ls = style_factor_ls.copy()
        self.db = db_zcs
        self.industry_standard = industry_standard
        self.A_return = 0.0
        self.del_ST, self.del_suspended, self.del_newlist = d_ST, d_suspended, d_newlist
        self.del_extremum, self.fill_nan, self.neutralize, self.standardize, self.orth = del_extremum, fill_nan, neutralize, standardize, orth
        self.block_data_obj = object
        self.stock_universe = []
        self.raw_data_df = pd.DataFrame()
        self.extremum_multi = None
        self.industry_mean_df = pd.DataFrame()
        self.cap_weight_df = pd.DataFrame()
        self.process_data_df = pd.DataFrame()
        self.industry_factor_ls = []
        self.wls = object
        self.factor_return_series = pd.Series()
        self.specific_return_series = pd.Series()
        self.ic_series = pd.Series()
        self.pure_weight_df = pd.DataFrame()

    def get_stock_universe(self):
        """
        self.stock_universe：股票池，list
        """
        self.block_data_obj = block_data(date=self.date)
        if isinstance(self.universe, str):
            if self.universe == 'A':
                universe = 'a_share'
            else:
                universe = self.universe
            stock_universe = eval("self.block_data_obj.%s().copy()" % universe)
        else:
            stock_universe = self.universe.copy()
        self.stock_universe = stock_universe.copy()
        if self.del_ST:
            self.stock_universe = del_ST(self.date, self.date, self.stock_universe)
        if self.del_suspended:
            self.stock_universe = del_suspended(self.date, self.date, self.stock_universe)
        if self.del_newlist:
            self.stock_universe = del_newlist(self.date, self.date, self.stock_universe)

    def get_raw_factor(self):
        """
        self.stock_universe：剔除行业代码为None的股票池，list
        self.raw_data_df：因子原始数据DataFrame，pandas.DataFrame，index是股票代码，columns是[因子名称，free_float_shares, close, free_mkt, return, CS]
        """
        self.get_stock_universe()

        # 获取因子基础数据
        tprice_data = self.db.ts_daily_adj_factor.find({'date': self.date, 'code': {'$in': self.stock_universe}},
                                                       {'_id': 0, 'code': 1, 'close': 1})
        tprice_df = pd.DataFrame(it for it in tprice_data).set_index('code')
        share_data = self.db.wind_financial_2014.find({'date': self.date, 'code': {'$in': self.stock_universe}},
                                                      {'_id': 0, 'code': 1, 'free_float_shares': 1})
        share_df = pd.DataFrame(list(share_data)).set_index('code')
        if len(self.style_factor_ls) != 0:
            keyword_dt = {'_id': 0, 'code': 1}
            for f in self.style_factor_ls:
                keyword_dt.update({f: 1})
            factor_data = self.db.factor_barra.find({'date': self.date, 'code': {'$in': self.stock_universe}}, keyword_dt)
            data_df = pd.concat([pd.DataFrame(list(factor_data)).set_index('code'), tprice_df, share_df],
                                join='outer', axis=1).reindex(tprice_df.index)
        else:
            data_df = pd.concat([tprice_df, share_df], join='outer', axis=1).reindex(tprice_df.index)
        data_df['free_mkt'] = data_df['free_float_shares'] * data_df['close']

        # 获取明天的收益率
        if self.cal_return == 'standard':
            if self.freq == 'd':
                return_data = self.db.wind_financial_2014.find(
                    {'date': self.tom_date, 'code': {'$in': self.stock_universe}},
                    {'_id': 0, 'code': 1, 'd_return': 1})
                return_df = pd.DataFrame(it for it in return_data).set_index('code').rename(
                    columns={'d_return': 'return'})
                data_df = pd.concat([data_df, return_df], axis=1, join='inner')
            elif self.freq == 'm':
                return_data = self.db.wind_financial_2014.find(
                    {'date': self.tom_date, 'code': {'$in': self.stock_universe}},
                    {'_id': 0, 'code': 1, '1m_return': 1})
                return_df = pd.DataFrame(it for it in return_data).set_index('code').rename(
                    columns={'1m_return': 'return'})
                data_df = pd.concat([data_df, return_df], axis=1, join='inner')
        elif self.cal_return == 'custom':
            price_data = self.db.ts_daily_adj_factor.find(
                {'date': {'$in': [self.date, self.tom_date]}, 'code': {'$in': self.stock_universe}},
                {'_id': 0, 'code': 1, 'date': 1, 'close': 1, 'adj_factor': 1}
            )
            price_df = pd.DataFrame(it for it in price_data).sort_values(['code', 'date']).set_index('code')
            price_df['post_close'] = price_df['close'] * price_df['adj_factor']
            return_df = pd.DataFrame(
                data=price_df['post_close'].groupby(level=0, group_keys=False).apply(lambda x: x.pct_change().tail(1)))
            return_df.rename(columns={'post_close': 'return'}, inplace=True)
            data_df = pd.concat([data_df, return_df], axis=1, join='inner')

        # 获取行业代码
        if self.industry_standard is not None:
            if self.industry_standard == 'CS':
                industry_series = self.block_data_obj.CS()
            elif self.industry_standard == 'SW':
                industry_series = self.block_data_obj.SW()
            industry_series = industry_series[industry_series.index.isin(self.stock_universe)]
            industry_df = pd.DataFrame(data=industry_series.dropna(), columns=[self.industry_standard])
            self.raw_data_df = pd.concat([data_df, industry_df], axis=1, join='inner')
            self.stock_universe = self.raw_data_df.index.tolist()

        else:
            self.raw_data_df = data_df

    def process_raw_factor(self, raw_factor_df=None, style_factor_ls=None, extremum_multi=3.0):
        """
        :param raw_factor_df: 在类外部进行了调整和计算后的原始因子序列，pandas.DataFrame，index是股票代码，columns是[因子名称，free_float_shares, close, free_mkt, return, CS]
        :param style_factor_ls: 风格因子序列，list
        :param extremum_multi: 去极值偏离倍数，int/float
        self.raw_data_df：经过外部调整和计算的新的原始因子序列，pandas.DataFrame，index是股票代码，columns是[因子名称，free_float_shares, close, free_mkt, return, CS]
        self.style_factor_ls：新的风格因子名称序列，list
        self.industry_mean_df：风格因子行业均值，pandas.DataFrame，index是行业代码，columns是风格因子名称
        self.cap_weight_df：市值权重，pandas.DataFrame，index是股票代码，columns是[free_mkt, sqrtmkt, weight]
        self.process_data_df：数据预处理后的因子数据，pandas.DataFrame，index是股票代码，columns是[因子名称，行业代码，free_float_shares, close, free_mkt, return, CS]
        self.industry_factor_ls：行业代码列表，list
        """
        self.extremum_multi = extremum_multi
        self.get_raw_factor()
        if raw_factor_df is None:
            data_df = self.raw_data_df.copy()
        else:
            data_df = pd.concat([self.raw_data_df, raw_factor_df], axis=1, join='outer').reindex(self.raw_data_df.index)
            self.style_factor_ls = style_factor_ls
        if len(self.style_factor_ls) != 0:
            self.cap_weight_df = data_df[['free_mkt']]
            self.cap_weight_df.loc[:, 'sqrtmkt'] = np.sqrt(self.cap_weight_df['free_mkt'])
            self.cap_weight_df.loc[:, 'weight'] = self.cap_weight_df['sqrtmkt'] / self.cap_weight_df['sqrtmkt'].sum()
            if self.industry_standard is not None:
                # 行业虚拟变量
                industry_dummy_df = pd.get_dummies(data_df['CS'], prefix_sep='')
                self.industry_factor_ls = industry_dummy_df.columns.tolist()
                data_df = pd.concat([data_df, industry_dummy_df], axis=1, join='inner')
            """
            self.cap_weight_df.loc[:, 'weight'] = self.cap_weight_df['mkt_cap_ard']/self.cap_weight_df['mkt_cap_ard'].sum()
            self.cap_weight_df.loc[:, 'lnmkt'] = np.log(self.cap_weight_df['mkt_cap_ard'])
            self.cap_weight_df.loc[:, 'weight'] = self.cap_weight_df['lnmkt']/self.cap_weight_df['lnmkt'].sum()
            """
            """
            self.cap_weight_df['weight'] = (self.cap_weight_df['lnmkt'] - self.cap_weight_df['lnmkt'].mean())/self.cap_weight_df['lnmkt'].std()
            self.cap_weight_df['weight_w'] = self.cap_weight_df['weight']/self.cap_weight_df['weight'].sum()
            """
            data_df[self.style_factor_ls] = data_df[self.style_factor_ls].astype('float64')
            if len(self.style_factor_ls) != 1 and data_df[self.style_factor_ls].dropna(axis=1, how='all').shape[1] != len(self.style_factor_ls):
                self.style_factor_ls = data_df[self.style_factor_ls].dropna(axis=1, how='all').columns.tolist()
                data_df.dropna(axis=1, how='all', inplace=True)
            # 去极值
            if self.del_extremum:
                de_factor = data_df[self.style_factor_ls].apply(lambda x: do_del_extremum(x, multi=self.extremum_multi))
                data_df.update(de_factor)
            # 填空值
            if self.fill_nan:
                self.industry_mean_df = data_df.reset_index().groupby('CS', as_index=True)[self.style_factor_ls].mean()
                fn_factor = data_df[self.style_factor_ls].apply(lambda x: do_fill_nan(x, data_df['CS'], self.industry_mean_df))
                data_df.update(fn_factor)
                drop_part = len(data_df[data_df.isna().any(axis=1)])/len(data_df)
                if drop_part < 0.3:  # 如果填空值后仍为空值的样本少于30%，则剔除该部分样本
                    data_df.dropna(axis=0, inplace=True)
                    if len(data_df) != len(self.stock_universe):
                        # 如果存在样本剔除，则展示剔除比例，并更新self.stock_universe和self.cap_weight_df
                        print('drop data:', drop_part)
                        self.stock_universe = data_df.index.tolist()
                        self.cap_weight_df = self.cap_weight_df.loc[self.stock_universe]
                        self.cap_weight_df['weight'] = self.cap_weight_df['weight']/self.cap_weight_df['weight'].sum()
            # 市值和行业中性化
            if self.neutralize:
                neu_factor = data_df[self.style_factor_ls].apply(
                    lambda x: do_neutralize(x, cap_indus_factor=data_df[['free_mkt'] + self.industry_factor_ls]))
                data_df.update(neu_factor)
            # 正态化
            # normal_factor = data_df[self.style_factor_ls].apply(lambda x: boxcox_normal(x))
            # data_df.update(normal_factor)
            # 标准化
            if self.standardize:
                standard_factor = data_df[self.style_factor_ls].apply(lambda x: do_standardize(x, None))
                data_df.update(standard_factor)
            # 正交化
            if self.orth:
                if 'vol' in self.style_factor_ls and 'size' in self.style_factor_ls and 'beta' in self.style_factor_ls:
                    data_df['vol'] = do_orth(one_factor=data_df['vol'], independent_factor=data_df[['size', 'beta']],
                                             weight_factor=self.cap_weight_df['weight'])
                if 'liq' in self.style_factor_ls and 'size' in self.style_factor_ls:
                    data_df['liq'] = do_orth(one_factor=data_df['liq'], independent_factor=data_df['size'],
                                             weight_factor=self.cap_weight_df['weight'])
            self.process_data_df = data_df.copy()
        else:
            # 行业虚拟变量
            industry_dummy_df = pd.get_dummies(data_df['CS'], prefix_sep='')
            self.industry_factor_ls = industry_dummy_df.columns.tolist()
            self.process_data_df = pd.concat([data_df, industry_dummy_df], axis=1, join='inner')
