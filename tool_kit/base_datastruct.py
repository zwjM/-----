
import statistics
from copy import copy
from tool_kit import db_zcs, np, pd, datetime, timedelta
from tool_kit.date_N_time import util_get_real_date, util_get_closed_month_end
from functools import lru_cache


db = db_zcs
trade_date = db.wind_trade_day.find({})
trade_df = pd.DataFrame(list(trade_date))
trade_date_sse = trade_df['date'].tolist()
ref_D = db.ts_stock_basic.find({})
ref_ST = db.wind_ST.find({})
cursor0 = db.ts_stock_basic.find({}, {'_id': 0, 'code': 1})
basic_codes = [data['code'] for data in cursor0]


class bar_data(object):
    def __init__(self, code=None, date=None, start=None, end=None, stock_list=None, n=None, coll=db.ts_daily_adj_factor):
        """
        股票行情数据接口
        :param code: 股票代码，str
        :param date: 日期，str，"%Y-%m-%d"
        :param start: 开始日期，str，"%Y-%m-%d"
        :param end: 结束日期，str，"%Y-%m-%d"
        :param stock_list: 股票池，list
        :param n: 交易日数量，int，+为向未来增加，-为向过去增加
        :param coll: 数据库变量，被连接的document为ts_daily_adj_factor
        """
        self.code = code
        self.stock_list = stock_list
        if date is not None:
            date = util_get_real_date(date, towards=-1)
        self.date = date
        self.coll_bar = coll
        self.start = start
        self.end = end
        self.n = n
        if self.code is not None and self.date is not None and self.n is None:
            self.coll = self.coll_bar.find({'date': self.date, 'code': self.code})
        elif self.code is not None and self.date is not None and self.n is not None:
            if self.n > 0:
                try:
                    self.coll = self.coll_bar.find(
                        {'date': {'$gte': self.date, '$lt': trade_date_sse[trade_date_sse.index(self.date) + self.n]},
                         'code': self.code})
                except Exception:
                    self.coll = self.coll_bar.find({'date': {'$gte': self.date}, 'code': self.code})
            else:
                try:
                    self.coll = self.coll_bar.find(
                        {'date': {'$gt': trade_date_sse[trade_date_sse.index(self.date) + self.n], '$lte': self.date},
                         'code': self.code})
                except Exception:
                    self.coll = self.coll_bar.find({'date': {'$lte': self.date}, 'code': self.code})
        elif self.start is not None and self.code is not None:
            if self.end is None:
                self.coll = self.coll_bar.find({'code': self.code, 'date': {'$gte': self.start}})
            else:
                self.coll = self.coll_bar.find({'code': self.code, 'date': {'$gte': self.start, '$lte': self.end}})
        elif self.stock_list is not None and self.date is not None and self.n is None:
            self.coll = self.coll_bar.find({'code': {'$in': self.stock_list}, 'date': self.date})
        elif self.stock_list is not None and self.date is not None and self.n is not None:
            if self.n > 0:
                try:
                    self.coll = self.coll_bar.find({'date': {'$gte': self.date, '$lt': trade_date_sse[
                        trade_date_sse.index(self.date) + self.n]}, 'code': {'$in': self.stock_list}})
                except Exception:
                    self.coll = self.coll_bar.find({'date': {'$gte': self.date}, 'code': {'$in': self.stock_list}})
            else:
                try:
                    self.coll = self.coll_bar.find({'date': {
                        '$gt': trade_date_sse[trade_date_sse.index(self.date) + self.n], '$lte': self.date},
                        'code': {'$in': self.stock_list}})
                except Exception:
                    self.coll = self.coll_bar.find({'date': {'$lte': self.date}, 'code': {'$in': self.stock_list}})
        elif self.stock_list is not None and self.start is not None:
            if self.end is None:
                self.coll = self.coll_bar.find({'code': {'$in': self.stock_list}, 'date': {'$gte': self.start}})
            else:
                self.coll = self.coll_bar.find(
                    {'code': {'$in': self.stock_list}, 'date': {'$gte': self.start, '$lte': self.end}})
        elif self.start is None and self.stock_list is None and self.code is None and self.date is not None:
            self.coll = self.coll_bar.find({'date': self.date})
        elif self.date is None and self.stock_list is None and self.code is None and self.start is not None:
            if self.end is None:
                self.coll = self.coll_bar.find({'date': {'$gte': self.start}})
            else:
                self.coll = self.coll_bar.find({'date': {'$gte': self.start, '$lte': self.end}})
        self.data = pd.DataFrame(item for item in self.coll).set_index(['date', 'code']).sort_index()
        self.data.drop(['_id'], axis=1, inplace=True)

    def __call__(self):
        """
        ✅如果需要暴露 DataFrame 内部数据对象，就用__call__来转换出 data （DataFrame）
        Emulating callable objects
        object.__call__(self[, args…])
        Called when the instance is “called” as a function;
        if this method is defined, x(arg1, arg2, ...) is a shorthand for x.__call__(arg1, arg2, ...).
        比如
        obj =  _quotation_base() 调用 __init__
        df = obj()  调用 __call__
        等同 df = obj.__call__()
        :return: 数据查询结果，pandas.DataFrame，index是日期和股票代码，columns是字段名称
        """
        return self.data

    def __len__(self):
        """
        返回记录的数目
        :return: 查询到的数据长度，int
        """
        return len(self.index)

    def __iter__(self):
        """
        📌关于 yield 的问题
        A yield statement is semantically equivalent to a yield expression.
        yield 的作用就是把一个函数变成一个 generator，
        带有 yield 的函数不再是一个普通函数，Python 解释器会将其视为一个 generator
        for iterObj in ThisObj
        📌关于__iter__ 的问题
        可以不被 __next__ 使用
        Return an iterator object
        iter the row one by one
        :return: 迭代生成器，每一次迭代为查询数据的一行
        """
        for i in range(len(self.index)):
            yield self.data.iloc[i]

    def __add__(self, DataStruct):
        """
        ➕合并数据，重复的数据drop
        :param DataStruct: _quotation_base 继承的子类  QA_DataStruct_XXXX
        :return: _quotation_base 继承的子类  QA_DataStruct_XXXX
        """
        assert isinstance(DataStruct, bar_data)
        return self.new(self.data.append(DataStruct.data).drop_duplicates())

    __radd__ = __add__

    def __sub__(self, DataStruct):
        """
        ⛔️不是提取公共数据， 去掉 DataStruct 中指定的数据
        :param DataStruct:  _quotation_base 继承的子类  QA_DataStruct_XXXX
        :return: _quotation_base 继承的子类  QA_DataStruct_XXXX
        """
        assert isinstance(DataStruct, bar_data)
        return self.new(data=self.data.drop(DataStruct.index).set_index(self.index.names, drop=False))

    __rsub__ = __sub__

    def __getitem__(self, key):
        """
        # 🛠todo 进一步研究 DataFrame __getitem__ 的意义。
        DataFrame调用__getitem__调用(key)
        :param key: 行标签或者列标签
        :return: 标签对应的行或列
        """
        data_to_init = self.data.__getitem__(key)
        if isinstance(data_to_init, pd.DataFrame) is True:
            return self.new(data=data_to_init)
        elif isinstance(data_to_init, pd.Series) is True:
            return data_to_init

    def __getattr__(self, attr):
        """
        # 🛠todo 为何不支持 __getattr__ ？？
        :param attr: 属性名称，str
        :return: self.data的该属性对应的值
        """
        try:
            self.new(data=self.data.__getattr__(attr))
        except:
            raise AttributeError('DataStruct_* Class Currently has no attribute {}'.format(attr))

    '''
    ########################################################################################################
    获取序列
    '''

    # def ix(self, key):
    #     return self.new(data=self.data.ix(key), dtype=self.type, if_fq=self.if_fq)
    #
    # def iloc(self, key):
    #     return self.new(data=self.data.iloc(key), dtype=self.type, if_fq=self.if_fq)
    #
    # def loc(self, key):
    #     return self.new(data=self.data.loc(key), dtype=self.type, if_fq=self.if_fq)

    # @property
    # # @lru_cache()
    def open(self):
        """
        :return: 开盘价，pandas.Series，index是[日期，股票代码]
        """
        return self.data.open

    # @property
    # @lru_cache(1024)
    def high(self):
        """
        :return: 最高价，pandas.Series，index是[日期，股票代码]
        """
        return self.data.high

    HIGH = high
    High = high

    # @property
    # # @lru_cache()
    def low(self):
        """
        :return: 最低价，pandas.Series，index是[日期，股票代码]
        """
        return self.data.low

    LOW = low
    Low = low

    # @property
    # # @lru_cache()
    def close(self):
        """
        :return: 收盘价，pandas.Series，index是[日期，股票代码]
        """
        return self.data.close

    CLOSE = close
    Close = close

    # @property
    # # @lru_cache()
    def volume(self):
        """
        :return: 成交量，pandas.Series，index是[日期，股票代码]
        """
        if 'volume' in self.data.columns:
            return self.data.volume
        elif 'vol' in self.data.columns:
            return self.data.vol
        elif 'trade' in self.data.columns:
            return self.data.trade
        else:
            return None

    vol = volume
    VOLUME = vol
    Volume = vol
    VOL = vol
    Vol = vol

    # @property
    # # @lru_cache()
    def amount(self):
        """
        :return: 成交额，pandas.Series，index是[日期，股票代码]
        """
        if 'amount' in self.data.columns:
            return self.data.amount
        else:
            return self.vol * self.price * 100

    amt = amount
    AMT = amount

    # @property
    # # @lru_cache()
    def price(self):
        """
        :return: 均价，pandas.Series，index是[日期，股票代码]
        """
        return (self.open + self.high + self.low + self.close) / 4

    PRICE = price
    Price = price

    # @property
    # # @lru_cache()
    def trade(self):
        """
        :return: 期货中，pandas.Series，index是[日期，股票代码]
        """
        if 'trade' in self.data.columns:
            return self.data.trade
        else:
            return None

    TRADE = trade
    Trade = trade

    # @property
    # # @lru_cache()
    def position(self):
        """
        :return: 持仓，pandas.Series，index是[日期，股票代码]
        """
        if 'position' in self.data.columns:
            return self.data.position
        else:
            return None

    POSITION = position
    Position = position

    # @property
    # # @lru_cache()
    def adj_factor(self):
        """
        :return: 复权因子，pandas.Series，index是[日期，股票代码]
        """
        return self.data.adj_factor

    # 交易日期
#     # @property
    # # @lru_cache()
    # def date(self):
    #     return self.data.date
    # DATE = date
    # Date = date
    # @property
    # # @lru_cache()
    def max(self):
        """
        :return: 最高均价，pandas.Series，index是股票代码
        """
        return self.price.groupby(level=1).apply(lambda x: x.max())

    MAX = max
    Max = max

    # @property
    # # @lru_cache()
    def min(self):
        """
        :return: 最低均价，pandas.Series，index是股票代码
        """
        return self.price.groupby(level=1).apply(lambda x: x.min())

    MIN = min
    Min = min

    # @property
    # # @lru_cache()
    def mean(self):
        """
        :return: 平均均价，pandas.Series，index是股票代码
        """
        return self.price.groupby(level=1).apply(lambda x: x.mean())

    MEAN = mean
    Mean = mean

    # 一阶差分序列
    # @property
    # # @lru_cache()
    def diff(self):
        """
        :return: 均价变化量，pandas.Series，index是[日期，股票代码]
        """
        return self.price.groupby(level=1).apply(lambda x: x.diff(1))

    DIFF = diff

    # @property
    # # @lru_cache()
    def pvariance(self):
        """
        :return: 均价总体方差，pandas.Series，index是股票代码
        """
        return self.price.groupby(level=1).apply(lambda x: statistics.pvariance(x))

    PVARIANCE = pvariance
    Pvariance = pvariance

    # @property
    # # @lru_cache()
    def variance(self):
        """
        :return: 均价样本方差，pandas.Series，index是股票代码
        """
        return self.price.groupby(level=1).apply(lambda x: statistics.variance(x))

    VARIANCE = variance
    Variance = variance

    # @property
    # # @lru_cache()
    def bar_pct_change(self):
        """
        :return: 当日涨跌幅，pandas.Series，index是[日期，股票代码]
        """
        return (self.close - self.open) / self.open

    BAR_PCT_CHANGE = bar_pct_change
    Bar_pct_change = bar_pct_change

    # @property
    # # @lru_cache()
    def bar_amplitude(self):
        """
        :return: bar振幅，pandas.Series，index是[日期，股票代码]
        """
        return (self.high - self.low) / self.low

    BAR_AMPLITUDE = bar_amplitude
    Bar_amplitude = bar_amplitude

    # @property
    # # @lru_cache()
    def stdev(self):
        """
        :return: 均价的样本标准差，pandas.Series，index是股票代码
        """
        return self.price.groupby(level=1).apply(lambda x: statistics.stdev(x))

    STDEV = stdev
    Stdev = stdev

    # @property
    # # @lru_cache()
    def pstdev(self):
        """
        :return: 均价的总体标准差，pandas.Series，index是股票代码
        """
        return self.price.groupby(level=1).apply(lambda x: statistics.pstdev(x))

    PSTDEV = pstdev
    Pstdev = pstdev

    # @property
    # # @lru_cache()
    def mean_harmonic(self):
        """
        :return: 均价的调和平均数，pandas.Series，index是股票代码
        """
        return self.price.groupby(level=1).apply(lambda x: statistics.harmonic_mean(x))

    MEAN_HARMONIC = mean_harmonic
    Mean_harmonic = mean_harmonic

    # @property
    # # @lru_cache()
    def mode(self):
        """
        :return: 均价的众数，pandas.Series，index是股票代码
        """
        try:
            return self.price.groupby(level=1).apply(lambda x: statistics.mode(x))
        except:
            return None

    MODE = mode
    Mode = mode

    # 振幅
    # @property
    # # @lru_cache()
    def amplitude(self):
        """
        :return: 均价在时间区间内的振幅，pandas.Series，index是股票代码
        """
        return self.price.groupby(level=1).apply(lambda x: (x.max() - x.min()) / x.min())

    AMPLITUDE = amplitude
    Amplitude = amplitude

    # 偏度 Skewness
    # @property
    # # @lru_cache()
    def skew(self):
        """
        :return: 均价的偏度，pandas.Series，index是股票代码
        """
        return self.price.groupby(level=1).apply(lambda x: x.skew())

    SKEW = skew
    Skew = skew

    # 峰度Kurtosis
    # @property
    # # @lru_cache()
    def kurt(self):
        """
        :return: 均价的峰度，pandas.Series，index是股票代码
        """
        '返回DataStruct.price的峰度'
        return self.price.groupby(level=1).apply(lambda x: x.kurt())

    Kurt = kurt
    KURT = kurt

    # 百分数变化
    # @property
    # # @lru_cache()
    def pct_change(self):
        """
        :return: 均价的百分比变化，pandas.Series，index是[日期，股票代码]
        """
        return self.price.groupby(level=1).apply(lambda x: x.pct_change())

    PCT_CHANGE = pct_change
    Pct_change = pct_change

    # 平均绝对偏差
    # @property
    # # @lru_cache()
    def mad(self):
        """
        :return: 均价的平均绝对偏差，pandas.Series，index是股票代码
        """
        return self.price.groupby(level=1).apply(lambda x: x.mad())

    MAD = mad
    Mad = mad

    # @property
    # # @lru_cache()
    def panel_gen(self):
        """
        :return: 时间迭代器，每一次迭代为该日期对应的全部数据，pandas.DataFrame，index是[日期，股票代码]，columns是字段名
        """
        for item in self.index.levels[0]:
            yield self.new(self.data.xs(item, level=0, drop_level=False))

    PANEL_GEN = panel_gen
    Panel_gen = panel_gen

    # @property
    # # @lru_cache()
    def security_gen(self):
        """
        :return: 代码迭代器，每一次迭代为该股票对应的全部数据，pandas.DataFrame，index是[日期，股票代码]，columns是字段名
        """
        for item in self.index.levels[1]:
            yield self.new(self.data.xs(item, level=1, drop_level=False))

    SECURITY_GEN = security_gen
    Security_gen = security_gen

    # @property
    # # @lru_cache()
    def index(self):
        """
        :return: 查询结果的索引，pandas.MultiIndex，[日期，股票代码]
        """
        return self.data.index.remove_unused_levels()

    INDEX = index
    Index = index

#     # @property
    # # @lru_cache()
    # def code(self):
    #     '返回结构体中的代码'
    #     return self.index.levels[1]
    # CODE = code
    # Code = code

    # @property
    # # @lru_cache()
    def dicts(self):
        """
        :return: dict形式数据，dict，key是索引，tuple，value是{字段名称：取值}
        """
        return self.to_dict('index')

    DICTS = dicts
    Dicts = dicts

    # @property
    # # @lru_cache()
    def len(self):
        """
        :return: 查询结果的长度，int
        """
        return len(self.data)

    LEN = len
    Len = len

    def qfq(self, series, df=None):
        """
        定点复权
        :param series: 价格序列，pandas.Series，index是[日期，股票代码]
        :param df: 传入含有复权因子数据的序列，pandas.DataFrame，index是[日期，股票代码]，columns是字段名
        :return: 以第一个交易日为复权基期计算的定点复权价格，pandas.Series，index是[日期，股票代码]
        """
        if df is None:
            dd = self.adj_factor * series
            for date in self.data.index.levels[0]:
                dd.loc[date, :] = dd.loc[date, :] / self.adj_factor.loc[self.data.index.levels[0][0], :].tolist()
        else:
            dd = df.adj_factor * series
            for date in df.index.levels[0]:
                dd.loc[date, :] = dd.loc[date, :] / df.adj_factor.loc[df.index.levels[0][0], :].tolist()
        return dd

    def hfq(self, series, df=None):
        """
        前复权
        :param series: 价格序列，pandas.Series，index是[日期，股票代码]
        :param df: 传入含有复权因子数据的序列，pandas.DataFrame，index是[日期，股票代码]，columns是字段名
        :return: 前复权价格，pandas.Series，index是[日期，股票代码]
        """
        if df is None:
            dd = self.adj_factor * series
            for date in self.data.index.levels[0]:
                dd.loc[date, :] = dd.loc[date, :] / self.adj_factor.loc[self.data.index.levels[0][-1], :].tolist()
        else:
            dd = df.adj_factor * series
            for date in df.index.levels[0]:
                dd.loc[date, :] = dd.loc[date, :] / df.adj_factor.loc[df.index.levels[0][-1], :].tolist()
        return dd

    def get_dict(self, time, code):
        """
        获取指定日期和股票代码的行情数据
        :param time: 日期，str，"%Y-%m-%d"
        :param code: 股票代码，str
        :return: 行情数据，dict，key是字段名称，value是取值
        """
        try:
            return self.dicts[(time, str(code))]
        except Exception as e:
            raise e

    # def plot(self, code=None):
    #     """plot the market_data"""
    #     if code is None:
    #         path_name = '.' + os.sep  + '_codepackage_' + '.html'
    #         kline = Kline('CodePackage_',width=1360, height=700, page_title='QUANTAXIS')
    #
    #         data_splits = self.splits()
    #
    #         for i_ in range(len(data_splits)):
    #             data = []
    #             axis = []
    #             for dates, row in data_splits[i_].data.iterrows():
    #                 open, high, low, close = row[1:5]
    #                 datas = [open, close, low, high]
    #                 axis.append(dates[0])
    #                 data.append(datas)
    #
    #             kline.add(self.code[i_], axis, data, mark_point=["max", "min"], is_datazoom_show=True, datazoom_orient='horizontal')
    #         kline.render(path_name)
    #         webbrowser.open(path_name)
    #         self.util_log_info('The Pic has been saved to your path: %s' % path_name)
    #     else:
    #         data = []
    #         axis = []
    #         for dates, row in self.select_code(code).data.iterrows():
    #             open, high, low, close = row[1:5]
    #             datas = [open, close, low, high]
    #             axis.append(dates[0])
    #             data.append(datas)
    #
    #         path_name = '.{}_{}.html'.format(os.sep,  code)
    #         kline = Kline(str(code),width=1360, height=700, page_title='QUANTAXIS')
    #         kline.add(code, axis, data, mark_point=["max", "min"], is_datazoom_show=True, datazoom_orient='horizontal')
    #         kline.render(path_name)
    #         webbrowser.open(path_name)
    #         self.util_log_info('The Pic has been saved to your path: {}'.format(path_name))

    def get(self, name):
        """
        获取性质
        :param name: 性质名称，str
        :return: 在该实例中该性质的值
        """
        if name in self.data.__dir__():
            return eval('self.{}'.format(name))
        else:
            raise ValueError('QADATASTRUCT CANNOT GET THIS PROPERTY')

    def query(self, context):
        """
        查询符合条件的data数据
        :param context: 查询条件，格式同pandas.DataFrame.query，str
        :return: 数据查询结果，pandas.DataFrame，index是[日期，股票代码]，columns是字段名
        """
        try:
            return self.data.query(context)
        except pd.core.computation.ops.UndefinedVariableError:
            print('CANNOT QUERY THIS {}'.format(context))
            pass

    def groupby(self, by=None, axis=0, level=None, as_index=False, sort=False, group_keys=False, squeeze=False,
                observed=False, **kwargs):
        """
        仿dataframe的groupby写法,但控制了by的code和datetime，参数传入同pandas.DataFrame.groupby
        :return: DataFrameGroupBy对象
        """
        if by == self.index.names[1]:
            by = None
            level = 1
        elif by == self.index.names[0]:
            by = None
            level = 0
        return self.data.groupby(by=by, axis=axis, level=level, as_index=as_index, sort=sort, group_keys=group_keys,
                                 squeeze=squeeze, observed=observed)

    def new(self, data=None, dtype=None, if_fq=None):
        """
        未完成方法，目的是保留原查询结果不被后续操作改动
         创建一个新的DataStruct
        data 默认是self.data
        🛠todo 没有这个？？ inplace 是否是对于原类的修改 ？？
        """
        data = self.data if data is None else data
        # data.index= data.index.remove_unused_levels()

        # 🛠todo 不是很理解这样做的意图， 已经copy了，还用data初始化
        # 🛠todo deepcopy 实现 ？还是 ？
        temp = copy(self)
        # temp.__init__(data)
        return data

    def reverse(self):
        """
        :return: 倒序排列的查询结果，pandas.DataFrame，index是[日期，股票代码]，columns是字段名称
        """
        return self.data[::-1]

    def tail(self, lens):
        """
        :param lens: 长度，int
        :return: 查询结果的后lens条数据，pandas.DataFrame，index是[日期，股票代码]，columns是字段名称
        """
        return self.data.tail(lens)

    def head(self, lens):
        """
        :param lens: 长度，int
        :return: 查询结果的前lens条数据，pandas.DataFrame，index是[日期，股票代码]，columns是字段名称
        """
        return self.data.head(lens)

    # def show(self):
    #     """
    #     打印数据包的内容
    #     """
    #     return util_log_info(self.data)

    def to_list(self):
        """
        :return: 查询结果的list形式，list，每一个元素为一条数据
        """
        return self.data.values.tolist()

    def to_pd(self):
        """
        :return: 查询结果的pandas.DataFrame形式
        """
        return self.data

    def to_numpy(self):
        """
        :return: 查询结果的numpy.ndarray形式
        """
        return self.data.values

    # def to_json(self):
    #     """
    #     转换DataStruct为json
    #     """
    #     return util_to_json_from_pandas(self.data)

    def to_dict(self, orient='dict'):
        """
        :param orient: 指定dict的value类型，str，dict/list/series/split/records/index
        :return: 查询结果的dict形式，key是字段名称，value是dict，{index：取值}
        """
        return self.data.to_dict(orient)

    def to_hdf(self, place, name):
        """
        :param place: hdf5文件存储路径，str
        :param name: 存储组名，str
        :return:
        """
        self.data.to_hdf(place, name)
        return place, name

    def splits(self):
        """
        :return: 按照股票代码分解查询结果，list，元素是单个股票的查询结果，pandas.DataFrame，index是[date，code]，columns是字段名
        """
        return list(map(self.select_code, self.stock_list))

    def select_code(self, code):
        """
        根据股票代码选择数据
        :param code: 股票代码，str
        :return: 该股票的全部数据，pandas.DataFrame，index是[date，code]，columns是字段名
        @2018/06/03 pandas 的索引问题导致
        https://github.com/pandas-dev/pandas/issues/21299

        因此先用set_index去重做一次index
        影响的有selects,select_time,select_month,get_bar

        @2018/06/04
        当选择的时间越界/股票不存在,raise ValueError

        @2018/06/04 pandas索引问题已经解决
        全部恢复
        """
        try:
            return self.data.loc[(slice(None), code), :]
        except KeyError:
            raise ValueError('QA CANNOT FIND THIS CODE {}'.format(code))

    def add_func(self, func, *arg, **kwargs):
        """
        增加计算指标
        :param func: 针对单支股票的指标计算函数
        :param arg: 不定长位置参数
        :param kwargs: 不定长键值参数
        :return: 按照指定函数计算出的指标
        """
        return self.groupby(level=1, sort=False).apply(func, *arg, **kwargs)

    def pivot(self, column_):
        """
        :param column_: 字段名，str/list
        :return: 单个字段名：字段值，pandas.DataFrame，index是日期，columns是股票代码
                  多个字段名：字段值，pandas.DataFrame，index是日期，columns是[字段名，股票代码]
        """
        try:
            return self.data.reset_index().pivot(index='datetime', columns='code', values=column_)
        except KeyError:
            return self.data.reset_index().pivot(index='date', columns='code', values=column_)

    def selects(self, start, end=None, code=None):
        """

        :param start: 开始时间，str，"%Y-%m-%d"
        :param end: 开始时间，str，"%Y-%m-%d"，None为取到结尾
        :param code: 股票代码，str，None为取全部股票
        :return: 根据start、end和code选出的查询结果，index是[日期, 股票代码]，columns是字段名
        @2018/06/03 pandas 的索引问题导致
        https://github.com/pandas-dev/pandas/issues/21299

        因此先用set_index去重做一次index
        影响的有selects,select_time,select_month,get_bar

        @2018/06/04
        当选择的时间越界/股票不存在,raise ValueError

        @2018/06/04 pandas索引问题已经解决
        全部恢复
        """
        return self.data.loc[(slice(start, end), slice(code)), :]

    def get_bar(self, code, time):
        """
        :param code: 股票代码，str
        :param time: 日期，str，"%Y-%m-%d"
        :return: 一个bar的数据，pandas.Series，index是字段名
        """
        try:
            return self.data.loc[(time, code)]
        except KeyError:
            raise ValueError(
                'DATASTRUCT CURRENTLY CANNOT FIND THIS BAR WITH {} {}'.format(code, time))

    def find_bar(self, code, time):
        """
        :param code: 股票代码，str
        :param time: 日期，str
        :return: 一个bar的数据，dict，key是字段名，value是字段值
        """
        return self.dicts[(time, code)]

    def fast_moving(self, pct):
        """
        查询快速上涨的数据
        :param pct: 涨幅下限，float，[-0.1，0.1]
        :return: 涨幅超过下限的数据，pandas.Series，index是[日期，股票代码]
        """
        return self.bar_pct_change[self.bar_pct_change > pct].sort_index()


class financial_data(object):
    def __init__(self, code=None, date=None, start=None, end=None, stock_list=None, n=None, coll=db.wind_financial_2014):
        """
        基本面数据接口
        :param code: 股票代码，str
        :param date: 日期，str，"%Y-%m-%d"
        :param start: 开始日期，str，"%Y-%m-%d"
        :param end: 结束日期，str，"%Y-%m-%d"
        :param stock_list: 股票池，list
        :param n: 交易日数量，int，+为向未来增加，-为向过去增加
        :param coll: 数据库变量，被连接的document为wind_financial_2014
        """
        self.code = code
        self.stock_list = stock_list
        if date is not None:
            date = util_get_real_date(date, towards=-1)
        # 原算法
        # if date not in trade_df.date.tolist():
        #     trade_df_date = trade_df.date.tolist()
        #     trade_df_date = trade_df_date[::-1]
        #     for i in range(len(trade_df_date)):
        #         if trade_df_date[i] < date:
        #             date = trade_df_date[i]
        #             break
        self.date = date
        self.coll_fin = coll
        self.start = start
        self.end = end
        self.n = n
        if self.code is not None and self.date is not None and self.n is None:
            self.coll = self.coll_fin.find({'date': self.date, 'code': self.code})
        elif self.code is not None and self.date is not None and self.n is not None:
            if self.n > 0:
                try:
                    self.coll = self.coll_fin.find(
                        {'date': {'$gte': self.date, '$lt': trade_date_sse[trade_date_sse.index(self.date) + self.n]},
                         'code': self.code})
                except Exception:
                    self.coll = self.coll_fin.find({'date': {'$gte': self.date}, 'code': self.code})
            else:
                try:
                    self.coll = self.coll_fin.find(
                        {'date': {'$gt': trade_date_sse[trade_date_sse.index(self.date) + self.n], '$lte': self.date},
                         'code': self.code})
                except Exception:
                    self.coll = self.coll_fin.find({'date': {'$lte': self.date}, 'code': self.code})
        elif self.start is not None and self.code is not None:
            if self.end is None:
                self.coll = self.coll_fin.find({'code': self.code, 'date': {'$gte': self.start}})
            else:
                self.coll = self.coll_fin.find({'code': self.code, 'date': {'$gte': self.start, '$lte': self.end}})
        elif self.stock_list is not None and self.date is not None and self.n is None:
            self.coll = self.coll_fin.find({'code': {'$in': self.stock_list}, 'date': self.date})
        elif self.stock_list is not None and self.date is not None and self.n is not None:
            if self.n > 0:
                try:
                    self.coll = self.coll_fin.find({'date': {'$gte': self.date, '$lt': trade_date_sse[
                        trade_date_sse.index(self.date) + self.n]}, 'code': {'$in': self.stock_list}})
                except Exception:
                    self.coll = self.coll_fin.find({'date': {'$gte': self.date}, 'code': {'$in': self.stock_list}})
            else:
                try:
                    self.coll = self.coll_fin.find({'date': {
                        '$gt': trade_date_sse[trade_date_sse.index(self.date) + self.n], '$lte': self.date},
                        'code': {'$in': self.stock_list}})
                except Exception:
                    self.coll = self.coll_fin.find({'date': {'$lte': self.date}, 'code': {'$in': self.stock_list}})
        elif self.stock_list is not None and self.start is not None:
            if self.end is None:
                self.coll = self.coll_fin.find({'code': {'$in': self.stock_list}, 'date': {'$gte': self.start}})
            else:
                self.coll = self.coll_fin.find(
                    {'code': {'$in': self.stock_list}, 'date': {'$gte': self.start, '$lte': self.end}})
        elif self.start is None and self.stock_list is None and self.code is None and self.date is not None:
            self.coll = self.coll_fin.find({'date': self.date})
        elif self.date is None and self.stock_list is None and self.code is None and self.start is not None:
            if self.end is None:
                self.coll = self.coll_fin.find({'date': {'$gte': self.start}})
            else:
                self.coll = self.coll_fin.find({'date': {'$gte': self.start, '$lte': self.end}})
        self.data = pd.DataFrame(item for item in self.coll).set_index(['date', 'code']).sort_index()
        self.data.drop(['_id'], axis=1, inplace=True)

    def __call__(self):
        """
        ✅如果需要暴露 DataFrame 内部数据对象，就用__call__来转换出 data （DataFrame）
        Emulating callable objects
        object.__call__(self[, args…])
        Called when the instance is “called” as a function;
        if this method is defined, x(arg1, arg2, ...) is a shorthand for x.__call__(arg1, arg2, ...).
        比如
        obj =  _quotation_base() 调用 __init__
        df = obj()  调用 __call__
        等同 df = obj.__call__()
        :return: 数据查询结果，pandas.DataFrame，index是日期和股票代码，columns是字段名称
        """
        return self.data

    def __len__(self):
        """
        返回记录的数目
        :return: 查询到的数据长度，int
        """
        return len(self.index)

    def __iter__(self):
        """
        📌关于 yield 的问题
        A yield statement is semantically equivalent to a yield expression.
        yield 的作用就是把一个函数变成一个 generator，
        带有 yield 的函数不再是一个普通函数，Python 解释器会将其视为一个 generator
        for iterObj in ThisObj
        📌关于__iter__ 的问题
        可以不被 __next__ 使用
        Return an iterator object
        iter the row one by one
        :return: 迭代生成器，每一次迭代为查询数据的一行
        """
        for i in range(len(self.index)):
            yield self.data.iloc[i]

    def __add__(self, DataStruct):
        """
        ➕合并数据，重复的数据drop
        :param DataStruct: _quotation_base 继承的子类  QA_DataStruct_XXXX
        :return: _quotation_base 继承的子类  QA_DataStruct_XXXX
        """
        assert isinstance(DataStruct, bar_data)
        return self.new(self.data.append(DataStruct.data).drop_duplicates())

    __radd__ = __add__

    def __sub__(self, DataStruct):
        """
        ⛔️不是提取公共数据， 去掉 DataStruct 中指定的数据
        :param DataStruct:  _quotation_base 继承的子类  QA_DataStruct_XXXX
        :return: _quotation_base 继承的子类  QA_DataStruct_XXXX
        """
        assert isinstance(DataStruct, bar_data)
        return self.new(data=self.data.drop(DataStruct.index).set_index(self.index.names, drop=False))

    __rsub__ = __sub__

    def __getitem__(self, key):
        """
        # 🛠todo 进一步研究 DataFrame __getitem__ 的意义。
        DataFrame调用__getitem__调用(key)
        :param key: 行标签或者列标签
        :return: 标签对应的行或列
        """
        data_to_init = self.data.__getitem__(key)
        if isinstance(data_to_init, pd.DataFrame) is True:

            return self.new(data=data_to_init)
        elif isinstance(data_to_init, pd.Series) is True:

            return data_to_init

    def __getattr__(self, attr):
        """
        # 🛠todo 为何不支持 __getattr__ ？？
        :param attr: 属性名称，str
        :return: self.data的该属性对应的值
        """
        try:
            self.new(data=self.data.__getattr__(attr))
        except:
            raise AttributeError('DataStruct_* Class Currently has no attribute {}'.format(attr))

    '''
    ########################################################################################################
    获取序列
    '''

    # def ix(self, key):
    #     return self.new(data=self.data.ix(key), dtype=self.type, if_fq=self.if_fq)
    #
    # def iloc(self, key):
    #     return self.new(data=self.data.iloc(key), dtype=self.type, if_fq=self.if_fq)
    #
    # def loc(self, key):
    #     return self.new(data=self.data.loc(key), dtype=self.type, if_fq=self.if_fq)

    # @property
    # # @lru_cache()
    def bp(self):
        """
        :return: 市净率倒数，pandas.Series，index是[日期，股票代码]
        """
        return self.data.bp

    BP = bp
    Bp = bp

    # @property
    # # @lru_cache()
    def deductedprofit_ttm(self):
        """
        :return: 扣非净利润TTM，pandas.Series，index是[日期，股票代码]
        """
        return self.data.deductedprofit_ttm

    DEDUCTEDPROFIT_TTM = deductedprofit_ttm
    Deductedprofit_ttm = deductedprofit_ttm
    DEDUCTEDPROFIT_ttm = deductedprofit_ttm

    # @property
    # # @lru_cache()
    def ep(self):
        """
        :return: 市盈率倒数，pandas.Series，index是[日期，股票代码]
        """
        return self.data.ep

    EP = ep
    Ep = ep

    # @property
    # # @lru_cache()
    def epcut(self):
        """
        :return: pandas.Series，index是[日期，股票代码]
        """
        return self.data.epcut

    EPcut = epcut

    # @property
    # # @lru_cache()
    def grossmargin_ttm(self):
        """
        :return: 毛利润TTM，pandas.Series，index是[日期，股票代码]
        """
        return self.data.grossmargin_ttm

    GROSSMARGIN_TTM = grossmargin_ttm
    Grossmargin_ttm = grossmargin_ttm
    GROSSMARGIN_ttm = grossmargin_ttm

    # @property
    # # @lru_cache()
    def mkt_cap_ard(self):
        """
        :return: 总市值，pandas.Series，index是[日期，股票代码]
        """
        return self.data.mkt_cap_ard

    MKT_CAP_ARD = mkt_cap_ard
    Mkt_cap_ard = mkt_cap_ard

    # @property
    # # @lru_cache()
    def ncfp(self):
        """
        :return: pandas.Series，index是[日期，股票代码]
        """
        return self.data.ncfp

    NCFP = ncfp
    Ncfp = ncfp

    # @property
    # # @lru_cache()
    def ocfp(self):
        """
        :return: pandas.Series，index是[日期，股票代码]
        """
        return self.data.ocfp

    OCFP = ocfp
    Ocfp = ocfp

    # # 交易日期
#     # @property
    # # @lru_cache()
    # def date(self):
    #     return self.data.date
    # DATE = date
    # Date = date

    # @property
    # # @lru_cache()
    def free_float_shares(self):
        """
        :return: 自由流通股本，pandas.Series，index是[日期，股票代码]
        """
        return self.data.free_float_shares

    FREE_FLOAT_SHARES = free_float_shares

    # @property
    # # @lru_cache()
    def total_share(self):
        """
        :return: 总股本，pandas.Series，index是[日期，股票代码]
        """
        return self.data.total_share

    TOTAL_SHARE = total_share

    # @property
    # # @lru_cache()
    def or_ttm(self):
        """
        :return: 营业收入TTM，pandas.Series，index是[日期，股票代码]
        """
        return self.data.or_ttm

    OR_TTM = or_ttm
    Or_ttm = or_ttm
    OR_ttm = or_ttm

    # @property
    # # @lru_cache()
    def pb_lf(self):
        """
        :return: pandas.Series，index是[日期，股票代码]
        """
        return self.data.pb_lf

    PB_LF = pb_lf
    Pb_lf = pb_lf

    # @property
    # # @lru_cache()
    def pcf_ncf_ttm(self):
        """
        :return: 市现率PCF(现金净流量)，pandas.Series，index是[日期，股票代码]
        """
        return self.data.pcf_ncf_ttm

    PCF_NCF_TTM = pcf_ncf_ttm
    Pcf_ncf_ttm = pcf_ncf_ttm
    PCF_NCF_ttm = pcf_ncf_ttm

    # @property
    # # @lru_cache()
    def pcf_ocf_ttm(self):
        """
        :return: 市现率PCF(经营现金流)，pandas.Series，index是[日期，股票代码]
        """
        return self.data.pcf_ocf_ttm

    PCF_OCF_TTM = pcf_ocf_ttm
    Pcf_ocf_ttm = pcf_ocf_ttm
    PCF_OCF_ttm = pcf_ocf_ttm

    # @property
    # # @lru_cache()
    def pe_ttm(self):
        """
        :return: 市盈率TTM，pandas.Series，index是[日期，股票代码]
        """
        return self.data.pe_ttm

    PE_TTM = pe_ttm
    PE_ttm = pe_ttm
    Pe_ttm = pe_ttm

    # @property
    # # @lru_cache()
    def profit_ttm(self):
        """
        :return: 净利润TTM，pandas.Series，index是[日期，股票代码]
        """
        return self.data.profit_ttm

    PROFIT_TTM = profit_ttm
    PROFIT_ttm = profit_ttm
    Profit_ttm = profit_ttm

    # @property
    # # @lru_cache()
    def roa_ttm2(self):
        """
        :return: 资产收益率TTM，pandas.Series，index是[日期，股票代码]
        """
        return self.data.roa_ttm2

    ROA_TTM = roa_ttm2
    ROA_ttm = roa_ttm2
    Roa_ttm = roa_ttm2

    # @property
    # # @lru_cache()
    def roe_ttm2(self):
        """
        :return: 净资产收益率TTM，pandas.Series，index是[日期，股票代码]
        """
        return self.data.roe_ttm2

    ROE_TTM = roe_ttm2
    ROE_ttm = roe_ttm2
    Roe_ttm = roe_ttm2

    # @property
    # # @lru_cache()
    def share_ntrd_prfshare(self):
        """
        :return: 优先股，pandas.Series，index是[日期，股票代码]
        """
        return self.data.share_ntrd_prfshare

    SHARE_NTRD_PRFSHARE = share_ntrd_prfshare
    Share_ntrd_prfshare = share_ntrd_prfshare

    # @property
    # # @lru_cache()
    def sp(self):
        """
        :return: 市销率倒数，pandas.Series，index是[日期，股票代码]
        """
        return self.data.sp

    SP = sp
    Sp = sp

    # @property
    # # @lru_cache()
    def wrating_avg_data(self):
        """
        :return: 综合评级（数值），pandas.Series，index是[日期，股票代码]
        """
        return self.data.wrating_avg_data

    WRATING_AVG_DATA = wrating_avg_data
    Wrating_avg_data = wrating_avg_data

    # @property
    # # @lru_cache()
    def wrating_downgrade(self):
        """
        :return: 评级低调家数，pandas.Series，index是[日期，股票代码]
        """
        return self.data.wrating_downgrade

    WRATING_DOWNGRADE = wrating_downgrade
    Wrating_downgrade = wrating_downgrade

    # @property
    # # @lru_cache()
    def wrating_targetprice(self):
        """
        :return: 一致预测目标价，pandas.Series，index是[日期，股票代码]
        """
        return self.data.wrating_targetprice

    WRATING_TARGETPRICE = wrating_targetprice
    Wrating_targetprice = wrating_targetprice

    # @property
    # # @lru_cache()
    def wrating_upgrade(self):
        """
        :return: 评级低调家数，pandas.Series，index是[日期，股票代码]
        """
        return self.data.wrating_upgrade

    WRATING_UPGRADE = wrating_upgrade
    Wrating_upgrade = wrating_upgrade

    # @property
    # # @lru_cache()
    def high_to_low_12m(self):
        """
        :return: 过去12个月涨跌幅，pandas.Series，index是[日期，股票代码]
        """
        return self.data['12m_high/low']

    HIGH_TO_LOW_12M = high_to_low_12m
    HIGH_TO_LOW_12m = high_to_low_12m
    High_to_low_12m = high_to_low_12m
    high_to_low_12M = high_to_low_12m

    # @property
    # # @lru_cache()
    def high_to_low_1m(self):
        """
        :return: 过去1个月涨跌幅，pandas.Series，index是[日期，股票代码]
        """
        return self.data['1m_high/low']

    HIGH_TO_LOW_1M = high_to_low_1m
    HIGH_TO_LOW_1m = high_to_low_1m
    High_to_low_1m = high_to_low_1m
    high_to_low_1M = high_to_low_1m

    # @property
    # # @lru_cache()
    def high_to_low_2m(self):
        """
        :return: 过去2个月涨跌幅，pandas.Series，index是[日期，股票代码]
        """
        return self.data['2m_high/low']

    HIGH_TO_LOW_2M = high_to_low_2m
    HIGH_TO_LOW_2m = high_to_low_2m
    High_to_low_2m = high_to_low_2m
    high_to_low_2M = high_to_low_2m

    # @property
    # # @lru_cache()
    def high_to_low_3m(self):
        """
        :return: 过去3个月涨跌幅，pandas.Series，index是[日期，股票代码]
        """
        return self.data['3m_high/low']

    HIGH_TO_LOW_3M = high_to_low_3m
    HIGH_TO_LOW_3m = high_to_low_3m
    High_to_low_3m = high_to_low_3m
    high_to_low_3M = high_to_low_3m

    # @property
    # # @lru_cache()
    def high_to_low_6m(self):
        """
        :return: 过去6个月涨跌幅，pandas.Series，index是[日期，股票代码]
        """
        return self.data['6m_high/low']

    HIGH_TO_LOW_6M = high_to_low_6m
    HIGH_TO_LOW_6m = high_to_low_6m
    High_to_low_6m = high_to_low_6m
    high_to_low_6M = high_to_low_6m

    # @property
    # # @lru_cache()
    def return_d(self):
        """
        :return: 日动量，pandas.Series，index是[日期，股票代码]
        """
        return self.data.d_return

    RETURN_D = return_d
    RETURN_d = return_d
    Return_d = return_d
    return_D = return_d

    # @property
    # # @lru_cache()
    def return_12m(self):
        """
        :return: 年动量，pandas.Series，index是[日期，股票代码]
        """
        return self.data['12m_return']

    RETURN_12M = return_12m
    RETURN_12m = return_12m
    Return_12m = return_12m
    return_12M = return_12m

    # @property
    # # @lru_cache()
    def return_1m(self):
        """
        :return: 月动量，pandas.Series，index是[日期，股票代码]
        """
        return self.data['1m_return']

    RETURN_1M = return_1m
    RETURN_1m = return_1m
    Return_1m = return_1m
    return_1M = return_1m

    # @property
    # # @lru_cache()
    def return_2m(self):
        """
        :return: 2个月动量，pandas.Series，index是[日期，股票代码]
        """
        return self.data['2m_return']

    RETURN_2M = return_2m
    RETURN_2m = return_2m
    Return_2m = return_2m
    return_2M = return_2m

    # @property
    # # @lru_cache()
    def return_3m(self):
        """
        :return: 3个月动量，pandas.Series，index是[日期，股票代码]
        """
        return self.data['3m_return']

    RETURN_3M = return_3m
    RETURN_3m = return_3m
    Return_3m = return_3m
    return_3M = return_3m

    # @property
    # # @lru_cache()
    def return_6m(self):
        """
        :return: 6个月动量，pandas.Series，index是[日期，股票代码]
        """
        return self.data['6m_return']

    RETURN_6M = return_6m
    RETURN_6m = return_6m
    Return_6m = return_6m
    return_6M = return_6m

    # @property
    # # @lru_cache()
    def std_12m(self):
        """
        :return: 年波动，pandas.Series，index是[日期，股票代码]
        """
        return self.data['12m_std']

    STD_12M = std_12m
    std_12M = std_12m
    STD_12m = std_12m
    Std_12m = std_12m

    # @property
    # # @lru_cache()
    def std_1m(self):
        """
        :return: 月波动，pandas.Series，index是[日期，股票代码]
        """
        return self.data['1m_std']

    STD_1M = std_1m
    std_1M = std_1m
    STD_1m = std_1m
    Std_1m = std_1m

    # @property
    # # @lru_cache()
    def std_2m(self):
        """
        :return: 2个月波动，pandas.Series，index是[日期，股票代码]
        """
        return self.data['2m_std']

    STD_2M = std_2m
    std_2M = std_2m
    STD_2m = std_2m
    Std_2m = std_2m

    # @property
    # # @lru_cache()
    def std_3m(self):
        """
        :return: 3个月波动，pandas.Series，index是[日期，股票代码]
        """
        return self.data['3m_std']

    STD_3M = std_3m
    std_3M = std_3m
    STD_3m = std_3m
    Std_3m = std_3m

    # @property
    # # @lru_cache()
    def std_6m(self):
        """
        :return: 6个月波动，pandas.Series，index是[日期，股票代码]
        """
        return self.data['6m_std']

    STD_6M = std_6m
    std_6M = std_6m
    STD_6m = std_6m
    Std_6m = std_6m

    # @property
    # # @lru_cache()
    def assetsturn1(self):
        """
        :return: 总资产周转率，pandas.Series，index是[日期，股票代码]
        """
        return self.data.assetsturn1

    ASSETSTURN1 = assetsturn1
    Assetsturn1 = assetsturn1
    ASSETSTURN = assetsturn1
    assetsturn = assetsturn1
    Assetsturn = assetsturn1

    # @property
    # # @lru_cache()
    def cashtocurrentdebt(self):
        """
        :return: 现金比率，pandas.Series，index是[日期，股票代码]
        """
        return self.data.cashtocurrentdebt

    CASHTOCURRENTDEBT = cashtocurrentdebt
    Cashtocurrentdebt = cashtocurrentdebt

    # @property
    # # @lru_cache()
    def current(self):
        """
        :return: 流动比率，pandas.Series，index是[日期，股票代码]
        """
        return self.data.current

    CURRENT = current
    Current = current

    # @property
    # # @lru_cache()
    def debtequityratio(self):
        """
        :return: 负债权益比，pandas.Series，index是[日期，股票代码]
        """
        return self.data.debtequityratio

    DEBTEQUITYRATIO = debtequityratio
    Debtequityratio = debtequityratio

    # @property
    # # @lru_cache()
    def debt_mrq(self):
        """
        :return: 负债合计，pandas.Series，index是[日期，股票代码]
        """
        return self.data.debt_mrq

    DEBT_mrq = debt_mrq
    DEBT_MRQ = debt_mrq
    Debt_mrq = debt_mrq

    # @property
    # # @lru_cache()
    def deductedprofit_g_yoy(self):
        """
        :return: pandas.Series，index是[日期，股票代码]
        """
        return self.data.deductedprofit_g_yoy

    DEDUCTEDPROFIT_G_YOY = deductedprofit_g_yoy
    Deductedprofit_g_yoy = deductedprofit_g_yoy

    # @property
    # # @lru_cache()
    def div_cashbeforetax2(self):
        """
        :return: 每股股利税前，pandas.Series，index是[日期，股票代码]
        """
        return self.data.div_cashbeforetax2

    DIV_CASHBEFORETAX2 = div_cashbeforetax2
    Div_cashbeforetax2 = div_cashbeforetax2
    div_cashbeforetax = div_cashbeforetax2
    Div_cashbeforetax = div_cashbeforetax2

    # @property
    # # @lru_cache()
    def deductedprofit_ttm_growth(self):
        """
        :return: 扣非净利润增长率ttm，pandas.Series，index是[日期，股票代码]
        """
        return self.data.deductedprofit_ttm_growth

    DEDUCTEDPROFIT_TTM_GROWTH = deductedprofit_ttm_growth
    Deductedprofit_ttm_growth = deductedprofit_ttm_growth

    # @property
    # # @lru_cache()
    def fcff(self):
        """
        :return: 企业自由现金流量，pandas.Series，index是[日期，股票代码]
        """
        return self.data.fcff

    FCFF = fcff
    Fcff = fcff

    # @property
    # # @lru_cache()
    def fcfp(self):
        """
        :return: pandas.Series，index是[日期，股票代码]
        """
        return self.data.fcfp

    FCFP = fcfp
    Fcfp = fcfp

    # @property
    # # @lru_cache()
    def financial_leverage(self):
        """
        :return: 财务杠杆，pandas.Series，index是[日期，股票代码]
        """
        return self.data.financial_leverage

    FINANCIAL_LEVERAGE = financial_leverage
    Financial_leverage = financial_leverage

#     # @property
    # # @lru_cache()
    # def FLOAT_A_SHARES(self):
    #     return self.data.FLOAT_A_SHARES
    #
    # float_a_shares = FLOAT_A_SHARES
    # Float_a_shares = FLOAT_A_SHARES

#     # @property
    # # @lru_cache()
    # def FREE_TURN(self):
    #     return self.data.FREE_TURN
    #
    # free_turn = FREE_TURN
    # Free_turn = FREE_TURN

    # @property
    # # @lru_cache()
    def gross_rate_qfa(self):
        """
        :return: 销售利润增长率（单季度），pandas.Series，index是[日期，股票代码]
        """
        return self.data.gross_profit_rate_qfa

    GROSS_RATE_QFA = gross_rate_qfa
    Gross_rate_qfa = gross_rate_qfa

    # @property
    # # @lru_cache()
    def gross_rate_ttm(self):
        """
        :return: 销售利润增长率TTM，pandas.Series，index是[日期，股票代码]
        """
        return self.data.gross_profit_rate_ttm

    GROSS_RATE_TTM = gross_rate_ttm
    GROSS_RATE_ttm = gross_rate_ttm

    # @property
    # # @lru_cache()
    def growth_or(self):
        """
        :return: 营业收入增长率，pandas.Series，index是[日期，股票代码]
        """
        return self.data.growth_or

    GROWTH_OR = growth_or
    GROWTH_or = growth_or

    # @property
    # # @lru_cache()
    def holder_avgpct(self):
        """
        :return: 户均持股比例，pandas.Series，index是[日期，股票代码]
        """
        return self.data.holder_avgpct

    HOLDER_AVGPCT = holder_avgpct

    # @property
    # # @lru_cache()
    def holder_havgpctchange(self):
        """
        :return: 户均持股比例半年增长率，pandas.Series，index是[日期，股票代码]
        """
        return self.data.holder_havgpctchange

    HOLDER_HAVGPCTCHANGE = holder_havgpctchange

#     # @property
    # # # @lru_cache()
    # def IPO_DATE(self):
    #     return self.data.IPO_DATE
    #
    # ipo_date = IPO_DATE

    # @property
    # # @lru_cache()
    def kf_pr_rate_qfa(self):
        """
        :return: pandas.Series，index是[日期，股票代码]
        """
        return self.data.kf_pr_rate_qfa

    KF_PR_RATE_QFA = kf_pr_rate_qfa

    # @property
    # # @lru_cache()
    def kf_pr_rate_ttm(self):
        """
        :return: pandas.Series，index是[日期，股票代码]
        """
        return self.data.kf_pr_rate_ttm

    KF_PR_RATE_TTM = kf_pr_rate_ttm
    KF_PR_RATE_ttm = kf_pr_rate_ttm

    # @property
    # # @lru_cache()
    def longdebttodebt(self):
        """
        :return: 长期负债占比，pandas.Series，index是[日期，股票代码]
        """
        return self.data.longdebttodebt

    LONGDEBTODEBT = longdebttodebt

    # @property
    # # @lru_cache()
    def marketvalue_leverage(self):
        """
        :return: 市场杠杆，pandas.Series，index是[日期，股票代码]
        """
        return self.data.marketvalue_leverage

    MARKETVALUE_LEVERAGE = marketvalue_leverage

    # @property
    # # @lru_cache()
    def ocftosales(self):
        """
        :return: pandas.Series，index是[日期，股票代码]
        """
        return self.data.ocftosales

    OCFTOSALES = ocftosales

    # @property
    # # @lru_cache()
    def or_growth_ttm(self):
        """
        :return: 营业收入增长率TTM，pandas.Series，index是[日期，股票代码]
        """
        return self.data.or_growth_ttm

    OR_GROWTH_TTM = or_growth_ttm

    # @property
    # # @lru_cache()
    def qfa_deductedprofit(self):
        """
        :return: 扣非净利润（单季度），pandas.Series，index是[日期，股票代码]
        """
        return self.data.qfa_deductedprofit

    QFA_DEDUCTEDPROFIT = qfa_deductedprofit

    # @property
    # # @lru_cache()
    def qfa_grossmargin(self):
        """
        :return: 毛利润（单季度），pandas.Series，index是[日期，股票代码]
        """
        return self.data.qfa_grossmargin

    QFA_GROSSMARGIN = qfa_grossmargin

    # @property
    # # @lru_cache()
    def qfa_net_profit_is(self):
        """
        :return: pandas.Series，index是[日期，股票代码]
        """
        return self.data.qfa_net_profit_is

    QFA_NET_PROFIT_IS = qfa_net_profit_is

    # @property
    # # @lru_cache()
    def qfa_net_profit_is_g(self):
        """
        :return: pandas.Series，index是[日期，股票代码]
        """
        return self.data.qfa_net_profit_is_g

    QFA_NET_PROFIT_IS_G = qfa_net_profit_is_g

    # @property
    # # @lru_cache()
    def qfa_oper_rev(self):
        """
        :return: pandas.Series，index是[日期，股票代码]
        """
        return self.data.qfa_oper_rev

    QFA_OPER_REV = qfa_oper_rev

    # @property
    # @lru_cache()
    def qfa_roa(self):
        """
        :return: pandas.Series，index是[日期，股票代码]
        """
        return self.data.qfa_roa

    QFA_ROA = qfa_roa

    # @property
    # @lru_cache()
    def qfa_roe(self):
        """
        :return: pandas.Series，index是[日期，股票代码]
        """
        return self.data.qfa_roe

    QFA_ROE = qfa_roe

    # @property
    # @lru_cache()
    def qfa_stot_cash_inflows_oper_act(self):
        """
        :return: pandas.Series，index是[日期，股票代码]
        """
        return self.data.qfa_stot_cash_inflows_oper_act

    QFA_STOT_CASH_INFLOWS_OPER_ACT = qfa_stot_cash_inflows_oper_act

    # @property
    # @lru_cache()
    def qfa_stot_cash_inflows_oper_act_g(self):
        """
        :return: pandas.Series，index是[日期，股票代码]
        """
        return self.data.qfa_stot_cash_inflows_oper_act_g

    QFA_STOT_CASH_INFLOWS_OPER_ACT_G = qfa_stot_cash_inflows_oper_act_g

    # @property
    # @lru_cache()
    def qfa_yoysales(self):
        """
        :return: pandas.Series，index是[日期，股票代码]
        """
        return self.data.qfa_yoysales

    QFA_YOYSALES = qfa_yoysales

    # @property
    # @lru_cache()
    def stm_issuingdate(self):
        """
        :return: pandas.Series，index是[日期，股票代码]
        """
        return self.data.stm_issuingdate

    STM_ISSUINGDATE = stm_issuingdate

    # @property
    # @lru_cache()
    def stot_cash_inflows_oper_act(self):
        """
        :return: pandas.Series，index是[日期，股票代码]
        """
        return self.data.stot_cash_inflows_oper_act

    STOT_CASH_INFLOWS_OPER_ACT = stot_cash_inflows_oper_act

#     # @property
    # # @lru_cache()
    # def STOT_CASH_INFLOWS_OPER_ACT_TTM(self):
    #     return self.data.STOT_CASH_INFLOWS_OPER_ACT_TTM
    #
    # stot_cash_inflows_oper_act_ttm = STOT_CASH_INFLOWS_OPER_ACT_TTM

    # @property
    # @lru_cache()
    def turnover_ttm(self):
        """
        :return: pandas.Series，index是[日期，股票代码]
        """
        return self.data.turnover_ttm

    TURNOVER_TTM = turnover_ttm

#     # @property
    # # @lru_cache()
    # def TURN_12M(self):
    #     return self.data.TURN_12M
    #
    # turn_12m = TURN_12M

#     # @property
    # # @lru_cache()
    # def TURN_1M(self):
    #     return self.data.TURN_1M
    #
    # turn_1m = TURN_1M

#     # @property
    # # @lru_cache()
    # def TURN_2M(self):
    #     return self.data.TURN_2M
    #
    # turn_12m = TURN_2M

#     # @property
    # # @lru_cache()
    # def TURN_3M(self):
    #     return self.data.TURN_3M
    #
    # turn_3m = TURN_3M

#     # @property
    # # @lru_cache()
    # def TURN_6M(self):
    #     return self.data.TURN_6M
    #
    # turn_6m = TURN_6M

#     # @property
    # # @lru_cache()
    # def TURN_D(self):
    #     return self.data.TURN_D
    #
    # turn_d = TURN_D

#     # @property
    # # @lru_cache()
    # def VOLUME(self):
    #     return self.data.VOLUME
    #
    # volume = VOLUME
    # vol = VOLUME
    # VOL = VOLUME

    # @property
    # @lru_cache()
    def wgsd_assets(self):
        """
        :return: pandas.Series，index是[日期，股票代码]
        """
        return self.data.wgsd_assets

    WGSD_ASSETS = wgsd_assets

    # @property
    # @lru_cache()
    def wgsd_com_eq_paholder(self):
        """
        :return: pandas.Series，index是[日期，股票代码]
        """
        return self.data.wgsd_com_eq_paholder

    WGSD_COM_EQ_PAHOLDER = wgsd_com_eq_paholder

    # @property
    # @lru_cache()
    def yoyocf(self):
        """
        :return: pandas.Series，index是[日期，股票代码]
        """
        return self.data.yoyocf

    YOYOCF = yoyocf

    # @property
    # @lru_cache()
    def panel_gen(self):
        """
        :return: 时间迭代器，每一次迭代为该日期对应的全部数据，pandas.DataFrame，index是[日期，股票代码]，columns是字段名
        """
        for item in self.index.levels[0]:
            yield self.data.xs(item, level=0, drop_level=False)

    PANEL_GEN = panel_gen

    # @property
    # @lru_cache()
    def security_gen(self):
        """
        :return: 代码迭代器，每一次迭代为该股票对应的全部数据，pandas.DataFrame，index是[日期，股票代码]，columns是字段名
        """
        for item in self.index.levels[1]:
            yield self.data.xs(item, level=1, drop_level=False)

    SECURITY_GEN = security_gen

    # @property
    # @lru_cache()
    def index(self):
        """
        :return: 查询结果的索引，pandas.MultiIndex，[日期，股票代码]
        """
        return self.data.index.remove_unused_levels()

    INDEX = index

#     # @property
    # # @lru_cache()
    # def code(self):
    #     '返回结构体中的代码'
    #     return self.index.levels[1]
    #
    # CODE = code

    # @property
    # @lru_cache()
    def dicts(self):
        """
        :return: dict形式数据，dict，key是索引，tuple，value是{字段名称：取值}
        """
        return self.to_dict('index')

    DICTS = dicts

    # @property
    # @lru_cache()
    def len(self):
        """
        :return: 查询结果的长度，int
        """
        return len(self.data)

    LEN = len

    def get_dict(self, time, code):
        """
        获取指定日期和股票代码的行情数据
        :param time: 日期，str，"%Y-%m-%d"
        :param code: 股票代码，str
        :return: 行情数据，dict，key是字段名称，value是取值
        """
        try:
            return self.dicts[(time, str(code))]
        except Exception as e:
            raise e

    def get(self, name):
        """
        获取性质
        :param name: 性质名称，str
        :return: 在该实例中该性质的值
        """
        if name in self.data.__dir__():
            return eval('self.{}'.format(name))
        else:
            raise ValueError('QADATASTRUCT CANNOT GET THIS PROPERTY')

    def query(self, context):
        """
        查询符合条件的data数据
        :param context: 查询条件，格式同pandas.DataFrame.query，str
        :return: 数据查询结果，pandas.DataFrame，index是[日期，股票代码]，columns是字段名
        """
        try:
            return self.data.query(context)

        except pd.core.computation.ops.UndefinedVariableError:
            print('CANNOT QUERY THIS {}'.format(context))
            pass

    def groupby(self, by=None, axis=0, level=None, as_index=False, sort=False, group_keys=False, squeeze=False,
                observed=False, **kwargs):
        """
        仿dataframe的groupby写法,但控制了by的code和datetime，参数传入同pandas.DataFrame.groupby
        :return: DataFrameGroupBy对象
        """

        if by == self.index.names[1]:
            by = None
            level = 1
        elif by == self.index.names[0]:
            by = None
            level = 0
        return self.data.groupby(by=by, axis=axis, level=level, as_index=as_index, sort=sort, group_keys=group_keys,
                                 squeeze=squeeze, observed=observed)

    def new(self, data=None, dtype=None, if_fq=None):
        """
        未完成方法，目的是保留原查询结果不被后续操作改动
         创建一个新的DataStruct
        data 默认是self.data
        🛠todo 没有这个？？ inplace 是否是对于原类的修改 ？？
        """
        data = self.data if data is None else data
        # data.index= data.index.remove_unused_levels()

        # 🛠todo 不是很理解这样做的意图， 已经copy了，还用data初始化
        # 🛠todo deepcopy 实现 ？还是 ？
        temp = copy(self)
        # temp.__init__(data)
        return data

    def reverse(self):
        """
        :return: 倒序排列的查询结果，pandas.DataFrame，index是[日期，股票代码]，columns是字段名称
        """
        return self.new(self.data[::-1])

    def tail(self, lens):
        """
        :param lens: 长度，int
        :return: 查询结果的后lens条数据，pandas.DataFrame，index是[日期，股票代码]，columns是字段名称
        """
        return self.new(self.data.tail(lens))

    def head(self, lens):
        """
        :param lens: 长度，int
        :return: 查询结果的前lens条数据，pandas.DataFrame，index是[日期，股票代码]，columns是字段名称
        """
        return self.new(self.data.head(lens))

    # def show(self):
    #     """
    #     打印数据包的内容
    #     """
    #     return self.util_log_info(self.data)

    def to_list(self):
        """
        :return: 查询结果的list形式，list，每一个元素为一条数据
        """
        return np.asarray(self.data).tolist()

    def to_pd(self):
        """
        :return: 查询结果的pandas.DataFrame形式
        """
        return self.data

    def to_numpy(self):
        """
        :return: 查询结果的numpy.ndarray形式
        """
        return np.asarray(self.data)

    # def to_json(self):
    #     """
    #     转换DataStruct为json
    #     """
    #     return util_to_json_from_pandas(self.data)

    def to_dict(self, orient='dict'):
        """
        :param orient: 指定dict的value类型，str，dict/list/series/split/records/index
        :return: 查询结果的dict形式，key是字段名称，value是dict，{index：取值}
        """
        return self.data.to_dict(orient)

    def to_hdf(self, place, name):
        """
        :param place: hdf5文件存储路径，str
        :param name: 存储组名，str
        :return:
        """
        self.data.to_hdf(place, name)
        return place, name

    def add_func(self, func, *arg, **kwargs):
        """
        增加计算指标
        :param func: 针对单支股票的指标计算函数
        :param arg: 不定长位置参数
        :param kwargs: 不定长键值参数
        :return: 按照指定函数计算出的指标
        """
        return self.groupby(level=1, sort=False).apply(func, *arg, **kwargs)

    def pivot(self, column_):
        """
        :param column_: 字段名，str/list
        :return: 单个字段名：字段值，pandas.DataFrame，index是日期，columns是股票代码
                  多个字段名：字段值，pandas.DataFrame，index是日期，columns是[字段名，股票代码]
        """
        try:
            return self.data.reset_index().pivot(index='datetime', columns='code', values=column_)
        except KeyError:
            return self.data.reset_index().pivot(index='date', columns='code', values=column_)

    def selects(self, start, end=None, code=None):
        """
        :param start: 开始时间，str，"%Y-%m-%d"
        :param end: 开始时间，str，"%Y-%m-%d"，None为取到结尾
        :param code: 股票代码，str，None为取全部股票
        :return: 根据start、end和code选出的查询结果，index是[日期, 股票代码]，columns是字段名
        @2018/06/03 pandas 的索引问题导致
        https://github.com/pandas-dev/pandas/issues/21299

        因此先用set_index去重做一次index
        影响的有selects,select_time,select_month,get_bar

        @2018/06/04
        当选择的时间越界/股票不存在,raise ValueError

        @2018/06/04 pandas索引问题已经解决
        全部恢复
        """
        try:
            return self.data.loc[(slice(start, end), slice(code)), :]
        except:
            raise ValueError(
                'CANNOT GET THIS CODE {}/START {}/END{} '.format(code, start, end))

    def get_financial(self, code, time):
        """
        :param code: 股票代码，str
        :param time: 日期，str，"%Y-%m-%d"
        :return: 一条数据，pandas.Series，index是字段名
        """
        try:
            return self.data.loc[(time, code)]
        except:
            raise ValueError(
                'DATASTRUCT CURRENTLY CANNOT FIND THIS BAR WITH {} {}'.format(code, time))


# trade = trade_df.set_index('date')
# DATE = trade[trade['m_last_trade_day'] == 1].index.tolist()


class block_data():
    def __init__(self, code=None, date=None, coll=db.wind_block_2014):
        """
        :param code: 股票代码，str
        :param date: 日期，str，"%Y-%m-%d"
        :param coll: 数据库变量，被连接的document为wind_block_2014
        """
        ref_ST = db.wind_ST.find({}, {'_id': 0})
        df = pd.DataFrame(item for item in ref_ST)
        df = df.where(df.notnull(), None)
        self.st = df
        self.DATE = date  # 原始日期
        if date is None:
            self.date = date
        else:
            self.date = util_get_closed_month_end(date)  # 上一月最后一个交易日
        self.code = code
        self.coll_block = coll
        if self.code is None and self.date is not None:
            self.coll = self.coll_block.find({'date': self.date})
        elif self.date is None and self.code is not None:
            self.coll = self.coll_block.find({'code': self.code})
        elif self.code is not None and self.date is not None:
            self.coll = self.coll_block.find({'code': self.code, 'date': self.date})
        else:
            raise ValueError
        self.data = pd.DataFrame(item for item in self.coll).set_index(['date', 'code']).sort_index()
        self.data = self.data[~self.data.index.duplicated()]
        self.data.drop(['_id'], axis=1, inplace=True)

    def __len__(self):
        """
        返回记录的数目
        :return: 查询到的数据长度，int
        """
        return len(self.index)

    def __iter__(self):
        """
        📌关于 yield 的问题
        A yield statement is semantically equivalent to a yield expression.
        yield 的作用就是把一个函数变成一个 generator，
        带有 yield 的函数不再是一个普通函数，Python 解释器会将其视为一个 generator
        for iterObj in ThisObj
        📌关于__iter__ 的问题
        可以不被 __next__ 使用
        Return an iterator object
        iter the row one by one
        :return: 迭代生成器，每一次迭代为查询数据的一行
        """
        for i in range(len(self.index)):
            yield self.data.iloc[i]

    def __add__(self, DataStruct):
        """
        ➕合并数据，重复的数据drop
        :param DataStruct: _quotation_base 继承的子类  QA_DataStruct_XXXX
        :return: _quotation_base 继承的子类  QA_DataStruct_XXXX
        """
        assert isinstance(DataStruct, bar_data)
        return self.new(self.data.append(DataStruct.data).drop_duplicates())

    __radd__ = __add__

    def __sub__(self, DataStruct):
        """
        ⛔️不是提取公共数据， 去掉 DataStruct 中指定的数据
        :param DataStruct:  _quotation_base 继承的子类  QA_DataStruct_XXXX
        :return: _quotation_base 继承的子类  QA_DataStruct_XXXX
        """
        assert isinstance(DataStruct, bar_data)
        return self.new(data=self.data.drop(DataStruct.index).set_index(self.index.names, drop=False))

    __rsub__ = __sub__

    def __getitem__(self, key):
        """
        # 🛠todo 进一步研究 DataFrame __getitem__ 的意义。
        DataFrame调用__getitem__调用(key)
        :param key: 行标签或者列标签
        :return: 标签对应的行或列
        """
        data_to_init = self.data.__getitem__(key)
        if isinstance(data_to_init, pd.DataFrame) is True:
            return self.new(data=data_to_init)
        elif isinstance(data_to_init, pd.Series) is True:
            return data_to_init

    def __getattr__(self, attr):
        """
        # 🛠todo 为何不支持 __getattr__ ？？
        :param attr: 属性名称，str
        :return: self.data的该属性对应的值
        """
        try:
            self.new(data=self.data.__getattr__(attr))
        except:
            raise AttributeError('DataStruct_* Class Currently has no attribute {}'.format(attr))

    """
    ########################################################################################################
    获取序列
    """

    # def ix(self, key):
    #     #return self.new(data=self.data.ix(key))
    #     return self.data.ix(key)
    #
    # def iloc(self, key):
    #     return self.new(data=self.data.iloc(key), dtype=self.type, if_fq=self.if_fq)
    #
    # def loc(self, key):
    #     return self.new(data=self.data.loc(key), dtype=self.type, if_fq=self.if_fq)

    @property
    # @lru_cache()
    def D(self):
        """
        :return: 已退市股票，pandas.DataFrame，index是股票代码，columns是字段名
        """
        ref_D = db.ts_stock_basic.find({}, {'_id': 0})
        df = pd.DataFrame(item for item in ref_D)
        dfD = df[df['list_status'] == 'D'].set_index('code')
        return dfD

    def del_tuishi(self, da):
        """
        :param da: 包含未剔除退市股票的原始数据，list/pandas.DataFrame
        :return: 剔除退市股票后的数据，list/pandas.DataFrame
        """
        d_list = self.D.index.tolist()
        if type(da) == list:
            r_da = []
            for code in da:
                if code not in d_list:
                    r_da.append(code)
                else:
                    if self.date < self.D.at[code, 'delist_date']:
                        r_da.append(code)
            return r_da
        else:
            for code in da.code.tolist():
                if code in d_list and self.date >= self.D.at[code, 'delist_date']:
                    da.drop(da.index[d_list.index(code)], inplace=True)
            da.reset_index(drop=True, inplace=True)
            return da

    # @property
    # @lru_cache()
    def sz50(self):
        """
        :return: 非ST上证50成分股，list
        """
        d_list = self.D.index.tolist()
        da = self.data[self.data['000016'] == 1].reset_index(level=1).drop_duplicates(subset=['code']).code.to_list()
        r_da = []
        for code in da:
            if code not in d_list:
                r_da.append(code)
            else:
                if self.DATE < self.D.at[code, 'delist_date']:
                    r_da.append(code)
        return r_da

    SZ50 = sz50

    # @property
    # @lru_cache()
    def a_share(self):
        """
        :return: 全A股，list
        """
        cursor1 = db.ts_daily_adj_factor.find({'date': self.DATE, 'code': {'$in': basic_codes}}, {'_id': 0, 'code': 1})
        return [data['code'] for data in cursor1]
        # # 原算法
        # d_list = self.D.index.tolist()
        # da = self.data.reset_index(level=1).drop_duplicates(subset=['code']).code.to_list()
        # r_da = []
        # for code in da:
        #     if code not in d_list:
        #         r_da.append(code)
        #     else:
        #         if self.DATE < self.D.at[code, 'delist_date']:
        #             r_da.append(code)
        # return r_da

    A_SHARE = a_share

    # @property
    # @lru_cache()
    def sz50w(self):
        """
        :return: 上证50成分股权重，pandas.Series，index是股票代码
        """
        return self.data['000016w'].reset_index(level=0, drop=True)
        # return self.data.set_index([self.index.levels[1]])['000016w'].dropna(axis=0, how='any')

    SZ50W = sz50w
    SZ50w = sz50w

    # @property
    # @lru_cache()
    def HS300(self):
        """
        :return: 非ST沪深300成分股，list
        """
        d_list = self.D.index.tolist()
        da = self.data[self.data['000300'] == 1].reset_index(level=1).drop_duplicates(subset=['code']).code.to_list()
        r_da = []
        for code in da:
            if code not in d_list:
                r_da.append(code)
            else:
                if self.DATE < self.D.at[code, 'delist_date']:
                    r_da.append(code)
        return r_da

    hs300 = HS300

    # @property
    # @lru_cache()
    def hs300w(self):
        """
        :return: 沪深300成分股权重，pandas.Series，index是股票代码
        """
        return self.data['000300w'].reset_index(level=0, drop=True)

    HS300W = hs300w
    HS300w = hs300w

    # @property
    # @lru_cache()
    def zz500(self):
        """
        :return: 非ST中证500成分股，list
        """
        d_list = self.D.index.tolist()
        da = self.data[self.data['000905'] == 1].reset_index(level=1).drop_duplicates(subset=['code']).code.to_list()
        r_da = []
        for code in da:
            if code not in d_list:
                r_da.append(code)
            else:
                if self.DATE < self.D.at[code, 'delist_date']:
                    r_da.append(code)
        return r_da

    ZZ500 = zz500

    # @property
    # @lru_cache()
    def zz500w(self):
        """
        :return: 中证500成分股权重，pandas.Series，index是股票代码
        """
        return self.data['000905w'].reset_index(level=0, drop=True)

    ZZ500W = zz500w
    ZZ500w = zz500w

    # @property
    # @lru_cache()
    def zz800(self):
        """
        :return: 非ST中证800成分股，list
        """
        d_list = self.D.index.tolist()
        da = self.data[self.data['000906'] == 1].reset_index(level=1).drop_duplicates(subset=['code']).code.to_list()
        r_da = []
        for code in da:
            if code not in d_list:
                r_da.append(code)
            else:
                if self.DATE < self.D.at[code, 'delist_date']:
                    r_da.append(code)
        return r_da

    ZZ800 = zz800

    # @property
    # @lru_cache()
    def zz1000(self):
        """
        :return: 非ST中证1000成分股，list
        """
        d_list = self.D.index.tolist()
        da = self.data[self.data['000852'] == 1].reset_index(level=1).drop_duplicates(subset=['code']).code.to_list()
        r_da = []
        for code in da:
            if code not in d_list:
                r_da.append(code)
            else:
                if self.DATE < self.D.at[code, 'delist_date']:
                    r_da.append(code)
        return r_da

    ZZ1000 = zz1000

    # # @property
    # # @lru_cache()
    # def st(self):
    #     return self.data.ST.replace(0,np.nan).dropna(axis=0, how='any').groupby(level = 'code').sum().index.tolist()
    # ST=st

    # @property
    # @lru_cache()
    def cs(self):
        """
        :return: 中信一级行业代码，pandas.Series，index是股票代码
        """
        # return self.data.set_index([self.index.levels[1]]).CS
        return self.data['CS'].reset_index(level=0, drop=True)
        # return self.data.xs(self.data.index.levels[0][0], level=0).CS

    CS = cs

    # @property
    # @lru_cache()
    def SW(self):
        """
        :return: 申万一级行业代码，pandas.Series，index是股票代码
        """
        return self.data['SW'].reset_index(level=0, drop=True)
        # return self.data.set_index([self.index.levels[1]]).SW

    sw = SW

    # @property
    # @lru_cache()
    def panel_gen(self):
        """
        :return: 时间迭代器，每一次迭代为该日期对应的全部数据，pandas.DataFrame，index是[日期，股票代码]，columns是字段名
        """
        for item in self.index.levels[0]:
            yield self.new(self.data.xs(item, level=0, drop_level=False))

    PANEL_GEN = panel_gen

    # @property
    # @lru_cache()
    def security_gen(self):
        """
        :return: 代码迭代器，每一次迭代为该股票对应的全部数据，pandas.DataFrame，index是[日期，股票代码]，columns是字段名
        """
        for item in self.index.levels[1]:
            yield self.new(self.data.xs(item, level=1, drop_level=False))

    SECURITY_GEN = security_gen

    # @property
    # @lru_cache()
    def index(self):
        """
        :return: 查询结果的索引，pandas.MultiIndex，[日期，股票代码]
        """
        return self.data.index.remove_unused_levels()

    INDEX = index

    # @property
    # # @lru_cache()
    # def code(self):
    #     '返回结构体中的代码'
    #     return self.index.levels[1]
    #
    # CODE = code
    # wind_code = code
    # WIND_CODE = code

    # @property
    # @lru_cache()
    def dicts(self):
        """
        :return: dict形式数据，dict，key是索引，tuple，value是{字段名称：取值}
        """
        return self.to_dict('index')

    DICTS = dicts

    # @property
    # @lru_cache()
    def len(self):
        """
        :return: 查询结果的长度，int
        """
        return len(self.data)

    LEN = len

    # def util_log_info(logs):
    #     logging.info(logs)

    def util_to_datetime(self, time):
        """
        转换字符串时间为datetime格式时间
        :param time: 字符串时间，str，"%Y-%m-%d"/"%Y-%m-%d %H:%M:%S"
        :return: datetime格式时间
        """
        if len(str(time)) == 10:
            _time = '{} 00:00:00'.format(time)
        elif len(str(time)) == 19:
            _time = str(time)
        else:
            print('WRONG DATETIME FORMAT {}'.format(time))
        return datetime.strptime(_time, '%Y-%m-%d %H:%M:%S')

    def get(self, name):
        """
        获取性质
        :param name: 性质名称，str
        :return: 在该实例中该性质的值
        """
        if name in self.data.__dir__():
            return eval('self.{}'.format(name))
        else:
            raise ValueError('QADATASTRUCT CANNOT GET THIS PROPERTY')

    def query(self, context):
        """
        查询符合条件的data数据
        :param context: 查询条件，格式同pandas.DataFrame.query，str
        :return: 数据查询结果，pandas.DataFrame，index是[日期，股票代码]，columns是字段名
        """
        try:
            return self.data.query(context)
        except pd.core.computation.ops.UndefinedVariableError:
            print('CANNOT QUERY THIS {}'.format(context))
            pass

    def groupby(self, by=None, axis=0, level=None, as_index=False, sort=False, group_keys=False, squeeze=False,
                observed=False, **kwargs):
        """
        仿dataframe的groupby写法,但控制了by的code和datetime，参数传入同pandas.DataFrame.groupby
        :return: DataFrameGroupBy对象
        """
        if by == self.index.names[1]:
            by = None
            level = 1
        elif by == self.index.names[0]:
            by = None
            level = 0
        return self.data.groupby(by=by, axis=axis, level=level, as_index=as_index, sort=sort, group_keys=group_keys,
                                 squeeze=squeeze, observed=observed)

    def new(self, data=None, dtype=None, if_fq=None):
        """
        未完成方法，目的是保留原查询结果不被后续操作改动
         创建一个新的DataStruct
        data 默认是self.data
        🛠todo 没有这个？？ inplace 是否是对于原类的修改 ？？
        """
        data = self.data if data is None else data
        # data.index= data.index.remove_unused_levels()

        # 🛠todo 不是很理解这样做的意图， 已经copy了，还用data初始化
        # 🛠todo deepcopy 实现 ？还是 ？
        temp = copy(self)
        # temp.__init__(data)
        return data

    def reverse(self):
        """
        :return: 倒序排列的查询结果，pandas.DataFrame，index是[日期，股票代码]，columns是字段名称
        """
        return self.new(self.data[::-1])

    def tail(self, lens):
        """
        :param lens: 长度，int
        :return: 查询结果的后lens条数据，pandas.DataFrame，index是[日期，股票代码]，columns是字段名称
        """
        return self.new(self.data.tail(lens))

    def head(self, lens):
        """
        :param lens: 长度，int
        :return: 查询结果的前lens条数据，pandas.DataFrame，index是[日期，股票代码]，columns是字段名称
        """
        return self.new(self.data.head(lens))

    # def show(self):
    #     """
    #     打印数据包的内容
    #     """
    #     return util_log_info(self.data)

    def to_list(self):
        """
        :return: 查询结果的list形式，list，每一个元素为一条数据
        """
        return self.data.values.tolist()

    def to_pd(self):
        """
        :return: 查询结果的pandas.DataFrame形式
        """
        return self.data

    def to_numpy(self):
        """
        :return: 查询结果的numpy.ndarray形式
        """
        return np.asarray(self.data)

    # def util_to_json_from_pandas(data):
    #     """需要对于datetime 和date 进行转换, 以免直接被变成了时间戳"""
    #     if 'datetime' in data.columns:
    #         data.datetime = data.datetime.apply(str)
    #     if 'date' in data.columns:
    #         data.date = data.date.apply(str)
    #     return json.loads(data.to_json(orient='records'))

    # def to_json(self):
    #     """
    #     转换DataStruct为json
    #     """
    #     return self.util_to_json_from_pandas(self.data)

    def to_dict(self, orient='dict'):
        """
        :param orient: 指定dict的value类型，str，dict/list/series/split/records/index
        :return: 查询结果的dict形式，key是字段名称，value是dict，{index：取值}
        """
        return self.data.to_dict(orient)

    def to_hdf(self, place, name):
        """
        :param place: hdf5文件存储路径，str
        :param name: 存储组名，str
        :return: （存储路径，存储组名），tuple
        """
        self.data.to_hdf(place, name)
        return place, name

    def add_func(self, func, *arg, **kwargs):
        """
        增加计算指标
        :param func: 针对单支股票的指标计算函数
        :param arg: 不定长位置参数
        :param kwargs: 不定长键值参数
        :return: 按照指定函数计算出的指标
        """
        return self.groupby(level=1, sort=False).apply(func, *arg, **kwargs)

    def pivot(self, column_):
        """
        :param column_: 字段名，str/list
        :return: 单个字段名：字段值，pandas.DataFrame，index是日期，columns是股票代码
                  多个字段名：字段值，pandas.DataFrame，index是日期，columns是[字段名，股票代码]
        """
        try:
            return self.data.reset_index().pivot(index='datetime', columns='code', values=column_)
        except KeyError:
            return self.data.reset_index().pivot(index='date', columns='code', values=column_)

    def selects(self, start, end=None, code=None):
        """
        :param start: 开始时间，str，"%Y-%m-%d"
        :param end: 开始时间，str，"%Y-%m-%d"，None为取到结尾
        :param code: 股票代码，str，None为取全部股票
        :return: 根据start、end和code选出的查询结果，index是[日期, 股票代码]，columns是字段名
        @2018/06/03 pandas 的索引问题导致
        https://github.com/pandas-dev/pandas/issues/21299

        因此先用set_index去重做一次index
        影响的有selects,select_time,select_month,get_bar

        @2018/06/04
        当选择的时间越界/股票不存在,raise ValueError

        @2018/06/04 pandas索引问题已经解决
        全部恢复
        """
        try:
            return self.new(self.data.loc[(slice(start, end), slice(code)), :])
        except:
            raise ValueError(
                'CANNOT GET THIS CODE {}/START {}/END{} '.format(code, start, end))

    def del_ST(self, code_list):
        """
        :param code_list: 包含未剔除ST股票的原始数据，list/pandas.DataFrame
        :return: 剔除ST股票后的数据，list/pandas.DataFrame
        """
        da = code_list.copy()
        for code in code_list:
            if code not in self.st.code.tolist():
                pass
            else:
                for i in range(len(self.st)):
                    if self.st.at[i, 'code'] == code:
                        if self.st.at[i, 'st_date'] is not None and self.st.at[i, 'rst_date'] is not None:
                            if self.st.at[i, 'st_date'] <= int(self.DATE.replace("-", "")) <= self.st.at[i, 'rst_date']:  # correct by lixiang on 2019-06-18
                                if code in da:
                                    da.remove(code)
                        else:
                            if self.st.at[i, 'st_date'] <= int(self.DATE.replace("-", "")):  # correct by lixiang on 2019-06-18
                                if code in da:
                                    da.remove(code)
        return da

        # c_list = code_list.copy()
        # for code in code_list:
        #     if code in self.st:
        #         c_list.remove(code)
        # return c_list

    def get_block(self, code, time):
        """
        :param code: 股票代码，str
        :param time: 日期，str，"%Y-%m-%d"
        :return: 一条数据，pandas.Series，index是字段名
        """
        time = util_get_closed_month_end(time)
        # 原算法
        # for day in DATE:
        #     if str(time)[0:7] in str(day):
        #         time = day
        try:
            return self.data.loc[(time, code)]
        except:
            raise ValueError(
                'DATASTRUCT CURRENTLY CANNOT FIND THIS BAR WITH {} {}'.format(code, time))


class fin_data():
    def __init__(self, code=None, date=None, stock_list=None, n=None, coll=db.wind_financial_q_data):
        """
        季报财务数据接口
        :param code: 股票代码，str
        :param date: 日期，str，"%Y-%m-%d"
        :param stock_list: 股票池，list
        :param n: 交易日数量，int，+为向未来增加，-为向过去增加
        :param coll: 数据库变量，被连接的document为wind_financial_q_data
        """
        self.code = code
        self.stock_list = stock_list
        self.date = date
        self.coll = coll
        if self.code is not None and self.stock_list is None:
            if n > 0:
                ref = self.coll.find({'code': code, 'stm_issuingdate': {'$gte': self.date}}).sort('stm_issuingdate',
                                                                                                  1).limit(n)
                data = pd.DataFrame(item for item in ref)
            else:
                ref = self.coll.find({'code': code, 'stm_issuingdate': {'$lte': self.date}}).sort('stm_issuingdate',
                                                                                                  -1).limit(-n)
                data = pd.DataFrame(item for item in ref)
            data.sort_values('stm_issuingdate', ascending=True)
        elif self.code is None and self.stock_list is not None:
            df_list = []
            for code in stock_list:
                if n > 0:
                    ref = self.coll.find({'code': code, 'stm_issuingdate': {'$gte': self.date}}).sort('stm_issuingdate',
                                                                                                      1).limit(n)
                    data = pd.DataFrame(item for item in ref)
                    df_list.append(data)
                else:
                    ref = self.coll.find({'code': code, 'stm_issuingdate': {'$lte': self.date}}).sort('stm_issuingdate',
                                                                                                      -1).limit(-n)
                    data = pd.DataFrame(item for item in ref)
                    df_list.append(data)
            data = pd.concat(df_list)
            data = data.sort_values('stm_issuingdate', ascending=True)
        data.drop(['_id'], axis=1, inplace=True)
        data = data[data['stm_issuingdate'] > '2011-01-01']
        data.rename(columns={'date': 'wind_q_last_day', 'stm_issuingdate': 'date'}, inplace=True)
        self.data = data.set_index(['date', 'code']).sort_index()


class trade_date():
    def __init__(self, date=None, n=None):
        """
        功能与tool_kit.function中的gen_trade_date相同
        :param date: 日期，str，"%Y-%m-%d"
        :param n: 交易日数量，int，+为向未来增加，-为向过去增加
        """
        if date is not None and n is not None:
            self.date = util_get_real_date(date, towards=-1)
            # 原算法
            # if date not in trade_df.date.tolist():
            #     trade_df_date = trade_df.date.tolist()
            #     trade_df_date = trade_df_date[::-1]
            #     for i in range(len(trade_df_date)):
            #         if trade_df_date[i] < date:
            #             date = trade_df_date[i]
            #             break
            self.n = n
            trade_df_date = trade_df.date.tolist()
            # need_date = []
            if self.n > 0:
                need_date = trade_df_date[trade_df_date.index(self.date): trade_df_date.index(self.date)+n]
                # for i in range(len(trade_df_date)):
                #     if self.date <= trade_df_date[i] and len(need_date) < n:
                #         need_date.append(trade_df_date[i])
            else:
                need_date = trade_df_date[trade_df_date.index(self.date) + n + 1: trade_df_date.index(self.date)+1]
                # trade_df_date = trade_df_date[::-1]
                # for i in range(len(trade_df_date)):
                #     if self.date >= trade_df_date[i] and len(need_date) < -n:
                #         need_date.append(trade_df_date[i])
                # need_date = need_date[::-1]
            self.data = need_date
        else:
            self.data = trade_df.date.tolist()
