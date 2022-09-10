import matplotlib.pyplot as plt
from scipy import stats
import pandas as pd
import numpy as np
from 单因子测试.tool_kit.date_N_time import gen_trade_date, shift_date
from 单因子测试.tool_kit.base_datastruct import block_data
from 单因子测试.tool_kit.utility_tool import cal_indicator


class BackTest(object):
    """
    单因子回测工具类，包括计算年化收益率、最大回撤、夏普比率、IC、ICIR，绘制分组回测净值图
    :param factor_df: 原始因子值矩阵，pandas.DataFrame，index是股票代码，columns是日期（只包含换仓日即可）
    :param s_date: 回测开始日期，如果不指定，则为因子矩阵的开始日期，"%Y-%m-%d"
    :param e_date: 回测结束日期，如果不指定，则为因子矩阵的结束日期向未来跳转一个频率窗口，"%Y-%m-%d"
    :param universe: 股票池，str，A/hs300/zz500/zz800
    :param group: 分组数量，int
    :param cal_ls_ret: 是否计算long-short收益，bool
    """
    def __init__(self, factor_df=None, s_date='', e_date='', freq='', universe='a_share', group=5, cal_ls_ret=False):
        self.factor_df = factor_df
        self.s_date = s_date
        self.e_date = e_date
        self.freq = freq
        self.trade_date_ls = factor_df.columns.tolist()
        self.date_ls = gen_trade_date(self.s_date, self.e_date)
        self.universe = universe
        self.group = group
        self.cal_ls_ret = cal_ls_ret
        self.db = db_zcs
        self.price_series = pd.Series()
        self.return_series = pd.Series()
        self.full_factor_df = pd.DataFrame()
        self.full_rank_df = pd.DataFrame()
        self.full_group_df = pd.DataFrame()
        self.full_group_series = pd.Series()
        self.group_return_series = pd.Series()
        self.group_return_df = pd.DataFrame()
        self.group_value_df = pd.DataFrame()
        self.indicator = pd.DataFrame()
        self.ic_series = pd.Series()
        self.ic_mean = 0.0
        self.icir = 0.0

    def match_price(self):
        """
        获取后复权价格序列，收益率序列
        self.price_series: 价格序列，pandas.Series，index是[股票代码，日期]
        self.return_series: 收益率序列，pandas.Series，index是[股票代码，日期]
        二者都是date从小到大排序
        """
        cursor = self.db.ts_daily_adj_factor.find({'date': {'$gte': self.s_date, '$lte': shift_date(self.e_date, self.freq, 'post')}},
                                                  {'_id': 0, 'date': 1, 'code': 1, 'close': 1, 'adj_factor': 1})
        price_df = pd.DataFrame(list(cursor)).set_index(['code', 'date'])
        price_df['post_close'] = price_df['close'] * price_df['adj_factor']
        price_df['return'] = price_df['post_close'].groupby('code').pct_change()
        self.price_series = price_df['post_close'].copy()
        self.return_series = price_df['return'].copy()

    def get_group(self):
        """
        按照因子值分组
        self.full_factor_df: 完整因子值矩阵，pandas.DataFrame，index是股票代码，columns是self.date_ls中的日期,月末换仓，月中的数据与月末相同
        self.full_rank_df: 完整因子百分比排名矩阵，pandas.DataFrame，index是股票代码，columns是self.date_ls中的日期
        self.full_group_df: 完整因子分组矩阵，pandas.DataFrame，index是股票代码，columns是self.date_ls中的日期
        self.full_group_series: 完整因子分组序列，pandas.Series，index是[股票代码，self.date_ls中的日期]，剔除nan值，
        并shift(1)
        self.group_return_series: 分组收益序列，pandas.Series，index是[组名，self.date_ls中的日期]
        self.group_return_df: 分组收益矩阵，pandas.DataFrame，index是self.date_ls中的日期，columns是组名+long-short
        self.group_value_df: 分组净值矩阵，pandas.DataFrame，index是self.date_ls中的日期，columns是组名+long-short
        """

        # 计算完整因子值矩阵self.full_factor_df ,数据填充，换仓日外的数据与换仓日相同
        self.full_factor_ls = []
        for date in self.date_ls:  # date_ls是所有交易日的日期
            if date in self.trade_date_ls:  # trade_date_ls是传入的factor_df的列，只包含换仓交易日日期
                if self.universe == 'a_share':
                    date_factor = self.factor_df[date].copy()
                else:
                    universe_codes = eval('block_data(date=date).%s()' % self.universe)
                    date_factor = self.factor_df[date].reindex(universe_codes)
                date_factor.rename(date, inplace=True)
            else:
                date_factor = self.full_factor_ls[-1].copy()
                date_factor.rename(date, inplace=True)
            self.full_factor_ls.append(date_factor)
        self.full_factor_df = pd.concat(self.full_factor_ls, axis=1, join='outer', sort=True)
        self.full_factor_df = self.full_factor_df.sort_index(axis=1)

        # 计算完整因子百分比排名矩阵self.full_rank_df
        self.full_rank_df = self.full_factor_df.rank(method='dense', pct=True)

        # 计算完整因子分组矩阵self.full_group_df，列为排序的股票，行为日期（所有交易日），值为该股票该天第几组
        self.full_group_df = ((self.full_rank_df*100)/((1/self.group)*100) + 1).fillna(0).astype(int)
        self.full_group_df.replace({0: np.nan, self.group + 1: self.group}, inplace=True)

        # 计算完整因子分组序列self.full_group_series，并shift(1)，满足T日收益按照T-1日因子分组，一重索引为groupby的股票代码，二重索引为该股票代码的所有交易日，值为组号
        self.full_group_series = self.full_group_df.stack(dropna=False)
        self.full_group_series.index.set_names(['code', 'date'], inplace=True)
        self.full_group_series = self.full_group_series.groupby('code').shift(1).dropna()

        # 计算分组收益序列self.group_return_series
        group_match_return_df = pd.concat([self.full_group_series.rename('group'), self.return_series.rename('return')],
                                          axis=1, join='outer').reindex(self.full_group_series.index)
        self.group_return_series = group_match_return_df.reset_index().groupby(['group', 'date'])['return'].mean()

        # 计算分组收益矩阵self.group_return_df
        self.group_return_df = self.group_return_series.unstack(level=0)
        self.group_return_df.loc[self.date_ls[0], :] = 0
        self.group_return_df.sort_index(inplace=True)
        if self.cal_ls_ret:
            self.group_return_df.loc[:, 'long-short'] = (self.group_return_df.loc[:, self.group] - self.group_return_df.loc[:, 1])*0.5
            
        # 计算分组净值矩阵self.group_value_df
        self.group_value_df = (self.group_return_df + 1).cumprod()

    def cal_indicator(self):
        """
        计算年化收益率、最大回撤、sharpe ratio
        self.indicator: 回测指标，pandas.DataFrame，index是组名+long-short，columns是指标名称
        """
        self.indicator = cal_indicator(self.group_value_df)

    def plot_value(self, factor_name='', picture_name=''):
        """
        画净值图，图片直接保存到本地
        :param factor_name: 因子名称，str
        :param picture_name: 存储图片名称，str
        """
        self.group_value_df['date'] = pd.to_datetime(self.group_value_df.index, format='%Y-%m-%d')
        self.group_value_df = self.group_value_df.reset_index(drop=True)
        pd.plotting.register_matplotlib_converters()
        plt.figure(figsize=(20, 10))
        plt.suptitle(factor_name)
        if self.cal_ls_ret:
            ax1 = plt.subplot(211)
            ax1.plot(self.group_value_df['date'], self.group_value_df.iloc[:, :self.group])
            ax1.grid(True)
            ax1.legend(labels=[str(i) for i in range(1, self.group+1)], loc=2)
            ax2 = plt.subplot(212)
            ax2.plot(self.group_value_df['date'], self.group_value_df['long-short'], color='#8B4513')
            ax2.grid(True)
            ax2.legend(labels=['long-short'], loc=2)
        else:
            ax1 = plt.subplot(111)
            ax1.plot(self.group_value_df['date'], self.group_value_df.iloc[:, :self.group])
            ax1.grid(True)
            ax1.legend(labels=[str(i) for i in range(1, self.group + 1)], loc=2)
        plt.savefig('%s.jpg' % picture_name)
        plt.show()

    def cal_icir(self, rank=False):
        """
        计算因子的IC和ICIR
        :param rank: 是否计算rankIC，bool，True为计算rankIC，Flase为计算IC
        self.ic_series: 因子IC，pandas.Series，index是日期
        self.ic_mean: IC均值，float
        self.icir: ICIR值，float
        """
        price_df = self.price_series.unstack(level=1).loc[:, self.trade_date_ls + [shift_date(self.e_date, self.freq, 'post')]]
        freq_return_df = price_df.pct_change(axis=1)
        ic_ls = []
        for i in range(0, self.factor_df.shape[1]):
            one_df = pd.concat([self.factor_df.iloc[:, i], freq_return_df.iloc[:, i+1]], join='inner', axis=1).dropna()
            if rank:
                result = stats.spearmanr(one_df)
            else:
                result = stats.pearsonr(one_df)
            if result[1] > 0.05:
                ic_ls.append(0)
            else:
                ic_ls.append(result[0])
        self.ic_series = pd.Series(ic_ls, index=self.trade_date_ls)
        self.ic_mean = self.ic_series.mean()
        self.icir = self.ic_series.mean()/self.ic_series.std()


def back_test_from_portfolio(portfolio_df=None, freq='', strategy_name=''):
    """
    根据具体组合持仓进行回测
    :param portfolio_df: 持仓权重，pandas.DataFrame，index是日期，columns是股票代码
    :param freq: 调仓频率，str，d/w/2w/m代表日频、周频、半月频、月频
    :param strategy_name: 策略名称，str
    :return: 净值图直接保存到本地，回测指标，dict，key是annual_return、max_drawdown、sharpe_ratio，value是指标值
    """
    date_ls = gen_trade_date(portfolio_df.index[0], shift_date(portfolio_df.index[-1], freq, direction='post'))
    full_portfolio_df = pd.DataFrame(index=date_ls, columns=portfolio_df.columns)
    full_portfolio_df.update(portfolio_df)
    full_portfolio_df.ffill(axis=0, inplace=True)
    full_portfolio_df = full_portfolio_df.shift(periods=1)
    db = db_zcs
    cursor = db.ts_daily_adj_factor.find({'code': {'$in': portfolio_df.columns.tolist()},
                                          'date': {'$gte': date_ls[0], '$lte': date_ls[-1]}},
                                         {'_id': 0, 'date': 1, 'code': 1, 'close': 1, 'adj_factor': 1})
    price_df = pd.DataFrame(list(cursor)).set_index(['date', 'code'])
    price_df['post_close'] = price_df['close'] * price_df['adj_factor']
    return_df = price_df['post_close'].unstack(level=1).pct_change()
    portfolio_return_series = (full_portfolio_df*return_df).sum(axis=1)
    value_series = (1 + portfolio_return_series).cumprod()
    annual_return = value_series.iloc[-1] ** (252/len(value_series)) - 1
    drawdown_series = pd.Series([value_series.loc[date]/value_series.loc[:date].max() - 1 for date in value_series.index], index=value_series.index)
    max_drawdown = drawdown_series.min() * -1
    sharpe_ratio = portfolio_return_series.mean()/portfolio_return_series.std()*(252**0.5)
    pd.plotting.register_matplotlib_converters()
    plt.figure()
    plt.title(strategy_name)
    plt.plot(pd.to_datetime(value_series.index, format='%Y-%m-%d'), value_series)
    plt.grid()
    plt.savefig('%s.jpg' % strategy_name)
    return {'annual_return': annual_return, 'max_drawdown': max_drawdown, 'sharpe_ratio': sharpe_ratio}
