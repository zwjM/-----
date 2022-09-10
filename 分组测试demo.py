import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
'''
构造虚假因子
list为调仓日期
trade_day 为交易日期
'''
df = pd.read_csv('close.csv')
df = df.set_index(['code', 'date'])
df = df['close'].unstack(level=1).T
df1 = df.shift(20)
df = (df - df1)/df
str = df.loc['2021-02-01': '2021-12-31']
df = pd.read_csv('wind_trade_day_2015_2022.csv').set_index('date')
df = df.loc['2021-02-01':'2021-12-31']
trade_day = list(df.loc['2021-02-26':'2021-12-31'].index)
df = df['m_last_trade_day']
list = list(df[df==1].index)
print(list)
str = str.loc[list].T
print(str)


"""
获取后复权价格序列，收益率序列
self.price_series: 价格序列，pandas.Series，index是[股票代码，日期]
self.return_series: 收益率序列，pandas.Series，index是[股票代码，日期]
二者都是date从小到大排序
"""
price_df = pd.read_csv('ts_daily_adj_factor_2015_2022.csv').set_index('date')
price_df = price_df.loc['2021-02-01':'2021-12-31']
price_df = price_df.reset_index()
price_df = price_df.set_index(['code', 'date'])
price_df = price_df[['close', 'adj_factor']]
print(price_df)
price_df['post_close'] = price_df['close'] * price_df['adj_factor']
price_df['return'] = price_df['post_close'].groupby('code').pct_change()
price_series = price_df['post_close'].copy()
return_series = price_df['return'].copy()
print(return_series)


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
full_factor_ls = []
for date in trade_day:  # trade_day是所有交易日的日期
    if date in list:  # list只包含换仓交易日日期
        date_factor = str[date].copy()
    else:
        date_factor = full_factor_ls[-1].copy()
        date_factor.rename(date, inplace=True)
    full_factor_ls.append(date_factor)
full_factor_df = pd.concat(full_factor_ls, axis=1, join='outer', sort=True)
full_factor_df = full_factor_df.sort_index(axis=1)
print(full_factor_df)

# 计算完整因子百分比排名矩阵self.full_rank_df
full_rank_df = full_factor_df.rank(method='dense', pct=True)

# 计算完整因子分组矩阵self.full_group_df，列为排序的股票，行为日期（所有交易日），值为该股票该天第几组
full_group_df = ((full_rank_df * 100) / ((1 / 10) * 100) + 1).fillna(0).astype(int)
full_group_df.replace({0: np.nan, 10 + 1: 10}, inplace=True)

# 计算完整因子分组序列self.full_group_series，并shift(1)，满足T日收益按照T-1日因子分组，一重索引为groupby的股票代码，二重索引为该股票代码的所有交易日，值为组号
full_group_series = full_group_df.stack(dropna=False)
full_group_series.index.set_names(['code', 'date'], inplace=True)
full_group_series = full_group_series.groupby('code').shift(1).dropna()

# 计算分组收益序列self.group_return_series
group_match_return_df = pd.concat([full_group_series.rename('group'), return_series.rename('return')],
                                axis=1, join='outer').reindex(full_group_series.index)
group_return_series = group_match_return_df.reset_index().groupby(['group', 'date'])['return'].mean()

# 计算分组收益矩阵self.group_return_df
group_return_df = group_return_series.unstack(level=0)
group_return_df.loc[trade_day[0], :] = 0
group_return_df.sort_index(inplace=True)

group_return_df.loc[:, 'long-short'] = (group_return_df.loc[:, 10] - group_return_df.loc[:, 1]) * 0.5

#计算分组净值矩阵self.group_value_df
group_value_df = (group_return_df + 1).cumprod()
print(group_value_df)




annual_return = group_value_df.iloc[-1, :] ** (252.0 / len(group_value_df)) - 1
drawdown_df = group_value_df.apply(lambda x: x / group_value_df.loc[:x.name, :].max() - 1, axis=1)
max_drawdown = drawdown_df.min() * -1
daily_ret_df = group_value_df.pct_change().fillna(0)
sharpe_ratio = daily_ret_df.mean() / daily_ret_df.std() * (252 ** 0.5)
indicator_df = pd.concat([annual_return.rename('annualized_returns'), max_drawdown.rename('max_drawdown'),
                              sharpe_ratio.rename('sharpe')], axis=1, join='outer')
print(indicator_df)


"""
画净值图，图片直接保存到本地
:param factor_name: 因子名称，str
:param picture_name: 存储图片名称，str
"""

group = 10
group_value_df['date'] = pd.to_datetime(group_value_df.index, format='%Y-%m-%d')
group_value_df = group_value_df.reset_index(drop=True)
pd.plotting.register_matplotlib_converters()
plt.figure(figsize=(20, 10))
plt.suptitle('str')
ax1 = plt.subplot(211)
ax1.plot(group_value_df['date'], group_value_df.iloc[:, :group].values)
ax1.grid(True)
print(group_value_df.iloc[:, :group].values)
ax2 = plt.subplot(212)
ax2.plot(group_value_df['date'], group_value_df['long-short'], color='#8B4513')
ax2.grid(True)
ax2.legend(labels=['long-short'], loc=2)
plt.show()
