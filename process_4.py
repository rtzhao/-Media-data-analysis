# -*-coding:utf-8-*-
import pandas as pd

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

r=pd.read_csv('../results/test03/result_1.csv',encoding='utf-8')


##########  5.3 ###########
# r.describe() 数据描述

#计算一下价格百分比的变化
returns = r['price'].pct_change()
print(returns.head())

# 计算两列的相关系数和协方差  （相关性）
# returns['MSFT'].corr(returns['IBM'])
# returns['MSFT'].cov(returns['IBM'])