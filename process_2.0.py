# -*-coding:utf-8-*-
import pandas as pd

########读取实验一最终完成的数据文件创建dataframe并舍去需要重新统计计算的列
sight_p = pd.read_csv('D:/script/Python/results/Result6.csv', encoding='utf-8', engine='python')
sight_p = sight_p.drop(['score_sum', 'score_avgerage', 'comment_img_count_sum', 'comment_count'], axis=1)

########读取评论文件创建dataframe，并按'sight_id'分组，聚合函数计算一个景点的最早和最晚评论
comment = pd.read_csv('D:/script/Python/examples/comment.csv', encoding='ansi', engine='python')
comment_sight = comment.groupby('sight_id').agg({'date': ['min', 'max']})
comment_sight.columns = [col[1] for col in comment_sight.columns.values]  # 将多重索引变为单层索引
comment_sight = comment_sight.reset_index()  # 重置索引

########遍历comment_sight的每一行，调用pandas.data_range生成时间序列，将时间序列作为'date'列数据，sight_id作为'sight_id'列数据
######## 创建temp这个临时dataframe，调用append()添加到总的dataframe上，得到sight_id和对应时间序列的dataframe
ts = pd.DataFrame(columns=['date', 'sight_id'])
for index, row in comment_sight.iterrows():
    rng = pd.date_range(start=row['min'], end=row['max'],normalize=True,freq='MS',closed=None)
    print(row['sight_id'],rng)
    temp = pd.DataFrame({'date': rng, 'sight_id': row['sight_id']})
    ts = ts.append(temp)

########将景点对应的时间序列按照sight_id合并到原dataframe上，得到原数据基础上添加完完时间序列的数据
sight_p = pd.merge(sight_p, ts, on='sight_id', how='outer')
sight_p.to_csv('../results/process1.csv', index=0)


