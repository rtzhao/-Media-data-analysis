# -*-coding:utf-8-*-
import pandas as pd

######## 上一步骤传递过来的待重新计算打分、评论、照片的数据
sight_p = pd.read_csv('D:/script/Python/results/process1.csv', encoding='utf-8', engine='python')

######## comment_p数据预处理，删除默认好评、重复评论、无用列
comment = pd.read_csv('D:/script/Python/examples/comment.csv', encoding='ansi')
comment_p = comment[comment['default'].isin(['0'])]  # 将默认好评，未评论的用户数据剔除
comment_p = comment_p.drop_duplicates(subset=['content', 'auther'])  # 去除重复的无用评论
comment_p = comment_p.drop(['id', 'auther', 'default'], axis=1)  #舍去不需要的列

######## 将sight_p和comment_p 按照 sight_id和date合并，得到待计算的数据sight_comment_P
sight_comment_p = pd.merge(sight_p, comment_p, on=['sight_id', 'date'], how='outer')

########  按照sight_id和date分组，但由于后续一些列仍需要用到，这里同样写上；分组后将对应列处理后聚合；重置索并引重命名；将处理结果输出到文件
def comment_img_count_sum(arr): return arr.sum()# 定义comment_img_count_sum求和方法，方便之后重命名
num_agg = {'score': ['sum', 'mean'], 'img_count': comment_img_count_sum, 'content': 'count'}  # 每列不同的聚合函数，用字典表示
sight_comment_p = sight_comment_p.groupby(['sight_id', 'date','sight_name','level','price','city','lng','lat']).agg(num_agg).reset_index()
sight_comment_p.columns = ['sight_id', 'date','sight_name','level','price','city','lng','lat','score_sum',
                           'score_avgerage', 'comment_img_count_sum', 'comment_count']
sight_comment_p.to_csv('../results/process2_4.csv', index=0)


