# -*-coding:utf-8-*-
import pandas as pd

#创建map，k用作匹配文件名
city_map = {'bj': 'beijing', 'cd': 'chengdu', 'cq': 'chongqing', 'hz': 'hangzhou', 'sh': 'shanghai', 'su': 'suzhou'}

#遍历map的每个键值对
for k, v in city_map.items():

    ########### 按区域读取每个城市的house、house_comment文件创建对应dataframe，并舍弃不需要的列
    xiaozhu= pd.read_csv("D:/script/Python/results/test02/xiaozhu_"+k+".csv", encoding='ansi')
    xiaozhu_comment = pd.read_csv("D:/script/Python/examples/Xiaozhu_Reviews_6cities/page2_comment_"+k+".csv",encoding="ansi")
    xiaozhu_comment=xiaozhu_comment.drop(['did','uid','uname','ulink'],axis=1)

    ########### 将house与house_comment按hid合并，并按照hid和enter_date分组计算评论和回复数并重置的索引输出到外部文件，
    # 这里为了后续处理方便，分组时添加了city、经纬度列
    xiaozhu_p=pd.merge(xiaozhu ,xiaozhu_comment, on=['hid'], how='outer')
    xiaozhu_p = xiaozhu_p.groupby(['hid', 'enter_date','city','lng','lat']).agg({'content': 'count', 'reply':'count'}).reset_index()
    xiaozhu_p.to_csv("D:/script/Python/results/test04/xiaozhu_comment_" + k + ".csv", index=0)

