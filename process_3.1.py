# -*-coding:utf-8-*-
import pandas as pd
import numpy as np
from math import isnan, radians, asin, sqrt, pow, sin, cos

#####创建map，key用来匹配文件名，value用来匹配城市名
city_map = {'bj': 'beijing', 'cd': 'chengdu', 'cq': 'chongqing', 'hz': 'hangzhou', 'sh': 'shanghai', 'su': 'suzhou'}

###############读取第二阶段处理后的文件数据创建dataframe，定义需拼接的dataframe，将两者合并之后将指定列数据设置为0
s = pd.read_csv('D:/script/Python/results/process2_4.csv', encoding='utf-8')
other_colums_df = pd.DataFrame(columns=["xz_entry1k", "xz_entry5k", "xz_entry10k",
                                        "xz_entries1k", "xz_entries5k", "xz_entries10k",
                                        "xz_entries_withhost1k", "xz_entries_withhost5k", "xz_entries_withhost10k",
                                        "xz_review1k", "xz_review5k", "xz_review10k",
                                        "xz_mr1k", "xz_mr5k", "xz_mr10k",
                                        "xz_price1k", "xz_price5k", "xz_price10k",
                                        "xz_area1k", "xz_area5k", "xz_area10k",
                                        "xz_beds1k", "xz_beds5k", "xz_beds10k"])
s = pd.concat([s, other_colums_df], axis=1)
s.loc[:, ["xz_entry1k", "xz_entry5k", "xz_entry10k"]] = 0

#####距离计算函数，返回值单位为千米，公式为借鉴老师提供的代码，调用map()函数对所有的经纬度计算弧度，之后套用计算距离的公式即可
EARTH_RADIUS = 6378.137 #地球半径
def get_distance(row, lat2, lon2):
    lat1 = row.loc['lat']
    lon1 = row.loc['lng']

    if isnan(row['lat']) or isnan(row['lng']):return np.nan

    radlat1, radlng1, radlat2, radlng2 = map(radians, [lon1, lat1, lon2, lat2])
    a = radlat1 - radlat2
    b = radlng1 - radlng2

    s = 2 * asin(sqrt(pow(sin(a / 2), 2) + cos(radlat1) * cos(radlat2) * pow(sin(b / 2), 2)))
    s = s * EARTH_RADIUS
    s = round(s * 10000.0) / 10000.0
    return s

new_s_df = pd.DataFrame() #创建空dataframe，用于保存后续步骤的数据切片，方便最终输出

#遍历city_map的所有键值对，匹配文件名和城市名
for k, v in city_map.items():

    ####### 分别读取经过处理后得到的每个城市的xiaozhu、以及xiaozhu_comment
    xiaozhu = pd.read_csv("D:/script/Python/results/test02/xiaozhu_"+k+".csv", encoding='ansi')
    xiaozhu_comment = pd.read_csv("D:/script/Python/results/test04/xiaozhu_comment_"+k+".csv", encoding='ansi',parse_dates=[0,1])

    ####### 获取s表中city为当前城市的切片sb，遍历sb的每一行数据
    sb = s[s['city'] == v].copy()
    i = 0
    for index, row in sb.iterrows():
        i += 1
        if i % 100 == 0: print(i)

        #################  xz_entry、xz_entries、xz_entries_withhost 列的处理 ################
        ##### 获取xiaozhu enter_date列等于sb date的所有行的切片组成xz
        xz = xiaozhu[xiaozhu['enter_date'] == row['date']].copy()
        if not xz.empty:

            ##### 对xz的每一行执行匿名函数：根据景区和房子的经纬度计算距离k，将其放入到zx的distence列中
            k = xz.apply(lambda x: get_distance(x, row.loc['lat'], row.loc['lng']), axis=1)
            xz.loc[:, 'distance'] = k

            ##### 计算距离在1、5、10km内的房子数量，结果分别填入到xz_entries1k、xz_entries5k、xz_entries10k对应行和列中
            xz_entries1k = xz[xz['distance'] <= 1.0]['hid'].count()
            xz_entries5k = xz[xz['distance'] <= 5.0]['hid'].count()
            xz_entries10k = xz[xz['distance'] <= 10.0]['hid'].count()

            sb.loc[index, 'xz_entries1k'] = xz_entries1k
            sb.loc[index, 'xz_entries5k'] = xz_entries5k
            sb.loc[index, 'xz_entries10k'] = xz_entries10k

            ##### 根据上一步计算的得出的结果，如果对应距离内有房子，就将xz_entry1k、xz_entry5k、xz_entry10k对应行列设为1，表示有房子在这个范围内
            if xz_entries1k > 0: sb.loc[index, 'xz_entry1k'] = 1
            if xz_entries5k > 0: sb.loc[index, 'xz_entry5k'] = 1
            if xz_entries10k > 0: sb.loc[index, 'xz_entry10k'] = 1

            ##### 计算距离在1、5、10km内的with_host列数据的和，结果分别填入到
            ##### xz_entries_withhost1k、xz_entries_withhost5k、xz_entries_withhost10k对应行和列中
            sb.loc[index, 'xz_entries_withhost1k'] = xz[xz['distance'] <= 1.0]['with_host'].sum()
            sb.loc[index, 'xz_entries_withhost5k'] = xz[xz['distance'] <= 5.0]['with_host'].sum()
            sb.loc[index, 'xz_entries_withhost10k'] = xz[xz['distance'] <= 10.0]['with_host'].sum()

        #################### xz_price、xz_area、xz_beds 列的处理###########################

        ##### 整体方法与上一个if判断语句中的内容相似，不同的是切片为enter_date<datede行，
        ##### 同时price、area、bedroom在距离范围内的平均值，再将结果存入dataframe中对应的位置
        xz = xiaozhu[xiaozhu['enter_date'] <= row['date']].copy()
        if not xz.empty:
            k = xz.apply(lambda x: get_distance(x, row.loc['lat'], row.loc['lng']), axis=1)
            xz.loc[:, 'distance'] = k

            sb.loc[index, 'xz_price1k'] = xz[xz['distance'] <= 1.0]['price'].mean()
            sb.loc[index, 'xz_price5k'] = xz[xz['distance'] <= 5.0]['price'].mean()
            sb.loc[index, 'xz_price10k'] = xz[xz['distance'] <= 10.0]['price'].mean()

            sb.loc[index, 'xz_area1k'] = xz[xz['distance'] <= 1.0]['area'].mean()
            sb.loc[index, 'xz_area5k'] = xz[xz['distance'] <= 5.0]['area'].mean()
            sb.loc[index, 'xz_area10k'] = xz[xz['distance'] <= 10.0]['area'].mean()

            sb.loc[index, 'xz_beds1k'] = xz[xz['distance'] <= 1.0]['bedroom'].mean()
            sb.loc[index, 'xz_beds5k'] = xz[xz['distance'] <= 5.0]['bedroom'].mean()
            sb.loc[index, 'xz_beds10k'] = xz[xz['distance'] <= 10.0]['bedroom'].mean()

        #################### xz_review、xz_mr列的处理 ####################################
        ###### 获取xiaozhu_comment中enter_date=当前行date的所有行的切片
        xz_c = xiaozhu_comment[xiaozhu_comment['enter_date'] == row['date']].copy()
        if not xz_c.empty:
            ###### 之后的步骤与第一个if条件语句中的内容相似，都是先根景点和房子的经纬度计算距离存入distance列中
            ###### 再根据该距离与距离范围1km、5km、10km比较，分别统计content以及reply列的和，最后将数据存入xz_review、xz_mr相关列中
            k = xz_c.apply(lambda x: get_distance(x, row.loc['lat'], row.loc['lng']), axis=1)
            xz_c.loc[:, 'distance'] = k

            sb.loc[index, 'xz_review1k'] = xz_c[xz_c['distance'] <= 1.0]['content'].sum()
            sb.loc[index, 'xz_review5k'] = xz_c[xz_c['distance'] <= 5.0]['content'].sum()
            sb.loc[index, 'xz_review10k'] = xz_c[xz_c['distance'] <= 10.0]['content'].sum()

            sb.loc[index, 'xz_mr1k'] = xz_c[xz_c['distance'] <= 1.0]['reply'].sum()
            sb.loc[index, 'xz_mr5k'] = xz_c[xz_c['distance'] <= 5.0]['reply'].sum()
            sb.loc[index, 'xz_mr10k'] = xz_c[xz_c['distance'] <= 10.0]['reply'].sum()

    new_s_df=new_s_df.append(sb)#每遍历一行将数据存入到new_s_df中

new_s_df.to_csv('../results/test03/result_2.csv', index=0) #将最后的结果输出到外部文件中

