# -*-coding:utf-8-*-
import pandas as pd

# 创建更名字典（同时k用作匹配文件名）、将withhost这列数据处理为01的字典
city_map = {'bj': 'beijing', 'cd': 'chengdu', 'cq': 'chongqing', 'hz': 'hangzhou', 'sh': 'shanghai', 'su': 'suzhou'}
with_host_map = {'与房东分别住不同房间可能与其他房客分住不同房间': '1', '与房东合住可能与房东或其他房客分享一个房间': '0',
                 '可能与其他房客分享一个房间': '0', '房客独享整套房屋': '0', '可能与其他房客分住不同房间': '0'}
#遍历map中的每一个键值对
for k, v in city_map.items():
    #######按地区分别读取house和comment文件，创建dataframe
    house = pd.read_csv("D:/script/Python/examples/Xiaozhu_Reviews_6cities/page1_house_" + k + ".csv", encoding='ansi')
    comment = pd.read_csv("D:/script/Python/examples/Xiaozhu_Reviews_6cities/page2_comment_" + k + ".csv",encoding="ansi")

    #######分离经纬度，选取所需列创建dataframe以及更改列名  location这一列按照','划分，不需要[],然后拼接到原dataframe上
    house = pd.concat([house, house['location'].str.split(',', expand=True)], axis=1)
    house = pd.DataFrame(house, columns=['hid', 'price', 'living_condition', 'area', 'bedroom', 'CITY', 0, 1])
    house = house.rename(columns={"CITY": "city", 0: "lat", 1: "lng", 'living_condition': 'with_host'})

    #######更改city列及with_host列数据，遍历map字典键值对，根据dataframe的apply方法对每行数据处理，key相等时，数据换为value
    def trans_host(x):
        for k, v in with_host_map.items():
            if x == k: x = v
        return x
    def trans_city(x):
        for k, v in city_map.items():
            if x == k: x = v
        return x
    house['with_host'] = house['with_host'].apply(trans_host)
    house['city'] = house['city'].apply(trans_city)

    ########按照comment表的hid分组，获取checkindate的最小值，再重置索引
    comment_date = pd.DataFrame(comment, columns=['hid', 'checkindate'])
    comment_date = comment_date.groupby('hid').agg({'checkindate': 'min'}).reset_index()

    ########根据comment的hid与house合并，更改该列的位置及名称
    xiaozhu = pd.merge(house, comment_date, on='hid', how='left')
    xiaozhu = xiaozhu.rename(columns={"checkindate": "enter_date"})
    xiaozhu = xiaozhu[['hid', 'enter_date', 'price', 'with_host', 'area', 'bedroom', 'city', 'lng', 'lat']]

    ########将数据按地区依次输出到外部
    xiaozhu.to_csv("D:/script/Python/results/test02/xiaozhu_" + k + ".csv", index=0)

