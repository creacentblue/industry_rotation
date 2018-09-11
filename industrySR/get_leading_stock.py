# -*- coding:utf-8 -*-
import datetime
import math
import numpy as np
import pandas as pd
import pymssql
# from pathlib import  Path
# import os
# import yaml
import pickle
# from cache import has_cache, load_cache, save_cache, global_cache_dir
# from tantra.utils.symbol import code2symbol
# from tantra.logger import logger
# from tantra.data.gta import GtaDB
import csv
from decimal import *


# 数据库连接
ip = "192.168.0.101"
port = 1433
user = 'WANGLEI'
password = '425189'


# 查询函数
def query(cmd_, db, as_dict):
    with pymssql.connect(
            server=ip, port=port, user=user, password=password, database=db) as conn:
        with conn.cursor(as_dict=as_dict) as cursor:
            cursor.execute(cmd_)
            return [row for row in cursor]


# 排序股票市值确定龙头股
def sort_stock(month_end_data):
    sort_list = sorted(month_end_data, key=lambda e: e.__getitem__('MARKETVALUE'), reverse=True)
    for i in range(len(sort_list)):
        sort_list[i] = sort_list[i]['SYMBOL']
    num = math.floor(0.1 * len(month_end_data))
    return sort_list[0:num]


# 获取某月末某行业的龙头股序列
def get_leading_stock(get_time, ind_code):
    # 行业代码转换
    csv_reader = csv.reader(open('ind_classify.csv'))
    for row in csv_reader:
        if ind_code == row[1]:
            ind_code1 = row[0][0:2]
    s1 = datetime.datetime.strptime("2010-12-31", "%Y-%m-%d")
    e1 = datetime.datetime.strptime("2018-03-13", "%Y-%m-%d")
    f1 = "[STK_MKT_QUOTATIONMONTH]"
    c1 = ' '.join((
        "select [SYMBOL],[CLOSEDATE],[CLOSEPRICE],[MARKETVALUE]",
        "FROM dbo.{}".format(f1),
        "where CLOSEDATE>='{:s}' and".format(str(s1)),
        "CLOSEDATE<='{:s}' and".format(str(e1)),
        "SYMBOL in (SELECT [SYMBOL] FROM [GTA_QIA_QDB].[dbo].[STK_INDUSTRYCLASS] where "
        "INDUSTRYCLASSIFICATIONID = 'P0211' and INDUSTRYCODE like '{}%')".format(ind_code1),
        "order by CLOSEDATE asc"
    ))
    symbol_data = query(c1, "GTA_QIA_QDB", True)
    month_end_data = []
    for item in symbol_data:
        if item['CLOSEDATE'] == get_time:
            month_end_data.append(item)
    leading_stock = sort_stock(month_end_data)
    return leading_stock


# 获取每月末日期
start_date = datetime.datetime.strptime("2010-12-31", "%Y-%m-%d")
end_date = datetime.datetime.strptime("2018-03-13", "%Y-%m-%d")
factor = "STK_CALENDARD"
cmd = ' '.join((
    "select distinct CALENDARDATE",
    "FROM dbo.{}".format(factor),
    "where CALENDARDATE>='{:s}' and".format(str(start_date)),
    "CALENDARDATE<='{:s}' and".format(str(end_date)),
    "ISOPEN='y' and (EXCHANGECODE = 'SSE' or EXCHANGECODE = 'SZSE')",
    "order by CALENDARDATE asc"
))
Count_Date = query(cmd, "GTA_QIA_QDB", True)
month_end = []
for n in range(len(Count_Date) - 1):
    pre = Count_Date[n]['CALENDARDATE'].month
    cur = Count_Date[n + 1]['CALENDARDATE'].month
    if pre != cur:
        month_end.append(Count_Date[n]['CALENDARDATE'])


# 申万行业指数
start_date = datetime.datetime.strptime("2009-12-31", "%Y-%m-%d")
end_date = datetime.datetime.strptime("2018-03-13", "%Y-%m-%d")
factor = "IDX_MKT_QUOTATION"
cmd = ' '.join((
    "select SYMBOL, TRADINGDATE, CLOSEPRICE",
    "FROM dbo.{}".format(factor),
    "where TRADINGDATE>='{:s}' and".format(str(start_date)),
    "TRADINGDATE<='{:s}'".format(str(end_date)),
    "and symbol in (SELECT [SYMBOL]"
    "FROM [GTA_QIA_QDB].[dbo].[IDX_INDEXINFO]"
    "where RELEASINGINSTITUTION = '上海申银万国证券研究所有限公司'"
    "  AND SYMBOL != '801060'"
    "  AND SYMBOL != '801070'"
    "  AND SYMBOL != '801090'"
    "  AND SYMBOL != '801100'"
    "  AND SYMBOL != '801190'"
    "  AND SYMBOL != '801220')"
    "order by SYMBOL asc, TRADINGDATE asc"
))
IndexData = query(cmd, "GTA_QIA_QDB", True)
pre_data = IndexData[0]
temp = []
FLAG = False
index_date = []
symbol = {}
for data in IndexData:
    if data['SYMBOL'] == pre_data['SYMBOL']:
        temp.append(data['CLOSEPRICE'])
        if not FLAG:
            index_date.append(data['TRADINGDATE'])
    else:
        symbol[pre_data['SYMBOL']] = temp
        temp = [data['CLOSEPRICE']]
        if not FLAG:
            FLAG = True
        pre_data = data
symbol[pre_data['SYMBOL']] = temp
df = pd.DataFrame(symbol, index=index_date)
df = df.astype(float)


start_time = datetime.datetime.now()
print("start time0", start_time)
#  获取每月统计一次的龙头股
leading_stock_by_month = {}
for now_time in month_end:
    # 计算距离矩阵
    history_time = now_time-datetime.timedelta(days=250)
    df1 = df[history_time:now_time]
    df1 = df1.corr()
    df1 = df1.applymap(lambda x: '%.2f' % math.sqrt(2*(1-x)))  # 行业距离矩阵
    re1 = df1.sort_values(by='801040')['801040']  # 周期
    re2 = df1.sort_values(by='801120')['801120']  # 消费
    re3 = df1.sort_values(by='801750')['801750']  # 成长
    re4 = df1.sort_values(by='801780')['801780']  # 金融
    # 每种风格选前十个距离最小的行业，
    range1 = re1.keys()[0:10]
    range2 = re2.keys()[0:10]
    range3 = re3.keys()[0:10]
    range4 = re4.keys()[0:10]
    # 选择风格中心行业（一个）
    core1 = [range1[0]]
    core2 = [range2[0]]
    core3 = [range3[0]]
    core4 = [range4[0]]
    core_num = 5  # 每个风格核心行业不超过5
    # 去掉风格重叠的行业，其余行业作为风格核心行业
    for i in range1:
        if i not in range2 and i not in range3 and i not in range4 and i not in core1 and len(core1) < core_num:
            core1.append(i)
    for i in range2:
        if i not in range1 and i not in range3 and i not in range4 and i not in core2 and len(core2) < core_num:
            core2.append(i)
    for i in range3:
        if i not in range2 and i not in range1 and i not in range4 and i not in core3 and len(core3) < core_num:
            core3.append(i)
    for i in range4:
        if i not in range2 and i not in range3 and i not in range1 and i not in core4 and len(core4) < core_num:
            core4.append(i)
    # 获取龙头股
    leading_stock1 = []
    for ind in core1:
        leading_stock1 += get_leading_stock(now_time, ind)
    leading_stock2 = []
    for ind in core2:
        leading_stock2 += get_leading_stock(now_time, ind)
    leading_stock3 = []
    for ind in core3:
        leading_stock3 += get_leading_stock(now_time, ind)
    leading_stock4 = []
    for ind in core4:
        leading_stock4 += get_leading_stock(now_time, ind)
    month_time = now_time.strftime("%Y-%m")
    leading_stock_by_month[month_time] = {'STYLE1': leading_stock1, 'STYLE2': leading_stock1,
                                          'STYLE3': leading_stock3, 'STYLE4': leading_stock4}
    print(now_time)


pickle.dump(leading_stock_by_month, open('leading_stock_by_month.pkl', 'wb'))

