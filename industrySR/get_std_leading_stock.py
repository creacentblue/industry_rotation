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
def get_leading_stock(get_time,ind_code):
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


start_time = datetime.datetime.now()
print("start time0", start_time)
#  获取每月统计一次的龙头股
leading_stock_by_month = {}
for now_time in month_end:
    core1 = ['801040', '801020', '801030', '801050']  # 周期
    core2 = ['801120', '801110', '801150']  # 消费
    core3 = ['801750', '801080', '801770', '801760']  # 成长
    core4 = ['801780', '801790']  # 金融
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
    leading_stock_by_month[month_time] = {'STYLE1': leading_stock1, 'STYLE2': leading_stock2,
                                          'STYLE3': leading_stock3, 'STYLE4': leading_stock4}
    print(now_time)


pickle.dump(leading_stock_by_month, open('leading_stock_by_month_std.pkl', 'wb'))

