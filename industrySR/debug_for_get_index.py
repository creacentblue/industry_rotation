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


leading_stock_by_month = pickle.load(open('leading_stock_by_month_std.pkl', 'rb'))


#  获取股票每日数据
start_date = datetime.datetime.strptime("2010-12-31", "%Y-%m-%d")
end_date = datetime.datetime.strptime("2018-03-13", "%Y-%m-%d")
factor = "STK_MKT_QUOTATION"
cmd = ' '.join((
    "select TRADINGDATE, SYMBOL, CLOSEPRICE",
    "FROM dbo.{}".format(factor),
    "where TRADINGDATE>='{:s}' and".format(str(start_date)),
    "TRADINGDATE<='{:s}'".format(str(end_date)),
    "order by TRADINGDATE asc"
))
STK_data1 = query(cmd, "GTA_QIA_QDB", True)


# 获取每月末日期
start_date = datetime.datetime.strptime("2010-12-31", "%Y-%m-%d")
end_date = datetime.datetime.strptime("2018-02-28", "%Y-%m-%d")
factor = "STK_CALENDARD"
cmd = ' '.join((
    "select distinct CALENDARDATE",
    "FROM dbo.{}".format(factor),
    "where CALENDARDATE>='{:s}' and".format(str(start_date)),
    "CALENDARDATE<='{:s}' and".format(str(end_date)),
    "ISOPEN='y' and (EXCHANGECODE = 'SSE' or EXCHANGECODE = 'SZSE')",
    "order by CALENDARDATE asc"
))
Calendar_Date1 = query(cmd, "GTA_QIA_QDB", True)
Calendar_Date = []
for item in Calendar_Date1:
    Calendar_Date.append(item['CALENDARDATE'])


n = 0
run_time = 0
start_time = datetime.datetime.now()
print("start time1", start_time)
#  制定风格指数（龙头股指数均值）
index1 = []
index2 = []
index3 = []
index4 = []
date_i = 0   # 时间段初始数据下标
date_temp = 0  # 时间段末尾数据下标
for date in Calendar_Date:
    pre_date = STK_data1[date_i]['TRADINGDATE']
    for ii in range(date_i, len(STK_data1)):
        if STK_data1[ii]['TRADINGDATE'] != pre_date:
            date_temp = ii
            break
    STK_data = STK_data1[date_i:date_temp]
    date_i = date_temp
    month_time = date.strftime("%Y-%m")
    stocks1 = leading_stock_by_month[month_time]['STYLE1']
    stocks2 = leading_stock_by_month[month_time]['STYLE2']
    stocks3 = leading_stock_by_month[month_time]['STYLE3']
    stocks4 = leading_stock_by_month[month_time]['STYLE4']
    sum_value = 0
    for stock in stocks1:
        for jj in STK_data:
            if jj['SYMBOL'] == stock:
                sum_value += jj['CLOSEPRICE']
                break
    index1.append(sum_value / len(stocks1))
    sum_value = 0
    for stock in stocks2:
        for jj in STK_data:
            if jj['SYMBOL'] == stock:
                sum_value += jj['CLOSEPRICE']
                break
    index2.append(sum_value / len(stocks2))
    sum_value = 0
    for stock in stocks3:
        for jj in STK_data:
            if jj['SYMBOL'] == stock:
                sum_value += jj['CLOSEPRICE']
                break
    index3.append(sum_value / len(stocks3))
    sum_value = 0
    for stock in stocks4:
        for jj in STK_data:
            if jj['SYMBOL'] == stock:
                sum_value += jj['CLOSEPRICE']
                break
    index4.append(sum_value / len(stocks4))
    n += 1
    run_time_now = datetime.datetime.now()
    run_time = run_time_now - start_time
    print(datetime.datetime.now(), "###", n, "###", run_time,  "###", date)


index = {'DATE': Calendar_Date, 'zhouqi': index1, 'chengzhang': index2, 'xiaofei': index3, 'jinrong': index4}
pickle.dump(index, open('4style_index.pkl', 'wb'))



