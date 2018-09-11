# 获取2010/08到2018/06每月月末对应的各行业成分股

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


# 获取月末日期 16/01~18/06
start_date = datetime.datetime.strptime("2016-01-01", "%Y-%m-%d")
end_date = datetime.datetime.strptime("2018-07-31", "%Y-%m-%d")
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


# 得到每年1月和7月第一个交易日日期10/07-18/01
start_date = datetime.datetime.strptime("2015-12-01", "%Y-%m-%d")
end_date = datetime.datetime.strptime("2018-06-01", "%Y-%m-%d")
factor = "STK_CALENDARD"
cmd = ' '.join((
    "select distinct CALENDARDATE",
    "FROM dbo.{}".format(factor),
    "where CALENDARDATE>='{:s}' and".format(str(start_date)),
    "CALENDARDATE<='{:s}' and".format(str(end_date)),
    "ISOPEN='y' and (EXCHANGECODE = 'SSE' or EXCHANGECODE = 'SZSE')"
    "order by CALENDARDATE asc"
))
Calender = query(cmd, "GTA_QIA_QDB", True)
change_date = []
for n in range(len(Calender) - 1):
    pre = Calender[n]['CALENDARDATE'].month
    cur = Calender[n + 1]['CALENDARDATE'].month
    if (pre == 12 and cur == 1) or (pre == 6 and cur == 7):
        change_date.append(Calender[n+1]['CALENDARDATE'])


constituent_stock = pickle.load(open(r'constituent_stock.pkl', 'rb'))


# 按月和行业分别计算AR


i = -1
ind_stock_num = []
for now_time in month_end:
    if now_time.month == 1 or now_time.month == 7:
        i += 1
    now_constituent_stock = constituent_stock[change_date[i]]
    ind_AR = []
    print(now_time)
    for ind in now_constituent_stock.keys():
        ind_stocks = now_constituent_stock[ind]
        stock_price_list = {}
        cnt = 0
        for stock in ind_stocks:
            cnt += 1
        print(ind, cnt)
        ind_stock_num.append({"INDUSTRY": ind, "NUM": cnt})


