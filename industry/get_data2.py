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


# 取预期净利润 NPF
start_date = datetime.datetime.strptime("2017-06-01", "%Y-%m-%d")
end_date = datetime.datetime.strptime("2018-06-01", "%Y-%m-%d")
factor = "QF_NetProfitF"
cmd = ' '.join((
    "SELECT B.TRADINGDATE AS TRADINGDATE,A.INDUSTRY as INDUSTRY, AVG(B.FACTORVALUE) AS VALUE_AVG ",
    "FROM (select distinct SYMBOL, FACTORVALUE as INDUSTRY from dbo.QF_IndustryCode12)as A",
    "inner join dbo.{} as B ON A.SYMBOL = B.SYMBOL".format(factor),
    "WHERE B.TRADINGDATE>='{:s}' and".format(str(start_date)),
    "B.TRADINGDATE<='{:s}'".format(str(end_date)),
    "GROUP BY B.TRADINGDATE,A.INDUSTRY",
    "order by TRADINGDATE asc,INDUSTRY asc"
))
NPFData2 = query(cmd, "GTA_QF_V3", True)


# 得到每月的最后一个交易日数据
def get_last(list_data):
    month_end = []
    month_end_data = []
    for n in range(len(list_data) - 1):
        pre = datetime.datetime.strptime(list_data[n]['TRADINGDATE'], "%Y-%m-%d %H:%M:%S.%f0").month
        cur = datetime.datetime.strptime(list_data[n + 1]['TRADINGDATE'], "%Y-%m-%d %H:%M:%S.%f0").month
        if pre != cur:
            month_end.append(list_data[n]['TRADINGDATE'])
    for n in range(len(list_data)):
        if list_data[n]['TRADINGDATE'] in month_end:
            month_end_data.append(list_data[n])
    return month_end_data


# 得到每月的最后一个交易日数据
NPFData = get_last(NPFData2)  # 10-18


# 取净利润 NP 1年前
start_date = datetime.datetime.strptime("2009-06-01", "%Y-%m-%d")
end_date = datetime.datetime.strptime("2017-06-01", "%Y-%m-%d")
factor = "QF_NetProfit"
cmd = ' '.join((
    "SELECT B.TRADINGDATE AS TRADINGDATE,A.INDUSTRY as INDUSTRY, AVG(B.FACTORVALUE) AS VALUE_AVG ",
    "FROM (select distinct SYMBOL, FACTORVALUE as INDUSTRY from dbo.QF_IndustryCode12)as A",
    "inner join dbo.{} as B ON A.SYMBOL = B.SYMBOL".format(factor),
    "WHERE B.TRADINGDATE>='{:s}' and".format(str(start_date)),
    "B.TRADINGDATE<='{:s}'".format(str(end_date)),
    "GROUP BY B.TRADINGDATE,A.INDUSTRY",
    "order by TRADINGDATE asc,INDUSTRY asc"
))
NPData1 = query(cmd, "GTA_QF_V3", True)


# 得到每月的最后一个交易日数据
NPData1 = get_last(NPData1)  # 09-17


# 计算NPF同比增速因子 strategy2
NPF_Growth = []
flag = False
j = 0  # NP1
for item in NPFData:  # 清洗数据
    # NP/NPF匹配
    while True:
        NP_date = datetime.datetime.strptime(NPData1[j]['TRADINGDATE'], "%Y-%m-%d %H:%M:%S.%f0")
        item_date = datetime.datetime.strptime(item['TRADINGDATE'], "%Y-%m-%d %H:%M:%S.%f0")
        if NP_date.year < item_date.year-1 or NP_date.month < item_date.month:
            j = j + 1
        elif NP_date.year == item_date.year-1 and NP_date.month == item_date.month:
            flag = False
            break
        else:
            flag = ~flag
            break
    while ~flag:
        if NPData1[j]['INDUSTRY'] < item['INDUSTRY']:
            j = j + 1
        elif NPData1[j]['INDUSTRY'] == item['INDUSTRY']:
            flag = False
            break
        else:
            flag = ~flag
    # 若匹配成功则进行计算
    if ~flag:
        NPF_G = (item['VALUE_AVG']-NPData1[j]['VALUE_AVG'])/abs(NPData1[j]['VALUE_AVG'])
        NPF_G = NPF_G.quantize(Decimal('0.000000'))
        NPF_Growth.append({'TRADINGDATE': item['TRADINGDATE'], 'INDUSTRY': item['INDUSTRY'], 'NPF_Growth': NPF_G, })


# 得到结果
pickle.dump(NPF_Growth, open('NPF_Growth.pkl', 'wb'))

