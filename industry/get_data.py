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
start_date = datetime.datetime.strptime("2010-06-01", "%Y-%m-%d")
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


# 取净利润 NP
start_date = datetime.datetime.strptime("2008-06-01", "%Y-%m-%d")
end_date = datetime.datetime.strptime("2016-06-01", "%Y-%m-%d")
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
NPData = query(cmd, "GTA_QF_V3", True)


# 取预期市盈率 PEF
start_date = datetime.datetime.strptime("2010-06-01", "%Y-%m-%d")
end_date = datetime.datetime.strptime("2018-06-01", "%Y-%m-%d")
factor = "QF_PEF"
cmd = ' '.join((
    "SELECT B.TRADINGDATE AS TRADINGDATE,A.INDUSTRY as INDUSTRY, AVG(B.FACTORVALUE) AS VALUE_AVG ",
    "FROM (select distinct SYMBOL, FACTORVALUE as INDUSTRY from dbo.QF_IndustryCode12)as A",
    "inner join dbo.{} as B ON A.SYMBOL = B.SYMBOL".format(factor),
    "WHERE B.TRADINGDATE>='{:s}' and".format(str(start_date)),
    "B.TRADINGDATE<='{:s}'".format(str(end_date)),
    "GROUP BY B.TRADINGDATE,A.INDUSTRY",
    "order by TRADINGDATE asc,INDUSTRY asc"
))
PEFData = query(cmd, "GTA_QF_V3", True)


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
NPData = get_last(NPData)  # 08-16
NPFData = get_last(NPFData2)  # 10-18
PEFData = get_last(PEFData)  # 10-18


# 计算PE/G因子值 strategy1
PEGData = []
flag = False  # 为真时意味着过慢需要更新item，用来直接跳过后续匹配
j = 0  # NPF
k = 0  # TA
for item in PEFData:  # 清洗数据
    # PEF/NPF匹配
    while True:  # 日期匹配
        if NPFData[j]['TRADINGDATE'] < item['TRADINGDATE']:  # 清洗NPF多余值
            j = j + 1
        elif NPFData[j]['TRADINGDATE'] == item['TRADINGDATE']:  # 命中
            flag = False
            break
        else:  # 清洗PEF多余值
            flag = ~flag
            break
    while ~flag:  # 行业匹配
        if NPFData[j]['INDUSTRY'] < item['INDUSTRY']:
            j = j + 1
        elif NPFData[j]['INDUSTRY'] == item['INDUSTRY']:
            flag = False
            break
        else:
            flag = ~flag
    # PEF/NP匹配
    while ~flag:
        NP_date = datetime.datetime.strptime(NPData[k]['TRADINGDATE'], "%Y-%m-%d %H:%M:%S.%f0")
        item_date = datetime.datetime.strptime(item['TRADINGDATE'], "%Y-%m-%d %H:%M:%S.%f0")
        if NP_date.year < item_date.year-2 or NP_date.month < item_date.month:
            k = k + 1
        elif NP_date.year == item_date.year-2 and NP_date.month == item_date.month:
            flag = False
            break
        else:
            flag = ~flag
    while ~flag:
        if NPData[k]['INDUSTRY'] < item['INDUSTRY']:
            k = k + 1
        elif NPData[k]['INDUSTRY'] == item['INDUSTRY']:
            flag = False
            break
        else:
            flag = ~flag
    # 若匹配成功则进行计算
    if ~flag:
        if (NPFData[j]['VALUE_AVG']/NPData[k]['VALUE_AVG']) >= 0:
            PEG = item['VALUE_AVG']/Decimal.from_float(
                ((math.sqrt(NPFData[j]['VALUE_AVG']/NPData[k]['VALUE_AVG'])-1)*100))
        else:
            PEG = item['VALUE_AVG'] / Decimal.from_float(
                ((-math.sqrt(-NPFData[j]['VALUE_AVG'] / NPData[k]['VALUE_AVG']) - 1) * 100))
        PEG = PEG.quantize(Decimal('0.000000'))
        PEGData.append({'TRADINGDATE': item['TRADINGDATE'], 'INDUSTRY': item['INDUSTRY'], 'PEG_VALUE': PEG, })


# 得到结果
pickle.dump(PEGData, open('PEGData.pkl', 'wb'))


