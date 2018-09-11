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


# 获取全行业所有股票symbol
start_date = datetime.datetime.strptime("2010-06-01", "%Y-%m-%d")
end_date = datetime.datetime.strptime("2018-06-01", "%Y-%m-%d")
cmd = ' '.join((
    "SELECT TRADINGDATE, FACTORVALUE as INDUSTRY, SYMBOL ",
    "from dbo.QF_IndustryCode12",
    "WHERE TRADINGDATE>='{:s}' and".format(str(start_date)),
    "TRADINGDATE<='{:s}'".format(str(end_date)),
    "order by TRADINGDATE asc"
))
stocks = query(cmd, "GTA_QF_V3", True)


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
stocks = get_last(stocks)  # 10-18
stocks_dict = {}
pre_date = stocks[0]['TRADINGDATE']
temp_list = []
for stock in stocks:
    if stock['TRADINGDATE'] == pre_date:
        temp_list.append(stock)
    else:
        j = pre_date[:-17]
        stocks_dict[j] = temp_list
        temp_list = []
        pre_date = stock['TRADINGDATE']
        temp_list.append(stock)
j = pre_date[:-17]
stocks_dict[j] = temp_list


pickle.dump(stocks_dict, open('Stocks_dict.pkl', 'wb'))
