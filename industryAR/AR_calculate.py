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
n = 0
run_time = 0
start_time = datetime.datetime.now()
print("start time", start_time)
AR_value = {}
i = 0
for now_time in month_end:
    if now_time.month == 1 or now_time.month == 7:
        i += 1
    now_constituent_stock = constituent_stock[change_date[i]]
    ind_AR = []
    for ind in now_constituent_stock.keys():
        ind_stocks = now_constituent_stock[ind]
        stock_price_list = {}
        for stock in ind_stocks:
            start_date = now_time - datetime.timedelta(100)

            end_date = now_time
            factor = "STK_MKT_QUOTATION"
            cmd = ' '.join((
                "select SYMBOL,PRECLOSEPRICE,CLOSEPRICE",
                "FROM dbo.{}".format(factor),
                "where TRADINGDATE>='{:s}' and".format(str(start_date)),
                "TRADINGDATE<='{:s}' and".format(str(end_date)),
                "symbol = {}".format(stock['SYMBOL']),
            ))
            data_for_100 = query(cmd, "GTA_QIA_QDB", True)
            temp = []
            for item in data_for_100:
                earning = (item['CLOSEPRICE']-item['PRECLOSEPRICE'])/item['PRECLOSEPRICE']
                temp.append(earning)
            stock_price_list[stock['SYMBOL']] = pd.Series(temp)
        # 计算AR
        df = pd.DataFrame(stock_price_list)
        dataMat = np.mat(df)
        meanVal = np.mean(dataMat, axis=0)  # 按列求均值，即求各个特征的均值
        newData = dataMat - meanVal
        covMat = np.cov(newData.astype(float))  # 求协方差矩阵
        eigVals, eigVects = np.linalg.eig(np.mat(covMat))  # 求特征值和特征向量,特征向量是按列放的，即一列代表一个特征向量
        eigValIndice = np.argsort(eigVals)  # 对特征值从小到大排序
        contribute_ratio = (eigVals[eigValIndice[-1]] + eigVals[eigValIndice[-2]]) / sum(eigVals)  # 特征根的大小就是方差贡献的大小
        ind_AR.append({"INDUSTRY": ind, "AR": contribute_ratio})
        n += 1
        run_time_now = datetime.datetime.now()
        run_time = run_time_now - start_time
        print(datetime.datetime.now(), "###", n, "###", run_time, "###", now_time.year, now_time.month, "###", ind, "###", contribute_ratio)
    AR_value[now_time] = ind_AR


# 得到结果
pickle.dump(AR_value, open('AR_value.pkl', 'wb'))



