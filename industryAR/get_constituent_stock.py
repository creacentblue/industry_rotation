# 获取2010/08到2018/06每月月末对应的各行业成分股
# 成分股每年1月和7月进行调整

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


# A股(去掉ST,*ST,PT)
factor = "STK_STOCKINFO"
cmd = ' '.join((
    "select Symbol, ListedDate, DelistedDate",
    "FROM dbo.{}".format(factor),
    "WHERE ShareType='A' and",
    "NOT (SHORTNAME  like 'ST%' or SHORTNAME like '*ST%' or SHORTNAME like 'PT%')"
))
AStock = query(cmd, "GTA_QIA_QDB", True)


# 得到每年1月和7月第一个交易日日期10/07-18/01
start_date = datetime.datetime.strptime("2010-06-01", "%Y-%m-%d")
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


# 为计算日均成交金额和日均市值而获取的每年6月末和12月末交易日日期09/06-17-12
start_date = datetime.datetime.strptime("2009-06-01", "%Y-%m-%d")
end_date = datetime.datetime.strptime("2018-01-31", "%Y-%m-%d")
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
    if (pre == 12 and cur == 1) or (pre == 6 and cur == 7):
        month_end.append(Count_Date[n]['CALENDARDATE'])


# 每年1月和7月更新成分股
constituent_stock = {}
for i in range(len(change_date)):
    now_time = change_date[i]
    sample = []  # 样本空间
    # 上市时间满3个月的A股
    for item in AStock:
        list_time = item['ListedDate']
        de_list_time = item['DelistedDate']
        if list_time is not None:
            if de_list_time is None:
                if now_time - list_time >= datetime.timedelta(91):
                    sample.append(item['Symbol'])
            else:
                if now_time-list_time >= datetime.timedelta(91) and now_time < de_list_time:
                    sample.append(item['Symbol'])
    # 将样本空间按日均成交金额排序并选出各行业日均总市值前100名
    if now_time.month == 7:
        date1 = month_end[i]
        date2 = month_end[i+1]
        date3 = month_end[i+2]
        factor = "STK_MKT_QUOTATIONYEAR"
        cmd = ' '.join((
            "select SYMBOL,TRADINGDAYS,AMOUNT",
            "FROM dbo.{}".format(factor),
            "where CLOSEDATE='{:s}' or".format(str(date1)),
            "CLOSEDATE='{:s}' or".format(str(date2)),
            "CLOSEDATE='{:s}'".format(str(date3)),
            "order by SYMBOL,CLOSEDATE"
        ))
        Count_Data1 = query(cmd, "GTA_QIA_QDB", True)
        # 取样本内股票的数据
        temp = []
        for data in Count_Data1:
            if data['SYMBOL'] in sample:
                temp.append(data)
        Count_Data1 = temp
        # 计算日均成交金额
        avg_list = []
        temp = []
        pre = Count_Data1[0]['SYMBOL']
        cnt = 0
        for data in Count_Data1:
            if data['SYMBOL'] == pre:
                cnt += 1
                temp.append(data)
            else:
                if cnt == 1:
                    avg_ = temp[-1]['AMOUNT']/temp[-1]['TRADINGDAYS']
                    avg_list.append({"SYMBOL": pre, "AVG_AMOUNT": avg_})
                elif cnt == 2:
                    avg_ = (temp[-2]['AMOUNT']+temp[-1]['AMOUNT'])/(temp[-2]['TRADINGDAYS']+temp[-1]['TRADINGDAYS'])
                    avg_list.append({"SYMBOL": pre, "AVG_AMOUNT": avg_})
                else:
                    if - temp[-3]['TRADINGDAYS'] + temp[-2]['TRADINGDAYS'] + temp[-1]['TRADINGDAYS'] != 0:
                        avg_ = (-temp[-3]['AMOUNT'] + temp[-2]['AMOUNT'] + temp[-1]['AMOUNT']) / (
                                    - temp[-3]['TRADINGDAYS']
                                    + temp[-2]['TRADINGDAYS']
                                    + temp[-1]['TRADINGDAYS'])
                        avg_list.append({"SYMBOL": pre, "AVG_AMOUNT": avg_})
                cnt = 1
                pre = data['SYMBOL']
                temp.append(data)
        if cnt == 1:
            avg_ = temp[-1]['AMOUNT'] / temp[-1]['TRADINGDAYS']
            avg_list.append({"SYMBOL": pre, "AVG_AMOUNT": avg_})
        elif cnt == 2:
            avg_ = (temp[-2]['AMOUNT'] + temp[-1]['AMOUNT']) / (temp[-2]['TRADINGDAYS'] + temp[-1]['TRADINGDAYS'])
            avg_list.append({"SYMBOL": pre, "AVG_AMOUNT": avg_})
        else:
            if - temp[-3]['TRADINGDAYS'] + temp[-2]['TRADINGDAYS'] + temp[-1]['TRADINGDAYS'] != 0:
                avg_ = (-temp[-3]['AMOUNT'] + temp[-2]['AMOUNT'] + temp[-1]['AMOUNT']) / (
                        - temp[-3]['TRADINGDAYS']
                        + temp[-2]['TRADINGDAYS']
                        + temp[-1]['TRADINGDAYS'])
                avg_list.append({"SYMBOL": pre, "AVG_AMOUNT": avg_})

        # 排序选出前80%
        avg_list = sorted(avg_list, key=lambda e: e.__getitem__('AVG_AMOUNT'), reverse=True)  # 降序
        num = math.floor(0.8*len(avg_list))
        sorted_list = avg_list[:num]
        temp = []
        for data in sorted_list:
            temp.append(data['SYMBOL'])
        sorted_list = temp
        # 按日均总市值排序
        start_date = month_end[i]
        end_date = month_end[i+2]
        factor = "STK_MKT_QUOTATIONYEAR"
        cmd = ' '.join((
            "select SYMBOL,avg(MARKETVALUE)as AVG_VALUE",
            "FROM dbo.{}".format(factor),
            "where CLOSEDATE>'{:s}' and".format(str(start_date)),
            "CLOSEDATE<='{:s}' ".format(str(end_date)),
            "group by SYMBOL",
            "order by AVG_VALUE desc"
        ))
        Count_Data1 = query(cmd, "GTA_QIA_QDB", True)
        # 取样本内股票的数据
        temp = []
        for data in Count_Data1:
            if data['SYMBOL'] in sorted_list:
                temp.append(data)
        Count_Data1 = temp
        # 按申万行业划分
        csv_reader = csv.reader(open('symbol.csv'))
        temp = []
        for row in csv_reader:
            for list_ in Count_Data1:
                if int(list_['SYMBOL']) == int(row[0]):
                    list_['INDUSTRY'] = row[1]
                    temp.append(list_)
        Count_Data1 = temp
        # 获取各行业排名前100名的股票
        sort_list = sorted(Count_Data1, key=lambda e: e.__getitem__('INDUSTRY'))
        pre = sort_list[0]['INDUSTRY']
        cnt = 0
        temp = []
        data_for_ind = {}
        for data in sort_list:
            if data['INDUSTRY'] == pre:
                if cnt < 100:
                    cnt += 1
                    temp.append(data)
                else:
                    cnt += 1
            else:
                data_for_ind[pre] = temp
                temp = []
                pre = data['INDUSTRY']
                cnt = 1
                temp.append(data)
        data_for_ind[pre] = temp
        constituent_stock[change_date[i]] = data_for_ind
    else:
        date1 = month_end[i+2]
        factor = "STK_MKT_QUOTATIONYEAR"
        cmd = ' '.join((
            "select SYMBOL,TRADINGDAYS,AMOUNT",
            "FROM dbo.{}".format(factor),
            "where CLOSEDATE='{:s}'".format(str(date1)),
            "order by SYMBOL,CLOSEDATE"
        ))
        Count_Data2 = query(cmd, "GTA_QIA_QDB", True)
        # 取样本内股票的数据
        temp = []
        for data in Count_Data2:
            if data['SYMBOL'] in sample:
                temp.append(data)
        Count_Data2 = temp
        # 计算日均成交金额
        avg_list = []
        for data in Count_Data2:
            if data['TRADINGDAYS'] != 0:
                avg_ = data['AMOUNT']/data['TRADINGDAYS']
                avg_list.append({"SYMBOL": data['SYMBOL'], "AVG_AMOUNT": avg_})
        # 排序选出前80%
        avg_list = sorted(avg_list, key=lambda e: e.__getitem__('AVG_AMOUNT'), reverse=True)  # 降序
        num = math.floor(0.8*len(avg_list))
        sorted_list = avg_list[:num]
        temp = []
        for data in sorted_list:
            temp.append(data['SYMBOL'])
        sorted_list = temp
        # 按日均总市值排序
        start_date = month_end[i]
        end_date = month_end[i+2]
        factor = "STK_MKT_QUOTATIONYEAR"
        cmd = ' '.join((
            "select SYMBOL,avg(MARKETVALUE)as AVG_VALUE",
            "FROM dbo.{}".format(factor),
            "where CLOSEDATE>'{:s}' and".format(str(start_date)),
            "CLOSEDATE<='{:s}'".format(str(end_date)),
            "group by SYMBOL",
            "order by AVG_VALUE desc"
        ))
        Count_Data2 = query(cmd, "GTA_QIA_QDB", True)
        # 取样本内股票的数据
        temp = []
        for data in Count_Data2:
            if data['SYMBOL'] in sorted_list:
                temp.append(data)
        Count_Data2 = temp
        # 按申万行业划分
        csv_reader = csv.reader(open('symbol.csv'))
        temp = []
        for row in csv_reader:
            for list_ in Count_Data2:
                if int(list_['SYMBOL']) == int(row[0]):
                    list_['INDUSTRY'] = row[1]
                    temp.append(list_)
        Count_Data2 = temp
        # 获取各行业排名前100名的股票
        sort_list = sorted(Count_Data2, key=lambda e: e.__getitem__('INDUSTRY'))
        pre = sort_list[0]['INDUSTRY']
        cnt = 0
        temp = []
        data_for_ind = {}
        for data in sort_list:
            if data['INDUSTRY'] == pre:
                if cnt < 100:
                    cnt += 1
                    temp.append(data)
                else:
                    cnt += 1
            else:
                data_for_ind[pre] = temp
                temp = []
                pre = data['INDUSTRY']
                cnt = 1
                temp.append(data)
        data_for_ind[pre] = temp
        constituent_stock[change_date[i]] = data_for_ind


# 得到结果
pickle.dump(constituent_stock, open('constituent_stock.pkl', 'wb'))

