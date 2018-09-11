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
    "order by TRADINGDATE,SYMBOL"
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


# 获取流通股本信息
factor = "[STK_SHARES_STRUCTURE]"
cmd = ' '.join((
    "select [SYMBOL],[CHANGEDATE],[TRADESHARESTOTAL]",
    "FROM dbo.{}".format(factor),
    "order by SYMBOL,CHANGEDATE"
))
cir_stock = query(cmd, "GTA_QIA_QDB", True)


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
    ind_code1 = '00'
    for row in csv_reader:
        if ind_code == row[1]:
            ind_code1 = row[0][0:2]
    s1 = datetime.datetime.strptime("2004-12-31", "%Y-%m-%d")
    f1 = "[STK_MKT_QUOTATIONMONTH]"
    c1 = ' '.join((
        "select [SYMBOL],[CLOSEDATE],[CLOSEPRICE],[MARKETVALUE]",
        "FROM dbo.{}".format(f1),
        "where CLOSEDATE='{:s}' and".format(str(s1)),
        "SYMBOL in (SELECT [SYMBOL] FROM [GTA_QIA_QDB].[dbo].[STK_INDUSTRYCLASS] where "
        "INDUSTRYCLASSIFICATIONID = 'P0211' and INDUSTRYCODE like '{}%')".format(ind_code1),
        "order by CLOSEDATE asc"
    ))
    symbol_data = query(c1, "GTA_QIA_QDB", True)
    month_end_data = []
    for items in symbol_data:
        if items['CLOSEDATE'] == get_time:
            month_end_data.append(items)
    leading_stock = sort_stock(month_end_data)
    return leading_stock


#  制定风格基期指数（龙头股指数均值）
now_time = datetime.datetime(year=2004, month=12, day=31, hour=0, minute=0, second=0, microsecond=0)
core1 = ['801040', '801020', '801030', '801050']  # 周期
core2 = ['801120', '801110', '801150']  # 消费
core3 = ['801750', '801080', '801770', '801760']  # 成长
core4 = ['801780', '801790']  # 金融
# 获取龙头股
stocks1 = []
for ind in core1:
    stocks1 += get_leading_stock(now_time, ind)
stocks2 = []
for ind in core2:
    stocks2 += get_leading_stock(now_time, ind)
stocks3 = []
for ind in core3:
    stocks3 += get_leading_stock(now_time, ind)
stocks4 = []
for ind in core4:
    stocks4 += get_leading_stock(now_time, ind)
sum_value = 0
for stock in stocks1:
    for jj in STK_data1:
        if jj['SYMBOL'] == stock:
            for kk in range(len(cir_stock)):
                if cir_stock[kk]['SYMBOL'] == stock and ((cir_stock[kk]['CHANGEDATE'] <= now_time < cir_stock[kk+1]['CHANGEDATE'] and cir_stock[kk+1]['SYMBOL'] == stock)or(cir_stock[kk]['CHANGEDATE'] <= now_time and cir_stock[kk+1]['SYMBOL'] != stock)):
                    sum_value += jj['CLOSEPRICE'] * cir_stock[kk]['TRADESHARESTOTAL'] * Decimal.from_float(0.15)
                    break
            break
base_index1 = sum_value
sum_value = 0
for stock in stocks2:
    for jj in STK_data1:
        if jj['SYMBOL'] == stock:
            for kk in range(len(cir_stock)):
                if cir_stock[kk]['SYMBOL'] == stock and ((cir_stock[kk]['CHANGEDATE'] <= now_time < cir_stock[kk+1]['CHANGEDATE'] and cir_stock[kk+1]['SYMBOL'] == stock)or(cir_stock[kk]['CHANGEDATE'] <= now_time and cir_stock[kk+1]['SYMBOL'] != stock)):
                    sum_value += jj['CLOSEPRICE'] * cir_stock[kk]['TRADESHARESTOTAL'] * Decimal.from_float(0.15)
                    break
            break
base_index2 = sum_value
sum_value = 0
for stock in stocks3:
    for jj in STK_data1:
        if jj['SYMBOL'] == stock:
            for kk in range(len(cir_stock)):
                if cir_stock[kk]['SYMBOL'] == stock and ((cir_stock[kk]['CHANGEDATE'] <= now_time < cir_stock[kk+1]['CHANGEDATE'] and cir_stock[kk+1]['SYMBOL'] == stock)or(cir_stock[kk]['CHANGEDATE'] <= now_time and cir_stock[kk+1]['SYMBOL'] != stock)):
                    sum_value += jj['CLOSEPRICE'] * cir_stock[kk]['TRADESHARESTOTAL'] * Decimal.from_float(0.15)
                    break
            break
base_index3 = sum_value
sum_value = 0
for stock in stocks4:
    for jj in STK_data1:
        if jj['SYMBOL'] == stock:
            for kk in range(len(cir_stock)):
                if cir_stock[kk]['SYMBOL'] == stock and ((cir_stock[kk]['CHANGEDATE'] <= now_time < cir_stock[kk+1]['CHANGEDATE'] and cir_stock[kk+1]['SYMBOL'] == stock)or(cir_stock[kk]['CHANGEDATE'] <= now_time and cir_stock[kk+1]['SYMBOL'] != stock)):
                    sum_value += jj['CLOSEPRICE'] * cir_stock[kk]['TRADESHARESTOTAL'] * Decimal.from_float(0.15)
                    break
            break
base_index4 = sum_value


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
                for kk in range(len(cir_stock)):
                    if cir_stock[kk]['SYMBOL'] == stock and ((cir_stock[kk]['CHANGEDATE'] <= date < cir_stock[kk+1]['CHANGEDATE'] and cir_stock[kk+1]['SYMBOL'] == stock)or(cir_stock[kk]['CHANGEDATE'] <= date and cir_stock[kk+1]['SYMBOL'] != stock)):
                        sum_value += jj['CLOSEPRICE'] * cir_stock[kk]['TRADESHARESTOTAL'] * Decimal.from_float(0.15)
                        break
                break
    index1.append(sum_value / base_index1 * 1000)
    sum_value = 0
    for stock in stocks2:
        for jj in STK_data:
            if jj['SYMBOL'] == stock:
                for kk in range(len(cir_stock)):
                    if cir_stock[kk]['SYMBOL'] == stock and ((cir_stock[kk]['CHANGEDATE'] <= date < cir_stock[kk+1]['CHANGEDATE'] and cir_stock[kk+1]['SYMBOL'] == stock)or(cir_stock[kk]['CHANGEDATE'] <= date and cir_stock[kk+1]['SYMBOL'] != stock)):
                        sum_value += jj['CLOSEPRICE'] * cir_stock[kk]['TRADESHARESTOTAL'] * Decimal.from_float(0.15)
                        break
                break
    index2.append(sum_value / base_index2 * 1000)
    sum_value = 0
    for stock in stocks3:
        for jj in STK_data:
            if jj['SYMBOL'] == stock:
                for kk in range(len(cir_stock)):
                    if cir_stock[kk]['SYMBOL'] == stock and ((cir_stock[kk]['CHANGEDATE'] <= date < cir_stock[kk+1]['CHANGEDATE'] and cir_stock[kk+1]['SYMBOL'] == stock)or(cir_stock[kk]['CHANGEDATE'] <= date and cir_stock[kk+1]['SYMBOL'] != stock)):
                        sum_value += jj['CLOSEPRICE'] * cir_stock[kk]['TRADESHARESTOTAL'] * Decimal.from_float(0.15)
                        break
                break
    index3.append(sum_value / base_index3 * 1000)
    sum_value = 0
    for stock in stocks4:
        for jj in STK_data:
            if jj['SYMBOL'] == stock:
                for kk in range(len(cir_stock)):
                    if cir_stock[kk]['SYMBOL'] == stock and ((cir_stock[kk]['CHANGEDATE'] <= date < cir_stock[kk+1]['CHANGEDATE'] and cir_stock[kk+1]['SYMBOL'] == stock)or(cir_stock[kk]['CHANGEDATE'] <= date and cir_stock[kk+1]['SYMBOL'] != stock)):
                        sum_value += jj['CLOSEPRICE'] * cir_stock[kk]['TRADESHARESTOTAL'] * Decimal.from_float(0.15)
                        break
                break
    index4.append(sum_value / base_index4 * 1000)
    n += 1
    run_time_now = datetime.datetime.now()
    run_time = run_time_now - start_time
    print(datetime.datetime.now(), "###", n, "###", run_time,  "###", date)


index = {'DATE': Calendar_Date, 'zhouqi': index1, 'chengzhang': index2, 'xiaofei': index3, 'jinrong': index4}
pickle.dump(index, open('4style_index.pkl', 'wb'))



