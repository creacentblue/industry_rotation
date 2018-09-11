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
start_date = datetime.datetime.strptime("2010-08-01", "%Y-%m-%d")
end_date = datetime.datetime.strptime("2018-06-01", "%Y-%m-%d")
factor = "QF_NetProfitF"
cmd = ' '.join((
    "select TRADINGDATE, SYMBOL, FACTORVALUE",
    "FROM dbo.{}".format(factor),
    "where TRADINGDATE>='{:s}' and".format(str(start_date)),
    "TRADINGDATE<='{:s}'".format(str(end_date)),
    "order by TRADINGDATE asc,SYMBOL asc"
))
NPFData = query(cmd, "GTA_QF_V3", True)


# 取净利润 NP
start_date = datetime.datetime.strptime("2008-08-01", "%Y-%m-%d")
end_date = datetime.datetime.strptime("2016-06-01", "%Y-%m-%d")
factor = "QF_NetProfit"
cmd = ' '.join((
    "select TRADINGDATE, SYMBOL, FACTORVALUE",
    "FROM dbo.{}".format(factor),
    "where TRADINGDATE>='{:s}' and".format(str(start_date)),
    "TRADINGDATE<='{:s}'".format(str(end_date)),
    "order by TRADINGDATE asc,SYMBOL asc"
))
NPData = query(cmd, "GTA_QF_V3", True)


# 取预期市盈率 PEF
start_date = datetime.datetime.strptime("2010-08-01", "%Y-%m-%d")
end_date = datetime.datetime.strptime("2018-06-01", "%Y-%m-%d")
factor = "QF_PEF"
cmd = ' '.join((
    "select TRADINGDATE, SYMBOL, FACTORVALUE",
    "FROM dbo.{}".format(factor),
    "where TRADINGDATE>='{:s}' and".format(str(start_date)),
    "TRADINGDATE<='{:s}'".format(str(end_date)),
    "order by TRADINGDATE asc,SYMBOL asc"
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


# 得到每月的最后一个交易日数据(list)
NPData = get_last(NPData)  # 14-16
NPFData = get_last(NPFData)  # 16-18
PEFData = get_last(PEFData)  # 16-18


# 按申万行业划分
csv_reader = csv.reader(open('symbol.csv'))
for row in csv_reader:
    for list_ in NPData:
        if int(list_['SYMBOL']) == int(row[0]):
            list_['INDUSTRY'] = row[1]
    for list_ in NPFData:
        if int(list_['SYMBOL']) == int(row[0]):
            list_['INDUSTRY'] = row[1]
    for list_ in PEFData:
        if int(list_['SYMBOL']) == int(row[0]):
            list_['INDUSTRY'] = row[1]
_NPData = []
_NPFData = []
_PEFData = []
for dict_ in NPData:
    if 'INDUSTRY' in dict_.keys():
        _NPData.append(dict_)
for dict_ in NPFData:
    if 'INDUSTRY' in dict_.keys():
        _NPFData.append(dict_)
for dict_ in PEFData:
    if 'INDUSTRY' in dict_.keys():
        _PEFData.append(dict_)


# 聚集函数,按行业聚集
def sort_and_avg(data):
    sort_list = sorted(data, key=lambda e: e.__getitem__('INDUSTRY'))
    pre_ind = sort_list[0]['INDUSTRY']
    sum_value = 0
    count = 0
    x = 0
    result = []
    while True:
        if x > len(sort_list)-1:
            break
        if sort_list[x]['INDUSTRY'] == pre_ind:
            sum_value += sort_list[x]['FACTORVALUE']
            count += 1
            x += 1
        else:
            avg_value = sum_value/count
            result.append({'TRADINGDATE': sort_list[x]['TRADINGDATE'], 'INDUSTRY': pre_ind, 'AVG_VALUE': avg_value})
            pre_ind = sort_list[x]['INDUSTRY']
            sum_value = 0
            count = 0
    avg_value = sum_value/count
    result.append({'TRADINGDATE': sort_list[x-1]['TRADINGDATE'], 'INDUSTRY': pre_ind, 'AVG_VALUE': avg_value})
    return result


# 求出平均值
pre_date = _NPData[0]['TRADINGDATE']
i = 0
temp_list = []
NPData_ = []
while True:
    if i > len(_NPData)-1:
        break
    if _NPData[i]['TRADINGDATE'] == pre_date:
        temp_list.append(_NPData[i])
        i += 1
    else:
        pre_date = _NPData[i]['TRADINGDATE']
        NPData_ += sort_and_avg(temp_list)
        temp_list = []
NPData_ += sort_and_avg(temp_list)
# 求出平均值
pre_date = _NPFData[0]['TRADINGDATE']
i = 0
temp_list = []
NPFData_ = []
while True:
    if i > len(_NPFData)-1:
        break
    if _NPFData[i]['TRADINGDATE'] == pre_date:
        temp_list.append(_NPFData[i])
        i += 1
    else:
        pre_date = _NPFData[i]['TRADINGDATE']
        NPFData_ += sort_and_avg(temp_list)
        temp_list = []
NPFData_ += sort_and_avg(temp_list)
# 求出平均值
pre_date = _PEFData[0]['TRADINGDATE']
i = 0
temp_list = []
PEFData_ = []
while True:
    if i > len(_PEFData)-1:
        break
    if _PEFData[i]['TRADINGDATE'] == pre_date:
        temp_list.append(_PEFData[i])
        i += 1
    else:
        pre_date = _PEFData[i]['TRADINGDATE']
        PEFData_ += sort_and_avg(temp_list)
        temp_list = []
PEFData_ += sort_and_avg(temp_list)


# 计算PE/G因子值 strategy1
PEGData = []
flag = False  # 为真时意味着过慢需要更新item，用来直接跳过后续匹配
j = 0  # NPF
k = 0  # NP
for item in PEFData_:  # 清洗数据
    # PEF/NPF匹配
    while True:  # 日期匹配
        if NPFData_[j]['TRADINGDATE'] < item['TRADINGDATE']:  # 清洗NPF多余值
            j = j + 1
        elif NPFData_[j]['TRADINGDATE'] == item['TRADINGDATE']:  # 命中
            flag = False
            break
        else:  # 清洗PEF多余值
            flag = ~flag
            break
    while ~flag:  # 行业匹配
        if NPFData_[j]['INDUSTRY'] < item['INDUSTRY']:
            j = j + 1
        elif NPFData_[j]['INDUSTRY'] == item['INDUSTRY']:
            flag = False
            break
        else:
            flag = ~flag
    # PEF/NP匹配
    while ~flag:
        NP_date = datetime.datetime.strptime(NPData_[k]['TRADINGDATE'], "%Y-%m-%d %H:%M:%S.%f0")
        item_date = datetime.datetime.strptime(item['TRADINGDATE'], "%Y-%m-%d %H:%M:%S.%f0")
        if NP_date.year < item_date.year-2 or NP_date.month < item_date.month:
            k = k + 1
        elif NP_date.year == item_date.year-2 and NP_date.month == item_date.month:
            flag = False
            break
        else:
            flag = ~flag
    while ~flag:
        if NPData_[k]['INDUSTRY'] < item['INDUSTRY']:
            k = k + 1
        elif NPData_[k]['INDUSTRY'] == item['INDUSTRY']:
            flag = False
            break
        else:
            flag = ~flag
    # 若匹配成功则进行计算
    if ~flag:
        if (NPFData_[j]['AVG_VALUE']/NPData_[k]['AVG_VALUE']) >= 0:
            PEG = item['AVG_VALUE']/Decimal.from_float(
                ((math.sqrt(NPFData_[j]['AVG_VALUE']/NPData_[k]['AVG_VALUE'])-1)*100))
        else:
            PEG = item['AVG_VALUE'] / Decimal.from_float(
                ((-math.sqrt(-NPFData_[j]['AVG_VALUE'] / NPData_[k]['AVG_VALUE']) - 1) * 100))
        PEG = PEG.quantize(Decimal('0.000000'))
        PEGData.append({'TRADINGDATE': item['TRADINGDATE'], 'INDUSTRY': item['INDUSTRY'], 'PEG_VALUE': PEG, })


# 得到结果
pickle.dump(PEGData, open('PEGData.pkl', 'wb'))


