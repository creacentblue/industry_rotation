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
Calendar_Date_ = query(cmd, "GTA_QIA_QDB", True)
Calendar_Date = []
for item in Calendar_Date_:
    Calendar_Date.append(item['CALENDARDATE'])
month_end = []
for n in range(len(Calendar_Date_) - 1):
    pre = Calendar_Date_[n]['CALENDARDATE'].month
    cur = Calendar_Date_[n + 1]['CALENDARDATE'].month
    if pre != cur:
        month_end.append(Calendar_Date_[n]['CALENDARDATE'])


core1 = ['801040', '801020', '801030', '801050']  # 周期
core2 = ['801120', '801110', '801150']  # 消费
core3 = ['801750', '801080', '801770', '801760']  # 成长
core4 = ['801780', '801790']  # 金融


style_index = pickle.load(open(r'4style_index.pkl', 'rb'))
strong_style = pickle.load(open(r'strong_style.pkl', 'rb'))
weak_style = pickle.load(open(r'weak_style.pkl', 'rb'))
sum_ = 0
cnt = 0
for iii in strong_style:
    sum_ += len(strong_style[iii])
    if len(strong_style[iii]) == 1:
        cnt += 1
print(sum_/len(strong_style))
print("{}%".format(100*cnt/len(strong_style)))


# 新的申万行业指数（只含交易日数据）
start_date = datetime.datetime.strptime("2010-12-31", "%Y-%m-%d")
end_date = datetime.datetime.strptime("2018-02-28", "%Y-%m-%d")
factor = "IDX_MKT_QUOTATION"
cmd = ' '.join((
    "select SYMBOL, TRADINGDATE, CLOSEPRICE",
    "FROM dbo.{}".format(factor),
    "where TRADINGDATE>='{:s}' and".format(str(start_date)),
    "TRADINGDATE<='{:s}'".format(str(end_date)),
    "and symbol in (SELECT [SYMBOL]"
    "FROM [GTA_QIA_QDB].[dbo].[IDX_INDEXINFO]"
    "where RELEASINGINSTITUTION = '上海申银万国证券研究所有限公司'"
    "  AND SYMBOL != '801060'"
    "  AND SYMBOL != '801070'"
    "  AND SYMBOL != '801090'"
    "  AND SYMBOL != '801100'"
    "  AND SYMBOL != '801190'"
    "  AND SYMBOL != '801220')"
    "order by SYMBOL asc, TRADINGDATE asc"
))
IndexData = query(cmd, "GTA_QIA_QDB", True)
pre_data = IndexData[0]
temp = []
symbol = {}
for data in IndexData:
    if data['SYMBOL'] == pre_data['SYMBOL']:
        if data['TRADINGDATE'] in Calendar_Date:
            temp.append(data['CLOSEPRICE'])
            if data['SYMBOL'] == '801740':
                print(data['TRADINGDATE'])
    else:
        symbol[pre_data['SYMBOL']] = temp
        if data['TRADINGDATE'] in Calendar_Date:
            temp = [data['CLOSEPRICE']]
        else:
            temp = []
        pre_data = data
symbol[pre_data['SYMBOL']] = temp
DF = pd.DataFrame(symbol, index=Calendar_Date)
DF = DF.astype(float)


selected_ind = {}
start_from = Calendar_Date[0]+datetime.timedelta(days=365)  # 11-12-31
for now_time in strong_style.keys():
    if now_time < start_from:
        continue
    if now_time in month_end:
        # 去掉核心行业，用风格指数代替
        drop_ind = []
        drop_ind = core1+core2+core3+core4
        DF1 = DF.copy()
        for iii in drop_ind:
            del DF1[iii]
        for tt in range(len(style_index['DATE'])):
            style_index['zhouqi'][tt] = style_index['zhouqi'][tt].quantize(Decimal('0.00'))
            style_index['chengzhang'][tt] = style_index['chengzhang'][tt].quantize(Decimal('0.00'))
            style_index['xiaofei'][tt] = style_index['xiaofei'][tt].quantize(Decimal('0.00'))
            style_index['jinrong'][tt] = style_index['jinrong'][tt].quantize(Decimal('0.00'))
        DF1['zhouqi'] = style_index['zhouqi']
        DF1['chengzhang'] = style_index['chengzhang']
        DF1['xiaofei'] = style_index['xiaofei']
        DF1['jinrong'] = style_index['jinrong']
        DF1 = DF1.astype(float)
        # 计算距离矩阵
        history_time = now_time-datetime.timedelta(days=250)
        df1 = DF1[history_time:now_time]
        df1 = df1.corr()
        df1 = df1.applymap(lambda x: '%.2f' % math.sqrt(2*(1-x)))  # 行业距离矩阵
        re1 = df1.sort_values(by='zhouqi')['zhouqi']  # 周期
        re2 = df1.sort_values(by='chengzhang')['chengzhang']  # 成长
        re3 = df1.sort_values(by='xiaofei')['xiaofei']  # 消费
        re4 = df1.sort_values(by='jinrong')['jinrong']  # 金融
        # 进行聚类:按距离聚类
        classify = {'zhouqi': [], 'chengzhang': [], 'xiaofei': [], 'jinrong': []}
        for item in df1.keys():
            if item == 'zhouqi' or item == 'chengzhang' or item == 'xiaofei' or item == 'jinrong':
                continue
            if df1[item]['zhouqi'] <= df1[item]['chengzhang'] and df1[item]['zhouqi'] <= df1[item]['xiaofei'] and df1[item]['zhouqi'] <= df1[item]['jinrong']:
                classify['zhouqi'].append(item)
            if df1[item]['chengzhang'] <= df1[item]['zhouqi'] and df1[item]['chengzhang'] <= df1[item]['xiaofei'] and df1[item]['chengzhang'] <= df1[item]['jinrong']:
                classify['chengzhang'].append(item)
            if df1[item]['xiaofei'] <= df1[item]['zhouqi'] and df1[item]['xiaofei'] <= df1[item]['chengzhang'] and df1[item]['xiaofei'] <= df1[item]['jinrong']:
                classify['xiaofei'].append(item)
            if df1[item]['jinrong'] <= df1[item]['zhouqi'] and df1[item]['jinrong'] <= df1[item]['chengzhang'] and df1[item]['jinrong'] <= df1[item]['xiaofei']:
                classify['jinrong'].append(item)
        classify['zhouqi'] += core1
        classify['chengzhang'] += core2
        classify['xiaofei'] += core3
        classify['jinrong'] += core4
        # 根据风格强弱选择卫星行业
        temp_ind = []
        for style in strong_style[now_time]:
            temp_ind += classify[style]
        now_time_ = now_time.strftime("%Y-%m-%d")
        selected_ind[now_time_] = temp_ind


pickle.dump(selected_ind, open('selected_ind.pkl', 'wb'))


