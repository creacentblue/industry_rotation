# 获取2010/08到2018/06每月月末对应的各行业成分股

# -*- coding:utf-8 -*-
import datetime
import numpy as np
import pandas as pd
import pymssql
import pickle
import decimal


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
STK_MKT_QUOTATION = pickle.load(open(r'STK_MKT_QUOTATION.pkl', 'rb'))


def query_local(symbol, start, end):
    oops = []
    Flag = False
    for oop in STK_MKT_QUOTATION:
        if oop['SYMBOL'] == symbol:
            if start <= oop['TRADINGDATE'] <= end:
                oops.append(oop)
                Flag = True
        if Flag and oop['TRADINGDATE'] > end:
            break
    return oops


# 得到交易日期
start_date = datetime.datetime.strptime("2015-01-01", "%Y-%m-%d")
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
trading_date = query(cmd, "GTA_QIA_QDB", True)


def get_calender_date(date, num):
    ii = 0
    index = 0
    for cal in trading_date:
        if cal['CALENDARDATE'] == date:
            index = ii
            break
        ii += 1
    result_index = index + num
    result = trading_date[result_index]['CALENDARDATE']
    return result


def get_day_gap(date_sma, date_big):
    gap = 0
    range_ = query_local('000001', date_sma, date_big)
    for cal in range_:
        if date_sma <= cal['TRADINGDATE'] < date_big:
            gap += 1
        if date_sma >= date_big:
            break
    return gap


# 按月和行业分别计算AR
for window in range(6):
    n = 0
    run_time = 0
    start_time = datetime.datetime.now()
    print("start time", start_time)
    AR_value1 = {}
    AR_value3 = {}
    AR_value4 = {}
    AR_value2 = {}
    i = -1
    for now_time in month_end:
        if now_time.month == 1 or now_time.month == 7:
            i += 1
        now_constituent_stock = constituent_stock[change_date[i]]
        ind_AR1 = []
        ind_AR3 = []
        ind_AR4 = []
        ind_AR2 = []
        for ind in now_constituent_stock.keys():
            ind_stocks = now_constituent_stock[ind]
            stock_price_list = {}
            for stock in ind_stocks:
                start_date = get_calender_date(now_time, -30*(window+1))
                end_date = now_time
                data_for_100 = query_local(stock['SYMBOL'], start_date, end_date)
                temp = []
                if data_for_100[0]['TRADINGDATE'] != start_date:
                    times = get_day_gap(start_date, data_for_100[0]['TRADINGDATE'])
                    for iii in range(times):
                        temp.append(decimal.Decimal(0))
                for item in data_for_100:
                    earning = (item['CLOSEPRICE'] - item['PRECLOSEPRICE']) / item['PRECLOSEPRICE']
                    temp.append(earning)
                stock_price_list[stock['SYMBOL']] = pd.Series(temp)
            # 计算AR
            df = pd.DataFrame(stock_price_list)
            dataMat = np.mat(df)
            meanVal = np.mean(dataMat, axis=0)  # 按列求均值，即求各个特征的均值
            newData = dataMat - meanVal
            covMat = np.cov(newData.astype(float), rowvar=0)  # 求协方差矩阵
            eigVals, eigVects = np.linalg.eig(np.mat(covMat))  # 求特征值和特征向量,特征向量是按列放的，即一列代表一个特征向量
            eigValIndice = np.argsort(eigVals)  # 对特征值从小到大排序
            sum_value = 0
            for nums in range(len(eigVals)):
                sum_value += np.var(eigVects[nums:nums+1])
            contribute_ratio1 = (np.var(eigVects[eigValIndice[-1]:eigValIndice[-1]+1])) / sum_value
            contribute_ratio3 = (np.var(eigVects[eigValIndice[-1]:eigValIndice[-1]+1]) +
                                 np.var(eigVects[eigValIndice[-2]:eigValIndice[-2]+1]) +
                                 np.var(eigVects[eigValIndice[-3]:eigValIndice[-3]+1])) / sum_value  # 特征根的大小就是方差贡献的大小
            contribute_ratio4 = (np.var(eigVects[eigValIndice[-1]:eigValIndice[-1]+1]) +
                                 np.var(eigVects[eigValIndice[-2]:eigValIndice[-2]+1]) +
                                 np.var(eigVects[eigValIndice[-3]:eigValIndice[-3]+1]) +
                                 np.var(eigVects[eigValIndice[-4]:eigValIndice[-4]+1])) / sum_value  # 特征根的大小就是方差贡献的大小
            contribute_ratio2 = (np.var(eigVects[eigValIndice[-1]:eigValIndice[-1]+1]) +
                                 np.var(eigVects[eigValIndice[-2]:eigValIndice[-2]+1])) / sum_value  # 特征根的大小就是方差贡献的大小

            ind_AR1.append({"INDUSTRY": ind, "AR": contribute_ratio1})
            ind_AR3.append({"INDUSTRY": ind, "AR": contribute_ratio3})
            ind_AR4.append({"INDUSTRY": ind, "AR": contribute_ratio4})
            ind_AR2.append({"INDUSTRY": ind, "AR": contribute_ratio2})

            n += 1
            run_time_now = datetime.datetime.now()
            run_time = run_time_now - start_time
            print(datetime.datetime.now(), "###", n, "###", run_time, "###", now_time.year, now_time.month, "###", ind,
                  "###", window)
        AR_value1[now_time] = ind_AR1
        AR_value3[now_time] = ind_AR3
        AR_value4[now_time] = ind_AR4
        AR_value2[now_time] = ind_AR2

    # 得到结果
    pickle.dump(AR_value1, open(r'./3-factors/AR_value1/{num}win.pkl'.format(num=window), 'wb'))
    pickle.dump(AR_value3, open(r'./3-factors/AR_value3/{num}win.pkl'.format(num=window), 'wb'))
    pickle.dump(AR_value4, open(r'./3-factors/AR_value4/{num}win.pkl'.format(num=window), 'wb'))
    pickle.dump(AR_value2, open(r'./3-factors/AR_value2/{num}win.pkl'.format(num=window), 'wb'))


