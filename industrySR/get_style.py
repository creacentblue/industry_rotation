# -*- coding:utf-8 -*-
import datetime
import math
import numpy as np
import pandas as pd
import pymssql
import pickle
# from cache import has_cache, load_cache, save_cache, global_cache_dir
# from tantra.utils.symbol import code2symbol
# from tantra.logger import logger
# from tantra.data.gta import GtaDB


# y=bx+a，线性回归
def linear_regression(x, y):
    x = np.array(x)
    y = np.array(y)
    num = len(x)
    b = (np.sum(x*y)-num*np.mean(x)*np.mean(y))/(np.sum(x*x)-num*np.mean(x)**2)
    return b


style_index = pickle.load(open(r'4style_index.pkl', 'rb'))
df = pd.DataFrame(style_index)

print(len(style_index['DATE']))
# 相对强弱曲线（6组）
zc = []
zx = []
zj = []
cx = []
cj = []
xj = []
for i in range(len(df)):
    zc.append(math.log(df['zhouqi'][i]/df['zhouqi'][0])-math.log(df['chengzhang'][i]/df['chengzhang'][0]))
    zx.append(math.log(df['zhouqi'][i]/df['zhouqi'][0])-math.log(df['xiaofei'][i]/df['xiaofei'][0]))
    zj.append(math.log(df['zhouqi'][i]/df['zhouqi'][0])-math.log(df['jinrong'][i]/df['jinrong'][0]))
    cx.append(math.log(df['chengzhang'][i]/df['chengzhang'][0])-math.log(df['xiaofei'][i]/df['xiaofei'][0]))
    cj.append(math.log(df['chengzhang'][i]/df['chengzhang'][0])-math.log(df['jinrong'][i]/df['jinrong'][0]))
    xj.append(math.log(df['xiaofei'][i]/df['xiaofei'][0])-math.log(df['jinrong'][i]/df['jinrong'][0]))


# 20MA曲线(移动平均过滤)
list1 = []
list2 = []
list3 = []
list4 = []
list5 = []
list6 = []
date = []
for i in range(len(df)-19):
    list1.append(sum(zc[i:i + 20]) / 20)
    list2.append(sum(zx[i:i + 20]) / 20)
    list3.append(sum(zj[i:i + 20]) / 20)
    list4.append(sum(cx[i:i + 20]) / 20)
    list5.append(sum(cj[i:i + 20]) / 20)
    list6.append(sum(xj[i:i + 20]) / 20)
    date.append(df['DATE'][i+19])


# 一阶导数(作为纵坐标)
list1_y = []
list2_y = []
list3_y = []
list4_y = []
list5_y = []
list6_y = []
date_y = []
for i in range(len(list1)-19):
    list1_y.append(linear_regression(np.array(range(20)), list1[i:i+20]))
    list2_y.append(linear_regression(np.array(range(20)), list2[i:i+20]))
    list3_y.append(linear_regression(np.array(range(20)), list3[i:i+20]))
    list4_y.append(linear_regression(np.array(range(20)), list4[i:i+20]))
    list5_y.append(linear_regression(np.array(range(20)), list5[i:i+20]))
    list6_y.append(linear_regression(np.array(range(20)), list6[i:i+20]))
    date_y.append(date[i+19])


# 二阶导数(作为横坐标)
list1_x = []
list2_x = []
list3_x = []
list4_x = []
list5_x = []
list6_x = []
date_x = []
for i in range(len(list1_y)-19):
    list1_x.append(linear_regression(np.array(range(20)), list1_y[i:i+20]))
    list2_x.append(linear_regression(np.array(range(20)), list2_y[i:i+20]))
    list3_x.append(linear_regression(np.array(range(20)), list3_y[i:i+20]))
    list4_x.append(linear_regression(np.array(range(20)), list4_y[i:i+20]))
    list5_x.append(linear_regression(np.array(range(20)), list5_y[i:i+20]))
    list6_x.append(linear_regression(np.array(range(20)), list6_y[i:i+20]))
    date_x.append(date_y[i+19])


# TODO:添加切换边界？？？  y=-5x


# 根据四象限中相对强弱曲线位置进行强势/弱势风格判断
strong_style = {}
weak_style = {}
for i in range(len(date_x)):
    z_cnt = 0
    c_cnt = 0
    x_cnt = 0
    j_cnt = 0
    # zc_cnt = 0
    if list1_y[i+19] + 5 * list1_x[i] > 0:
        z_cnt += 1
    elif list1_y[i+19] + 5 * list1_x[i] < 0:
        c_cnt += 1
    # zx_cnt = 0
    if list2_y[i+19] + 5 * list2_x[i] > 0:
        z_cnt += 1
    elif list2_y[i+19] + 5 * list2_x[i] < 0:
        x_cnt += 1
    # zj_cnt = 0
    if list3_y[i+19] + 5 * list3_x[i] > 0:
        z_cnt += 1
    elif list3_y[i+19] + 5 * list3_x[i] < 0:
        j_cnt += 1
    # cx_cnt = 0
    if list4_y[i+19] + 5 * list4_x[i] > 0:
        c_cnt += 1
    elif list4_y[i+19] + 5 * list4_x[i] < 0:
        x_cnt += 1
    # cj_cnt = 0
    if list5_y[i+19] + 5 * list5_x[i] > 0:
        c_cnt += 1
    elif list5_y[i+19] + 5 * list5_x[i] < 0:
        j_cnt += 1
    # xj_cnt = 0
    if list6_y[i+19] + 5 * list6_x[i] > 0:
        x_cnt += 1
    elif list6_y[i+19] + 5 * list6_x[i] < 0:
        j_cnt += 1
    # 选出票数最高的风格
    tag = []
    if z_cnt >= c_cnt and z_cnt >= x_cnt and z_cnt >= j_cnt:
        if z_cnt > c_cnt and z_cnt > x_cnt and z_cnt > j_cnt:
            tag.append('zhouqi')
        else:
            tag.append('zhouqi')
            if z_cnt == c_cnt:
                tag.append('chengzhang')
            if z_cnt == x_cnt:
                tag.append('xiaofei')
            if z_cnt == j_cnt:
                tag.append('jinrong')
    elif c_cnt >= z_cnt and c_cnt >= x_cnt and c_cnt >= j_cnt:
        if c_cnt > z_cnt and c_cnt > x_cnt and c_cnt > j_cnt:
            tag.append('chengzhang')
        else:
            tag.append('chengzhang')
            if c_cnt == z_cnt:
                tag.append('zhouqi')
            if c_cnt == x_cnt:
                tag.append('xiaofei')
            if c_cnt == j_cnt:
                tag.append('jinrong')
    elif x_cnt >= z_cnt and x_cnt >= c_cnt and x_cnt >= j_cnt:
        if x_cnt > z_cnt and x_cnt > c_cnt and x_cnt > j_cnt:
            tag.append('xiaofei')
        else:
            tag.append('xiaofei')
            if x_cnt == z_cnt:
                tag.append('zhouqi')
            if x_cnt == c_cnt:
                tag.append('chengzhang')
            if x_cnt == j_cnt:
                tag.append('jinrong')
    elif j_cnt >= z_cnt and j_cnt >= c_cnt and j_cnt >= x_cnt:
        if j_cnt > z_cnt and j_cnt > c_cnt and j_cnt > x_cnt:
            tag.append('jinrong')
        else:
            tag.append('jinrong')
            if j_cnt == z_cnt:
                tag.append('zhouqi')
            if j_cnt == c_cnt:
                tag.append('chengzhang')
            if j_cnt == x_cnt:
                tag.append('xiaofei')
    strong_style[date_x[i]] = tag
    # 选出票数最低的风格
    tag = []
    if z_cnt <= c_cnt and z_cnt <= x_cnt and z_cnt <= j_cnt:
        if z_cnt < c_cnt and z_cnt < x_cnt and z_cnt < j_cnt:
            tag.append('zhouqi')
        else:
            tag.append('zhouqi')
            if z_cnt == c_cnt:
                tag.append('chengzhang')
            if z_cnt == x_cnt:
                tag.append('xiaofei')
            if z_cnt == j_cnt:
                tag.append('jinrong')
    elif c_cnt <= z_cnt and c_cnt <= x_cnt and c_cnt <= j_cnt:
        if c_cnt < z_cnt and c_cnt < x_cnt and c_cnt < j_cnt:
            tag.append('chengzhang')
        else:
            tag.append('chengzhang')
            if c_cnt == z_cnt:
                tag.append('zhouqi')
            if c_cnt == x_cnt:
                tag.append('xiaofei')
            if c_cnt == j_cnt:
                tag.append('jinrong')
    elif x_cnt <= z_cnt and x_cnt <= c_cnt and x_cnt <= j_cnt:
        if x_cnt < z_cnt and x_cnt < c_cnt and x_cnt < j_cnt:
            tag.append('xiaofei')
        else:
            tag.append('xiaofei')
            if x_cnt == z_cnt:
                tag.append('zhouqi')
            if x_cnt == c_cnt:
                tag.append('chengzhang')
            if x_cnt == j_cnt:
                tag.append('jinrong')
    elif j_cnt <= z_cnt and j_cnt <= c_cnt and j_cnt <= x_cnt:
        if j_cnt < z_cnt and j_cnt < c_cnt and j_cnt < x_cnt:
            tag.append('jinrong')
        else:
            tag.append('jinrong')
            if j_cnt == z_cnt:
                tag.append('zhouqi')
            if j_cnt == c_cnt:
                tag.append('chengzhang')
            if j_cnt == x_cnt:
                tag.append('xiaofei')
    weak_style[date_x[i]] = tag

print(len(df['DATE']))
print(len(date))
print(len(date_y))
print(len(date_x))
print(len(strong_style))

pickle.dump(strong_style, open('strong_style.pkl', 'wb'))
pickle.dump(weak_style, open('weak_style.pkl', 'wb'))



