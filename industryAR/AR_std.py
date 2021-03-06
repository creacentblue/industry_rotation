# 标准化处理


import pickle
import numpy as np


ar_value = pickle.load(open(r'AR_value.pkl', 'rb'))
# 取出所有行业
ind_symbol = []
for k in ar_value:
    li = ar_value[k]
    for m in li:
        ind_symbol.append(m['INDUSTRY'])
    break
# pickle.dump(ind_symbol, open('ind_symbol.pkl', 'wb'))
# 取出所有日期
AR_date = []
for k in ar_value:
    AR_date.append(k)


industry_data = {}
for i in ind_symbol:  # 行业
    temp = []
    for date in ar_value:
        for item in ar_value[date]:
            if item['INDUSTRY'] == i:
                temp.append(item['AR'])
    industry_data[i] = temp


sum_value = 0
num = 0
for ind in industry_data:
    for item in industry_data[ind]:
        sum_value += item
        num += 1
    avg_value = sum_value/num
    std_value = np.std(industry_data[ind])
    for i in range(len(industry_data[ind])):
        industry_data[ind][i] = (industry_data[ind][i] - avg_value)/std_value


std_data = {}
cnt = 0
for i in AR_date:  # 日期
    temp = []
    for ind_ in industry_data:
        j = 0
        target = ind_
        for item in industry_data[ind_]:
            if j == cnt:
                target = item
                break
            j += 1
        temp.append({'INDUSTRY': ind_, 'AR': target})
    std_data[i] = temp
    cnt += 1


pickle.dump(std_data, open('AR_std.pkl', 'wb'))
