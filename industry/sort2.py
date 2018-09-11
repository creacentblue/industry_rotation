import pickle


PEGData = pickle.load(open(r'PEGData.pkl', 'rb'))
PEG_dict = {}
pre_date = PEGData[0]['TRADINGDATE']
temp_list = []
for i in PEGData:
    if i['TRADINGDATE'] == pre_date:
        temp_list.append(i)
    else:
        sort_list = sorted(temp_list, key=lambda e: e.__getitem__('PEG_VALUE'), reverse=True)
        j = pre_date[:-17]  # 将交易日期的格式标准化为日期
        PEG_dict[j] = sort_list
        temp_list = []
        pre_date = i['TRADINGDATE']
        temp_list.append(i)
sort_list = sorted(temp_list, key=lambda e: e.__getitem__('PEG_VALUE'), reverse=True)
j = pre_date[:-17]  # 将交易日期的格式标准化为日期
PEG_dict[j] = sort_list
pickle.dump(PEG_dict, open('PEGData_dict.pkl', 'wb'))



















PEGData_dict_new