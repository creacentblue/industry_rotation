from rqalpha.api import *
import pickle


def init(context):
    context.mon = 1
    context.con = 0
    context.num = 3  # 买入/卖出最高/低的前n只
    context.sum_value = 0
    context.day_cnt = 0
    context.series = []


def before_trading(context):
    pass


def handle_bar(context, bar_dict):
    ar_value = pickle.load(open(r'./std-3-factors/AR_value{con}/{num}win.pkl'.format(
            num=context.mon, con=context.con+1
        ), 'rb'))
    ar_sorted = {}
    for i in ar_value:
        sort_list = sorted(ar_value[i], key=lambda e: e.__getitem__('AR'))  # 升序
        j = i.strftime("%Y-%m-%d")
        ar_sorted[j] = sort_list
    now_time = context.now.strftime("%Y-%m-%d")
    context.day_cnt += 1
    industry_top = []
    ind = []
    if now_time in ar_sorted.keys():
        for i in range(context.num):
            industry_top.append(ar_sorted[now_time][i]['INDUSTRY']+'.INDX')
        # 先卖出
        for symbol_ in context.portfolio.positions:
            order_target_value(symbol_, 0)
        # 再买入
        cash = context.portfolio.cash
        buy_amount = cash / context.num
        for symbol_ in industry_top:
            order_value(symbol_, buy_amount)
    # context.sum_value = context.sum_value+context.portfolio.daily_returns
    # plot("日收益率", context.sum_value)



