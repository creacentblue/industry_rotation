# 行业预期数据复合因子
# PE/G、一致预期净利润同比、一致预期 ROE
# 三个因子等权重加权
from rqalpha.api import *
import pickle


def init(context):
    peg = pickle.load(open(r'PEGData_dict.pkl', 'rb'))
    context.data = peg
    context.num = 5  # 买入/卖出最高/低的前n只
    context.sum_value = 0
    context.day_cnt = 0

def before_trading(context):
    pass


def handle_bar(context, bar_dict):
    now_time = context.now.strftime("%Y-%m-%d")
    context.day_cnt += 1
    industry_top = []
    if now_time in context.data.keys():
        for i in range(context.num):
            industry_top.append(context.data[now_time][i+1]['INDUSTRY']+'.INDX')
        # 先卖出
        for symbol_ in context.portfolio.positions:
            order_target_value(symbol_, 0)
        # 再买入
        cash = context.portfolio.cash
        buy_amount = cash / context.num
        for symbol_ in industry_top:
            order_value(symbol_, buy_amount)
    context.sum_value = context.sum_value+context.portfolio.daily_returns
    plot("日收益率", context.sum_value)



