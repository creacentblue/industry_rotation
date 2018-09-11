# 行业预期数据复合因子
# PE/G、一致预期净利润同比、一致预期 ROE
# 三个因子等权重加权
from rqalpha.api import *
import pickle


def init(context):
    peg = pickle.load(open(r'PEGData_dict.pkl', 'rb'))
    context.data = peg
    context.num = 5  # 买入/卖出最高/低的前n只
    stocks = pickle.load(open(r'Stocks_dict.pkl', 'rb'))
    context.stocks = stocks


def before_trading(context):
    pass


def handle_bar(context, bar_dict):
    now_time=context.now.strftime("%Y-%m-%d")
    industry_top = []
    industry_bottom = []
    symbol_top = []
    symbol_bottom = []
    symbol_buy = []
    symbol_sell = []
    buy_count = 0
    sell_count = 0
    plot()

    # 每月月初买入排名最高的10只股票，卖出排名最低的10只股票
    if now_time in context.data.keys():
        for i in range(context.num):
            industry_top.append(context.data[now_time][i]['INDUSTRY'])
            industry_bottom.append(context.data[now_time][-i]['INDUSTRY'])
        for stock in context.stocks[now_time]:
            if stock['INDUSTRY'] in industry_top:
                symbol_top.append(stock['SYMBOL'])
            if stock['INDUSTRY'] in industry_bottom:
                symbol_bottom.append(stock['SYMBOL'])
        for symbol in symbol_top:
            if symbol[0] == '6' :   # 沪A
                symbol = symbol + '.XSHG'
            else:                  # 0深A 3创业板
                symbol = symbol + '.XSHE'
            if not is_suspended(symbol):
                symbol_buy.append(symbol)
                buy_count=buy_count+1
        for symbol in symbol_bottom:
            if symbol[0] == '6' :   # 沪A
                symbol = symbol + '.XSHG'
            else:                  # 0深A 3创业板
                symbol = symbol + '.XSHE'
            if not is_suspended(symbol):
                symbol_sell.append(symbol)
                sell_count = sell_count+1
        # 先卖出
        for symbol in symbol_sell:
            order_target_value(symbol, 0)
        # 再买入
        cash = context.portfolio.cash
        buy_amount = cash / sell_count
        for symbol in symbol_buy:
            order_value(symbol, buy_amount)

