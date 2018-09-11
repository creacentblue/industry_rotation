from rqalpha.api import *
import pickle


def init(context):
    ind = pickle.load(open(r'ind_symbol.pkl', 'rb'))
    context.ind = ind
    context.fired = False
    context.num = 28


def before_trading(context):
    pass


def handle_bar(context, bar_dict):
    if not context.fired:
        context.fired = True
        industry_top = []
        for i in range(context.num):
            industry_top.append(context.ind[i] + '.INDX')
        cash = context.portfolio.cash
        buy_amount = cash / context.num
        for symbol_ in industry_top:
            order_value(symbol_, buy_amount)
    # context.sum_value = context.sum_value+context.portfolio.daily_returns
    # plot("日收益率", context.sum_value)

