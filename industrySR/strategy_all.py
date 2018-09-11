from rqalpha.api import *
import pickle


def init(context):
    context.data = ['801010','801020','801030','801040','801050','801080','801880',
                    '801110','801120','801130','801140','801150','801160','801170',
                    '801180','801200','801210','801780','801790','801230','801710',
                    '801720','801730','801890','801740','801750','801760','801770']


def before_trading(context):
    pass


def handle_bar(context, bar_dict):
    buy_ = []
    industry_top = context.data
    for i in industry_top:
        buy_.append(i + '.INDX')
    # 先卖出
    for symbol_ in context.portfolio.positions:
        order_target_value(symbol_, 0)
    # 再买入
    cash = context.portfolio.cash
    buy_amount = cash / len(buy_)
    for symbol_ in buy_:
        order_value(symbol_, buy_amount)
    # context.sum_value = context.sum_value+context.portfolio.daily_returns
    # plot("日收益率", context.sum_value)



