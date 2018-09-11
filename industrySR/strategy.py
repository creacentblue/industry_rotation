from rqalpha.api import *
import pickle


def init(context):
    selected_ind = pickle.load(open(r'selected_ind.pkl', 'rb'))
    context.data = selected_ind


def before_trading(context):
    pass


def handle_bar(context, bar_dict):
    now_time = context.now.strftime("%Y-%m-%d")
    buy_ = []
    if now_time in context.data.keys():
        industry_top = context.data[now_time]
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



