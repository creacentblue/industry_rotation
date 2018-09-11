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
        cash = context.portfolio.cash
        order_value("801030.INDX", cash)

