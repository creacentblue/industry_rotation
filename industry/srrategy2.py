# 行业预期数据复合因子
# PE/G、一致预期净利润同比、一致预期 ROE
# 三个因子等权重加权
from rqalpha.api import *
import pickle


def init(context):
    context.num = 5  # 买入/卖出最高/低的前n只


def before_trading(context):
    pass


def handle_bar(context, bar_dict):
    order_value('000300.XSHG', context.portfolio.cash)

