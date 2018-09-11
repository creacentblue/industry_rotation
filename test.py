from rqalpha.api import *
def init(context):
    context.s1 = "000001.XSHE"
    context.fired = False

def handler_bar(context, bar_dict):
    if not context.fired:
        # order_percent并且传入1代表买入该股票并且使其占有投资组合的100%
        order_percent(context.s1, 1)
        context.fired = True
