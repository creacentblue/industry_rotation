import datetime
import math
import numpy as np
import pandas as pd
import pymssql
# from pathlib import  Path
# import os
# import yaml
import pickle
# from cache import has_cache, load_cache, save_cache, global_cache_dir
# from tantra.utils.symbol import code2symbol
# from tantra.logger import logger
# from tantra.data.gta import GtaDB
import csv
from decimal import *


# 数据库连接
ip = "192.168.0.101"
port = 1433
user = 'WANGLEI'
password = '425189'


# 查询函数
def query(cmd_, db, as_dict):
    with pymssql.connect(
            server=ip, port=port, user=user, password=password, database=db) as conn:
        with conn.cursor(as_dict=as_dict) as cursor:
            cursor.execute(cmd_)
            return [row for row in cursor]


start_date = datetime.datetime.strptime("2010-01-01", "%Y-%m-%d")
end_date = datetime.datetime.strptime("2018-07-01", "%Y-%m-%d")
cmd = ' '.join((
    "select SYMBOL,TRADINGDATE,PRECLOSEPRICE,CLOSEPRICE",
    "FROM dbo.STK_MKT_QUOTATION",
    "where TRADINGDATE>='{:s}' and".format(str(start_date)),
    "TRADINGDATE<='{:s}' and".format(str(end_date)),
    "order by SYMBOL,TRADINGDATE"
))
STK_MKT_QUOTATION = query(cmd, "GTA_QIA_QDB", True)


# 得到结果
pickle.dump(STK_MKT_QUOTATION, open('STK_MKT_QUOTATION.pkl', 'wb'))

