import datetime
import numpy as np
import pandas as pd
import pymssql
import pickle
import decimal
import csv


more_industry = pickle.load(open(r'more_industry.pkl', 'rb'))
empty_industry = pickle.load(open(r'empty_industry.pkl', 'rb'))

df1 = pd.DataFrame(more_industry)
df2 = pd.DataFrame(empty_industry)
df = pd.merge(df1, df2, on='DATE')
print(df['LONG'][2][0],df['LONG'][2][1],df['LONG'][2][2])


writer = pd.ExcelWriter('test.xlsx')

df.to_excel(writer, sheet_name='Data', startcol=0, index=False)

