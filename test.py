from sqlalchemy import create_engine
import pandas as pd
import akshare as ak
from datetime import datetime, date, timedelta

# 创建数据库引擎

username = 'postgres'
password = '0220'
host = 'localhost'
port = '5432'
database = 'stock_db'

engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{database}')

# sql ='''
# select * from public.stock_price where day >='2024-01-01'
# '''

# df = pd.read_sql(sql, con=engine)
# df.to_excel('D:/bigo_work/A_Share_investment_Agent-main/本地策略/stock_price20240101.xlsx')

df = pd.read_excel('../本地策略/stock_price20240101.xlsx', dtype={'code': str})
df.to_sql(name='stock_price1', con=engine ,if_exists='append', index=False)  #