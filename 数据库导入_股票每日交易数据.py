import akshare as ak
from sqlalchemy import create_engine
import pprint as pp
from tabulate import tabulate
import time
import pandas as pd
from colorama import Fore, Style, init
from datetime import datetime,date
from apscheduler.schedulers.blocking import BlockingScheduler

username = 'postgres'
password = '0220'
host = 'localhost'
port = '5432'
database = 'stock_db'

engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{database}')


#获取当日日期
today = date.today()

start_date = '20250404'
end_date = '20250423'


stock_info = ak.stock_info_a_code_name()  #所有股票代码和名称 code name
stock_list =stock_info['code'].to_list()

# 定义要排除的前缀列表
exclude_prefixes = ("688", "8", "9", "3", "4")

# 使用列表推导式过滤
filtered_stocks = [
    code for code in stock_list
    if not code.startswith(exclude_prefixes)
]

# filtered_stocks = ['000001']
stock_df = pd.DataFrame()

for i in filtered_stocks:
    temp_df = ak.stock_zh_a_hist(symbol=i, period='daily', start_date=start_date, end_date=end_date,adjust='qfq')
    stock_df = pd.concat([stock_df, temp_df])

sql_df = stock_df[['日期','开盘','收盘','最高','最低','成交量','成交额','振幅','涨跌幅','涨跌额','换手率','股票代码']]
sql_df = sql_df.rename(columns={
    '日期': 'day',
    '开盘': 'open',
    '收盘': 'close',
    '最高': 'high',
    '最低': 'low',
    '成交量': 'volumn',
    '成交额': 'value',
    '振幅': 'zhenfu',
    '涨跌幅': 'change_rate',
    '涨跌额': 'change_amount',
    '换手率': 'turnover',
    '股票代码': 'code'
})
sql_df.to_sql(name='stock_price', con=engine ,if_exists='append', index=False)








