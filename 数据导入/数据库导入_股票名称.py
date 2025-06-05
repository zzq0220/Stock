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

stock_info = ak.stock_info_a_code_name()
stock_info.to_sql(name='stock_name', con=engine ,if_exists='append', index=False)


#如果上述方法接口超时，直接导入excel文件
# df = pd.read_excel('../本地策略/stock_name.xlsx', dtype={'code': str})
# df.to_sql(name='stock_name', con=engine ,if_exists='append', index=False)