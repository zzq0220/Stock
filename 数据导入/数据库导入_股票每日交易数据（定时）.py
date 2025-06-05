import akshare as ak
from sqlalchemy import create_engine
from datetime import datetime,date
import pandas as pd
import logging

logging.basicConfig(filename='E:/stock_trading/bat/tcok_price_daily.log', level=logging.INFO, format='%(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

'''本脚本需每日收盘后定时执行，仅导入当天交易数据。定时每日17：00'''

username = 'postgres'
password = '0220'
host = 'localhost'
port = '5432'
database = 'stock_db'

engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{database}')


#获取当日日期
current_date = datetime.now().strftime('%Y-%m-%d')

#获取交易日历
jyrl = ak.tool_trade_date_hist_sina()
jyrl['trade_date'] = jyrl['trade_date'].astype(str)

#判断本日是否为交易日
if current_date in jyrl['trade_date'].values:

        # 获取最新股价数据
        temp_df = ak.stock_zh_a_spot_em()
        temp_df['日期'] = current_date

        temp_df = temp_df[
                ['日期', '今开', '最新价', '最高', '最低', '成交量', '成交额', '振幅', '涨跌幅', '换手率', '代码']]

        # 筛选 仅保留沪深主板
        exclude_prefixes = ("688", "8", "9", "3", "4")
        filtered_df = temp_df[~temp_df['代码'].str.startswith(tuple(exclude_prefixes))]

        # 筛选 去除无数据记录（停牌、退市）
        filtered_df = filtered_df.dropna(subset=['今开'])

        filtered_df = filtered_df.rename(columns={
                '日期': 'day',
                '今开': 'open',
                '最新价': 'close',
                '最高': 'high',
                '最低': 'low',
                '成交量': 'volumn',
                '成交额': 'value',
                '振幅': 'zhenfu',
                '涨跌幅': 'change_rate',
                '换手率': 'turnover',
                '代码': 'code'
        })
        filtered_df.to_sql(name='stock_price', con=engine, if_exists='append', index=False)

        log_message = f"今日 is 交易日，已导入交易数据   {current_date}"
        logging.info(log_message)
        print(log_message)

else:
        log_message = f"今日 is not 交易日，不进行操作  {current_date}"
        logging.info(log_message)
        print(log_message)


