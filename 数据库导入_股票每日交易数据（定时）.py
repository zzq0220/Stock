import akshare as ak
from sqlalchemy import create_engine
from datetime import datetime,date

'''本脚本需每日收盘后定时执行，仅导入当天交易数据。定时每日17：00'''

username = 'postgres'
password = '0220'
host = 'localhost'
port = '5432'
database = 'stock_db'

engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{database}')


temp_df = ak.stock_zh_a_spot_em()
current_date = datetime.now().strftime('%Y-%m-%d')
temp_df['日期'] = current_date

temp_df = temp_df[['日期','今开','最新价','最高','最低','成交量','成交额','振幅','涨跌幅','涨跌额','换手率','代码']]

#筛选 仅保留沪深主板
exclude_prefixes = ("688", "8", "9", "3", "4")
filtered_df = temp_df[~temp_df['代码'].str.startswith(tuple(exclude_prefixes))]

#筛选 去除无数据记录（停牌、退市）
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
    '涨跌额': 'change_amount',
    '换手率': 'turnover',
    '代码': 'code'
})
filtered_df.to_sql(name='stock_price', con=engine ,if_exists='append', index=False)

