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


#交易日历
trade_date_df = ak.tool_trade_date_hist_sina()
trade_date_df["date_str"] = trade_date_df["trade_date"].apply(lambda x: x.strftime("%Y-%m-%d"))
jyrl = trade_date_df["date_str"].tolist()


#筛选日期
start_date = "2025-01-01"
end_date = "2025-03-31"

# 筛选 2025-01-01 至 2025-04-01 的交易日
filtered_dates = [
    date for date in jyrl
    if start_date <= date <= end_date
]


df = pd.DataFrame()

for day in filtered_dates:
    next_day = trade_date_df[trade_date_df["date_str"]>day]['date_str'].min()

# day = '2025-03-31'
# next_day = '2025-04-01'
    sql1 = f"""
        with t1 as (
    select day,code,rate_high,rate_close,rate_open,turnover,volumn
        from public.stock_price_cleaned 
        where 
            day = '{day}'
            and rate_close >= 9.9
     ),
    t2 as (
    select day,code,rate_high,rate_close,rate_open,turnover,volumn
        from public.stock_price_cleaned 
        where 
            day = '{next_day}'
    )
    select 
        t2.day,
        t2.code,
        t2.rate_close,
        t2.rate_open,
        t1.rate_close rate_previousday,
        t1.rate_high rate_high_previousday,
        t1.turnover turnover_previousday,
        t1.volumn volumn_previousday
    from 
        t1
    left join t2 on t1.code = t2.code 
        """

    df_temp = pd.read_sql_query(sql1, engine)
    df = pd.concat([df, df_temp])

print(df)
df.to_excel('D:/bigo_work/A_Share_investment_Agent-main/本地策略/炸板rzk.xlsx',index=False)
