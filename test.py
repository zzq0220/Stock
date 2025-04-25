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

sql1 = f"""
select 
	*
from 
	public.stock_price_cleaned 
where 
	day >='2025-01-01'
	and day <= '2025-04-23'
--     and code ='000620'
        """

df = pd.read_sql_query(sql1, engine)

result_list = []

for code,group in df.groupby('code'):
    group = group.sort_values('day').reset_index(drop=True)
    # 标记涨停日
    group['is_limit_up'] = (group['rate_close'] >= 9.9)

    # 找出连续涨停的起始日（前一天也是涨停）
    group['prev_limit_up'] = group['is_limit_up'].shift(1)

    # 计算连续涨停天数
    streak = 0
    group['limit_up_streak'] = 0  # 初始化列
    for i in range(len(group)):
            if group.iloc[i]['is_limit_up']:
                    streak += 1
                    group.iloc[i, group.columns.get_loc('limit_up_streak')] = streak
            else:
                    streak = 0

    group_indices = group.index.tolist()
    for i in group_indices:
        if i <= 2:
           continue

        if i == len(group_indices)-1:
           continue

        #i日涨停（该涨停为第一个涨停），i+1日开收盘差值<=2，振幅>4，有上影线。i+1日成交量大于i日
        if (group.loc[i,'rate_close'] >=9.9
                and group.loc[i,'limit_up_streak'] ==1
                and abs(group.loc[i+1,'rate_open'] - group.loc[i+1,'rate_close']) <= 2
                and group.loc[i+1,'zhenfu'] >4
                and group.loc[i+1,'rate_high']>group.loc[i+1,'rate_open']
                and group.loc[i+1,'rate_high']
                and group.loc[i+1,'volumn']>2*group.loc[i,'volumn']
                and group.loc[i,'volumn'] >(group.loc[i-1,'volumn']+group.loc[i-2,'volumn'])/2):
           day = group.loc[i,'day']
           code = group.loc[i,'code']
           result_list.append({'day': day, 'code': code})

result_df = pd.DataFrame(result_list)
result_df.to_excel('../11.xlsx')
print(result_df)
