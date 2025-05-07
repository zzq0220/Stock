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
	b.name,a.*
from 
	public.stock_price_cleaned a
left join public.stock_name b on a.code =b.code
where 
	day >='2025-03-01'
	and day <= '2025-05-01'
--     and a.code ='000062'
        """

df = pd.read_sql_query(sql1, engine)

result_list = []

for code,group in df.groupby('code'):
    group = group.sort_values('day').reset_index(drop=True)

    #标记阳线阴线
    group.loc[:,'color'] ='green'
    group.loc[group['rate_close']-group['rate_open']>=0,'color'] ='red'
    group.loc[group['rate_close']>=0,'color'] ='red'

    # 标记涨停日
    group['is_limit_up'] = (group['rate_close'] >= 9.8)

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


    #len(group) = 11 ,01234567 89 10 11  ; for i in range(len(group)) i:0 1 2 ... 10

    group_indices = group.index.tolist()
    for i in group_indices:
        # if i <= 4:
        #    continue

        if i > len(group)-5:
           continue

        temp_index = min(i+5,len(group))
        end_index = min(i+13,len(group))

        watch_window = group.loc[i+3:end_index]

        red_df =watch_window[watch_window['color']=='red']
        green_df =watch_window[watch_window['color']=='green']

        red_mean =watch_window[watch_window['color']=='red']['volumn'].mean()
        green_mean =watch_window[watch_window['color']=='green']['volumn'].mean()


        if (group.loc[i,'is_limit_up']==True and group.loc[i,'limit_up_streak']<=2 #i日为涨停，最多2板
                and group.loc[i+1,'is_limit_up']==False   #第二天不是涨停
                and group.loc[i+1:temp_index]['rate_close'].sum()<=2
                and len(watch_window[watch_window['color']=='red'])>=2 #最少有两天阳线
                and watch_window[watch_window['color']=='red']['volumn'].mean() >= watch_window[watch_window['color']=='green']['volumn'].mean() * 1.3
                and len(watch_window[watch_window['is_limit_up']==True]) == 0 #观察窗口内无涨停
            ):

           day = group.loc[i,'day']
           code = group.loc[i,'code']
           name = group.loc[i,'name']
           result_list.append({'day': day, 'code': code, 'name': name})

result_df = pd.DataFrame(result_list)
result_df.to_excel('../i日激活换手建仓20250507 new.xlsx')
print(result_df)
