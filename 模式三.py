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



sql1 = f"""
select 
	a.*,b.name
from 
	public.stock_price_cleaned a
left join  public.stock_name b on a.code =b.code
where 
	day >='2025-04-10'
	and day <= '2025-05-15'
-- 	and a.code ='000558'
        """

df = pd.read_sql_query(sql1, engine)

result_list = []

for code,group in df.groupby('code'):
    group = group.sort_values('day').reset_index(drop=True)

    # ==================== 1. 标记涨停日 ====================
    # 标记涨停日
    group['is_limit_up'] = (group['rate_high'] >= 9.8)

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
        # 确定滑动窗口的终点
        end_index = min(i + 9, len(group)-1)
        watch_window = group.loc[i:end_index]

        #检查窗口是否满足条件
        if watch_window.loc[i,'limit_up_streak'] == 1:
            if watch_window['limit_up_streak'].max() <= 2:
                #首板索引
                shouban_index = watch_window[watch_window['limit_up_streak'] == 1].index
                #涨停索引
                zhangting_index = watch_window[watch_window['is_limit_up'] == True].index

                if len(shouban_index) >= 2:
                    for j in range(len(shouban_index) - 1):
                        temp_df = watch_window.loc[shouban_index[j]+1:zhangting_index.max(),'rate_close']

                        if shouban_index[j + 1] - shouban_index[j] <= 5\
                        and watch_window.loc[shouban_index[j]+1:zhangting_index.max()+1,'rate_close'].sum() <= 20\
                        and watch_window.loc[shouban_index[j]+1:zhangting_index.max(),'rate_close'].sum() <= 20:

                            day = group.loc[i, 'day']
                            code = group.loc[i, 'code']
                            name = group.loc[i, 'name']
                            result_list.append({'day': day, 'code': code, 'name': name})
                            break


result_df = pd.DataFrame(result_list)
result_df.to_excel('../模式三.xlsx')
print(result_df)
