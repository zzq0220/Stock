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
	day >='2025-03-20'
	and day <= '2025-05-13'
-- 	and a.code ='600310'
        """

df = pd.read_sql_query(sql1, engine)

code_list = []

for code,group in df.groupby('code'):
    group = group.sort_values('day').reset_index(drop=True)

    # ==================== 1. 标记涨停日 ====================
    # 标记涨停日
    group['is_limit_up'] = (group['rate_high'] >= 8.5)

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

    continuous_limit_up = group[group['is_limit_up'] & group['prev_limit_up']]
    if len(continuous_limit_up) < 1:
        continue # 没有连续涨停，跳过

    # ==================== 2. 检测涨停中断 ====================
    # 找到连续涨停的结束日（最后一天涨停的日期）
    last_limit_up_day = continuous_limit_up['day'].max()
    last_limit_up_idx = group[group['day'] == last_limit_up_day].index[0]

    # 检查涨停结束后1-2天是否中断（不再涨停）
    interrupt_window = group.iloc[last_limit_up_idx + 1: last_limit_up_idx + 3]  # 往后取1-2天
    if len(interrupt_window) == 0:
        continue  # 数据不足（如最后一天涨停后无后续数据）


    # ==================== 3. 检测大跌阶段 ====================
    # 检查中断后1-3天内是否出现大跌（跌幅≥7%或跌停）
    fall_window = group.iloc[last_limit_up_idx + 1: last_limit_up_idx + 4]  # 中断后1-3天
    if len(fall_window) == 0:
        continue

    # 判断是否出现大跌
    big_fall = fall_window[fall_window['rate_close'] <= -6.0] # 跌停或跌幅≥6%
    if len(big_fall) == 0:
        continue

    # ==================== 4. 检查巨额成交量 ====================
    # 计算涨停开始前的5日均成交量（作为基准）
    first_limit_up_idx = last_limit_up_idx - group.iloc[last_limit_up_idx]['limit_up_streak'] + 1
    before_window = group.iloc[first_limit_up_idx - 5: first_limit_up_idx]
    average_volumn = before_window['volumn'].mean()

    #阶段二建仓期 是否出现大量
    target_window = group.iloc[last_limit_up_idx + 2 : last_limit_up_idx + 6]
    huge_volume = target_window[target_window['volumn'] >= 1.7 * average_volumn]

    if len(huge_volume) == 0:
        continue

        # ==================== 5. 所有条件满足，记录股票 ====================
    code_list.append(code)

stock_name = pd.read_sql_query('select * from public.stock_name', engine)
filtered_stocks = stock_name[stock_name['code'].isin(code_list)].reset_index(drop=True)
filtered_stocks.to_excel('../模式一备选股（变种）.xlsx',index=False)
print(filtered_stocks)