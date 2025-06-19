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
	day >='2025-04-14'
	and day <= '2025-05-06'
	and a.code ='002329'
        """

df = pd.read_sql_query(sql1, engine)

result_list = []

for code,group in df.groupby('code'):
    group = group.sort_values('day').reset_index(drop=True)

    # ==================== 1. 标记涨停日 ====================
    # 标记涨停日
    group['is_limit_up'] = (group['rate_close'] >= 9.7)

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

    # 标记炸板日
    group['is_zhaban'] = (group['rate_high'] >= 9.7) & (group['rate_close'] < 9.4)

    # 标记涨停or炸板日
    group['is_ban'] = (group['is_zhaban'] == True) | (group['is_limit_up'] == True )




    group_indices = group.index.tolist()
    group_end_index = max(group_indices)

    for i in group_indices:
        # 确定滑动窗口的终点
        end_index = min(i + 20, len(group)-1)
        # end_index = min(i + 20, len(group)-1)
        watch_window = group.loc[i:end_index]

        #检查窗口是否满足无连板,若有连扳pass
        if watch_window['limit_up_streak'].max() >= 2:
            if watch_window.loc[i,'limit_up_streak'] == 2:
                pass
            if watch_window.loc[i+1:, 'limit_up_streak'].max() >=2:
                continue

        if watch_window.loc[i,'is_ban'] == True:
            #如果在i之后，还出现了涨停板/炸板的情况
            if watch_window.loc[i + 1:, 'is_ban'].any():
                # 找到下一个 is_ban 为 True 的记录的索引
                next_ban_index = watch_window.loc[i + 1:, 'is_ban'].idxmax()

                if next_ban_index == i+1:
                    continue

                #设置一个累计涨幅观察窗口，起点为首次涨停下一日（i+1），终点为第二次板的两日后（next_ban_index+2）和end_index之间的最小值
                leiji_zhangfu_window_end_index = min(next_ban_index+2,group_end_index)
                #如果从第一个板到第二个板+2天以内，累计涨幅小于5（基本还停留在原来的价位）
                if group.loc[i+1:leiji_zhangfu_window_end_index]['rate_close'].sum() > 5:
                    continue

                #运作阶段成交量应该大于 运作前的两倍
                if i >= 3 :
                    if group.loc[next_ban_index + 1:leiji_zhangfu_window_end_index]['volumn'].mean() < 1.39 * group.loc[i-3:i - 1]['volumn'].mean():
                        continue

                if i<3 and i >0:
                    if group.loc[next_ban_index + 1:leiji_zhangfu_window_end_index]['volumn'].mean() < 1.39 * group.loc[0:i - 1]['volumn'].mean():
                        continue

                #运作后无大涨
                if group.loc[next_ban_index+1:group_end_index]['rate_close'].sum() > 5:
                    continue



                day = group.loc[i, 'day']
                code = group.loc[i, 'code']
                name = group.loc[i, 'name']
                result_list.append({'day': day, 'code': code, 'name': name})
                continue




result_df = pd.DataFrame(result_list)
# result_df.to_excel('../模式四.xlsx')
print(result_df)
