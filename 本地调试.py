import akshare as ak
import pprint as pp
from tabulate import tabulate
import time
import pandas as pd
from colorama import Fore, Style, init
from datetime import datetime,date
from apscheduler.schedulers.blocking import BlockingScheduler

#获取当日日期
today = date.today()
today_str = today.strftime("%Y%m%d") #20250401
today_str = '20250401' #20250401

#stock_info = ak.stock_info_a_code_name()  #所有股票代码和名称 code name

#监测股票列表
stock_symbols = ['600619','600602']
stock_df = pd.DataFrame()
latest_df = pd.DataFrame()


#历史行情东财--每次获取单个标的数据 --ak.stock_zh_a_hist()
def get_stock_info(symbol,date):
    stock_zh_a_hist_df = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=date, end_date=date, adjust="qfq")
    return stock_zh_a_hist_df

#获取股票实时数据
def read_stock_info(stock_symbols):
    global stock_df,latest_df

    temp_df = pd.DataFrame()
    for symbol in stock_symbols:
        temp_df = pd.concat([temp_df,get_stock_info(symbol,today_str)])

    #获取查询时刻的时间
    tick_time = datetime.now().time().replace(microsecond=0)

    for i in stock_symbols:
        if temp_df.loc[temp_df['股票代码']==i , '涨跌幅'].values[0] >= 3:
            temp_df.loc[temp_df['股票代码'] == i, 'tick_time'] = tick_time
            temp_df.loc[temp_df['股票代码'] == i, 'note']  = 'Warning：Stock Price Increasing Rapidly！！！'
        elif temp_df.loc[temp_df['股票代码'] == i , '涨跌幅'].values[0] <= -3:
            temp_df.loc[temp_df['股票代码'] == i, 'tick_time'] = tick_time
            temp_df.loc[temp_df['股票代码'] == i, 'note'] = 'Warning：Stock Price Decreasing Rapidly！！！'
        else:
            temp_df.loc[temp_df['股票代码'] == i, 'tick_time'] = tick_time
            temp_df.loc[temp_df['股票代码'] == i, 'note']  = ''

    stock_df = pd.concat([stock_df, temp_df])

    latest_df = temp_df[['tick_time','股票代码','涨跌幅','note']]
    table = tabulate(latest_df, headers="keys", tablefmt="grid", showindex=False, stralign='center')
    highlighted_table = []
    for line in table.split("\n"):
        if "Increasing" in line:
            highlighted_table.append(f"{Fore.YELLOW}{Style.BRIGHT}{line}{Style.RESET_ALL}")
        elif "Decreasing" in line:
            highlighted_table.append(f"{Fore.RED}{Style.BRIGHT}{line}{Style.RESET_ALL}")
        else:
            highlighted_table.append(line)
    # print(highlighted_table)
    print("\n".join(highlighted_table))


scheduler = BlockingScheduler()
scheduler.add_job(read_stock_info, 'interval', seconds=10, args=(stock_symbols,))
try:
    scheduler.start()
except KeyboardInterrupt:
    scheduler.shutdown()





# stock_sh_a_spot_em_df = ak.stock_zh_a_spot()
# stock_traget = stock_sh_a_spot_em_df[stock_sh_a_spot_em_df['名称'].isin(['海立股份','云赛智联'])]
# print(stock_traget[['名称','涨跌幅']])



