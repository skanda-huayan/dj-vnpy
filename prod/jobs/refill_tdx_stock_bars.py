# flake8: noqa
"""
下载通达信股票合约1分钟&日线bar => vnpy项目目录/bar_data/
上海股票 => SSE子目录
深圳股票 => SZSE子目录
修改为多进程模式
"""
import os
import sys
import csv
import json
from collections import OrderedDict
import pandas as pd
from multiprocessing import Pool
from concurrent.futures import ThreadPoolExecutor

from copy import copy

vnpy_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if vnpy_root not in sys.path:
    sys.path.append(vnpy_root)

os.environ["VNPY_TESTING"] = "1"

from vnpy.data.tdx.tdx_stock_data import *
from vnpy.data.common import resample_bars_file
from vnpy.trader.utility import load_json
from vnpy.trader.utility import get_csv_last_dt
from vnpy.trader.util_wechat import send_wx_msg

# 保存的1分钟指数 bar目录
bar_data_folder = os.path.abspath(os.path.join(vnpy_root, 'bar_data'))

# 开始日期（每年大概需要几分钟）
start_date = '20160101'

# 创建API对象
api_01 = TdxStockData()

# 额外需要数据下载的基金列表
stock_list = load_json('stock_list.json')

# 强制更新缓存
api_01.cache_config()
symbol_dict = api_01.symbol_dict
#
# thread_executor = ThreadPoolExecutor(max_workers=1)
# thread_tasks = []


def refill(symbol_info):
    period = symbol_info['period']
    progress = symbol_info['progress']
    # print("{}_{}".format(period, symbol_info['code']))
    # return
    stock_code = symbol_info['code']

    # if stock_code in stock_list:
    # print(symbol_info['code'])
    if symbol_info['exchange'] == 'SZSE':
        exchange_name = '深交所'
        exchange = Exchange.SZSE
    else:
        exchange_name = '上交所'
        exchange = Exchange.SSE

    # num_stocks += 1

    stock_name = symbol_info.get('name')
    print(f'开始更新:{exchange_name}/{stock_name}, 代码:{stock_code}')
    bar_file_folder = os.path.abspath(os.path.join(bar_data_folder, f'{exchange.value}'))
    if not os.path.exists(bar_file_folder):
        os.makedirs(bar_file_folder)
    # csv数据文件名
    p_name = period.replace('min', 'm').replace('day', 'd').replace('hour', 'h')
    bar_file_path = os.path.abspath(os.path.join(bar_file_folder, f'{stock_code}_{p_name}.csv'))

    # 如果文件存在，
    if os.path.exists(bar_file_path):
        # 取最后一条时间
        last_dt = get_csv_last_dt(bar_file_path)
    else:
        last_dt = None

    if last_dt:
        start_dt = last_dt - timedelta(days=1)
        print(f'文件{bar_file_path}存在，最后时间:{start_dt}')
    else:
        start_dt = datetime.strptime(start_date, '%Y%m%d')
        print(f'文件{bar_file_path}不存在，或读取最后记录错误,开始时间:{start_date}')

    d1 = datetime.now()
    result, bars = api_01.get_bars(symbol=stock_code,
                                   period=period,
                                   callback=None,
                                   start_dt=start_dt,
                                   return_bar=False)
    # [dict] => dataframe
    if not result or len(bars) == 0:
        return

    need_resample = False
    # 全新数据
    if last_dt is None:
        data_df = pd.DataFrame(bars)
        data_df.set_index('datetime', inplace=True)
        data_df = data_df.sort_index()
        # print(data_df.head())
        print(data_df.tail())
        data_df.to_csv(bar_file_path, index=True)
        d2 = datetime.now()
        microseconds = (d1 - d1).microseconds
        print(f'{progress}% 首次更新{stock_code} {stock_name}数据 {microseconds} 毫秒=> 文件{bar_file_path}')
        need_resample = True

    # 增量更新
    else:
        # 获取标题
        headers = []
        with open(bar_file_path, "r", encoding='utf8') as f:
            reader = csv.reader(f)
            for header in reader:
                headers = header
                break

        bar_count = 0
        # 写入所有大于最后bar时间的数据
        # with open(bar_file_path, 'a', encoding='utf8', newline='\n') as csvWriteFile:
        with open(bar_file_path, 'a', encoding='utf8') as csvWriteFile:

            writer = csv.DictWriter(f=csvWriteFile, fieldnames=headers, dialect='excel',
                                    extrasaction='ignore')
            for bar in bars:
                if bar['datetime'] <= last_dt:
                    continue
                bar_count += 1
                writer.writerow(bar)
                if not need_resample:
                    need_resample = True
            d2 = datetime.now()
            microseconds = round((d2 - d1).microseconds / 100, 0)
            print(f'{progress}%,更新{stock_code}  {stock_name} 数据 {microseconds}毫秒 => 文件{bar_file_path}, 最后记录:{bars[-1]}')

    # 采用多线程方式输出 5、15、30分钟的数据
    # if period == '1min' and need_resample:
    #     task = thread_executor.submit(resample, stock_code, exchange, [5, 15, 30])
    #     thread_tasks.append(task)


def resample(symbol, exchange, x_mins=[5, 15, 30]):
    """
    更新多周期文件
    :param symbol:
    :param exchange:
    :param x_mins:
    :return:
    """
    d1 = datetime.now()
    out_files, err_msg = resample_bars_file(vnpy_root=vnpy_root,
                                            symbol=symbol,
                                            exchange=exchange,
                                            x_mins=x_mins)
    d2 = datetime.now()
    microseconds = round((d2 - d1).microseconds / 100, 0)
    if len(err_msg) > 0:
        print(err_msg, file=sys.stderr)

    if out_files:
        print(f'{microseconds}毫秒,生成 =>{out_files}')


if __name__ == '__main__':

    # 下载所有的股票数据
    num_progress = 0
    total_tasks = len(symbol_dict.keys()) * 2
    tasks = []
    for period in ['1min', '5min', '15min', '30min', '1hour', '1day']:
        for symbol in symbol_dict.keys():
            info = copy(symbol_dict[symbol])
            stock_code = info['code']
            if ('stock_type' in info.keys() and info['stock_type'] in ['stock_cn',
                                                                       'cb_cn']) or stock_code in stock_list:
                info['period'] = period
                tasks.append(info)
                # if len(tasks) > 12:
                #     break

    total_tasks = len(tasks)
    for task in tasks:
        num_progress += 1
        task['progress'] = round(100 * num_progress / total_tasks, 2)

    p = Pool(12)
    p.map(refill, tasks)
    p.close()
    p.join()

    #
    msg = 'tdx股票数据补充完毕: num_stocks={}'.format(total_tasks)
    send_wx_msg(content=msg)
    os._exit(0)
