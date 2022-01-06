# flake8: noqa
"""
下载天勤期货历史tick数据 => vnpy项目目录/bar_data/tq/
"""
import os
import sys
import json
import csv
from collections import OrderedDict
import pandas as pd
from contextlib import closing
from datetime import datetime, timedelta
import argparse
from tqsdk import TqApi, TqSim,TqAuth

vnpy_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if vnpy_root not in sys.path:
    sys.path.append(vnpy_root)

os.environ["VNPY_TESTING"] = "1"

from vnpy.data.tdx.tdx_future_data import get_future_contracts, Exchange
from vnpy.trader.utility import get_csv_last_dt, get_underlying_symbol, extract_vt_symbol
from vnpy.data.tq.downloader import DataDownloader,get_account_config

if __name__ == "__main__":
    if len(sys.argv) <= 1:
        print('请使用 --help 查看说明')
    # 参数分析
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--symbol', type=str, default='', help='下载合约，格式： rb2010 或者 SHFE.rb2010')
    parser.add_argument('-b', '--begin', type=str, default='20160101', help='开始日期，格式：20160101')
    parser.add_argument('-e', '--end', type=str, default=datetime.now().strftime('%Y%m%d'),
                        help='结束日期,格式:{}'.format(datetime.now().strftime('%Y%m%d')))
    args = parser.parse_args()
    if len(args.symbol) == 0:
        print('下载合约未设定 参数 -s rb2010')
        os._exit(0)
    auth_dict = get_account_config()
    # 开始下载(使用快期的免费行情websocket)
    api = TqApi(account=TqSim(), auth=TqAuth(auth_dict['user_name'],auth_dict['password'])) #  url="wss://u.shinnytech.com/t/md/front/mobile"
    download_tasks = {}
    begin_date = datetime.strptime(args.begin, '%Y%m%d')
    end_date = datetime.strptime(args.end, '%Y%m%d')
    if end_date > datetime.now():
        end_date = datetime.now()
    n_days = (end_date - begin_date).days

    future_contracts = get_future_contracts()
    if '.' not in args.symbol:
        underly_symbol = get_underlying_symbol(args.symbol).upper()
        contract_info = future_contracts.get(underly_symbol)
        symbol = args.symbol
        exchange = Exchange(contract_info.get('exchange'))
    else:
        symbol, exchange = extract_vt_symbol(args.symbol)

    if n_days <= 0:
        n_days = 1

    for n in range(n_days + 1):
        download_date = begin_date + timedelta(days=n)
        if download_date.isoweekday() in [6, 7]:
            continue

        save_folder = os.path.abspath(os.path.join(
            vnpy_root, 'tick_data', 'tq', 'future',
            download_date.strftime('%Y%m')))
        if not os.path.exists(save_folder):
            os.makedirs(save_folder)

        save_file = os.path.abspath(os.path.join(save_folder,
                                                 "{}_{}.csv".format(symbol, download_date.strftime('%Y%m%d'))))
        zip_file = os.path.abspath(os.path.join(save_folder,
                                                "{}_{}.pkb2".format(symbol, download_date.strftime('%Y%m%d'))))

        if os.path.exists(save_file):
            # 文件size是否大于1024字节
            if os.path.getsize(save_file) > 1024:
                # 获取最后的tick时间
                dt = get_csv_last_dt(file_name=save_file, dt_format='%Y-%m-%d %H:%M:%S.%f')
                # 判断是否为交易日最后
                if dt and dt.hour == 14 and dt.minute == 59:
                    continue

        if os.path.exists(zip_file):
            if os.path.getsize(save_file) > 1024:
                continue

        # 下载从 2018-05-01凌晨0点 到 2018-06-01凌晨0点 的 T1809 盘口Tick数据
        download_tasks["{}_{}_tick".format(symbol, download_date.strftime('%Y%m%d'))] = DataDownloader(
            api,
            symbol_list=f"{exchange.value}.{symbol}",
            dur_sec=0,
            start_dt=download_date.date(),
            end_dt=download_date.replace(hour=16), csv_file_name=save_file)

    # 使用with closing机制确保下载完成后释放对应的资源
    with closing(api):
        while not all([v.is_finished() for v in download_tasks.values()]):
            api.wait_update()
            print("progress: ", {k: ("%.2f%%" % v.get_progress()) for k, v in download_tasks.items()})
