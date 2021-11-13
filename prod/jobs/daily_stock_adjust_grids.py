# flake8: noqa
# encoding: UTF-8

# 功能:
# 每日夜盘扫描一次
# 根据账号所在目录下得cta_stock_setting.json，检查所有运行中网格策略实例的持仓合约，
# 如果当日存在除权除息情况，就进行计算更新

# AUTHOR:李来佳
# WeChat/QQ: 28888502
# 广东华富资产管理

import sys, os, copy, csv, json

import sys, os, platform
from datetime import datetime, timedelta
import pandas as pd
import traceback
import matplotlib
import json
from pymongo import *

from datetime import datetime

VNPY_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if VNPY_ROOT not in sys.path:
    print(f'append {VNPY_ROOT} into sys.path')
    sys.path.append(VNPY_ROOT)

os.environ["VNPY_TESTING"] = "1"
from vnpy.trader.utility import load_json, save_json, append_data
from vnpy.data.stock.adjust_factor import download_adjust_factor, get_adjust_factor, get_stock_base
from vnpy.trader.util_wechat import send_wx_msg

if __name__ == "__main__":

    if len(sys.argv) <= 1:
        print(u'请输入:{}目录下的子目录作为参数1'.format(os.path.abspath(os.path.join(VNPY_ROOT, 'prod'))))
        exit()

    print('下载更新所有复权因子')
    download_adjust_factor()

    # 进行报告的账号目录
    account_folder = sys.argv[1]
    account_folder = os.path.abspath(os.path.join(VNPY_ROOT, 'prod', account_folder))

    # 策略实例配置文件
    cta_setting_file = os.path.abspath(os.path.join(account_folder, 'cta_stock_setting.json'))
    # 除权调整记录文件
    adj_record_file = os.path.abspath(os.path.join(account_folder, 'data', 'adj_records.csv'))
    field_names = ['date', 'strategy_name', 'vt_symbol', 'name', 'pre_volume', 'new_volume', 'rate', 'pre_back_adj',
                   'last_back_adj']
    print('开始扫描:{}'.format(cta_setting_file))
    if not os.path.exists(cta_setting_file):
        print(u'不存在策略实例配置文件{}'.format(cta_setting_file), file=sys.stderr)
        exit()

    # 获取所有股票基本信息
    all_symbols = get_stock_base()

    # 读取策略
    cta_settings = []
    with open(cta_setting_file, encoding='UTF-8') as f:
        cta_settings = json.load(f)

    # 逐一策略扫描
    all_margin_usage = 0
    for strategy_name in cta_settings.keys():
        # 获取策略实例配置
        strategy_setting = cta_settings.get(strategy_name)
        # 策略类
        strategy_class = strategy_setting.get("class_name", "")

        setting = strategy_setting.get('setting', {})

        grids = load_json(os.path.abspath(os.path.join(account_folder, 'data', f'{strategy_name}_Grids.json')),
                          auto_save=False)

        changed = False

        margin_usage = 0
        for grid in grids['dn_grids']:
            if not grid['open_status']:
                continue

            vt_symbol = grid['vt_symbol']
            info = all_symbols.get(vt_symbol,{})
            name = info.get('name',vt_symbol)
            factor = get_adjust_factor(vt_symbol)
            if factor is None or len(factor) < 2:
                print(f'没有找到{vt_symbol}的除权因子')
                continue

            # 检查除权日子的最后日期
            last_data = factor[-1]
            pre_data = factor[-2]

            print(f'{vt_symbol}[{name}]复权因子:\n{pre_data} => \n{last_data}')
            dividOperateDate = last_data['dividOperateDate']
            foreAdjustFactor = float(last_data['foreAdjustFactor'])
            adjusted_date = grid['snapshot'].get('adjusted_date', "")
            yd_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

            # 记录的除权执行日期，跟昨日或今日不一样，除权日发生在昨日或今日
            if adjusted_date != dividOperateDate and dividOperateDate >= yd_date and foreAdjustFactor == 1.0:
                adj_rate = last_data.get('backAdjustFactor') / pre_data.get('backAdjustFactor')

                # 当前持仓
                cur_volume = grid['volume']
                # 除权后的股票新数量
                adj_volume = int(cur_volume * adj_rate)
                # 更新数量
                grid['volume'] = adj_volume

                # 更新开仓\平仓\止损价格
                open_price = grid['open_price']
                new_open_price = round(float(open_price * adj_rate), 3)
                close_price = grid['close_price']
                new_close_price = round(float(close_price * adj_rate), 3)
                stop_price = grid['stop_price']
                new_stop_price = round(float(stop_price * adj_rate), 3)

                # 更新执行日期
                grid['snapshot'].update({'adjusted_date': dividOperateDate})
                msg = f'{strategy_name}:{vt_symbol}[{name}]发生除权调整:持仓{cur_volume}=>{adj_volume},' \
                    f'开仓价:{open_price}=>{new_open_price},' \
                    f'平仓价:{close_price}=>{new_close_price},' \
                    f'止损价:{stop_price}=>{new_stop_price}'

                send_wx_msg(msg)
                print(msg)
                append_data(adj_record_file, dict_data={
                    'date': dividOperateDate,
                    'strategy_name': strategy_name,
                    'vt_symbol': vt_symbol,
                    'name': name,
                    'pre_volume': cur_volume,
                    'adj_volume': adj_volume,
                    'rate': adj_rate,
                    'pre_back_adj': pre_data.get('backAdjustFactor'),
                    'last_back_adj': last_data.get('backAdjustFactor')
                })

                changed = True

        if changed:
            print('保存更新后的Grids.json文件')
            save_json(os.path.abspath(os.path.join(account_folder, 'data', f'{strategy_name}_Grids.json')), grids)
