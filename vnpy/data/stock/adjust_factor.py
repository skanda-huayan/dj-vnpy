# flake8: noqa
"""
# 追加/更新股票复权因子
"""

import os
import sys
import json
from typing import Any
from collections import OrderedDict
import pandas as pd

vnpy_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
if vnpy_root not in sys.path:
    sys.path.append(vnpy_root)

os.environ["VNPY_TESTING"] = "1"
import baostock as bs
from vnpy.trader.constant import Exchange
from vnpy.trader.utility import load_json, load_data_from_pkb2, save_data_to_pkb2, extract_vt_symbol
from vnpy.data.tdx.tdx_common import get_stock_type
from vnpy.data.stock.stock_base import get_stock_base
import baostock as bs
import pandas as pd

ADJUST_FACTOR_FILE = 'stock_adjust_factor.pkb2'
# 格式： {vt_symbol: [{dict},{dict}]
#d = {
#  'exchange': exchange.value,  # 证券交易所
#  'code': stock_code,  # 证券代码
#  'name': stock_name,  # 证券中文名称
#  'dividOperateDate': row[1],  # 除权除息日期
#  'foreAdjustFactor': float(row[2]),  # 向前复权因子 除权除息日前一个交易日的收盘价/除权除息日最近的一个交易日的前收盘价
#  'backAdjustFactor': float(row[3]),  # 向后复权因子 除权除息日最近的一个交易日的前收盘价/除权除息日前一个交易日的收盘价
#  'adjustFactor': float(row[4])  # 本次复权因子
#   }

def get_all_adjust_factor():
    """ 获取所有股票复权因子"""
    cache_file_name = os.path.abspath(os.path.join(os.path.dirname(__file__), ADJUST_FACTOR_FILE))

    data = load_data_from_pkb2(cache_file_name)
    if data is None:
        return download_adjust_factor()
    else:
        return data

def get_adjust_factor(vt_symbol: str, stock_name: str = '', need_login: bool = True):
    """
    通过baostock，获取复权因子
    :param vt_symbol:
    :param stock_name:
    :param need_login:
    :return:
    """
    if need_login:
        login_msg = bs.login()
        if login_msg.error_code != '0':
            print(f'证券宝登录错误代码:{login_msg.error_code}, 错误信息:{login_msg.error_msg}')
            return []

    symbol, exchange = extract_vt_symbol(vt_symbol)
    bs_code = '.'.join(['sh' if exchange == Exchange.SSE else 'sz', symbol])

    print(f'开始获取{stock_name} {bs_code}得复权因子')
    rs = bs.query_adjust_factor(
        code=bs_code,
        start_date='2000-01-01'
    )
    if rs.error_code != '0':
        print(f'证券宝获取沪深A股复权因子数据，错误代码:{rs.error_code}, 错误信息:{rs.error_msg}')
        return []

    # [dict] => dataframe

    print(f'返回字段:{rs.fields}')
    result_list = []
    while (rs.error_code == '0') and rs.next():
        row = rs.get_row_data()
        exchange_code, stock_code = row[0].split('.')
        d = {
            'exchange': exchange.value,  # 证券交易所
            'code': stock_code,  # 证券代码
            'name': stock_name,  # 证券中文名称
            'dividOperateDate': row[1],  # 除权除息日期
            'foreAdjustFactor': float(row[2]),  # 向前复权因子 除权除息日前一个交易日的收盘价/除权除息日最近的一个交易日的前收盘价
            'backAdjustFactor': float(row[3]),  # 向后复权因子 除权除息日最近的一个交易日的前收盘价/除权除息日前一个交易日的收盘价
            'adjustFactor': float(row[4])  # 本次复权因子

        }
        result_list.append(d)

        print(f'{d}')
    return result_list


def download_adjust_factor():
    """
    下载更新股票复权因子
    :return:
    """

    # 获取所有股票基础信息
    base_dict = get_stock_base()

    # 尝试从本地缓存获取
    cache_file_name = os.path.abspath(os.path.join(os.path.dirname(__file__), ADJUST_FACTOR_FILE))
    factor_dict = load_data_from_pkb2(cache_file_name)
    if factor_dict is None:
        factor_dict = dict()

    login_msg = bs.login()
    if login_msg.error_code != '0':
        print(f'证券宝登录错误代码:{login_msg.error_code}, 错误信息:{login_msg.error_msg}')
        return None

    for k, v in base_dict.items():
        if v.get('类型') != '股票':
            continue

        factor_list = get_adjust_factor(vt_symbol=k, stock_name=v.get('name'), need_login=False)

        if len(factor_list) > 0:
            factor_dict.update({k: factor_list})

    if len(factor_dict) > 0:
        save_data_to_pkb2(factor_dict, cache_file_name)
        print(f'保存除权除息至文件:{cache_file_name}')

    return factor_dict

if __name__ == '__main__':
    download_adjust_factor()
