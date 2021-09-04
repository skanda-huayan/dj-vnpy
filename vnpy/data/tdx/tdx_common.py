# encoding: UTF-8

import sys
import os
import pickle
import bz2
from functools import lru_cache
from logging import INFO, ERROR
from vnpy.trader.utility import load_json, save_json

# 期货的配置文件
TDX_FUTURE_CONFIG = 'tdx_future_config.json'
# 股票的配置文件
# 存储格式 dict{
#   "cache_time": datetime,
#   "symbol_dict": {
#       "symbol_marketid": {
#           'code', '395001',
#           'volunit', 100,
#           'decimal_point', 2,
#           'name', '主板Ａ股',
#           'pre_close', 458.0,
#           'exchagne','SZSE',
#           'stock_type', 'index_cn',
#           'market_id', 0
#           }
#    } }
TDX_STOCK_CONFIG = 'tdx_stock_config.pkb2'

TDX_PROXY_CONFIG = 'tdx_proxy_config.json'


@lru_cache()
def get_tdx_market_code(code):
    # 获取通达信股票的market code
    code = str(code)
    if code[0] in ['5', '6', '9'] or code[:3] in ["880","009", "126", "110", "201", "202", "203", "204"]:
        # 上海证券交易所
        return 1
    # 深圳证券交易所
    return 0


# 通达信 K 线种类
# 0 -   5 分钟K 线
# 1 -   15 分钟K 线
# 2 -   30 分钟K 线
# 3 -   1 小时K 线
# 4 -   日K 线
# 5 -   周K 线
# 6 -   月K 线
# 7 -   1 分钟
# 8 -   1 分钟K 线
# 9 -   日K 线
# 10 -  季K 线
# 11 -  年K 线
PERIOD_MAPPING = {}
PERIOD_MAPPING['1min'] = 8
PERIOD_MAPPING['5min'] = 0
PERIOD_MAPPING['15min'] = 1
PERIOD_MAPPING['30min'] = 2
PERIOD_MAPPING['1hour'] = 3
PERIOD_MAPPING['1day'] = 4
PERIOD_MAPPING['1week'] = 5
PERIOD_MAPPING['1month'] = 6

# 期货行情服务器清单
TDX_FUTURE_HOSTS =[
     {'ip': '42.193.151.197', 'port': 7727, 'name': '广州期货双线1', 'speed': 6.622},
     {'ip': '119.29.63.178', 'port': 7727, 'name': '广州期货双线3', 'speed': 7.716},
     {'ip': '81.71.76.101', 'port': 7727, 'name': '广州期货双线2', 'speed': 14.914},
     {'ip': '47.107.75.159', 'port': 7727, 'name': '扩展市场深圳双线3', 'speed': 34.542},
     {'ip': '112.74.214.43', 'port': 7727, 'name': '扩展市场深圳双线1', 'speed': 37.881},
     {'ip': '59.175.238.38', 'port': 7727, 'name': '扩展市场武汉主站3', 'speed': 49.63},
     {'ip': '119.97.185.5', 'port': 7727, 'name': '扩展市场武汉主站1', 'speed': 70.563},
     {'ip': '218.80.248.229', 'port': 7721, 'name': '备用服务器1', 'speed': 86.91300000000001},
     {'ip': '119.97.185.7', 'port': 7727, 'name': '港股期货武汉主站1', 'speed': 101.06099999999999},
     {'ip': '106.14.95.149', 'port': 7727, 'name': '扩展市场上海双线', 'speed': 105.294},
     {'ip': '113.105.142.136', 'port': 443, 'name': '扩展市场东莞主站', 'speed': 10000.0},
     {'ip': '113.105.142.133', 'port': 443, 'name': '港股期货东莞电信', 'speed': 10000.0},
     {'ip': '119.97.185.9', 'port': 7727, 'name': '港股期货武汉主站2', 'speed': 10000.0},
     {'ip': '202.103.36.71', 'port': 443, 'name': '扩展市场武汉主站2', 'speed': 10000.0},
     {'ip': '47.92.127.181', 'port': 7727, 'name': '扩展市场北京主站', 'speed': 10000.0},
     {'ip': '124.74.236.94', 'port': 7721, 'name': '备用服务器2', 'speed': 10000.0},
     {'ip': '58.246.109.27', 'port': 7721, 'name': '备用服务器3', 'speed': 10000.0}
]


def get_future_contracts():
    """获取期货合约信息"""
    return get_cache_json('future_contracts.json')


def save_future_contracts(future_contracts_dict: dict):
    """保存期货合约信息"""
    save_cache_json(future_contracts_dict, 'future_contracts.json')

def get_cache_config(config_file_name):
    """获取本地缓存的配置地址信息"""
    config_file_name = os.path.abspath(os.path.join(os.path.dirname(__file__), config_file_name))
    config = {}
    if not os.path.exists(config_file_name):
        return config
    try:
        with bz2.BZ2File(config_file_name, 'rb') as f:
            config = pickle.load(f)
            return config
    except Exception as ex:
        print(f'读取缓存本地文件:{config_file_name}异常{str(ex)}')
        return config

def save_cache_config(data: dict, config_file_name):
    """保存本地缓存的配置地址信息"""
    config_file_name = os.path.abspath(os.path.join(os.path.dirname(__file__), config_file_name))

    with bz2.BZ2File(config_file_name, 'wb') as f:
        pickle.dump(data, f)


def get_cache_json(json_file_name: str):
    """获取本地缓存的json配置信息"""
    config_file_name = os.path.abspath(os.path.join(os.path.dirname(__file__), json_file_name))
    return load_json(config_file_name)


def save_cache_json(data_dict: dict, json_file_name: str):
    """保存本地缓存的JSON配置信息"""
    config_file_name = os.path.abspath(os.path.join(os.path.dirname(__file__), json_file_name))
    save_json(filename=config_file_name, data=data_dict)


def get_stock_type(code,market_id = None ):
    """获取股票得分类"""
    if market_id is None:
        market_id = get_tdx_market_code(code)

    if market_id == 0:
        return get_stock_type_sz(code)
    else:
        return get_stock_type_sh(code)


def get_stock_type_sz(code):
    """深市代码分类
    Arguments:
        code {[type]} -- [description]
    Returns:
        [type] -- [description]
    """

    if str(code)[0:2] in ['00', '30', '02']:
        return 'stock_cn'
    elif str(code)[0:2] in ['39']:
        return 'index_cn'
    elif str(code)[0:2] in ['15']:
        return 'etf_cn'
    elif str(code)[0:2] in ['10', '11', '13']:
        # 10xxxx 国债现货
        # 11xxxx 债券
        # 12xxxx 国债回购
        return 'bond_cn'
    elif str(code)[0:2] in ['12']:
        # 12xxxx 可转换债券
        return 'cb_cn'

    elif str(code)[0:2] in ['20']:
        return 'stockB_cn'
    else:
        return 'undefined'


def get_stock_type_sh(code):
    if str(code)[0] == '6':
        return 'stock_cn'
    elif str(code)[0:3] in ['000', '880']:
        return 'index_cn'
    elif str(code)[0:2] == '51':
        return 'etf_cn'
    # 110×××120×××企业债券；
    # 129×××100×××可转换债券；
    elif str(code)[0:3] in ["009", "112", '120', "132", "204"]:
        return 'bond_cn'

    elif str(code)[0:3] in ["110", "113", "121", "122", "126",
                            "130", "181", "190", "191", "192", "201", "202", "203"]:
        return 'cb_cn'
    else:
        return 'undefined'


class FakeStrategy(object):
    """制作一个假得策略，用于测试"""

    def write_log(self, content, level=INFO):
        if level == INFO:
            print(content)
        else:
            print(content, file=sys.stderr)

    def write_error(self, content):

        self.write_log(content, level=ERROR)

    def display_bar(self, bar, bar_is_completed=True, freq=1):
        print(u'{} {}'.format(bar.vt_symbol, bar.datetime))
