# 恒投交易客户端 文件接口
# 1. 支持csv/dbf文件的读写
# 2. 采用tdx作为行情数据源
# 华富资产 李来佳 28888502

import os
import sys
import copy
import csv
import dbf
import traceback
import pandas as pd
from typing import Any, Dict, List
from datetime import datetime, timedelta
from time import sleep
from functools import lru_cache
from collections import OrderedDict
from multiprocessing.dummy import Pool
from threading import Thread

from pytdx.hq import TdxHq_API
from pytdx.config.hosts import hq_hosts
from pytdx.params import TDXParams

from vnpy.event import EventEngine
from vnpy.trader.event import EVENT_TIMER
from vnpy.trader.constant import (
    Exchange,
    Product,
    Direction,
    OrderType,
    Status,
    Offset,
    Interval
)
from vnpy.trader.gateway import BaseGateway, LocalOrderManager
from vnpy.trader.object import (
    BarData,
    CancelRequest,
    OrderRequest,
    SubscribeRequest,
    TickData,
    ContractData,
    OrderData,
    TradeData,
    PositionData,
    AccountData,
    HistoryRequest
)
from vnpy.trader.utility import get_folder_path, print_dict, extract_vt_symbol, get_stock_exchange, append_data
from vnpy.data.tdx.tdx_common import get_stock_type_sz, get_stock_type_sh

# 通达信股票行情
from vnpy.data.tdx.tdx_common import get_cache_config, get_tdx_market_code

# 代码 <=> 中文名称
symbol_name_map: Dict[str, str] = {}
# 代码 <=> 交易所
symbol_exchange_map: Dict[str, Exchange] = {}

# 时间戳对齐
TIME_GAP = 8 * 60 * 60 * 1000000000
INTERVAL_VT2TQ = {
    Interval.MINUTE: 60,
    Interval.HOUR: 60 * 60,
    Interval.DAILY: 60 * 60 * 24,
}

# 功能<->文件对应
PB_FILE_NAMES = {
    'send_order': 'XHPT_WT',  # 通用接口_委托
    'cancel_order': 'XHPT_CD',  # 通用接口_撤单
    'update_orders': 'XHPT_WTCX',  # 通用接口_委托查询
    'update_trades': 'XHPT_CJCX',  # 通用接口_成交查询
    'positions': 'CC_STOCK_',  # 持仓明细
    'orders': 'WT_STOCK_',  # 当日委托明细
    'trades': 'CJ_STOCK_',  # 当日成交明细
    'accounts': 'ZJ_STOCK_'  # 资金
}

SEND_ORDER_FIELDS = OrderedDict({
    "CPBH": "C32",  # 产品代码/基金代码 <-- 输入参数 -->
    "ZCDYBH": "C16",  # 单元编号/组合编号
    "ZHBH": "C16",  # 组合编号
    "GDDM": "C20",  # 股东代码
    "JYSC": "C3",  # 交易市场
    "ZQDM": "C16",  # 证券代码
    "WTFX": "C4",  # 委托方向
    "WTJGLX": "C1",  # 委托价格类型
    "WTJG": "N11.4",  # 委托价格
    "WTSL": "N12",  # 委托数量
    "WBZDYXH": "N9",  # 第三方系统自定义号
    "WTXH": "N8",  # 委托序号  <-- 输出参数  -->
    "WTSBDM": "N8",  # 委托失败代码
    "SBYY": "C254",  # 失败原因
    "CLBZ": "C1",  # 处理标志  <-- 内部自用字段 -->
    "BYZD": "C2",  # 备用字段
    "WTJE": "N16.2",  # 委托金额  <-- 扩充参数 -->
    "TSBS": "C64",  # 特殊标识
    "YWBS": "C2",  # 业务标识
})

# 撤单csv字段格式定义
CANCEL_ORDER_FIELDS = OrderedDict({
    "WTXH": "N8",  # 委托序号
    "JYSC": "C3",  # 交易市场
    "ZQDM": "C16",  # 证券代码
    "CDCGBZ": "C1",  # 撤单成功标志
    "SBYY": "C254",  # 失败原因
    "CLBZ": "C1",  # 处理标志
    "BYZD": "C2",  # 备用字段
    "BYZD2": "C16",  # 备用字段2
})

# 通用接口_委托查询
UPDATE_ORDER_FIELDS = OrderedDict({
    "WTRQ": "N8",  # 委托日期
    "WTSJ": "N6",  # 委托时间
    "WTXH": "N8",  # 委托序号
    "WBZDYXH": "N9",  # 第三方系统自定义号
    "CPBH": "C32",  # 产品(账户)编号
    "ZCDYBH": "C16",  # 资产单元编号
    "ZHBH": "C16",  # 组合编号
    "GDDM": "C20",  # 股东代码
    "JYSC": "C3",  # 交易市场
    "ZQDM": "C16",  # 证券代码
    "WTFX": "C4",  # 委托方向
    "WTJGLX": "C1",  # 委托价格类型
    "WTJG": "N11.4",  # 委托价格
    "WTSL": "N12",  # 委托数量
    "YMDJJE": "N16.2",  # 预买冻结金额
    "YMSRJE": "N16.2",  # 预卖收入金额
    "WTZT": "C1",  # 委托状态
    "WTCCSL": "N12",  # 委托撤成数量
    "FDYY": "C254",  # 废单原因
    "JYSSBBH": "C64",  # 交易所申报编号
    "CLBZ": "C1",  # 处理标志
    "BYZD": "C2",  # 备用字段
    "WTJE": "N16.2",  # 委托金额
    "TSBS": "C64",  # 特殊标识
})

# 通用接口_成交查询
UPDATE_TRADE_FIELDS = OrderedDict({
    "CJRQ": "N8",  # 成交日期
    "CJBH": "C64",  # 成交序号
    "WTXH": "N8",  # 委托序号
    "WBZDYXH": "N9",  # 第三方系统自定义号
    "CPBH": "C32",  # 产品(账户)编号
    "ZCDYBH": "C16",  # 资产单元编号
    "ZHBH": "C16",  # 组合编号
    "GDDM": "C20",  # 股东代码
    "JYSC": "C3",  # 交易市场
    "ZQDM": "C16",  # 证券代码
    "WTFX": "C4",  # 委托方向
    "CJSL": "N16",  # 成交数量
    "CJJG": "N11.4",  # 成交价格
    "CJJE": "N16.2",  # 成交金额
    "ZFY": "N16.2",  # 总费用
    "CJSJ": "N6",  # 成交时间
    "CLBZ": "C1",  # 处理标志
    "BYZD": "C2",  # 备用字段
    "TSBS": "C64",  # 特殊标识
    "JYSCJBH": "C64",  # 成交编号
})
# 交易所id <=> Exchange
EXCHANGE_PB2VT: Dict[str, Exchange] = {
    "1": Exchange.SSE,
    "2": Exchange.SZSE,
    "3": Exchange.SHFE,
    "4": Exchange.CZCE,
    "7": Exchange.CFFEX,
    "9": Exchange.DCE,
    "k": Exchange.INE
}
EXCHANGE_VT2PB: Dict[Exchange, str] = {v: k for k, v in EXCHANGE_PB2VT.items()}
EXCHANGE_NAME2VT: Dict[str, Exchange] = {
    "上交所A": Exchange.SSE,
    "深交所A": Exchange.SZSE
}

# 方向  <=> Direction, Offset
DIRECTION_STOCK_PB2VT: Dict[str, Any] = {
    "1": (Direction.LONG, Offset.NONE),  # 买
    "2": (Direction.SHORT, Offset.NONE),  # 卖
    "V": (Direction.LONG, Offset.OPEN),  # 多，开
    "X": (Direction.SHORT, Offset.OPEN),  # 空，开
    "Y": (Direction.LONG, Offset.CLOSE),  # 多，平
    "W": (Direction.SHORT, Offset.CLOSE)  # 空， 平
}
DIRECTION_STOCK_VT2PB: Dict[Any, str] = {v: k for k, v in DIRECTION_STOCK_PB2VT.items()}
DIRECTION_STOCK_NAME2VT: Dict[str, Any] = {
    "卖出": Direction.SHORT,
    "买入": Direction.LONG,
    "债券买入": Direction.LONG,
    "债券卖出": Direction.SHORT,
    "申购": Direction.LONG
}

DIRECTION_ORDER_PB2VT: Dict[str, Any] = {
    "1": Direction.LONG,
    "2": Direction.SHORT,
    "3": Direction.LONG,
    "4": Direction.SHORT
}

# 持仓方向 <=> Direction
POSITION_DIRECTION_PB2VT = {
    "1": Direction.LONG,
    "2": Direction.SHORT,
}

# 委托单类型
ORDERTYPE_PB2VT: Dict[str, OrderType] = {
    "0": OrderType.LIMIT,  # 限价单
    "a": OrderType.MARKET,  # 五档即成剩撤（上交所市价）
    "b": OrderType.MARKET,  # 五档即成剩转（上交所市价）
    "A": OrderType.MARKET,  # 五档即成剩撤（深交所市价）
    "C": OrderType.MARKET,  # 即成剩撤（深交所市价）
    "D": OrderType.MARKET,  # 对手方最优（深交所市价，上交所科创板市价）
    "E": OrderType.MARKET,  # 本方最优（深交所市价，上交所科创板市价）
}


def format_dict(d, dict_define):
    """根据dict格式定义进行value转换"""

    for k in dict_define.keys():
        # 原值
        v = d.get(k, '')
        # 目标转换格式
        v_format = dict_define.get(k, None)
        if v_format is None:
            continue
        if 'C' in v_format:
            str_len = int(v_format.replace('C', ''))
            new_v = '{}{}'.format(' ' * (str_len - len(v)), v)
            d.update({k: new_v})
            continue
        elif "N" in v_format:
            v_format = v_format.replace('N', '')
            if '.' in v_format:
                int_len, float_len = v_format.split('.')
                int_len = int(int_len)
                float_len = int(float_len)
                str_v = str(v)
                new_v = '{}{}'.format(' ' * (int_len - len(str_v)), str_v)
            else:
                int_len = int(v_format)
                str_v = str(v)
                new_v = '{}{}'.format(' ' * (int_len - len(str_v)), str_v)
            d.update({k: new_v})

    return d


def get_pb_order_type(exchange, order_type):
    """获取pb的委托类型"""
    # 限价单
    if order_type == OrderType.LIMIT:
        return "0"
    # 市价单
    if exchange == Exchange.SSE:
        return "a"

    if exchange == Exchange.SZSE:
        return "C"

    return "0"


ORDERTYPE_NAME2VT: Dict[str, OrderType] = {
    "五档即成剩撤": OrderType.MARKET,
    "五档即成剩转": OrderType.MARKET,
    "即成剩撤": OrderType.MARKET,
    "对手方最优": OrderType.MARKET,
    "本方最优": OrderType.MARKET,
    "限价单": OrderType.LIMIT,
}

STATUS_NAME2VT: Dict[str, Status] = {
    "未报": Status.SUBMITTING,
    "待报": Status.SUBMITTING,
    "正报": Status.SUBMITTING,
    "已报": Status.NOTTRADED,
    "废单": Status.REJECTED,
    "部成": Status.PARTTRADED,
    "已成": Status.ALLTRADED,
    "部撤": Status.CANCELLED,
    "已撤": Status.CANCELLED,
    "待撤": Status.CANCELLING,
    "未审批": Status.UNKNOWN,
    "审批拒绝": Status.UNKNOWN,
    "未审批即撤销": Status.UNKNOWN,
}

STATUS_PB2VT: Dict[str, Status] = {
    "1": Status.SUBMITTING,
    "2": Status.SUBMITTING,
    "3": Status.SUBMITTING,
    "4": Status.NOTTRADED,
    "5": Status.REJECTED,
    "6": Status.PARTTRADED,
    "7": Status.ALLTRADED,
    "8": Status.CANCELLED,
    "9": Status.CANCELLED,
    "a": Status.CANCELLING,
    "b": Status.UNKNOWN,
    "c": Status.UNKNOWN,
    "d": Status.UNKNOWN,
}

STOCK_CONFIG_FILE = 'tdx_stock_config.pkb2'


class PbGateway(BaseGateway):
    default_setting: Dict[str, Any] = {
        "资金账号": "",
        "数据目录": "",
        "产品编号": "",
        "单元编号": "",
        "股东代码_沪": "",
        "股东代码_深": "",
        "文件格式": "dbf",
        "导出子目录": "数据导出",
        "pb版本": "2018"
    }

    # 接口支持得交易所清单
    exchanges: List[Exchange] = list(EXCHANGE_VT2PB.keys())

    def __init__(self, event_engine: EventEngine, gateway_name='PB'):
        """"""
        super().__init__(event_engine, gateway_name=gateway_name)
        self.connect_time = datetime.now().strftime("%H%M")
        self.order_manager = LocalOrderManager(self, self.connect_time, 4)

        self.md_api = PbMdApi(self)
        self.td_api = PbTdApi(self)
        self.tq_api = None

        self.tdx_connected = False  # 通达信行情API得连接状态

        self.file_type = 'dbf'
        self.pb_version = '2018'

    def connect(self, setting: dict) -> None:
        """"""
        userid = setting["资金账号"]
        csv_folder = setting["数据目录"]
        product_id = setting["产品编号"]
        unit_id = setting["单元编号"]
        holder_ids = {
            Exchange.SSE: setting["股东代码_沪"],
            Exchange.SZSE: setting["股东代码_深"]
        }
        self.file_type = setting.get('文件格式', 'dbf')
        self.pb_version = setting.get('pb版本', '2018')

        # 2019版，导出目录，自动增加一个‘数据导出’的子文件夹
        # 2018版，导出目录，无自动增加的子目录
        export_sub_folder = setting.get('导出子目录', '数据导出')
        if len(export_sub_folder) > 0:
            # 2019款
            export_folder = os.path.abspath(os.path.join(csv_folder, export_sub_folder))
        else:
            # 2018款
            export_folder = csv_folder

        self.md_api.connect()
        self.td_api.connect(user_id=userid,
                            order_folder=csv_folder,
                            account_folder=export_folder,
                            product_id=product_id,
                            unit_id=unit_id,
                            holder_ids=holder_ids)
        # self.tq_api = TqMdApi(self)
        # self.tq_api.connect()
        self.init_query()

    def close(self) -> None:
        """"""
        self.md_api.close()
        self.td_api.close()

    def subscribe(self, req: SubscribeRequest) -> None:
        """"""
        if self.tq_api and self.tq_api.is_connected:
            self.tq_api.subscribe(req)
        else:
            self.md_api.subscribe(req)

    def send_order(self, req: OrderRequest) -> str:
        """"""
        return self.td_api.send_order(req)

    def cancel_order(self, req: CancelRequest) -> None:
        """"""
        self.td_api.cancel_order(req)

    def query_account(self) -> None:
        """"""
        self.td_api.query_account()

    def query_position(self) -> None:
        """"""
        self.td_api.query_position()

    def query_orders(self) -> None:
        self.td_api.query_orders()

    def query_trades(self) -> None:
        self.td_api.query_trades()

    def process_timer_event(self, event) -> None:
        """"""
        self.count += 1
        if self.count < 2:
            return
        self.count = 0

        func = self.query_functions.pop(0)
        func()
        self.query_functions.append(func)

    def init_query(self) -> None:
        """"""
        self.count = 0
        self.query_functions = [self.query_account, self.query_position]
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)


class PbMdApi(object):

    def __init__(self, gateway: PbGateway):
        """"""
        super().__init__()

        self.gateway: PbGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.connect_status: bool = False
        self.login_status: bool = False

        self.req_interval = 0.5  # 操作请求间隔500毫秒
        self.req_id = 0  # 操作请求编号
        self.connection_status = False  # 连接状态

        self.symbol_exchange_dict = {}  # tdx合约与vn交易所的字典
        self.symbol_market_dict = {}  # tdx合约与tdx市场的字典
        self.symbol_vn_dict = {}  # tdx合约与vtSymbol的对应
        self.symbol_tick_dict = {}  # tdx合约与最后一个Tick得字典
        self.registed_symbol_set = set()

        self.config = get_cache_config(STOCK_CONFIG_FILE)
        self.symbol_dict = self.config.get('symbol_dict', {})
        self.cache_time = self.config.get('cache_time', datetime.now() - timedelta(days=7))
        self.commission_dict = {}
        self.contract_dict = {}

        # self.queue = Queue()            # 请求队列
        self.pool = None  # 线程池
        # self.req_thread = None          # 定时器线程

        # copy.copy(hq_hosts)

        self.ip_list = [{'ip': "180.153.18.170", 'port': 7709},
                        {'ip': "180.153.18.171", 'port': 7709},
                        {'ip': "180.153.18.172", 'port': 80},
                        {'ip': "202.108.253.130", 'port': 7709},
                        {'ip': "202.108.253.131", 'port': 7709},
                        {'ip': "202.108.253.139", 'port': 80},
                        {'ip': "60.191.117.167", 'port': 7709},
                        {'ip': "115.238.56.198", 'port': 7709},
                        {'ip': "218.75.126.9", 'port': 7709},
                        {'ip': "115.238.90.165", 'port': 7709},
                        {'ip': "124.160.88.183", 'port': 7709},
                        {'ip': "60.12.136.250", 'port': 7709},
                        {'ip': "218.108.98.244", 'port': 7709},
                        # {'ip': "218.108.47.69", 'port': 7709},
                        {'ip': "114.80.63.12", 'port': 7709},
                        {'ip': "114.80.63.35", 'port': 7709},
                        {'ip': "180.153.39.51", 'port': 7709},
                        # {'ip': '14.215.128.18', 'port': 7709},
                        # {'ip': '59.173.18.140', 'port': 7709}
                        ]

        self.best_ip = {'ip': None, 'port': None}
        self.api_dict = {}  # API 的连接会话对象字典
        self.last_tick_dt = {}  # 记录该会话对象的最后一个tick时间

        self.security_count = 50000

        # 股票code name列表
        self.stock_codelist = None

    def ping(self, ip, port=7709):
        """
        ping行情服务器
        :param ip:
        :param port:
        :param type_:
        :return:
        """
        apix = TdxHq_API()
        __time1 = datetime.now()
        try:
            with apix.connect(ip, port):
                if apix.get_security_count(TDXParams.MARKET_SZ) > 9000:  # 0：深市 股票数量 = 9260
                    _timestamp = datetime.now() - __time1
                    self.gateway.write_log('服务器{}:{},耗时:{}'.format(ip, port, _timestamp))
                    return _timestamp
                else:
                    self.gateway.write_log(u'该服务器IP {}无响应'.format(ip))
                    return timedelta(9, 9, 0)
        except:
            self.gateway.write_error(u'tdx ping服务器，异常的响应{}'.format(ip))
            return timedelta(9, 9, 0)

    def select_best_ip(self):
        """
        选择行情服务器
        :return:
        """
        self.gateway.write_log(u'选择通达信股票行情服务器')
        data_future = [self.ping(x.get('ip'), x.get('port')) for x in self.ip_list]

        best_future_ip = self.ip_list[data_future.index(min(data_future))]

        self.gateway.write_log(u'选取 {}:{}'.format(
            best_future_ip['ip'], best_future_ip['port']))
        return best_future_ip

    def connect(self, n=3):
        """
        连接通达讯行情服务器
        :param n:
        :return:
        """
        if self.connection_status:
            for api in self.api_dict:
                if api is not None or getattr(api, "client", None) is not None:
                    self.gateway.write_log(u'当前已经连接,不需要重新连接')
                    return

        self.gateway.write_log(u'开始通达信行情服务器')

        if len(self.symbol_dict) == 0:
            self.gateway.write_error(f'本地没有股票信息的缓存配置文件')
        else:
            self.cov_contracts()

        # 选取最佳服务器
        if self.best_ip['ip'] is None and self.best_ip['port'] is None:
            self.best_ip = self.select_best_ip()

        # 创建n个api连接对象实例
        for i in range(n):
            try:
                api = TdxHq_API(heartbeat=True, auto_retry=True, raise_exception=True)
                api.connect(self.best_ip['ip'], self.best_ip['port'])
                # 尝试获取市场合约统计
                c = api.get_security_count(TDXParams.MARKET_SZ)
                if c is None or c < 10:
                    err_msg = u'该服务器IP {}/{}无响应'.format(self.best_ip['ip'], self.best_ip['port'])
                    self.gateway.write_error(err_msg)
                else:
                    self.gateway.write_log(u'创建第{}个tdx连接'.format(i + 1))
                    self.api_dict[i] = api
                    self.last_tick_dt[i] = datetime.now()
                    self.connection_status = True
                    self.security_count = c

                    # if len(symbol_name_map) == 0:
                    #    self.get_stock_list()

            except Exception as ex:
                self.gateway.write_error(u'连接服务器tdx[{}]异常:{},{}'.format(i, str(ex), traceback.format_exc()))
                return

        # 创建连接池，每个连接都调用run方法
        self.pool = Pool(n)
        self.pool.map_async(self.run, range(n))

        # 设置上层的连接状态
        self.gateway.tdxConnected = True

    def reconnect(self, i):
        """
        重连
        :param i:
        :return:
        """
        try:
            self.best_ip = self.select_best_ip()
            api = TdxHq_API(heartbeat=True, auto_retry=True)
            api.connect(self.best_ip['ip'], self.best_ip['port'])
            # 尝试获取市场合约统计
            c = api.get_security_count(TDXParams.MARKET_SZ)
            if c is None or c < 10:
                err_msg = u'该服务器IP {}/{}无响应'.format(self.best_ip['ip'], self.best_ip['port'])
                self.gateway.write_error(err_msg)
            else:
                self.gateway.write_log(u'重新创建第{}个tdx连接'.format(i + 1))
                self.api_dict[i] = api

            sleep(1)
        except Exception as ex:
            self.gateway.write_error(u'重新连接服务器tdx[{}]异常:{},{}'.format(i, str(ex), traceback.format_exc()))
            return

    def close(self):
        """退出API"""
        self.connection_status = False

        # 设置上层的连接状态
        self.gateway.tdxConnected = False

        if self.pool is not None:
            self.pool.close()
            self.pool.join()

    def subscribe(self, req):
        """订阅合约"""
        # 这里的设计是，如果尚未登录就调用了订阅方法
        # 则先保存订阅请求，登录完成后会自动订阅
        vn_symbol = str(req.symbol)
        if '.' in vn_symbol:
            vn_symbol = vn_symbol.split('.')[0]

        self.gateway.write_log(u'通达信行情订阅 {}'.format(str(vn_symbol)))

        tdx_symbol = vn_symbol  # [0:-2] + 'L9'
        tdx_symbol = tdx_symbol.upper()
        self.gateway.write_log(u'{}=>{}'.format(vn_symbol, tdx_symbol))
        self.symbol_vn_dict[tdx_symbol] = vn_symbol

        if tdx_symbol not in self.registed_symbol_set:
            self.registed_symbol_set.add(tdx_symbol)

        # 查询股票信息
        self.qry_instrument(vn_symbol)

        self.check_status()

    def check_status(self):
        # self.gateway.write_log(u'检查tdx接口状态')
        if len(self.registed_symbol_set) == 0:
            return True

        # 若还没有启动连接，就启动连接
        over_time = [((datetime.now() - dt).total_seconds() > 60) for dt in self.last_tick_dt.values()]
        if not self.connection_status or len(self.api_dict) == 0 or any(over_time):
            self.gateway.write_log(u'tdx还没有启动连接，就启动连接')
            self.close()
            self.pool = None
            self.api_dict = {}
            pool_cout = getattr(self.gateway, 'tdx_pool_count', 3)
            self.connect(pool_cout)

        # self.gateway.write_log(u'tdx接口状态正常')

    def qry_instrument(self, symbol):
        """
        查询/更新股票信息
        :return:
        """
        if not self.connection_status:
            return

        api = self.api_dict.get(0)
        if api is None:
            self.gateway.write_log(u'取不到api连接，更新合约信息失败')
            return

        # TODO： 取得股票的中文名
        market_code = get_tdx_market_code(symbol)
        api.to_df(api.get_finance_info(market_code, symbol))

        # 如果有预定的订阅合约，提前订阅
        # if len(all_contacts) > 0:
        #     cur_folder =  os.path.dirname(__file__)
        #     export_file = os.path.join(cur_folder,'contracts.csv')
        #     if not os.path.exists(export_file):
        #         df = pd.DataFrame(all_contacts)
        #         df.to_csv(export_file)

    def cov_contracts(self):
        """转换本地缓存=》合约信息推送"""
        for symbol_marketid, info in self.symbol_dict.items():
            symbol, market_id = symbol_marketid.split('_')
            exchange = info.get('exchange', '')
            if len(exchange) == 0:
                continue

            vn_exchange_str = get_stock_exchange(symbol)
            if exchange != vn_exchange_str:
                continue
            exchange = Exchange(exchange)

            if info['stock_type'] == 'stock_cn':
                product = Product.EQUITY
            elif info['stock_type'] in ['bond_cn', 'cb_cn']:
                product = Product.BOND
            elif info['stock_type'] == 'index_cn':
                product = Product.INDEX
            elif info['stock_type'] == 'etf_cn':
                product = Product.ETF
            else:
                product = Product.EQUITY

            volume_tick = info['volunit']
            if symbol.startswith('688'):
                volume_tick = 200

            contract = ContractData(
                gateway_name=self.gateway_name,
                symbol=symbol,
                exchange=exchange,
                name=info['name'],
                product=product,
                pricetick=round(0.1 ** info['decimal_point'], info['decimal_point']),
                size=1,
                min_volume=volume_tick,
                margin_rate=1
            )

            if product != Product.INDEX:
                # 缓存 合约 =》 中文名
                symbol_name_map.update({contract.symbol: contract.name})

                # 缓存代码和交易所的印射关系
                symbol_exchange_map[contract.symbol] = contract.exchange

                self.contract_dict.update({contract.symbol: contract})
                self.contract_dict.update({contract.vt_symbol: contract})
                # 推送
                self.gateway.on_contract(contract)

    def get_stock_list(self):
        """股票所有的code&name列表"""

        api = self.api_dict.get(0)
        if api is None:
            self.gateway.write_log(u'取不到api连接，更新合约信息失败')
            return None

        self.gateway.write_log(f'查询所有的股票信息')

        data = pd.concat(
            [pd.concat([api.to_df(api.get_security_list(j, i * 1000)).assign(sse='sz' if j == 0 else 'sh').set_index(
                ['code', 'sse'], drop=False) for i in range(int(api.get_security_count(j) / 1000) + 1)], axis=0) for j
                in range(2)], axis=0)
        sz = data.query('sse=="sz"')
        sh = data.query('sse=="sh"')
        sz = sz.assign(sec=sz.code.apply(get_stock_type_sz))
        sh = sh.assign(sec=sh.code.apply(get_stock_type_sh))

        temp_df = pd.concat([sz, sh]).query('sec in ["stock_cn","etf_cn","bond_cn","cb_cn"]').sort_index().assign(
            name=data['name'].apply(lambda x: str(x)[0:6]))
        hq_codelist = temp_df.loc[:, ['code', 'name']].set_index(['code'], drop=False)

        for i in range(0, len(temp_df)):
            row = temp_df.iloc[i]
            if row['sec'] == 'etf_cn':
                product = Product.ETF
            elif row['sec'] in ['bond_cn', 'cb_cn']:
                product = Product.BOND
            else:
                product = Product.EQUITY

            volume_tick = 100 if product != Product.BOND else 10
            if row['code'].startswith('688'):
                volume_tick = 200

            contract = ContractData(
                gateway_name=self.gateway_name,
                symbol=row['code'],
                exchange=Exchange.SSE if row['sse'] == 'sh' else Exchange.SZSE,
                name=row['name'],
                product=product,
                pricetick=round(0.1 ** row['decimal_point'], row['decimal_point']),
                size=1,
                min_volume=volume_tick,
                margin_rate=1

            )
            # 缓存 合约 =》 中文名
            symbol_name_map.update({contract.symbol: contract.name})

            # 缓存代码和交易所的印射关系
            symbol_exchange_map[contract.symbol] = contract.exchange

            self.contract_dict.update({contract.symbol: contract})
            self.contract_dict.update({contract.vt_symbol: contract})
            # 推送
            self.gateway.on_contract(contract)

        return hq_codelist

    def run(self, i):
        """
        版本1：Pool内得线程，持续运行,每个线程从queue中获取一个请求并处理
        版本2：Pool内线程，从订阅合约集合中，取出符合自己下标 mode n = 0的合约，并发送请求
        :param i:
        :return:
        """
        # 版本2：
        try:
            api_count = len(self.api_dict)
            last_dt = datetime.now()
            self.gateway.write_log(u'开始运行tdx[{}],{}'.format(i, last_dt))
            while self.connection_status:
                symbols = set()
                for idx, tdx_symbol in enumerate(list(self.registed_symbol_set)):
                    # self.gateway.write_log(u'tdx[{}], api_count:{}, idx:{}, tdx_symbol:{}'.format(i, api_count, idx, tdx_symbol))
                    if idx % api_count == i:
                        try:
                            symbols.add(tdx_symbol)
                            self.processReq(tdx_symbol, i)
                        except BrokenPipeError as bex:
                            self.gateway.write_error(u'BrokenPipeError{},重试重连tdx[{}]'.format(str(bex), i))
                            self.reconnect(i)
                            sleep(5)
                            break
                        except Exception as ex:
                            self.gateway.write_error(
                                u'tdx[{}] exception:{},{}'.format(i, str(ex), traceback.format_exc()))

                            # api = self.api_dict.get(i,None)
                            # if api is None or getattr(api,'client') is None:
                            self.gateway.write_error(u'重试重连tdx[{}]'.format(i))
                            print(u'重试重连tdx[{}]'.format(i), file=sys.stderr)
                            self.reconnect(i)

                # self.gateway.write_log(u'tdx[{}] sleep'.format(i))
                sleep(self.req_interval)
                dt = datetime.now()
                if last_dt.minute != dt.minute:
                    self.gateway.write_log('tdx[{}] check point. {}, process symbols:{}'.format(i, dt, symbols))
                    last_dt = dt
        except Exception as ex:
            self.gateway.write_error(u'tdx[{}] pool.run exception:{},{}'.format(i, str(ex), traceback.format_exc()))

        self.gateway.write_error(u'tdx[{}] {}退出'.format(i, datetime.now()))

    def processReq(self, req, i):
        """
        处理行情信息ticker请求
        :param req:
        :param i:
        :return:
        """
        symbol = req
        if '.' in symbol:
            symbol, exchange = symbol.split('.')
            if exchange == 'SZSE':
                market_code = 0
            else:
                market_code = 1
        else:
            market_code = get_tdx_market_code(symbol)
            exchange = get_stock_exchange(symbol)

        exchange = Exchange(exchange)

        api = self.api_dict.get(i, None)
        if api is None:
            self.gateway.write_log(u'tdx[{}] Api is None'.format(i))
            raise Exception(u'tdx[{}] Api is None'.format(i))

        symbol_config = self.symbol_dict.get('{}_{}'.format(symbol, market_code), {})
        decimal_point = symbol_config.get('decimal_point', 2)

        # self.gateway.write_log(u'tdx[{}] get_instrument_quote:({},{})'.format(i,self.symbol_market_dict.get(symbol),symbol))
        rt_list = api.get_security_quotes([(market_code, symbol)])
        if rt_list is None or len(rt_list) == 0:
            self.gateway.write_log(u'tdx[{}]: rt_list为空'.format(i))
            return
        # else:
        #    self.gateway.write_log(u'tdx[{}]: rt_list数据:{}'.format(i, rt_list))
        if i in self.last_tick_dt:
            self.last_tick_dt[i] = datetime.now()

        # <class 'list'>: [OrderedDict([
        # ('market', 0),
        # ('code', '000001'),
        # ('active1', 1385),
        # ('price', 13.79),
        # ('last_close', 13.69),
        # ('open', 13.65), ('high', 13.81), ('low', 13.56),
        # ('reversed_bytes0', 10449822), ('reversed_bytes1', -1379),
        # ('vol', 193996), ('cur_vol', 96),
        # ('amount', 264540864.0),
        # ('s_vol', 101450),
        # ('b_vol', 92546),
        # ('reversed_bytes2', 0), ('reversed_bytes3', 17185),
        # ('bid1', 13.79), ('ask1', 13.8), ('bid_vol1', 877), ('ask_vol1', 196),
        # ('bid2', 13.78), ('ask2', 13.81), ('bid_vol2', 2586), ('ask_vol2', 1115),
        # ('bid3', 13.77), ('ask3', 13.82), ('bid_vol3', 1562), ('ask_vol3', 807),
        # ('bid4', 13.76), ('ask4', 13.83), ('bid_vol4', 211), ('ask_vol4', 711),
        # ('bid5', 13.75), ('ask5', 13.84), ('bid_vol5', 1931), ('ask_vol5', 1084),
        # ('reversed_bytes4', (385,)), ('reversed_bytes5', 1), ('reversed_bytes6', -41), ('reversed_bytes7', -29), ('reversed_bytes8', 1), ('reversed_bytes9', 0.88),
        # ('active2', 1385)])]
        dt = datetime.now()
        for d in list(rt_list):
            # 忽略成交量为0的无效单合约tick数据
            if d.get('cur_vol', 0) <= 0:
                # self.gateway.write_log(u'忽略成交量为0的无效单合约tick数据:')
                continue

            code = d.get('code', None)
            if symbol != code and code is not None:
                self.gateway.write_log(u'忽略合约{} {} 不一致的tick数据:{}'.format(symbol, d.get('code'), rt_list))
                continue

            tick = TickData(
                gateway_name=self.gateway_name,
                symbol=symbol,
                exchange=exchange,
                datetime=dt,
                date=dt.strftime('%Y-%m-%d'),
                time=dt.strftime('%H:%M:%S')
            )

            if decimal_point > 2:
                tick.pre_close = round(d.get('last_close') / (10 ** (decimal_point - 2)), decimal_point)
                tick.high_price = round(d.get('high') / (10 ** (decimal_point - 2)), decimal_point)
                tick.open_price = round(d.get('open') / (10 ** (decimal_point - 2)), decimal_point)
                tick.low_price = round(d.get('low') / (10 ** (decimal_point - 2)), decimal_point)
                tick.last_price = round(d.get('price') / (10 ** (decimal_point - 2)), decimal_point)

                tick.bid_price_1 = round(d.get('bid1') / (10 ** (decimal_point - 2)), decimal_point)
                tick.bid_volume_1 = d.get('bid_vol1')
                tick.ask_price_1 = round(d.get('ask1') / (10 ** (decimal_point - 2)), decimal_point)
                tick.ask_volume_1 = d.get('ask_vol1')

                if d.get('bid5'):
                    tick.bid_price_2 = round(d.get('bid2') / (10 ** (decimal_point - 2)), decimal_point)
                    tick.bid_volume_2 = d.get('bid_vol2')
                    tick.ask_price_2 = round(d.get('ask2') / (10 ** (decimal_point - 2)), decimal_point)
                    tick.ask_volume_2 = d.get('ask_vol2')

                    tick.bid_price_3 = round(d.get('bid3') / (10 ** (decimal_point - 2)), decimal_point)
                    tick.bid_volume_3 = d.get('bid_vol3')
                    tick.ask_price_3 = round(d.get('ask3') / (10 ** (decimal_point - 2)), decimal_point)
                    tick.ask_volume_3 = d.get('ask_vol3')

                    tick.bid_price_4 = round(d.get('bid4') / (10 ** (decimal_point - 2)), decimal_point)
                    tick.bid_volume_4 = d.get('bid_vol4')
                    tick.ask_price_4 = round(d.get('ask4') / (10 ** (decimal_point - 2)), decimal_point)
                    tick.ask_volume_4 = d.get('ask_vol4')

                    tick.bid_price_5 = round(d.get('bid5') / (10 ** (decimal_point - 2)), decimal_point)
                    tick.bid_volume_5 = d.get('bid_vol5')
                    tick.ask_price_5 = round(d.get('ask5') / (10 ** (decimal_point - 2)), decimal_point)
                    tick.ask_volume_5 = d.get('ask_vol5')

            else:
                tick.pre_close = d.get('last_close')
                tick.high_price = d.get('high')
                tick.open_price = d.get('open')
                tick.low_price = d.get('low')
                tick.last_price = d.get('price')

                tick.bid_price_1 = d.get('bid1')
                tick.bid_volume_1 = d.get('bid_vol1')
                tick.ask_price_1 = d.get('ask1')
                tick.ask_volume_1 = d.get('ask_vol1')

                if d.get('bid5'):
                    tick.bid_price_2 = d.get('bid2')
                    tick.bid_volume_2 = d.get('bid_vol2')
                    tick.ask_price_2 = d.get('ask2')
                    tick.ask_volume_2 = d.get('ask_vol2')

                    tick.bid_price_3 = d.get('bid3')
                    tick.bid_volume_3 = d.get('bid_vol3')
                    tick.ask_price_3 = d.get('ask3')
                    tick.ask_volume_3 = d.get('ask_vol3')

                    tick.bid_price_4 = d.get('bid4')
                    tick.bid_volume_4 = d.get('bid_vol4')
                    tick.ask_price_4 = d.get('ask4')
                    tick.ask_volume_4 = d.get('ask_vol4')

                    tick.bid_price_5 = d.get('bid5')
                    tick.bid_volume_5 = d.get('bid_vol5')
                    tick.ask_price_5 = d.get('ask5')
                    tick.ask_volume_5 = d.get('ask_vol5')

            tick.volume = d.get('vol', 0)
            tick.open_interest = d.get('amount', 0)

            # 修正毫秒
            last_tick = self.symbol_tick_dict.get(symbol, None)
            if (last_tick is not None) and tick.datetime.replace(microsecond=0) == last_tick.datetime:
                # 与上一个tick的时间（去除毫秒后）相同,修改为500毫秒
                tick.datetime = tick.datetime.replace(microsecond=500)
                tick.time = tick.datetime.strftime('%H:%M:%S.%f')[0:12]
            else:
                tick.datetime = tick.datetime.replace(microsecond=0)
                tick.time = tick.datetime.strftime('%H:%M:%S.%f')[0:12]

            tick.date = tick.datetime.strftime('%Y-%m-%d')
            tick.trading_day = tick.datetime.strftime('%Y-%m-%d')

            # 指数没有涨停和跌停，就用昨日收盘价正负10%
            tick.limit_up = tick.pre_close * 1.1
            tick.limit_down = tick.pre_close * 0.9

            # 排除非交易时间得tick
            if tick.datetime.hour not in [9, 10, 11, 13, 14, 15]:
                return
            elif tick.datetime.hour == 9 and tick.datetime.minute <= 25:
                return
            elif tick.datetime.hour == 15 and tick.datetime.minute >= 0:
                return

            self.symbol_tick_dict[symbol] = tick

            self.gateway.on_tick(tick)


class PbTdApi(object):

    def __init__(self, gateway: PbGateway):
        """"""
        super().__init__()
        self._active = False
        self.gateway: PbGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.userid: str = ""  # 资金账号
        self.product_id: str = ""  # 产品编号（在pb客户端看到）
        self.unit_id: str = "1"  # 单元编号（在pb客户端设置)，缺省是1
        self.holder_ids = {}

        self.order_folder = ""  # 埋单csv文件所在目录
        self.account_folder = ""  # 账号导出csv所在目录

        # 缓存了当前交易日
        self.trading_day = datetime.now().strftime('%Y-%m-%d')
        self.trading_date = self.trading_day.replace('-', '')

        self.connect_status: bool = False
        self.login_status: bool = False

        # 所有交易
        self.trades = {}  # tradeid: trade
        # 本gateway以外的委托
        self.orders = {}  # sys_orderid: order

        # 未获取本地更新检查的orderid清单
        self.unchecked_orderids = []

        # 警告
        self.warning_dict = {}

    def close(self):
        pass

    def connect(self, user_id, order_folder, account_folder, product_id, unit_id="1", holder_ids={}):
        """连接"""
        self.userid = user_id
        self.order_folder = order_folder
        self.product_id = product_id
        self.unit_id = unit_id
        self.holder_ids = holder_ids

        if os.path.exists(self.order_folder):
            self.connect_status = True

        self.account_folder = account_folder
        if os.path.exists(self.account_folder):
            self.login_status = True

            # 仅查询一次
            self.query_trades()
            # 仅全局查询一次
            self.query_orders()

            # 首次连接时，优先全部撤单
            self.cancel_all()

            if self.gateway.file_type == 'dbf':
                self.gateway.query_functions.append(self.query_update_trades_dbf)
                self.gateway.query_functions.append(self.query_update_orders_dbf)

    def get_data(self, file_path, field_names=None):
        """获取文件内容"""
        if not os.path.exists(file_path):
            return None

        results = []
        try:
            with open(file=file_path, mode='r', encoding='gbk', ) as f:
                reader = csv.DictReader(f=f, fieldnames=field_names, delimiter=",")
                for row in reader:
                    results.append(row)
        except Exception as ex:
            self.gateway.write_error(f'读取csv文件数据异常:{str(ex)}')
        return results

    def query_account(self):

        if self.gateway.file_type == 'dbf':
            self.query_account_dbf()
        else:
            self.query_account_csv()

    def query_account_dbf(self):
        """获取资金账号信息"""
        # dbf 文件名
        account_dbf = os.path.abspath(os.path.join(self.account_folder,
                                                   '{}{}.dbf'.format(
                                                       PB_FILE_NAMES.get('accounts'),
                                                       self.trading_date)))
        try:
            # dbf => 资金帐号信息
            self.gateway.write_log(f'扫描资金帐号信息:{account_dbf}')
            table = dbf.Table(account_dbf, codepage='cp936')
            table.open(dbf.READ_ONLY)
            for data in table:
                # ["资金账户"]
                if str(data.zjzh).strip() != self.userid:
                    continue
                account = AccountData(
                    gateway_name=self.gateway_name,
                    accountid=self.userid,
                    balance=float(data.dyjz),  # ["单元净值"]
                    frozen=float(data.dyjz) - float(data.kyye),  # data["可用余额"]
                    currency="人民币",
                    trading_day=self.trading_day
                )
                self.gateway.on_account(account)

            table.close()
            self.warning_dict.pop('query_account', None)

        except Exception as ex:
            err_msg = f'dbf扫描资金帐号异常:{str(ex)}'
            tra_msg = traceback.format_exc()
            err_info = self.warning_dict.get('query_account', {})
            err_count = err_info.get('err_count', 1)
            if err_count > 10:
                self.gateway.write_error(err_msg)
                self.gateway.write_error(tra_msg)
            else:
                err_count += 1
                err_info.update({'err_count': err_count, 'err_msg': err_msg, 'tra_msg': tra_msg})
                self.warning_dict.update({'query_account': err_info})

    def query_account_csv(self):
        """获取资金账号信息"""
        if self.gateway.pb_version == '2018':
            # 账号的文件
            accounts_csv = os.path.abspath(os.path.join(self.account_folder,
                                                        '{}{}.csv'.format(
                                                            PB_FILE_NAMES.get('accounts'),
                                                            self.trading_date)))
        else:
            # 账号的文件
            accounts_csv = os.path.abspath(os.path.join(self.account_folder,
                                                        self.trading_date,
                                                        '{}{}.csv'.format(
                                                            PB_FILE_NAMES.get('accounts'),
                                                            self.trading_date)))
        # csv => 所有账号资金清单
        account_list = self.get_data(accounts_csv)
        if not account_list:
            return

        for data in account_list:
            if data["资金账户"] != self.userid:
                continue
            account = AccountData(
                gateway_name=self.gateway_name,
                accountid=self.userid,
                balance=float(data["单元净值"]),
                frozen=float(data["单元净值"]) - float(data["可用余额"]),
                currency="人民币",
                trading_day=self.trading_day
            )
            self.gateway.on_account(account)

    def query_position(self):
        """获取持仓信息"""
        if self.gateway.file_type == 'dbf':
            self.query_position_dbf()
        else:
            self.query_position_csv()

    def query_position_dbf(self):
        """从dbf文件获取持仓信息"""
        # fields:['zqgs', 'zjzh', 'zhlx', 'zqdm', 'zqmc', 'zqlb', 'zxjg', 'cbjg', 'cpbh', 'cpmc', 'dybh', 'dymc', 'ccsl', 'dqcb', 'kysl', 'jjsz', 'qjsz', 'zqlx'
        # , 'jysc', 'jybz', 'dryk', 'ljyk', 'fdyk', 'fyl', 'ykl', 'tzlx', 'gddm', 'mrsl', 'mcsl', 'mrje', 'mcje', 'zdf', 'bbj', 'qjcb', 'gtcb', 'gtyk', 'zgb']
        # dbf 文件名
        position_dbf = os.path.abspath(os.path.join(self.account_folder,
                                                    '{}{}.dbf'.format(
                                                        PB_FILE_NAMES.get('positions'),
                                                        self.trading_date)))
        try:
            # dbf => 股票持仓信息
            self.gateway.write_log(f'扫描股票持仓信息:{position_dbf}')
            table = dbf.Table(position_dbf, codepage='cp936')
            table.open(dbf.READ_ONLY)
            for data in table:
                if str(data.zjzh).strip() != self.userid:
                    continue
                symbol = str(data.zqdm).strip()  # ["证券代码"]

                # symbol => Exchange
                exchange = symbol_exchange_map.get(symbol, None)
                if not exchange:
                    exchange_str = get_stock_exchange(code=symbol)
                    if len(exchange_str) > 0:
                        exchange = Exchange(exchange_str)
                        symbol_exchange_map.update({symbol: exchange})

                name = symbol_name_map.get(symbol, None)
                if not name:
                    name = data.zqmc  # ["证券名称"]
                    symbol_name_map.update({symbol: name})

                position = PositionData(
                    gateway_name=self.gateway_name,
                    accountid=self.userid,
                    symbol=symbol,  # ["证券代码"],
                    exchange=exchange,
                    direction=Direction.NET,
                    name=name,
                    volume=int(data.ccsl),  # ["持仓数量"]
                    yd_volume=int(data.kysl),  # ["可用数量"]
                    price=float(data.cbjg),  # ["成本价"]
                    cur_price=float(data.zxjg),  # ["最新价"]
                    pnl=float(data.fdyk),  # ["浮动盈亏"]
                    holder_id=str(data.gddm).strip()  # ["股东"]
                )
                self.gateway.on_position(position)

            table.close()
            self.warning_dict.pop('query_position', None)

        except Exception as ex:

            err_msg = f'dbf扫描股票持仓异常:{str(ex)}'
            tra_msg = traceback.format_exc()
            err_info = self.warning_dict.get('query_position', {})
            err_count = err_info.get('err_count', 1)
            if err_count > 10:
                self.gateway.write_error(err_msg)
                self.gateway.write_error(tra_msg)
            else:
                err_count += 1
                err_info.update({'err_count': err_count, 'err_msg': err_msg, 'tra_msg': tra_msg})
                self.warning_dict.update({'query_position': err_info})

    def query_position_csv(self):
        """从csv获取持仓信息"""
        if self.gateway.pb_version == '2018':
            # 持仓的文件
            positions_csv = os.path.abspath(os.path.join(self.account_folder,
                                                         '{}{}.csv'.format(
                                                             PB_FILE_NAMES.get('positions'),
                                                             self.trading_date)))
        else:
            # 持仓的文件
            positions_csv = os.path.abspath(os.path.join(self.account_folder,
                                                         self.trading_date,
                                                         '{}{}.csv'.format(
                                                             PB_FILE_NAMES.get('positions'),
                                                             self.trading_date)))
        # csv => 所有持仓清单
        position_list = self.get_data(positions_csv)
        if not position_list:
            return

        for data in position_list:
            if data["资金账户"] != self.userid:
                continue
            symbol = data["证券代码"]

            # symbol => Exchange
            exchange = symbol_exchange_map.get(symbol, None)
            if not exchange:
                exchange_str = get_stock_exchange(code=symbol)
                if len(exchange_str) > 0:
                    exchange = Exchange(exchange_str)
                    symbol_exchange_map.update({symbol: exchange})

            name = symbol_name_map.get(symbol, None)
            if not name:
                name = data["证券名称"]
                symbol_name_map.update({symbol: name})

            position = PositionData(
                gateway_name=self.gateway_name,
                accountid=self.userid,
                symbol=data["证券代码"],
                exchange=exchange,
                direction=Direction.NET,
                name=name,
                volume=int(data["持仓数量"]),
                yd_volume=int(data["可用数量"]),
                price=float(data["成本价"]),
                cur_price=float(data["最新价"]),
                pnl=float(data["浮动盈亏"]),
                holder_id=data["股东"]
            )
            self.gateway.on_position(position)

    def query_orders(self):
        if self.gateway.file_type == 'dbf':
            self.query_orders_dbf()
        else:
            self.query_orders_csv()

    def query_orders_dbf(self):
        """dbf文件获取所有委托"""
        # fields:['zqgs', 'zjzh', 'zhlx', 'cpbh', 'cpmc', 'dybh', 'dymc', 'wtph', 'wtxh', 'zqdm', 'zqmc', 'wtfx', 'jglx', 'wtjg', 'wtsl', 'wtzt', 'cjsl', 'wtje'
        # , 'cjjj', 'cdsl', 'jysc', 'fdyy', 'wtly', 'wtrq', 'wtsj', 'jybz']
        orders_dbf = os.path.abspath(os.path.join(self.account_folder,
                                                  '{}{}.dbf'.format(
                                                      PB_FILE_NAMES.get('orders'),
                                                      self.trading_date)))
        try:
            # dbf => 股票委托信息
            self.gateway.write_log(f'扫描股票委托信息:{orders_dbf}')
            table = dbf.Table(orders_dbf, codepage='cp936')
            table.open(dbf.READ_ONLY)
            for data in table:
                if str(data.zjzh).strip() != self.userid:  # ["资金账户"]
                    continue

                sys_orderid = str(data.wtxh).strip()  # ["委托序号"]

                # 检查是否存在本地order_manager缓存中
                order = self.gateway.order_manager.get_order_with_sys_orderid(sys_orderid)
                order_date = str(data.wtrq).strip()  # ["委托日期"]
                order_time = str(data.wtsj).strip()  # ["委托时间"]
                order_status = STATUS_NAME2VT.get(str(data.wtzt).strip())  # ["委托状态"]

                # 检查是否存在本地orders缓存中（系统级别的委托单）
                sys_order = self.orders.get(sys_orderid, None)

                if order is not None:
                    continue
                # 委托单不存在本地映射库，说明是其他地方下的单子，不是通过本接口下单
                if sys_order is None:

                    # 不处理以下状态
                    if order_status in [Status.SUBMITTING, Status.REJECTED, Status.CANCELLED, Status.CANCELLING]:
                        continue

                    order_dt = datetime.strptime(f'{order_date} {order_time}', "%Y%m%d %H%M%S")
                    direction = DIRECTION_STOCK_NAME2VT.get(str(data.wtfx).strip())  # ["委托方向"]
                    offset = Offset.NONE
                    if direction is None:
                        direction = Direction.NET
                    elif direction == Direction.LONG:
                        offset = Offset.OPEN
                    elif direction == Direction.SHORT:
                        offset = Offset.CLOSE
                    sys_order = OrderData(
                        gateway_name=self.gateway_name,
                        symbol=str(data.zqdm).strip(),  # ["证券代码"]
                        exchange=EXCHANGE_NAME2VT.get(str(data.jysc).strip()),  # ["交易市场"]
                        orderid=sys_orderid,
                        sys_orderid=sys_orderid,
                        accountid=self.userid,
                        type=ORDERTYPE_NAME2VT.get(str(data.jglx).strip(), OrderType.LIMIT),  # ["价格类型"]
                        direction=direction,
                        offset=offset,
                        price=float(data.wtjg),  # ["委托价格"]
                        volume=float(data.wtsl),  # ["委托数量"]
                        traded=float(data.cjsl),  # ["成交数量"]
                        status=order_status,
                        datetime=order_dt,
                        time=order_dt.strftime('%H:%M:%S')
                    )
                    # 直接发出订单更新事件
                    self.gateway.write_log(f'账号订单查询，新增:{sys_order.__dict__}')
                    self.orders.update({sys_order.sys_orderid: sys_order})
                    self.gateway.on_order(sys_order)
                    continue

                # 存在账号缓存，判断状态是否更新
                else:
                    # 暂不处理，交给XHPT_WTCX模块处理
                    if sys_order.status != order_status or sys_order.traded != float(data.cjsl):  # ["成交数量"]
                        sys_order.traded = float(data.cjsl)  # ["成交数量"]
                        sys_order.status = order_status
                        self.orders.update({sys_order.sys_orderid: sys_order})
                        self.gateway.write_log(f'账号订单查询，更新:{sys_order.__dict__}')
                        self.gateway.on_order(sys_order)
                        continue

            table.close()
            self.warning_dict.pop('query_orders', None)
        except Exception as ex:

            err_msg = f'dbf扫描股票委托异常:{str(ex)}'
            tra_msg = traceback.format_exc()
            err_info = self.warning_dict.get('query_orders', {})
            err_count = err_info.get('err_count', 1)
            if err_count > 10:
                self.gateway.write_error(err_msg)
                self.gateway.write_error(tra_msg)
            else:
                err_count += 1
                err_info.update({'err_count': err_count, 'err_msg': err_msg, 'tra_msg': tra_msg})
                self.warning_dict.update({'query_orders': err_info})

    def query_orders_csv(self):
        """获取所有委托"""
        # 所有委托的文件
        if self.gateway.pb_version == '2018':
            orders_csv = os.path.abspath(os.path.join(self.account_folder,
                                                      '{}{}.csv'.format(
                                                          PB_FILE_NAMES.get('orders'),
                                                          self.trading_date)))
        else:
            orders_csv = os.path.abspath(os.path.join(self.account_folder,
                                                      self.trading_date,
                                                      '{}{}.csv'.format(
                                                          PB_FILE_NAMES.get('orders'),
                                                          self.trading_date)))

        # csv => 所有委托记录
        order_list = self.get_data(orders_csv)
        if not order_list:
            return

        for data in order_list:
            if data["资金账户"] != self.userid:
                continue

            sys_orderid = str(data["委托序号"])

            # 检查是否存在本地order_manager缓存中
            order = self.gateway.order_manager.get_order_with_sys_orderid(sys_orderid)
            order_date = data["委托日期"]
            order_time = data["委托时间"]
            order_status = STATUS_NAME2VT.get(data["委托状态"])

            # 检查是否存在本地orders缓存中（系统级别的委托单）
            sys_order = self.orders.get(sys_orderid, None)

            if order is not None:
                continue
            # 委托单不存在本地映射库，说明是其他地方下的单子，不是通过本接口下单
            if sys_order is None:

                # 不处理以下状态
                if order_status in [Status.SUBMITTING, Status.REJECTED, Status.CANCELLED, Status.CANCELLING]:
                    continue

                order_dt = datetime.strptime(f'{order_date} {order_time}', "%Y%m%d %H%M%S")
                direction = DIRECTION_STOCK_NAME2VT.get(data["委托方向"])
                offset = Offset.NONE
                if direction is None:
                    direction = Direction.NET
                elif direction == Direction.LONG:
                    offset = Offset.OPEN
                elif direction == Direction.SHORT:
                    offset = Offset.CLOSE
                sys_order = OrderData(
                    gateway_name=self.gateway_name,
                    symbol=data["证券代码"],
                    exchange=EXCHANGE_NAME2VT.get(data["交易市场"]),
                    orderid=sys_orderid,
                    sys_orderid=sys_orderid,
                    accountid=self.userid,
                    type=ORDERTYPE_NAME2VT.get(data["价格类型"], OrderType.LIMIT),
                    direction=direction,
                    offset=offset,
                    price=float(data["委托价格"]),
                    volume=float(data["委托数量"]),
                    traded=float(data["成交数量"]),
                    status=order_status,
                    datetime=order_dt,
                    time=order_dt.strftime('%H:%M:%S')
                )
                # 直接发出订单更新事件
                self.gateway.write_log(f'账号订单查询，新增:{sys_order.__dict__}')
                self.orders.update({sys_order.sys_orderid: sys_order})
                self.gateway.on_order(sys_order)
                continue

            # 存在账号缓存，判断状态是否更新
            else:
                # 暂不处理，交给XHPT_WTCX模块处理
                if sys_order.status != order_status or sys_order.traded != float(data["成交数量"]):
                    sys_order.traded = float(data["成交数量"])
                    sys_order.status = order_status
                    self.orders.update({sys_order.sys_orderid: sys_order})
                    self.gateway.write_log(f'账号订单查询，更新:{sys_order.__dict__}')
                    self.gateway.on_order(sys_order)
                    continue

    def query_update_orders_dbf(self):
        """扫描批量下单的委托查询(dbf文件格式）"""
        # XHPT_WTCX委托的dbf文件
        orders_dbf = os.path.abspath(os.path.join(self.order_folder,
                                                  '{}{}.dbf'.format(
                                                      PB_FILE_NAMES.get('update_orders'),
                                                      self.trading_date)))
        # dbf => 所有委托记录
        try:
            # dbf => 所有成交记录
            self.gateway.write_log(f'扫描所有委托查询:{orders_dbf}')
            table = dbf.Table(orders_dbf, codepage='cp936')
            table.open(dbf.READ_ONLY)

            for data in table:
                # 第三方系统自定义号
                local_orderid = str(data.wbzdyxh)
                if len(local_orderid) == 0:
                    self.gateway.write_log(f'获取不到本地委托号:{print_dict(data.__dict__)}')
                    continue
                # 如果不足8位，自动补充0
                if len(local_orderid) < 8:
                    local_orderid = local_orderid.rjust(8, '0')

                # 委托状态=>
                order_status = STATUS_PB2VT.get(str(data.wtzt))
                # 恒生平台返回的委托序号
                sys_orderid = str(data.wtxh)
                if len(sys_orderid) == 0:
                    self.gateway.write_log(f'获取不到恒生平台的委托序号:{print_dict(data.__dict__)}')
                    continue

                # 通过本地委托编号，检查是否存在本地订单列表中
                order = self.gateway.order_manager.get_order_with_local_orderid(local_orderid)
                if order is None:
                    self.gateway.write_log(f'本地委托编号{local_orderid}不在本地订单中')

                    direction = DIRECTION_STOCK_NAME2VT.get(str(data.wtfx).strip())
                    offset = Offset.NONE
                    if direction is None:
                        direction = Direction.NET
                    elif direction == Direction.LONG:
                        offset = Offset.OPEN
                    elif direction == Direction.SHORT:
                        offset = Offset.CLOSE
                    if order_status == Status.ALLTRADED:
                        traded = data.wtsl
                    else:
                        traded = 0
                    order_dt = datetime.strptime(f'{data.wtrq} {data.wtsj}', "%Y%m%d %H%M%S")
                    exchange = EXCHANGE_PB2VT.get(str(data.jysc).strip())
                    new_order = OrderData(
                        gateway_name=self.gateway_name,
                        symbol=str(data.zqdm).strip(),
                        exchange=exchange,
                        orderid=local_orderid,
                        sys_orderid=sys_orderid,
                        accountid=self.userid,
                        type=ORDERTYPE_PB2VT.get(str(data.wtjglx).strip(), OrderType.LIMIT),
                        direction=direction,
                        offset=offset,
                        price=float(data.wtjg),
                        volume=float(data.wtsl),
                        traded=traded,
                        status=order_status,
                        datetime=order_dt,
                        time=order_dt.strftime('%H:%M:%S')
                    )
                    self.gateway.write_log(f'补充委托记录:{print_dict(new_order.__dict__)}')
                    self.gateway.order_manager.on_order(new_order)

                    continue

                if order.sys_orderid != sys_orderid:
                    pre_sys_orderid = order.sys_orderid
                    order.sys_orderid = sys_orderid
                    self.gateway.order_manager.update_orderid_map(local_orderid=local_orderid, sys_orderid=sys_orderid)
                    self.gateway.write_log(
                        f'绑定local_orderid:{local_orderid}, <=> 系统委托号:{pre_sys_orderid}=>{sys_orderid}')

                    if local_orderid in self.unchecked_orderids:
                        self.unchecked_orderids.remove(local_orderid)

                # 如果委托状态是已经撤单，拒单，已成交，就不处理
                if order.status in [Status.CANCELLED, Status.REJECTED, Status.ALLTRADED]:
                    continue

                if order.status != order_status:
                    self.gateway.write_log(f'{local_orderid} 状态:{order.status.value} => {order_status.value}')
                    order.status = order_status
                    if order.status == Status.CANCELLED:
                        order.cancel_time = datetime.now().strftime('%H:%M:%S')
                    if order.status == Status.ALLTRADED and order.traded != order.volume:
                        self.gateway.write_log(f'dbf批量下单，委托单全成交,成交数:{order.traded}=>{order.volume}')
                        order.traded = order.volume

                    self.gateway.write_log(f'dbf批量下单，委托单更新:{order.__dict__}')
                    self.gateway.order_manager.on_order(order)
                    continue

            table.close()
            self.warning_dict.pop('query_update_order', None)
        except Exception as ex:

            err_msg = f'dbf查询委托库异常:{str(ex)}'
            tra_msg = traceback.format_exc()
            err_info = self.warning_dict.get('query_update_order', {})
            err_count = err_info.get('err_count', 1)
            if err_count > 10:
                self.gateway.write_error(err_msg)
                self.gateway.write_error(tra_msg)
            else:
                err_count += 1
                err_info.update({'err_count': err_count, 'err_msg': err_msg, 'tra_msg': tra_msg})
                self.warning_dict.update({'query_update_order': err_info})

    def query_update_orders_csv(self):
        """扫描批量下单的委托查询(csv文件格式）"""
        # XHPT_WTCX委托的CSV文件
        orders_csv = os.path.abspath(os.path.join(self.order_folder,
                                                  '{}{}.csv'.format(
                                                      PB_FILE_NAMES.get('update_orders'),
                                                      self.trading_date)))
        # csv => 所有委托记录
        order_list = self.get_data(orders_csv, field_names=UPDATE_ORDER_FIELDS.keys())
        if not order_list:
            return

        for data in order_list:
            # 第三方系统自定义号
            local_orderid = str(data["WBZDYXH"]).lstrip()
            if len(local_orderid) == 0:
                continue
            if len(local_orderid) < 8:
                local_orderid = local_orderid.rjust(8, '0')

            order = self.gateway.order_manager.get_order_with_local_orderid(local_orderid)
            if order is None:
                continue

            # 恒生平台返回的委托序号
            sys_orderid = str(data['WTXH']).lstrip()
            if len(sys_orderid) == 0:
                continue

            if order.sys_orderid != sys_orderid:
                pre_sys_orderid = order.sys_orderid
                order.sys_orderid = sys_orderid
                self.gateway.order_manager.update_orderid_map(local_orderid=local_orderid, sys_orderid=sys_orderid)
                self.gateway.write_log(f'绑定local_orderid:{local_orderid}, <=> 系统委托号:{pre_sys_orderid}=>{sys_orderid}')
                if local_orderid in self.unchecked_orderids:
                    self.unchecked_orderids.remove(local_orderid)

            # 如果委托状态是已经撤单，拒单，已成交，就不处理
            if order.status in [Status.CANCELLED, Status.REJECTED, Status.ALLTRADED]:
                continue

            order_status = STATUS_PB2VT.get(data["WTZT"])

            if order.status != order_status:
                self.gateway.write_log(f'{local_orderid} 状态:{order.status.value} => {order_status.value}')
                order.status = order_status
                if order.status == Status.CANCELLED:
                    order.cancel_time = datetime.now().strftime('%H:%M:%S')
                if order.status == Status.ALLTRADED and order.traded != order.volume:
                    self.gateway.write_log(f'csv批量下单，委托单全成交,成交数:{order.traded}=>{order.volume}')
                    order.traded = order.volume

                self.gateway.write_log(f'csv批量下单，委托更新:{order.__dict__}')
                self.gateway.order_manager.on_order(order)
                continue

    def query_trades(self):
        if self.gateway.file_type == 'dbf':
            self.query_trades_dbf()
        else:
            self.query_trades_csv()

    def query_trades_dbf(self):
        """dbf文件获取所有成交"""
        # fields:['zqgs', 'zjzh', 'zhlx', 'cpbh', 'cpmc', 'dybh', 'dymc', 'cjxh', 'wtph', 'wtxh', 'zqdm', 'zqmc', 'wtfx', 'zqlb', 'ywfl', 'cjrq', 'cjsj', 'cjsl'
        # , 'cjjg', 'zfy', 'cjje', 'jysc', 'jybz', 'wtly', 'rybh', 'rymc']
        trades_dbf = os.path.abspath(os.path.join(self.account_folder,
                                                  '{}{}.dbf'.format(
                                                      PB_FILE_NAMES.get('trades'),
                                                      self.trading_date)))
        try:
            # dbf => 股票成交信息
            self.gateway.write_log(f'扫描股票成交信息:{trades_dbf}')
            table = dbf.Table(trades_dbf, codepage='cp936')
            table.open(dbf.READ_ONLY)
            for data in table:
                if str(data.zjzh).strip() != self.userid:  # ["资金账户"]
                    continue

                sys_orderid = str(data.wtxh)  # ["委托序号"]
                sys_tradeid = str(data.cjxh)  # ["成交序号"]

                # 检查是否存在本地trades缓存中
                trade = self.trades.get(sys_tradeid, None)
                order = self.gateway.order_manager.get_order_with_sys_orderid(sys_orderid)

                # 如果交易不再本地映射关系
                if trade is None and order is None:
                    trade_date = str(data.cjrq).strip()  # ["成交日期"]
                    trade_time = str(data.cjsj).strip()  # ["成交时间"]
                    trade_dt = datetime.strptime(f'{trade_date} {trade_time}', "%Y%m%d %H%M%S")
                    direction = DIRECTION_STOCK_NAME2VT.get(str(data.wtfx).strip())  # ["委托方向"]
                    offset = Offset.NONE
                    if direction is None:
                        direction = Direction.NET
                    elif direction == Direction.LONG:
                        offset = Offset.OPEN
                    elif direction == Direction.SHORT:
                        offset = Offset.CLOSE
                    trade = TradeData(
                        gateway_name=self.gateway_name,
                        symbol=str(data.zqdm).strip(),  # ["证券代码"]
                        exchange=EXCHANGE_NAME2VT.get(str(data.jysc).strip()),  # ["交易市场"]
                        orderid=sys_tradeid,
                        tradeid=sys_tradeid,
                        sys_orderid=sys_orderid,
                        accountid=self.userid,
                        direction=direction,
                        offset=offset,
                        price=float(data.cjjg),  # ["成交价格"]
                        volume=float(data.cjsl),  # ["成交数量"]
                        datetime=trade_dt,
                        time=trade_dt.strftime('%H:%M:%S'),
                        trade_amount=float(data.cjje),  # ["成交金额"]
                        commission=float(data.zfy)  # ["总费用"]
                    )
                    self.trades[sys_tradeid] = trade
                    self.gateway.on_trade(copy.copy(trade))
                    continue
            table.close()
            self.warning_dict.pop('query_trades', None)
        except Exception as ex:

            err_msg = f'dbf扫描股票成交异常:{str(ex)}'
            tra_msg = traceback.format_exc()
            err_info = self.warning_dict.get('query_trades', {})
            err_count = err_info.get('err_count', 1)
            if err_count > 10:
                self.gateway.write_error(err_msg)
                self.gateway.write_error(tra_msg)
            else:
                err_count += 1
                err_info.update({'err_count': err_count, 'err_msg': err_msg, 'tra_msg': tra_msg})
                self.warning_dict.update({'query_trades': err_info})

    def query_trades_csv(self):
        """获取所有成交"""
        # 所有成交的文件
        if self.gateway.pb_version == '2018':
            trades_csv = os.path.abspath(os.path.join(self.account_folder,
                                                      '{}{}.csv'.format(
                                                          PB_FILE_NAMES.get('trades'),
                                                          self.trading_date)))
        else:
            trades_csv = os.path.abspath(os.path.join(self.account_folder,
                                                      self.trading_date,
                                                      '{}{}.csv'.format(
                                                          PB_FILE_NAMES.get('trades'),
                                                          self.trading_date)))

        # csv => 所有成交记录
        trade_list = self.get_data(trades_csv)
        if not trade_list:
            return

        for data in trade_list:
            if data["资金账户"] != self.userid:
                continue

            sys_orderid = str(data["委托序号"])
            sys_tradeid = str(data["成交序号"])

            # 检查是否存在本地trades缓存中
            trade = self.trades.get(sys_tradeid, None)
            order = self.gateway.order_manager.get_order_with_sys_orderid(sys_orderid)

            # 如果交易不再本地映射关系
            if trade is None and order is None:
                trade_date = data["成交日期"]
                trade_time = data["成交时间"]
                trade_dt = datetime.strptime(f'{trade_date} {trade_time}', "%Y%m%d %H%M%S")
                direction = DIRECTION_STOCK_NAME2VT.get(data["委托方向"])
                offset = Offset.NONE
                if direction is None:
                    direction = Direction.NET
                elif direction == Direction.LONG:
                    offset = Offset.OPEN
                elif direction == Direction.SHORT:
                    offset = Offset.CLOSE
                trade = TradeData(
                    gateway_name=self.gateway_name,
                    symbol=data["证券代码"],
                    exchange=EXCHANGE_NAME2VT.get(data["交易市场"]),
                    orderid=sys_tradeid,
                    tradeid=sys_tradeid,
                    sys_orderid=sys_orderid,
                    accountid=self.userid,
                    direction=direction,
                    offset=offset,
                    price=float(data["成交价格"]),
                    volume=float(data["成交数量"]),
                    datetime=trade_dt,
                    time=trade_dt.strftime('%H:%M:%S'),
                    trade_amount=float(data["成交金额"]),
                    commission=float(data["总费用"])
                )
                self.trades[sys_tradeid] = trade
                self.gateway.on_trade(copy.copy(trade))
                continue

    def query_update_trades_dbf(self):
        """获取接口的dbf成交更新"""
        # 所有成交的dbf文件
        trades_dbf = os.path.abspath(os.path.join(self.order_folder,
                                                  '{}{}.dbf'.format(
                                                      PB_FILE_NAMES.get('update_trades'),
                                                      self.trading_date)))

        try:
            # dbf => 所有成交记录
            self.gateway.write_log(f'扫描所有成交记录:{trades_dbf}')
            table = dbf.Table(trades_dbf, codepage='cp936')
            table.open(dbf.READ_ONLY)

            for data in table:
                # 本地委托号
                local_orderid = str(data.wbzdyxh).strip()
                if 0 < len(local_orderid) < 8:
                    local_orderid = local_orderid.rjust(8, '0')
                # 系统委托号
                sys_orderid = str(data.wtxh).strip()
                # 系统交易号
                sys_tradeid = str(data.cjbh).strip()

                # 检查是否存在本地trades缓存中
                trade = self.trades.get(sys_tradeid, None)
                order = self.gateway.order_manager.get_order_with_sys_orderid(sys_orderid)

                # 如果交易不再本地映射关系
                if trade is None and order:
                    trade_date = str(data.cjrq).strip()
                    trade_time = str(data.cjsj).strip()
                    trade_dt = datetime.strptime(f'{trade_date} {trade_time}', "%Y%m%d %H%M%S")
                    direction = DIRECTION_ORDER_PB2VT.get(str(data.wtfx).strip())
                    offset = Offset.NONE
                    if direction is None:
                        direction = Direction.NET
                    elif direction == Direction.LONG:
                        offset = Offset.OPEN
                    elif direction == Direction.SHORT:
                        offset = Offset.CLOSE
                    trade = TradeData(
                        gateway_name=self.gateway_name,
                        symbol=str(data.zqdm).strip(),
                        exchange=EXCHANGE_PB2VT.get(str(data.jysc).strip()),
                        orderid=local_orderid,
                        tradeid=sys_tradeid,
                        sys_orderid=sys_orderid,
                        accountid=self.userid,
                        direction=direction,
                        offset=offset,
                        price=float(data.cjjg),
                        volume=int(data.cjsl),
                        datetime=trade_dt,
                        time=trade_dt.strftime('%H:%M:%S'),
                        trade_amount=float(data.cjje),
                        commission=float(data.zfy),
                        holder_id=str(data.gddm).strip()
                    )
                    # 保存交易记录
                    self.trades[sys_tradeid] = trade

                    # 更新订单的成交数量
                    if order.volume >= order.traded + trade.volume:
                        pre_traded = order.traded
                        order.traded += trade.volume
                        self.gateway.write_log(
                            f'{local_orderid}/{sys_orderid} 成交数量:{pre_traded} =>{order.traded} ,目标:{order.volume}')

                    # 发送成交更新
                    self.gateway.on_trade(copy.copy(trade))
                    continue

            table.close()
            self.warning_dict.pop('query_update_trade', None)

        except Exception as ex:

            err_msg = f'dbf查询成交库异常:{str(ex)}'
            tra_msg = traceback.format_exc()
            err_info = self.warning_dict.get('query_update_trade', {})
            err_count = err_info.get('err_count', 1)
            if err_count > 10:
                self.gateway.write_error(err_msg)
                self.gateway.write_error(tra_msg)
            else:
                err_count += 1
                err_info.update({'err_count': err_count, 'err_msg': err_msg, 'tra_msg': tra_msg})
                self.warning_dict.update({'query_update_trade': err_info})

    def query_update_trades_csv(self):
        """获取接口的csv成交更新"""
        # 所有成交的csv文件
        trades_csv = os.path.abspath(os.path.join(self.order_folder,
                                                  '{}{}.csv'.format(
                                                      PB_FILE_NAMES.get('update_trades'),
                                                      self.trading_date)))

        # csv => 所有成交记录
        trade_list = self.get_data(trades_csv, field_names=UPDATE_TRADE_FIELDS.keys())
        if not trade_list:
            return

        for data in trade_list:
            local_orderid = str(data["WBZDYXH"]).lstrip()
            if len(local_orderid) < 8:
                local_orderid = local_orderid.rjust(8, '0')

            sys_orderid = str(data["WTXH"]).lstrip()
            sys_tradeid = str(data["CJBH"]).lstrip()

            # 检查是否存在本地trades缓存中
            trade = self.trades.get(sys_tradeid, None)
            order = self.gateway.order_manager.get_order_with_sys_orderid(sys_orderid)

            # 如果交易不再本地映射关系
            if trade is None and order:
                trade_date = str(data["CJRQ"]).lstrip()
                trade_time = str(data["CJSJ"]).lstrip()
                trade_dt = datetime.strptime(f'{trade_date} {trade_time}', "%Y%m%d %H%M%S")
                direction = DIRECTION_ORDER_PB2VT.get(str(data.wtfx).strip())
                offset = Offset.NONE
                if direction is None:
                    direction = Direction.NET
                elif direction == Direction.LONG:
                    offset = Offset.OPEN
                elif direction == Direction.SHORT:
                    offset = Offset.CLOSE
                trade = TradeData(
                    gateway_name=self.gateway_name,
                    symbol=str(data["ZQDM"]).lstrip(),
                    exchange=EXCHANGE_PB2VT.get(str(data["JYSC"]).lstrip()),
                    orderid=local_orderid,
                    tradeid=sys_tradeid,
                    sys_orderid=sys_orderid,
                    accountid=self.userid,
                    direction=direction,
                    offset=offset,
                    price=float(str(data["CJJG"]).lstrip()),
                    volume=float(str(data["CJSL"]).lstrip()),
                    datetime=trade_dt,
                    time=trade_dt.strftime('%H:%M:%S'),
                    trade_amount=float(str(data["CJJE"]).lstrip()),
                    commission=float(str(data["ZFY"]).lstrip()),
                    holder_id=str(data['GDDM']).lstrip()
                )
                # 保存交易记录
                self.trades[sys_tradeid] = trade

                # 更新订单的成交数量
                if order.volume >= order.traded + trade.volume:
                    pre_traded = order.traded
                    order.traded += trade.volume
                    self.gateway.write_log(
                        f'{local_orderid}/{sys_orderid} 成交数量:{pre_traded} =>{order.traded} ,目标:{order.volume}')

                # 发送成交更新
                self.gateway.on_trade(copy.copy(trade))
                continue

    def check_send_order_dbf(self):
        """检查更新委托文件dbf"""

        dbf_file = os.path.abspath(os.path.join(self.order_folder,
                                                '{}{}.dbf'.format(PB_FILE_NAMES.get('send_order'), self.trading_date)))
        try:
            table = dbf.Table(dbf_file, codepage='cp936')
            table.open(dbf.READ_ONLY)
            for record in table:
                local_orderid = str(record.wbzdyxh)
                if len(local_orderid) < 8:
                    local_orderid = local_orderid.rjust(8, '0')

                if local_orderid not in self.unchecked_orderids:
                    continue

                # 从本地order_manager中获取order
                order = self.gateway.order_manager.get_order_with_local_orderid(local_orderid)
                # 判断order取不到，或者order状态不是SUBMITTING
                if order is None or order.status != Status.SUBMITTING:
                    continue

                # 检查是否具有系统委托编号
                if order.sys_orderid == "":
                    sys_orderid = str(getattr(record, 'wtxh', ''))
                    if len(sys_orderid) == 0:
                        continue

                    # 委托失败标志
                    if sys_orderid == "0":
                        err_msg = record.sbyy
                        if isinstance(err_msg, bytes):
                            err_msg = err_msg.decode('gbk')

                        if len(err_msg) == 0 or record.wtsbdm == 0:
                            self.gateway.write_log(f'收到失败，又没有失败原因')
                            continue

                        err_id = str(getattr(record, 'wtsbdm', '')).strip()
                        order.status = Status.REJECTED
                        self.gateway.write_log(f'dbf批量下单，委托被拒:{order.__dict__}')
                        self.gateway.order_manager.on_order(order)
                        self.gateway.write_error(msg=f'{order.direction.value},{order.vt_symbol},{err_msg}',
                                                 error={"ErrorID": err_id, "ErrorMsg": "委托失败"})

                    if sys_orderid not in ['0','None']:
                        self.gateway.order_manager.update_orderid_map(local_orderid=local_orderid,
                                                                      sys_orderid=sys_orderid)
                        order.sys_orderid = sys_orderid
                        order.status = Status.NOTTRADED
                        self.gateway.write_log(f'绑定本地local_orderid:{local_orderid} <=>sys_orderid:{sys_orderid}')
                        self.gateway.write_log(f'dbf批量下单，委托接受:{order.__dict__}')
                        self.gateway.order_manager.on_order(order)
                        self.gateway.write_log(f'委托成功')

                    # 移除检查的id
                    self.gateway.write_log(f'本地委托单更新检查完毕，移除{local_orderid}')
                    self.unchecked_orderids.remove(local_orderid)

            table.close()
            self.warning_dict.pop('query_send_order', None)

        except Exception as ex:

            err_msg = f'dbf查询系统委托号异常:{str(ex)}'
            tra_msg = traceback.format_exc()
            err_info = self.warning_dict.get('query_send_order', {})
            err_count = err_info.get('err_count', 1)
            if err_count > 10:
                self.gateway.write_error(err_msg)
                self.gateway.write_error(tra_msg)
            else:
                err_count += 1
                err_info.update({'err_count': err_count, 'err_msg': err_msg, 'tra_msg': tra_msg})
                self.warning_dict.update({'query_send_order': err_info})

    def check_send_order_csv(self):
        """检查更新委托文件csv"""

        # 当日send_order的文件
        send_order_csv = os.path.abspath(os.path.join(self.order_folder,
                                                      '{}{}.csv'.format(
                                                          PB_FILE_NAMES.get('send_order'),
                                                          self.trading_date)))
        # csv => 所有send_order记录
        order_list = self.get_data(send_order_csv, field_names=SEND_ORDER_FIELDS.keys())

        # 逐一处理
        for data in order_list:
            local_orderid = data.get('WBZDYXH', "").lstrip(' ')
            if local_orderid is "":
                continue

            if local_orderid not in self.unchecked_orderids:
                continue

            # 从本地order_manager中获取order
            order = self.gateway.order_manager.get_order_with_local_orderid(local_orderid)
            # 判断order取不到，或者order状态不是SUBMITTING
            if order is None or order.status != Status.SUBMITTING:
                continue

            # 检查是否具有系统委托编号
            if order.sys_orderid == "":
                sys_orderid = data.get('WTXH', '').lstrip(' ')
                if len(sys_orderid) == 0:
                    continue
                err_msg = data.get('SBYY', '').lstrip(' ')

                # 委托失败标志
                if sys_orderid == "0":
                    if len(err_msg) == 0:
                        self.gateway.write_log(f'收到失败标准，又没有失败原因:{print_dict(data.__dict__)}')
                        continue

                    err_id = data.get('WTSBDM', '').lstrip(' ')
                    order.status = Status.REJECTED
                    self.gateway.write_log(f'csv批量下单，委托被拒:{order.__dict__}')
                    self.gateway.order_manager.on_order(order)
                    self.gateway.write_error(msg=err_msg, error={"ErrorID": err_id, "ErrorMsg": "委托失败"})

                if sys_orderid != '0':
                    self.gateway.order_manager.update_orderid_map(local_orderid=local_orderid, sys_orderid=sys_orderid)
                    order.sys_orderid = sys_orderid
                    order.status = Status.NOTTRADED
                    self.gateway.write_log(f'csv批量下单，委托被接受:{order.__dict__}')
                    self.gateway.order_manager.on_order(order)
                    self.gateway.write_log(f'委托成功')

                # 移除检查的id
                self.gateway.write_log(f'本地委托单更新检查完毕，移除{local_orderid}')
                self.unchecked_orderids.remove(local_orderid)

    def send_order(self, req: OrderRequest):
        """委托发单"""
        self.gateway.write_log(f'委托发单:{req.__dict__}')
        if self.gateway.file_type == 'dbf':
            return self.send_order_dbf(req)
        else:
            return self.send_order_csv(req)

    def send_order_dbf(self, req: OrderRequest):
        """通过dbf文件进行发单"""
        # 发生委托，才添加批量埋单接口的委托、成交检查
        if self.query_update_trades_dbf not in self.gateway.query_functions:
            self.gateway.query_functions.append(self.query_update_trades_dbf)
        if self.query_update_orders_dbf not in self.gateway.query_functions:
            self.gateway.query_functions.append(self.query_update_orders_dbf)

        # 创建本地orderid(str格式， HHMM+00序列号)
        local_orderid = self.gateway.order_manager.new_local_orderid()

        # req => order
        order = req.create_order_data(orderid=local_orderid, gateway_name=self.gateway_name)

        dbf_file = os.path.abspath(os.path.join(self.order_folder,
                                                '{}{}.dbf'.format(PB_FILE_NAMES.get('send_order'), self.trading_date)))
        # 股票买卖，强制offset = Offset.NONE
        order.offset = Offset.NONE

        contract = self.gateway.md_api.contract_dict.get(f'{order.symbol}.{order.exchange.value}')
        direction = DIRECTION_STOCK_VT2PB.get((order.direction, order.offset))  # 委托方向
        if contract:
            if contract.product == Product.BOND:
                if direction == '1':
                    direction = '3'
                else:
                    direction = '4'

        data = (
            self.product_id,  # "CPBH": "C32",  # 产品代码/基金代码 <-- 输入参数 -->
            self.unit_id,  # "ZCDYBH": "C16",  # 单元编号/组合编号
            self.unit_id,  # "ZHBH": "C16",  # 组合编号
            self.holder_ids.get(order.exchange),  # "GDDM": "C20",  # 股东代码
            EXCHANGE_VT2PB.get(order.exchange),  # "JYSC": "C3",  # 交易市场
            order.symbol,  # "ZQDM": "C16",  # 证券代码
            direction,  # "WTFX": "C4",  # 委托方向
            get_pb_order_type(order.exchange, order.type),  # "WTJGLX": "C1",  # 委托价格类型
            round(order.price, 2),  # "WTJG": "N11.4",  # 委托价格
            int(order.volume),  # "WTSL": "N12",  # 委托数量
            local_orderid,  # "WBZDYXH": "N9",  # 第三方系统自定义号( 如果字符串不是数字，会报错，如果前面有0，自动去掉）
            None,  # "WTXH": "N8",  # 委托序号  <-- 输出参数  -->
            None,  # "WTSBDM": "N8",  # 委托失败代码
            "",  # "SBYY": "C254",  # 失败原因
            "",  # "CLBZ": "C1",  # 处理标志  <-- 内部自用字段 -->
            "",  # "BYZD": "C2",  # 备用字段
            0,  # "WTJE": "N16.2",  # 委托金额  <-- 扩充参数 -->
            "",  # "TSBS": "C64",  # 特殊标识
            ""  # "YWBS": "C2",  # 业务标识
        )
        try:
            # 打开dbf文件=》table
            table = dbf.Table(dbf_file)
            # 读取、写入模式
            table.open(dbf.READ_WRITE)
            # 写入数据
            table.append(data)
            # 关闭dbf文件
            table.close()
            self.warning_dict.pop('send_order', None)
        except Exception as ex:

            err_msg = f'dbf添加发单记录异常:{str(ex)}'
            tra_msg = traceback.format_exc()
            err_info = self.warning_dict.get('send_order', {})
            err_count = err_info.get('err_count', 1)
            if err_count > 10:
                self.gateway.write_error(err_msg)
                self.gateway.write_error(tra_msg)
            else:
                err_count += 1
                err_info.update({'err_count': err_count, 'err_msg': err_msg, 'tra_msg': tra_msg})
                self.warning_dict.update({'send_order': err_info})

            return ""

        # 设置状态为提交中
        order.status = Status.SUBMITTING
        # 添加待检查列表
        self.unchecked_orderids.append(local_orderid)
        # 登记并发送on_order事件
        self.gateway.write_log(f'send_order，提交dbf委托:{order.__dict__}')
        self.gateway.order_manager.on_order(order)

        # 添加定时检查任务
        if self.check_send_order_dbf not in self.gateway.query_functions:
            self.gateway.write_log(f'添加扫描系统委托号任务到任务队列中')
            self.gateway.query_functions.append(self.check_send_order_dbf)

        return order.vt_orderid

    def send_order_csv(self, req: OrderRequest):
        """csv文件格式委托"""

        # 发生委托，才添加批量埋单接口的委托、成交检查
        if self.query_update_trades_csv not in self.gateway.query_functions:
            self.gateway.query_functions.append(self.query_update_trades_csv)
        if self.query_update_orders_csv not in self.gateway.query_functions:
            self.gateway.query_functions.append(self.query_update_orders_csv)

        # 创建本地orderid
        local_orderid = self.gateway.order_manager.new_local_orderid()

        # req => order
        order = req.create_order_data(orderid=local_orderid, gateway_name=self.gateway_name)

        csv_file = os.path.abspath(os.path.join(self.order_folder,
                                                '{}{}.csv'.format(PB_FILE_NAMES.get('send_order'), self.trading_date)))
        # 股票买卖，强制offset = Offset.NONE
        order.offset = Offset.NONE

        contract = self.gateway.md_api.contract_dict.get(f'{order.symbol}.{order.exchange.value}')
        direction = DIRECTION_STOCK_VT2PB.get((order.direction, order.offset))  # 委托方向
        if contract:
            if contract.product == Product.BOND:
                if direction == '1':
                    direction = '3'
                else:
                    direction = '4'
        data = {
            "CPBH": self.product_id,  # 产品代码/基金代码 <-- 输入参数 -->
            "ZCDYBH": self.unit_id,  # 单元编号/组合编号
            "ZHBH": self.unit_id,  # 组合编号
            "GDDM": self.holder_ids.get(order.exchange),  # 股东代码
            "JYSC": EXCHANGE_VT2PB.get(order.exchange),  # 交易市场
            "ZQDM": order.symbol,  # 证券代码
            "WTFX": direction,
            "WTJGLX": get_pb_order_type(order.exchange, order.type),  # 委托价格类型
            "WTJG": round(order.price, 4),  # 委托价格
            "WTSL": int(order.volume),  # 委托数量
            "WBZDYXH": local_orderid  # 第三方系统自定义号
        }

        # 更新所有字段得长度
        order_data = format_dict(data, SEND_ORDER_FIELDS)

        append_data(file_name=csv_file,
                    dict_data=order_data,
                    field_names=list(SEND_ORDER_FIELDS.keys()),
                    auto_header=False,
                    encoding='gbk')

        # 设置状态为提交中
        order.status = Status.SUBMITTING
        # 添加待检查列表
        self.unchecked_orderids.append(local_orderid)
        # 登记并发送on_order事件
        self.gateway.write_log(f'send_order,提交csv下单:{order.__dict__}')
        self.gateway.order_manager.on_order(order)

        # 添加定时检查任务
        if self.check_send_order_csv not in self.gateway.query_functions:
            self.gateway.write_log(f'添加定时检查到任务队列中')
            self.gateway.query_functions.append(self.check_send_order_csv)

        return order.vt_orderid

    def cancel_order(self, req: CancelRequest):
        if self.gateway.file_type == 'dbf':
            return self.cancel_order_dbf(req)
        else:
            return self.cancel_order_csv(req)

    def cancel_order_dbf(self, req: CancelRequest):
        """
        dbf文件撤单
        :param req:
        :return:
        """
        self.gateway.write_log(f'dbf委托撤单:{req.__dict__}')
        try:
            # 获取订单
            order = self.gateway.order_manager.get_order_with_local_orderid(local_orderid=req.orderid)

            # 订单不存在
            if order is None:
                self.gateway.write_error(f'订单{req.orderid}不存在, 撤单失败')
                return False

            # 或者已经全部成交，已经被拒单，已经撤单
            if order.status in [Status.ALLTRADED, Status.REJECTED, Status.CANCELLING,
                                Status.CANCELLED]:
                self.gateway.write_error(f'订单{req.orderid}存在, 状态为:{order.status}, 不能再撤单')
                return False

            sys_orderid = self.gateway.order_manager.get_sys_orderid(req.orderid)

            if sys_orderid is None or len(sys_orderid) == 0 or sys_orderid == 'None':
                self.gateway.write_error(f'订单{req.orderid}=》系统委托id:{sys_orderid}不存在，撤单失败')
                return False

            data = (
                int(sys_orderid),  # 委托序号
                None,  # "JYSC": "C3",  # 交易市场
                None,  # "ZQDM": "C16",  # 证券代码
                None,  # "CDCGBZ": "C1",  # 撤单成功标志
                None,  # "SBYY": "C254",  # 失败原因
                None,  # "CLBZ": "C1",  # 处理标志
                None,  # "BYZD": "C2",  # 备用字段
                None  # "BYZD2": "C16",  # 备用字段2
            )

            dbf_file = os.path.abspath(os.path.join(self.order_folder,
                                                    '{}{}.dbf'.format(PB_FILE_NAMES.get('cancel_order'),
                                                                      self.trading_date)))

            # 打开dbf文件=》table
            table = dbf.Table(dbf_file)
            # 读取、写入模式
            table.open(dbf.READ_WRITE)
            # 写入数据
            table.append(data)
            # 关闭dbf文件
            table.close()
            self.warning_dict.pop('cancel_order', None)
            return True
        except Exception as ex:

            err_msg = f'dbf委托撤单异常:{str(ex)}'
            tra_msg = traceback.format_exc()
            err_info = self.warning_dict.get('cancel_order', {})
            err_count = err_info.get('err_count', 1)
            if err_count > 10:
                self.gateway.write_error(err_msg)
                self.gateway.write_error(tra_msg)
            else:
                err_count += 1
                err_info.update({'err_count': err_count, 'err_msg': err_msg, 'tra_msg': tra_msg})
                self.warning_dict.update({'cancel_order': err_info})

            return False

    def cancel_order_csv(self, req: CancelRequest):
        """csv文件撤单"""
        self.gateway.write_log(f'处理撤单请求{req.__dict__}')

        # 获取订单
        order = self.gateway.order_manager.get_order_with_local_orderid(local_orderid=req.orderid)

        # 订单不存在
        if order is None:
            self.gateway.write_log(f'订单{req.orderid}不存在, 撤单失败')
            return False

        # 或者已经全部成交，已经被拒单，已经撤单
        if order.status in [Status.ALLTRADED, Status.REJECTED, Status.CANCELLING,
                            Status.CANCELLED]:
            self.gateway.write_log(f'订单{req.orderid}存在, 状态为:{order.status}, 不能再撤单')
            return False

        sys_orderid = self.gateway.order_manager.get_sys_orderid(req.orderid)

        if len(sys_orderid) == 0:
            self.gateway.write_log(f'订单{req.orderid}=》系统委托id不存在，撤单失败')
            return False

        data = {
            "WTXH": sys_orderid,  # 委托序号
        }
        # 更新所有字段得长度
        cancel_data = format_dict(data, CANCEL_ORDER_FIELDS)

        csv_file = os.path.abspath(os.path.join(self.order_folder,
                                                '{}{}.csv'.format(PB_FILE_NAMES.get('cancel_order'),
                                                                  self.trading_date)))
        append_data(file_name=csv_file,
                    dict_data=cancel_data,
                    field_names=list(CANCEL_ORDER_FIELDS.keys()),
                    auto_header=False,
                    encoding='gbk')
        return True

    def cancel_all(self):
        if self.gateway.file_type == 'dbf':
            return self.cancel_all_dbf()
        else:
            return self.cancel_all_csv()

    def cancel_all_dbf(self):
        """dbf文件全策略单d"""
        # XHPT_WTCX委托的dbf文件
        orders_dbf = os.path.abspath(os.path.join(self.order_folder,
                                                  '{}{}.dbf'.format(
                                                      PB_FILE_NAMES.get('update_orders'),
                                                      self.trading_date)))

        cancel_dbf = os.path.abspath(os.path.join(self.order_folder,
                                                  '{}{}.dbf'.format(PB_FILE_NAMES.get('cancel_order'),
                                                                    self.trading_date)))

        # dbf => 所有委托记录
        try:
            # dbf => 所有成交记录
            self.gateway.write_log(f'全撤单，扫描所有委托查询记录:{orders_dbf}')
            orders_table = dbf.Table(orders_dbf, codepage='cp936')
            orders_table.open(dbf.READ_ONLY)

            cancel_table = dbf.Table(cancel_dbf, codepage='cp936')
            cancel_table.open(dbf.READ_WRITE)

            for data in orders_table:
                # 委托状态=>
                order_status = STATUS_PB2VT.get(str(data.wtzt))
                # 恒生平台返回的委托序号
                sys_orderid = str(data.wtxh)

                if order_status in [Status.NOTTRADED] and len(sys_orderid) > 0:
                    self.gateway.write_log(f'撤单：{data.wtxh}')
                    cancel_data = (int(sys_orderid), None, None, None, None, None, None, None)
                    cancel_table.append(cancel_data)

            orders_table.close()
            cancel_table.close()

        except Exception as ex:
            self.gateway.write_error(f'dbf全委托撤单异常:{str(ex)}')
            self.gateway.write_error(traceback.format_exc())
            return False

    def cancel_all_csv(self):
        pass


class TqMdApi():
    """天勤行情API"""

    def __init__(self, gateway):
        """"""
        super().__init__()

        self.gateway = gateway
        self.gateway_name = gateway.gateway_name

        self.api = None
        self.is_connected = False
        self.subscribe_array = []
        # 行情对象列表
        self.quote_objs = []

        # 数据更新线程
        self.update_thread = None
        # 所有的合约
        self.all_instruments = []

        self.ticks = {}

    def connect(self, setting={}):
        """"""
        if self.api and self.is_connected:
            self.gateway.write_log(f'天勤行情已经接入，无需重新连接')
            return
        try:
            from tqsdk import TqApi
            self.api = TqApi(_stock=True, url="wss://api.shinnytech.com/t/nfmd/front/mobile")
        except Exception as e:
            self.gateway.write_log(f'天勤股票行情API接入异常:'.format(str(e)))
            self.gateway.write_log(traceback.format_exc())
        if self.api:
            self.is_connected = True
            self.gateway.write_log(f'天勤股票行情API已连接')
            self.update_thread = Thread(target=self.update)
            self.update_thread.start()

    def generate_tick_from_quote(self, vt_symbol, quote) -> TickData:
        """
        生成TickData
        """
        # 清洗 nan
        quote = {k: 0 if v != v else v for k, v in quote.items()}
        symbol, exchange = extract_vt_symbol(vt_symbol)
        return TickData(
            symbol=symbol,
            exchange=exchange,
            datetime=datetime.strptime(quote["datetime"], "%Y-%m-%d %H:%M:%S.%f"),
            name=symbol,
            volume=quote["volume"],
            open_interest=quote["open_interest"],
            last_price=quote["last_price"],
            limit_up=quote["upper_limit"],
            limit_down=quote["lower_limit"],
            open_price=quote["open"],
            high_price=quote["highest"],
            low_price=quote["lowest"],
            pre_close=quote["pre_close"],
            bid_price_1=quote["bid_price1"],
            bid_price_2=quote["bid_price2"],
            bid_price_3=quote["bid_price3"],
            bid_price_4=quote["bid_price4"],
            bid_price_5=quote["bid_price5"],
            ask_price_1=quote["ask_price1"],
            ask_price_2=quote["ask_price2"],
            ask_price_3=quote["ask_price3"],
            ask_price_4=quote["ask_price4"],
            ask_price_5=quote["ask_price5"],
            bid_volume_1=quote["bid_volume1"],
            bid_volume_2=quote["bid_volume2"],
            bid_volume_3=quote["bid_volume3"],
            bid_volume_4=quote["bid_volume4"],
            bid_volume_5=quote["bid_volume5"],
            ask_volume_1=quote["ask_volume1"],
            ask_volume_2=quote["ask_volume2"],
            ask_volume_3=quote["ask_volume3"],
            ask_volume_4=quote["ask_volume4"],
            ask_volume_5=quote["ask_volume5"],
            gateway_name=self.gateway_name
        )

    def update(self) -> None:
        """
        更新行情/委托/账户/持仓
        """
        while self.api.wait_update():

            # 更新行情信息
            for vt_symbol, quote in self.quote_objs:
                if self.api.is_changing(quote):
                    tick = self.generate_tick_from_quote(vt_symbol, quote)
                    tick and self.gateway.on_tick(tick) and self.gateway.on_custom_tick(tick)

    def subscribe(self, req: SubscribeRequest) -> None:
        """
        订阅行情
        """
        if req.vt_symbol not in self.subscribe_array:
            symbol, exchange = extract_vt_symbol(req.vt_symbol)
            try:
                quote = self.api.get_quote(f'{exchange.value}.{symbol}')
                self.quote_objs.append((req.vt_symbol, quote))
                self.subscribe_array.append(req.vt_symbol)
            except Exception as ex:
                self.gateway.write_log('订阅天勤行情异常:{}'.format(str(ex)))

    def query_history(self, req: HistoryRequest) -> List[BarData]:
        """
        获取历史数据
        """
        symbol = req.symbol
        exchange = req.exchange
        interval = req.interval
        start = req.start
        end = req.end
        # 天勤需要的数据
        tq_symbol = f'{exchange.value}.{symbol}'
        tq_interval = INTERVAL_VT2TQ.get(interval)
        end += timedelta(1)
        total_days = end - start
        # 一次最多只能下载 8964 根Bar
        min_length = min(8964, total_days.days * 500)
        df = self.api.get_kline_serial(tq_symbol, tq_interval, min_length).sort_values(
            by=["datetime"]
        )

        # 时间戳对齐
        df["datetime"] = pd.to_datetime(df["datetime"] + TIME_GAP)

        # 过滤开始结束时间
        df = df[(df["datetime"] >= start - timedelta(days=1)) & (df["datetime"] < end)]

        data: List[BarData] = []
        if df is not None:
            for ix, row in df.iterrows():
                bar = BarData(
                    symbol=symbol,
                    exchange=exchange,
                    interval=interval,
                    datetime=row["datetime"].to_pydatetime(),
                    open_price=row["open"],
                    high_price=row["high"],
                    low_price=row["low"],
                    close_price=row["close"],
                    volume=row["volume"],
                    open_interest=row.get("close_oi", 0),
                    gateway_name=self.gateway_name,
                )
                data.append(bar)
        return data

    def close(self) -> None:
        """"""
        try:
            if self.api and self.api.wait_update():
                self.api.close()
                self.is_connected = False
                if self.update_thread:
                    self.update_thread.join()
        except Exception as e:
            self.gateway.write_log('退出天勤行情api异常:{}'.format(str(e)))
