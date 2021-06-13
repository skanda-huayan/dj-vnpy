# 同花顺交易客户端 + easytrader 接口
# 大佳 28888502

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
from vnpy.api.easytrader.remoteclient import use as easytrader_use

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

EXCHANGE_NAME2VT: Dict[str, Exchange] = {
    "上交所A": Exchange.SSE,
    "深交所A": Exchange.SZSE,
    "上A": Exchange.SSE,
    "深A": Exchange.SZSE
}

DIRECTION_STOCK_NAME2VT: Dict[str, Any] = {
    "证券卖出": Direction.SHORT,
    "证券买入": Direction.LONG,
    "卖出": Direction.SHORT,
    "买入": Direction.LONG,
    "债券买入": Direction.LONG,
    "债券卖出": Direction.SHORT,
    "申购": Direction.LONG,
    "申购确认": Direction.LONG
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


ORDERTYPE_NAME2VT: Dict[str, OrderType] = {
    "五档即成剩撤": OrderType.MARKET,
    "五档即成剩转": OrderType.MARKET,
    "即成剩撤": OrderType.MARKET,
    "对手方最优": OrderType.MARKET,
    "本方最优": OrderType.MARKET,
    "限价单": OrderType.LIMIT,
}

STATUS_NAME2VT: Dict[str, Status] = {
    "未成交": Status.NOTTRADED,
    "未报": Status.SUBMITTING,
    "待报": Status.SUBMITTING,
    "正报": Status.SUBMITTING,
    "已报": Status.NOTTRADED,
    "废单": Status.REJECTED,
    "部成": Status.PARTTRADED,
    "已成": Status.ALLTRADED,
    "全部成交": Status.ALLTRADED,
    "部撤": Status.CANCELLED,
    "已撤": Status.CANCELLED,
    "全部撤单": Status.CANCELLED,
    "待撤": Status.CANCELLING,
    "已报待撤": Status.CANCELLING,
    "未审批": Status.UNKNOWN,
    "审批拒绝": Status.UNKNOWN,
    "未审批即撤销": Status.UNKNOWN,
}

STOCK_CONFIG_FILE = 'tdx_stock_config.pkb2'
from pytdx.hq import TdxHq_API
# 通达信股票行情
from vnpy.data.tdx.tdx_common import get_cache_config, get_tdx_market_code
from pytdx.config.hosts import hq_hosts
from pytdx.params import TDXParams
from vnpy.trader.util_wechat import send_wx_msg

class ThsGateway(BaseGateway):
    """同花顺股票gateway"""

    default_setting: Dict[str, Any] = {
        "资金账号": "",
        "登录密码": "",
        "RPC IP": "localhost",
        "RPC Port": 1430
    }

    # 接口支持得交易所清单
    exchanges: List[Exchange] = [Exchange.SSE, Exchange.SZSE]

    def __init__(self, event_engine: EventEngine, gateway_name='GJ'):
        """构造函数"""
        super().__init__(event_engine, gateway_name=gateway_name)

        # tdx 基础股票数据+行情
        self.md_api = TdxMdApi(self)
        # easytrader交易接口
        self.td_api = ThsTdApi(self)
        # 天勤行情
        self.tq_api = None
        # 通达信是否连接成功
        self.tdx_connected = False  # 通达信行情API得连接状态

    def connect(self, setting: dict) -> None:
        """连接"""
        userid = setting["资金账号"]
        password = setting["登录密码"]

        # 运行easytrader restful 服务端的IP地址、端口
        host = setting["RPC IP"]
        port = setting["RPC Port"]
        force_restart = setting.get('强制重启',False)

        self.md_api.connect()
        self.td_api.connect(user_id=userid,
                            user_pwd=password,
                            host=host,
                            port=port,
                            force_restart=force_restart)
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
        return self.td_api.cancel_order(req)

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
        """定时器"""
        self.count += 1
        # 8秒，不要太快
        if self.count < 8:
            return
        self.count = 0

        func = self.query_functions.pop(0)
        func()
        self.query_functions.append(func)

    def init_query(self) -> None:
        """初始化查询"""
        self.count = 0
        self.query_functions = [self.query_account, self.query_position, self.query_orders, self.query_trades]
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)

    def reset_query(self) -> None:
        """重置查询（在委托发单、撤单后），优先查询订单和交易"""
        self.count = 0
        self.query_functions = [self.query_orders, self.query_trades, self.query_account, self.query_position]


class TdxMdApi(object):
    """通达信行情和基础数据"""

    def __init__(self, gateway: ThsGateway):
        """"""
        super().__init__()

        self.gateway: ThsGateway = gateway
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

            # 排除通达信的指数代码
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
                    self.gateway.write_log('tdx[{}] cp{}, symbols:{},last_dt:{}'.format(i, dt, symbols, self.last_tick_dt))
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


class ThsTdApi(object):
    """同花顺的easytrader交易接口"""

    def __init__(self, gateway: ThsGateway):
        """"""
        super().__init__()
        self.gateway: ThsGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.userid: str = ""  # 资金账号
        self.password: str = ""  # 登录密码
        self.host: str = "127.0.0.1"
        self.port: int = 1430

        # easytrader 对应的API
        self.api = None

        # 缓存了当前交易日
        self.trading_day = datetime.now().strftime('%Y-%m-%d')  # 格式 2020-01-13
        self.trading_date = self.trading_day.replace('-', '')  # 格式 20200113

        self.connect_status: bool = False
        self.login_status: bool = False

        # 所有交易
        self.trades = {}  # tradeid: trade
        # 本gateway的委托
        self.orders = {}  # sys_orderid: order

        self.error_counts = 0

    def close(self):
        self.api = None

    def connect(self, user_id, user_pwd, host, port, force_restart=False):
        """连接"""
        self.userid = user_id
        self.password = user_pwd
        self.rpc_host = host
        self.rpc_port = port

        # 创建 easy客户端
        self.api = easytrader_use(broker='pingan_ths', host=self.rpc_host, port=self.rpc_port)

        # 输入参数(资金账号、密码）
        self.api.prepare(exe_path='C:\\THS\\xiadan.exe', user=self.userid, password=self.password,force_restart=force_restart)

        self.login_status = True

    def reconnect(self):
        """连接"""
        self.gateway.write_log(f'重新连接')
        # 创建 easy客户端
        self.api = easytrader_use(broker='pingan_ths', host=self.rpc_host, port=self.rpc_port)

        # 输入参数(资金账号、密码）
        self.api.prepare(exe_path='C:\\THS\\xiadan.exe', user=self.userid, password=self.password,force_restart=True)

        self.login_status = True

        self.error_counts = 0

    def update_error(self):
        if self.error_counts > 30:
            msg = f'累计错误超过30，强制重新连接'
            self.gateway.write_log(msg)
            send_wx_msg(content=msg, level='CRITICAL')
            self.reconnect()
        else:
            self.error_counts +=1

    def query_account(self):
        """获取资金账号信息"""

        if not self.api:
            return
        try:
            data = self.api.balance
            if not isinstance(data, dict):
                return
            if '总资产' not in data:
                return
            account = AccountData(
                gateway_name=self.gateway_name,
                accountid=self.userid,
                balance=float(data["总资产"]),
                frozen=float(data["总资产"]) - float(data["资金余额"]),
                currency="人民币",
                trading_day=self.trading_day
            )
            self.gateway.on_account(account)

        except Exception as ex:
            msg = f'获取ths资产异常:{str(ex)}'
            self.gateway.write_error(msg)
            send_wx_msg(content=msg, level='ERROR')
            self.update_error()

    def query_position(self):
        """获取持仓信息"""
        if not self.api:
            return
        try:
            for data in self.api.position:

                if not isinstance(data, dict):
                    continue
                symbol = data.get("证券代码", None)
                if not symbol:
                    continue

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
                    symbol=symbol,
                    exchange=exchange,
                    direction=Direction.NET,
                    name=name,
                    volume=int(data["持仓数量"]),
                    yd_volume=int(data["可用数量"]),
                    frozen=int(data["冻结数量"]),
                    price=float(data["参考成本价"]),
                    cur_price=float(data["当前价"]),
                    pnl=float(data["浮动盈亏"])
                )
                self.gateway.on_position(position)
        except Exception as ex:
            msg = f'获取ths持仓异常:{str(ex)}'
            self.gateway.write_error(msg)
            send_wx_msg(content=msg, level='ERROR')
            self.update_error()

    def query_orders(self):
        """获取所有委托"""
        if not self.api:
            return
        try:
            for data in self.api.today_entrusts:

                if not isinstance(data, dict):
                    continue

                sys_orderid = str(data.get("合同编号", ''))
                if not sys_orderid:
                    continue

                # 检查是否存在本地缓存中
                order = self.orders.get(sys_orderid, None)
                order_date = datetime.now().strftime("%Y%m%d") #data["委托日期"]  # 20170313
                order_time = data["委托时间"]  # '09:40:30'
                order_status = STATUS_NAME2VT.get(data["备注"])

                if order:
                    if order_status == order.status and order.traded == float(data["成交数量"]):
                        continue
                    order.status = order_status
                    order.traded = float(data["成交数量"])
                # 委托单不存在本地映射库
                else:
                    # 不处理以下状态
                    # if order_status in [Status.SUBMITTING, Status.REJECTED, Status.CANCELLED, Status.CANCELLING]:
                    #     continue
                    self.gateway.write_log(f'委托单不存在本地映射库:{print_dict(data)}')
                    order_dt = datetime.strptime(f'{order_date} {order_time}', "%Y%m%d %H:%M:%S")
                    direction = DIRECTION_STOCK_NAME2VT.get(data["操作"])
                    symbol = data.get("证券代码")
                    if not symbol:
                        continue
                    exchange = Exchange(get_stock_exchange(symbol))
                    if not exchange:
                        continue

                    if direction is None:
                        direction = Direction.NET
                        offset = Offset.NONE
                    else:
                        if direction == Direction.LONG:
                            offset = Offset.OPEN
                        else:
                            offset = Offset.CLOSE
                    order = OrderData(
                        gateway_name=self.gateway_name,
                        symbol=symbol,
                        exchange=exchange,
                        orderid=sys_orderid,
                        sys_orderid=sys_orderid,
                        accountid=self.userid,
                        type=ORDERTYPE_NAME2VT.get(data.get("价格类型"), OrderType.LIMIT),
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
                    self.gateway.write_log(f'账号订单查询，新增:{order.__dict__}')

                self.orders[order.orderid] = order
                self.gateway.on_order(copy.deepcopy(order))

                continue

        except Exception as ex:
            msg = f'获取ths所有委托异常:{str(ex)}'
            self.gateway.write_error(msg)
            send_wx_msg(content=msg, level='ERROR')
            self.update_error()

    def query_trades(self):
        """获取所有成交"""

        if not self.api:
            return
        try:
            for data in self.api.today_trades:
                if not isinstance(data, dict):
                    continue
                sys_orderid = str(data.get("合同编号", ""))
                sys_tradeid = str(data.get("成交编号", ""))
                if not sys_orderid:
                    continue
                # 检查是否存在本地trades缓存中
                trade = self.trades.get(sys_tradeid, None)
                order = self.orders.get(sys_orderid, None)

                # 如果交易不再本地映射关系
                if trade is None: # and order is None:
                    trade_date = self.trading_day
                    trade_time = data["成交时间"]
                    trade_dt = datetime.strptime(f'{trade_date} {trade_time}', "%Y-%m-%d %H:%M:%S")
                    symbol = data.get('证券代码')
                    exchange = Exchange(get_stock_exchange(symbol))
                    direction = DIRECTION_STOCK_NAME2VT.get(data["操作"])
                    if direction == Direction.LONG:
                        offset = Offset.OPEN
                    elif direction == Direction.SHORT:
                        offset = Offset.CLOSE
                    else:
                        offset = Offset.NONE
                    trade = TradeData(
                        gateway_name=self.gateway_name,
                        symbol=symbol,
                        exchange=exchange,
                        orderid=sys_orderid,
                        tradeid=sys_tradeid,
                        sys_orderid=sys_orderid,
                        accountid=self.userid,
                        direction=direction,
                        offset=offset,
                        price=float(data["成交均价"]),
                        volume=float(data["成交数量"]),
                        datetime=trade_dt,
                        time=trade_dt.strftime('%H:%M:%S'),
                        trade_amount=float(data["成交金额"]),
                        commission=0
                    )
                    self.trades[sys_tradeid] = trade
                    self.gateway.on_trade(copy.copy(trade))
                    continue

        except Exception as ex:
            msg = f'获取ths所有成交异常:{str(ex)}'
            self.gateway.write_error(msg)
            send_wx_msg(content=msg, level='ERROR')
            self.update_error()

    def send_order(self, req: OrderRequest):
        """委托发单"""
        self.gateway.write_log(f'委托发单:{req.__dict__}')

        try:
            if req.direction == Direction.LONG:
                ret = self.api.buy(req.symbol, price=req.price, amount=req.volume)
            else:
                ret = self.api.sell(req.symbol, price=req.price, amount=req.volume)

            if isinstance(ret, dict) and 'entrust_no' in ret:
                sys_orderid = str(ret['entrust_no'])
                # req => order
                order = req.create_order_data(orderid=sys_orderid, gateway_name=self.gateway_name)
                if order.direction == Direction.LONG:
                    order.offset = Offset.OPEN
                elif order.direction == Direction.SHORT:
                    order.offset = Offset.CLOSE
                #order.offset = Offset.NONE
                order.sys_orderid = sys_orderid
                order.accountid = self.userid
                # 设置状态为提交中
                order.status = Status.SUBMITTING
                # 重置查询
                self.gateway.reset_query()
                # 登记并发送on_order事件
                self.gateway.write_log(f'send_order，提交easytrader委托:{order.__dict__}')
                self.orders[sys_orderid] = order
                self.gateway.on_order(order)
                return order.vt_orderid
            else:
                self.gateway.write_error('返回异常:{ret}')
                return ""
        except Exception as ex:
            self.gateway.write_error(f'委托异常:{str(ex)}')
            self.update_error()
            return ""

    def cancel_order(self, req: CancelRequest):
        """
        撤单
        :param req:
        :return:
        """
        self.gateway.write_log(f'委托撤单:{req.__dict__}')
        if not self.api:
            return False

        # 获取订单
        order = self.orders.get(req.orderid, None)

        # 订单不存在
        if order is None:
            self.gateway.write_log(f'订单{req.orderid}不存在, 撤单失败')
            return False

        # 或者已经全部成交，已经被拒单，已经撤单
        if order.status in [Status.ALLTRADED, Status.REJECTED, Status.CANCELLING,
                            Status.CANCELLED]:
            self.gateway.write_log(f'订单{req.orderid}存在, 状态为:{order.status}, 不能再撤单')
            return False
        try:
            ret = self.api.cancel_entrust(order.sys_orderid)

            if 'success' in ret.get('message', ''):
                # 重置查询
                self.gateway.reset_query()
                return True
            else:
                self.gateway.write_error('委托撤单失败:{}'.format(ret.get('message')))
                return False

        except Exception as ex:
            self.gateway.write_error(f'撤单异常:{str(ex)}')
            return False

    def cancel_all(self):
        """
        全撤单
        :return:
        """
        self.gateway.write_log(f'全撤单')
        if not self.api:
            return
        for order in self.orders.values():
            if order.status in [Status.SUBMITTING, Status.NOTTRADED, Status.PARTTRADED, Status.UNKNOWN] \
                    and order.sys_orderid:
                ret = self.api.cancel_entrust(order.sys_orderid)


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
