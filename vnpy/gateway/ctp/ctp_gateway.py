"""
"""
import sys
import traceback
import json
from datetime import datetime, timedelta
from time import time
from copy import copy, deepcopy
from functools import lru_cache
from typing import List
import pandas as pd

from vnpy.api.ctp import (
    MdApi,
    TdApi,
    THOST_FTDC_OAS_Submitted,
    THOST_FTDC_OAS_Accepted,
    THOST_FTDC_OAS_Rejected,
    THOST_FTDC_OST_NoTradeQueueing,
    THOST_FTDC_OST_PartTradedQueueing,
    THOST_FTDC_OST_AllTraded,
    THOST_FTDC_OST_Canceled,
    THOST_FTDC_D_Buy,
    THOST_FTDC_D_Sell,
    THOST_FTDC_PD_Long,
    THOST_FTDC_PD_Short,
    THOST_FTDC_OPT_LimitPrice,
    THOST_FTDC_OPT_AnyPrice,
    THOST_FTDC_OF_Open,
    THOST_FTDC_OFEN_Close,
    THOST_FTDC_OFEN_CloseYesterday,
    THOST_FTDC_OFEN_CloseToday,
    THOST_FTDC_PC_Futures,
    THOST_FTDC_PC_Options,
    THOST_FTDC_PC_SpotOption,
    THOST_FTDC_PC_Combination,
    THOST_FTDC_CP_CallOptions,
    THOST_FTDC_CP_PutOptions,
    THOST_FTDC_HF_Speculation,
    THOST_FTDC_CC_Immediately,
    THOST_FTDC_FCC_NotForceClose,
    THOST_FTDC_TC_GFD,
    THOST_FTDC_VC_AV,
    THOST_FTDC_TC_IOC,
    THOST_FTDC_VC_CV,
    THOST_FTDC_AF_Delete
)
from vnpy.trader.constant import (
    Direction,
    Offset,
    Exchange,
    OrderType,
    Product,
    Status,
    OptionType,
    Interval
)
from vnpy.trader.gateway import BaseGateway, TickCombiner, IndexGenerator
from vnpy.trader.object import (
    TickData,
    BarData,
    OrderData,
    TradeData,
    PositionData,
    AccountData,
    ContractData,
    OrderRequest,
    CancelRequest,
    SubscribeRequest,
    HistoryRequest
)
from vnpy.trader.utility import (
    extract_vt_symbol,
    get_folder_path,
    get_trading_date,
    get_underlying_symbol,
    round_to,
    BarGenerator,
    print_dict
)
from vnpy.trader.event import EVENT_TIMER

from vnpy.api.websocket import WebsocketClient

# 增加通达信指数接口行情
from time import sleep
from threading import Thread
from pytdx.exhq import TdxExHq_API
from vnpy.amqp.consumer import subscriber
from vnpy.data.tdx.tdx_common import (
    TDX_FUTURE_HOSTS,
    get_future_contracts,
    save_future_contracts,
    get_cache_json,
    save_cache_json,
    TDX_FUTURE_CONFIG)
from vnpy.component.base import (
    MARKET_DAY_ONLY, NIGHT_MARKET_23, NIGHT_MARKET_SQ2
)

STATUS_CTP2VT = {
    THOST_FTDC_OAS_Submitted: Status.SUBMITTING,
    THOST_FTDC_OAS_Accepted: Status.SUBMITTING,
    THOST_FTDC_OAS_Rejected: Status.REJECTED,
    THOST_FTDC_OST_NoTradeQueueing: Status.NOTTRADED,
    THOST_FTDC_OST_PartTradedQueueing: Status.PARTTRADED,
    THOST_FTDC_OST_AllTraded: Status.ALLTRADED,
    THOST_FTDC_OST_Canceled: Status.CANCELLED
}

DIRECTION_VT2CTP = {
    Direction.LONG: THOST_FTDC_D_Buy,
    Direction.SHORT: THOST_FTDC_D_Sell
}
DIRECTION_CTP2VT = {v: k for k, v in DIRECTION_VT2CTP.items()}
DIRECTION_CTP2VT[THOST_FTDC_PD_Long] = Direction.LONG
DIRECTION_CTP2VT[THOST_FTDC_PD_Short] = Direction.SHORT

ORDERTYPE_VT2CTP = {
    OrderType.LIMIT: THOST_FTDC_OPT_LimitPrice,
    OrderType.MARKET: THOST_FTDC_OPT_AnyPrice
}
ORDERTYPE_CTP2VT = {v: k for k, v in ORDERTYPE_VT2CTP.items()}

OFFSET_VT2CTP = {
    Offset.OPEN: THOST_FTDC_OF_Open,
    Offset.CLOSE: THOST_FTDC_OFEN_Close,
    Offset.CLOSETODAY: THOST_FTDC_OFEN_CloseToday,
    Offset.CLOSEYESTERDAY: THOST_FTDC_OFEN_CloseYesterday,
}
OFFSET_CTP2VT = {v: k for k, v in OFFSET_VT2CTP.items()}

EXCHANGE_CTP2VT = {
    "CFFEX": Exchange.CFFEX,
    "SHFE": Exchange.SHFE,
    "CZCE": Exchange.CZCE,
    "DCE": Exchange.DCE,
    "INE": Exchange.INE,
    "SPD": Exchange.SPD

}

PRODUCT_CTP2VT = {
    THOST_FTDC_PC_Futures: Product.FUTURES,
    THOST_FTDC_PC_Options: Product.OPTION,
    THOST_FTDC_PC_SpotOption: Product.OPTION,
    THOST_FTDC_PC_Combination: Product.SPREAD
}

OPTIONTYPE_CTP2VT = {
    THOST_FTDC_CP_CallOptions: OptionType.CALL,
    THOST_FTDC_CP_PutOptions: OptionType.PUT
}

MAX_FLOAT = sys.float_info.max

symbol_exchange_map = {}
option_name_map = {}
symbol_name_map = {}
symbol_size_map = {}
index_contracts = {}
# tdx 期货配置本地缓存
future_contracts = get_future_contracts()

# 时间戳对齐
TIME_GAP = 8 * 60 * 60 * 1000000000
INTERVAL_VT2TQ = {
    Interval.MINUTE: 60,
    Interval.HOUR: 60 * 60,
    Interval.DAILY: 60 * 60 * 24,
}

TQ2VT_TYPE = {
    "FUTURE_OPTION": Product.OPTION,
    "INDEX": Product.INDEX,
    "FUTURE_COMBINE": Product.SPREAD,
    "SPOT": Product.SPOT,
    "FUTURE_CONT": Product.INDEX,
    "FUTURE": Product.FUTURES,
    "FUTURE_INDEX": Product.INDEX,
    "OPTION": Product.OPTION,
}


@lru_cache(maxsize=9999)
def vt_to_tq_symbol(symbol: str, exchange: Exchange) -> str:
    """
    TQSdk exchange first
    """
    for count, word in enumerate(symbol):
        if word.isdigit():
            break

    fix_symbol = symbol
    if exchange in [Exchange.INE, Exchange.SHFE, Exchange.DCE]:
        fix_symbol = symbol.lower()

    # Check for index symbol
    time_str = symbol[count:]

    if time_str in ["88"]:
        return f"KQ.m@{exchange.value}.{fix_symbol[:count]}"
    if time_str in ["99"]:
        return f"KQ.i@{exchange.value}.{fix_symbol[:count]}"

    return f"{exchange.value}.{fix_symbol}"


@lru_cache(maxsize=9999)
def tq_to_vt_symbol(tq_symbol: str) -> str:
    """"""
    if "KQ.m" in tq_symbol:
        ins_type, instrument = tq_symbol.split("@")
        exchange, symbol = instrument.split(".")
        return f"{symbol}88.{exchange}"
    elif "KQ.i" in tq_symbol:
        ins_type, instrument = tq_symbol.split("@")
        exchange, symbol = instrument.split(".")
        return f"{symbol}99.{exchange}"
    else:
        exchange, symbol = tq_symbol.split(".")
        return f"{symbol}.{exchange}"


class CtpGateway(BaseGateway):
    """
    VN Trader Gateway for CTP .
    """

    default_setting = {
        "用户名": "",
        "密码": "",
        "经纪商代码": "",
        "交易服务器": "",
        "行情服务器": "",
        "产品名称": "",
        "授权编码": "",
        "产品信息": ""
    }
    # 注
    # 如果采用rabbit_mq拓展tdx指数行情，default_setting中，需要增加:
    # "rabbit":
    # {
    #    "host": "192.168.1.211",
    #    "exchange": "x_fanout_idx_tick"
    # }
    exchanges = list(EXCHANGE_CTP2VT.values())

    def __init__(self, event_engine, gateway_name="CTP"):
        """Constructor"""
        super().__init__(event_engine, gateway_name)

        self.td_api = None
        self.md_api = None
        self.l2_md_api = None
        self.tdx_api = None
        self.rabbit_api = None
        self.tq_api = None

        self.subscribed_symbols = set()  # 已订阅合约代码

        self.combiner_conf_dict = {}  # 保存合成器配置
        # 自定义价差/加比的tick合成器
        self.combiners = {}
        self.tick_combiner_map = {}

        # 本地指数行情合成器{ 'rb2110':x, 'rb2201':x',,}
        self.index_generators = {}
        # 已经创建得指数合成器symbol列表 ['RB99','J99',,,]
        self.subscribed_index_symbols = []

    def connect(self, setting: dict):
        """
        连接交易服务器、行情服务器
        行情服务器包括：ctp普通行情、ctp5档行情（上海、能源所）、tdx指数行情、rabbitMQ指数行情、天勤指数行情
        :param setting:
        :return:
        """
        userid = setting["用户名"]
        password = setting["密码"]
        brokerid = setting["经纪商代码"]
        td_address = setting["交易服务器"]
        md_address = setting["行情服务器"]
        md_address_level2 = setting.get("行情服务器_五档", None)
        appid = setting["产品名称"]
        auth_code = setting["授权编码"]
        product_info = setting["产品信息"]
        rabbit_dict = setting.get('rabbit', None)
        tq_dict = setting.get('tq', None)
        tdx_dict = setting.get('tdx', None)
        if (
                (not td_address.startswith("tcp://"))
                and (not td_address.startswith("ssl://"))
        ):
            td_address = "tcp://" + td_address

        if (
                (not md_address.startswith("tcp://"))
                and (not md_address.startswith("ssl://"))
        ):
            md_address = "tcp://" + md_address
        if md_address_level2:
            if (
                    (not md_address_level2.startswith("tcp://"))
                    and (not md_address_level2.startswith("ssl://"))
            ):
                md_address_level2 = "tcp://" + md_address_level2

        # 获取自定义价差/价比合约的配置
        try:
            from vnpy.trader.engine import CustomContract
            c = CustomContract()
            self.combiner_conf_dict = c.get_config()
            if len(self.combiner_conf_dict) > 0:
                self.write_log(u'加载的自定义价差/价比配置:{}'.format(self.combiner_conf_dict))

                contract_dict = c.get_contracts()
                for vt_symbol, contract in contract_dict.items():
                    contract.gateway_name = self.gateway_name
                    symbol_exchange_map[contract.symbol] = contract.exchange
                    self.on_contract(contract)

        except Exception as ex:  # noqa
            pass
        if not self.td_api:
            self.td_api = CtpTdApi(self)
        self.td_api.connect(td_address, userid, password, brokerid, auth_code, appid, product_info)
        if not self.md_api:
            self.md_api = CtpMdApi(self)
        self.md_api.connect(md_address, userid, password, brokerid)

        if not self.l2_md_api and md_address_level2:
            self.write_log(f'激活五档行情配置:{md_address_level2}')
            self.l2_md_api = CtpMdApi(gateway=self, level2=True)
            self.l2_md_api.connect(md_address_level2, userid, password, brokerid)

        if rabbit_dict:
            self.write_log(f'激活RabbitMQ行情接口')
            self.rabbit_api = SubMdApi(gateway=self)
            self.rabbit_api.connect(rabbit_dict)
        elif tq_dict is not None:
            self.write_log(f'激活天勤行情接口')
            self.tq_api = TqMdApi(gateway=self)
            self.tq_api.connect(tq_dict)
        elif tdx_dict is not None:
            self.write_log(f'激活通达信行情接口')
            self.tdx_api = TdxMdApi(gateway=self)
            self.tdx_api.connect()

        self.init_query()

        for (vt_symbol, is_bar) in list(self.subscribed_symbols):
            symbol, exchange = extract_vt_symbol(vt_symbol)
            # 获取合约的缩写号
            underlying_symbol = get_underlying_symbol(vt_symbol)
            dt = datetime.now()
            # 若为中金所等的合约，白天才提交订阅请求
            if underlying_symbol in MARKET_DAY_ONLY and not (8 < dt.hour < 16):
                continue

            req = SubscribeRequest(
                symbol=symbol,
                exchange=exchange,
                is_bar=is_bar
            )
            # 指数合约，从tdx行情、天勤订阅
            if req.symbol[-2:] in ['99']:
                req.symbol = req.symbol.upper()
                if self.tdx_api is not None:
                    self.write_log(u'有指数订阅，连接通达信行情服务器')
                    self.tdx_api.connect()
                    self.tdx_api.subscribe(req)
                elif self.rabbit_api is not None:
                    # 使用rabbitmq获取
                    self.rabbit_api.subscribe(req)
                elif self.tq_api:
                    # 使用天勤行情获取
                    self.tq_api.subscribe(req)
            else:
                # 上期所、上能源支持五档行情，使用天勤接口
                if self.tq_api and req.exchange in [Exchange.SHFE, Exchange.INE]:
                    self.write_log(f'使用天勤接口订阅')
                    self.tq_api.subscribe(req)
                else:
                    self.md_api.subscribe(req)

    def check_status(self):
        """检查状态"""

        # 检查交易接口、行情接口的连接状态
        if self.td_api.connect_status and self.md_api.connect_status:
            self.status.update({'con': True})

        # 检查通达信行情接口（直接连通达信）
        if self.tdx_api:
            self.tdx_api.check_status()

        # 检查天勤行情接口
        if self.tq_api:
            self.tq_api.check_status()

        if not self.td_api.connect_status or self.md_api.connect_status:
            return False

        return True

    def subscribe(self, req: SubscribeRequest):
        """
        订阅合约行情
        普通合约 => ctp行情、5档行情
        指数合约 => 通达信、rabbitMQ，天勤
        套利合约 => 合约合并器
        :param req:
        :return:
        """
        try:
            if self.md_api:
                # 如果是自定义的套利合约符号
                if req.symbol in self.combiner_conf_dict:
                    self.write_log(u'订阅自定义套利合约:{}'.format(req.symbol))
                    # 创建合成器
                    if req.symbol not in self.combiners:
                        setting = self.combiner_conf_dict.get(req.symbol)
                        setting.update({"symbol": req.symbol})
                        combiner = TickCombiner(self, setting)
                        # 更新合成器
                        self.write_log(u'添加{}与合成器映射'.format(req.symbol))
                        self.combiners.update({setting.get('symbol'): combiner})

                        # 增加映射（ leg1 对应的合成器列表映射)
                        leg1_symbol = setting.get('leg1_symbol')
                        leg1_exchange = Exchange(setting.get('leg1_exchange'))
                        combiner_list = self.tick_combiner_map.get(leg1_symbol, [])
                        if combiner not in combiner_list:
                            self.write_log(u'添加Leg1:{}与合成器得映射'.format(leg1_symbol))
                            combiner_list.append(combiner)
                        self.tick_combiner_map.update({leg1_symbol: combiner_list})

                        # 增加映射（ leg2 对应的合成器列表映射)
                        leg2_symbol = setting.get('leg2_symbol')
                        leg2_exchange = Exchange(setting.get('leg2_exchange'))
                        combiner_list = self.tick_combiner_map.get(leg2_symbol, [])
                        if combiner not in combiner_list:
                            self.write_log(u'添加Leg2:{}与合成器得映射'.format(leg2_symbol))
                            combiner_list.append(combiner)
                        self.tick_combiner_map.update({leg2_symbol: combiner_list})

                        self.write_log(u'订阅leg1:{}'.format(leg1_symbol))
                        leg1_req = SubscribeRequest(
                            symbol=leg1_symbol,
                            exchange=leg1_exchange
                        )
                        self.subscribe(leg1_req)

                        self.write_log(u'订阅leg2:{}'.format(leg2_symbol))
                        leg2_req = SubscribeRequest(
                            symbol=leg2_symbol,
                            exchange=leg2_exchange
                        )
                        self.subscribe(leg2_req)

                        self.subscribed_symbols.add((req.vt_symbol, req.is_bar))
                    else:
                        self.write_log(u'{}合成器已经在存在'.format(req.symbol))
                    return
                elif req.exchange == Exchange.SPD:
                    self.write_error(u'自定义合约{}不在CTP设置中'.format(req.symbol))
                    return

                # 指数合约，从tdx行情订阅
                if req.symbol[-2:] in ['99']:
                    req.symbol = req.symbol.upper()
                    if self.tdx_api:
                        self.write_log(f'使用通达信接口订阅{req.symbol}')
                        self.tdx_api.subscribe(req)
                    elif self.rabbit_api:
                        self.write_log(f'使用RabbitMQ接口订阅{req.symbol}')
                        self.rabbit_api.subscribe(req)
                    elif self.tq_api:
                        self.write_log(f'使用天勤接口订阅{req.symbol}')
                        self.tq_api.subscribe(req)
                    else:
                        if req.symbol not in self.subscribed_index_symbols:
                            self.write_log(f'使用本地指数生成器进行订阅')
                            self.subscribe_local_index(req)

                else:
                    # 上期所、上能源支持五档行情，使用天勤接口
                    if self.tq_api and req.exchange in [Exchange.SHFE, Exchange.INE]:
                        self.write_log(f'使用天勤接口订阅{req.symbol}')
                        self.tq_api.subscribe(req)
                    if self.l2_md_api and req.exchange in [Exchange.SHFE, Exchange.INE]:
                        self.write_log(f'使用五档行情接口订阅:{req.symbol}')
                        self.l2_md_api.subscribe(req)

                    else:
                        #self.write_log(f'使用CTP接口订阅{req.symbol}')
                        self.md_api.subscribe(req)

            # Allow the strategies to start before the connection
            self.subscribed_symbols.add((req.vt_symbol, req.is_bar))
            if req.is_bar:
                self.subscribe_bar(req)

        except Exception as ex:
            self.write_error(u'订阅合约异常:{},{}'.format(str(ex), traceback.format_exc()))

    def subscribe_bar(self, req: SubscribeRequest):
        """订阅1分钟行情"""

        vt_symbol = req.vt_symbol
        if vt_symbol in self.klines:
            return

        # 创建1分钟bar产生器
        self.write_log(u'创建:{}的一分钟行情产生器'.format(vt_symbol))
        bg = BarGenerator(on_bar=self.on_bar)
        self.klines.update({vt_symbol: bg})

    def subscribe_local_index(self, req):
        """
        订阅本地合约
        :param req:
        :return:
        """
        underlying_symbol = get_underlying_symbol(req.symbol)
        symbol_info = future_contracts.get(underlying_symbol,None)
        if symbol_info:
            generator = IndexGenerator(gateway=self, setting=symbol_info)

            # 登记订阅真实合约 <=>合成器 关系
            for vn_symbol in generator.symbols:
                self.index_generators[vn_symbol] = generator

            # 登记指数合约到本地已订阅信息
            self.subscribed_index_symbols.append(req.symbol)
        else:
            self.write_error(f'{underlying_symbol}信息没有在vnpy/data/tdx/future_contracts.json文件中,不能创建指数订阅')

    def send_order(self, req: OrderRequest):
        """"""
        return self.td_api.send_order(req)

    def cancel_order(self, req: CancelRequest):
        """"""
        self.td_api.cancel_order(req)
        return True

    def query_account(self):
        """"""
        self.td_api.query_account()

    def query_position(self):
        """"""
        self.td_api.query_position()

    def query_history(self, req: HistoryRequest) -> List[BarData]:
        """查询K线历史"""
        if self.tq_api:
            return self.tq_api.query_history(req)
        else:
            return []

    def close(self):
        """"""
        if self.md_api:
            self.write_log('断开行情API',on_log=True)
            tmp1 = self.md_api
            self.md_api = None
            tmp1.close()

        if self.l2_md_api:
            self.write_log('断开五档行情API',on_log=True)
            tmp1 = self.l2_md_api
            self.l2_md_api = None
            tmp1.close()

        if self.td_api:
            self.write_log('断开交易API',on_log=True)
            tmp2 = self.td_api
            self.td_api = None
            tmp2.close()

        if self.tdx_api:
            self.write_log(u'断开tdx指数行情API',on_log=True)
            tmp3 = self.tdx_api
            self.tdx_api = None
            tmp3.close()

        if self.rabbit_api:
            self.write_log(u'断开rabbit MQ tdx指数行情API',on_log=True)
            tmp4 = self.rabbit_api
            self.rabbit_api = None
            tmp4.close()

        if self.tq_api:
            self.write_log(u'断开天勤行情API',on_log=True)
            tmp5 = self.tq_api
            self.tq_api = None
            tmp5.close()

    def process_timer_event(self, event):
        """"""
        self.count += 1
        if self.count < 2:
            return
        self.count = 0

        func = self.query_functions.pop(0)
        func()
        self.query_functions.append(func)

    def init_query(self):
        """"""
        self.count = 0
        self.query_functions = [self.query_account, self.query_position]
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)

    def on_custom_tick(self, tick):
        """推送自定义合约行情"""
        # 自定义合约行情
        for combiner in self.tick_combiner_map.get(tick.symbol, []):
            tick = copy(tick)
            combiner.on_tick(tick)

        # 推送至指数生成器
        if tick.symbol in self.index_generators:
            tick = copy(tick)
            # 推送on_tick()方法
            self.index_generators[tick.symbol].on_tick(tick)

class CtpMdApi(MdApi):
    """"""

    def __init__(self, gateway, level2=False):
        """Constructor"""
        super(CtpMdApi, self).__init__()

        self.gateway = gateway
        self.gateway_name = gateway.gateway_name
        self.level2 = level2  # 5档行情
        if self.level2:
            self.name = "L2_"
        else:
            self.name = ""
        self.reqid = 0

        self.connect_status = False
        self.login_status = False
        self.subscribed = set()

        self.userid = ""
        self.password = ""
        self.brokerid = ""
        # 缓存tick的交易日和当前累计volume
        self.last_ticks_info = {}  # {symbol: {'trading_dat':'xxx', volume:xxx}}

    def onFrontConnected(self):
        """
        Callback when front server is connected.
        """
        self.gateway.write_log(f"{self.name}行情服务器连接成功",on_log=True)
        self.connect_status = True
        self.login()
        self.gateway.status.update(
            {f'{self.name}md_con': True, f'{self.name}md_con_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')})

    def onFrontDisconnected(self, reason: int):
        """
        Callback when front server is disconnected.
        """
        self.login_status = False
        self.connect_status = False
        self.gateway.write_log(f"{self.name}行情服务器连接断开，原因{reason}",on_log=True)
        self.gateway.status.update(
            {f'{self.name}md_con': False, f'{self.name}md_dis_con_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')})

    def onRspUserLogin(self, data: dict, error: dict, reqid: int, last: bool):
        """
        Callback when user is logged in.
        """
        if not error["ErrorID"]:
            self.login_status = True
            self.gateway.write_log(f"{self.name}行情服务器登录成功",on_log=True)

            for symbol in self.subscribed:
                self.subscribeMarketData(symbol)
        else:
            self.gateway.write_error(f"{self.name}行情服务器登录失败", error)

    def onRspError(self, error: dict, reqid: int, last: bool):
        """
        Callback when error occured.
        """
        self.gateway.write_error(f"{self.name}行情接口报错", error)

    def onRspSubMarketData(self, data: dict, error: dict, reqid: int, last: bool):
        """"""
        if not error or not error["ErrorID"]:
            return

        self.gateway.write_error(f"{self.name}行情订阅失败", error)

    def onRtnDepthMarketData(self, data: dict):
        """
        Callback of tick data update.
        """
        symbol = data["InstrumentID"]
        exchange = symbol_exchange_map.get(symbol, "")
        if not exchange:
            return
        # 取当前时间
        dt = datetime.now()
        s_date = dt.strftime('%Y-%m-%d')
        timestamp = f"{s_date} {data['UpdateTime']}.{int(data['UpdateMillisec'] / 100)}"
        dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S.%f")

        # 不处理开盘前的tick数据
        if dt.hour in [7, 8, 18, 19, 20] and dt.minute <= 59:
            return
        if exchange is Exchange.CFFEX and dt.hour == 9 and dt.minute <= 29:
            return

        today_volume = data["Volume"]
        last_tick_info = self.last_ticks_info.get(symbol, {})
        trading_day = get_trading_date(dt)
        last_trading_day = last_tick_info.get('trading_day', None)
        if last_trading_day == trading_day:
            volume_changed = max(0, today_volume - last_tick_info.get('volume', 0))
        else:
            volume_changed = today_volume
        self.last_ticks_info.update({symbol: {'trading_day': trading_day, 'volume': today_volume}})
        tick = TickData(
            symbol=symbol,
            exchange=exchange,
            datetime=dt,
            date=s_date,
            time=dt.strftime('%H:%M:%S.%f'),
            trading_day=trading_day,
            name=symbol_name_map.get(symbol,symbol),
            volume=today_volume,
            last_volume=volume_changed,
            open_interest=data["OpenInterest"],
            last_price=data["LastPrice"],
            limit_up=data["UpperLimitPrice"],
            limit_down=data["LowerLimitPrice"],
            open_price=adjust_price(data["OpenPrice"]),
            high_price=adjust_price(data["HighestPrice"]),
            low_price=adjust_price(data["LowestPrice"]),
            pre_close=adjust_price(data["PreClosePrice"]),
            bid_price_1=adjust_price(data["BidPrice1"]),
            ask_price_1=adjust_price(data["AskPrice1"]),
            bid_volume_1=data["BidVolume1"],
            ask_volume_1=data["AskVolume1"],
            gateway_name=self.gateway_name
        )

        # 处理一下标准套利合约的last_price
        if '&' in symbol:
            tick.last_price = (tick.ask_price_1 + tick.bid_price_1) / 2

        if data["BidVolume2"] or data["AskVolume2"]:
            tick.bid_price_2 = adjust_price(data["BidPrice2"])
            tick.bid_price_3 = adjust_price(data["BidPrice3"])
            tick.bid_price_4 = adjust_price(data["BidPrice4"])
            tick.bid_price_5 = adjust_price(data["BidPrice5"])

            tick.ask_price_2 = adjust_price(data["AskPrice2"])
            tick.ask_price_3 = adjust_price(data["AskPrice3"])
            tick.ask_price_4 = adjust_price(data["AskPrice4"])
            tick.ask_price_5 = adjust_price(data["AskPrice5"])

            tick.bid_volume_2 = adjust_price(data["BidVolume2"])
            tick.bid_volume_3 = adjust_price(data["BidVolume3"])
            tick.bid_volume_4 = adjust_price(data["BidVolume4"])
            tick.bid_volume_5 = adjust_price(data["BidVolume5"])

            tick.ask_volume_2 = adjust_price(data["AskVolume2"])
            tick.ask_volume_3 = adjust_price(data["AskVolume3"])
            tick.ask_volume_4 = adjust_price(data["AskVolume4"])
            tick.ask_volume_5 = adjust_price(data["AskVolume5"])

        self.gateway.on_tick(tick)
        self.gateway.on_custom_tick(tick)

    def connect(self, address: str, userid: str, password: str, brokerid: int):
        """
        Start connection to server.
        """
        self.userid = userid
        self.password = password
        self.brokerid = brokerid

        # If not connected, then start connection first.
        if not self.connect_status:
            path = get_folder_path(self.gateway_name.lower())
            self.createFtdcMdApi(str(path) + "\\Md")

            self.registerFront(address)
            self.init()

            self.connect_status = True
        # If already connected, then login immediately.
        elif not self.login_status:
            self.login()

    def login(self):
        """
        Login onto server.
        """
        self.gateway.write_log(f'{self.name}向行情服务器发出登录请求')
        req = {
            "UserID": self.userid,
            "Password": self.password,
            "BrokerID": self.brokerid
        }

        self.reqid += 1
        self.reqUserLogin(req, self.reqid)

    def subscribe(self, req: SubscribeRequest):
        """
        Subscribe to tick data update.
        """
        if req.symbol not in self.subscribed:
            self.gateway.write_log(f'{self.name}订阅:{req.exchange} {req.symbol}')
        if self.login_status:
            self.subscribeMarketData(req.symbol)
        self.subscribed.add(req.symbol)

    def close(self):
        """
        Close the connection.
        """
        if self.connect_status:
            self.exit()


class CtpTdApi(TdApi):
    """"""

    def __init__(self, gateway):
        """Constructor"""
        super(CtpTdApi, self).__init__()

        self.gateway = gateway
        self.gateway_name = gateway.gateway_name

        self.reqid = 0
        self.order_ref = 0

        self.connect_status = False
        self.login_status = False
        self.auth_staus = False
        self.login_failed = False

        self.userid = ""
        self.password = ""
        self.brokerid = ""
        self.auth_code = ""
        self.appid = ""
        self.product_info = ""

        self.frontid = 0
        self.sessionid = 0

        self.order_data = []
        self.trade_data = []
        self.positions = {}
        self.sysid_orderid_map = {}
        self.future_contract_changed = False

        self.accountid = self.userid

        self.long_option_cost = None  # 多头期权动态市值
        self.short_option_cost = None  # 空头期权动态市值

    def onFrontConnected(self):
        """"""
        self.gateway.write_log("交易服务器连接成功",on_log=True)
        self.connect_status = True
        if self.auth_code:
            self.gateway.write_log("向交易服务器提交授权码验证")
            self.authenticate()
        else:
            self.gateway.write_log("向交易服务器进行帐号登录")
            self.login()

    def onFrontDisconnected(self, reason: int):
        """"""
        self.login_status = False
        self.gateway.write_log(f"交易服务器连接断开，原因{reason}",on_log=True)
        self.gateway.status.update({'td_con': False, 'td_dis_con_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')})

    def onRspAuthenticate(self, data: dict, error: dict, reqid: int, last: bool):
        """"""
        if not error['ErrorID']:
            self.auth_staus = True
            self.gateway.write_log("交易服务器授权验证成功")
            self.gateway.status.update({"td_auth": True})
            self.login()
        else:
            self.gateway.write_error("交易服务器授权验证失败", error)
            self.gateway.status.update({"td_auth":False})

    def onRspUserLogin(self, data: dict, error: dict, reqid: int, last: bool):
        """"""
        if not error["ErrorID"]:
            self.frontid = data["FrontID"]
            self.sessionid = data["SessionID"]
            self.login_status = True
            self.gateway.status.update({'td_con': True, 'td_con_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')})
            self.gateway.write_log("交易帐号登录完成",on_log=True)

            # Confirm settlement
            req = {
                "BrokerID": self.brokerid,
                "InvestorID": self.userid
            }
            self.reqid += 1
            self.reqSettlementInfoConfirm(req, self.reqid)
        else:
            self.login_failed = True
            self.gateway.status.update({'td_con': False,'td_login_fail_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')})
            self.gateway.write_error("交易服务器登录失败", error)

    def onRspOrderInsert(self, data: dict, error: dict, reqid: int, last: bool):
        """"""
        order_ref = data["OrderRef"]
        orderid = f"{self.frontid}_{self.sessionid}_{order_ref}"

        symbol = data["InstrumentID"]
        exchange = symbol_exchange_map[symbol]

        order_type = OrderType.LIMIT
        if data["OrderPriceType"] == THOST_FTDC_OPT_LimitPrice and data["TimeCondition"] == THOST_FTDC_TC_IOC:
            if data["VolumeCondition"] == THOST_FTDC_VC_AV:
                order_type = OrderType.FAK
            elif data["VolumeCondition"] == THOST_FTDC_VC_CV:
                order_type = OrderType.FOK

        if data["OrderPriceType"] == THOST_FTDC_OPT_AnyPrice:
            order_type = OrderType.MARKET

        order = OrderData(
            symbol=symbol,
            exchange=exchange,
            accountid=self.accountid,
            orderid=orderid,
            type=order_type,
            direction=DIRECTION_CTP2VT[data["Direction"]],
            offset=OFFSET_CTP2VT.get(data["CombOffsetFlag"], Offset.NONE),
            price=data["LimitPrice"],
            volume=data["VolumeTotalOriginal"],
            status=Status.REJECTED,
            gateway_name=self.gateway_name
        )
        self.gateway.on_order(order)

        # self.gateway.write_error("交易委托失败", error)

    def onRspOrderAction(self, data: dict, error: dict, reqid: int, last: bool):
        """"""
        self.gateway.write_error("交易撤单失败", error)

    def onRspParkedOrderInsert(self, data: dict, error: dict, reqid: int, last: bool):
        """"""
        self.gateway.write_log('预埋单回报')

        order_ref = data["OrderRef"]
        orderid = f"{self.frontid}_{self.sessionid}_{order_ref}"

        symbol = data["InstrumentID"]
        exchange = symbol_exchange_map[symbol]

        order_type = OrderType.LIMIT
        if data["OrderPriceType"] == THOST_FTDC_OPT_LimitPrice and data["TimeCondition"] == THOST_FTDC_TC_IOC:
            if data["VolumeCondition"] == THOST_FTDC_VC_AV:
                order_type = OrderType.FAK
            elif data["VolumeCondition"] == THOST_FTDC_VC_CV:
                order_type = OrderType.FOK

        if data["OrderPriceType"] == THOST_FTDC_OPT_AnyPrice:
            order_type = OrderType.MARKET

        order = OrderData(
            symbol=symbol,
            exchange=exchange,
            accountid=self.accountid,
            orderid=orderid,
            type=order_type,
            direction=DIRECTION_CTP2VT[data["Direction"]],
            offset=OFFSET_CTP2VT.get(data["CombOffsetFlag"], Offset.NONE),
            price=data["LimitPrice"],
            volume=data["VolumeTotalOriginal"],
            status=Status.REJECTED,
            gateway_name=self.gateway_name
        )
        self.gateway.on_order(order)

        # self.gateway.write_error("交易委托失败", error)

    def onRspParkedOrderAction(self, data: dict, error: dict, reqid: int, last: bool):
        """"""
        self.gateway.write_error("预埋单交易撤单失败", error)

    def onRspQueryMaxOrderVolume(self, data: dict, error: dict, reqid: int, last: bool):
        """"""
        pass

    def onRspSettlementInfoConfirm(self, data: dict, error: dict, reqid: int, last: bool):
        """
        Callback of settlment info confimation.
        """
        self.gateway.write_log("结算信息确认成功",on_log=True)

        while True:
            self.reqid += 1
            n = self.reqQryInstrument({}, self.reqid)

            if not n:
                break
            else:
                sleep(1)

    def onRspQryInvestorPosition(self, data: dict, error: dict, reqid: int, last: bool):
        """"""
        if not data:
            return

        # Check if contract data received
        if data["InstrumentID"] in symbol_exchange_map:
            # Get buffered position object
            key = f"{data['InstrumentID'], data['PosiDirection']}"
            position = self.positions.get(key, None)
            if not position:
                position = PositionData(
                    accountid=self.accountid,
                    symbol=data["InstrumentID"],
                    exchange=symbol_exchange_map[data["InstrumentID"]],
                    direction=DIRECTION_CTP2VT[data["PosiDirection"]],
                    gateway_name=self.gateway_name
                )
                self.positions[key] = position

            # For SHFE and INE position data update
            if position.exchange in [Exchange.SHFE, Exchange.INE]:
                if data["YdPosition"] and not data["TodayPosition"]:
                    position.yd_volume = data["Position"]
            # For other exchange position data update
            else:
                position.yd_volume = data["Position"] - data["TodayPosition"]

            # Get contract size (spread contract has no size value)
            size = symbol_size_map.get(position.symbol, 0)

            # Calculate previous position cost
            cost = position.price * position.volume * size

            # Update new position volume
            position.volume += data["Position"]
            if data["PositionProfit"] == 0 and position.symbol in option_name_map:
                position.pnl += data["PositionCost"] - data["OpenCost"]
            else:
                position.pnl += data["PositionProfit"]

            # Calculate average position price
            if position.volume and size:
                cost += data["PositionCost"]
                position.price = cost / (position.volume * size)

            # Get frozen volume
            if position.direction == Direction.LONG:
                position.frozen += data["ShortFrozen"]
            else:
                position.frozen += data["LongFrozen"]

            position.cur_price = self.gateway.prices.get(position.vt_symbol, None)
            if position.cur_price is None:
                position.cur_price = position.price
                # 交易所有时候会给一些奇怪得套利合约，并且主动腿和被动腿是相同得，排除掉这些垃圾合约
                if position.symbol.startswith('SP') and '&' in position.symbol:
                    act_symbol, pas_symbol = position.symbol.split(' ')[-1].split('&')
                    if act_symbol != pas_symbol:
                        self.gateway.subscribe(SubscribeRequest(symbol=position.symbol, exchange=position.exchange))
                else:
                    # 获取合约的缩写号
                    underlying_symbol = get_underlying_symbol(position.symbol)
                    dt = datetime.now()
                    # 若为中金所等的合约，白天才提交订阅请求
                    if not (underlying_symbol in MARKET_DAY_ONLY and not (8 < dt.hour < 16)):
                        self.gateway.subscribe(SubscribeRequest(symbol=position.symbol, exchange=position.exchange))

        if last:
            self.long_option_cost = None
            self.short_option_cost = None
            for position in self.positions.values():
                if position.symbol in option_name_map:
                    # 重新累计多头期权动态权益
                    if position.direction == Direction.LONG:
                        if self.long_option_cost is None:
                            self.long_option_cost = position.cur_price * position.volume * symbol_size_map.get(
                                position.symbol, 0)
                        else:
                            self.long_option_cost += position.cur_price * position.volume * symbol_size_map.get(
                                position.symbol, 0)

                    # 重新累计空头期权动态权益
                    if position.direction == Direction.SHORT:
                        if self.short_option_cost is None:
                            self.short_option_cost = position.cur_price * position.volume * symbol_size_map.get(
                                position.symbol, 0)
                        else:
                            self.short_option_cost += position.cur_price * position.volume * symbol_size_map.get(
                                position.symbol, 0)

                self.gateway.on_position(position)

            self.positions.clear()

    def onRspQryTradingAccount(self, data: dict, error: dict, reqid: int, last: bool):
        """"""
        if "AccountID" not in data:
            return
        if len(self.accountid) == 0:
            self.accountid = data['AccountID']

        balance = float(data["Balance"])
        if self.long_option_cost is not None:
            balance += self.long_option_cost
        if self.short_option_cost is not None:
            balance -= self.short_option_cost

        account = AccountData(
            accountid=data["AccountID"],
            pre_balance=round(float(data['PreBalance']), 7),
            balance=round(balance, 7),
            frozen=round(data["FrozenMargin"] + data["FrozenCash"] + data["FrozenCommission"], 7),
            gateway_name=self.gateway_name
        )
        account.available = round(float(data["Available"]), 7)
        account.commission = round(float(data['Commission']), 7)
        account.margin = round(float(data['CurrMargin']), 7)
        account.close_profit = round(float(data['CloseProfit']), 7)  # + round(
        # float(data.get("SpecProductCloseProfit", 0)), 7)
        account.holding_profit = round(float(data['PositionProfit']), 7)  # + round(
        # float(data.get("SpecProductPositionProfit", 0)), 7) + round(
        # float(data.get("SpecProductPositionProfitByAlg", 0)), 7)
        account.trading_day = str(data['TradingDay'])
        if '-' not in account.trading_day and len(account.trading_day) == 8:
            account.trading_day = '-'.join(
                [
                    account.trading_day[0:4],
                    account.trading_day[4:6],
                    account.trading_day[6:8]
                ]
            )

        self.gateway.on_account(account)

    def onRspQryInstrument(self, data: dict, error: dict, reqid: int, last: bool):
        """
        Callback of instrument query.
        """
        product = PRODUCT_CTP2VT.get(data["ProductClass"], None)
        if product:

            contract = ContractData(
                symbol=data["InstrumentID"],
                exchange=EXCHANGE_CTP2VT[data["ExchangeID"]],
                name=data["InstrumentName"],
                product=product,
                size=data["VolumeMultiple"],
                pricetick=data["PriceTick"],
                gateway_name=self.gateway_name
            )
            # if 'SA' in contract.symbol:
            #     self.gateway.write_log(print_dict(data))

            # 保证金费率(期权合约的保证金比例数值可能不对，所以设置个0.2的最大值)
            contract.margin_rate = min(0.2, max(data.get('LongMarginRatio', 0), data.get('ShortMarginRatio', 0)))
            if contract.margin_rate == 0:
                contract.margin_rate = 0.1

            # For option only
            if contract.product == Product.OPTION:
                # Remove C/P suffix of CZCE option product name
                if contract.exchange == Exchange.CZCE:
                    contract.option_portfolio = data["ProductID"][:-1]
                else:
                    contract.option_portfolio = data["ProductID"]

                contract.option_underlying = data["UnderlyingInstrID"]
                contract.option_type = OPTIONTYPE_CTP2VT.get(data["OptionsType"], None)
                contract.option_strike = data["StrikePrice"]
                contract.option_index = str(data["StrikePrice"])
                contract.option_expiry = datetime.strptime(data["ExpireDate"], "%Y%m%d")
                option_name_map[contract.symbol] = contract.name

            self.gateway.on_contract(contract)

            symbol_exchange_map[contract.symbol] = contract.exchange
            symbol_name_map[contract.symbol] = contract.name
            symbol_size_map[contract.symbol] = contract.size

            if contract.product == Product.FUTURES:
                # 生成指数合约信息
                underlying_symbol = data["ProductID"]  # 短合约名称
                underlying_symbol = underlying_symbol.upper()
                # 只推送普通合约的指数
                if len(underlying_symbol) <= 2:
                    idx_contract = index_contracts.get(underlying_symbol, None)
                    if idx_contract is None:
                        idx_contract = deepcopy(contract)
                        idx_contract.symbol = '{}99'.format(underlying_symbol)
                        idx_contract.name = u'{}指数'.format(underlying_symbol)
                        idx_contract.vt_symbol = f'{idx_contract.symbol}.{idx_contract.exchange.value}'
                        self.gateway.on_contract(idx_contract)

                        # 获取data/tdx/future_contracts.json中的合约记录
                        future_contract = future_contracts.get(underlying_symbol, {})
                        mi_contract_symbol = future_contract.get('mi_symbol', '')
                        margin_rate = float(future_contract.get('margin_rate', 0))
                        mi_margin_rate = round(idx_contract.margin_rate, 4)
                        if mi_contract_symbol == contract.symbol:
                            if margin_rate != mi_margin_rate:
                                self.gateway.write_log(
                                    f"{underlying_symbol}合约主力{mi_contract_symbol} 保证金{margin_rate}=>{mi_margin_rate}")
                                future_contract.update({'margin_rate': mi_margin_rate})
                                future_contract.update({'symbol_size': idx_contract.size})
                                future_contract.update({'price_tick': idx_contract.pricetick})
                                if 'exchange' not in future_contract:
                                    future_contract.update({'exchange': contract.exchange.value})
                                future_contracts.update({underlying_symbol: future_contract})
                                self.future_contract_changed = True
                                index_contracts.update({underlying_symbol: idx_contract})
        if last:
            self.gateway.write_log("合约信息查询成功")

            if self.future_contract_changed:
                self.gateway.write_log('更新vnpy/data/tdx/future_contracts.json')
                save_future_contracts(future_contracts)

            for data in self.order_data:
                self.onRtnOrder(data)
            self.order_data.clear()

            for data in self.trade_data:
                self.onRtnTrade(data)
            self.trade_data.clear()

    def onRtnOrder(self, data: dict):
        """
        Callback of order status update.
        """
        symbol = data["InstrumentID"]
        exchange = symbol_exchange_map.get(symbol, "")
        if not exchange:
            self.order_data.append(data)
            return

        frontid = data["FrontID"]
        sessionid = data["SessionID"]
        order_ref = data["OrderRef"]
        orderid = f"{frontid}_{sessionid}_{order_ref}"

        order_type = OrderType.LIMIT
        if data["OrderPriceType"] == THOST_FTDC_OPT_LimitPrice and data["TimeCondition"] == THOST_FTDC_TC_IOC:
            if data["VolumeCondition"] == THOST_FTDC_VC_AV:
                order_type = OrderType.FAK
            elif data["VolumeCondition"] == THOST_FTDC_VC_CV:
                order_type = OrderType.FOK

        if data["OrderPriceType"] == THOST_FTDC_OPT_AnyPrice:
            order_type = OrderType.MARKET

        order = OrderData(
            accountid=self.accountid,
            symbol=symbol,
            exchange=exchange,
            orderid=orderid,
            sys_orderid=data.get('OrderSysID', orderid),
            type=order_type,
            direction=DIRECTION_CTP2VT[data["Direction"]],
            offset=OFFSET_CTP2VT[data["CombOffsetFlag"]],
            price=data["LimitPrice"],
            volume=data["VolumeTotalOriginal"],
            traded=data["VolumeTraded"],
            status=STATUS_CTP2VT[data["OrderStatus"]],
            time=data["InsertTime"],
            cancel_time=data["CancelTime"],
            gateway_name=self.gateway_name
        )
        self.gateway.on_order(order)

        self.sysid_orderid_map[data["OrderSysID"]] = orderid

    def onRtnTrade(self, data: dict):
        """
        Callback of trade status update.
        """
        symbol = data["InstrumentID"]
        exchange = symbol_exchange_map.get(symbol, "")
        if not exchange:
            self.trade_data.append(data)
            return

        orderid = self.sysid_orderid_map[data["OrderSysID"]]

        trade_date = data['TradeDate']
        if '-' not in trade_date and len(trade_date) == 8:
            trade_date = trade_date[0:4] + '-' + trade_date[4:6] + '-' + trade_date[6:8]
        trade_time = data['TradeTime']
        trade_datetime = datetime.strptime(f'{trade_date} {trade_time}', '%Y-%m-%d %H:%M:%S')
        # print(f'raw_data:{print_dict(data)}')
        # 修正 郑商所、大商所的TradeDate错误
        if exchange in [Exchange.DCE, Exchange.CZCE]:
            dt_now = datetime.now()
            # 交易发生在夜盘
            if trade_datetime.hour >= 21:
                # 系统时间在夜盘，使用系统时间
                if dt_now.hour >= 21:
                    trade_date = dt_now.strftime('%Y-%m-%d')
                    trade_datetime = datetime.strptime(f'{trade_date} {trade_time}', '%Y-%m-%d %H:%M:%S')

                # 系统时间在日盘
                else:
                    # 星期一 =》 星期五
                    if dt_now.isoweekday() == 1:
                        trade_datetime -= timedelta(days=3)
                        # print(f'trade time =>{trade_datetime}')
                    # 星期二~星期五 =》上一天
                    else:
                        trade_datetime -= timedelta(days=1)
                        # print(f'trade time =>{trade_datetime}')

        tradeid = data["TradeID"]
        trade = TradeData(
            accountid=self.accountid,
            symbol=symbol,
            exchange=exchange,
            orderid=orderid,
            sys_orderid=data.get("OrderSysID", orderid),
            tradeid=tradeid.replace(' ', ''),
            direction=DIRECTION_CTP2VT[data["Direction"]],
            offset=OFFSET_CTP2VT[data["OffsetFlag"]],
            price=data["Price"],
            volume=data["Volume"],
            time=data["TradeTime"],
            datetime=trade_datetime,
            gateway_name=self.gateway_name
        )
        self.gateway.on_trade(trade)

    def connect(
            self,
            address: str,
            userid: str,
            password: str,
            brokerid: int,
            auth_code: str,
            appid: str,
            product_info
    ):
        """
        Start connection to server.
        """
        self.userid = userid
        self.password = password
        self.brokerid = brokerid
        self.auth_code = auth_code
        self.appid = appid
        self.product_info = product_info

        if not self.connect_status:
            path = get_folder_path(self.gateway_name.lower())
            self.createFtdcTraderApi(str(path) + "\\Td")

            self.subscribePrivateTopic(0)
            self.subscribePublicTopic(0)

            self.registerFront(address)
            self.init()
            self.gateway.write_log(f'交易前端连接成功')
            self.connect_status = True
        else:
            self.authenticate()

    def authenticate(self):
        """
        Authenticate with auth_code and appid.
        """

        req = {
            "UserID": self.userid,
            "BrokerID": self.brokerid,
            "AuthCode": self.auth_code,
            "AppID": self.appid
        }

        if self.product_info:
            req["UserProductInfo"] = self.product_info

        self.reqid += 1
        self.reqAuthenticate(req, self.reqid)

    def login(self):
        """
        Login onto server.
        """
        if self.login_failed:
            return

        req = {
            "UserID": self.userid,
            "Password": self.password,
            "BrokerID": self.brokerid,
            "AppID": self.appid
        }
        self.accountid = copy(self.userid)
        if self.product_info:
            req["UserProductInfo"] = self.product_info

        self.reqid += 1
        self.reqUserLogin(req, self.reqid)

    def send_order(self, req: OrderRequest):
        """
        Send new order.
        """
        if req.offset not in OFFSET_VT2CTP:
            self.gateway.write_log("请选择开平方向")
            return ""

        self.order_ref += 1

        ctp_req = {
            "InstrumentID": req.symbol,
            "ExchangeID": req.exchange.value,
            "LimitPrice": req.price,
            "VolumeTotalOriginal": int(req.volume),
            "OrderPriceType": ORDERTYPE_VT2CTP.get(req.type, ""),
            "Direction": DIRECTION_VT2CTP.get(req.direction, ""),
            "CombOffsetFlag": OFFSET_VT2CTP.get(req.offset, ""),
            "OrderRef": str(self.order_ref),
            "InvestorID": self.userid,
            "UserID": self.userid,
            "BrokerID": self.brokerid,
            "CombHedgeFlag": THOST_FTDC_HF_Speculation,
            "ContingentCondition": THOST_FTDC_CC_Immediately,
            "ForceCloseReason": THOST_FTDC_FCC_NotForceClose,
            "IsAutoSuspend": 0,
            "TimeCondition": THOST_FTDC_TC_GFD,
            "VolumeCondition": THOST_FTDC_VC_AV,
            "MinVolume": 1
        }

        if req.type == OrderType.FAK:
            ctp_req["OrderPriceType"] = THOST_FTDC_OPT_LimitPrice
            ctp_req["TimeCondition"] = THOST_FTDC_TC_IOC
            ctp_req["VolumeCondition"] = THOST_FTDC_VC_AV
        elif req.type == OrderType.FOK:
            ctp_req["OrderPriceType"] = THOST_FTDC_OPT_LimitPrice
            ctp_req["TimeCondition"] = THOST_FTDC_TC_IOC
            ctp_req["VolumeCondition"] = THOST_FTDC_VC_CV

        self.reqid += 1
        dt = datetime.now()
        use_packed = False
        if 3 < dt.hour < 8 or 15 < dt.hour < 20:
            use_packed = True
        else:
            if dt.hour in [8,20] and dt.minute <55:
                use_packed = True
            if req.exchange != Exchange.CFFEX:
                if dt.hour == 10 and 15< dt.minute<30:
                    use_packed = True
                if dt.hour == 11 and dt.minute > 30:
                    use_packed = True
                if dt.hour == 12:
                    use_packed = True
                if dt.hour == 13 and dt.minute < 30:
                    use_packed = True
            else:
                if dt.hour == 9 and dt.minute < 25:
                    use_packed = True
                if dt.hour == 11 and dt.minute > 30:
                    use_packed = True
                if dt.hour == 12:
                    use_packed = True
        if use_packed:
            self.gateway.write_log(f'使用预埋单下单')
            self.reqParkedOrderInsert(ctp_req, self.reqid)
        else:
            self.reqOrderInsert(ctp_req, self.reqid)

        orderid = f"{self.frontid}_{self.sessionid}_{self.order_ref}"
        order = req.create_order_data(orderid, self.gateway_name)
        order.accountid = self.accountid
        order.vt_accountid = f"{self.gateway_name}.{self.accountid}"
        self.gateway.on_order(order)

        return order.vt_orderid

    def cancel_order(self, req: CancelRequest):
        """
        Cancel existing order.
        """
        frontid, sessionid, order_ref = req.orderid.split("_")

        ctp_req = {
            "InstrumentID": req.symbol,
            "ExchangeID": req.exchange.value,
            "OrderRef": order_ref,
            "FrontID": int(frontid),
            "SessionID": int(sessionid),
            "ActionFlag": THOST_FTDC_AF_Delete,
            "BrokerID": self.brokerid,
            "InvestorID": self.userid
        }

        self.reqid += 1
        self.reqOrderAction(ctp_req, self.reqid)

    def query_account(self):
        """
        Query account balance data.
        """
        self.reqid += 1
        self.reqQryTradingAccount({}, self.reqid)

    def query_position(self):
        """
        Query position holding data.
        """
        if not symbol_exchange_map:
            return

        req = {
            "BrokerID": self.brokerid,
            "InvestorID": self.userid
        }

        self.reqid += 1
        self.reqQryInvestorPosition(req, self.reqid)

    def close(self):
        """"""
        if self.connect_status:
            self.exit()


def adjust_price(price: float) -> float:
    """"""
    if price == MAX_FLOAT:
        price = 0
    return price


class TdxMdApi():
    """
    通达信数据行情API实现
    订阅的指数行情，更新合约的数据

    """

    def __init__(self, gateway):
        self.gateway = gateway  # gateway对象
        self.gateway_name = gateway.gateway_name  # gateway对象名称

        self.req_interval = 0.5  # 操作请求间隔500毫秒
        self.req_id = 0  # 操作请求编号
        self.connection_status = False  # 连接状态

        self.symbol_exchange_dict = {}  # tdx合约与vn交易所的字典
        self.symbol_market_dict = {}  # tdx合约与tdx市场的字典
        self.symbol_vn_dict = {}  # tdx合约与vt_symbol的对应
        self.symbol_tick_dict = {}  # tdx合约与最后一个Tick得字典

        self.registered_symbol_set = set()

        self.thread = None  # 查询线程

        self.ip_list = TDX_FUTURE_HOSTS

        #  调出
        self.best_ip = {}  # 最佳IP地址和端口
        self.api = None  # API 的连接会话对象
        self.last_tick_dt = datetime.now()  # 记录该会话对象的最后一个tick时间

        self.instrument_count = 50000

        self.has_qry_instrument = False

    # ----------------------------------------------------------------------
    def ping(self, ip, port=7709):
        """
        ping行情服务器
        :param ip:
        :param port:
        :param type_:
        :return:
        """
        apix = TdxExHq_API()
        __time1 = datetime.now()
        try:
            with apix.connect(ip, port):
                if apix.get_instrument_count() > 10000:
                    _timestamp = (datetime.now() - __time1).total_seconds() * 1000
                    self.gateway.write_log('服务器{}:{},耗时:{}ms'.format(ip, port, _timestamp))
                    return _timestamp
                else:
                    self.gateway.write_log(u'该服务器IP {}无响应.'.format(ip))
                    return timedelta(seconds=10).total_seconds() * 1000
        except Exception as ex:
            self.gateway.write_log(u'tdx ping服务器{}，异常的响应{}'.format(ip, str(ex)))
            return timedelta(seconds=10).total_seconds() * 1000

    def sort_ip_speed(self):
        """
        对所有服务器进行速度排序
        :return:
        """

        speed_result = []
        for x in self.ip_list:
            speed = self.ping(x['ip'], x['port'])
            x.update({'speed': speed})
            speed_result.append(copy(x))

        # 更新服务器，按照速度排序
        speed_result = sorted(speed_result, key=lambda s: s['speed'])
        self.gateway.write_log(u'服务器访问速度排序:{}'.format(speed_result))
        return speed_result

    # ----------------------------------------------------------------------
    def select_best_ip(self, exclude_ip: str = None):
        """
        选择行情服务器
        :param: exclude_ip, 排除的ip地址
        :return:
        """
        self.gateway.write_log(u'选择通达信行情服务器')

        ip_list = self.sort_ip_speed()

        valid_ip_list = [x for x in ip_list if x.get('speed', 10000) < 10000 and x.get('ip') != exclude_ip]

        if len(valid_ip_list) == 0:
            self.gateway.write_error(u'未能找到合适速度得行情服务器')
            return None
        best_future_ip = valid_ip_list[0]
        save_cache_json(best_future_ip, TDX_FUTURE_CONFIG)
        return best_future_ip

    def connect(self, is_reconnect=False):
        """
        连接通达讯行情服务器
        :param is_reconnect:是否重连
        :return:
        """
        # 创建api连接对象实例
        try:
            if self.api is None or not self.connection_status:
                self.gateway.write_log(u'开始连接通达信行情服务器',on_log=True)
                self.api = TdxExHq_API(heartbeat=True, auto_retry=True, raise_exception=True)

                # 选取最佳服务器
                if is_reconnect or len(self.best_ip) == 0:
                    self.best_ip = get_cache_json(TDX_FUTURE_CONFIG)

                if len(self.best_ip) == 0:
                    self.best_ip = self.select_best_ip()

                self.api.connect(self.best_ip['ip'], self.best_ip['port'])
                # 尝试获取市场合约统计
                c = self.api.get_instrument_count()
                if c < 10:
                    err_msg = u'该服务器IP {}/{}无响应'.format(self.best_ip['ip'], self.best_ip['port'])
                    self.gateway.write_error(err_msg)
                else:
                    self.gateway.write_log(u'创建tdx连接, IP: {}/{}'.format(self.best_ip['ip'], self.best_ip['port']))
                    self.connection_status = True
                    self.gateway.status.update(
                        {'tdx_con': True, 'tdx_con_time': datetime.now().strftime('%Y-%m-%d %H:%M%S')})
                    self.thread = Thread(target=self.run)
                    self.thread.start()

        except Exception as ex:
            self.gateway.write_log(u'连接服务器tdx异常:{},{}'.format(str(ex), traceback.format_exc()),on_log=True)
            return

    def close(self):
        """退出API"""
        self.gateway.write_log(u'退出tdx API',on_log=True)
        self.connection_status = False

        if self.thread:
            self.thread.join()

    # ----------------------------------------------------------------------
    def subscribe(self, subscribeReq):
        """订阅合约"""
        # 这里的设计是，如果尚未登录就调用了订阅方法
        # 则先保存订阅请求，登录完成后会自动订阅
        vn_symbol = str(subscribeReq.symbol)
        vn_symbol = vn_symbol.upper()
        self.gateway.write_log(u'通达信行情订阅 {}'.format(str(vn_symbol)))

        if vn_symbol[-2:] != '99':
            self.gateway.write_log(u'{}不是指数合约，不能订阅'.format(vn_symbol))
            return

        tdx_symbol = vn_symbol[0:-2] + 'L9'
        tdx_symbol = tdx_symbol.upper()
        self.gateway.write_log(u'{}=>{}'.format(vn_symbol, tdx_symbol))
        self.symbol_vn_dict[tdx_symbol] = vn_symbol

        if tdx_symbol not in self.registered_symbol_set:
            self.registered_symbol_set.add(tdx_symbol)

        self.check_status()

    def check_status(self):
        """
        检查通达信直连状态
        :return:
        """
        # self.write_log(u'检查tdx接口状态')
        if len(self.registered_symbol_set) == 0:
            return

        # 若还没有启动连接，就启动连接
        over_time = (datetime.now() - self.last_tick_dt).total_seconds() > 60
        if not self.connection_status or self.api is None or over_time:
            self.gateway.write_log(u'tdx还没有启动连接，就启动连接')
            self.close()
            self.thread = None
            self.connect(is_reconnect=True)

    def qry_instrument(self):
        """
        查询/更新合约信息
        :return:
        """
        if not self.connection_status:
            self.gateway.write_error(u'tdx连接状态为断开，不能查询和更新合约信息')
            return

        if self.has_qry_instrument:
            self.gateway.write_error(u'已经查询过一次合约信息，不再查询')
            return

        # 取得所有的合约信息
        num = self.api.get_instrument_count()
        if not isinstance(num, int):
            return

        all_contacts = sum(
            [self.api.get_instrument_info((int(num / 500) - i) * 500, 500) for i in range(int(num / 500) + 1)], [])
        # [{"category":category,"market": int,"code":sting,"name":string,"desc":string},{}]

        # 对所有合约处理，更新字典 指数合约-tdx市场，指数合约-交易所
        for tdx_contract in all_contacts:
            tdx_symbol = tdx_contract.get('code', None)
            if tdx_symbol is None or tdx_symbol[-2:] not in ['L9']:
                continue
            tdx_market_id = tdx_contract.get('market')
            self.symbol_market_dict[tdx_symbol] = tdx_market_id
            if tdx_market_id == 47:  # 中金所
                self.symbol_exchange_dict[tdx_symbol] = Exchange.CFFEX
            elif tdx_market_id == 28:  # 郑商所
                self.symbol_exchange_dict[tdx_symbol] = Exchange.CZCE
            elif tdx_market_id == 29:  # 大商所
                self.symbol_exchange_dict[tdx_symbol] = Exchange.DCE
            elif tdx_market_id == 30:  # 上期所+能源
                self.symbol_exchange_dict[tdx_symbol] = Exchange.SHFE
            elif tdx_market_id == 60:  # 主力合约
                self.gateway.write_log(u'主力合约:{}'.format(tdx_contract))
        self.has_qry_instrument = True

    def run(self):
        # 直接查询板块
        try:
            last_dt = datetime.now()
            self.gateway.write_log(u'开始运行tdx查询指数行情线程,{}'.format(last_dt))
            while self.connection_status:
                if len(self.registered_symbol_set) > 0:
                    try:
                        self.process_index_req()
                    except BrokenPipeError as bex:
                        self.gateway.write_error(u'BrokenPipeError{},重试重连tdx[{}]'.format(str(bex), 0))
                        self.connect(is_reconnect=True)
                        sleep(5)
                        break
                    except Exception as ex:
                        self.gateway.write_error(u'tdx exception:{},{}'.format(str(ex), traceback.format_exc()))
                        self.gateway.write_error(u'重试重连tdx')
                        self.connect(is_reconnect=True)

                sleep(self.req_interval)
                dt = datetime.now()
                if last_dt.minute != dt.minute:
                    self.gateway.write_log(
                        'tdx check point. {}, process symbols:{}'.format(dt, self.registered_symbol_set))
                    last_dt = dt
        except Exception as ex:
            self.gateway.write_error(u'tdx thead.run exception:{},{}'.format(str(ex), traceback.format_exc()))

        self.gateway.write_error(u'tdx查询线程 {}退出'.format(datetime.now()))

    def process_index_req(self):
        """处理板块获取指数行情tick"""

        # 获取通达信指数板块所有行情
        rt_list = self.api.get_instrument_quote_list(42, 3, 0, 100)

        if rt_list is None or len(rt_list) == 0:
            self.gateway.write_log(u'tdx: rt_list为空')
            return

        # 记录该接口的行情最后更新时间
        self.last_tick_dt = datetime.now()

        for d in list(rt_list):
            tdx_symbol = d.get('code', None)
            if tdx_symbol not in self.registered_symbol_set and tdx_symbol is not None:
                continue
            # tdx_symbol => vn_symbol
            vn_symbol = self.symbol_vn_dict.get(tdx_symbol, None)
            if vn_symbol is None:
                self.gateway.write_error(u'self.symbol_vn_dict 取不到映射得:{}'.format(tdx_symbol))
                continue
            # vn_symbol => exchange
            exchange = self.symbol_exchange_dict.get(tdx_symbol, None)
            underlying_symbol = get_underlying_symbol(vn_symbol)

            if exchange is None:
                symbol_info = future_contracts.get(underlying_symbol, None)
                if not symbol_info:
                    continue
                exchange_value = symbol_info.get('exchange', None)
                exchange = Exchange(exchange_value)
                if exchange is None:
                    continue
                self.symbol_exchange_dict.update({tdx_symbol: exchange})

            tick_datetime = datetime.now()
            # 修正毫秒
            last_tick = self.symbol_tick_dict.get(vn_symbol, None)
            if (last_tick is not None) and tick_datetime.replace(microsecond=0) == last_tick.datetime:
                # 与上一个tick的时间（去除毫秒后）相同,修改为500毫秒
                tick_datetime = tick_datetime.replace(microsecond=500)
            else:
                tick_datetime = tick_datetime.replace(microsecond=0)

            tick = TickData(gateway_name=self.gateway_name,
                            symbol=vn_symbol,
                            exchange=exchange,
                            datetime=tick_datetime)

            tick.pre_close = float(d.get('ZuoJie', 0.0))
            tick.high_price = float(d.get('ZuiGao', 0.0))
            tick.open_price = float(d.get('JinKai', 0.0))
            tick.low_price = float(d.get('ZuiDi', 0.0))
            tick.last_price = float(d.get('MaiChu', 0.0))
            tick.volume = int(d.get('XianLiang', 0))
            tick.last_volume = tick.volume
            tick.open_interest = d.get('ChiCangLiang')

            tick.time = tick.datetime.strftime('%H:%M:%S.%f')[0:12]
            tick.date = tick.datetime.strftime('%Y-%m-%d')

            tick.trading_day = get_trading_date(tick.datetime)

            # 指数没有涨停和跌停，就用昨日收盘价正负10%
            tick.limit_up = tick.pre_close * 1.1
            tick.limit_down = tick.pre_close * 0.9

            # CTP只有一档行情
            tick.bid_price_1 = float(d.get('MaiRuJia', 0.0))
            tick.bid_volume_1 = int(d.get('MaiRuLiang', 0))
            tick.ask_price_1 = float(d.get('MaiChuJia', 0.0))
            tick.ask_volume_1 = int(d.get('MaiChuLiang', 0))

            # 排除非交易时间得tick
            if tick.exchange is Exchange.CFFEX:
                if tick.datetime.hour not in [9, 10, 11, 13, 14, 15]:
                    continue
                if tick.datetime.hour == 9 and tick.datetime.minute < 15:
                    continue
                # 排除早盘 11:30~12:00
                if tick.datetime.hour == 11 and tick.datetime.minute >= 30:
                    continue
                if tick.datetime.hour == 15 and tick.datetime.minute >= 15 and underlying_symbol in ['T', 'TF', 'TS']:
                    continue
                if tick.datetime.hour == 15 and underlying_symbol in ['IH', 'IF', 'IC']:
                    continue
            else:  # 大商所/郑商所，上期所，上海能源
                # 排除非开盘小时
                if tick.datetime.hour in [3, 4, 5, 6, 7, 8, 12, 15, 16, 17, 18, 19, 20]:
                    continue
                # 排除早盘 10:15~10:30
                if tick.datetime.hour == 10 and 15 <= tick.datetime.minute < 30:
                    continue
                # 排除早盘 11:30~12:00
                if tick.datetime.hour == 11 and tick.datetime.minute >= 30:
                    continue
                # 排除午盘 13:00 ~13:30
                if tick.datetime.hour == 13 and tick.datetime.minute < 30:
                    continue
                # 排除凌晨2:30~3:00
                if tick.datetime.hour == 2 and tick.datetime.minute >= 30:
                    continue

                # 排除大商所/郑商所夜盘数据上期所夜盘数据 23:00 收盘
                if underlying_symbol in NIGHT_MARKET_23:
                    if tick.datetime.hour in [23, 0, 1, 2]:
                        continue
                # 排除上期所夜盘数据 1:00 收盘
                if underlying_symbol in NIGHT_MARKET_SQ2:
                    if tick.datetime.hour in [1, 2]:
                        continue

            # 排除日盘合约在夜盘得数据
            if underlying_symbol in MARKET_DAY_ONLY and (tick.datetime.hour < 9 or tick.datetime.hour > 16):
                # self.write_log(u'排除日盘合约{}在夜盘得数据'.format(short_symbol))
                continue

            # self.gateway.write_log(f'{tick.__dict__}')
            pre_tick = self.symbol_tick_dict.get(tick.symbol, None)
            self.symbol_tick_dict[tick.symbol] = tick

            # 排除指数的异常数据(tdx有些服务器异常，返回数据偏差超过上一tick的20%）
            if pre_tick:
                if tick.last_price > pre_tick.last_price * 1.2 or tick.last_price < pre_tick.last_price * 0.8:
                    continue

            self.gateway.on_tick(tick)
            self.gateway.on_custom_tick(tick)


class SubMdApi():
    """
    RabbitMQ Subscriber 数据行情接收API
    """

    def __init__(self, gateway):
        self.gateway = gateway
        self.gateway_name = gateway.gateway_name

        self.symbol_tick_dict = {}  # 合约与最后一个Tick得字典
        self.registed_symbol_set = set()  # 订阅的合约记录集

        self.last_tick_dt = None

        self.sub = None
        self.setting = {}
        self.connect_status = False
        self.thread = None

    def connect(self, setting={}):
        """连接"""
        self.setting = setting
        try:
            self.sub = subscriber(
                host=self.setting.get('host', 'localhost'),
                port=self.setting.get('port', 5672),
                user=self.setting.get('user', 'admin'),
                password=self.setting.get('password', 'admin'),
                exchange=self.setting.get('exchange', 'x_fanout_idx_tick'))

            self.sub.set_callback(self.on_message)
            self.thread = Thread(target=self.sub.start)
            self.thread.start()
            self.connect_status = True
            self.gateway.status.update({'sub_con': True, 'sub_con_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')})
        except Exception as ex:
            self.gateway.write_error(u'连接RabbitMQ {} 异常:{}'.format(self.setting, str(ex)))
            self.gateway.write_error(traceback.format_exc())
            self.connect_status = False

    def check_status(self):
        """接口状态的健康检查"""
        self.gateway.write_log("检查sub接口的状态")
        # 订阅的合约
        d = {'sub_symbols': sorted(self.symbol_tick_dict.keys())}

        # 合约的最后时间
        if self.last_tick_dt:
            d.update({"sub_tick_time": self.last_tick_dt.strftime('%Y-%m-%d %H:%M:%S')})

        if len(self.symbol_tick_dict) > 0:
            dt_now = datetime.now()
            hh_mm = dt_now.hour * 100 + dt_now.minute
            # 期货交易时间内
            if 900 <= hh_mm <= 1130 or 1300 <= hh_mm <= 1500 or hh_mm < 230 or hh_mm >= 2100:
                # 未有数据到达
                if self.last_tick_dt is None:
                    d.update({"sub_status": False, "sub_error": u"rabbitmq未有行情数据到达"})
                else: # 有数据

                    # 超时15分钟以上
                    if (dt_now - self.last_tick_dt).total_seconds() > 60 * 15:
                        d.update({"sub_status": False,
                                  "sub_error": u"{}rabbitmq行情数据超时15分钟以上".format(hh_mm)})
                    else:
                        d.update({"sub_status": True})
                        self.gateway.status.pop("sub_error", None)

            # 非交易时间
            else:
                self.gateway.status.pop("sub_status", None)
                self.gateway.status.pop("sub_error", None)

        # 更新到gateway的状态中去
        self.gateway.status.update(d)

    def on_message(self, chan, method_frame, _header_frame, body, userdata=None):
        #print(" [x] %r" % body)
        try:
            str_tick = body.decode('utf-8')
            d = json.loads(str_tick)
            d.pop('rawData', None)

            d = self.conver_update(d)

            symbol = d.pop('symbol', None)
            if symbol == 'ZC99':
                a = 1
            str_datetime = d.pop('datetime', None)
            if symbol not in self.registed_symbol_set or str_datetime is None:
                return
            if '.' in str_datetime:
                dt = datetime.strptime(str_datetime, '%Y-%m-%d %H:%M:%S.%f')
            else:
                dt = datetime.strptime(str_datetime, '%Y-%m-%d %H:%M:%S')

            tick = TickData(gateway_name=self.gateway_name,
                            exchange=Exchange(d.get('exchange')),
                            symbol=symbol,
                            datetime=dt)

            d.pop('exchange', None)
            d.pop('symbol', None)
            tick.__dict__.update(d)
            if len(tick.trading_day) == 0:
                tick.trading_day = get_trading_date(dt)

            pre_tick = self.symbol_tick_dict.get(symbol,None)
            self.symbol_tick_dict[symbol] = tick
            # 排除指数的异常数据(tdx有些服务器异常，返回数据偏差超过上一tick的20%）
            if pre_tick:
                if tick.last_price > pre_tick.last_price * 1.2 or tick.last_price < pre_tick.last_price * 0.8:
                    return

            self.last_tick_dt = tick.datetime

            self.gateway.on_tick(tick)
            self.gateway.on_custom_tick(tick)

        except Exception as ex:
            self.gateway.write_error(u'RabbitMQ on_message 异常:{}'.format(str(ex)))
            self.gateway.write_error(traceback.format_exc())

    def conver_update(self, d):
        """转换dict， vnpy1 tick dict => vnpy2 tick dict"""
        if 'vtSymbol' not in d:
            return d
        symbol = d.get('symbol')
        exchange = d.get('exchange')
        vtSymbol = d.pop('vtSymbol', symbol)
        if '.' not in symbol:
            d.update({'vt_symbol': f'{symbol}.{exchange}'})
        else:
            d.update({'vt_symbol': f'{symbol}.{Exchange.LOCAL.value}'})

        # 成交数据
        d.update({'last_price': d.pop('lastPrice', 0.0)})  # 最新成交价
        d.update({'last_volume': d.pop('lastVolume', 0)})  # 最新成交量

        d.update({'open_interest': d.pop('openInterest', 0)})  # 昨持仓量

        d.update({'open_interest': d.pop('tradingDay', get_trading_date())})

        # 常规行情
        d.update({'open_price': d.pop('openPrice', 0)})  # 今日开盘价
        d.update({'high_price': d.pop('highPrice', 0)})  # 今日最高价
        d.update({'low_price': d.pop('lowPrice', 0)})  # 今日最低价
        d.update({'pre_close': d.pop('preClosePrice', 0)})  # 昨收盘价
        d.update({'limit_up': d.pop('upperLimit', 0)})  # 涨停价
        d.update({'limit_down': d.pop('lowerLimit', 0)})  # 跌停价

        # 五档行情
        d.update({'bid_price_1': d.pop('bidPrice1', 0.0)})
        d.update({'bid_price_2': d.pop('bidPrice2', 0.0)})
        d.update({'bid_price_3': d.pop('bidPrice3', 0.0)})
        d.update({'bid_price_4': d.pop('bidPrice4', 0.0)})
        d.update({'bid_price_5': d.pop('bidPrice5', 0.0)})

        d.update({'ask_price_1': d.pop('askPrice1', 0.0)})
        d.update({'ask_price_2': d.pop('askPrice2', 0.0)})
        d.update({'ask_price_3': d.pop('askPrice3', 0.0)})
        d.update({'ask_price_4': d.pop('askPrice4', 0.0)})
        d.update({'ask_price_5': d.pop('askPrice5', 0.0)})

        d.update({'bid_volume_1': d.pop('bidVolume1', 0.0)})
        d.update({'bid_volume_2': d.pop('bidVolume2', 0.0)})
        d.update({'bid_volume_3': d.pop('bidVolume3', 0.0)})
        d.update({'bid_volume_4': d.pop('bidVolume4', 0.0)})
        d.update({'bid_volume_5': d.pop('bidVolume5', 0.0)})

        d.update({'ask_volume_1': d.pop('askVolume1', 0.0)})
        d.update({'ask_volume_2': d.pop('askVolume2', 0.0)})
        d.update({'ask_volume_3': d.pop('askVolume3', 0.0)})
        d.update({'ask_volume_4': d.pop('askVolume4', 0.0)})
        d.update({'ask_volume_5': d.pop('askVolume5', 0.0)})

        return d

    def close(self):
        """退出API"""
        self.gateway.write_log(u'退出rabbit行情订阅API')
        self.connection_status = False

        try:
            if self.sub:
                self.gateway.write_log(u'关闭订阅器')
                self.sub.close()

            if self.thread is not None:
                self.gateway.write_log(u'关闭订阅器接收线程')
                self.thread.join()
        except Exception as ex:
            self.gateway.write_error(u'退出rabbitMQ行情api异常:{}'.format(str(ex)))

    # ----------------------------------------------------------------------
    def subscribe(self, subscribeReq):
        """订阅合约"""
        # 这里的设计是，如果尚未登录就调用了订阅方法
        # 则先保存订阅请求，登录完成后会自动订阅
        vn_symbol = str(subscribeReq.symbol)
        vn_symbol = vn_symbol.upper()

        if vn_symbol not in self.registed_symbol_set:
            self.registed_symbol_set.add(vn_symbol)
            self.gateway.write_log(u'RabbitMQ行情订阅 {}'.format(str(vn_symbol)))


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

        self.last_ticks_info = {}

    def connect(self, setting):
        """"""
        try:
            from tqsdk import TqApi
            self.api = TqApi(url="wss://u.shinnytech.com/t/md/front/mobile")
        except Exception as e:
            self.gateway.write_log(f'天勤行情API接入异常'.format(str(e)),on_log=True)
        if self.api:
            self.is_connected = True
            self.gateway.write_log(f'天勤行情API已连接')
            self.update_thread = Thread(target=self.update)
            self.update_thread.start()

    def check_status(self):
        """检查接口状态"""

        pass

    def generate_tick_from_quote(self, vt_symbol, quote) -> TickData:
        """
        生成TickData
        """
        # 清洗 nan
        quote = {k: 0 if v != v else v for k, v in quote.items()}
        symbol, exchange = extract_vt_symbol(vt_symbol)
        tick_dt = datetime.strptime(quote["datetime"], "%Y-%m-%d %H:%M:%S.%f")

        today_volume = quote["volume"]
        last_tick_info = self.last_ticks_info.get(symbol, {})
        trading_day = get_trading_date(tick_dt)
        last_trading_day = last_tick_info.get('trading_day', None)
        if last_trading_day == trading_day:
            volume_changed = max(0, today_volume - last_tick_info.get('volume', 0))
        else:
            volume_changed = today_volume

        self.last_ticks_info.update({symbol: {'trading_day': trading_day, 'volume': today_volume}})

        tick = TickData(
            symbol=symbol,
            exchange=exchange,
            datetime=tick_dt,
            trading_day=trading_day,
            name=symbol,
            volume=today_volume,
            last_volume=volume_changed,
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
        if symbol.endswith('99') and tick.ask_price_1 == 0.0 and tick.bid_price_1 == 0.0:
            price_tick = quote['price_tick']
            if isinstance(price_tick, float) or isinstance(price_tick, int):
                tick.ask_price_1 = tick.last_price + price_tick
                tick.ask_volume_1 = 1
                tick.bid_price_1 = tick.last_price - price_tick
                tick.bid_volume_1 = 1

        return tick

    def update(self) -> None:
        """
        更新行情/委托/账户/持仓
        """
        while self.is_connected:
            deadline = time() + 5
            self.api.wait_update(deadline=deadline)
            # 更新行情信息
            for vt_symbol, quote in self.quote_objs:
                if self.api.is_changing(quote):
                    tick = self.generate_tick_from_quote(vt_symbol, quote)
                    if tick:
                        self.gateway.on_tick(tick)
                        self.gateway.on_custom_tick(tick)

    def subscribe(self, req: SubscribeRequest) -> None:
        """
        订阅行情
        """
        if req.vt_symbol not in self.subscribe_array:
            symbol, exchange = extract_vt_symbol(req.vt_symbol)
            try:
                quote = self.api.get_quote(vt_to_tq_symbol(symbol, exchange))
                self.quote_objs.append((req.vt_symbol, quote))
                self.subscribe_array.append(req.vt_symbol)
            except Exception as ex:
                self.gateway.write_log('订阅天勤行情异常:{}'.format(str(ex)),on_log=True)

    def query_contracts(self) -> None:
        """"""
        self.all_instruments = [
            v for k, v in self.api._data["quotes"].items() if v["expired"] == False
        ]
        for contract in self.all_instruments:
            if (
                    "SSWE" in contract["instrument_id"]
                    or "CSI" in contract["instrument_id"]
            ):
                # vnpy没有这两个交易所，需要可以自行修改vnpy代码
                continue

            vt_symbol = tq_to_vt_symbol(contract["instrument_id"])
            symbol, exchange = extract_vt_symbol(vt_symbol)

            if TQ2VT_TYPE[contract["ins_class"]] == Product.OPTION:
                contract_data = ContractData(
                    symbol=symbol,
                    exchange=exchange,
                    name=symbol,
                    product=TQ2VT_TYPE[contract["ins_class"]],
                    size=contract["volume_multiple"],
                    pricetick=contract["price_tick"],
                    history_data=True,
                    option_strike=contract["strike_price"],
                    option_underlying=tq_to_vt_symbol(contract["underlying_symbol"]),
                    option_type=OptionType[contract["option_class"]],
                    option_expiry=datetime.fromtimestamp(contract["expire_datetime"]),
                    option_index=tq_to_vt_symbol(contract["underlying_symbol"]),
                    gateway_name=self.gateway_name,
                )
            else:
                contract_data = ContractData(
                    symbol=symbol,
                    exchange=exchange,
                    name=symbol,
                    product=TQ2VT_TYPE[contract["ins_class"]],
                    size=contract["volume_multiple"],
                    pricetick=contract["price_tick"],
                    history_data=True,
                    gateway_name=self.gateway_name,
                )
            self.gateway.on_contract(contract_data)

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
        tq_symbol = vt_to_tq_symbol(symbol, exchange)
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
            if self.api:
                self.is_connected = False
                if self.update_thread:
                    self.update_thread.join()
                self.api.close()

        except Exception as e:
            self.gateway.write_log('退出天勤行情api异常:{}'.format(str(e)),on_log=True)
