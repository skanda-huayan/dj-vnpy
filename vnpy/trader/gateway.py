"""

"""
import sys
from abc import ABC, abstractmethod
from typing import Any, Sequence, Dict, List, Optional, Callable
from copy import copy,deepcopy
from logging import INFO, DEBUG, ERROR
from datetime import datetime

from vnpy.event import Event, EventEngine
from .event import (
    EVENT_TICK,
    EVENT_BAR,
    EVENT_ORDER,
    EVENT_TRADE,
    EVENT_POSITION,
    EVENT_ACCOUNT,
    EVENT_CONTRACT,
    EVENT_LOG,
)
from .object import (
    TickData,
    BarData,
    OrderData,
    TradeData,
    PositionData,
    AccountData,
    ContractData,
    LogData,
    OrderRequest,
    CancelRequest,
    SubscribeRequest,
    HistoryRequest,
    Exchange
)

from vnpy.trader.utility import get_folder_path, round_to, get_underlying_symbol, get_real_symbol_by_exchange
from vnpy.trader.util_logger import setup_logger


class BaseGateway(ABC):
    """
    Abstract gateway class for creating gateways connection
    to different trading systems.

    # How to implement a gateway:

    ---
    ## Basics
    A gateway should satisfies:
    * this class should be thread-safe:
        * all methods should be thread-safe
        * no mutable shared properties between objects.
    * all methods should be non-blocked
    * satisfies all requirements written in docstring for every method and callbacks.
    * automatically reconnect if connection lost.

    ---
    ## methods must implements:
    all @abstractmethod

    ---
    ## callbacks must response manually:
    * on_tick
    * on_bar
    * on_trade
    * on_order
    * on_position
    * on_account
    * on_contract

    All the XxxData passed to callback should be constant, which means that
        the object should not be modified after passing to on_xxxx.
    So if you use a cache to store reference of data, use copy.copy to create a new object
    before passing that data into on_xxxx



    """

    # Fields required in setting dict for connect function.
    default_setting: Dict[str, Any] = {}

    # Exchanges supported in the gateway.
    exchanges: List[Exchange] = []

    def __init__(self, event_engine: EventEngine, gateway_name: str):
        """"""
        self.event_engine: EventEngine = event_engine
        self.gateway_name: str = gateway_name
        self.logger = None

        self.accountid = ""

        self.create_logger()

        # 所有订阅on_bar的都会添加
        self.klines = {}
        self.status = {'name': gateway_name, 'con': False}

        self.prices: Dict[str, float] = {}  # vt_symbol, last_price

        self.query_functions = []

    def create_logger(self):
        """
        创建engine独有的日志
        :return:
        """
        log_path = get_folder_path("log")
        log_filename = str(log_path.joinpath(self.gateway_name))
        print(u'create logger:{}'.format(log_filename))
        from vnpy.trader.setting import SETTINGS
        self.logger = setup_logger(file_name=log_filename, name=self.gateway_name,
                                   log_level=SETTINGS.get('log.level', DEBUG))

    def on_event(self, type: str, data: Any = None) -> None:
        """
        General event push.
        """
        event = Event(type, data)
        self.event_engine.put(event)

    def on_tick(self, tick: TickData) -> None:
        """
        Tick event push.
        Tick event of a specific vt_symbol is also pushed.
        """
        self.prices.update({tick.vt_symbol: tick.last_price})
        self.on_event(EVENT_TICK, tick)
        # self.on_event(EVENT_TICK + tick.vt_symbol, tick)

        # 推送Bar
        kline = self.klines.get(tick.vt_symbol, None)
        if kline:
            kline.update_tick(tick)

    def on_bar(self, bar: BarData) -> None:
        """市场行情推送"""
        # bar, 或者 barDict
        self.on_event(EVENT_BAR, bar)
        #self.write_log(f'on_bar Event:{bar.__dict__}')

    def on_trade(self, trade: TradeData) -> None:
        """
        Trade event push.
        Trade event of a specific vt_symbol is also pushed.
        """
        self.on_event(EVENT_TRADE, trade)
        # self.on_event(EVENT_TRADE + trade.vt_symbol, trade)

    def on_order(self, order: OrderData) -> None:
        """
        Order event push.
        Order event of a specific vt_orderid is also pushed.
        """
        self.on_event(EVENT_ORDER, order)
        # self.on_event(EVENT_ORDER + order.vt_orderid, order)

    def on_position(self, position: PositionData) -> None:
        """
        Position event push.
        Position event of a specific vt_symbol is also pushed.
        """
        self.on_event(EVENT_POSITION, position)
        # self.on_event(EVENT_POSITION + position.vt_symbol, position)

    def on_account(self, account: AccountData) -> None:
        """
        Account event push.
        Account event of a specific vt_accountid is also pushed.
        """
        self.on_event(EVENT_ACCOUNT, account)
        # self.on_event(EVENT_ACCOUNT + account.vt_accountid, account)

    def on_log(self, log: LogData) -> None:
        """
        Log event push.
        """
        self.on_event(EVENT_LOG, log)

    def on_contract(self, contract: ContractData) -> None:
        """
        Contract event push.
        """
        self.on_event(EVENT_CONTRACT, contract)

    def write_log(self, msg: str, level: int = INFO, on_log: bool = False) -> None:
        """
        Write a log event from gateway.
        """
        if self.logger:
            self.logger.log(level, msg)

        if on_log:
            log = LogData(msg=msg, level=level, gateway_name=self.gateway_name)
            self.on_log(log)

    def write_error(self, msg: str, error: dict = {}):
        """
        write error log
        :param msg:
        :return:
        """
        if len(error) > 0:
            error_id = error.get("ErrorID", '')
            error_msg = error.get("ErrorMsg", '')
            msg = f"{msg}，代码：{error_id}，信息：{error_msg}"
        self.write_log(msg, level=ERROR, on_log=True)
        print(msg, file=sys.stderr)

    def check_status(self) -> bool:
        """
        check gateway connection or market data status.
        """
        return False

    @abstractmethod
    def connect(self, setting: dict) -> None:
        """
        Start gateway connection.

        to implement this method, you must:
        * connect to server if necessary
        * log connected if all necessary connection is established
        * do the following query and response corresponding on_xxxx and write_log
            * contracts : on_contract
            * account asset : on_account
            * account holding: on_position
            * orders of account: on_order
            * trades of account: on_trade
        * if any of query above is failed,  write log.

        future plan:
        response callback/change status instead of write_log

        """
        pass

    @abstractmethod
    def close(self) -> None:
        """
        Close gateway connection.
        """
        pass

    @abstractmethod
    def subscribe(self, req: SubscribeRequest) -> None:
        """
        Subscribe tick data update.
        """
        pass

    @abstractmethod
    def send_order(self, req: OrderRequest) -> str:
        """
        Send a new order to server.

        implementation should finish the tasks blow:
        * create an OrderData from req using OrderRequest.create_order_data
        * assign a unique(gateway instance scope) id to OrderData.orderid
        * send request to server
            * if request is sent, OrderData.status should be set to Status.SUBMITTING
            * if request is failed to sent, OrderData.status should be set to Status.REJECTED
        * response on_order:
        * return vt_orderid

        :return str vt_orderid for created OrderData
        """
        pass

    @abstractmethod
    def cancel_order(self, req: CancelRequest) -> None:
        """
        Cancel an existing order.
        implementation should finish the tasks blow:
        * send request to server
        """
        return False

    def send_orders(self, reqs: Sequence[OrderRequest]) -> List[str]:
        """
        Send a batch of orders to server.
        Use a for loop of send_order function by default.
        Reimplement this function if batch order supported on server.
        """
        vt_orderids = []

        for req in reqs:
            vt_orderid = self.send_order(req)
            vt_orderids.append(vt_orderid)

        return vt_orderids

    def cancel_orders(self, reqs: Sequence[CancelRequest]) -> None:
        """
        Cancel a batch of orders to server.
        Use a for loop of cancel_order function by default.
        Reimplement this function if batch cancel supported on server.
        """
        for req in reqs:
            self.cancel_order(req)

    @abstractmethod
    def query_account(self) -> None:
        """
        Query account balance.
        """
        pass

    @abstractmethod
    def query_position(self) -> None:
        """
        Query holding positions.
        """
        pass

    def query_history(self, req: HistoryRequest) -> List[BarData]:
        """
        Query bar history data.
        """
        pass

    def get_default_setting(self) -> Dict[str, Any]:
        """
        Return default setting dict.
        """
        return self.default_setting

    def get_status(self) -> Dict[str, Any]:
        """
        return gateway status
        :return:
        """
        return self.status


class TickCombiner(object):
    """
    Tick合成类
    """

    def __init__(self, gateway, setting):
        self.gateway = gateway
        self.gateway_name = self.gateway.gateway_name
        self.gateway.write_log(u'创建tick合成类:{}'.format(setting))

        self.symbol = setting.get('symbol', None)
        self.leg1_symbol = setting.get('leg1_symbol', None)
        self.leg2_symbol = setting.get('leg2_symbol', None)
        self.leg1_ratio = setting.get('leg1_ratio', 1)  # 腿1的数量配比
        self.leg2_ratio = setting.get('leg2_ratio', 1)  # 腿2的数量配比
        self.price_tick = setting.get('price_tick', 1)  # 合成价差加比后的最小跳动
        # 价差
        self.is_spread = setting.get('is_spread', False)
        # 价比
        self.is_ratio = setting.get('is_ratio', False)

        self.last_leg1_tick = None
        self.last_leg2_tick = None

        # 价差日内最高/最低价
        self.spread_high = None
        self.spread_low = None

        # 价比日内最高/最低价
        self.ratio_high = None
        self.ratio_low = None

        # 当前交易日
        self.trading_day = None

        if self.is_ratio and self.is_spread:
            self.gateway.write_error(u'{}参数有误，不能同时做价差/加比.setting:{}'.format(self.symbol, setting))
            return

        self.gateway.write_log(u'初始化{}合成器成功'.format(self.symbol))
        if self.is_spread:
            self.gateway.write_log(
                u'leg1:{} * {} - leg2:{} * {}'.format(self.leg1_symbol, self.leg1_ratio, self.leg2_symbol,
                                                      self.leg2_ratio))
        if self.is_ratio:
            self.gateway.write_log(
                u'leg1:{} * {} / leg2:{} * {}'.format(self.leg1_symbol, self.leg1_ratio, self.leg2_symbol,
                                                      self.leg2_ratio))

    def on_tick(self, tick):
        """OnTick处理"""
        combinable = False

        if tick.symbol == self.leg1_symbol:
            # leg1合约
            self.last_leg1_tick = tick
            if self.last_leg2_tick is not None:
                #if self.last_leg1_tick.datetime.replace(microsecond=0) == self.last_leg2_tick.datetime.replace(
                #        microsecond=0):
                # 有些跨交易所时间戳会不一致，差1~2秒
                if abs((self.last_leg1_tick.datetime - self.last_leg2_tick.datetime).total_seconds())<3:
                    combinable = True

        elif tick.symbol == self.leg2_symbol:
            # leg2合约
            self.last_leg2_tick = tick
            if self.last_leg1_tick is not None:
                # if self.last_leg2_tick.datetime.replace(microsecond=0) == self.last_leg1_tick.datetime.replace(
                #         microsecond=0):
                if abs((self.last_leg1_tick.datetime - self.last_leg2_tick.datetime).total_seconds()) < 3:
                    combinable = True

        # 不能合并
        if not combinable:
            return

        if not self.is_ratio and not self.is_spread:
            return

        # 以下情况，基本为单腿涨跌停，不合成价差/价格比 Tick
        if (self.last_leg1_tick.ask_price_1 == 0 or self.last_leg1_tick.bid_price_1 == self.last_leg1_tick.limit_up) \
                and self.last_leg1_tick.ask_volume_1 == 0:
            self.gateway.write_log(
                u'leg1:{0}涨停{1}，不合成价差Tick'.format(self.last_leg1_tick.vt_symbol, self.last_leg1_tick.bid_price_1))
            return
        if (self.last_leg1_tick.bid_price_1 == 0 or self.last_leg1_tick.ask_price_1 == self.last_leg1_tick.limit_down) \
                and self.last_leg1_tick.bid_volume_1 == 0:
            self.gateway.write_log(
                u'leg1:{0}跌停{1}，不合成价差Tick'.format(self.last_leg1_tick.vt_symbol, self.last_leg1_tick.ask_price_1))
            return
        if (self.last_leg2_tick.ask_price_1 == 0 or self.last_leg2_tick.bid_price_1 == self.last_leg2_tick.limit_up) \
                and self.last_leg2_tick.ask_volume_1 == 0:
            self.gateway.write_log(
                u'leg2:{0}涨停{1}，不合成价差Tick'.format(self.last_leg2_tick.vt_symbol, self.last_leg2_tick.bid_price_1))
            return
        if (self.last_leg2_tick.bid_price_1 == 0 or self.last_leg2_tick.ask_price_1 == self.last_leg2_tick.limit_down) \
                and self.last_leg2_tick.bid_volume_1 == 0:
            self.gateway.write_log(
                u'leg2:{0}跌停{1}，不合成价差Tick'.format(self.last_leg2_tick.vt_symbol, self.last_leg2_tick.ask_price_1))
            return

        if self.trading_day != tick.trading_day:
            self.trading_day = tick.trading_day
            self.spread_high = None
            self.spread_low = None
            self.ratio_high = None
            self.ratio_low = None

        if self.is_spread:
            spread_tick = TickData(gateway_name=self.gateway_name,
                                   symbol=self.symbol,
                                   exchange=Exchange.SPD,
                                   datetime=tick.datetime)

            spread_tick.trading_day = tick.trading_day
            spread_tick.date = tick.date
            spread_tick.time = tick.time

            # 叫卖价差=leg1.ask_price_1 * 配比 - leg2.bid_price_1 * 配比，volume为两者最小
            spread_tick.ask_price_1 = round_to(target=self.price_tick,
                                               value=self.last_leg1_tick.ask_price_1 * self.leg1_ratio - self.last_leg2_tick.bid_price_1 * self.leg2_ratio)
            spread_tick.ask_volume_1 = min(self.last_leg1_tick.ask_volume_1, self.last_leg2_tick.bid_volume_1)

            # 叫买价差=leg1.bid_price_1 * 配比 - leg2.ask_price_1 * 配比，volume为两者最小
            spread_tick.bid_price_1 = round_to(target=self.price_tick,
                                               value=self.last_leg1_tick.bid_price_1 * self.leg1_ratio - self.last_leg2_tick.ask_price_1 * self.leg2_ratio)
            spread_tick.bid_volume_1 = min(self.last_leg1_tick.bid_volume_1, self.last_leg2_tick.ask_volume_1)

            # 最新价
            spread_tick.last_price = round_to(target=self.price_tick,
                                              value=(spread_tick.ask_price_1 + spread_tick.bid_price_1) / 2)
            # 昨收盘价
            if self.last_leg2_tick.pre_close > 0 and self.last_leg1_tick.pre_close > 0:
                spread_tick.pre_close = round_to(target=self.price_tick,
                                                 value=self.last_leg1_tick.pre_close * self.leg1_ratio - self.last_leg2_tick.pre_close * self.leg2_ratio)
            # 开盘价
            if self.last_leg2_tick.open_price > 0 and self.last_leg1_tick.open_price > 0:
                spread_tick.open_price = round_to(target=self.price_tick,
                                                  value=self.last_leg1_tick.open_price * self.leg1_ratio - self.last_leg2_tick.open_price * self.leg2_ratio)
            # 最高价
            if self.spread_high:
                self.spread_high = max(self.spread_high, spread_tick.ask_price_1)
            else:
                self.spread_high = spread_tick.ask_price_1
            spread_tick.high_price = self.spread_high

            # 最低价
            if self.spread_low:
                self.spread_low = min(self.spread_low, spread_tick.bid_price_1)
            else:
                self.spread_low = spread_tick.bid_price_1

            spread_tick.low_price = self.spread_low

            self.gateway.on_tick(spread_tick)

        if self.is_ratio:
            ratio_tick = TickData(
                gateway_name=self.gateway_name,
                symbol=self.symbol,
                exchange=Exchange.SPD,
                datetime=tick.datetime
            )

            ratio_tick.trading_day = tick.trading_day
            ratio_tick.date = tick.date
            ratio_tick.time = tick.time

            # 比率tick = (腿1 * 腿1 手数 / 腿2价格 * 腿2手数) 百分比
            ratio_tick.ask_price_1 = 100 * self.last_leg1_tick.ask_price_1 * self.leg1_ratio \
                                     / (self.last_leg2_tick.bid_price_1 * self.leg2_ratio)  # noqa
            ratio_tick.ask_price_1 = round_to(
                target=self.price_tick,
                value=ratio_tick.ask_price_1
            )

            ratio_tick.ask_volume_1 = min(self.last_leg1_tick.ask_volume_1, self.last_leg2_tick.bid_volume_1)
            ratio_tick.bid_price_1 = 100 * self.last_leg1_tick.bid_price_1 * self.leg1_ratio \
                                     / (self.last_leg2_tick.ask_price_1 * self.leg2_ratio)  # noqa
            ratio_tick.bid_price_1 = round_to(
                target=self.price_tick,
                value=ratio_tick.bid_price_1
            )

            ratio_tick.bid_volume_1 = min(self.last_leg1_tick.bid_volume_1, self.last_leg2_tick.ask_volume_1)
            ratio_tick.last_price = (ratio_tick.ask_price_1 + ratio_tick.bid_price_1) / 2
            ratio_tick.last_price = round_to(
                target=self.price_tick,
                value=ratio_tick.last_price
            )

            # 昨收盘价
            if self.last_leg2_tick.pre_close > 0 and self.last_leg1_tick.pre_close > 0:
                ratio_tick.pre_close = 100 * self.last_leg1_tick.pre_close * self.leg1_ratio / (
                        self.last_leg2_tick.pre_close * self.leg2_ratio)  # noqa
                ratio_tick.pre_close = round_to(
                    target=self.price_tick,
                    value=ratio_tick.pre_close
                )

            # 开盘价
            if self.last_leg2_tick.open_price > 0 and self.last_leg1_tick.open_price > 0:
                ratio_tick.open_price = 100 * self.last_leg1_tick.open_price * self.leg1_ratio / (
                        self.last_leg2_tick.open_price * self.leg2_ratio)  # noqa
                ratio_tick.open_price = round_to(
                    target=self.price_tick,
                    value=ratio_tick.open_price
                )

            # 最高价
            if self.ratio_high:
                self.ratio_high = max(self.ratio_high, ratio_tick.ask_price_1)
            else:
                self.ratio_high = ratio_tick.ask_price_1
            ratio_tick.high_price = self.spread_high

            # 最低价
            if self.ratio_low:
                self.ratio_low = min(self.ratio_low, ratio_tick.bid_price_1)
            else:
                self.ratio_low = ratio_tick.bid_price_1

            ratio_tick.low_price = self.spread_low

            self.gateway.on_tick(ratio_tick)

class IndexGenerator:
    """
    指数生成器
    """

    def __init__(self, gateway, setting):
        self.gateway = gateway
        self.gateway_name = self.gateway.gateway_name
        self.gateway.write_log(u'创建指数合成类:{}'.format(setting))

        self.ticks = {}  # 所有真实合约, symbol: tick
        self.last_dt = None  # 最后tick得时间
        self.underlying_symbol = setting.get('underlying_symbol')
        self.exchange = setting.get('exchange', None)
        self.price_tick = setting.get('price_tick')
        self.symbols = setting.get('symbols', {})
        # 订阅行情
        self.subscribe()

        self.n = len(self.symbols)

    def subscribe(self):
        """订阅行情"""
        dt_now = datetime.now()
        for symbol in list(self.symbols.keys()):
            pre_open_interest = self.symbols.get(symbol,0)
            # 全路径合约 => 标准合约 ,如 ZC2109 => ZC109, RB2110 => rb2110
            vn_symbol = get_real_symbol_by_exchange(symbol, Exchange(self.exchange))
            # 先移除
            self.symbols.pop(symbol, None)
            if symbol.replace(self.underlying_symbol, '') < dt_now.strftime('%Y%m%d'):
                self.gateway.write_log(f'移除早于当月的合约{symbol}')
                continue

            # 重新登记合约
            self.symbols[vn_symbol] = pre_open_interest

            # 发出订阅
            req = SubscribeRequest(
                symbol=vn_symbol,
                exchange=Exchange(self.exchange)
            )
            self.gateway.subscribe(req)

    def on_tick(self, tick):
        """tick到达事件"""
        # 更新tick
        if self.ticks is {}:
            self.ticks.update({tick.symbol: tick})
            return

        # 进行指数合成
        if self.last_dt and tick.datetime.second != self.last_dt.second:
            all_amount = 0
            all_interest = 0
            all_volume = 0
            all_ask1 = 0
            all_bid1 = 0
            last_price = 0
            ask_price_1 = 0
            bid_price_1 = 0
            mi_tick = None

            # 已经积累的行情tick数量，不足总数减1，不处理

            if len(self.ticks) < min(self.n * 0.8, 3):
                self.gateway.write_log(f'{self.underlying_symbol}合约数据{len(self.ticks)}不足{self.n} 0.8,暂不合成指数')
                return

            # 计算所有合约的累加持仓量、资金、成交量、找出最大持仓量的主力合约
            for t in self.ticks.values():
                all_interest += t.open_interest
                all_amount += t.last_price * t.open_interest
                all_volume += t.volume
                all_ask1 += t.ask_price_1 * t.open_interest
                all_bid1 += t.bid_price_1 * t.open_interest
                if mi_tick is None or mi_tick.open_interest < t.open_interest:
                    mi_tick = t

            # 总量 > 0
            if all_interest > 0 and all_amount > 0:
                last_price = round(float(all_amount / all_interest), 4)
            # 卖1价
            if all_ask1 > 0 and all_interest > 0:
                ask_price_1 = round(float(all_ask1 / all_interest), 4)
            # 买1价
            if all_bid1 > 0 and all_interest > 0:
                bid_price_1 = round(float(all_bid1 / all_interest), 4)

            if mi_tick and last_price > 0:
                idx_tick = deepcopy(mi_tick)
                idx_tick.symbol = f'{self.underlying_symbol}99'
                idx_tick.vt_symbol = f'{idx_tick.symbol}.{self.exchange}'
                idx_tick.open_interest = all_interest
                idx_tick.volume = all_volume
                idx_tick.last_price = last_price
                idx_tick.ask_price_1 = ask_price_1
                idx_tick.bid_price_1 = bid_price_1

                self.gateway.on_tick(idx_tick)

        # 更新时间
        self.last_dt = tick.datetime
        # 更新tick
        self.ticks.update({tick.symbol: tick})


class LocalOrderManager:
    """
    Management tool to support use local order id for trading.
    """

    def __init__(self, gateway: BaseGateway, order_prefix: str = "", order_rjust: int = 8):
        """"""
        self.gateway: BaseGateway = gateway

        # For generating local orderid
        self.order_prefix: str = order_prefix
        self.order_rjust: int = order_rjust
        self.order_count: int = 0

        self.orders: Dict[str, OrderData] = {}  # local_orderid: order

        # Map between local and system orderid
        self.local_sys_orderid_map: Dict[str, str] = {}
        self.sys_local_orderid_map: Dict[str, str] = {}

        # Push order data buf
        self.push_data_buf: Dict[str, Dict] = {}  # sys_orderid: data

        # Callback for processing push order data
        self.push_data_callback: Callable = None

        # Cancel request buf
        self.cancel_request_buf: Dict[str, CancelRequest] = {}  # local_orderid: req

        # Hook cancel order function
        self._cancel_order = gateway.cancel_order
        gateway.cancel_order = self.cancel_order

    def new_local_orderid(self) -> str:
        """
        Generate a new local orderid.
        """
        self.order_count += 1
        local_orderid = self.order_prefix + str(self.order_count).rjust(self.order_rjust, "0")
        return local_orderid

    def get_local_orderid(self, sys_orderid: str) -> str:
        """
        Get local orderid with sys orderid.
        """
        local_orderid = self.sys_local_orderid_map.get(sys_orderid, "")

        if not local_orderid:
            local_orderid = self.new_local_orderid()
            self.update_orderid_map(local_orderid, sys_orderid)

        return local_orderid

    def get_sys_orderid(self, local_orderid: str) -> str:
        """
        Get sys orderid with local orderid.
        """
        sys_orderid = self.local_sys_orderid_map.get(local_orderid, "")
        return sys_orderid

    def update_orderid_map(self, local_orderid: str, sys_orderid: str) -> None:
        """
        Update orderid map.
        """
        self.sys_local_orderid_map[sys_orderid] = local_orderid
        self.local_sys_orderid_map[local_orderid] = sys_orderid

        self.check_cancel_request(local_orderid)
        self.check_push_data(sys_orderid)

    def check_push_data(self, sys_orderid: str) -> None:
        """
        Check if any order push data waiting.
        """
        if sys_orderid not in self.push_data_buf:
            return

        data = self.push_data_buf.pop(sys_orderid)
        if self.push_data_callback:
            self.push_data_callback(data)

    def add_push_data(self, sys_orderid: str, data: dict) -> None:
        """
        Add push data into buf.
        """
        self.push_data_buf[sys_orderid] = data

    def get_order_with_sys_orderid(self, sys_orderid: str) -> Optional[OrderData]:
        """"""
        local_orderid = self.sys_local_orderid_map.get(sys_orderid, None)
        if not local_orderid:
            return None
        else:
            return self.get_order_with_local_orderid(local_orderid)

    def get_order_with_local_orderid(self, local_orderid: str) -> OrderData:
        """"""
        order = self.orders.get(local_orderid, None)
        if order:
            return copy(order)
        else:
            return None

    def on_order(self, order: OrderData) -> None:
        """
        Keep an order buf before pushing it to gateway.
        """
        self.orders[order.orderid] = copy(order)
        self.gateway.on_order(order)

    def cancel_order(self, req: CancelRequest) -> None:
        """
        """
        sys_orderid = self.get_sys_orderid(req.orderid)
        if not sys_orderid:
            self.cancel_request_buf[req.orderid] = req
            return

        self._cancel_order(req)

    def check_cancel_request(self, local_orderid: str) -> None:
        """
        """
        if local_orderid not in self.cancel_request_buf:
            return

        req = self.cancel_request_buf.pop(local_orderid)
        self.gateway.cancel_order(req)
