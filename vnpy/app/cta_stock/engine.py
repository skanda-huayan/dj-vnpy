"""
股票CTA策略运行引擎
华富资产：
"""

import importlib
import os
import sys
import traceback
import json
import pickle
import bz2

from collections import defaultdict
from pathlib import Path
from typing import Any, Callable, List, Dict
from datetime import datetime, timedelta
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from copy import copy
from functools import lru_cache
from uuid import uuid1

from vnpy.event import Event, EventEngine
from vnpy.trader.engine import BaseEngine, MainEngine
from vnpy.trader.object import (
    OrderRequest,
    SubscribeRequest,
    LogData,
    TickData,
    BarData,
    PositionData,
    ContractData,
    HistoryRequest
)

from vnpy.trader.event import (
    EVENT_TIMER,
    EVENT_TICK,
    EVENT_BAR,
    EVENT_ORDER,
    EVENT_TRADE,
    EVENT_POSITION,
    EVENT_STRATEGY_POS,
    EVENT_STRATEGY_SNAPSHOT
)
from vnpy.trader.constant import (
    Direction,
    OrderType,
    Offset,
    Status,
    Interval
)
from vnpy.trader.utility import (
    load_json,
    save_json,
    extract_vt_symbol,
    round_to,
    TRADER_DIR,
    get_folder_path,
    get_underlying_symbol,
    append_data,
    import_module_by_str)

from vnpy.trader.util_logger import setup_logger, logging
from vnpy.trader.util_wechat import send_wx_msg
from vnpy.trader.converter import PositionHolding

from .base import (
    APP_NAME,
    EVENT_CTA_LOG,
    EVENT_CTA_STRATEGY,
    EVENT_CTA_STOPORDER,
    EngineType,
    StopOrder,
    StopOrderStatus,
    STOPORDER_PREFIX,
)
from .template import CtaTemplate
from vnpy.component.cta_position import CtaPosition

STOP_STATUS_MAP = {
    Status.SUBMITTING: StopOrderStatus.WAITING,
    Status.NOTTRADED: StopOrderStatus.WAITING,
    Status.PARTTRADED: StopOrderStatus.TRIGGERED,
    Status.ALLTRADED: StopOrderStatus.TRIGGERED,
    Status.CANCELLED: StopOrderStatus.CANCELLED,
    Status.REJECTED: StopOrderStatus.CANCELLED
}


class CtaEngine(BaseEngine):
    """
    策略引擎【股票版】
    """

    engine_type = EngineType.LIVE  # live trading engine

    # 策略配置文件
    setting_filename = "cta_stock_setting.json"
    # 引擎配置文件
    engine_filename = "cta_stock_config.json"

    def __init__(self, main_engine: MainEngine, event_engine: EventEngine):
        """
        构造函数
        :param main_engine: 主引擎
        :param event_engine: 事件引擎
        """
        super().__init__(main_engine, event_engine, APP_NAME)

        # 股票引擎得配置，包括
        #  "accountid" : "xxxx",  资金账号，一般用于推送消息时附带
        #  "strategy_group": "cta_strategy_pro", # 当前实例名。多个实例时，区分开
        #  "trade_2_wx": true  # 是否交易记录转发至微信通知
        # "event_log: false    # 是否转发日志到event bus，显示在图形界面
        # "snapshot2file": false # 是否保存切片到文件
        self.engine_config = {}
        # 是否激活 write_log写入event bus(比较耗资源）
        self.event_log = False
        self.strategy_setting = {}  # strategy_name: dict
        self.strategy_data = {}  # strategy_name: dict

        self.classes = {}  # class_name: stategy_class
        self.class_module_map = {}  # class_name: mudule_name
        self.strategies = {}  # strategy_name: strategy

        # Strategy pos dict,key:strategy instance name, value: pos dict
        self.strategy_pos_dict = {}
        self.strategy_loggers = {}  # strategy_name: logger

        # 未能订阅的symbols,支持策略启动时，并未接入gateway
        # gateway_name.vt_symbol: set() of (strategy_name, is_bar)
        self.pending_subcribe_symbol_map = defaultdict(set)

        self.symbol_strategy_map = defaultdict(list)  # vt_symbol: strategy list
        self.bar_strategy_map = defaultdict(list)  # vt_symbol: strategy list
        self.strategy_symbol_map = defaultdict(set)  # strategy_name: vt_symbol set

        self.orderid_strategy_map = {}  # vt_orderid: strategy
        self.strategy_orderid_map = defaultdict(
            set)  # strategy_name: orderid list

        self.stop_order_count = 0  # for generating stop_orderid
        self.stop_orders = {}  # stop_orderid: stop_order

        self.thread_executor = ThreadPoolExecutor(max_workers=1)  # 异步线程任务执行
        self.thread_tasks = []

        self.vt_tradeids = set()  # for filtering duplicate trade

        self.positions = {}

        self.last_minute = datetime.now().strftime('%H%M')

        self.health_status = {}

        self.symbol_bar_dict = {}  # vt_symbol: bar(一分钟bar)

    def init_engine(self):
        """
        """
        self.register_event()
        self.register_funcs()

        self.load_strategy_class()
        self.load_strategy_setting()

        self.write_log("CTA策略股票引擎初始化成功")

    def close(self):
        """停止所属有的策略"""
        self.stop_all_strategies()

    def register_event(self):
        """注册事件"""
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)
        self.event_engine.register(EVENT_TICK, self.process_tick_event)
        self.event_engine.register(EVENT_BAR, self.process_bar_event)
        self.event_engine.register(EVENT_ORDER, self.process_order_event)
        self.event_engine.register(EVENT_TRADE, self.process_trade_event)
        self.event_engine.register(EVENT_POSITION, self.process_position_event)

    def register_funcs(self):
        """
        register the funcs to main_engine
        :return:
        """
        self.main_engine.get_name = self.get_name
        self.main_engine.get_strategy_status = self.get_strategy_status
        self.main_engine.get_strategy_pos = self.get_strategy_pos
        self.main_engine.compare_pos = self.compare_pos
        self.main_engine.add_strategy = self.add_strategy
        self.main_engine.init_strategy = self.init_strategy
        self.main_engine.start_strategy = self.start_strategy
        self.main_engine.stop_strategy = self.stop_strategy
        self.main_engine.remove_strategy = self.remove_strategy
        self.main_engine.reload_strategy = self.reload_strategy
        self.main_engine.save_strategy_data = self.save_strategy_data
        self.main_engine.save_strategy_snapshot = self.save_strategy_snapshot
        self.main_engine.clean_strategy_cache = self.clean_strategy_cache

        # 注册到远程服务调用
        if self.main_engine.rpc_service:
            self.main_engine.rpc_service.register(self.main_engine.get_strategy_status)
            self.main_engine.rpc_service.register(self.main_engine.get_strategy_pos)
            self.main_engine.rpc_service.register(self.main_engine.compare_pos)
            self.main_engine.rpc_service.register(self.main_engine.add_strategy)
            self.main_engine.rpc_service.register(self.main_engine.init_strategy)
            self.main_engine.rpc_service.register(self.main_engine.start_strategy)
            self.main_engine.rpc_service.register(self.main_engine.stop_strategy)
            self.main_engine.rpc_service.register(self.main_engine.remove_strategy)
            self.main_engine.rpc_service.register(self.main_engine.reload_strategy)
            self.main_engine.rpc_service.register(self.main_engine.save_strategy_data)
            self.main_engine.rpc_service.register(self.main_engine.save_strategy_snapshot)
            self.main_engine.rpc_service.register(self.main_engine.clean_strategy_cache)

    def process_timer_event(self, event: Event):
        """ 处理定时器事件"""

        dt = datetime.now()
        # 每分钟执行的逻辑
        if self.last_minute != dt.strftime('%H%M'):
            self.last_minute = dt.strftime('%H%M')

            # 检测策略的交易情况
            self.health_check()

            # 在国内股市开盘期间做检查
            if '0930' < self.last_minute < '1530':
                # 主动获取所有策略得持仓信息
                all_strategy_pos = self.get_all_strategy_pos()

                # 每5分钟检查一次
                if dt.minute % 10 == 0:
                    # 比对仓位，使用上述获取得持仓信息，不用重复获取
                    self.compare_pos(strategy_pos_list=copy(all_strategy_pos))

                # 推送到事件
                self.put_all_strategy_pos_event(all_strategy_pos)

        for strategy_name in list(self.strategies.keys()):
            strategy = self.strategies.get(strategy_name, None)
            if strategy and strategy.inited:
                self.call_strategy_func(strategy, strategy.on_timer)


    def process_tick_event(self, event: Event):
        """处理tick到达事件"""
        tick = event.data

        key = f'{tick.gateway_name}.{tick.vt_symbol}'
        v = self.pending_subcribe_symbol_map.pop(key, None)
        if v:
            # 这里不做tick/bar的判断了，因为基本有tick就有bar
            self.write_log(f'{key} tick已经到达,移除未订阅记录:{v}')

        strategies = self.symbol_strategy_map[tick.vt_symbol]
        if not strategies:
            return

        self.check_stop_order(tick)

        for strategy in strategies:
            if strategy.inited:
                self.call_strategy_func(strategy, strategy.on_tick, {tick.vt_symbol:tick})

    def process_bar_event(self, event: Event):
        """处理bar到达事件"""
        bar = event.data
        # 更新bar
        self.symbol_bar_dict[bar.vt_symbol] = bar
        # 寻找订阅了该bar的策略
        strategies = self.symbol_strategy_map[bar.vt_symbol]
        if not strategies:
            return
        for strategy in strategies:
            if strategy.inited:
                self.call_strategy_func(strategy, strategy.on_bar, {bar.vt_symbol: bar})

    def process_order_event(self, event: Event):
        """"""
        order = event.data

        strategy = self.orderid_strategy_map.get(order.vt_orderid, None)
        if not strategy:
            self.write_log(f'委托单没有对应的策略设置:order:{order.__dict__}')
            self.write_log(f'当前策略侦听委托单:{list(self.orderid_strategy_map.keys())}')
            return
        self.write_log(f'委托更新:{order.vt_orderid} => 策略:{strategy.strategy_name}')
        # Remove vt_orderid if order is no longer active.
        vt_orderids = self.strategy_orderid_map[strategy.strategy_name]
        if order.vt_orderid in vt_orderids and not order.is_active():
            vt_orderids.remove(order.vt_orderid)

        # For server stop order, call strategy on_stop_order function
        if order.type == OrderType.STOP:
            so = StopOrder(
                vt_symbol=order.vt_symbol,
                direction=order.direction,
                offset=order.offset,
                price=order.price,
                volume=order.volume,
                stop_orderid=order.vt_orderid,
                strategy_name=strategy.strategy_name,
                status=STOP_STATUS_MAP[order.status],
                vt_orderids=[order.vt_orderid],
            )
            self.call_strategy_func(strategy, strategy.on_stop_order, so)

        # Call strategy on_order function
        self.call_strategy_func(strategy, strategy.on_order, order)

    def process_trade_event(self, event: Event):
        """"""
        trade = event.data

        # Filter duplicate trade push
        if trade.vt_tradeid in self.vt_tradeids:
            self.write_log(f'成交单的交易编号{trade.vt_tradeid}已处理完毕,不再处理')
            return
        self.vt_tradeids.add(trade.vt_tradeid)

        strategy = self.orderid_strategy_map.get(trade.vt_orderid, None)
        if not strategy:
            self.write_log(f'成交单没有对应的策略设置:trade:{trade.__dict__}')
            self.write_log(f'当前策略侦听委托单:{list(self.orderid_strategy_map.keys())}')
            return

        self.write_log(f'成交更新:{trade.vt_orderid} => 策略:{strategy.strategy_name}')

        # Update strategy pos before calling on_trade method
        # 取消外部干预策略pos，由策略自行完成更新
        # if trade.direction == Direction.LONG:
        #     strategy.pos += trade.volume
        # else:
        #     strategy.pos -= trade.volume
        # 根据策略名称，写入 data\straetgy_name_trade.csv文件
        strategy_name = getattr(strategy, 'strategy_name')
        trade_fields = ['datetime', 'symbol', 'exchange', 'vt_symbol', 'tradeid', 'vt_tradeid', 'orderid', 'vt_orderid',
                        'direction', 'offset', 'price', 'volume', 'idx_price']
        trade_dict = OrderedDict()
        try:
            for k in trade_fields:
                if k == 'datetime':
                    dt = getattr(trade, 'datetime')
                    if isinstance(dt, datetime):
                        trade_dict[k] = dt.strftime('%Y-%m-%d %H:%M:%S')
                    else:
                        trade_dict[k] = datetime.now().strftime('%Y-%m-%d') + ' ' + getattr(trade, 'time', '')
                if k in ['exchange', 'direction', 'offset']:
                    trade_dict[k] = getattr(trade, k).value
                else:
                    trade_dict[k] = getattr(trade, k, '')

            # 添加指数价格
            symbol = trade_dict.get('symbol')
            idx_symbol = get_underlying_symbol(symbol).upper() + '99.' + trade_dict.get('exchange')
            idx_price = self.get_price(idx_symbol)
            if idx_price:
                trade_dict.update({'idx_price': idx_price})
            else:
                trade_dict.update({'idx_price': trade_dict.get('price')})

            if strategy_name is not None:
                trade_file = str(get_folder_path('data').joinpath('{}_trade.csv'.format(strategy_name)))
                append_data(file_name=trade_file, dict_data=trade_dict)
        except Exception as ex:
            self.write_error(u'写入交易记录csv出错：{},{}'.format(str(ex), traceback.format_exc()))

        self.call_strategy_func(strategy, strategy.on_trade, trade)

        # Sync strategy variables to data file
        # 取消此功能，由策略自身完成数据持久化
        # self.sync_strategy_data(strategy)

        # Update GUI
        self.put_strategy_event(strategy)

        if self.engine_config.get('trade_2_wx', False):
            accountid = self.engine_config.get('accountid', 'XXX')
            d = {
                'account': accountid,
                'strategy': strategy_name,
                'symbol': trade.symbol,
                'action': f'{trade.direction.value} {trade.offset.value}',
                'price': str(trade.price),
                'volume': trade.volume,
                'remark': f'{accountid}:{strategy_name}',
                'timestamp': trade.time
            }
            send_wx_msg(content=d, target=accountid, msg_type='TRADE')

    def process_position_event(self, event: Event):
        """"""
        position = event.data

        self.positions.update({position.vt_positionid: position})

    def check_unsubscribed_symbols(self):
        """检查未订阅合约"""

        for key in self.pending_subcribe_symbol_map.keys():
            # gateway_name.symbol.exchange = > gateway_name, vt_symbol
            keys = key.split('.')
            gateway_name = keys[0]
            vt_symbol = '.'.join(keys[1:])

            contract = self.main_engine.get_contract(vt_symbol)
            is_bar = True if vt_symbol in self.bar_strategy_map else False
            if contract:
                dt = datetime.now()

                self.write_log(f'重新提交合约{vt_symbol}订阅请求')
                for strategy_name, is_bar in list(self.pending_subcribe_symbol_map[vt_symbol]):
                    self.subscribe_symbol(strategy_name=strategy_name,
                                          vt_symbol=vt_symbol,
                                          gateway_name=gateway_name,
                                          is_bar=is_bar)
            else:
                try:
                    self.write_log(f'找不到合约{vt_symbol}信息，尝试请求所有接口')
                    symbol, exchange = extract_vt_symbol(vt_symbol)
                    req = SubscribeRequest(symbol=symbol, exchange=exchange)
                    req.is_bar = is_bar
                    self.main_engine.subscribe(req, gateway_name)

                except Exception as ex:
                    self.write_error(
                        u'重新订阅{}.{}异常:{},{}'.format(gateway_name, vt_symbol, str(ex), traceback.format_exc()))
                    return

    def check_stop_order(self, tick: TickData):
        """"""
        for stop_order in list(self.stop_orders.values()):
            if stop_order.vt_symbol != tick.vt_symbol:
                continue

            long_triggered = stop_order.direction == Direction.LONG and tick.last_price >= stop_order.price
            short_triggered = stop_order.direction == Direction.SHORT and tick.last_price <= stop_order.price

            if long_triggered or short_triggered:
                strategy = self.strategies[stop_order.strategy_name]

                # To get excuted immediately after stop order is
                # triggered, use limit price if available, otherwise
                # use ask_price_5 or bid_price_5
                if stop_order.direction == Direction.LONG:
                    if tick.limit_up:
                        price = tick.limit_up
                    else:
                        price = tick.ask_price_5
                else:
                    if tick.limit_down:
                        price = tick.limit_down
                    else:
                        price = tick.bid_price_5

                contract = self.main_engine.get_contract(stop_order.vt_symbol)

                vt_orderids = self.send_limit_order(
                    strategy=strategy,
                    contract=contract,
                    direction=stop_order.direction,
                    offset=stop_order.offset,
                    price=price,
                    volume=stop_order.volume
                )

                # Update stop order status if placed successfully
                if vt_orderids:
                    # Remove from relation map.
                    self.stop_orders.pop(stop_order.stop_orderid)

                    strategy_vt_orderids = self.strategy_orderid_map[strategy.strategy_name]
                    if stop_order.stop_orderid in strategy_vt_orderids:
                        strategy_vt_orderids.remove(stop_order.stop_orderid)

                    # Change stop order status to cancelled and update to strategy.
                    stop_order.status = StopOrderStatus.TRIGGERED
                    stop_order.vt_orderids = vt_orderids

                    self.call_strategy_func(
                        strategy, strategy.on_stop_order, stop_order
                    )
                    self.put_stop_order_event(stop_order)

    def send_server_order(
            self,
            strategy: CtaTemplate,
            contract: ContractData,
            direction: Direction,
            offset: Offset,
            price: float,
            volume: float,
            type: OrderType,
            gateway_name: str = None
    ):
        """
        Send a new order to server.
        """
        # Create request and send order.
        req = OrderRequest(
            symbol=contract.symbol,
            exchange=contract.exchange,
            direction=direction,
            offset=offset,
            type=type,
            price=price,
            volume=volume,
            strategy_name=strategy.strategy_name
        )

        # 如果没有指定网关，则使用合约信息内的网关
        if contract.gateway_name and not gateway_name:
            gateway_name = contract.gateway_name

        # Send Orders
        vt_orderids = []

        vt_orderid = self.main_engine.send_order(
            req, gateway_name)

        vt_orderids.append(vt_orderid)

        # Save relationship between orderid and strategy.
        self.write_log(f'委托成功绑定{vt_orderid} <==> {strategy.strategy_name}')
        self.orderid_strategy_map[vt_orderid] = strategy
        self.strategy_orderid_map[strategy.strategy_name].add(vt_orderid)

        return vt_orderids

    def send_limit_order(
            self,
            strategy: CtaTemplate,
            contract: ContractData,
            direction: Direction,
            offset: Offset,
            price: float,
            volume: float,
            gateway_name: str = None
    ):
        """
        Send a limit order to server.
        """
        return self.send_server_order(
            strategy=strategy,
            contract=contract,
            direction=direction,
            offset=offset,
            price=price,
            volume=volume,
            type=OrderType.LIMIT,
            gateway_name=gateway_name
        )

    def send_fak_order(
            self,
            strategy: CtaTemplate,
            contract: ContractData,
            direction: Direction,
            offset: Offset,
            price: float,
            volume: float,
            gateway_name: str = None
    ):
        """
        Send a limit order to server.
        """
        return self.send_server_order(
            strategy=strategy,
            contract=contract,
            direction=direction,
            offset=offset,
            price=price,
            volume=volume,
            type=OrderType.FAK,
            gateway_name=gateway_name
        )

    def send_server_stop_order(
            self,
            strategy: CtaTemplate,
            contract: ContractData,
            direction: Direction,
            offset: Offset,
            price: float,
            volume: float,
            gateway_name: str = None
    ):
        """
        Send a stop order to server.

        Should only be used if stop order supported
        on the trading server.
        """
        return self.send_server_order(
            strategy=strategy,
            contract=contract,
            direction=direction,
            offset=offset,
            price=price,
            volume=volume,
            type=OrderType.STOP,
            gateway_name=gateway_name
        )

    def send_local_stop_order(
            self,
            strategy: CtaTemplate,
            vt_symbol: str,
            direction: Direction,
            offset: Offset,
            price: float,
            volume: float,
            gateway_name: str = None
    ):
        """
        Create a new local stop order.
        """
        self.stop_order_count += 1
        stop_orderid = f"{STOPORDER_PREFIX}.{self.stop_order_count}"

        stop_order = StopOrder(
            vt_symbol=vt_symbol,
            direction=direction,
            offset=offset,
            price=price,
            volume=volume,
            stop_orderid=stop_orderid,
            strategy_name=strategy.strategy_name,
            gateway_name=gateway_name
        )

        self.stop_orders[stop_orderid] = stop_order

        vt_orderids = self.strategy_orderid_map[strategy.strategy_name]
        vt_orderids.add(stop_orderid)

        self.call_strategy_func(strategy, strategy.on_stop_order, stop_order)
        self.put_stop_order_event(stop_order)

        return [stop_orderid]

    def cancel_server_order(self, strategy: CtaTemplate, vt_orderid: str):
        """
        Cancel existing order by vt_orderid.
        """
        order = self.main_engine.get_order(vt_orderid)
        if not order:
            self.write_log(msg=f"撤单失败，找不到委托{vt_orderid}",
                           strategy_name=strategy.strategy_name,
                           level=logging.ERROR)
            return False

        req = order.create_cancel_request()
        return self.main_engine.cancel_order(req, order.gateway_name)

    def cancel_local_stop_order(self, strategy: CtaTemplate, stop_orderid: str):
        """
        Cancel a local stop order.
        """
        stop_order = self.stop_orders.get(stop_orderid, None)
        if not stop_order:
            return False
        strategy = self.strategies[stop_order.strategy_name]

        # Remove from relation map.
        self.stop_orders.pop(stop_orderid)

        vt_orderids = self.strategy_orderid_map[strategy.strategy_name]
        if stop_orderid in vt_orderids:
            vt_orderids.remove(stop_orderid)

        # Change stop order status to cancelled and update to strategy.
        stop_order.status = StopOrderStatus.CANCELLED

        self.call_strategy_func(strategy, strategy.on_stop_order, stop_order)
        self.put_stop_order_event(stop_order)
        return True

    def send_order(
            self,
            strategy: CtaTemplate,
            vt_symbol: str,
            direction: Direction,
            offset: Offset,
            price: float,
            volume: float,
            stop: bool,
            order_type: OrderType = OrderType.LIMIT,
            gateway_name: str = None
    ):
        """
        该方法供策略使用，发送委托。
        """
        contract = self.main_engine.get_contract(vt_symbol)
        if not contract:
            self.write_log(msg=f"委托失败，找不到合约：{vt_symbol}",
                           strategy_name=strategy.strategy_name,
                           level=logging.ERROR)
            return ""
        if contract.gateway_name and not gateway_name:
            gateway_name = contract.gateway_name
        # Round order price and volume to nearest incremental value
        price = round_to(price, contract.pricetick)
        volume = round_to(volume, contract.min_volume)

        if stop:
            if contract.stop_supported:
                # 发送服务器停止单
                return self.send_server_stop_order(
                    strategy=strategy,
                    contract=contract,
                    direction=direction,
                    offset=offset,
                    price=price,
                    volume=volume,
                    gateway_name=gateway_name)
            else:
                # 创建本地停止单
                return self.send_local_stop_order(
                    strategy=strategy,
                    vt_symbol=vt_symbol,
                    direction=direction,
                    offset=offset,
                    price=price,
                    volume=volume,
                    gateway_name=gateway_name)
        if order_type == OrderType.FAK:
            return self.send_fak_order(
                strategy=strategy,
                contract=contract,
                direction=direction,
                offset=offset,
                price=price,
                volume=volume,
                gateway_name=gateway_name)
        else:
            return self.send_limit_order(
                strategy=strategy,
                contract=contract,
                direction=direction,
                offset=offset,
                price=price,
                volume=volume,
                gateway_name=gateway_name)

    def cancel_order(self, strategy: CtaTemplate, vt_orderid: str):
        """
        """
        if vt_orderid is None or len(vt_orderid) == 0:
            return False

        if vt_orderid.startswith(STOPORDER_PREFIX):
            return self.cancel_local_stop_order(strategy, vt_orderid)
        else:
            return self.cancel_server_order(strategy, vt_orderid)

    def cancel_all(self, strategy: CtaTemplate):
        """
        Cancel all active orders of a strategy.
        """
        vt_orderids = self.strategy_orderid_map[strategy.strategy_name]
        if not vt_orderids:
            return

        for vt_orderid in copy(vt_orderids):
            self.cancel_order(strategy, vt_orderid)

    def subscribe_symbol(self, strategy_name: str, vt_symbol: str, gateway_name: str = '', is_bar: bool = False):
        """订阅合约"""
        strategy = self.strategies.get(strategy_name, None)
        if not strategy:
            return False

        contract = self.main_engine.get_contract(vt_symbol)
        if contract:
            if contract.gateway_name and not gateway_name:
                gateway_name = contract.gateway_name
            req = SubscribeRequest(
                symbol=contract.symbol, exchange=contract.exchange)
            self.main_engine.subscribe(req, gateway_name)
        else:
            self.write_log(msg=f"找不到合约{vt_symbol},添加到待订阅列表",
                           strategy_name=strategy.strategy_name)
            self.pending_subcribe_symbol_map[f'{gateway_name}.{vt_symbol}'].add((strategy_name, is_bar))
            try:
                self.write_log(f'找不到合约{vt_symbol}信息，尝试请求所有接口')
                symbol, exchange = extract_vt_symbol(vt_symbol)
                req = SubscribeRequest(symbol=symbol, exchange=exchange)
                req.is_bar = is_bar
                self.main_engine.subscribe(req, gateway_name)

            except Exception as ex:
                self.write_error(u'重新订阅{}异常:{},{}'.format(vt_symbol, str(ex), traceback.format_exc()))

        # 如果是订阅bar
        if is_bar:
            strategies = self.bar_strategy_map[vt_symbol]
            if strategy not in strategies:
                strategies.append(strategy)
                self.bar_strategy_map.update({vt_symbol: strategies})
        else:
            # 添加 合约订阅 vt_symbol <=> 策略实例 strategy 映射.
            strategies = self.symbol_strategy_map[vt_symbol]
            if strategy not in strategies:
                strategies.append(strategy)

        # 添加 策略名 strategy_name  <=> 合约订阅 vt_symbol 的映射
        subscribe_symbol_set = self.strategy_symbol_map[strategy.strategy_name]
        subscribe_symbol_set.add(vt_symbol)

        return True

    @lru_cache()
    def get_name(self, vt_symbol: str):
        """查询合约的name"""
        contract = self.main_engine.get_contract(vt_symbol)
        if contract is None:
            self.write_error(f'查询不到{vt_symbol}合约信息')
            return vt_symbol
        return contract.name

    @lru_cache()
    def get_size(self, vt_symbol: str):
        """查询合约的size"""
        contract = self.main_engine.get_contract(vt_symbol)
        if contract is None:
            self.write_error(f'查询不到{vt_symbol}合约信息')
            return 10
        return contract.size

    @lru_cache()
    def get_margin_rate(self, vt_symbol: str):
        """查询保证金比率"""
        contract = self.main_engine.get_contract(vt_symbol)
        if contract is None:
            self.write_error(f'查询不到{vt_symbol}合约信息')
            return 0.1
        if contract.margin_rate == 0:
            return 0.1
        return contract.margin_rate

    @lru_cache()
    def get_price_tick(self, vt_symbol: str):
        """查询价格最小跳动"""
        contract = self.main_engine.get_contract(vt_symbol)
        if contract is None:
            self.write_error(f'查询不到{vt_symbol}合约信息')
            return 0.01

        return contract.pricetick

    @lru_cache()
    def get_volume_tick(self, vt_symbol: str):
        """查询合约最小成交单位"""
        contract = self.main_engine.get_contract(vt_symbol)
        if contract is None:
            self.write_error(f'查询不到{vt_symbol}合约信息')
            return 100

        return contract.min_volume

    def get_tick(self, vt_symbol: str):
        """获取合约得最新tick"""
        return self.main_engine.get_tick(vt_symbol)

    def get_price(self, vt_symbol: str):
        """查询合约的最新价格"""
        tick = self.main_engine.get_tick(vt_symbol)
        if tick:
            return tick.last_price

        bar = self.symbol_bar_dict.get(vt_symbol)
        if bar:
            return bar.close_price

        return None

    def get_contract(self, vt_symbol):
        return self.main_engine.get_contract(vt_symbol)

    def get_account(self, vt_accountid: str = ""):
        """ 查询账号的资金"""
        # 如果启动风控，则使用风控中的最大仓位
        if self.main_engine.rm_engine and len(vt_accountid) > 0:
            return self.main_engine.rm_engine.get_account(vt_accountid)

        if len(vt_accountid) > 0:
            account = self.main_engine.get_account(vt_accountid)
            return account.balance, account.available, round(account.frozen * 100 / (account.balance + 0.01), 2), 100
        else:
            accounts = self.main_engine.get_all_accounts()
            if len(accounts) > 0:
                account = accounts[0]
                return account.balance, account.available, round(account.frozen * 100 / (account.balance + 0.01),
                                                                 2), 100
            else:
                return 0, 0, 0, 0

    def get_position(self, vt_symbol: str, direction: Direction, gateway_name: str = '') -> PositionData:
        """ 查询合约在账号的持仓,需要指定方向"""
        contract = self.main_engine.get_contract(vt_symbol)
        if contract:
            if contract.gateway_name and not gateway_name:
                gateway_name = contract.gateway_name

        vt_position_id = f"{gateway_name}.{vt_symbol}.{direction.value}"
        return self.main_engine.get_position(vt_position_id)

    def get_engine_type(self):
        """"""
        return self.engine_type

    @lru_cache()
    def get_data_path(self):
        data_path = os.path.abspath(os.path.join(TRADER_DIR, 'data'))
        return data_path

    @lru_cache()
    def get_logs_path(self):
        log_path = os.path.abspath(os.path.join(TRADER_DIR, 'log'))
        return log_path

    def load_bar(
            self,
            vt_symbol: str,
            days: int,
            interval: Interval,
            callback: Callable[[BarData], None]
    ):
        """"""
        symbol, exchange = extract_vt_symbol(vt_symbol)
        end = datetime.now()
        start = end - timedelta(days)
        bars = []

        # Query bars from gateway if available
        contract = self.main_engine.get_contract(vt_symbol)

        if contract and contract.history_data:
            req = HistoryRequest(
                symbol=symbol,
                exchange=exchange,
                interval=interval,
                start=start,
                end=end
            )
            bars = self.main_engine.query_history(req, contract.gateway_name)

        for bar in bars:
            if bar.trading_day:
                bar.trading_day = bar.datetime.strftime('%Y-%m-%d')

            callback(bar)

    def call_strategy_func(
            self, strategy: CtaTemplate, func: Callable, params: Any = None
    ):
        """
        Call function of a strategy and catch any exception raised.
        """
        try:
            if params:
                func(params)
            else:
                func()
        except Exception:
            strategy.trading = False
            strategy.inited = False

            msg = f"触发异常已停止\n{traceback.format_exc()}"
            self.write_log(msg=msg,
                           strategy_name=strategy.strategy_name,
                           level=logging.CRITICAL)

    def add_strategy(
            self, class_name: str,
            strategy_name: str,
            vt_symbols: List[str],
            setting: dict,
            auto_init: bool = False,
            auto_start: bool = False
    ):
        """
        Add a new strategy.
        """
        if strategy_name in self.strategies:
            msg = f"创建策略失败，存在重名{strategy_name}"
            self.write_log(msg=msg,
                           level=logging.CRITICAL)
            return False, msg

        strategy_class = self.classes.get(class_name, None)
        if not strategy_class:
            msg = f"创建策略失败，找不到策略类{class_name}"
            self.write_log(msg=msg,
                           level=logging.CRITICAL)
            return False, msg

        self.write_log(f'开始添加策略类{class_name}，实例名:{strategy_name}')
        strategy = strategy_class(self, strategy_name, vt_symbols, setting)
        self.strategies[strategy_name] = strategy

        # Add vt_symbol to strategy map.
        subscribe_symbol_set = self.strategy_symbol_map[strategy_name]
        for vt_symbol in vt_symbols:
            strategies = self.symbol_strategy_map[vt_symbol]
            strategies.append(strategy)
            subscribe_symbol_set.add(vt_symbol)

        # Update to setting file.
        self.update_strategy_setting(strategy_name, setting, auto_init, auto_start)

        self.put_strategy_event(strategy)

        # 判断设置中是否由自动初始化和自动启动项目
        if auto_init:
            self.init_strategy(strategy_name, auto_start=auto_start)

        return True, f'成功添加{strategy_name}'

    def init_strategy(self, strategy_name: str, auto_start: bool = False):
        """
        Init a strategy.
        """
        task = self.thread_executor.submit(self._init_strategy, strategy_name, auto_start)
        self.thread_tasks.append(task)

    def _init_strategy(self, strategy_name: str, auto_start: bool = False):
        """
        Init strategies in queue.
        """
        strategy = self.strategies[strategy_name]

        if strategy.inited:
            self.write_error(f"{strategy_name}已经完成初始化，禁止重复操作")
            return

        self.write_log(f"{strategy_name}开始执行初始化")

        # Call on_init function of strategy
        self.call_strategy_func(strategy, strategy.on_init)

        # Restore strategy data(variables)
        # Pro 版本不使用自动恢复除了内部数据功能，由策略自身初始化时完成
        # data = self.strategy_data.get(strategy_name, None)
        # if data:
        #     for name in strategy.variables:
        #         value = data.get(name, None)
        #         if value:
        #             setattr(strategy, name, value)

        # Subscribe market data 订阅缺省的vt_symbol, 如果有其他合约需要订阅，由策略内部初始化时提交订阅即可。
        for vt_symbol in strategy.vt_symbols:
            self.subscribe_symbol(strategy_name=strategy_name, vt_symbol=vt_symbol)

        # Put event to update init completed status.
        strategy.inited = True
        self.put_strategy_event(strategy)
        self.write_log(f"{strategy_name}初始化完成")

        # 初始化后，自动启动策略交易
        if auto_start:
            self.start_strategy(strategy_name)

    def start_strategy(self, strategy_name: str):
        """
        Start a strategy.
        """
        strategy = self.strategies[strategy_name]
        if not strategy.inited:
            msg = f"策略{strategy.strategy_name}启动失败，请先初始化"
            self.write_error(msg)
            return False, msg

        if strategy.trading:
            msg = f"{strategy_name}已经启动，请勿重复操作"
            self.write_error(msg)
            return False, msg

        self.call_strategy_func(strategy, strategy.on_start)
        strategy.trading = True

        self.put_strategy_event(strategy)

        return True, f'成功启动策略{strategy_name}'

    def stop_strategy(self, strategy_name: str):
        """
        Stop a strategy.
        """
        strategy = self.strategies[strategy_name]
        if not strategy.trading:
            msg = f'{strategy_name}策略实例已处于停止交易状态'
            self.write_log(msg)
            return False, msg

        # Call on_stop function of the strategy
        self.write_log(f'调用{strategy_name}的on_stop,停止交易')
        self.call_strategy_func(strategy, strategy.on_stop)

        # Change trading status of strategy to False
        strategy.trading = False

        # Cancel all orders of the strategy
        self.write_log(f'撤销{strategy_name}所有委托')
        self.cancel_all(strategy)

        # Sync strategy variables to data file
        #  取消此功能，由策略自身完成数据的持久化
        # self.sync_strategy_data(strategy)

        # Update GUI
        self.put_strategy_event(strategy)
        return True, f'成功停止策略{strategy_name}'

    def edit_strategy(self, strategy_name: str, setting: dict):
        """
        Edit parameters of a strategy.
        风险警示： 该方法强行干预策略的配置
        """
        strategy = self.strategies[strategy_name]
        auto_init = setting.pop('auto_init', False)
        auto_start = setting.pop('auto_start', False)

        strategy.update_setting(setting)

        self.update_strategy_setting(strategy_name, setting, auto_init, auto_start)
        self.put_strategy_event(strategy)

    def remove_strategy(self, strategy_name: str):
        """
        Remove a strategy.
        """
        strategy = self.strategies[strategy_name]
        if strategy.trading:
            # err_msg = f"策略{strategy.strategy_name}正在运行，先停止"
            #self.write_error(err_msg)
            #return False, err_msg
            ret , msg = self.stop_strategy(strategy_name)
            if not ret:
                return False, msg
            else:
                self.write_log(msg)

        # Remove setting
        self.remove_strategy_setting(strategy_name)

        # 移除订阅合约与策略的关联关系
        for vt_symbol in self.strategy_symbol_map[strategy_name]:
            # Remove from symbol strategy map
            self.write_log(f'移除{vt_symbol}《=》{strategy_name}的订阅关系')
            strategies = self.symbol_strategy_map[vt_symbol]
            strategies.remove(strategy)

        # Remove from active orderid map
        if strategy_name in self.strategy_orderid_map:
            vt_orderids = self.strategy_orderid_map.pop(strategy_name)
            self.write_log(f'移除{strategy_name}的所有委托订单映射关系')
            # Remove vt_orderid strategy map
            for vt_orderid in vt_orderids:
                if vt_orderid in self.orderid_strategy_map:
                    self.orderid_strategy_map.pop(vt_orderid)

        # Remove from strategies
        self.write_log(f'移除{strategy_name}策略实例')
        self.strategies.pop(strategy_name)

        return True, f'成功移除{strategy_name}策略实例'

    def reload_strategy(self, strategy_name: str, vt_symbols: List[str] = [], setting: dict = {}):
        """
        重新加载策略
        一般使用于在线更新策略代码，或者更新策略参数，需要重新启动策略
        :param strategy_name:
        :param setting:
        :return:
        """
        self.write_log(f'开始重新加载策略{strategy_name}')

        # 优先判断重启的策略，是否已经加载
        if strategy_name not in self.strategies or strategy_name not in self.strategy_setting:
            err_msg = f"{strategy_name}不在运行策略中，不能重启"
            self.write_error(err_msg)
            return False, err_msg

        # 从本地配置文件中读取
        if len(setting) == 0:
            strategies_setting = load_json(self.setting_filename)
            old_strategy_config = strategies_setting.get(strategy_name, {})
        else:
            old_strategy_config = copy(self.strategy_setting[strategy_name])

        class_name = old_strategy_config.get('class_name')
        if len(vt_symbols) == 0:
            vt_symbols = old_strategy_config.get('vt_symbols')
        if len(setting) == 0:
            setting = old_strategy_config.get('setting')

        module_name = self.class_module_map[class_name]
        # 重新load class module
        #if not self.load_strategy_class_from_module(module_name):
        #    err_msg = f'不能加载模块:{module_name}'
        #    self.write_error(err_msg)
        #    return False, err_msg
        if module_name:
            new_class_name = module_name + '.' + class_name
            self.write_log(u'转换策略为全路径:{}'.format(new_class_name))

            strategy_class = import_module_by_str(new_class_name)
            if strategy_class is None:
                err_msg = u'加载策略模块失败:{}'.format(class_name)
                self.write_error(err_msg)
                return False, err_msg

            self.write_log(f'重新加载模块成功，使用新模块:{new_class_name}')
            self.classes[class_name] = strategy_class

        # 停止当前策略实例的运行，撤单
        self.stop_strategy(strategy_name)

        # 移除运行中的策略实例
        self.remove_strategy(strategy_name)

        # 重新添加策略
        self.add_strategy(class_name=class_name,
                          strategy_name=strategy_name,
                          vt_symbols=vt_symbols,
                          setting=setting,
                          auto_init=old_strategy_config.get('auto_init', False),
                          auto_start=old_strategy_config.get('auto_start', False))

        msg = f'成功重载策略{strategy_name}'
        self.write_log(msg)
        return True, msg

    def save_strategy_data(self, select_name: str = 'ALL'):
        """ save strategy data"""
        has_executed = False
        msg = ""
        # 1.判断策略名称是否存在字典中
        for strategy_name in list(self.strategies.keys()):
            if select_name != 'ALL':
                if strategy_name != select_name:
                    continue
            # 2.提取策略
            strategy = self.strategies.get(strategy_name, None)
            if not strategy:
                continue

            # 3.判断策略是否运行
            if strategy.inited and strategy.trading:
                task = self.thread_executor.submit(self.thread_save_strategy_data, strategy_name)
                self.thread_tasks.append(task)
                msg += f'{strategy_name}执行保存数据\n'
                has_executed = True
            else:
                self.write_log(f'{strategy_name}未初始化/未启动交易，不进行保存数据')
        return has_executed, msg

    def thread_save_strategy_data(self, strategy_name):
        """异步线程保存策略数据"""
        strategy = self.strategies.get(strategy_name, None)
        if strategy is None:
            return
        try:
            # 保存策略数据
            strategy.sync_data()
        except Exception as ex:
            self.write_error(u'保存策略{}数据异常:'.format(strategy_name, str(ex)))
            self.write_error(traceback.format_exc())

    def clean_strategy_cache(self, strategy_name):
        """清除策略K线缓存文件"""
        cache_file = os.path.abspath(os.path.join(self.get_data_path(), f'{strategy_name}_klines.pkb2'))
        if os.path.exists(cache_file):
            self.write_log(f'移除策略缓存文件:{cache_file}')
            os.remove(cache_file)
        else:
            self.write_log(f'策略缓存文件不存在:{cache_file}')

    def get_strategy_snapshot(self, strategy_name):
        """实时获取策略的K线切片（比较耗性能）"""
        strategy = self.strategies.get(strategy_name, None)
        if strategy is None:
            return None

        try:
            # 5.保存策略切片
            snapshot = strategy.get_klines_snapshot()
            if not snapshot:
                self.write_log(f'{strategy_name}返回得K线切片数据为空')
                return None
            return snapshot

        except Exception as ex:
            self.write_error(u'获取策略{}切片数据异常:'.format(strategy_name, str(ex)))
            self.write_error(traceback.format_exc())
            return None

    def save_strategy_snapshot(self, select_name: str = 'ALL'):
        """
        保存策略K线切片数据
        :param select_name:
        :return:
        """
        has_executed = False
        msg = ""
        # 1.判断策略名称是否存在字典中
        for strategy_name in list(self.strategies.keys()):
            if select_name != 'ALL':
                if strategy_name != select_name:
                    continue
            # 2.提取策略
            strategy = self.strategies.get(strategy_name, None)
            if not strategy:
                continue

            if not hasattr(strategy, 'get_klines_snapshot'):
                continue

            # 3.判断策略是否运行
            if strategy.inited and strategy.trading:
                task = self.thread_executor.submit(self.thread_save_strategy_snapshot, strategy_name)
                self.thread_tasks.append(task)
                msg += f'{strategy_name}执行保存K线切片\n'
                has_executed = True

        return has_executed, msg

    def thread_save_strategy_snapshot(self, strategy_name):
        """异步线程保存策略切片"""
        strategy = self.strategies.get(strategy_name, None)
        if strategy is None:
            return

        try:
            # 5.保存策略切片
            snapshot = strategy.get_klines_snapshot()
            if not snapshot:
                self.write_log(f'{strategy_name}返回得K线切片数据为空')
                return

            if self.engine_config.get('snapshot2file', False):
                # 剩下工作：保存本地文件/数据库
                snapshot_folder = get_folder_path(f'data/snapshots/{strategy_name}')
                snapshot_file = snapshot_folder.joinpath('{}.pkb2'.format(datetime.now().strftime('%Y%m%d_%H%M%S')))
                with bz2.BZ2File(str(snapshot_file), 'wb') as f:
                    pickle.dump(snapshot, f)
                    self.write_log(u'切片保存成功:{}'.format(str(snapshot_file)))

            # 通过事件方式，传导到account_recorder
            snapshot.update({
                'account_id': self.engine_config.get('accountid', '-'),
                'strategy_group':  self.engine_config.get('strategy_group', self.engine_name),
                'guid': str(uuid1())
            })
            event = Event(EVENT_STRATEGY_SNAPSHOT, snapshot)
            self.event_engine.put(event)

        except Exception as ex:
            self.write_error(u'获取策略{}切片数据异常:'.format(strategy_name, str(ex)))
            self.write_error(traceback.format_exc())

    def load_strategy_class(self):
        """
        Load strategy class from source code.
        """
        # 加载 vnpy/app/cta_strategy_pro/strategies的所有策略
        path1 = Path(__file__).parent.joinpath("strategies")
        self.load_strategy_class_from_folder(
            path1, "vnpy.app.cta_stock.strategies")

        # 加载 当前运行目录下strategies子目录的所有策略
        path2 = Path.cwd().joinpath("strategies")
        self.load_strategy_class_from_folder(path2, "strategies")

    def load_strategy_class_from_folder(self, path: Path, module_name: str = ""):
        """
        Load strategy class from certain folder.
        """
        for dirpath, dirnames, filenames in os.walk(str(path)):
            for filename in filenames:
                if filename.endswith(".py"):
                    strategy_module_name = ".".join(
                        [module_name, filename.replace(".py", "")])
                elif filename.endswith(".pyd"):
                    strategy_module_name = ".".join(
                        [module_name, filename.split(".")[0]])
                elif filename.endswith(".so"):
                    strategy_module_name = ".".join(
                        [module_name, filename.split(".")[0]])
                else:
                    continue
                self.load_strategy_class_from_module(strategy_module_name)

    def load_strategy_class_from_module(self, module_name: str):
        """
        Load/Reload strategy class from module file.
        """
        try:
            module = importlib.import_module(module_name)

            for name in dir(module):
                value = getattr(module, name)
                if (isinstance(value, type) and issubclass(value, CtaTemplate) and value is not CtaTemplate):
                    class_name = value.__name__
                    if class_name not in self.classes:
                        self.write_log(f"加载策略类{module_name}.{class_name}")
                    else:
                        self.write_log(f"更新策略类{module_name}.{class_name}")
                    self.classes[class_name] = value
                    self.class_module_map[class_name] = module_name
            return True
        except:  # noqa
            account = self.engine_config.get('accountid', '')
            msg = f"cta_stock:{account}策略文件{module_name}加载失败，触发异常：\n{traceback.format_exc()}"
            self.write_log(msg=msg, level=logging.CRITICAL)
            return False

    def load_strategy_data(self):
        """
        Load strategy data from json file.
        """
        print(f'load_strategy_data 此功能已取消，由策略自身完成数据的持久化加载', file=sys.stderr)
        return
        # self.strategy_data = load_json(self.data_filename)

    def sync_strategy_data(self, strategy: CtaTemplate):
        """
        Sync strategy data into json file.
        """
        # data = strategy.get_variables()
        # data.pop("inited")      # Strategy status (inited, trading) should not be synced.
        # data.pop("trading")
        # self.strategy_data[strategy.strategy_name] = data
        # save_json(self.data_filename, self.strategy_data)
        print(f'sync_strategy_data此功能已取消，由策略自身完成数据的持久化保存', file=sys.stderr)

    def get_all_strategy_class_names(self):
        """
        Return names of strategy classes loaded.
        """
        return list(self.classes.keys())

    def get_strategy_status(self):
        """
        return strategy inited/trading status
        :param strategy_name:
        :return:
        """
        return {k: {'inited': v.inited, 'trading': v.trading} for k, v in self.strategies.items()}

    def get_strategy_pos(self, name, strategy=None):
        """
        获取策略的持仓字典
        :param name:策略名
        :return: [ {},{}]
        """
        # 兼容处理，如果strategy是None，通过name获取
        if strategy is None:
            if name not in self.strategies:
                self.write_log(u'get_strategy_pos 策略实例不存在：' + name)
                return []
            # 获取策略实例
            strategy = self.strategies[name]

        pos_list = []

        if strategy.inited:
            # 如果策略具有getPositions得方法，则调用该方法
            if hasattr(strategy, 'get_positions'):
                pos_list = strategy.get_positions()
                for pos in pos_list:
                    vt_symbol = pos.get('vt_symbol', None)
                    if vt_symbol:
                        symbol, exchange = extract_vt_symbol(vt_symbol)
                        pos.update({'symbol': symbol})
                        pos.update({'name': self.get_name(vt_symbol)})

            # 如果策略有 positions 属性
            elif hasattr(strategy, 'positions') and issubclass(strategy.positions, Dict):
                symbol, exchange = extract_vt_symbol(strategy.vt_symbol)
                # 多仓
                for vt_symbol, pos in strategy.get_positions.items():
                    long_pos = {}
                    long_pos['vt_symbol'] = vt_symbol
                    long_pos['symbol'] = vt_symbol.split('.')[0]
                    long_pos['name'] = self.get_name(vt_symbol)
                    long_pos['direction'] = 'long'
                    long_pos['volume'] = pos.volume
                    long_pos['price'] = pos.price
                    long_pos['pnl'] = pos.pnl
                    if long_pos['volume'] > 0:
                        pos_list.append(long_pos)

        # update local pos dict
        self.strategy_pos_dict.update({name: pos_list})

        return pos_list

    def get_all_strategy_pos(self):
        """
        获取所有得策略仓位明细
        """
        strategy_pos_list = []
        for strategy_name in list(self.strategies.keys()):
            d = OrderedDict()
            d['accountid'] = self.engine_config.get('accountid', '-')
            d['strategy_group'] = self.engine_config.get('strategy_group', '-')
            d['strategy_name'] = strategy_name
            dt = datetime.now()
            d['date'] = dt.strftime('%Y%m%d')
            d['hour'] = dt.hour
            d['datetime'] = datetime.now()
            strategy = self.strategies.get(strategy_name)
            d['inited'] = strategy.inited
            d['trading'] = strategy.trading
            try:
                d['pos'] = self.get_strategy_pos(name=strategy_name)
            except Exception as ex:
                self.write_error(
                    u'get_strategy_pos exception:{},{}'.format(str(ex), traceback.format_exc()))
                d['pos'] = []
            strategy_pos_list.append(d)

        return strategy_pos_list

    def get_strategy_class_parameters(self, class_name: str):
        """
        Get default parameters of a strategy class.
        """
        strategy_class = self.classes[class_name]

        parameters = {}
        for name in strategy_class.parameters:
            parameters[name] = getattr(strategy_class, name)

        return parameters

    def get_strategy_parameters(self, strategy_name):
        """
        Get parameters of a strategy.
        """
        strategy = self.strategies[strategy_name]
        strategy_config = self.strategy_setting.get(strategy_name, {})
        d = {}
        d.update({'auto_init': strategy_config.get('auto_init', False)})
        d.update({'auto_start': strategy_config.get('auto_start', False)})
        d.update(strategy.get_parameters())
        return d

    def get_none_strategy_pos_list(self):
        """获取非策略持有的仓位"""
        # 格式 [  'strategy_name':'account', 'pos': [{'vt_symbol': '', 'direction': 'xxx', 'volume':xxx }] } ]
        none_strategy_pos_file = os.path.abspath(os.path.join(os.getcwd(), 'data', 'none_strategy_pos.json'))
        if not os.path.exists(none_strategy_pos_file):
            return []
        try:
            with open(none_strategy_pos_file, encoding='utf8') as f:
                pos_list = json.load(f)
                if isinstance(pos_list, list):
                    return pos_list

            return []
        except Exception as ex:
            self.write_error(u'未能读取或解释{}'.format(none_strategy_pos_file))
            return []

    def compare_pos(self, strategy_pos_list=[]):
        """
        对比账号&策略的持仓,不同的话则发出微信提醒
        :return:
        """
        # 当前没有接入网关
        if len(self.main_engine.gateways) == 0:
            return False, u'当前没有接入网关'

        self.write_log(u'开始对比账号&策略的持仓')

        # 获取当前策略得持仓
        if len(strategy_pos_list) == 0:
            strategy_pos_list = self.get_all_strategy_pos()
        self.write_log(u'策略持仓清单:{}'.format(strategy_pos_list))

        none_strategy_pos = self.get_none_strategy_pos_list()
        if len(none_strategy_pos) > 0:
            strategy_pos_list.extend(none_strategy_pos)

        # 需要进行对比得合约集合（来自策略持仓/账号持仓）
        vt_symbols = set()

        # 账号的持仓处理 => account_pos
        compare_pos = dict()  # vt_symbol: {'账号多单': xx,'策略多单':[]}
        for position in list(self.positions.values()):
            # gateway_name.symbol.exchange => symbol.exchange
            vt_symbol = position.vt_symbol
            vt_symbols.add(vt_symbol)

            compare_pos[vt_symbol] = OrderedDict(
                {
                    '账号多单': position.volume,
                    '策略多单': 0,
                    '多单策略': []
                }
            )

        # 逐一根据策略仓位，与Account_pos进行处理比对
        for strategy_pos in strategy_pos_list:
            for pos in strategy_pos.get('pos', []):
                vt_symbol = pos.get('vt_symbol')
                if not vt_symbol:
                    continue
                vt_symbols.add(vt_symbol)
                symbol_pos = compare_pos.get(vt_symbol, None)
                if symbol_pos is None:
                    self.write_log(u'账号持仓信息获取不到{}，创建一个'.format(vt_symbol))
                    symbol_pos = OrderedDict(
                        {
                            '账号多单': 0,
                            '策略多单': 0,
                            '多单策略': []
                        }
                    )

                if pos.get('direction') == 'long':
                    symbol_pos.update({'策略多单': symbol_pos.get('策略多单', 0) + abs(pos.get('volume', 0))})
                    symbol_pos['多单策略'].append(
                        u'{}({})'.format(strategy_pos['strategy_name'], abs(pos.get('volume', 0))))
                    self.write_log(u'更新{}策略持多仓=>{}'.format(vt_symbol, symbol_pos.get('策略多单', 0)))

        pos_compare_result = ''
        # 精简输出
        compare_info = ''
        for vt_symbol in sorted(vt_symbols):
            # 发送不一致得结果
            symbol_pos = compare_pos.pop(vt_symbol, {
                    '账号多单': 0,
                    '策略多单': 0,
                    '多单策略': []
                })

            # 多单都一致
            if round(symbol_pos['账号多单'], 7) == round(symbol_pos['策略多单'], 7):
                msg = u'{}多单都一致.{}\n'.format(vt_symbol, json.dumps(symbol_pos, indent=2, ensure_ascii=False))
                self.write_log(msg)
                compare_info += msg
            else:
                pos_compare_result += '\n{}: '.format(vt_symbol)
                # 多单不一致
                if round(symbol_pos['策略多单'], 7) != round(symbol_pos['账号多单'], 7):
                    msg = '{}[账号({}), 策略{},共({})], ' \
                        .format(self.get_name(vt_symbol),
                                symbol_pos['账号多单'],
                                symbol_pos['多单策略'],
                                symbol_pos['策略多单'])

                    pos_compare_result += msg
                    self.write_error(u'{}不一致:{}'.format(vt_symbol, msg))
                    compare_info += u'{}不一致:{}\n'.format(vt_symbol, msg)

        # 不匹配，输入到stdErr通道
        if pos_compare_result != '':
            msg = u'账户{}持仓不匹配: {}' \
                .format(self.engine_config.get('account_id', '-'),
                        pos_compare_result)
            try:
                from vnpy.trader.util_wechat import send_wx_msg
                send_wx_msg(content=msg)
            except Exception:  # noqa
                pass
            ret_msg = u'持仓不匹配: {}' \
                .format(pos_compare_result)
            self.write_error(ret_msg)
            return True, compare_info + ret_msg
        else:
            self.write_log(u'账户持仓与策略一致')
            return True, compare_info

    def health_check(self):
        """策略健康检查"""
        for strategy_name in list(self.strategies.keys()):
            # 获取策略
            strategy = self.strategies.get(strategy_name)
            if not strategy:
                continue

            health_count = self.health_status.get(strategy_name, 0)

            # 取消计数器
            if health_count > 0 and strategy.trading:
                self.health_status.pop(strategy_name, None)
                continue

            # 增加计数器，如果超时十次，发出告警信息
            if not strategy.trading:
                health_count += 1
                if health_count > 10:
                    self.send_wechat(f'{strategy_name}交易状态停止超过10次检查周期,请检查')
                    health_count = 0
                    self.health_status.update({strategy_name: health_count})

    def init_all_strategies(self):
        """
        """
        for strategy_name in self.strategies.keys():
            self.init_strategy(strategy_name)

    def start_all_strategies(self):
        """
        """
        for strategy_name in self.strategies.keys():
            self.start_strategy(strategy_name)

    def stop_all_strategies(self):
        """
        """
        for strategy_name in self.strategies.keys():
            self.stop_strategy(strategy_name)

    def load_strategy_setting(self):
        """
        Load setting file.
        """
        # 读取引擎得配置
        #  "accountid" : "xxxx",  资金账号，一般用于推送消息时附带
        #  "strategy_group": "cta_strategy_pro", # 当前实例名。多个实例时，区分开
        #  "trade_2_wx": true  # 是否交易记录转发至微信通知
        # "event_log: false    # 是否转发日志到event bus，显示在图形界面
        self.engine_config = load_json(self.engine_filename)
        # 是否产生event log 日志（一般GUI界面才产生，而且比好消耗资源)
        self.event_log = self.engine_config.get('event_log', False)
        # 读取策略得配置
        self.strategy_setting = load_json(self.setting_filename)

        for strategy_name, strategy_config in self.strategy_setting.items():
            self.add_strategy(
                class_name=strategy_config["class_name"],
                strategy_name=strategy_name,
                vt_symbols=strategy_config["vt_symbols"],
                setting=strategy_config["setting"],
                auto_init=strategy_config.get('auto_init', False),
                auto_start=strategy_config.get('auto_start', False)
            )

    def update_strategy_setting(self, strategy_name: str, setting: dict, auto_init: bool = False,
                                auto_start: bool = False):
        """
        Update setting file.
        """
        strategy = self.strategies[strategy_name]
        # 原配置
        old_config = self.strategy_setting.get('strategy_name', {})
        new_config = {
            "class_name": strategy.__class__.__name__,
            "vt_symbols": strategy.vt_symbols,
            "auto_init": auto_init,
            "auto_start": auto_start,
            "setting": setting
        }

        if old_config:
            self.write_log(f'{strategy_name} 配置变更:\n{old_config} \n=> \n{new_config}')

        self.strategy_setting[strategy_name] = new_config

        sorted_setting = OrderedDict()
        for k in sorted(self.strategy_setting.keys()):
            sorted_setting.update({k: self.strategy_setting.get(k)})

        save_json(self.setting_filename, sorted_setting)

    def remove_strategy_setting(self, strategy_name: str):
        """
        Update setting file.
        """
        if strategy_name not in self.strategy_setting:
            return
        self.write_log(f'移除CTA股票引擎{strategy_name}的配置')
        self.strategy_setting.pop(strategy_name)
        sorted_setting = OrderedDict()
        for k in sorted(self.strategy_setting.keys()):
            sorted_setting.update({k: self.strategy_setting.get(k)})
        save_json(self.setting_filename, sorted_setting)

    def put_stop_order_event(self, stop_order: StopOrder):
        """
        Put an event to update stop order status.
        """
        event = Event(EVENT_CTA_STOPORDER, stop_order)
        self.event_engine.put(event)

    def put_strategy_event(self, strategy: CtaTemplate):
        """
        Put an event to update strategy status.
        """
        data = strategy.get_data()
        event = Event(EVENT_CTA_STRATEGY, data)
        self.event_engine.put(event)

    def put_all_strategy_pos_event(self, strategy_pos_list: list = []):
        """推送所有策略得持仓事件"""
        for strategy_pos in strategy_pos_list:
            event = Event(EVENT_STRATEGY_POS, copy(strategy_pos))
            self.event_engine.put(event)

    def write_log(self, msg: str, strategy_name: str = '', level: int = logging.INFO):
        """
        Create cta engine log event.
        """
        if self.event_log:
            # 推送至全局CTA_LOG Event
            log = LogData(msg=f"{strategy_name}: {msg}" if strategy_name else msg,
                          gateway_name="CtaStrategy",
                          level=level)
            event = Event(type=EVENT_CTA_LOG, data=log)
            self.event_engine.put(event)

        # 保存单独的策略日志
        if strategy_name:
            strategy_logger = self.strategy_loggers.get(strategy_name, None)
            if not strategy_logger:
                log_path = get_folder_path('log')
                log_filename = str(log_path.joinpath(str(strategy_name)))
                print(u'create logger:{}'.format(log_filename))
                self.strategy_loggers[strategy_name] = setup_logger(file_name=log_filename,
                                                                    name=str(strategy_name))
                strategy_logger = self.strategy_loggers.get(strategy_name)
            if strategy_logger:
                strategy_logger.log(level, msg)
        else:
            if self.logger:
                self.logger.log(level, msg)

        # 如果日志数据异常，错误和告警，输出至sys.stderr
        if level in [logging.CRITICAL]:
            log_msg = f"{strategy_name}: {msg}" if strategy_name else msg
            print(log_msg, file=sys.stderr)
            send_wx_msg(log_msg)

    def write_error(self, msg: str, strategy_name: str = ''):
        """写入错误日志"""
        self.write_log(msg=msg, strategy_name=strategy_name, level=logging.ERROR)

    def send_email(self, msg: str, strategy: CtaTemplate = None):
        """
        Send email to default receiver.
        """
        if strategy:
            subject = f"{strategy.strategy_name}"
        else:
            subject = "CTA策略股票引擎"

        self.main_engine.send_email(subject, msg)

    def send_wechat(self, msg: str, strategy: CtaTemplate = None):
        """
        send wechat message to default receiver
        :param msg:
        :param strategy:
        :return:
        """
        if strategy:
            subject = f"{strategy.strategy_name}"
        else:
            subject = "CTASTOCK引擎"

        send_wx_msg(content=f'{subject}:{msg}')
