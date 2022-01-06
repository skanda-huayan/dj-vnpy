"""
CTA期权策略运行引擎
华富资产
"""

import importlib
import os
import sys
import traceback
import json
import pickle
import bz2
import pandas as pd
import numpy as np

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
    OrderData,
    TradeData,
    OrderRequest,
    SubscribeRequest,
    LogData,
    TickData,
    BarData,
    PositionData,
    ContractData,
    HistoryRequest,
    Interval

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
    Exchange,
    Product,
    OrderType,
    Offset,
    Status
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
    import_module_by_str,
    print_dict,
    get_csv_last_dt)

from vnpy.trader.util_logger import setup_logger, logging
from vnpy.trader.util_wechat import send_wx_msg
from vnpy.data.mongo.mongo_data import MongoData
from vnpy.trader.setting import SETTINGS
from vnpy.data.stock.adjust_factor import get_all_adjust_factor
from vnpy.data.stock.stock_base import get_stock_base
from vnpy.data.common import stock_to_adj

from .base import (
    APP_NAME,
    EVENT_CTA_LOG,
    EVENT_CTA_OPTION,
    EVENT_CTA_STOPORDER,
    EngineType,
    StopOrder,
    StopOrderStatus,
    STOPORDER_PREFIX,
)
from .template import CtaTemplate
from vnpy.component.base import MARKET_DAY_ONLY, MyEncoder
from vnpy.component.cta_position import CtaPosition

STOP_STATUS_MAP = {
    Status.SUBMITTING: StopOrderStatus.WAITING,
    Status.NOTTRADED: StopOrderStatus.WAITING,
    Status.PARTTRADED: StopOrderStatus.TRIGGERED,
    Status.ALLTRADED: StopOrderStatus.TRIGGERED,
    Status.CANCELLED: StopOrderStatus.CANCELLED,
    Status.REJECTED: StopOrderStatus.CANCELLED
}

# 假期，后续可以从cta_option_config.json文件中获取更新
holiday_dict = {
    # 放假第一天:放假最后一天
    "2000124": "20200130",
    "20200501": "20200505",
    "20201001": "20201008",
    "20210211": "20210217",
    "20210501": "20210505",
    "20211001": "20211007",
}


class CtaOptionEngine(BaseEngine):
    """
    期权策略引擎

    """

    engine_type = EngineType.LIVE  # live trading engine

    # 策略配置文件
    setting_filename = "cta_option_setting.json"
    # 引擎配置文件
    config_filename = "cta_option_config.json"

    # 期权策略引擎得特殊参数配置
    #  "accountid" : "xxxx",  资金账号，一般用于推送消息时附带，后续入数据库时，可根据accountid归结到统一个账号中
    #  "strategy_group": "cta_option", # 当前实例名。多个实例时，区分开
    #  "trade_2_wx": true  # 是否交易记录转发至微信通知
    #  "event_log: false    # 是否转发日志到event bus，显示在图形界面
    #  "snapshot2file": false # 是否保存切片到文件
    #  "compare_pos": false # False，强制不进行 账号 <=> 引擎实例 得仓位比对。（一般分布式RPC运行时，其他得实例都不进行比对）
    #  "get_pos_from_db": false  # True，使用数据库得 策略<=>pos 数据作为比较（一般分布式RPC运行时，其中一个使用即可）; False，使用当前引擎实例得 策略.pos进行比对
    #  "holiday_dict": { "开始日期":"结束日期"}

    def __init__(self, main_engine: MainEngine, event_engine: EventEngine):
        """
        构造函数
        :param main_engine: 主引擎
        :param event_engine: 事件引擎
        """
        super().__init__(main_engine, event_engine, APP_NAME)

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

        # 未能订阅的symbols,支持策略启动时，并未接入gateway，如果没有收到tick，再定时重新订阅
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

        # 异步线程执行，一般用于策略得初始化数据等加载，不影响交易
        self.thread_executor = ThreadPoolExecutor(max_workers=1)
        self.thread_tasks = []

        self.vt_tradeids = set()  # for filtering duplicate trade
        self.active_orders = {}
        self.internal_orderids = set()
        self.single_execute_volume = 1

        self.net_pos_target = {}  # 净仓目标， vt_symbol: {pos: 正负数}
        self.net_pos_holding = {}  # 净仓持有， vt_symbol: {pos: 正负数}
        self.int_orderid_count = 1

        self.last_minute = None
        self.symbol_bar_dict = {}  # vt_symbol: bar(一分钟bar)

        self.stock_adjust_factors = get_all_adjust_factor()
        # 获取全量股票信息
        self.write_log(f'获取全量股票信息')
        self.symbol_dict = get_stock_base()
        self.write_log(f'共{len(self.symbol_dict)}个股票')
        # 除权因子
        self.write_log(f'获取所有除权因子')
        self.adjust_factor_dict = get_all_adjust_factor()
        self.write_log(f'共{len(self.adjust_factor_dict)}条除权信息')

        # 寻找数据文件所在目录
        vnpy_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
        self.write_log(f'项目所在目录:{vnpy_root}')
        self.bar_data_folder = os.path.abspath(os.path.join(vnpy_root, 'bar_data'))
        if os.path.exists(self.bar_data_folder):
            SSE_folder = os.path.abspath(os.path.join(vnpy_root, 'bar_data', 'SSE'))
            if os.path.exists(SSE_folder):
                self.write_log(f'上交所bar数据目录:{SSE_folder}')
            else:
                self.write_error(f'不存在上交所数据目录:{SSE_folder}')

            SZSE_folder = os.path.abspath(os.path.join(vnpy_root, 'bar_data', 'SZSE'))
            if os.path.exists(SZSE_folder):
                self.write_log(f'深交所bar数据目录:{SZSE_folder}')
            else:
                self.write_error(f'不存在深交所数据目录:{SZSE_folder}')
        else:
            self.write_error(f'不存在bar数据目录:{self.bar_data_folder}')
            self.bar_data_folder = None

    def init_engine(self):
        """
        初始化引擎
        """
        self.register_event()
        self.register_funcs()

        # 恢复内部数据
        self.load_internal_data()

        self.load_strategy_class()
        self.load_strategy_setting()

        self.write_log("CTA策略引擎初始化成功")

        if self.engine_config.get('single_execute_volume',0) > 0:
            self.single_execute_volume = self.engine_config.get('single_execute_volume',1)
            self.write_log(f'使用配置得单笔下仓数量:{self.single_execute_volume}')

        if self.engine_config.get('get_pos_from_db', False):
            self.write_log(f'激活数据库策略仓位比对模式')
            self.init_mongo_data()

    def init_mongo_data(self):
        """初始化hams数据库"""
        host = SETTINGS.get('hams.host', 'localhost')
        port = SETTINGS.get('hams.port', 27017)
        self.write_log(f'初始化hams数据库连接:{host}:{port}')
        try:
            # Mongo数据连接客户端
            self.mongo_data = MongoData(host=host, port=port)

            if self.mongo_data and self.mongo_data.db_has_connected:
                self.write_log(f'连接成功')
            else:
                self.write_error(f'HAMS数据库{host}:{port}连接异常.')
        except Exception as ex:
            self.write_error(f'HAMS数据库{host}:{port}连接异常.{str(ex)}')

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

        all_trading = True
        dt = datetime.now()

        # 触发每个策略的定时接口
        for strategy in list(self.strategies.values()):
            if strategy and strategy.inited:
                self.call_strategy_func(strategy, strategy.on_timer)

            if not strategy.trading:
                all_trading = False

            # 临近夜晚收盘前，强制发出撤单
            if dt.hour == 2 and dt.minute == 59 and dt.second >= 55:
                self.cancel_all(strategy)


        # 每分钟执行的逻辑
        if self.last_minute != dt.minute:
            self.last_minute = dt.minute

            # 内部订单超时处理
            for vt_orderid in list(self.active_orders.keys()):
                if vt_orderid not in self.internal_orderids:
                    self.write_log(f'{vt_orderid}不在内部活动订单中，不撤单')
                    continue

                order = self.active_orders.get(vt_orderid, None)
                if order is None:
                    self.write_error(f'找不到内部活动订单，不撤单')
                    continue

                # 检查超时
                if order.datetime and \
                        (datetime.now() - order.datetime).total_seconds() > 60 and \
                        order.status in [Status.NOTTRADED, Status.PARTTRADED]:
                    self.write_log(
                        f'内部活动订单{order.orderid}, {order.vt_symbol}[{order.name}], {order.direction.value}, {order.offset.value},超时.发出撤单')
                    req = order.create_cancel_request()
                    self.main_engine.cancel_order(req, order.gateway_name)

            for vt_symbol in set(self.net_pos_target.keys()).union(set(self.net_pos_holding.keys())):
                self.execute_pos_target(vt_symbol)

            # 保存内部数据
            self.save_internal_data()

            if all_trading:
                # 主动获取所有策略得持仓信息
                all_strategy_pos = self.get_all_strategy_pos()

                # 每5分钟检查一次
                if dt.minute % 5 == 0 and self.engine_config.get('compare_pos', True):
                    # 比对仓位，使用上述获取得持仓信息，不用重复获取
                    self.compare_pos(strategy_pos_list=copy(all_strategy_pos))

                # 推送到事件
                self.put_all_strategy_pos_event(all_strategy_pos)

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
                self.call_strategy_func(strategy, strategy.on_tick, {tick.vt_symbol: tick})

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
        """
        委托更新事件处理
        :param event:
        :return:
        """
        order = event.data

        strategy = self.orderid_strategy_map.get(order.vt_orderid, None)
        if not strategy:
            if order.vt_orderid in self.internal_orderids:
                self.write_log(f'委托更新 => 内部仓位: {print_dict(order.__dict__)}')
                if order.sys_orderid and order.sys_orderid != order.orderid and order.sys_orderid not in self.internal_orderids:
                    self.write_log(f'添加系统编号 {order.sys_orderid}=> 内部订单')
                    self.internal_orderids.add(order.sys_orderid)

                # self.write_log(f'当前策略侦听委托单:{list(self.orderid_strategy_map.keys())}')
                if order.type != OrderType.STOP:
                    if order.status in [Status.ALLTRADED, Status.CANCELLED, Status.REJECTED]:
                        self.write_log(f'委托更新 => 内部仓位 => 移除活动订单')
                        self.active_orders.pop(order.vt_orderid, None)

                    elif order.status in [Status.SUBMITTING, Status.NOTTRADED, Status.PARTTRADED]:
                        self.write_log(f'委托更新 => 内部仓位 => 更新活动订单')
                        self.active_orders.update({order.vt_orderid: copy(order)})
            else:
                self.write_log(f'委托更新 => 系统账号 => {print_dict(order.__dict__)}')
            return
        self.write_log(f'委托更新:{order.vt_orderid} => 策略:{strategy.strategy_name}')
        if len(order.sys_orderid) > 0 and  order.sys_orderid not in self.orderid_strategy_map:
            self.write_log(f'登记系统委托号 {order.sys_orderid} => 策略：{strategy.strategy_name} 映射')

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
        """
        成交更新事件处理
        :param event:
        :return:
        """
        trade = event.data

        # Filter duplicate trade push
        if trade.vt_tradeid in self.vt_tradeids:
            self.write_log(f'成交更新 => 交易编号{trade.vt_tradeid}已处理完毕,不再处理')
            return
        self.vt_tradeids.add(trade.vt_tradeid)

        strategy = self.orderid_strategy_map.get(trade.vt_orderid, None)

        # 该成交得单子，不属于策略，可能是内部，或者其他实例得成交
        if not strategy:

            # 属于内部单子
            if trade.vt_orderid in self.internal_orderids or trade.sys_orderid in self.internal_orderids:
                cur_pos = self.net_pos_holding.get(trade.vt_symbol, 0)
                if trade.direction == Direction.LONG:
                    new_pos = cur_pos + trade.volume
                else:
                    new_pos = cur_pos - trade.volume
                self.write_log(f'成交更新 => 内部订单 {trade.vt_symbol}[{trade.name}]: {cur_pos} => {new_pos}')
                self.write_log(f'成交单:trade:{print_dict(trade.__dict__)}')
                self.net_pos_holding.update({trade.vt_symbol: new_pos})
                self.save_internal_data()
                return

            if trade.sys_orderid and trade.sys_orderid in self.orderid_strategy_map:
                self.write_log(f'使用系统委托单号{trade.sys_orderid} => 策略')
                strategy = self.orderid_strategy_map.get(trade.sys_orderid, None)

            # 可能是其他实例得
            if not strategy:
                self.write_log(f'成交更新 => 没有对应的策略设置:trade:{trade.__dict__}')
                self.write_log(f'成交更新 => 当前策略侦听委托单:{list(self.orderid_strategy_map.keys())}')
                self.write_log(f'成交更新 => 当前内部订单清单:{self.internal_orderids}')
                return

        self.write_log(f'成交更新 =>:{trade.vt_orderid} => 策略:{strategy.strategy_name}')

        # Update strategy pos before calling on_trade method
        # 取消外部干预策略pos，由策略自行完成更新
        # if trade.direction == Direction.LONG:
        #     strategy.pos += trade.volume
        # else:
        #     strategy.pos -= trade.volume
        # 根据策略名称，写入 data\straetgy_name_trade.csv文件
        strategy_name = getattr(strategy, 'strategy_name')
        trade_fields = ['datetime', 'symbol', 'exchange', 'vt_symbol', 'name', 'tradeid', 'vt_tradeid', 'orderid',
                        'vt_orderid',
                        'direction', 'offset', 'price', 'volume']
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

        # 如果配置文件 cta_stock_config.json中，有trade_2_wx的设置项，则发送微信通知
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
                    strategy,
                    contract,
                    stop_order.direction,
                    stop_order.offset,
                    price,
                    stop_order.volume
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
            strategy_name: str,
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
        original_req = OrderRequest(
            symbol=contract.symbol,
            exchange=contract.exchange,
            direction=direction,
            offset=offset,
            type=type,
            price=price,
            volume=volume,
            strategy_name=strategy_name
        )

        # 如果没有指定网关，则使用合约信息内的网关
        if contract.gateway_name and not gateway_name:
            gateway_name = contract.gateway_name

        # Convert with offset converter
        req_list = [original_req]

        # Send Orders
        vt_orderids = []

        for req in req_list:
            vt_orderid = self.main_engine.send_order(
                req, gateway_name)

            # Check if sending order successful
            if not vt_orderid:
                continue

            vt_orderids.append(vt_orderid)

            # Save relationship between orderid and strategy.
            strategy = self.strategies.get(strategy_name, None)
            if strategy:
                self.orderid_strategy_map[vt_orderid] = strategy
                self.strategy_orderid_map[strategy.strategy_name].add(vt_orderid)

        return vt_orderids

    def send_limit_order(
            self,
            strategy_name: str,
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
            strategy_name,
            contract,
            direction,
            offset,
            price,
            volume,
            OrderType.LIMIT,
            gateway_name
        )

    def send_fak_order(
            self,
            strategy_name: str,
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
            strategy_name,
            contract,
            direction,
            offset,
            price,
            volume,
            OrderType.FAK,
            gateway_name
        )

    def send_server_stop_order(
            self,
            strategy_name: str,
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
            strategy_name,
            contract,
            direction,
            offset,
            price,
            volume,
            OrderType.STOP,
            gateway_name
        )

    def send_local_stop_order(
            self,
            strategy_name: str,
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
            strategy_name=strategy_name,
            gateway_name=gateway_name
        )

        self.stop_orders[stop_orderid] = stop_order

        vt_orderids = self.strategy_orderid_map[strategy_name]
        vt_orderids.add(stop_orderid)
        strategy = self.strategies.get(strategy_name, None)
        if strategy:
            self.call_strategy_func(strategy, strategy.on_stop_order, stop_order)
        self.put_stop_order_event(stop_order)

        return [stop_orderid]

    def cancel_server_order(self, strategy_name: str, vt_orderid: str):
        """
        Cancel existing order by vt_orderid.
        """
        order = self.main_engine.get_order(vt_orderid)
        if not order:
            self.write_log(msg=f"撤单失败，找不到委托{vt_orderid}",
                           strategy_name=strategy_name,
                           level=logging.ERROR)
            return False

        req = order.create_cancel_request()
        return self.main_engine.cancel_order(req, order.gateway_name)

    def cancel_local_stop_order(self, strategy_name: str, stop_orderid: str):
        """
        Cancel a local stop order.
        """
        stop_order = self.stop_orders.get(stop_orderid, None)
        if not stop_order:
            return False
        strategy = self.strategies[strategy_name]

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
            strategy_name: str,
            vt_symbol: str,
            direction: Direction,
            offset: Offset,
            price: float,
            volume: float,
            stop: bool,
            order_type: OrderType = OrderType.LIMIT,
            gateway_name: str = None,
            internal=False
    ):
        """
        该方法供策略、引擎使用，发送委托。
        internal: True,引擎内部使用,执行自动轧差; False 直接使用
        """
        contract = self.main_engine.get_contract(vt_symbol)
        if not contract:
            self.write_log(msg=f"委托失败，找不到合约：{vt_symbol}",
                           strategy_name=strategy_name,
                           level=logging.ERROR)
            return ""
        if contract.gateway_name and not gateway_name:
            gateway_name = contract.gateway_name
        # Round order price and volume to nearest incremental value
        price = round_to(price, contract.pricetick)
        volume = round_to(volume, contract.min_volume)
        if volume <= 0:
            self.write_error(msg=f"委托失败，合约：{vt_symbol},委托数量{volume}不符合正数",
                             strategy_name=strategy_name,
                             level=logging.ERROR)
            return ""
        if stop:
            if contract.stop_supported:
                return self.send_server_stop_order(strategy_name, contract, direction, offset, price, volume,
                                                   gateway_name)
            else:
                return self.send_local_stop_order(strategy_name, vt_symbol, direction, offset, price, volume,
                                                  gateway_name)
        # 内部订单
        if internal:
            return self.handel_internal_order(
                strategy_name=strategy_name,
                vt_symbol=vt_symbol,
                direction=direction,
                offset=offset,
                price=price,
                volume=volume,
                gateway_name=gateway_name)

        # 直接调用主引擎
        if order_type == OrderType.FAK:
            return self.send_fak_order(strategy_name, contract, direction, offset, price, volume, gateway_name)
        else:
            return self.send_limit_order(strategy_name, contract, direction, offset, price, volume, gateway_name)

    def cancel_order(self, strategy_name: str, vt_orderid: str):
        """
        """
        if vt_orderid.startswith(STOPORDER_PREFIX):
            return self.cancel_local_stop_order(strategy_name, vt_orderid)
        else:
            return self.cancel_server_order(strategy_name, vt_orderid)

    def cancel_all(self, strategy_name: str):
        """
        Cancel all active orders of a strategy.
        """
        vt_orderids = self.strategy_orderid_map[strategy_name]
        if not vt_orderids:
            return

        for vt_orderid in copy(vt_orderids):
            self.cancel_order(strategy_name, vt_orderid)

    def handel_internal_order(self, **kwargs):
        """
        处理内部订单：
        策略 => 内部订单 => 产生内部订单号 => 登记内部处理逻辑 => 添加后续异步task
        :param kwargs:
        :return:
        """
        self.write_log(f'内部订单 => 开始处理')
        vt_symbol = kwargs.get('vt_symbol')
        symbol, exchange = extract_vt_symbol(vt_symbol)
        orderid = f'o_{self.int_orderid_count}'
        strategy_name = kwargs.get('strategy_name', "")

        self.int_orderid_count += 1
        order = OrderData(
            symbol=symbol,
            exchange=exchange,
            name=self.get_name(vt_symbol),
            orderid=orderid,
            direction=kwargs.get('direction'),
            offset=kwargs.get('offset'),
            type=OrderType.LIMIT,
            price=kwargs.get('price'),
            volume=kwargs.get('volume'),
            datetime=datetime.now(),
            gateway_name=kwargs.get('gateway_name', "")
        )
        self.write_log(f'内部订单 => 生成 \n{print_dict(order.__dict__)}')
        strategy = self.strategies.get(strategy_name, None)
        if strategy:
            self.write_log(f'内部订单 => 绑定 {order.vt_orderid} <=>策略{strategy_name}')
            self.orderid_strategy_map[order.vt_orderid] = strategy
            self.strategy_orderid_map[strategy.strategy_name].add(order.vt_orderid)

        task = self.thread_executor.submit(self._handle_internal_order, order, strategy_name)
        self.thread_tasks.append(task)

        return [order.vt_orderid]

    def _handle_internal_order(self, order: OrderData, strategy_name: str):
        """
        线程执行内部订单
        :param order:
        :return:
        """
        self.write_log(f'内部订单 => 异步处理')
        vt_symbol = order.vt_symbol
        # 发送委托更新
        order.sys_orderid = order.orderid
        order.status = Status.ALLTRADED
        order.traded = order.volume
        order.time = order.datetime.strftime("%H:%M:%S")

        # 制作假的成交单
        trade = TradeData(
            symbol=order.symbol,
            exchange=order.exchange,
            direction=order.direction,
            offset=order.offset,
            name=order.name,
            strategy_name=strategy_name,
            orderid=order.orderid,
            tradeid=f't_{order.orderid}',
            price=order.price,
            volume=order.volume,
            datetime=datetime.now(),
            time=order.time,
            gateway_name=order.gateway_name
        )
        self.write_log(f'内部订单 => 生成成交单 \n {print_dict(trade.__dict__)}')
        # 发出委托更新、订单更新
        self.event_engine.put(Event(type=EVENT_ORDER, data=order))
        self.event_engine.put(Event(type=EVENT_TRADE, data=trade))

        target_pos = self.net_pos_target.get(vt_symbol, 0)

        if order.direction == Direction.LONG:
            new_target_pos = target_pos + order.volume
        else:
            new_target_pos = target_pos - order.volume

        self.net_pos_target.update({vt_symbol: new_target_pos})

        self.write_log(
            f'{strategy_name} {order.direction.value}  {order.offset.value}: net_pos_target: {target_pos} => {new_target_pos}')
        # 记录日志
        append_data(
            file_name=os.path.abspath(os.path.join(self.get_data_path(), 'cta_option_internal_orders.csv')),
            dict_data=OrderedDict({
                'datetime': order.datetime.strftime('%Y-%m-%d %H:%M:%S'),
                'strategy_name': strategy_name,
                'vt_symbol': order.volume,
                'name': order.name,
                'direction': order.direction.value,
                'offset': order.offset.value,
                'price': order.price,
                'volume': order.volume,
                'old_target': target_pos,
                'new_target': new_target_pos
            }))

    def load_internal_data(self):
        """
        加载内部数据
        :return:
        """
        f_name = os.path.abspath(os.path.join(self.get_data_path(),f'{self.engine_name}_datas.json'))
        try:
            j = load_json(f_name,auto_save=True)
            self.net_pos_target = j.get('net_pos_target', {})
            self.net_pos_holding = j.get('net_pos_holding', {})
            self.write_log('恢复内部目标持仓：{}'.format(
                ';'.join([f'{k}[{self.get_name(k)}]:{v}' for k,v in self.net_pos_target.items()])))
            self.write_log('恢复内部现有持仓：{}'.format(
                ';'.join([f'{k}[{self.get_name(k)}]:{v}' for k, v in self.net_pos_target.items()])))

        except Exception as ex:
            self.write_error(f'恢复内部数据异常：{str(ex)}')

    def save_internal_data(self):
        """
        保存内部数据
        :return:
        """
        f_name = os.path.abspath(os.path.join(self.get_data_path(), f'{self.engine_name}_datas.json'))
        try:
            d = {
                "net_pos_target":self.net_pos_target,
                "net_pos_holding":self.net_pos_holding
            }
            save_json(f_name,d)
        except Exception as ex:
            self.write_error(f'保存内部数据异常：{str(ex)}')

    def execute_pos_target(self, vt_symbol):
        """
        执行仓位目标
        :param vt_symbol:
        :return:
        """
        target_pos = self.net_pos_target.get(vt_symbol, 0)   # 该合约内部得目标持仓
        holding_pos = self.net_pos_holding.get(vt_symbol, 0)  # 该合约内部得现有持仓

        diff_pos = target_pos - holding_pos   # 找出差异
        if diff_pos == 0:
            return
        # 获取最新价
        cur_price = self.get_price(vt_symbol)
        if not cur_price:
            self.write_log(f'仓位目标执行 =>  订阅{vt_symbol}行情')
            contract = self.main_engine.get_contract(vt_symbol)
            if contract:
                gateway_name = ""
                if contract.gateway_name:
                    gateway_name = contract.gateway_name
                req = SubscribeRequest(
                    symbol=contract.symbol, exchange=contract.exchange)
                self.main_engine.subscribe(req, gateway_name)

            return
        # 获取最新tick
        cur_tick = self.get_tick(vt_symbol)
        if cur_tick is None:
            return

        price_tick = self.get_price_tick(vt_symbol)
        # 需要增加仓位（ buy or cover)
        if diff_pos > 0:
            # 账号得多、空仓位
            acc_long_position = self.get_position(vt_symbol=vt_symbol, direction=Direction.LONG)
            acc_long_pos = 0 if acc_long_position is None else acc_long_position.volume-acc_long_position.frozen
            acc_short_position = self.get_position(vt_symbol=vt_symbol, direction=Direction.SHORT)
            acc_short_pos = 0 if acc_short_position is None else acc_short_position.volume-acc_short_position.frozen

            if diff_pos > self.single_execute_volume:
                self.write_log(f'内部仓位 => 执行{vt_symbol} => 降低交易头寸: {diff_pos} -> {self.single_execute_volume}')
                diff_pos = self.single_execute_volume

            # 仅平仓
            if acc_short_pos > 0:
                # 优先平空单
                cover_pos = min(diff_pos, acc_short_pos)
                buy_pos = diff_pos - cover_pos
            else:
                # 仅开仓
                cover_pos = 0
                buy_pos = diff_pos

            self.write_log(f'内部仓位 => 执行{vt_symbol}[{self.get_name(vt_symbol)}]: ' +
                           f'[账号多单:{acc_long_pos},空单:{acc_short_pos}]' +
                           f'[holding:{holding_pos} =>target:{target_pos} ] => cover:{cover_pos} + buy:{buy_pos}')
            if cover_pos > 0:
                if not self.exist_order(vt_symbol, direction=Direction.LONG, offset=Offset.CLOSE):
                    vt_orderids = self.send_order(
                        strategy_name="",
                        vt_symbol=vt_symbol,
                        price=cur_price,
                        volume=cover_pos,
                        direction=Direction.LONG,
                        offset=Offset.CLOSE,
                        order_type=OrderType.LIMIT,
                        stop=False,
                        internal=False
                    )
                    if len(vt_orderids) > 0:
                        self.write_log(f'内部仓位 => 执行 =>  cover 登记委托编号:{vt_orderids}')
                        self.internal_orderids =self.internal_orderids.union(vt_orderids)

            if buy_pos > 0:
                if not self.exist_order(vt_symbol, direction=Direction.LONG, offset=Offset.OPEN):
                    vt_orderids = self.send_order(
                        strategy_name="",
                        vt_symbol=vt_symbol,
                        price=cur_price,
                        volume=buy_pos,
                        direction=Direction.LONG,
                        offset=Offset.OPEN,
                        order_type=OrderType.LIMIT,
                        stop=False,
                        internal=False
                    )
                    if len(vt_orderids) > 0:
                        self.write_log(f'内部仓位 => 执行 => buy 登记委托编号:{vt_orderids}')
                        self.internal_orderids= self.internal_orderids.union(vt_orderids)
        # 需要卖出 ( diff_pos < 0)
        else:
            # 账号得多、空单
            acc_long_position = self.get_position(vt_symbol=vt_symbol, direction=Direction.LONG)
            acc_long_pos = 0 if acc_long_position is None else acc_long_position.volume - acc_long_position.frozen
            acc_short_position = self.get_position(vt_symbol=vt_symbol, direction=Direction.SHORT)
            acc_short_pos = 0 if acc_short_position is None else acc_short_position.volume - acc_short_position.frozen

            if abs(diff_pos) > self.single_execute_volume:
                self.write_log(f'内部仓位 => 执行{vt_symbol} => 降低交易头寸: {abs(diff_pos)} -> {self.single_execute_volume}')
                diff_pos = -self.single_execute_volume

            # 如果账号持有多单，优先平掉账号多单
            if acc_long_pos > 0:
                sell_pos = min(abs(diff_pos), acc_long_pos)
                short_pos = abs(diff_pos) - sell_pos
            else:
                # 仅开仓
                sell_pos = 0
                short_pos = abs(diff_pos)

            self.write_log(f'内部仓位 => 执行{vt_symbol}[{self.get_name(vt_symbol)}]' +
                           f'[账号多单:{acc_long_pos},空单:{acc_short_pos}]，' +
                           f'[holding:{holding_pos} => target:{target_pos}] => sell:{sell_pos}, short:{short_pos}')

            if sell_pos > 0:
                if not self.exist_order(vt_symbol, direction=Direction.SHORT, offset=Offset.CLOSE):
                    vt_orderids = self.send_order(
                        strategy_name="",
                        vt_symbol=vt_symbol,
                        price=cur_price,
                        volume=sell_pos,
                        direction=Direction.SHORT,
                        offset=Offset.CLOSE,
                        order_type=OrderType.LIMIT,
                        stop=False,
                        internal=False
                    )
                    if len(vt_orderids) > 0:
                        self.write_log(f'内部仓位 => 执行 => sell 登记委托编号:{vt_orderids}')
                        self.internal_orderids = self.internal_orderids.union(vt_orderids)
            if short_pos > 0:
                if not self.exist_order(vt_symbol, direction=Direction.SHORT, offset=Offset.OPEN):
                    vt_orderids = self.send_order(
                        strategy_name="",
                        vt_symbol=vt_symbol,
                        price=cur_price,
                        volume=short_pos,
                        direction=Direction.SHORT,
                        offset=Offset.OPEN,
                        order_type=OrderType.LIMIT,
                        stop=False,
                        internal=False
                    )
                    if len(vt_orderids) > 0:
                        self.write_log(f'内部仓位 => 执行 => short 登记委托编号:{vt_orderids}')
                        self.internal_orderids = self.internal_orderids.union(vt_orderids)

    def exist_order(self, vt_symbol, direction, offset):
        """
        是否存在相同得委托
        :param vt_symbol:
        :param direction:
        :param offset:
        :return:
        """
        if len(self.active_orders) == 0:
            self.write_log(f'内部活动订单中，数量为零. 查询{vt_symbol}，方向:{direction.value}, 开平:{offset.value}')
            return False

        for vt_orderid in list(self.active_orders.keys()):
            order = self.active_orders.get(vt_orderid, None)
            if order is None:
                continue

            if order.vt_symbol == vt_symbol and order.direction == direction and order.offset == offset:
                self.write_log(f'引擎存在相同的内部活动订单:{order.name}')
                return True

            if order.vt_symbol == vt_symbol and order.direction != direction and order.offset != offset:
                self.write_log(f'引擎存在可能自成交的内部活动订单:{order.name}')
                return True

        return False

    def subscribe_symbol(self, strategy_name: str, vt_symbol: str, gateway_name: str = '', is_bar: bool = False):
        """订阅合约"""
        strategy = self.strategies.get(strategy_name, None)
        if not strategy:
            return False
        if len(vt_symbol) == 0:
            self.write_error(f'不能为{strategy_name}订阅空白合约')
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
    def get_exchange(self, symbol):
        return self.main_engine.get_exchange(symbol)

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
            self.write_error(f'查询不到{vt_symbol}合约信息，缺省使用1作为价格跳动')
            return 0.0001

        return contract.pricetick

    @lru_cache()
    def get_volume_tick(self, vt_symbol: str):
        """查询合约的最小成交数量"""
        contract = self.main_engine.get_contract(vt_symbol)
        if contract is None:
            self.write_error(f'查询不到{vt_symbol}合约信息,缺省使用1作为最小成交数量')
            return 1

        return contract.min_volume

    def get_margin(self, vt_symbol: str):
        """
        按照当前价格，计算1手合约需要得保证金
        :param vt_symbol:
        :return: 普通合约/期权 => 当前价格 * size * margin_rate

        """

        cur_price = self.get_price(vt_symbol)
        cur_size = self.get_size(vt_symbol)
        cur_margin_rate = self.get_margin_rate(vt_symbol)
        if cur_price and cur_size and cur_margin_rate:
            return abs(cur_price * cur_size * cur_margin_rate)
        else:
            # 取不到价格，取不到size，或者取不到保证金比例
            self.write_error(f'无法计算{vt_symbol}的保证金，价格:{cur_price}或size:{cur_size}或margin_rate:{cur_margin_rate}')
            return None

    def get_tick(self, vt_symbol: str):
        """获取合约得最新tick"""
        return self.main_engine.get_tick(vt_symbol)

    def get_price(self, vt_symbol: str):
        """查询合约的最新价格"""
        price = self.main_engine.get_price(vt_symbol)
        if price:
            return price

        tick = self.main_engine.get_tick(vt_symbol)
        if tick:
            if '&' in tick.symbol:
                return (tick.ask_price_1 + tick.bid_price_1) / 2
            else:
                return tick.last_price

        return None

    def get_contract(self, vt_symbol):
        return self.main_engine.get_contract(vt_symbol)

    def get_all_contracts(self):
        return self.main_engine.get_all_contracts()

    def get_option_list(self, underlying_symbol, year_month):
        """
        获取ETF期权的交易合约
        :param underlying_symbol: 标的物合约，例如 510050.SSE
        :param year_month  2112, 表示2021年12月
        :return:
        """
        symbol = underlying_symbol.split('.')[0]

        all_contracts = self.get_all_contracts()

        # 510050C2112M03100
        cur_month_contracts = [c for c in all_contracts
                               if c.product == Product.OPTION \
                               and len(c.option_index) >= 17 \
                               and c.option_index.startswith(symbol) \
                               and c.option_index[7:11] == str(year_month)]
        d = {}
        for c in cur_month_contracts:
            if c.vt_symbol in d:
                continue
            d[c.vt_symbol] = c
        return list(d.values())

    def get_holiday(self):
        """获取假日"""
        return self.engine_config.get('holiday_dict', holiday_dict)

    def get_option_rest_days(self, cur_date: str, expire_date: str):
        """
        获取期权从当前日到行权价得结束天数
        :param cur_date: 当前日期
        :param expire_date: 行权日期
        :return:
        """
        holidays = self.get_holiday()
        if cur_date > expire_date:
            return 0
        rest_days = 0  # 剩余天数
        # 开始日期 > 结束日期
        for d in range(int(cur_date), int(expire_date)):
            _s = str(d)

            # 判断是否周六日
            try:
                _c = datetime.strptime(_s, '%Y%m%d')

                if _c.isoweekday() in [6, 7]:
                    continue

            except Exception as ex:
                continue
            # 剩余天数先增加一天
            rest_days += 1

            # 如果存在假期内，就减除1天
            for s, e in holidays.items():
                if s <= _s <= e:
                    rest_days -= 1
                    break

        return max(0, rest_days)

    def get_account(self, vt_accountid: str = ""):
        """ 查询账号的资金"""
        # 如果启动风控，则使用风控中的最大仓位
        if self.main_engine.rm_engine:
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

    def get_position(self, vt_symbol: str, direction: Direction, gateway_name: str = ''):
        """ 查询合约在账号的持仓,需要指定方向"""
        if len(gateway_name) == 0:
            contract = self.main_engine.get_contract(vt_symbol)
            if contract and contract.gateway_name:
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
            callback: Callable[[BarData], None],
            interval_num: int = 1
    ):
        """获取历史记录"""
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
                interval_num=interval_num,
                start=start,
                end=end
            )
            bars = self.main_engine.query_history(req, contract.gateway_name)

            if bars is None:
                self.write_error(f'获取不到历史K线:{req.__dict__}')
                return

        for bar in bars:
            if bar.trading_day:
                bar.trading_day = bar.datetime.strftime('%Y-%m-%d')

            callback(bar)

    def get_bars(
            self,
            vt_symbol: str,
            days: int,
            interval: Interval,
            interval_num: int = 1
    ):
        """获取历史记录"""
        symbol, exchange = extract_vt_symbol(vt_symbol)
        end = datetime.now()
        start = end - timedelta(days)
        bars = []

        # 检查股票代码
        if vt_symbol not in self.symbol_dict:
            self.write_error(f'{vt_symbol}不在基础配置股票信息中')
            return bars

        # 检查数据文件目录
        if not self.bar_data_folder:
            self.write_error(f'没有bar数据目录')
            return bars
        # 按照交易所的存放目录
        bar_file_folder = os.path.abspath(os.path.join(self.bar_data_folder, f'{exchange.value}'))

        resample_min = False
        resample_hour = False
        resample_day = False
        file_interval_num = 1
        # 只有1,5,15,30分钟，日线数据
        if interval == Interval.MINUTE:
            # 如果存在相应的分钟文件，直接读取
            bar_file_path = os.path.abspath(os.path.join(bar_file_folder, f'{symbol}_{interval_num}m.csv'))
            if interval_num in [1, 5, 15, 30] and os.path.exists(bar_file_path):
                file_interval_num = interval
            # 需要resample
            else:
                resample_min = True
                if interval_num > 5:
                    file_interval_num = 5

        elif interval == Interval.HOUR:
            file_interval_num = 5
            resample_hour = True
            bar_file_path = os.path.abspath(os.path.join(bar_file_folder, f'{symbol}_{file_interval_num}m.csv'))
        elif interval == Interval.DAILY:
            bar_file_path = os.path.abspath(os.path.join(bar_file_folder, f'{symbol}_{interval_num}d.csv'))
            if not os.path.exists(bar_file_path):
                file_interval_num = 5
                resample_day = True
                bar_file_path = os.path.abspath(os.path.join(bar_file_folder, f'{symbol}_{file_interval_num}m.csv'))
        else:
            self.write_error(f'目前仅支持分钟,小时，日线数据')
            return bars

        bar_interval_seconds = interval_num * 60

        if not os.path.exists(bar_file_path):
            self.write_error(f'没有bar数据文件：{bar_file_path}')
            return bars

        try:
            data_types = {
                "datetime": str,
                "open": float,
                "high": float,
                "low": float,
                "close": float,
                "volume": float,
                "amount": float,
                "symbol": str,
                "trading_day": str,
                "date": str,
                "time": str
            }

            symbol_df = None
            qfq_bar_file_path = bar_file_path.replace('.csv', '_qfq.csv')
            use_qfq_file = False
            last_qfq_dt = get_csv_last_dt(qfq_bar_file_path)
            if last_qfq_dt is not None:
                last_dt = get_csv_last_dt(bar_file_path)

                if last_qfq_dt == last_dt:
                    use_qfq_file = True

            if use_qfq_file:
                self.write_log(f'使用前复权文件:{qfq_bar_file_path}')
                symbol_df = pd.read_csv(qfq_bar_file_path, dtype=data_types)
            else:
                # 加载csv文件 =》 dateframe
                self.write_log(f'使用未复权文件:{bar_file_path}')
                symbol_df = pd.read_csv(bar_file_path, dtype=data_types)

            # 转换时间，str =》 datetime
            symbol_df["datetime"] = pd.to_datetime(symbol_df["datetime"], format="%Y-%m-%d %H:%M:%S")
            # 设置时间为索引
            symbol_df = symbol_df.set_index("datetime")

            # 裁剪数据
            symbol_df = symbol_df.loc[start:end]

            if resample_day:
                self.write_log(f'{vt_symbol} resample:{file_interval_num}m => {interval}day')
                symbol_df = self.resample_bars(df=symbol_df, to_day=True)
            elif resample_hour:
                self.write_log(f'{vt_symbol} resample:{file_interval_num}m => {interval}hour')
                symbol_df = self.resample_bars(df=symbol_df, x_hour=interval_num)
            elif resample_min:
                self.write_log(f'{vt_symbol} resample:{file_interval_num}m => {interval}m')
                symbol_df = self.resample_bars(df=symbol_df, x_min=interval_num)

            if len(symbol_df) == 0:
                return bars

            if not use_qfq_file:
                # 复权转换
                adj_list = self.adjust_factor_dict.get(vt_symbol, [])
                # 按照结束日期，裁剪复权记录
                adj_list = [row for row in adj_list if
                            row['dividOperateDate'].replace('-', '') <= end.strftime('%Y%m%d')]

                if len(adj_list) > 0:
                    self.write_log(f'需要对{vt_symbol}进行前复权处理')
                    for row in adj_list:
                        row.update({'dividOperateDate': row.get('dividOperateDate')[:10] + ' 09:30:00'})
                    # list -> dataframe, 转换复权日期格式
                    adj_data = pd.DataFrame(adj_list)
                    adj_data["dividOperateDate"] = pd.to_datetime(adj_data["dividOperateDate"],
                                                                  format="%Y-%m-%d %H:%M:%S")
                    adj_data = adj_data.set_index("dividOperateDate")
                    # 调用转换方法，对open,high,low,close, volume进行复权, fore, 前复权， 其他，后复权
                    symbol_df = stock_to_adj(symbol_df, adj_data, adj_type='fore')

            for dt, bar_data in symbol_df.iterrows():
                bar_datetime = dt  # - timedelta(seconds=bar_interval_seconds)

                bar = BarData(
                    gateway_name='backtesting',
                    symbol=symbol,
                    exchange=exchange,
                    datetime=bar_datetime
                )
                if 'open' in bar_data:
                    bar.open_price = float(bar_data['open'])
                    bar.close_price = float(bar_data['close'])
                    bar.high_price = float(bar_data['high'])
                    bar.low_price = float(bar_data['low'])
                else:
                    bar.open_price = float(bar_data['open_price'])
                    bar.close_price = float(bar_data['close_price'])
                    bar.high_price = float(bar_data['high_price'])
                    bar.low_price = float(bar_data['low_price'])

                bar.volume = int(bar_data['volume']) if not np.isnan(bar_data['volume']) else 0
                bar.date = dt.strftime('%Y-%m-%d')
                bar.time = dt.strftime('%H:%M:%S')
                str_td = str(bar_data.get('trading_day', ''))
                if len(str_td) == 8:
                    bar.trading_day = str_td[0:4] + '-' + str_td[4:6] + '-' + str_td[6:8]
                else:
                    bar.trading_day = bar.date

                bars.append(bar)

        except Exception as ex:
            self.write_error(u'回测时读取{} csv文件{}失败:{}'.format(vt_symbol, bar_file_path, ex))
            self.write_error(traceback.format_exc())
            return bars

        return bars

    def resample_bars(self, df, x_min=None, x_hour=None, to_day=False):
        """
        重建x分钟K线（或日线）
        :param df: 输入分钟数
        :param x_min: 5, 15, 30, 60
        :param x_hour: 1, 2, 3, 4
        :param include_day: 重建日线, True得时候，不会重建分钟数
        :return:
        """
        # 设置df数据中每列的规则
        ohlc_rule = {
            'open': 'first',  # open列：序列中第一个的值
            'high': 'max',  # high列：序列中最大的值
            'low': 'min',  # low列：序列中最小的值
            'close': 'last',  # close列：序列中最后一个的值
            'volume': 'sum',  # volume列：将所有序列里的volume值作和
            'amount': 'sum',  # amount列：将所有序列里的amount值作和
            "symbol": 'first',
            "trading_date": 'first',
            "date": 'first',
            "time": 'first'
        }

        if isinstance(x_min, int) and not to_day:
            # 合成x分钟K线并删除为空的行 参数 closed：left类似向上取值既 09：30的k线数据是包含09：30-09：35之间的数据
            df_target = df.resample(f'{x_min}min', closed='left', label='left').agg(ohlc_rule).dropna(axis=0,
                                                                                                      how='any')
            return df_target
        if isinstance(x_hour, int) and not to_day:
            # 合成x小时K线并删除为空的行 参数 closed：left类似向上取值既 09：30的k线数据是包含09：30-09：35之间的数据
            df_target = df.resample(f'{x_hour}hour', closed='left', label='left').agg(ohlc_rule).dropna(axis=0,
                                                                                                        how='any')
            return df_target

        if to_day:
            # 合成x分钟K线并删除为空的行 参数 closed：left类似向上取值既 09：30的k线数据是包含09：30-09：35之间的数据
            df_target = df.resample(f'D', closed='left', label='left').agg(ohlc_rule).dropna(axis=0, how='any')
            return df_target

        return df

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
            accountid = self.engine_config.get('accountid', 'XXX')

            msg = f"{accountid}/{strategy.strategy_name}触发异常已停止\n{traceback.format_exc()}"
            self.write_log(msg=msg,
                           strategy_name=strategy.strategy_name,
                           level=logging.CRITICAL)
            self.send_wechat(msg)

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
        try:
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

        except Exception as ex:
            msg = f'添加策略实例{strategy_name}失败,{str(ex)}'
            self.write_error(msg)
            self.write_error(traceback.format_exc())
            self.send_wechat(msg)

            return False, f'添加策略实例{strategy_name}失败'

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
            self.write_log(msg)
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
            # self.write_error(err_msg)
            # return False, err_msg
            ret, msg = self.stop_strategy(strategy_name)
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
            if strategy in strategies:
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
            self.write_log(f'使用配置文件的配置:{old_strategy_config}')
        else:
            old_strategy_config = copy(self.strategy_setting[strategy_name])
            self.write_log(f'使用已经运行的配置:{old_strategy_config}')

        class_name = old_strategy_config.get('class_name')
        self.write_log(f'使用策略类名:{class_name}')

        # 没有配置vt_symbol时，使用配置文件/旧配置中的vt_symbol
        if len(vt_symbols) == 0:
            vt_symbols = old_strategy_config.get('vt_symbols')
            self.write_log(f'使用配置文件/已运行配置的vt_symbols:{vt_symbols}')

        # 没有新配置时，使用配置文件/旧配置中的setting
        if len(setting) == 0:
            setting = old_strategy_config.get('setting')
            self.write_log(f'没有新策略参数，使用配置文件/旧配置中的setting:{setting}')

        module_name = self.class_module_map[class_name]
        # 重新load class module
        # if not self.load_strategy_class_from_module(module_name):
        #    err_msg = f'不能加载模块:{module_name}'
        #    self.write_error(err_msg)
        #    return False, err_msg
        if module_name:
            new_class_name = module_name + '.' + class_name
            self.write_log(u'转换策略为全路径:{}'.format(new_class_name))
            old_strategy_class = self.classes[class_name]
            self.write_log(f'旧策略ID:{id(old_strategy_class)}')
            strategy_class = import_module_by_str(new_class_name)
            if strategy_class is None:
                err_msg = u'加载策略模块失败:{}'.format(new_class_name)
                self.write_error(err_msg)
                return False, err_msg

            self.write_log(f'重新加载模块成功，使用新模块:{new_class_name}')
            self.write_log(f'新策略ID:{id(strategy_class)}')
            self.classes[class_name] = strategy_class
        else:
            self.write_log(f'没有{class_name}的module_name,无法重新加载模块')

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

    def get_strategy_kline_names(self, strategy_name):
        """
        获取策略实例内的K线名称
        :param strategy_name:策略实例名称
        :return:
        """
        info = {}
        strategy = self.strategies.get(strategy_name, None)
        if strategy is None:
            return info
        if hasattr(strategy, 'get_klines_info'):
            info = strategy.get_klines_info()
        return info

    def get_strategy_snapshot(self, strategy_name, include_kline_names=[]):
        """
        实时获取策略的K线切片（比较耗性能）
        :param strategy_name: 策略实例
        :param include_kline_names: 指定若干kline名称
        :return:
        """
        strategy = self.strategies.get(strategy_name, None)
        if strategy is None:
            return None

        try:
            # 5.获取策略切片
            snapshot = strategy.get_klines_snapshot(include_kline_names)
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
                'strategy_group': self.engine_config.get('strategy_group', self.engine_name),
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
            path1, "vnpy.app.cta_option.strategies")

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

    def get_all_strategy_pos_from_hams(self):
        """
        获取hams中该账号下所有策略仓位明细
        """
        strategy_pos_list = []
        if not self.mongo_data:
            self.init_mongo_data()

        if self.mongo_data and self.mongo_data.db_has_connected:
            filter = {'account_id': self.engine_config.get('accountid', '-')}

            pos_list = self.mongo_data.db_query(
                db_name='Account',
                col_name='today_strategy_pos',
                filter_dict=filter
            )
            for pos in pos_list:
                strategy_pos_list.append(pos)

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

    def get_strategy_value(self, strategy_name: str, parameter: str):
        """获取策略的某个参数值"""
        strategy = self.strategies.get(strategy_name)
        if not strategy:
            return None

        value = getattr(strategy, parameter, None)
        return value

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

    def compare_pos(self, strategy_pos_list=[], auto_balance=False):
        """
        对比账号&策略的持仓,不同的话则发出微信提醒
        :return:
        """
        # 当前没有接入网关
        if len(self.main_engine.gateways) == 0:
            return False, u'当前没有接入网关'

        self.write_log(u'开始对比账号&策略的持仓')

        # 获取hams数据库中所有运行实例得策略
        if self.engine_config.get("get_pos_from_db", False):
            strategy_pos_list = self.get_all_strategy_pos_from_hams()
        else:
            # 获取当前实例运行策略得持仓
            if len(strategy_pos_list) == 0:
                strategy_pos_list = self.get_all_strategy_pos()
        self.write_log(u'策略持仓清单:{}'.format(strategy_pos_list))

        none_strategy_pos = self.get_none_strategy_pos_list()
        if len(none_strategy_pos) > 0:
            strategy_pos_list.extend(none_strategy_pos)

        # 需要进行对比得合约集合（来自策略持仓/账号持仓）
        vt_symbols = set()

        # 账号的持仓处理 => account_pos

        compare_pos = dict()  # vt_symbol: {'账号多单': xx, '账号空单':xxx, '策略空单':[], '策略多单':[]}

        for pos in self.main_engine.get_all_positions():
            vt_symbols.add(pos.vt_symbol)
            vt_symbol_pos = compare_pos.get(pos.vt_symbol, {
                "账号空单": 0,
                '账号多单': 0,
                '策略空单': 0,
                '策略多单': 0,
                '空单策略': [],
                '多单策略': []
            })
            if pos.direction == Direction.LONG:
                vt_symbol_pos['账号多单'] = vt_symbol_pos['账号多单'] + pos.volume
            else:
                vt_symbol_pos['账号空单'] = vt_symbol_pos['账号空单'] + pos.volume

            compare_pos.update({pos.vt_symbol: vt_symbol_pos})

        # 逐一根据策略仓位，与Account_pos进行处理比对
        for strategy_pos in strategy_pos_list:
            for pos in strategy_pos.get('pos', []):
                vt_symbol = pos.get('vt_symbol')
                if not vt_symbol:
                    continue
                vt_symbols.add(vt_symbol)
                symbol_pos = compare_pos.get(vt_symbol, None)
                if symbol_pos is None:
                    # self.write_log(u'账号持仓信息获取不到{}，创建一个'.format(vt_symbol))
                    symbol_pos = OrderedDict(
                        {
                            "账号空单": 0,
                            '账号多单': 0,
                            '策略空单': 0,
                            '策略多单': 0,
                            '空单策略': [],
                            '多单策略': []
                        }
                    )

                if pos.get('direction') == 'short':
                    symbol_pos.update({'策略空单': symbol_pos.get('策略空单', 0) + abs(pos.get('volume', 0))})
                    symbol_pos['空单策略'].append(
                        u'{}({})'.format(strategy_pos['strategy_name'], abs(pos.get('volume', 0))))
                    self.write_log(u'更新{}策略持空仓=>{}'.format(vt_symbol, symbol_pos.get('策略空单', 0)))
                if pos.get('direction') == 'long':
                    symbol_pos.update({'策略多单': symbol_pos.get('策略多单', 0) + abs(pos.get('volume', 0))})
                    symbol_pos['多单策略'].append(
                        u'{}({})'.format(strategy_pos['strategy_name'], abs(pos.get('volume', 0))))
                    self.write_log(u'更新{}策略持多仓=>{}'.format(vt_symbol, symbol_pos.get('策略多单', 0)))

                compare_pos.update({vt_symbol: symbol_pos})

        pos_compare_result = ''
        # 精简输出
        compare_info = ''
        diff_pos_dict = {}
        for vt_symbol in sorted(vt_symbols):
            # 发送不一致得结果
            symbol_pos = compare_pos.pop(vt_symbol, {})

            d_long = {
                'account_id': self.engine_config.get('accountid', '-'),
                'vt_symbol': vt_symbol,
                'direction': Direction.LONG.value,
                'strategy_list': symbol_pos.get('多单策略', [])}

            d_short = {
                'account_id': self.engine_config.get('accountid', '-'),
                'vt_symbol': vt_symbol,
                'direction': Direction.SHORT.value,
                'strategy_list': symbol_pos.get('空单策略', [])}

            # 帐号多/空轧差， vs 策略多空轧差 是否一致；

            diff_match = (symbol_pos.get('账号多单', 0) - symbol_pos.get('账号空单', 0)) == (
                    symbol_pos.get('策略多单', 0) - symbol_pos.get('策略空单', 0))
            pos_match = symbol_pos.get('账号空单', 0) == symbol_pos.get('策略空单', 0) and \
                        symbol_pos.get('账号多单', 0) == symbol_pos.get('策略多单', 0)
            match = diff_match
            # 轧差一致，帐号/策略持仓不一致
            if diff_match and not pos_match:
                if symbol_pos.get('账号多单', 0) > symbol_pos.get('策略多单', 0):
                    self.write_log('{}轧差持仓：多:{},空:{} 大于 策略持仓 多:{},空:{}'.format(
                        vt_symbol,
                        symbol_pos.get('账号多单', 0),
                        symbol_pos.get('账号空单', 0),
                        symbol_pos.get('策略多单', 0),
                        symbol_pos.get('策略空单', 0)
                    ))
                    diff_pos_dict.update({vt_symbol: {"long": symbol_pos.get('账号多单', 0) - symbol_pos.get('策略多单', 0),
                                                      "short": symbol_pos.get('账号空单', 0) - symbol_pos.get('策略空单',
                                                                                                          0)}})

            # 多空都一致
            if match:
                msg = u'{}[{}]多空都一致.{}\n'.format(vt_symbol, self.get_name(vt_symbol),
                                                 json.dumps(symbol_pos, indent=2, ensure_ascii=False))
                self.write_log(msg)
                compare_info += msg
            else:
                pos_compare_result += '\n{}: '.format(vt_symbol)
                # 判断是多单不一致？
                diff_long_volume = round(symbol_pos.get('账号多单', 0), 7) - round(symbol_pos.get('策略多单', 0), 7)
                if diff_long_volume != 0:
                    msg = '{}多单[账号({}), 策略{},共({})], ' \
                        .format(vt_symbol,
                                symbol_pos.get('账号多单'),
                                symbol_pos.get('多单策略'),
                                symbol_pos.get('策略多单'))

                    pos_compare_result += msg
                    self.write_error(u'{}不一致:{}'.format(vt_symbol, msg))
                    compare_info += u'{}不一致:{}\n'.format(vt_symbol, msg)
                    if auto_balance:
                        self.balance_pos(vt_symbol, Direction.LONG, diff_long_volume)

                # 判断是空单不一致:
                diff_short_volume = round(symbol_pos.get('账号空单', 0), 7) - round(symbol_pos.get('策略空单', 0), 7)

                if diff_short_volume != 0:
                    msg = '{}[{}] 空单[账号({}), 策略{},共({})], ' \
                        .format(vt_symbol,
                                self.get_name(vt_symbol),
                                symbol_pos.get('账号空单'),
                                symbol_pos.get('空单策略'),
                                symbol_pos.get('策略空单'))
                    pos_compare_result += msg
                    self.write_error(u'{}[{}]不一致:{}'.format(vt_symbol, self.get_name(vt_symbol), msg))
                    compare_info += u'{}[{}]不一致:{}\n'.format(vt_symbol, self.get_name(vt_symbol), msg)
                    if auto_balance:
                        self.balance_pos(vt_symbol, Direction.SHORT, diff_short_volume)

        # 不匹配，输入到stdErr通道
        if pos_compare_result != '':
            msg = u'账户{}持仓不匹配: {}' \
                .format(self.engine_config.get('accountid', '-'),
                        pos_compare_result)
            try:
                from vnpy.trader.util_wechat import send_wx_msg
                send_wx_msg(content=msg)
            except Exception as ex:  # noqa
                pass
            ret_msg = u'持仓不匹配: {}' \
                .format(pos_compare_result)
            self.write_error(ret_msg)
            return True, compare_info + ret_msg
        else:
            self.write_log(u'账户持仓与策略一致')
            if len(diff_pos_dict) > 0:
                for k, v in diff_pos_dict.items():
                    self.write_log(f'{k} 存在大于策略的轧差持仓:{v}')
            return True, compare_info

    def balance_pos(self, vt_symbol, direction, volume):
        """
        平衡仓位
        :param vt_symbol: 需要平衡得合约
        :param direction: 合约原始方向
        :param volume: 合约需要调整得数量（正数，需要平仓， 负数，需要开仓）
        :return:
        """
        tick = self.get_tick(vt_symbol)
        if tick is None:
            gateway_names = self.main_engine.get_all_gateway_names()
            gateway_name = gateway_names[0] if len(gateway_names) > 0 else ""
            symbol, exchange = extract_vt_symbol(vt_symbol)
            self.main_engine.subscribe(req=SubscribeRequest(symbol=symbol, exchange=exchange),
                                       gateway_name=gateway_name)
            self.write_log(f'{vt_symbol}无最新tick，订阅行情')

        if volume > 0 and tick:
            contract = self.main_engine.get_contract(vt_symbol)
            req = OrderRequest(
                symbol=contract.symbol,
                exchange=contract.exchange,
                direction=Direction.SHORT if direction == Direction.LONG else Direction.LONG,
                offset=Offset.CLOSE,
                type=OrderType.LIMIT,
                price=tick.ask_price_1 if direction == Direction.SHORT else tick.bid_price_1,
                volume=round(volume, 7)
            )
            reqs = [req]
            self.write_log(f'平衡仓位，减少 {vt_symbol}，方向:{direction}，数量:{req.volume} ')
            for req in reqs:
                self.main_engine.send_order(req, contract.gateway_name)
        elif volume < 0 and tick:
            contract = self.main_engine.get_contract(vt_symbol)
            req = OrderRequest(
                symbol=contract.symbol,
                exchange=contract.exchange,
                direction=direction,
                offset=Offset.OPEN,
                type=OrderType.FAK,
                price=tick.ask_price_1 if direction == Direction.LONG else tick.bid_price_1,
                volume=round(abs(volume), 7)
            )
            reqs = [req]
            self.write_log(f'平衡仓位， 增加{vt_symbol}， 方向:{direction}, 数量: {req.volume}')
            for req in reqs:
                self.main_engine.send_order(req, contract.gateway_name)

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
        self.engine_config = load_json(self.config_filename)
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
        self.write_log(f'移除CTA期权引擎{strategy_name}的配置')
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
        event = Event(EVENT_CTA_OPTION, data)
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
        if level in [logging.CRITICAL, logging.ERROR, logging.WARNING]:
            print(f"{strategy_name}: {msg}" if strategy_name else msg, file=sys.stderr)

        if level in [logging.CRITICAL, logging.WARN, logging.WARNING]:
            send_wx_msg(content=f"{strategy_name}: {msg}" if strategy_name else msg,
                        target=self.engine_config.get('accountid', 'XXX'))

    def write_error(self, msg: str, strategy_name: str = '', level: int = logging.ERROR):
        """写入错误日志"""
        self.write_log(msg=msg, strategy_name=strategy_name, level=level)

    def send_email(self, msg: str, strategy: CtaTemplate = None):
        """
        Send email to default receiver.
        """
        if strategy:
            subject = f"{strategy.strategy_name}"
        else:
            subject = "CTA期权策略引擎"

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
            subject = "CTA Option引擎"

        send_wx_msg(content=f'{subject}:{msg}')
