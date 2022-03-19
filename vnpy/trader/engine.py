"""
"""

import logging
from logging import Logger
import smtplib
import os
import sys
from abc import ABC
from datetime import datetime
from email.message import EmailMessage
from queue import Empty, Queue
from threading import Thread
from typing import Any, Sequence, Type, Dict, List, Optional

from vnpy.event import Event, EventEngine
from .app import BaseApp
from .event import (
    EVENT_TICK,
    EVENT_ORDER,
    EVENT_TRADE,
    EVENT_POSITION,
    EVENT_ACCOUNT,
    EVENT_CONTRACT,
    EVENT_LOG
)
from .gateway import BaseGateway
from .object import (
    Direction,
    Exchange,
    CancelRequest,
    LogData,
    OrderRequest,
    SubscribeRequest,
    HistoryRequest,
    OrderData,
    BarData,
    TickData,
    TradeData,
    PositionData,
    AccountData,
    ContractData
)
from .setting import SETTINGS
from .utility import get_folder_path, TRADER_DIR

# 专有的logger文件
from .util_logger import setup_logger


class MainEngine:
    """
    Acts as the core of VN Trader.
    """

    def __init__(self, event_engine: EventEngine = None):
        """"""
        if event_engine:
            self.event_engine: EventEngine = event_engine
        else:
            self.event_engine = EventEngine()
        self.event_engine.start()

        self.gateways: Dict[str, BaseGateway] = {}
        self.engines: Dict[str, BaseEngine] = {}
        self.apps: Dict[str, BaseApp] = {}
        self.exchanges: List[Exchange] = []

        self.rm_engine = None
        self.algo_engine = None
        self.rpc_service = None

        os.chdir(TRADER_DIR)  # Change working directory
        self.init_engines()  # Initialize function engines

    def add_engine(self, engine_class: Any) -> "BaseEngine":
        """
        Add function engine.
        """
        engine = engine_class(self, self.event_engine)
        self.engines[engine.engine_name] = engine
        return engine

    def add_gateway(self, gateway_class: Type[BaseGateway], gateway_name: str = None) -> BaseGateway:
        """
        Add gateway.
        """
        if gateway_name:
            # 使用指定的gateway_name， 可以区分相同接口不同账号的gateway同时接入
            gateway = gateway_class(self.event_engine, gateway_name=gateway_name)
        else:
            # 缺省使用了接口自己定义的gateway_name
            gateway = gateway_class(self.event_engine)
            gateway_name = gateway.gateway_name
        self.write_log(f'添加{gateway_name}网关')
        self.gateways[gateway_name] = gateway

        # Add gateway supported exchanges into engine
        for exchange in gateway.exchanges:
            if exchange not in self.exchanges:
                self.exchanges.append(exchange)

        return gateway

    def add_app(self, app_class: Type[BaseApp]) -> "BaseEngine":
        """
        Add app.
        """
        app = app_class()
        self.apps[app.app_name] = app

        engine = self.add_engine(app.engine_class)
        if app.app_name == "RiskManager":
            self.rm_engine = engine
        elif app.app_name == "AlgoTrading":
            self.algo_engine = engine
        elif app.app_name == 'RpcService':
            self.rpc_service = engine

        return engine

    def init_engines(self) -> None:
        """
        Init all engines.
        """
        self.add_engine(LogEngine)
        self.add_engine(OmsEngine)
        self.add_engine(EmailEngine)

    def write_log(self, msg: str, source: str = "") -> None:
        """
        Put log event with specific message.
        """
        log = LogData(msg=msg, gateway_name=source)
        event = Event(EVENT_LOG, log)
        self.event_engine.put(event)

    def write_error(self, msg: str, source: str = "") -> None:
        """
        Put log event with specific message.
        """
        log = LogData(msg=msg, gateway_name=source)
        event = Event(EVENT_LOG, log)
        self.event_engine.put(event)

        print(msg, file=sys.stderr)

    def get_gateway(self, gateway_name: str) -> BaseGateway:
        """
        Return gateway object by name.
        """
        gateway = self.gateways.get(gateway_name, None)
        if not gateway:
            # 增加兼容得写法，如果没有输入gateway_name，但当前只有一个gateway时，就使用当前gateway
            if len(self.gateways.keys()) == 1:
                return list(self.gateways.values())[0]

            self.write_error(f"在{self.gateways.keys()}中找不到底层接口：{gateway_name}")
        return gateway

    def get_engine(self, engine_name: str) -> "BaseEngine":
        """
        Return engine object by name.
        """
        engine = self.engines.get(engine_name, None)
        if not engine:
            self.write_error(f"找不到引擎：{engine_name}")
        return engine

    def get_default_setting(self, gateway_name: str) -> Optional[Dict[str, Any]]:
        """
        Get default setting dict of a specific gateway.
        """
        gateway = self.get_gateway(gateway_name)
        if gateway:
            return gateway.get_default_setting()
        else:
            self.write_error(f'获取网格设置时，找不到{gateway_name}')
        return None

    def get_all_gateway_names(self) -> List[str]:
        """
        Get all names of gatewasy added in main engine.
        """
        return list(self.gateways.keys())

    def get_all_gateway_status(self) -> List[dict]:
        """
        Get all gateway status
        :return:
        """
        return list([{k: v.get_status()} for k, v in self.gateways.items()])

    def get_all_apps(self) -> List[BaseApp]:
        """
        Get all app objects.
        """
        return list(self.apps.values())

    def get_all_exchanges(self) -> List[Exchange]:
        """
        Get all exchanges.
        """
        return self.exchanges

    def connect(self, setting: dict, gateway_name: str) -> None:
        """
        Start connection of a specific gateway.
        """
        gateway = self.get_gateway(gateway_name)
        if gateway:
            try:
                gateway.connect(setting)
            except Exception as ex:
                msg = f'gateway:{gateway_name}启动连接失败:{str(ex)}'
                self.write_log(msg=msg)
        else:
            self.write_error(f'连接{gateway_name}时，系统找不到{gateway_name}')

    def subscribe(self, req: SubscribeRequest, gateway_name: str) -> None:
        """
        Subscribe tick data update of a specific gateway.
        如果没有指定gateway，那么所有的gateway都会接收改订阅请求
        """
        if gateway_name:
            gateway = self.get_gateway(gateway_name)
            if gateway:
                gateway.subscribe(req)
            else:
                self.write_error(f'订阅合约时，找不到{gateway_name}')
        else:
            for gateway in self.gateways.values():
                if gateway:
                    gateway.subscribe(req)

    def send_order(self, req: OrderRequest, gateway_name: str) -> str:
        """
        Send new order request to a specific gateway.
        扩展支持自定义套利合约。 由cta_strategy_pro发出算法单委托，由算法引擎进行处理
        """
        # 自定义套利合约，交给算法引擎处理
        if self.algo_engine and req.exchange == Exchange.SPD:
            return self.algo_engine.send_spd_order(
                req=req,
                gateway_name=gateway_name)

        gateway = self.get_gateway(gateway_name)
        if gateway:
            return gateway.send_order(req)
        else:
            self.write_error(f'发送委托时，找不到{gateway_name}')
            return ""

    def cancel_order(self, req: CancelRequest, gateway_name: str) -> bool:
        """
        Send cancel order request to a specific gateway.
        """
        # 自定义套利合约，交给算法引擎处理
        if self.algo_engine and req.exchange == Exchange.SPD:
            return self.algo_engine.cancel_spd_order(
                req=req)

        gateway = self.get_gateway(gateway_name)
        if gateway:
            return gateway.cancel_order(req)
        else:
            self.write_error(f'撤单时，找不到{gateway_name}')
        return False

    def send_orders(self, reqs: Sequence[OrderRequest], gateway_name: str) -> List[str]:
        """
        批量发单
        """
        gateway = self.get_gateway(gateway_name)
        if gateway:
            return gateway.send_orders(reqs)
        else:
            self.write_error(f'批量发单时，找不到{gateway_name}')
            return ["" for req in reqs]

    def cancel_orders(self, reqs: Sequence[CancelRequest], gateway_name: str) -> None:
        """
        """
        gateway = self.get_gateway(gateway_name)
        if gateway:
            gateway.cancel_orders(reqs)
        else:
            self.write_error(f'批量撤单时，找不到{gateway_name}')

    def query_history(self, req: HistoryRequest, gateway_name: str) -> Optional[List[BarData]]:
        """
        Send cancel order request to a specific gateway.
        """
        gateway = self.get_gateway(gateway_name)
        if gateway:
            return gateway.query_history(req)
        else:
            self.write_error(f'找不到网关{gateway_name}，请检查合约得网关是否与连接得网关一致')
            return None

    def close(self) -> None:
        """
        Make sure every gateway and app is closed properly before
        programme exit.
        """
        if hasattr(self, 'save_contracts'):
            self.save_contracts()

        # Stop event engine first to prevent new timer event.
        self.event_engine.stop()

        for engine in self.engines.values():
            engine.close()

        for gateway in self.gateways.values():
            gateway.close()


class BaseEngine(ABC):
    """
    Abstract class for implementing an function engine.
    """

    def __init__(
            self,
            main_engine: MainEngine,
            event_engine: EventEngine,
            engine_name: str,
    ):
        """"""
        self.main_engine = main_engine
        self.event_engine = event_engine
        self.engine_name = engine_name

        self.logger = None
        self.create_logger(engine_name)

    def create_logger(self, logger_name: str = 'base_engine'):
        """
        创建engine独有的日志
        :param logger_name: 日志名，缺省为engine的名称
        :return:
        """
        log_path = get_folder_path("log")
        log_filename = str(log_path.joinpath(logger_name))
        print(u'create logger:{}'.format(log_filename))
        self.logger = setup_logger(file_name=log_filename, name=logger_name,
                                   log_level=SETTINGS.get('log.level', logging.DEBUG))

    def write_log(self, msg: str, source: str = "", level: int = logging.DEBUG):
        """
        写入日志
        :param msg: 日志内容
        :param source: 来源
        :param level: 日志级别
        :return:
        """
        if self.logger:
            if len(source) > 0:
                msg = f'[{source}]{msg}'
            self.logger.log(level, msg)
        else:
            log = LogData(msg=msg, level=level, gateway_name='')
            event = Event(EVENT_LOG, log)
            self.event_engine.put(event)

    def close(self):
        """"""
        pass


class LogEngine(BaseEngine):
    """
    Processes log event and output with logging module.
    """

    def __init__(self, main_engine: MainEngine, event_engine: EventEngine):
        """"""
        super(LogEngine, self).__init__(main_engine, event_engine, "log")

        if not SETTINGS["log.active"]:
            return

        self.level: int = SETTINGS["log.level"]

        self.logger: Logger = logging.getLogger("VN Trader")
        self.logger.setLevel(self.level)

        self.formatter = logging.Formatter(
            "%(asctime)s  %(levelname)s: %(message)s"
        )

        self.add_null_handler()

        if SETTINGS["log.console"]:
            self.add_console_handler()

        if SETTINGS["log.file"]:
            self.add_file_handler()

        self.register_event()

    def add_null_handler(self) -> None:
        """
        Add null handler for logger.
        """
        null_handler = logging.NullHandler()
        self.logger.addHandler(null_handler)

    def add_console_handler(self) -> None:
        """
        Add console output of log.
        """
        console_handler = logging.StreamHandler()
        console_handler.setLevel(self.level)
        console_handler.setFormatter(self.formatter)
        self.logger.addHandler(console_handler)

    def add_file_handler(self) -> None:
        """
        Add file output of log.
        """
        today_date = datetime.now().strftime("%Y%m%d")
        filename = f"vt_{today_date}.log"
        log_path = get_folder_path("log")
        file_path = log_path.joinpath(filename)

        file_handler = logging.FileHandler(
            file_path, mode="a", encoding="utf8"
        )
        file_handler.setLevel(self.level)
        file_handler.setFormatter(self.formatter)
        self.logger.addHandler(file_handler)

    def register_event(self) -> None:
        """"""
        self.event_engine.register(EVENT_LOG, self.process_log_event)

    def process_log_event(self, event: Event) -> None:
        """
        Process log event.
        """
        log = event.data
        self.logger.log(log.level, log.msg)


class OmsEngine(BaseEngine):
    """
    Provides order management system function for VN Trader.
    """

    def __init__(self, main_engine: MainEngine, event_engine: EventEngine):
        """"""
        super(OmsEngine, self).__init__(main_engine, event_engine, "oms")

        self.ticks: Dict[str, TickData] = {}
        self.orders: Dict[str, OrderData] = {}
        self.trades: Dict[str, TradeData] = {}
        self.positions: Dict[str, PositionData] = {}
        self.accounts: Dict[str, AccountData] = {}
        self.contracts: Dict[str, ContractData] = {}
        self.today_contracts: Dict[str, ContractData] = {}

        # 自定义合约
        self.custom_contracts = {}  # vt_symbol: ContractData
        self.custom_settings = {}  # symbol: dict
        self.symbol_spd_maping = {}  # symbol: [spd_symbol]

        self.prices = {}

        self.active_orders: Dict[str, OrderData] = {}

        self.add_function()
        self.register_event()
        self.load_contracts()

    def __del__(self):
        """保存缓存"""
        self.save_contracts()

    def load_contracts(self) -> None:
        """从本地缓存加载合约字典"""
        import bz2
        import pickle
        contract_file_name = 'vn_contract.pkb2'
        if os.path.exists(contract_file_name):
            try:
                with bz2.BZ2File(contract_file_name, 'rb') as f:
                    self.contracts = pickle.load(f)
                    self.write_log(f'加载缓存合约字典:{contract_file_name}')
            except Exception as ex:
                self.write_log(f'加载缓存合约异常:{str(ex)}')

        # 更新自定义合约
        custom_contracts = self.get_all_custom_contracts()
        self.get_all_custom_contracts(rtn_setting=True)
        for contract in custom_contracts.values():

            # 更新合约缓存
            self.contracts.update({contract.symbol: contract})
            self.contracts.update({contract.vt_symbol: contract})
            self.today_contracts[contract.vt_symbol] = contract
            self.today_contracts[contract.symbol] = contract

            # 获取自定义合约的主动腿/被动腿
            setting = self.custom_settings.get(contract.symbol, {})
            leg1_symbol = setting.get('leg1_symbol')
            leg2_symbol = setting.get('leg2_symbol')

            # 构建映射关系
            for symbol in [leg1_symbol, leg2_symbol]:
                spd_mapping_list = self.symbol_spd_maping.get(symbol, [])

                # 更新映射 symbol => spd_symbol
                if (not contract.symbol.endswith('.SPD')) and contract.symbol not in spd_mapping_list:
                    spd_mapping_list.append(contract.symbol)
                    self.symbol_spd_maping.update({symbol: spd_mapping_list})

    def save_contracts(self) -> None:
        """持久化合约对象到缓存文件"""
        import bz2
        import pickle
        contract_file_name = 'vn_contract.pkb2'
        with bz2.BZ2File(contract_file_name, 'wb') as f:
            if len(self.today_contracts) > 0:
                self.write_log(f'保存今日合约对象到缓存文件')
                pickle.dump(self.today_contracts, f)
            else:
                pickle.dump(self.contracts, f)

    def add_function(self) -> None:
        """Add query function to main engine."""
        self.main_engine.get_tick = self.get_tick
        self.main_engine.get_order = self.get_order
        self.main_engine.get_price = self.get_price
        self.main_engine.get_trade = self.get_trade
        self.main_engine.get_position = self.get_position
        self.main_engine.get_account = self.get_account
        self.main_engine.get_contract = self.get_contract
        self.main_engine.get_exchange = self.get_exchange
        self.main_engine.get_custom_contract = self.get_custom_contract
        self.main_engine.get_all_ticks = self.get_all_ticks
        self.main_engine.get_all_orders = self.get_all_orders
        self.main_engine.get_all_trades = self.get_all_trades
        self.main_engine.get_all_positions = self.get_all_positions
        self.main_engine.get_all_accounts = self.get_all_accounts
        self.main_engine.get_all_contracts = self.get_all_contracts
        self.main_engine.get_all_active_orders = self.get_all_active_orders
        self.main_engine.get_all_custom_contracts = self.get_all_custom_contracts
        self.main_engine.get_mapping_spd = self.get_mapping_spd
        self.main_engine.save_contracts = self.save_contracts

    def register_event(self) -> None:
        """"""
        self.event_engine.register(EVENT_TICK, self.process_tick_event)
        self.event_engine.register(EVENT_ORDER, self.process_order_event)
        self.event_engine.register(EVENT_TRADE, self.process_trade_event)
        self.event_engine.register(EVENT_POSITION, self.process_position_event)
        self.event_engine.register(EVENT_ACCOUNT, self.process_account_event)
        self.event_engine.register(EVENT_CONTRACT, self.process_contract_event)

    def process_tick_event(self, event: Event) -> None:
        """"""
        tick = event.data
        self.ticks[tick.vt_symbol] = tick

        if '&' not in tick.symbol and tick.last_price:
            self.prices[tick.vt_symbol] = tick.last_price

    def process_order_event(self, event: Event) -> None:
        """"""
        order = event.data
        self.orders[order.vt_orderid] = order

        # If order is active, then update data in dict.
        if order.is_active():
            self.active_orders[order.vt_orderid] = order
        # Otherwise, pop inactive order from in dict
        elif order.vt_orderid in self.active_orders:
            self.active_orders.pop(order.vt_orderid)

    def process_trade_event(self, event: Event) -> None:
        """"""
        trade = event.data
        self.trades[trade.vt_tradeid] = trade

    def process_position_event(self, event: Event) -> None:
        """"""
        position = event.data
        self.positions[position.vt_positionid] = position

        if position.exchange != Exchange.SPD:
            self.create_spd_position_event(position.symbol, position.direction)

    def reverse_direction(self, direction):
        """返回反向持仓"""
        if direction == Direction.LONG:
            return Direction.SHORT
        elif direction == Direction.SHORT:
            return Direction.LONG
        return direction

    def create_spd_position_event(self, symbol, direction):
        """创建自定义品种对持仓信息"""
        spd_symbols = self.symbol_spd_maping.get(symbol, [])
        if not spd_symbols:
            return
        for spd_symbol in spd_symbols:
            spd_setting = self.custom_settings.get(spd_symbol, None)
            if not spd_setting:
                continue

            leg1_symbol = spd_setting.get('leg1_symbol')
            leg2_symbol = spd_setting.get('leg2_symbol')
            leg1_contract = self.contracts.get(leg1_symbol)
            leg2_contract = self.contracts.get(leg2_symbol)
            spd_contract = self.contracts.get(spd_symbol)

            if leg1_contract is None or leg2_contract is None:
                continue
            leg1_ratio = spd_setting.get('leg1_ratio', 1)
            leg2_ratio = spd_setting.get('leg2_ratio', 1)

            # 找出leg1，leg2的持仓，并判断出spd的方向
            spd_pos = None
            if leg1_symbol == symbol:
                k1 = f"{leg1_contract.gateway_name}.{leg1_contract.vt_symbol}.{direction.value}"
                leg1_pos = self.positions.get(k1)
                k2 = f"{leg2_contract.gateway_name}.{leg2_contract.vt_symbol}.{self.reverse_direction(direction).value}"
                leg2_pos = self.positions.get(k2)
                spd_direction = direction
                k3 = f"{spd_contract.gateway_name}.{spd_symbol}.{Exchange.SPD.value}.{spd_direction.value}"
                spd_pos = self.positions.get(k3)
            elif leg2_symbol == symbol:
                k1 = f"{leg1_contract.gateway_name}.{leg1_contract.vt_symbol}.{self.reverse_direction(direction).value}"
                leg1_pos = self.positions.get(k1)
                k2 = f"{leg2_contract.gateway_name}.{leg2_contract.vt_symbol}.{direction.value}"
                leg2_pos = self.positions.get(k2)
                spd_direction = self.reverse_direction(direction)
                k3 = f"{spd_contract.gateway_name}.{spd_symbol}.{Exchange.SPD.value}.{spd_direction.value}"
                spd_pos = self.positions.get(k3)
            else:
                continue

            if leg1_pos is None or leg2_pos is None:  # or leg1_pos.volume ==0 or leg2_pos.volume == 0:
                continue

            # 根据leg1/leg2的volume ratio，计算出最小spd_volume
            spd_volume = min(int(leg1_pos.volume / leg1_ratio), int(leg2_pos.volume / leg2_ratio))
            if spd_volume <= 0 and spd_pos is None:
                continue

            if spd_setting.get('is_ratio', False) and leg2_pos.price > 0:
                spd_price = 100 * (leg2_pos.price * leg1_ratio) / (leg2_pos.price * leg2_ratio)
            elif spd_setting.get('is_spread', False):
                spd_price = leg1_pos.price * leg1_ratio - leg2_pos.price * leg2_ratio
            else:
                spd_price = 0

            spd_pos = PositionData(
                gateway_name=spd_contract.gateway_name,
                accountid=leg1_pos.accountid,
                symbol=spd_symbol,
                exchange=Exchange.SPD,
                direction=spd_direction,
                volume=spd_volume,
                price=spd_price
            )
            event = Event(EVENT_POSITION, data=spd_pos)
            self.event_engine.put(event)

    def process_account_event(self, event: Event) -> None:
        """"""
        account = event.data
        self.accounts[account.vt_accountid] = account

    def process_contract_event(self, event: Event) -> None:
        """"""
        contract = event.data
        self.contracts[contract.vt_symbol] = contract
        self.contracts[contract.symbol] = contract
        self.today_contracts[contract.vt_symbol] = contract
        self.today_contracts[contract.symbol] = contract

    def get_exchange(self, symbol: str) -> Exchange:
        """获取合约对应的交易所"""
        contract = self.contracts.get(symbol, None)
        if contract is None:
            return Exchange.LOCAL
        return contract.exchange

    def get_tick(self, vt_symbol: str) -> Optional[TickData]:
        """
        Get latest market tick data by vt_symbol.
        """
        return self.ticks.get(vt_symbol, None)

    def get_price(self, vt_symbol):
        """
        get the lastest price by vt_symbol
        :param vt_symbol:
        :return:
        """
        return self.prices.get(vt_symbol, None)

    def get_order(self, vt_orderid) -> Optional[OrderData]:
        """
        Get latest order data by vt_orderid.
        """
        return self.orders.get(vt_orderid, None)

    def get_trade(self, vt_tradeid: str) -> Optional[TradeData]:
        """
        Get trade data by vt_tradeid.
        """
        return self.trades.get(vt_tradeid, None)

    def get_position(self, vt_positionid: str) -> Optional[PositionData]:
        """
        Get latest position data by vt_positionid.
        """
        return self.positions.get(vt_positionid, None)

    def get_account(self, vt_accountid: str) -> Optional[AccountData]:
        """
        Get latest account data by vt_accountid.
        """
        return self.accounts.get(vt_accountid, None)

    def get_contract(self, vt_symbol: str) -> Optional[ContractData]:
        """
        Get contract data by vt_symbol.
        """
        return self.contracts.get(vt_symbol, None)

    def get_all_ticks(self) -> List[TickData]:
        """
        Get all tick data.
        """
        return list(self.ticks.values())

    def get_all_orders(self) -> List[OrderData]:
        """
        Get all order data.
        """
        return list(self.orders.values())

    def get_all_trades(self) -> List[TradeData]:
        """
        Get all trade data.
        """
        return list(self.trades.values())

    def get_all_positions(self) -> List[PositionData]:
        """
        Get all position data.
        """
        return list(self.positions.values())

    def get_all_accounts(self) -> List[AccountData]:
        """
        Get all account data.
        """
        return list(self.accounts.values())

    def get_all_contracts(self) -> List[ContractData]:
        """
        Get all contract data.
        """
        return list(self.contracts.values())

    def get_all_active_orders(self, vt_symbol: str = "") -> List[OrderData]:
        """
        Get all active orders by vt_symbol.

        If vt_symbol is empty, return all active orders.
        """
        if not vt_symbol:
            return list(self.active_orders.values())
        else:
            active_orders = [
                order
                for order in self.active_orders.values()
                if order.vt_symbol == vt_symbol
            ]
            return active_orders

    def get_custom_contract(self, symbol):
        """
        获取自定义合约的设置
        :param symbol:    "pb2012-1-pb2101-1-CJ"
        :return: {
        "name": "pb跨期价差",
        "exchange": "SPD",
        "leg1_symbol": "pb2012",
        "leg1_exchange": "SHFE",
        "leg1_ratio": 1,
        "leg2_symbol": "pb2101",
        "leg2_exchange": "SHFE",
        "leg2_ratio": 1,
        "is_spread": true,
        "size": 1,
        "margin_rate": 0.1,
        "price_tick": 5
        }
        """
        return self.custom_settings.get(symbol, None)

    def get_all_custom_contracts(self, rtn_setting=False):
        """
        获取所有自定义合约
        :return:
        """
        if rtn_setting:
            if len(self.custom_settings) == 0:
                c = CustomContract()
                self.custom_settings = c.get_config()
            return self.custom_settings

        if len(self.custom_contracts) == 0:
            c = CustomContract()
            self.custom_settings = c.get_config()
            self.custom_contracts = c.get_contracts()
        return self.custom_contracts

    def get_mapping_spd(self, symbol):
        """根据主动腿/被动腿symbol，获取自定义套利对的symbol list"""
        return self.symbol_spd_maping.get(symbol, [])


class CustomContract(object):
    """
    定制合约
    # 适用于初始化系统时，补充到本地合约信息文件中 contracts.vt
    # 适用于CTP网关，加载自定义的套利合约，做内部行情撮合
    """
    # 运行本地目录下，定制合约的配置文件（dict）
    file_name = 'custom_contracts.json'

    def __init__(self):
        """构造函数"""
        from vnpy.trader.utility import load_json
        self.setting = load_json(self.file_name)  # 所有设置

    def get_config(self):
        """获取配置"""
        return self.setting

    def get_contracts(self):
        """获取所有合约信息"""
        d = {}
        from vnpy.trader.object import ContractData, Exchange
        for symbol, setting in self.setting.items():
            gateway_name = setting.get('gateway_name', None)
            if gateway_name is None:
                gateway_name = SETTINGS.get('gateway_name', '')
            vn_exchange = Exchange(setting.get('exchange', 'SPD'))
            contract = ContractData(
                gateway_name=gateway_name,
                symbol=symbol,
                exchange=vn_exchange,
                name=setting.get('name', symbol),
                size=setting.get('size', 100),
                product=None,
                pricetick=setting.get('price_tick', 0.01),
                margin_rate=setting.get('margin_rate', 0.1)
            )
            d[contract.vt_symbol] = contract

        return d


class EmailEngine(BaseEngine):
    """
    Provides email sending function for VN Trader.
    """

    def __init__(self, main_engine: MainEngine, event_engine: EventEngine):
        """"""
        super(EmailEngine, self).__init__(main_engine, event_engine, "email")

        self.thread: Thread = Thread(target=self.run)
        self.queue: Queue = Queue()
        self.active: bool = False

        self.main_engine.send_email = self.send_email

    def send_email(self, subject: str, content: str, receiver: str = "") -> None:
        """"""
        # Start email engine when sending first email.
        if not self.active:
            self.start()

        # Use default receiver if not specified.
        if not receiver:
            receiver = SETTINGS["email.receiver"]

        msg = EmailMessage()
        msg["From"] = SETTINGS["email.sender"]
        msg["To"] = receiver
        msg["Subject"] = subject
        msg.set_content(content)

        self.queue.put(msg)

    def run(self) -> None:
        """"""
        while self.active:
            try:
                msg = self.queue.get(block=True, timeout=1)

                with smtplib.SMTP_SSL(
                        SETTINGS["email.server"], SETTINGS["email.port"]
                ) as smtp:
                    smtp.login(
                        SETTINGS["email.username"], SETTINGS["email.password"]
                    )
                    smtp.send_message(msg)
            except Empty:
                pass

    def start(self) -> None:
        """"""
        self.active = True
        self.thread.start()

    def close(self) -> None:
        """"""
        if not self.active:
            return

        self.active = False
        self.thread.join()
