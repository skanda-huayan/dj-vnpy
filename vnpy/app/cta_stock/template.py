""""""
import os
import sys
import uuid
import bz2
import pickle
import traceback
import zlib
import json
from abc import ABC
from copy import copy,deepcopy
from typing import Any, Callable, List, Dict
from logging import INFO, ERROR
from datetime import datetime,timedelta
from vnpy.trader.constant import Interval, Direction, Offset, Status, OrderType, Exchange, Color
from vnpy.trader.object import BarData, TickData, OrderData, TradeData, PositionData
from vnpy.trader.utility import virtual, append_data, extract_vt_symbol, get_underlying_symbol, round_to

from .base import StopOrder,EngineType
from vnpy.component.cta_grid_trade import CtaGrid, CtaGridTrade
from vnpy.component.cta_position import CtaPosition
from vnpy.component.cta_policy import CtaPolicy
from vnpy.component.base import MyEncoder

class CtaTemplate(ABC):
    """CTA股票策略模板"""

    author = "华富资产"
    parameters = []
    variables = []

    # 保存委托单编号和相关委托单的字典
    # key为委托单编号
    # value为该合约相关的委托单
    active_orders = {}

    def __init__(
            self,
            cta_engine: Any,
            strategy_name: str,
            vt_symbols: List[str],
            setting: dict,
    ):
        """"""
        self.cta_engine = cta_engine
        self.strategy_name = strategy_name
        self.vt_symbols = vt_symbols

        self.inited = False  # 是否初始化完毕
        self.trading = False  # 是否开始交易
        self.positions = {}  # 持仓，vt_symbol: position data
        self.entrust = 0  # 是否正在委托, 0, 无委托 , 1, 委托方向是LONG， -1, 委托方向是SHORT

        self.tick_dict = {}  # 记录所有on_tick传入最新tick
        self.active_orders = {}
        # Copy a new variables list here to avoid duplicate insert when multiple
        # strategy instances are created with the same strategy class.
        self.variables = copy(self.variables)
        self.variables.insert(0, "inited")
        self.variables.insert(1, "trading")
        self.variables.insert(2, "entrust")

    def update_setting(self, setting: dict):
        """
        Update strategy parameter wtih value in setting dict.
        """
        for name in self.parameters:
            if name in setting:
                setattr(self, name, setting[name])

    @classmethod
    def get_class_parameters(cls):
        """
        Get default parameters dict of strategy class.
        """
        class_parameters = {}
        for name in cls.parameters:
            class_parameters[name] = getattr(cls, name)
        return class_parameters

    def get_parameters(self):
        """
        Get strategy parameters dict.
        """
        strategy_parameters = {}
        for name in self.parameters:
            strategy_parameters[name] = getattr(self, name)
        return strategy_parameters

    def get_variables(self):
        """
        Get strategy variables dict.
        """
        strategy_variables = {}
        for name in self.variables:
            strategy_variables[name] = getattr(self, name)
        return strategy_variables

    def get_data(self):
        """
        Get strategy data.
        """
        strategy_data = {
            "strategy_name": self.strategy_name,
            "vt_symbols": self.vt_symbols,
            "class_name": self.__class__.__name__,
            "author": self.author,
            "parameters": self.get_parameters(),
            "variables": self.get_variables(),
        }
        return strategy_data

    def get_positions(self):
        """ 返回持仓数量"""
        pos_list = []
        for k, v in self.positions.items():
            pos_list.append({
                "vt_symbol": k,
                "direction": "long",
                "volume": v.volume,
                "price": v.price,
                'pnl': v.pnl
            })

        if len(pos_list) > 0:
            self.write_log(f'策略返回持仓信息:{pos_list}')
        return pos_list

    @virtual
    def on_timer(self):
        pass

    @virtual
    def on_init(self):
        """
        Callback when strategy is inited.
        """
        pass

    @virtual
    def on_start(self):
        """
        Callback when strategy is started.
        """
        pass

    @virtual
    def on_stop(self):
        """
        Callback when strategy is stopped.
        """
        pass

    @virtual
    def on_tick(self, tick_dict: Dict[str, TickData]):
        """
        Callback of new tick data update.
        """
        pass

    @virtual
    def on_bar(self, bar_dict: Dict[str, BarData]):
        """
        Callback of new bar data update.
        """
        pass

    @virtual
    def on_trade(self, trade: TradeData):
        """
        Callback of new trade data update.
        """
        pass

    @virtual
    def on_order(self, order: OrderData):
        """
        Callback of new order data update.
        """
        pass

    @virtual
    def on_stop_order(self, stop_order: StopOrder):
        """
        Callback of stop order update.
        """
        pass

    def before_trading(self):
        """开盘前/初始化后调用一次"""
        self.write_log('开盘前调用')

    def after_trading(self):
        """收盘后调用一次"""
        self.write_log('收盘后调用')

    def buy(self, price: float, volume: float, stop: bool = False,
            vt_symbol: str = '', order_type: OrderType = OrderType.LIMIT,
            order_time: datetime = None, grid: CtaGrid = None):
        """
        Send buy order to open a long position.
        """
        if order_type in [OrderType.FAK, OrderType.FOK]:
            if self.is_upper_limit(vt_symbol):
                self.write_error(u'涨停价不做FAK/FOK委托')
                return []
        return self.send_order(vt_symbol=vt_symbol,
                               direction=Direction.LONG,
                               offset=Offset.OPEN,
                               price=price,
                               volume=volume,
                               stop=stop,
                               order_type=order_type,
                               order_time=order_time,
                               grid=grid)

    def sell(self, price: float, volume: float, stop: bool = False,
             vt_symbol: str = '', order_type: OrderType = OrderType.LIMIT,
             order_time: datetime = None, grid: CtaGrid = None):
        """
        Send sell order to close a long position.
        """
        if order_type in [OrderType.FAK, OrderType.FOK]:
            if self.is_lower_limit(vt_symbol):
                self.write_error(u'跌停价不做FAK/FOK sell委托')
                return []
        return self.send_order(vt_symbol=vt_symbol,
                               direction=Direction.SHORT,
                               offset=Offset.CLOSE,
                               price=price,
                               volume=volume,
                               stop=stop,
                               order_type=order_type,
                               order_time=order_time,
                               grid=grid)

    def send_order(
            self,
            vt_symbol: str,
            direction: Direction,
            offset: Offset,
            price: float,
            volume: float,
            stop: bool = False,
            order_type: OrderType = OrderType.LIMIT,
            order_time: datetime = None,
            grid: CtaGrid = None
    ):
        """
        Send a new order.
        """
        if vt_symbol == '':
           return []

        if not self.trading:
            return []

        vt_orderids = self.cta_engine.send_order(
            strategy=self,
            vt_symbol=vt_symbol,
            direction=direction,
            offset=offset,
            price=price,
            volume=volume,
            stop=stop,
            order_type=order_type
        )

        if order_time is None:
            order_time = datetime.now()

        for vt_orderid in vt_orderids:
            d = {
                'direction': direction,
                'offset': offset,
                'vt_symbol': vt_symbol,
                'price': price,
                'volume': volume,
                'order_type': order_type,
                'traded': 0,
                'order_time': order_time,
                'status': Status.SUBMITTING
            }
            if grid:
                d.update({'grid': grid})
                if len(vt_orderid) > 0:
                    grid.order_ids.append(vt_orderid)
                grid.order_time = order_time
            self.active_orders.update({vt_orderid: d})
        if direction == Direction.LONG:
            self.entrust = 1
        elif direction == Direction.SHORT:
            self.entrust = -1
        return vt_orderids

    def cancel_order(self, vt_orderid: str):
        """
        Cancel an existing order.
        """
        if self.trading:
            return self.cta_engine.cancel_order(self, vt_orderid)

        return False

    def cancel_all(self):
        """
        Cancel all orders sent by strategy.
        """
        if self.trading:
            self.cta_engine.cancel_all(self)

    def is_upper_limit(self, symbol):
        """是否涨停"""
        tick = self.tick_dict.get(symbol, None)
        if tick is None or tick.limit_up is None or tick.limit_up == 0:
            return False
        if tick.bid_price_1 == tick.limit_up:
            return True

    def is_lower_limit(self, symbol):
        """是否跌停"""
        tick = self.tick_dict.get(symbol, None)
        if tick is None or tick.limit_down is None or tick.limit_down == 0:
            return False
        if tick.ask_price_1 == tick.limit_down:
            return True

    def write_log(self, msg: str, level: int = INFO):
        """
        Write a log message.
        """
        self.cta_engine.write_log(msg=msg, strategy_name=self.strategy_name, level=level)

    def write_error(self, msg: str):
        """write error log message"""
        self.write_log(msg=msg, level=ERROR)

    def get_engine_type(self):
        """
        Return whether the cta_engine is backtesting or live trading.
        """
        return self.cta_engine.get_engine_type()

    def put_event(self):
        """
        Put an strategy data event for ui update.
        """
        if self.inited:
            self.cta_engine.put_strategy_event(self)

    def send_email(self, msg):
        """
        Send email to default receiver.
        """
        if self.inited:
            self.cta_engine.send_email(msg, self)

    def sync_data(self):
        """
        Sync strategy variables value into disk storage.
        """
        if self.trading:
            self.cta_engine.sync_strategy_data(self)

class StockPolicy(CtaPolicy):

    def __init__(self, strategy):
        super().__init__(strategy)
        self.cur_trading_date = None    # 已执行pre_trading方法后更新的当前交易日
        self.signals = {}  # kline_name: { 'last_signal': '', 'last_signal_time': datetime }
        self.sub_tns = {}   # 子事务

    def from_json(self, json_data):
        """将数据从json_data中恢复"""
        super().from_json(json_data)

        self.cur_trading_date = json_data.get('cur_trading_date', None)
        self.sub_tns = json_data.get('sub_tns',{})
        signals = json_data.get('signals', {})
        for k, signal in signals.items():
            last_signal = signal.get('last_signal', "")
            str_ast_signal_time = signal.get('last_signal_time', "")
            try:
                if len(str_ast_signal_time) > 0:
                    last_signal_time = datetime.strptime(str_ast_signal_time, '%Y-%m-%d %H:%M:%S')
                else:
                    last_signal_time = None
            except Exception as ex:
                last_signal_time = None
            self.signals.update({k: {'last_signal': last_signal, 'last_signal_time': last_signal_time}})

    def to_json(self):
        """转换至json文件"""
        j = super().to_json()
        j['cur_trading_date'] = self.cur_trading_date
        j['sub_tns'] = self.sub_tns
        d = {}
        for kline_name, signal in self.signals.items():
            last_signal_time = signal.get('last_signal_time', None)
            c_signal = {}
            c_signal.update(signal)
            c_signal.update({'last_signal': signal.get('last_signal', ''),
                           'last_signal_time': last_signal_time.strftime(
                               '%Y-%m-%d %H:%M:%S') if last_signal_time is not None else ""
                           })
            d.update({kline_name: c_signal})
        j['signals'] = d
        return j


class CtaStockTemplate(CtaTemplate):
    """
    股票增强模板
    """

    # 委托类型
    order_type = OrderType.LIMIT
    cancel_seconds = 120  # 撤单时间(秒)

    # 资金相关
    max_invest_rate = 0.1  # 策略使用账号的最大仓位(0~1)
    max_invest_margin = 0  # 策略资金上限， 0，不限制
    max_single_margin = 0  # 策略内，各只股票使用的资金上限

    # 是否回测状态
    backtesting = False

    # 逻辑过程日志
    dist_fieldnames = ['datetime', 'symbol', 'name', 'volume', 'price',
                       'operation', 'signal', 'stop_price', 'target_price',
                       'long_pos']

    def __init__(self, cta_engine, strategy_name, vt_symbols, setting):
        """"""

        self.policy = None  # 事务执行组件
        self.gt = None  # 网格交易组件（使用了dn_grids，作为买入/持仓/卖出任务）
        self.klines = {}  # K线组件字典: kline_name: kline
        self.positions = {}     # 策略内持仓记录，  vt_symbol: PositionData
        self.order_type = OrderType.LIMIT
        self.cancel_seconds = 120  # 撤单时间(秒)

        # 资金相关
        self.max_invest_rate = 0.1  # 最大仓位(0~1)
        self.max_invest_margin = 0  # 资金上限 0，不限制

        # 是否回测状态
        self.backtesting = False

        self.cur_datetime: datetime = None  # 当前Tick时间
        self.last_minute = None  # 最后的分钟,用于on_tick内每分钟处理的逻辑

        super().__init__(
            cta_engine, strategy_name, vt_symbols, setting
        )

        self.policy = StockPolicy(self)  # 事务执行组件
        self.gt = CtaGridTrade(strategy=self)  # 网格持久化模块

        if 'backtesting' not in self.parameters:
            self.parameters.append('backtesting')

    def update_setting(self, setting: dict):
        """
        Update strategy parameter wtih value in setting dict.
        """
        for name in self.parameters:
            if name in setting:
                setattr(self, name, setting[name])

    def sync_data(self):
        """同步更新数据"""
        if not self.backtesting:
            self.write_log(u'保存k线缓存数据')
            self.save_klines_to_cache()

        if self.inited and self.trading:
            self.write_log(u'保存policy数据')
            self.policy.save()

    def save_klines_to_cache(self, kline_names: list = [], vt_symbol: str = ""):
        """
        保存K线数据到缓存
        :param kline_names: 一般为self.klines的keys
        :param vt_symbol: 指定股票代码,
            如果使用该选项，加载 data/klines/strategyname_vtsymbol_klines.pkb2
            如果空白，加载 data/strategyname_klines.pkb2
        :return:
        """
        if len(kline_names) == 0:
            kline_names = list(self.klines.keys())

        try:
            # 如果是指定合约的话，使用klines子目录
            if len(vt_symbol) > 0:
                kline_names = [n for n in kline_names if vt_symbol in n]
                save_path = os.path.abspath(os.path.join(self.cta_engine.get_data_path(), 'klines'))
                if not os.path.exists(save_path):
                    os.makedirs(save_path)
                file_name = os.path.abspath(os.path.join(save_path, f'{self.strategy_name}_{vt_symbol}_klines.pkb2'))
            else:
                # 获取保存路径
                save_path = self.cta_engine.get_data_path()
                # 保存缓存的文件名
                file_name = os.path.abspath(os.path.join(save_path, f'{self.strategy_name}_klines.pkb2'))

            with bz2.BZ2File(file_name, 'wb') as f:
                klines = {}
                for kline_name in kline_names:
                    kline = self.klines.get(kline_name, None)
                    # if kline:
                    #    kline.strategy = None
                    #    kline.cb_on_bar = None
                    klines.update({kline_name: kline})
                pickle.dump(klines, f)
            self.write_log(f'保存{vt_symbol} K线数据成功=>{file_name}')
        except Exception as ex:
            self.write_error(f'保存k线数据异常:{str(ex)}')
            self.write_error(traceback.format_exc())

    def load_klines_from_cache(self, kline_names: list = [], vt_symbol: str = ""):
        """
        从缓存加载K线数据
        :param kline_names: 指定需要加载的k线名称列表
        :param vt_symbol: 指定股票代码,
            如果使用该选项，加载 data/klines/strategyname_vtsymbol_klines.pkb2
            如果空白，加载 data/strategyname_klines.pkb2
        :return:
        """
        if len(kline_names) == 0:
            kline_names = list(self.klines.keys())

        # 如果是指定合约的话，使用klines子目录
        if len(vt_symbol) > 0:
            kline_names = [n for n in kline_names if vt_symbol in n]
            save_path = os.path.abspath(os.path.join(self.cta_engine.get_data_path(), 'klines'))
            if not os.path.exists(save_path):
                os.makedirs(save_path)
            file_name = os.path.abspath(os.path.join(save_path, f'{self.strategy_name}_{vt_symbol}_klines.pkb2'))
        else:
            save_path = self.cta_engine.get_data_path()
            file_name = os.path.abspath(os.path.join(save_path, f'{self.strategy_name}_klines.pkb2'))
        try:
            last_bar_dt = None
            with bz2.BZ2File(file_name, 'rb') as f:
                klines = pickle.load(f)
                # 逐一恢复K线
                for kline_name in kline_names:
                    # 缓存的k线实例
                    cache_kline = klines.get(kline_name, None)
                    # 当前策略实例的K线实例
                    strategy_kline = self.klines.get(kline_name, None)

                    if cache_kline and strategy_kline:
                        # 临时保存当前的回调函数
                        cb_on_bar = strategy_kline.cb_on_bar
                        # 缓存实例数据 =》 当前实例数据
                        strategy_kline.__dict__.update(cache_kline.__dict__)

                        # 所有K线的最后时间
                        if last_bar_dt and strategy_kline.cur_datetime:
                            last_bar_dt = max(last_bar_dt, strategy_kline.cur_datetime)
                        else:
                            last_bar_dt = strategy_kline.cur_datetime

                        # 重新绑定k线策略与on_bar回调函数
                        strategy_kline.strategy = self
                        strategy_kline.cb_on_bar = cb_on_bar

                        self.write_log(f'恢复{kline_name}缓存数据,最新bar结束时间:{last_bar_dt}')

                self.write_log(u'加载缓存k线数据完毕')
                return last_bar_dt
        except Exception as ex:
            self.write_error(f'加载缓存K线数据失败:{str(ex)}')
        return None

    def get_klines_info(self):
        """
        返回当前所有kline的信息
        :return: {"股票中文":[kline_name1, kline_name2]}
        """
        info = {}
        for kline_name in list(self.klines.keys()):
            # 策略中如果kline不是按照 vtsymbol_xxxx 的命名方式，需要策略内部自行实现方法
            vt_symbol = kline_name.split('_')[0]
            # vt_symbol => 中文名
            cn_name = self.cta_engine.get_name(vt_symbol)

            # 添加到列表 => 排序
            kline_names = info.get(cn_name, [])
            kline_names.append(kline_name)
            kline_names = sorted(kline_names)

            # 更新
            info[cn_name] = kline_names

        return info

    def get_klines_snapshot(self, include_kline_names=[]):
        """
        返回当前klines的切片数据
        :param include_kline_names: 如果存在，则只保留这些指定得K线
        :return:
        """
        try:
            self.write_log(f'获取{self.strategy_name}的切片数据')
            d = {
                'strategy': self.strategy_name,
                'datetime': datetime.now()}
            klines = {}
            for kline_name in sorted(self.klines.keys()):
                if len(include_kline_names) > 0:
                    if kline_name not in include_kline_names:
                        continue
                klines.update({kline_name: self.klines.get(kline_name).get_data()})
            kline_names = list(klines.keys())
            binary_data = zlib.compress(pickle.dumps(klines))
            d.update({'kline_names': kline_names, 'klines': binary_data, 'zlib': True})
            return d
        except Exception as ex:
            self.write_error(f'获取klines切片数据失败:{str(ex)}')
            return {}

    def init_policy(self):
        """初始化Policy"""
        self.write_log(u'init_policy(),初始化执行逻辑')
        self.policy.load()
        self.write_log('{}'.format(json.dumps(self.policy.to_json(),indent=2, ensure_ascii=False,cls=MyEncoder)))

    def init_position(self):
        """
        初始化Position
        使用网格的持久化，获取开仓状态的持仓，更新
        :return:
        """
        self.write_log(u'init_position(),初始化持仓')
        subscribe_symbols = []

        if len(self.gt.dn_grids) <= 0:
            # 加载已开仓的多数据，网格JSON
            long_grids = self.gt.load(direction=Direction.LONG, open_status_filter=[True, False])
            if len(long_grids) == 0:
                self.write_log(u'没有持久化的多单数据')
                self.gt.dn_grids = []
            else:
                self.gt.dn_grids = long_grids
                for lg in long_grids:
                    if len(lg.order_ids) > 0:
                        self.write_log(f'清除委托单：{lg.order_ids}')
                        [self.cta_engine.cancel_order(self, vt_orderid) for vt_orderid in lg.order_ids]
                        lg.order_ids = []

                    # 持仓
                    if lg.open_status and not lg.close_status and not lg.order_status and lg.volume > 0:
                        pos = self.get_position(lg.vt_symbol)
                        pos.price = round((pos.price * pos.volume + lg.open_price * lg.volume) / (pos.volume + lg.volume),3)
                        pos.volume += lg.volume
                        lg.traded_volume = 0
                        self.write_log(u'持仓状态，加载持仓多单[{},价格:{},数量:{}手, 开仓时间:{}'
                                       .format(lg.vt_symbol, lg.open_price, lg.volume, lg.open_time))
                        self.positions.update({lg.vt_symbol: pos})

                    # 部分开仓
                    elif lg.order_status and not lg.open_status and not lg.close_status and lg.traded_volume > 0:
                        pos = self.get_position(lg.vt_symbol)
                        pos.price = round(
                            (pos.price * pos.volume + lg.open_price * lg.traded_volume) / (pos.volume + lg.traded_volume), 3)
                        pos.volume += lg.traded_volume
                        self.write_log(u'开仓状态，加载部分持仓多单[{},价格:{},数量:{}手, 开仓时间:{}'
                                       .format(lg.vt_symbol, lg.open_price, lg.traded_volume, lg.open_time))
                        self.positions.update({lg.vt_symbol: pos})

                    # 正在平仓 =>
                    elif lg.order_status and lg.open_status and lg.close_status and lg.volume > 0:
                        if lg.traded_volume > 0:
                            old_volume = lg.volume
                            lg.volume -= lg.traded_volume
                            self.write_log(f'平仓状态，已成交:{lg.traded_volume} => 0 , 持仓:{old_volume}=>{lg.volume}')
                            lg.traded_volume = 0

                        pos = self.get_position(lg.vt_symbol)
                        pos.price = round(
                            (pos.price * pos.volume + lg.open_price * lg.volume) / (pos.volume + lg.volume), 3)
                        pos.volume += lg.volume
                        self.write_log(u'卖出状态，加载持仓多单[{},价格:{},数量:{}手, 开仓时间:{}'
                                       .format(lg.vt_symbol, lg.open_price, lg.volume, lg.open_time))

                        self.positions.update({lg.vt_symbol: pos})

                    if len(lg.vt_symbol) > 0 and lg.vt_symbol not in self.vt_symbols and lg.vt_symbol not in subscribe_symbols:
                        subscribe_symbols.append(lg.vt_symbol)

            for vt_symbol in subscribe_symbols:
                self.write_log(f'{vt_symbol}不在配置清单中，添加行情订阅')
                self.cta_engine.subscribe_symbol(strategy_name=self.strategy_name, vt_symbol=vt_symbol)

        self.gt.save()
        self.display_grids()

    def get_position(self, vt_symbol) -> PositionData:
        """
        获取策略某vt_symbol持仓()
        :return:
        """
        pos = self.positions.get(vt_symbol)
        if pos is None:
            symbol, exchange = extract_vt_symbol(vt_symbol)
            contract = self.cta_engine.get_contract(vt_symbol)
            pos = PositionData(
                gateway_name=contract.gateway_name if contract else '',
                symbol=symbol,
                exchange=exchange,
                direction=Direction.NET
            )
            self.positions.update({vt_symbol: pos})

        return pos

    def compare_pos(self,auto_balance=False):
        """比较仓位"""
        for vt_symbol, position in self.positions.items():
            name = self.cta_engine.get_name(vt_symbol)
            acc_pos = self.cta_engine.get_position(vt_symbol=vt_symbol, Direction=Direction.NET)
            if position.volume > 0:
                if not acc_pos:
                    self.write_error(f'账号中，没有{name}[{vt_symbol}]的持仓')
                    continue
                if acc_pos.volume < position.volume:
                    self.write_error(f'{name}[{vt_symbol}]的账号持仓{acc_pos} 小于策略持仓:{position.volume}')

    def before_trading(self, dt: datetime = None):
        """开盘前/初始化后调用一次"""
        self.write_log(f'{self.strategy_name}开盘前检查')

        self.compare_pos()

        if not self.backtesting:
            self.policy.cur_trading_date = datetime.strftime('%Y-%m-%d')
        else:
            if dt:
                self.policy.cur_trading_date = dt.strftime('%Y-%m-%d')


    def check_adjust(self, vt_symbol):
        """
        检查股票的最新除权时间，是否在一周内
        :param vt_symbol:
        :return: True: 一周内没有发生除权； False：一周内发生过除权
        """
        last_adjust_factor = self.cta_engine.get_adjust_factor(vt_symbol)
        if last_adjust_factor is None:
            return True
        last_adjust_date = last_adjust_factor.get('dividOperateDate', None)
        # 最后在除权出息日，发生在一周内
        if last_adjust_date and (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d') <= last_adjust_date:
            self.write_log(
                '{}[{}]发生除权除息，日期:{}'.format(vt_symbol, last_adjust_factor.get('name'), last_adjust_date))
            return False

        return True

    def after_trading(self):
        """收盘后调用一次"""
        self.write_log(f'{self.strategy_name}收盘后调用')
        self.compare_pos()

    def on_trade(self, trade: TradeData):
        """交易更新"""
        self.write_log(u'{},交易更新:{}'
                       .format(self.cur_datetime,
                               trade.__dict__))

        dist_record = dict()
        if self.backtesting:
            dist_record['datetime'] = trade.time
        else:
            dist_record['datetime'] = ' '.join([self.cur_datetime.strftime('%Y-%m-%d'), trade.time])
        dist_record['volume'] = trade.volume
        dist_record['price'] = trade.price
        dist_record['symbol'] = trade.vt_symbol
        pos = self.get_position(trade.vt_symbol)
        if trade.direction == Direction.LONG:
            dist_record['operation'] = 'buy'
            pos.volume += trade.volume

        if trade.direction == Direction.SHORT:
            dist_record['operation'] = 'sell'
            pos.volume -= trade.volume

        self.positions[trade.vt_symbol] = pos

        self.save_dist(dist_record)

    def on_order(self, order: OrderData):
        """报单更新"""
        # 未执行的订单中，存在是异常，删除
        self.write_log(u'{}报单更新，{}'.format(self.cur_datetime, order.__dict__))

        if order.vt_orderid in self.active_orders:

            if order.volume == order.traded and order.status in [Status.ALLTRADED]:
                self.on_order_all_traded(order)

            elif order.offset == Offset.OPEN and order.status in [Status.CANCELLED]:
                # 开仓委托单被撤销
                self.on_order_open_canceled(order)

            elif order.offset != Offset.OPEN and order.status in [Status.CANCELLED]:
                # 平仓委托单被撤销
                self.on_order_close_canceled(order)

            elif order.status == Status.REJECTED:
                if order.direction == Direction.LONG:
                    self.write_error(u'买入委托单被拒:{}，委托价:{},总量:{},已成交:{}，状态:{}'
                                     .format(order.vt_symbol, order.price, order.volume,
                                             order.traded, order.status))
                    self.on_order_open_canceled(order)
                else:
                    self.write_error(u'卖出委托单被拒:{}，委托价:{},总量:{},已成交:{}，状态:{}'
                                     .format(order.vt_symbol, order.price, order.volume,
                                             order.traded, order.status))
                    self.on_order_close_canceled(order)
            else:
                self.write_log(u'委托单未完成,{} 委托总量:{},已成交:{},委托状态:{}'
                               .format(order.vt_symbol, order.volume, order.traded, order.status))
        else:
            self.write_error(u'委托单{}不在策略的未完成订单列表中:{}'.format(order.vt_orderid, self.active_orders))

    def on_order_all_traded(self, order: OrderData):
        """
        订单全部成交
        :param order:
        :return:
        """
        self.write_log(u'{},委托单:{}, 状态: 全部完成'.format(order.time, order.vt_orderid))
        order_info = self.active_orders[order.vt_orderid]

        # 通过vt_orderid，找到对应的网格
        grid = order_info.get('grid', None)
        if grid is not None:
            # 移除当前委托单
            if order.vt_orderid in grid.order_ids:
                grid.order_ids.remove(order.vt_orderid)

            # 更新成交
            old_traded_volume = grid.traded_volume
            grid.traded_volume += order.volume
            grid.traded_volume = round(grid.traded_volume, 7)

            self.write_log(f'{order.vt_symbol}, 方向:{order.direction.value},{order.volume} 成交，'
                           + f'网格volume:{grid.volume}, traded_volume:{old_traded_volume}=>{grid.traded_volume}')
            if len(grid.order_ids) > 0:
                self.write_log(f'剩余委托单号:{grid.order_ids}')

            """
            # 网格的所有委托单已经执行完毕
            if grid.volume <= grid.traded_volume:
                grid.order_status = False
                if grid.volume < grid.traded_volume:
                    self.write_error(f'{order.vt_symbol} 已成交总量:{grid.traded_volume}超出{grid.volume}, 更新=>{grid.traded_volume}')
                    grid.volume = grid.traded_volume

                # 卖出完毕（sell）
                if order.direction != Direction.LONG:
                    grid.open_status = False
                    grid.close_status = True

                    self.write_log(f'卖出{order.vt_symbol}完毕，总量:{grid.volume},最后一笔委托价:{order.price}'
                                   + f',成交数量:{order.volume}')

                    self.write_log(f'移除网格:{grid.to_json()}')
                    self.gt.remove_grids_by_ids(direction=grid.direction, ids=[grid.id])

                # 开仓完毕( buy)
                else:
                    grid.open_status = True
                    grid.traded_volume = 0
                    grid.open_time = self.cur_datetime
                    self.write_log(f'买入{order.vt_symbol}完毕,总量:{grid.volume},最后一笔委托价:{order.price}'
                                   + f',成交:{order.volume}')
            """
            self.gt.save()

        # 在策略得活动订单中，移除
        self.write_log(f'移除活动订单:{order.vt_orderid}')
        self.active_orders.pop(order.vt_orderid, None)

    def on_order_open_canceled(self, order: OrderData):
        """
        委托开仓单撤销
        :param order:
        :return:
        """
        self.write_log(u'委托开仓单撤销:{}'.format(order.__dict__))

        if order.vt_orderid not in self.active_orders:
            self.write_error(u'{}不在未完成的委托单中{}。'.format(order.vt_orderid, self.active_orders))
            return

        old_order = self.active_orders[order.vt_orderid]
        self.write_log(u'{} 委托信息:{}'.format(order.vt_orderid, old_order))
        # 更新成交数量
        old_order['traded'] = order.traded
        # 获取订单对应的网格
        grid = old_order.get('grid', None)

        # 状态 =》 撤单
        pre_status = old_order.get('status', Status.NOTTRADED)
        old_order.update({'status': Status.CANCELLED})
        self.write_log(u'委托单状态:{}=>{}'.format(pre_status, old_order.get('status')))

        if grid:
            if order.vt_orderid in grid.order_ids:
                self.write_log(f'移除网格的开仓委托单:{order.vt_orderid}')
                grid.order_ids.remove(order.vt_orderid)

            if order.traded > 0:
                self.write_log(f'撤单中有成交，网格累计成交:{grid.traded_volume} => {grid.traded_volume + order.traded}')
                grid.traded_volume += order.traded

            self.gt.save()

        self.active_orders.update({order.vt_orderid: old_order})

        self.display_grids()

    def on_order_close_canceled(self, order: OrderData):
        """委托平仓单撤销"""
        self.write_log(u'委托平仓单撤销:{}'.format(order.__dict__))

        if order.vt_orderid not in self.active_orders:
            self.write_error(u'{}不在未完成的委托单中:{}。'.format(order.vt_orderid, self.active_orders))
            return

        # 更新
        old_order = self.active_orders[order.vt_orderid]
        self.write_log(u'{} 订单信息:{}'.format(order.vt_orderid, old_order))

        old_order['traded'] = order.traded
        grid = old_order.get('grid', None)

        pre_status = old_order.get('status', Status.NOTTRADED)
        old_order.update({'status': Status.CANCELLED})
        self.write_log(u'委托单状态:{}=>{}'.format(pre_status, old_order.get('status')))

        if grid:
            if order.vt_orderid in grid.order_ids:
                self.write_log(f'移除网格的平仓委托单:{order.vt_orderid}')
                grid.order_ids.remove(order.vt_orderid)

            if order.traded > 0:
                self.write_log(f'撤单中有成交，网格累计成交:{grid.traded_volume} => {grid.traded_volume + order.traded}')
                grid.traded_volume += order.traded

            self.gt.save()

        self.active_orders.update({order.vt_orderid: old_order})
        self.display_grids()

    def on_stop_order(self, stop_order: StopOrder):
        self.write_log(f'停止单触发:{stop_order.__dict__}')

    def grid_check_stop(self):
        """
        网格逐一止损/止盈检查 (根据指数价格进行止损止盈）
        :return:
        """
        #if self.entrust != 0:
        #    return

        if not self.trading and not self.inited:
            self.write_error(u'当前不允许交易')
            return

        remove_gids = []
        changed = False
        # 多单网格逐一止损/止盈检查：
        long_grids = self.gt.get_opened_grids(direction=Direction.LONG)
        for lg in long_grids:
            if lg.close_status or lg.order_status or not lg.open_status:
                continue

            cur_price = self.cta_engine.get_price(lg.vt_symbol)
            if cur_price is None:
                self.write_log(f'没有获取到{lg.vt_symbol}的当前价格，提交订阅')
                self.cta_engine.subscribe_symbol(strategy_name=self.strategy_name, vt_symbol=lg.vt_symbol)
                continue

            # 主动止盈
            if 0 < lg.close_price <= cur_price:
                cn_name = self.cta_engine.get_name(lg.vt_symbol)
                # 调用平仓模块
                self.write_log(u'{} {}[{}] 当前价:{} 触发止盈{},开仓价:{},v：{}'.
                               format(self.cur_datetime,
                                      lg.vt_symbol,
                                      cn_name,
                                      cur_price,
                                      lg.close_price,
                                      lg.open_price,
                                      lg.volume))
                changed = True
                if lg.traded_volume > 0:
                    lg.volume -= lg.traded_volume
                    lg.traded_volume = 0
                if lg.volume <= 0:
                    remove_gids.append(lg.id)
                    lg.open_status = False
                    lg.order_status = False
                    lg.close_status = False
                    continue

                lg.order_status = True
                lg.close_status = True
                self.write_log(f'{lg.vt_symbol}[{cn_name}] 数量:{lg.volume}，准备卖出')

                continue

            # 止损
            if lg.stop_price != 0 and lg.stop_price > cur_price > 0:
                cn_name = self.cta_engine.get_name(lg.vt_symbol)

                # 调用平仓模块
                self.write_log(u'{} {}[{}] 当前价:{} 触发止损线{},开仓价:{},v：{}'.
                               format(self.cur_datetime,
                                      lg.vt_symbol,
                                      cn_name,
                                      cur_price,
                                      lg.stop_price,
                                      lg.open_price,
                                      lg.volume))
                changed = True
                if lg.traded_volume > 0:
                    lg.volume -= lg.traded_volume
                    lg.traded_volume = 0
                if lg.volume <= 0:
                    remove_gids.append(lg.id)
                    lg.open_status = False
                    lg.order_status = False
                    lg.close_status = False
                    continue

                lg.order_status = True
                lg.close_status = True
                self.write_log(f'{lg.vt_symbol}[{cn_name}] 数量:{lg.volume}，准备卖出')

        if changed:
            if len(remove_gids) > 0:
                self.gt.remove_grids_by_ids(direction=Direction.LONG, ids=remove_gids)
            self.gt.save()

    def tns_excute_sell_grids(self, vt_symbol=None, force=False):
        """
        事务执行卖出网格
         1、找出所有order_status=True,open_status=Talse, close_status=True的网格。
        2、比对volume和traded volume, 如果两者得数量差，大于min_trade_volume，继续发单
        :return:
        """
        if not self.trading:
            return

        if self.cur_datetime and 9 <= self.cur_datetime.hour <= 14:
            if self.cur_datetime.hour == 12:
                return
            if self.cur_datetime.hour == 9 and self.cur_datetime.minute < 30:
                return
            if self.cur_datetime.hour == 11 and self.cur_datetime.minute >= 30:
                return

        ordering_grid = None
        for grid in self.gt.dn_grids:
            # 只扫描vt_symbol 匹配的网格
            if vt_symbol and vt_symbol != grid.vt_symbol:
                continue

            # 排除: 未开仓/非平仓/非委托的网格
            if not grid.open_status or not grid.close_status or not grid.open_status:
                continue

            # 排除存在委托单号的网格
            if len(grid.order_ids) > 0:
                continue

            if grid.volume <= grid.traded_volume:
                self.write_log(u'网格计划卖出:{}，已成交:{}'.format(grid.volume, grid.traded_volume))
                self.tns_finish_sell_grid(grid)
                continue

            # 定位到首个满足条件的网格
            ordering_grid = grid

            acc_symbol_pos = self.cta_engine.get_position(
                vt_symbol=ordering_grid.vt_symbol,
                direction=Direction.NET)
            if acc_symbol_pos is None:
                self.write_error(f'{self.strategy_name}当前{ordering_grid.vt_symbol}持仓查询不到, 无法执行卖出')
                continue

            vt_symbol = ordering_grid.vt_symbol
            sell_volume = ordering_grid.volume - ordering_grid.traded_volume

            if sell_volume > acc_symbol_pos.volume:
                if not force:
                    self.write_error(u'账号{}持仓{},不满足减仓目标:{}'
                                       .format(vt_symbol, acc_symbol_pos.volume, sell_volume))
                    continue
                else:
                    self.write_log(u'账号{}持仓{},不满足减仓目标:{}, 修正卖出数量:{}=>{}'
                                   .format(vt_symbol, acc_symbol_pos.volume, sell_volume, sell_volume,
                                           acc_symbol_pos.volume))
                    sell_volume = acc_symbol_pos.volume

            if sell_volume == 0:
                self.write_log(f'账号{vt_symbol}持仓{acc_symbol_pos.volume},卖出目标:{sell_volume}=0 不执行')
                continue

            cur_price = self.cta_engine.get_price(vt_symbol)
            if not cur_price:
                self.cta_engine.subscribe_symbol(strategy_name=self.strategy_name, vt_symbol=vt_symbol)
                continue

            # 实盘运行时，要加入市场买卖量的判断
            limit_down = None
            if not force and not self.backtesting:
                symbol_tick = self.cta_engine.get_tick(vt_symbol)
                if symbol_tick:
                    if symbol_tick.limit_down > 0:
                        limit_down = symbol_tick.limit_down
                    symbol_volume_tick = self.cta_engine.get_volume_tick(vt_symbol)
                    # 根据市场计算，前5档买单数量
                    if all([symbol_tick.ask_volume_1, symbol_tick.ask_volume_2, symbol_tick.ask_volume_3,
                            symbol_tick.ask_volume_4, symbol_tick.ask_volume_5]) \
                            and all(
                        [symbol_tick.bid_volume_1, symbol_tick.bid_volume_2, symbol_tick.bid_volume_3, symbol_tick.bid_volume_4,
                         symbol_tick.bid_volume_5]):
                        market_ask_volumes = symbol_tick.ask_volume_1 + symbol_tick.ask_volume_2 + symbol_tick.ask_volume_3 + symbol_tick.ask_volume_4 + symbol_tick.ask_volume_5
                        market_bid_volumes = symbol_tick.bid_volume_1 + symbol_tick.bid_volume_2 + symbol_tick.bid_volume_3 + symbol_tick.bid_volume_4 + symbol_tick.bid_volume_5
                        # 乘以基本系数
                        market_ask_volumes = market_ask_volumes * symbol_volume_tick
                        market_bid_volumes = market_bid_volumes * symbol_volume_tick

                        org_sell_volume = sell_volume
                        if market_bid_volumes > 0 and market_ask_volumes > 0 and org_sell_volume >= 2 * symbol_volume_tick:
                            sell_volume = min(market_bid_volumes / 4, market_ask_volumes / 4, sell_volume)
                            sell_volume = max(round_to(value=sell_volume, target=symbol_volume_tick), symbol_volume_tick)
                            if org_sell_volume != sell_volume:
                                self.write_log(u'修正批次卖出{}数量:{}=>{}'.format(vt_symbol, org_sell_volume, sell_volume))

            # 获取当前价格
            if limit_down is None or cur_price > limit_down:
                sell_price = cur_price - self.cta_engine.get_price_tick(vt_symbol)
            else:
                sell_price = cur_price

            # 发出委托卖出
            vt_orderids = self.sell(
                vt_symbol=vt_symbol,
                price=sell_price,
                volume=sell_volume,
                order_time=self.cur_datetime,
                grid=ordering_grid)
            if vt_orderids is None or len(vt_orderids) == 0:
                self.write_error(f'{vt_symbol} 委托卖出失败，委托价:{sell_price} 数量:{sell_volume}')
                continue
            else:
                self.write_log(f'{vt_symbol} 已委托卖出，{sell_volume},委托价:{sell_price}, 数量:{sell_volume}')

    def tns_finish_sell_grid(self, grid:CtaGrid):
        """
        事务完成卖出网格
        :param grid:
        :return:
        """
        self.write_log(
            u'卖出网格执行完毕,price:{},v:{},traded:{},type:'.format(grid.open_price, grid.volume, grid.traded_volume, grid.type))
        grid.order_status = False
        grid.open_status = False
        volume = grid.volume
        traded_volume = grid.traded_volume
        if grid.traded_volume > 0:
            grid.volume = grid.traded_volume
        grid.traded_volume = 0
        self.write_log(u'{} {} {} 委托状态为: {}，完成状态:{} v:{}=>{},traded:{}=>{}'
                         .format(grid.type, grid.direction, grid.vt_symbol,
                                 grid.order_status, grid.open_status,
                                 volume, grid.volume,
                                 traded_volume, grid.traded_volume))

        dist_record = dict()
        dist_record['volume'] = grid.volume
        dist_record['price'] = self.cta_engine.get_price(grid.vt_symbol)
        dist_record['operation'] = 'sell finished'
        dist_record['signal'] = grid.type
        self.save_dist(dist_record)

        id = grid.id
        self.write_log(u'移除卖出网格:{}'.format(id))
        self.gt.remove_grids_by_ids(direction=Direction.LONG, ids=[id])
        self.gt.save()
        self.policy.save()

    def tns_execute_buy_grids(self, vt_symbol=None, force=False):
        """
        事务执行买入网格
        :return:
        """
        if not self.trading:
            return
        if self.cur_datetime and 9 <= self.cur_datetime.hour <= 14:
            if self.cur_datetime.hour == 12:
                return
            if self.cur_datetime.hour == 9 and self.cur_datetime.minute < 30:
                return
            if self.cur_datetime.hour == 11 and self.cur_datetime.minute >= 30:
                return

        ordering_grid = None
        for grid in self.gt.dn_grids:

            # 只扫描vt_symbol 匹配的网格
            if vt_symbol and vt_symbol != vt_symbol:
                continue

            # 排除已经执行完毕(处于开仓状态）的网格， 或者处于平仓状态的网格
            if grid.open_status or grid.close_status:
                continue
            # 排除非委托状态的网格
            if not grid.order_status:
                continue

            # 排除存在委托单号的网格
            if len(grid.order_ids) > 0:
                continue

            if grid.volume <= grid.traded_volume:
                self.write_log(u'网格计划买入:{}，已成交:{}'.format(grid.volume, grid.traded_volume))
                self.tns_finish_buy_grid(grid)
                return

            # 定位到首个满足条件的网格，
            ordering_grid = grid

            balance, availiable, _, _ = self.cta_engine.get_account()
            if availiable <= 0:
                if not self.backtesting:
                    self.write_log(u'当前可用资金{}不足,总资金:{}'.format(availiable, balance))
                continue
            vt_symbol = ordering_grid.vt_symbol
            cur_price = self.cta_engine.get_price(vt_symbol)
            if cur_price is None:
                self.write_error(f'暂时不能获取{vt_symbol}最新价格')
                continue

            buy_volume = ordering_grid.volume - ordering_grid.traded_volume
            # if buy_volume <= 0:
            #     self.write_error(f'{grid.vt_symbol} 已买入数量:{ordering_grid.traded_volume} 超过委托数量:{ordering_grid.volume}')
            #     continue

            min_trade_volume = self.cta_engine.get_volume_tick(vt_symbol)
            if availiable < buy_volume * cur_price:
                self.write_error(f'可用资金{availiable},不满足买入{vt_symbol},数量:{buy_volume} X价格{cur_price}')
                max_buy_volume = int(availiable / cur_price)
                max_buy_volume = max_buy_volume - max_buy_volume % min_trade_volume
                if max_buy_volume <= min_trade_volume:
                    continue
                # 计划买入数量，与可用资金买入数量的差别
                diff_volume = buy_volume - max_buy_volume
                # 降低计划买入数量
                self.write_log(f'总计划{vt_symbol}买入数量:{ordering_grid.volume}=>{ordering_grid.volume - diff_volume}')
                ordering_grid.volume -= diff_volume
                self.gt.save()
                buy_volume = max_buy_volume

            # 实盘运行时，要加入市场买卖量的判断
            if not force and not self.backtesting and 'market' in ordering_grid.snapshot:
                symbol_tick = self.cta_engine.get_tick(vt_symbol)
                if symbol_tick:
                    # 根据市场计算，前5档买单数量
                    if all([symbol_tick.ask_volume_1, symbol_tick.ask_volume_2, symbol_tick.ask_volume_3,
                            symbol_tick.ask_volume_4, symbol_tick.ask_volume_5]) \
                            and all(
                        [symbol_tick.bid_volume_1, symbol_tick.bid_volume_2, symbol_tick.bid_volume_3, symbol_tick.bid_volume_4,
                         symbol_tick.bid_volume_5]):
                        market_ask_volumes = symbol_tick.ask_volume_1 + symbol_tick.ask_volume_2 + symbol_tick.ask_volume_3 + symbol_tick.ask_volume_4 + symbol_tick.ask_volume_5
                        market_bid_volumes = symbol_tick.bid_volume_1 + symbol_tick.bid_volume_2 + symbol_tick.bid_volume_3 + symbol_tick.bid_volume_4 + symbol_tick.bid_volume_5
                        if market_bid_volumes > 0 and market_ask_volumes > 0:
                            buy_volume = min(market_bid_volumes / 4, market_ask_volumes / 4, buy_volume)
                            buy_volume = max(buy_volume - buy_volume % min_trade_volume, min_trade_volume)

            buy_price = cur_price + self.cta_engine.get_price_tick(vt_symbol) * 10

            vt_orderids = self.buy(
                vt_symbol=vt_symbol,
                price=buy_price,
                volume=buy_volume,
                order_time=self.cur_datetime,
                grid=ordering_grid)
            if vt_orderids is None or len(vt_orderids) == 0:
                self.write_error(f'委托买入失败，{vt_symbol} 委托价:{buy_price} 数量:{buy_volume}')
                continue
            else:
                self.write_log(f'{self.strategy_name}, {vt_orderids},已委托买入，{vt_symbol} 委托价:{buy_price} 数量:{buy_volume}')

    def tns_finish_buy_grid(self, grid:CtaGrid):
        """
        事务完成买入网格
        :return:
        """
        self.write_log(u'事务完成买入网格:{},计划数量:{}，计划价格:{}，实际数量:{}'
                         .format(grid.type, grid.volume, grid.open_price, grid.traded_volume))
        if grid.volume != grid.traded_volume:
            grid.volume = grid.traded_volume
        grid.traded_volume = 0
        grid.open_status = True
        grid.order_status = False
        grid.open_time = self.cur_datetime

        dist_record = dict()
        dist_record['symbol'] = grid.vt_symbol
        dist_record['volume'] = grid.volume
        dist_record['price'] = self.cta_engine.get_price(grid.vt_symbol)
        dist_record['operation'] = '{} finished'.format(grid.type)
        dist_record['signal'] = grid.type
        self.save_dist(dist_record)

        self.gt.save()

    def cancel_all_orders(self):
        """
        重载撤销所有正在进行得委托
        :return:
        """
        self.write_log(u'撤销所有正在进行得委托')
        self.tns_cancel_logic(dt=datetime.now(), force=True)

    def tns_cancel_logic(self, dt, force=False):
        "撤单逻辑"""
        if len(self.active_orders) < 1:
            self.entrust = 0
            return

        canceled_ids = []

        for vt_orderid in list(self.active_orders.keys()):
            order_info = self.active_orders[vt_orderid]
            order_vt_symbol = order_info.get('vt_symbol')
            order_time = order_info['order_time']
            order_volume = order_info['volume'] - order_info['traded']
            # order_price = order_info['price']
            # order_direction = order_info['direction']
            # order_offset = order_info['offset']
            order_grid = order_info['grid']
            order_status = order_info.get('status', Status.NOTTRADED)
            order_type = order_info.get('order_type', OrderType.LIMIT)
            over_seconds = (dt - order_time).total_seconds()

            # 只处理未成交的限价委托单
            if order_status in [Status.SUBMITTING, Status.NOTTRADED] and order_type == OrderType.LIMIT:
                if over_seconds > self.cancel_seconds or force:  # 超过设置的时间还未成交
                    self.write_log(u'超时{}秒未成交，取消委托单：vt_orderid:{},order:{}'
                                   .format(over_seconds, vt_orderid, order_info))
                    order_info.update({'status': Status.CANCELLING})
                    self.active_orders.update({vt_orderid: order_info})
                    ret = self.cancel_order(str(vt_orderid))
                    if not ret:
                        self.write_log(f'撤单失败:{order_info}')
                        # self.write_log(u'撤单失败,更新状态为撤单成功')
                        # order_info.update({'status': Status.CANCELLED})
                        # self.active_orders.update({vt_orderid: order_info})
                        # if order_grid:
                        #     if vt_orderid in order_grid.order_ids:
                        #         order_grid.order_ids.remove(vt_orderid)

                continue

            # 处理状态为‘撤销’的委托单
            elif order_status == Status.CANCELLED:
                self.write_log(u'委托单{}已成功撤单，删除{}'.format(vt_orderid, order_info))
                canceled_ids.append(vt_orderid)
            elif order_status == Status.CANCELLING:

                if over_seconds > self.cancel_seconds * 3:
                    self.write_log(u'委托单{}正在撤单，超时{},删除{}'.format(vt_orderid,over_seconds, order_info))
                    canceled_ids.append(vt_orderid)

        # 删除撤单的订单
        for vt_orderid in canceled_ids:
            self.write_log(u'删除orderID:{0}'.format(vt_orderid))
            self.active_orders.pop(vt_orderid, None)

        if len(self.active_orders) == 0:
            self.entrust = 0

    def display_grids(self):
        """更新网格显示信息"""
        if not self.inited:
            return

        opening_info = ""
        closing_info = ""
        holding_info = ""

        for grid in self.gt.dn_grids:
            name = self.cta_engine.get_name(grid.vt_symbol)

            if not grid.open_status and grid.order_status:
                opening_info += f'网格{grid.type},买入状态:{name}[{grid.vt_symbol}], 买入价:{grid.open_price} [已买入:{grid.traded_volume} => 目标:{grid.volume}, 委托时间:{grid.order_time}\n'
                continue

            if grid.open_status and not grid.close_status:
                holding_info += f'网格{grid.type},持有状态:{name}[{grid.vt_symbol}],买入价:{grid.open_price} [数量:{grid.volume}, 开仓时间:{grid.open_time}]\n'
                continue

            if grid.open_status and grid.close_status:
                closing_info += f'网格{grid.type},卖出状态:{name}[{grid.vt_symbol}],卖出价:{grid.close_price} [已卖出:{grid.traded_volume} => 目标:{grid.volume}, 委托时间:{grid.order_time}\n'

        if len(opening_info) > 0:
            self.write_log(opening_info)
        if len(holding_info) > 0:
            self.write_log(holding_info)
        if len(closing_info) > 0:
            self.write_log(closing_info)

    def display_tns(self):
        """显示事务的过程记录=》 log"""
        if not self.inited:
            return

        if not self.backtesting:
            for vt_symbol in self.vt_symbols:
                name = self.cta_engine.get_name(vt_symbol)
                price = self.cta_engine.get_price(vt_symbol)
                self.write_log('%-11s'%vt_symbol + '[%-12s'%name + f'] 当前价格: {price}')

            self.write_log(f'当前委托状态:{self.entrust}')

            if len(self.active_orders) > 0:
                self.write_log('当前活动订单:{}'.format(self.active_orders))

        if hasattr(self, 'policy'):
            policy = getattr(self, 'policy')
            op = getattr(policy, 'to_json', None)
            if callable(op):
                self.write_log(u'当前Policy:{}'.format(json.dumps(policy.to_json(), indent=2, ensure_ascii=False,cls=MyEncoder)))

    def save_dist(self, dist_data):
        """
        保存策略逻辑过程记录=》 csv文件按
        :param dist_data:
        :return:
        """
        if self.backtesting:
            save_path = self.cta_engine.get_logs_path()
        else:
            save_path = self.cta_engine.get_data_path()
        try:

            if 'datetime' not in dist_data:
                dist_data.update({'datetime': self.cur_datetime})
            if 'long_pos' not in dist_data:
                vt_symbol = dist_data.get('symbol')
                if vt_symbol:
                    pos = self.get_position(vt_symbol)
                    dist_data.update({'long_pos': pos.volume})
                    if 'name' not in dist_data:
                        dist_data['name'] = self.cta_engine.get_name(vt_symbol)

            file_name = os.path.abspath(os.path.join(save_path, f'{self.strategy_name}_dist.csv'))
            append_data(file_name=file_name, dict_data=dist_data, field_names=self.dist_fieldnames)
        except Exception as ex:
            self.write_error(u'save_dist 异常:{} {}'.format(str(ex), traceback.format_exc()))

    def save_tns(self, tns_data):
        """
        保存多空事务记录=》csv文件,便于后续分析
        :param tns_data:
        :return:
        """
        if self.backtesting:
            save_path = self.cta_engine.get_logs_path()
        else:
            save_path = self.cta_engine.get_data_path()

        try:
            file_name = os.path.abspath(os.path.join(save_path, f'{self.strategy_name}_tns.csv'))
            append_data(file_name=file_name, dict_data=tns_data)
        except Exception as ex:
            self.write_error(u'save_tns 异常:{} {}'.format(str(ex), traceback.format_exc()))

    def send_wechat(self, msg: str):
        """实盘时才发送微信"""
        if self.backtesting:
            return
        self.cta_engine.send_wechat(msg=msg, strategy=self)

