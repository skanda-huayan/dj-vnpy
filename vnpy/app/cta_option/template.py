# 期权模板
# 华富资产 @ 李来佳

import os
import traceback

import bz2
import pickle
import zlib
from vnpy.trader.utility import append_data, extract_vt_symbol

from abc import ABC
from copy import copy, deepcopy
from typing import Any, Callable, List, Dict
from logging import INFO, ERROR
from datetime import datetime
from vnpy.trader.constant import Interval, Direction, Offset, Status, OrderType, Color, Exchange
from vnpy.trader.object import BarData, TickData, OrderData, TradeData, PositionData, ContractData, HistoryRequest
from vnpy.trader.utility import virtual, append_data, extract_vt_symbol, get_underlying_symbol
# from vnpy.app.cta_option import CtaOptionEngine
from .base import StopOrder, EngineType
from vnpy.component.cta_grid_trade import CtaGrid, CtaGridTrade, LOCK_GRID

from vnpy.component.cta_policy import CtaPolicy  # noqa
from vnpy.trader.utility import print_dict

DIRECTION_MAP = {
    Direction.LONG.value: 'long',
    Direction.SHORT.value: 'short',
    Direction.NET.value: 'long'
}
class CtaOptionPolicy(CtaPolicy):
    """
    期权策略逻辑&持仓持久化组件
    满足使用target_pos方式得策略
    """
    def __init__(self, strategy):
        super().__init__(strategy)
        self.cur_trading_date = None    # 已执行pre_trading方法后更新的当前交易日
        self.signals = {}     # kline_name: { 'last_signal': '', 'last_signal_time': datetime }
        self.sub_tns = {}     # 子事务， 事务名称: 事务内容dict
        self.datas = {}   # 数据名称： 数据内容
        self.holding_pos = {}   # 当前策略得持仓, 合约_方向: 数量
        self.target_pos = {}  # 当前策略得目标持仓，合约_方向: 数量

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

        self.datas = json_data.get('datas', {})
        self.holding_pos = json_data.get('holding_pos', {})
        self.target_pos = json_data.get('target_pos', {})

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
        j['datas'] = self.datas
        j['holding_pos'] = self.holding_pos
        j['target_pos'] = self.target_pos

        return j

class CtaTemplate(ABC):
    """CTA策略模板"""

    author = ""
    parameters = []
    variables = []

    # 保存委托单编号和相关委托单的字典
    # key为委托单编号
    # value为该合约相关的委托单
    active_orders = {}
    # 是否回测状态
    backtesting = False

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

        self.backtesting = False  # True, 回测状态； False，实盘状态
        self.inited = False  # 是否初始化完毕
        self.trading = False  # 是否开始交易
        self.positions = {}  # 持仓，vt_symbol_direction: position data
        self.entrust = 0  # 是否正在委托, 0, 无委托 , 1, 委托方向是LONG， -1, 委托方向是SHORT

        self.cur_datetime = datetime.now()  # 当前时间

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

    def get_position(self, vt_symbol, direction) -> PositionData:
        """
        获取策略内某vt_symbol+方向得持仓
        :return:
        """
        k = f'{vt_symbol}_{direction.value}'
        pos = self.positions.get(k, None)
        if pos is None:
            symbol, exchange = extract_vt_symbol(vt_symbol)
            contract = self.cta_engine.get_contract(vt_symbol)
            pos = PositionData(
                gateway_name=contract.gateway_name if contract else '',
                symbol=symbol,
                name=contract.name,
                exchange=exchange,
                direction=direction
            )
            self.positions.update({k: pos})

        return pos

    def get_positions(self):
        """ 返回持仓数量"""
        pos_list = []
        for k, v in self.positions.items():
            # 分解出vt_symbol和方向
            vt_symbol, direction = k.split('_')
            pos_list.append({
                "vt_symbol": vt_symbol,
                "direction": DIRECTION_MAP.get(direction,'long'),
                "name": v.name,
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
        if volume == 0:
            self.write_error(f'委托数量有误，必须大于0，{vt_symbol}, price:{price}')
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
        if volume == 0:
            self.write_error(f'委托数量有误，必须大于0，{vt_symbol}, price:{price}')
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

    def short(self, price: float, volume: float, stop: bool = False,
              vt_symbol: str = '', order_type: OrderType = OrderType.LIMIT,
              order_time: datetime = None, grid: CtaGrid = None):
        """
        Send short order to open as short position.
        """
        if order_type in [OrderType.FAK, OrderType.FOK]:
            if self.is_lower_limit(vt_symbol):
                self.write_error(u'跌停价不做FAK/FOK short委托')
                return []
        if volume == 0:
            self.write_error(f'委托数量有误，必须大于0，{vt_symbol}, price:{price}')
            return []
        return self.send_order(vt_symbol=vt_symbol,
                               direction=Direction.SHORT,
                               offset=Offset.OPEN,
                               price=price,
                               volume=volume,
                               stop=stop,
                               order_type=order_type,
                               order_time=order_time,
                               grid=grid)

    def cover(self, price: float, volume: float, stop: bool = False,
              vt_symbol: str = '', order_type: OrderType = OrderType.LIMIT,
              order_time: datetime = None, grid: CtaGrid = None):
        """
        Send cover order to close a short position.
        """
        if order_type in [OrderType.FAK, OrderType.FOK]:
            if self.is_upper_limit(vt_symbol):
                self.write_error(u'涨停价不做FAK/FOK cover委托')
                return []
        if volume == 0:
            self.write_error(f'委托数量有误，必须大于0，{vt_symbol}, price:{price}')
            return []
        return self.send_order(vt_symbol=vt_symbol,
                               direction=Direction.LONG,
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
        # 兼容cta_strategy的模板，缺省不指定vt_symbol时，使用策略配置的vt_symbol
        if vt_symbol == '':
           return []

        if not self.trading:
            self.write_log(f'非交易状态')
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
        if len(vt_orderids) == 0:
            self.write_error(f'{self.strategy_name}调用cta_engine.send_order委托返回失败,vt_symbol:{vt_symbol}')
                             # f',direction:{direction.value},offset:{offset.value},'
                             # f'price:{price},volume:{volume},stop:{stop},lock:{lock},'
                             # f'order_type:{order_type}')

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

    def load_bar(
            self,
            vt_symbol:str,
            days: int,
            interval: Interval = Interval.MINUTE,
            callback: Callable = None,
            interval_num: int = 1
    ):
        """
        Load historical bar data for initializing strategy.
        """
        if not callback:
            callback = self.on_bar

        self.cta_engine.load_bar(vt_symbol, days, interval, callback, interval_num)

    def load_tick(self, vt_symbol: str, days: int):
        """
        Load historical tick data for initializing strategy.
        """
        self.cta_engine.load_tick(vt_symbol, days, self.on_tick)

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

class CtaOptionTemplate(CtaTemplate):
    """期权交易增强版模板"""


    # 逻辑过程日志
    dist_fieldnames = ['datetime', 'vt_symbol', 'name', 'volume', 'price',
                       'operation', 'signal', 'stop_price', 'target_price',
                       'long_pos','short_pos']

    def __init__(self, cta_engine, strategy_name, vt_symbol, setting):
        """"""
        super().__init__(cta_engine, strategy_name, vt_symbol, setting)

        self.cancel_seconds = 60  # 撤单时间

        self.klines = {}   # 所有K线

        # 策略事务逻辑与持仓组件
        self.policy = CtaOptionPolicy(strategy=self)


    def update_setting(self, setting: dict):
        """更新配置参数"""
        super().update_setting(setting)

    def init_policy(self):
        """加载policy"""
        self.write_log(f'{self.strategy_name} => 初始化Policy')
        self.policy.load()
        self.write_log(u'Policy:{}'.format(print_dict(self.policy.to_json())))

        # self.policy持仓 => self.positions
        for k in list(self.policy.holding_pos.keys()):
            v = self.policy.holding_pos.get(k)
            if v == 0:
                self.policy.holding_pos.pop(k,None)
                continue
            vt_symbol, direction = k.split('_')
            cur_pos = self.get_position(vt_symbol, Direction(direction))
            cur_pos.volume = v
            self.positions.update({k:cur_pos})
            # 订阅行情
            self.cta_engine.subscribe_symbol(
                strategy_name=self.strategy_name,
                vt_symbol=vt_symbol)
            self.write_log(f'{self.strategy_name} => 恢复持仓 {cur_pos.vt_symbol}[{cur_pos.name}] {cur_pos.direction}:{cur_pos.volume}')

    def display_tns(self):
        """
        打印日志
        :return:
        """
        if self.backtesting:
            return

        self.write_log('当前policy:\n{}'.format(print_dict(self.policy.to_json())))


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

    def on_start(self):
        """启动策略（必须由用户继承实现）"""
        self.write_log(f'{self.strategy_name} => 策略启动')
        self.trading = True
        self.put_event()

    def on_stop(self):
        """停止策略（必须由用户继承实现）"""
        self.active_orders.clear()
        self.entrust = 0

        self.write_log(f'{self.strategy_name} => 策略停止')
        self.put_event()

    def on_trade(self, trade: TradeData):
        """
        交易更新
        :param trade:
        :return:
        """

        if (trade.direction == Direction.LONG and trade.offset == Offset.OPEN) \
                or (trade.direction == Direction.SHORT and trade.offset != Offset.OPEN):
            cur_pos = self.get_position(trade.vt_symbol, Direction.LONG)
        else:
            cur_pos = self.get_position(trade.vt_symbol, Direction.SHORT)

        self.write_log(u'{},交易更新 =>{}\n,\n 当前持仓：\n{} '
                       .format(self.cur_datetime,
                               print_dict(trade.__dict__),
                               print_dict(cur_pos.__dict__)))

        dist_record = dict()
        if self.backtesting:
            dist_record['datetime'] = trade.time
        else:
            dist_record['datetime'] = ' '.join([self.cur_datetime.strftime('%Y-%m-%d'), trade.time])
        dist_record['volume'] = trade.volume
        dist_record['price'] = trade.price
        dist_record['symbol'] = trade.vt_symbol

        if trade.direction == Direction.LONG and trade.offset == Offset.OPEN:
            dist_record['operation'] = 'buy'
            cur_pos.volume += trade.volume
            dist_record['long_pos'] = cur_pos.volume
            dist_record['short_pos'] = 0

        if trade.direction == Direction.SHORT and trade.offset == Offset.OPEN:
            dist_record['operation'] = 'short'
            cur_pos.volume += trade.volume
            dist_record['long_pos'] = 0
            dist_record['short_pos'] = cur_pos.volume

        if trade.direction == Direction.LONG and trade.offset != Offset.OPEN:
            dist_record['operation'] = 'cover'
            cur_pos.volume = max(0, cur_pos.volume - trade.volume)
            dist_record['long_pos'] = 0
            dist_record['short_pos'] = cur_pos.volume

        if trade.direction == Direction.SHORT and trade.offset != Offset.OPEN:
            dist_record['operation'] = 'sell'
            cur_pos.volume = max(0, cur_pos.volume - trade.volume)
            dist_record['long_pos'] = cur_pos.volume
            dist_record['short_pos'] = 0

        k = f'{cur_pos.vt_symbol}_{cur_pos.direction.value}'
        # 更新 self.positions
        self.positions.update({k: cur_pos})
        self.write_log(f'{self.strategy_name} ,positions[{k}]持仓更新 =>\n{print_dict(cur_pos.__dict__)}')

        # 更新 policy.holding_pos
        self.write_log(f'{self.strategy_name} ,policy.holding_pos[{k}]持仓更新 => {cur_pos.volume}')
        if cur_pos.volume == 0:
            self.policy.holding_pos.pop(k, None)
        else:
            self.policy.holding_pos[k] = int(cur_pos.volume)
        self.policy.save()

        # 这里要判断订单是否全部完成，如果完成，就移除活动订单
        if trade.vt_orderid in self.active_orders:
            if self.active_orders[trade.vt_orderid].get('volume',-1) == self.active_orders[trade.vt_orderid].get('traded',0):
                self.write_log(f'{trade.vt_orderid}全部执行完毕,移除活动订单')
                self.active_orders.pop(trade.vt_orderid, None)

        self.save_dist(dist_record)

    def on_order(self, order: OrderData):
        """报单更新"""
        # 未执行的订单中，存在是异常，删除
        self.write_log(u'{}报单更新 =>\n {}'.format(self.cur_datetime, print_dict(order.__dict__)))

        if order.vt_orderid in self.active_orders:
            d = self.active_orders[order.vt_orderid]
            if d['traded'] != order.traded:
                self.write_log(f'委托单交易 已交易{d["traded"]} => {order.traded}, 总委托:{order.volume}')
                d['traded'] = order.traded

            if order.status in [Status.ALLTRADED]:
                # 全部成交
                self.write_log(f'报单更新 => 委托开仓 => {order.status}')
                # 这里不去掉active_orders,由on_trade进行去除

            elif order.status in [Status.CANCELLED, Status.REJECTED]:
                # 撤单、拒单
                self.write_log(f'报单更新 => 委托开仓 => {order.status}')
                self.active_orders.pop(order.vt_orderid, None)
            else:
                # 未完成、部分成交..
                self.write_log(u'委托单未完成,total:{},traded:{},tradeStatus:{}'
                               .format(order.volume, order.traded, order.status))
        else:
            self.write_error(u'委托单{}不在策略的未完成订单列表中:{}'.format(order.vt_orderid, self.active_orders))

    def on_stop_order(self, stop_order: StopOrder):
        """
        停止单更新
        需要自己重载，处理各类触发、撤单等情况
        """
        self.write_log(f'停止单触发:{stop_order.__dict__}')

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

            order_status = order_info.get('status', Status.NOTTRADED)
            order_type = order_info.get('order_type', OrderType.LIMIT)
            over_seconds = (dt - order_time).total_seconds()

            # 只处理未成交的限价委托单
            if order_status in [Status.NOTTRADED, Status.SUBMITTING] and order_type == OrderType.LIMIT:
                if over_seconds > self.cancel_seconds or force:  # 超过设置的时间还未成交
                    self.write_log(u'撤单逻辑 => 超时{}秒未成交，取消委托单：vt_orderid:{},order:{}'
                                   .format(over_seconds, vt_orderid, order_info))
                    order_info.update({'status': Status.CANCELLING})
                    self.active_orders.update({vt_orderid: order_info})
                    ret = self.cancel_order(str(vt_orderid))
                    if not ret:
                        self.write_error(f'{self.strategy_name}撤单逻辑 => {order_vt_symbol}撤单失败')

                continue

            # 处理状态为‘撤销’的委托单
            elif order_status == Status.CANCELLED:
                self.write_log(u'撤单逻辑 => 委托单{}已成功撤单，将删除未完成订单{}'.format(vt_orderid, order_info))
                canceled_ids.append(vt_orderid)

        # 删除撤单的订单
        for vt_orderid in canceled_ids:
            self.write_log(u'撤单逻辑 => 删除未完成订单:{}'.format(vt_orderid))
            self.active_orders.pop(vt_orderid, None)

        if len(self.active_orders) == 0:
            self.entrust = 0

    def tns_balance_pos(self, vt_symbol):
        """
        事务自动平衡 policy得holding_pos & target_pos 仓位
        这里委托单时，还需要看看当前tick得ask1&bid1差距情况
        :param vt_symbol:
        :return:
        """
        option_name = self.cta_engine.get_name(vt_symbol)
        c = self.cta_engine.get_contract(vt_symbol)

        for direction in [Direction.LONG, Direction.SHORT]:
            k = f'{vt_symbol}_{direction.value}'

            target_pos = self.policy.target_pos.get(k, 0)
            holding_pos = self.policy.holding_pos.get(k, 0)
            diff_pos = target_pos - holding_pos

            if diff_pos == 0:
                continue

            # 获取最新价
            cur_price = self.cta_engine.get_price(vt_symbol)
            if not cur_price:
                continue
            # 获取最新tick
            cur_tick = self.cta_engine.get_tick(vt_symbol)
            price_tick = self.cta_engine.get_price_tick(vt_symbol)
            if diff_pos > 0:  # 需要增加仓位，增加多单或空单
                self.write_log(f'平衡仓位，{vt_symbol} [{c.name}]{direction.value}单,{holding_pos} =>{target_pos} => 增加 {diff_pos}手')
                # 检查是否存在相同得开仓委托
                if self.exist_order(vt_symbol, direction, Offset.OPEN):
                    self.write_log(f'存在相同得开仓委托,暂不处理')
                    continue

                if direction == Direction.LONG:   # 买入多单
                    # 发出委托
                    vt_orderid = self.buy(vt_symbol=vt_symbol,
                             price=cur_price,
                             volume=diff_pos,
                             order_type=OrderType.LIMIT,
                             order_time=self.cur_datetime)
                    if vt_orderid:
                        self.write_log(f'{self.strategy_name} 调整目标:{vt_symbol}[{option_name}]' +
                                       f' {holding_pos} =>{target_pos} 开多:{diff_pos} ' +
                                       f'价格:{cur_price} 委托编号:{vt_orderid}')
                else:   # 卖出空单
                    # 发出委托
                    vt_orderid = self.short(vt_symbol=vt_symbol,
                                          price=cur_price,
                                          volume=diff_pos,
                                          order_type=OrderType.LIMIT,
                                          order_time=self.cur_datetime)
                    if vt_orderid:
                        self.write_log(f'{self.strategy_name} 调整目标:{vt_symbol}[{option_name}]' +
                                       f' {holding_pos} =>{target_pos} 开空:{diff_pos} ' +
                                       f'价格:{cur_price} 委托编号:{vt_orderid}')

            else:  # 需要减少仓位，平多单或平空单
                self.write_log(
                    f'平衡仓位，{vt_symbol} [{c.name}]{direction.value}单,{holding_pos} =>{target_pos} => 减少 {abs(diff_pos)}手')
                close_direction = Direction.LONG if direction == Direction.SHORT else Direction.SHORT
                # 检查是否存在相同得平仓委托
                if self.exist_order(vt_symbol, close_direction, Offset.CLOSE):
                    self.write_log(f'存在相同得平仓委托,暂不处理')
                    continue

                if direction == Direction.LONG:  # 平仓多单
                    sell_price = cur_price
                    # 价格 tick检查，叫卖价降低1个跳卖出
                    if cur_tick and cur_tick.ask_price_1 and cur_tick.bid_price_1:
                        sell_price = max(cur_tick.ask_price_1 - price_tick,sell_price)

                    # 发出委托
                    vt_orderid = self.sell(vt_symbol=vt_symbol,
                                          price=sell_price,
                                          volume=abs(diff_pos),
                                          order_type=OrderType.LIMIT,
                                          order_time=self.cur_datetime)
                    if vt_orderid:
                        self.write_log(f'{self.strategy_name} 调整目标:{vt_symbol}[{option_name}]' +
                                       f' {holding_pos} =>{target_pos} 多单平仓:{abs(diff_pos)} ' +
                                       f'价格:{sell_price} 委托编号:{vt_orderid}')
                else:  # 平仓空单
                    cover_price = cur_price
                    # 价格 tick检查，叫买价提高1个跳卖出
                    if cur_tick and cur_tick.ask_price_1 and cur_tick.bid_price_1:
                        cover_price = min(cur_tick.bid_price_1 + price_tick, cover_price)

                    # 发出委托
                    vt_orderid = self.cover(vt_symbol=vt_symbol,
                                            price=cover_price,
                                            volume=abs(diff_pos),
                                            order_type=OrderType.LIMIT,
                                            order_time=self.cur_datetime)
                    if vt_orderid:
                        self.write_log(f'{self.strategy_name} 调整目标:{vt_symbol}[{option_name}]' +
                                       f' {holding_pos} =>{target_pos} 空单平仓:{abs(diff_pos)} ' +
                                       f'价格:{cover_price} 委托编号:{vt_orderid}')

    def exist_order(self, vt_symbol, direction, offset):
        """
        是否存在相同得委托
        :param vt_symbol:
        :param direction:
        :param offset:
        :return:
        """
        if len(self.active_orders) == 0:
            self.write_log(f'当前活动订单中，数量为零. 查询{vt_symbol}，方向:{direction.value}, 开平:{offset.value}')
            return False

        for orderid, order in self.active_orders.items():
            self.write_log(f'当前活动订单:\n{print_dict(order)}')
            if order['vt_symbol'] == vt_symbol and order['direction'] == direction and order['offset'] == offset:
                self.write_log(f'存在相同得活动订单')
                return True

        return False

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
                vt_symbol = dist_data.get('vt_symbol')
                if vt_symbol:
                    # pos = self.get_position(vt_symbol)
                    # dist_data.update({'long_pos': pos.volume})
                    if 'name' not in dist_data:
                        dist_data['name'] = self.cta_engine.get_name(vt_symbol)

            file_name = os.path.abspath(os.path.join(save_path, f'{self.strategy_name}_dist.csv'))
            append_data(file_name=file_name, dict_data=dist_data, field_names=self.dist_fieldnames)
        except Exception as ex:
            self.write_error(u'save_dist 异常:{} {}'.format(str(ex), traceback.format_exc()))
