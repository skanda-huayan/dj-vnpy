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
from copy import copy
from typing import Any, Callable
from logging import INFO, ERROR
from datetime import datetime
from vnpy.trader.constant import Interval, Direction, Offset, Status, OrderType
from vnpy.trader.object import BarData, TickData, OrderData, TradeData
from vnpy.trader.utility import virtual, append_data, extract_vt_symbol, get_underlying_symbol

from .base import StopOrder
from vnpy.component.cta_grid_trade import CtaGrid, CtaGridTrade
from vnpy.component.cta_position import CtaPosition
from vnpy.component.cta_policy import CtaPolicy
from vnpy.component.base import MyEncoder

class CtaTemplate(ABC):
    """CTA策略模板"""

    author = ""
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
            vt_symbol: str,
            setting: dict,
    ):
        """"""
        self.cta_engine = cta_engine
        self.strategy_name = strategy_name
        self.vt_symbol = vt_symbol

        self.inited = False  # 是否初始化完毕
        self.trading = False  # 是否开始交易
        self.pos = 0  # 持仓/仓差
        self.entrust = 0  # 是否正在委托, 0, 无委托 , 1, 委托方向是LONG， -1, 委托方向是SHORT

        self.tick_dict = {}  # 记录所有on_tick传入最新tick
        self.active_orders = {}

        # Copy a new variables list here to avoid duplicate insert when multiple
        # strategy instances are created with the same strategy class.
        self.variables = copy(self.variables)
        self.variables.insert(0, "inited")
        self.variables.insert(1, "trading")
        self.variables.insert(2, "pos")
        self.variables.insert(3, "entrust")

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
            "vt_symbol": self.vt_symbol,
            "class_name": self.__class__.__name__,
            "author": self.author,
            "parameters": self.get_parameters(),
            "variables": self.get_variables(),
        }
        return strategy_data

    def get_positions(self):
        """ 返回持仓数量"""
        pos_list = []
        if self.pos > 0:
            pos_list.append({
                "vt_symbol": self.vt_symbol,
                "direction": "long",
                "volume": self.pos
            })
        elif self.pos < 0:
            pos_list.append({
                "vt_symbol": self.vt_symbol,
                "direction": "short",
                "volume": abs(self.pos)
            })
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
    def on_tick(self, tick: TickData):
        """
        Callback of new tick data update.
        """
        pass

    @virtual
    def on_bar(self, bar: BarData):
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
            vt_symbol = self.vt_symbol

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
                grid.order_ids.append(vt_orderid)
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
            days: int,
            interval: Interval = Interval.MINUTE,
            callback: Callable = None,
    ):
        """
        Load historical bar data for initializing strategy.
        """
        if not callback:
            callback = self.on_bar

        self.cta_engine.load_bar(self.vt_symbol, days, interval, callback)

    def load_tick(self, days: int):
        """
        Load historical tick data for initializing strategy.
        """
        self.cta_engine.load_tick(self.vt_symbol, days, self.on_tick)

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


class CtaFutureTemplate(CtaTemplate):
    """
    合约期货模板
    """

    price_tick = 1  # 商品的最小价格跳动
    symbol_size = 10  # 商品得合约乘数
    margin_rate = 0.1  # 商品的保证金
    volumn_tick = 1  # 商品最小成交数量

    # 委托类型
    order_type = OrderType.LIMIT
    cancel_seconds = 120  # 撤单时间(秒)
    activate_market = False

    # 资金相关
    max_invest_rate = 0.1  # 最大仓位(0~1)
    max_invest_margin = 0  # 资金上限 0，不限制
    max_invest_pos = 0  # 单向头寸数量上限 0，不限制

    # 是否回测状态
    backtesting = False

    # 逻辑过程日志
    dist_fieldnames = ['datetime', 'symbol', 'volume', 'price', 'margin',
                       'operation', 'signal', 'stop_price', 'target_price',
                       'long_pos', 'short_pos']

    def __init__(self, cta_engine, strategy_name, vt_symbol, setting):
        """"""
        self.position = None  # 仓位组件
        self.policy = None  # 事务执行组件
        self.gt = None  # 网格交易组件
        self.klines = {}  # K线组件字典: kline_name: kline

        self.price_tick = 1  # 商品的最小价格跳动
        self.symbol_size = 10  # 商品得合约乘数
        self.margin_rate = 0.1  # 商品的保证金
        self.volumn_tick = 1  # 商品最小成交数量
        self.cancel_seconds = 120  # 撤单时间(秒)
        self.activate_market = False
        self.order_type = OrderType.LIMIT
        self.backtesting = False

        self.cur_datetime: datetime = None  # 当前Tick时间
        self.cur_tick: TickData = None  # 最新的合约tick( vt_symbol)
        self.cur_price = None  # 当前价（主力合约 vt_symbol)
        self.account_pos = None  # 当前账号vt_symbol持仓信息

        self.last_minute = None  # 最后的分钟,用于on_tick内每分钟处理的逻辑
        self.display_bars = True

        super().__init__(
            cta_engine, strategy_name, vt_symbol, setting
        )

        # 增加仓位管理模块
        self.position = CtaPosition(strategy=self)
        self.position.maxPos = sys.maxsize
        # 增加网格持久化模块
        self.gt = CtaGridTrade(strategy=self)

        if 'backtesting' not in self.parameters:
            self.parameters.append('backtesting')

    def update_setting(self, setting: dict):
        """
        Update strategy parameter wtih value in setting dict.
        """
        for name in self.parameters:
            if name in setting:
                setattr(self, name, setting[name])

        self.price_tick = self.cta_engine.get_price_tick(self.vt_symbol)
        self.symbol_size = self.cta_engine.get_size(self.vt_symbol)
        self.margin_rate = self.cta_engine.get_margin_rate(self.vt_symbol)
        self.volumn_tick = self.cta_engine.get_volume_tick(self.vt_symbol)

        if self.activate_market:
            self.write_log(f'{self.strategy_name}使用市价单委托方式')
            self.order_type = OrderType.MARKET
        else:
            if not self.backtesting:
                self.cancel_seconds = 10
                self.write_log(f'实盘撤单时间10秒')

    def sync_data(self):
        """同步更新数据"""
        if not self.backtesting:
            self.write_log(u'保存k线缓存数据')
            self.save_klines_to_cache()

        if self.inited and self.trading:
            self.write_log(u'保存policy数据')
            self.policy.save()


    def save_klines_to_cache(self, kline_names: list = []):
        """
        保存K线数据到缓存
        :param kline_names: 一般为self.klines的keys
        :return:
        """
        if len(kline_names) == 0:
            kline_names = list(self.klines.keys())

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

    def load_klines_from_cache(self, kline_names: list = []):
        """
        从缓存加载K线数据
        :param kline_names:
        :return:
        """
        if len(kline_names) == 0:
            kline_names = list(self.klines.keys())

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

    def get_klines_snapshot(self):
        """返回当前klines的切片数据"""
        try:
            d = {
                'strategy': self.strategy_name,
                'datetime': datetime.now()}
            klines = {}
            for kline_name in sorted(self.klines.keys()):
                klines.update({kline_name: self.klines.get(kline_name).get_data()})
            kline_names = list(klines.keys())
            binary_data = zlib.compress(pickle.dumps(klines))
            d.update({'kline_names': kline_names, 'klines': binary_data, 'zlib': True})
            return d
        except Exception as ex:
            self.write_error(f'获取klines切片数据失败:{str(ex)}')
            return {}

    def init_policy(self):
        self.write_log(f'{self.strategy_name} => init_policy(),初始化执行逻辑')
        if self.policy:
            self.policy.load()
            self.write_log(f'{self.strategy_name} => init_policy(),初始化执行逻辑完成')

    def init_position(self):
        """
        初始化Positin
        使用网格的持久化，获取开仓状态的多空单，更新
        :return:
        """
        self.write_log(u'init_position(),初始化持仓')
        changed = False
        if len(self.gt.up_grids) <= 0:
            self.position.short_pos = 0
            # 加载已开仓的空单数据，网格JSON
            short_grids = self.gt.load(direction=Direction.SHORT, open_status_filter=[True])
            if len(short_grids) == 0:
                self.write_log(u'没有持久化的空单数据')
                self.gt.up_grids = []

            else:
                self.gt.up_grids = short_grids
                for sg in short_grids:
                    if len(sg.order_ids) > 0 or sg.order_status:
                        self.write_log(f'重置委托状态:{sg.order_status},清除委托单：{sg.order_ids}')
                        sg.order_status = False
                        [self.cancel_order(vt_orderid) for vt_orderid in sg.order_ids]
                        sg.order_ids = []
                        changed = True

                    self.write_log(u'加载持仓空单[{},价格:{},数量:{}手,开仓时间:{}'
                                   .format(self.vt_symbol, sg.open_price,
                                           sg.volume, sg.open_time))
                    self.position.short_pos = round(self.position.short_pos - sg.volume, 7)

                self.write_log(u'持久化空单，共持仓:{}手'.format(abs(self.position.short_pos)))

        if len(self.gt.dn_grids) <= 0:
            # 加载已开仓的多数据，网格JSON
            self.position.long_pos = 0
            long_grids = self.gt.load(direction=Direction.LONG, open_status_filter=[True])
            if len(long_grids) == 0:
                self.write_log(u'没有持久化的多单数据')
                self.gt.dn_grids = []
            else:
                self.gt.dn_grids = long_grids
                for lg in long_grids:

                    if len(lg.order_ids) > 0 or lg.order_status:
                        self.write_log(f'重置委托状态:{lg.order_status},清除委托单：{lg.order_ids}')
                        lg.order_status = False
                        [self.cancel_order(vt_orderid) for vt_orderid in lg.order_ids]
                        lg.order_ids = []
                        changed = True

                    self.write_log(u'加载持仓多单[{},价格:{},数量:{}手, 开仓时间:{}'
                                   .format(self.vt_symbol, lg.open_price, lg.volume, lg.open_time))
                    self.position.long_pos = round(self.position.long_pos + lg.volume, 7)

                self.write_log(f'持久化多单，共持仓:{self.position.long_pos}手')

        self.position.pos = round(self.position.long_pos + self.position.short_pos, 7)

        self.write_log(u'{}加载持久化数据完成，多单:{}，空单:{},共:{}手'
                       .format(self.strategy_name,
                               self.position.long_pos,
                               abs(self.position.short_pos),
                               self.position.pos))
        self.pos = self.position.pos
        if changed:
            self.gt.save()
        self.display_grids()

    def get_positions(self):
        """
        获取策略当前持仓(重构，使用主力合约）
        :return: [{'vt_symbol':symbol,'direction':direction,'volume':volume]
        """
        if not self.position:
            return []
        pos_list = []

        if self.position.long_pos > 0:
            for g in self.gt.get_opened_grids(direction=Direction.LONG):
                pos_list.append({'vt_symbol': self.vt_symbol,
                                 'direction': 'long',
                                 'volume': g.volume - g.traded_volume,
                                 'price': g.open_price})

        if abs(self.position.short_pos) > 0:
            for g in self.gt.get_opened_grids(direction=Direction.SHORT):
                pos_list.append({'vt_symbol': self.vt_symbol,
                                 'direction': 'short',
                                 'volume': abs(g.volume - g.traded_volume),
                                 'price': g.open_price})

        if self.cur_datetime and (datetime.now() - self.cur_datetime).total_seconds() < 10:
            self.write_log(u'{}当前持仓:{}'.format(self.strategy_name, pos_list))
        return pos_list

    def on_trade(self, trade: TradeData):
        """交易更新"""
        self.write_log(u'{},交易更新:{},当前持仓：{} '
                       .format(self.cur_datetime,
                               trade.__dict__,
                               self.position.pos))

        dist_record = dict()
        if self.backtesting:
            dist_record['datetime'] = trade.time
        else:
            dist_record['datetime'] = ' '.join([self.cur_datetime.strftime('%Y-%m-%d'), trade.time])
        dist_record['volume'] = trade.volume
        dist_record['price'] = trade.price
        dist_record['margin'] = trade.price * trade.volume * self.cta_engine.get_margin_rate(trade.vt_symbol)
        dist_record['symbol'] = trade.vt_symbol

        if trade.direction == Direction.LONG and trade.offset == Offset.OPEN:
            dist_record['operation'] = 'buy'
            self.position.open_pos(trade.direction, volume=trade.volume)
            dist_record['long_pos'] = self.position.long_pos
            dist_record['short_pos'] = self.position.short_pos

        if trade.direction == Direction.SHORT and trade.offset == Offset.OPEN:
            dist_record['operation'] = 'short'
            self.position.open_pos(trade.direction, volume=trade.volume)
            dist_record['long_pos'] = self.position.long_pos
            dist_record['short_pos'] = self.position.short_pos

        if trade.direction == Direction.LONG and trade.offset != Offset.OPEN:
            dist_record['operation'] = 'cover'
            self.position.close_pos(trade.direction, volume=trade.volume)
            dist_record['long_pos'] = self.position.long_pos
            dist_record['short_pos'] = self.position.short_pos

        if trade.direction == Direction.SHORT and trade.offset != Offset.OPEN:
            dist_record['operation'] = 'sell'
            self.position.close_pos(trade.direction, volume=trade.volume)
            dist_record['long_pos'] = self.position.long_pos
            dist_record['short_pos'] = self.position.short_pos

        self.save_dist(dist_record)
        self.pos = self.position.pos

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
                if order.offset == Offset.OPEN:
                    self.write_error(u'{}委托单开{}被拒，price:{},total:{},traded:{}，status:{}'
                                     .format(order.vt_symbol, order.direction, order.price, order.volume,
                                             order.traded, order.status))
                    self.on_order_open_canceled(order)
                else:
                    self.write_error(u'OnOrder({})委托单平{}被拒，price:{},total:{},traded:{}，status:{}'
                                     .format(order.vt_symbol, order.direction, order.price, order.volume,
                                             order.traded, order.status))
                    self.on_order_close_canceled(order)
            else:
                self.write_log(u'委托单未完成,total:{},traded:{},tradeStatus:{}'
                               .format(order.volume, order.traded, order.status))
        else:
            self.write_error(u'委托单{}不在策略的未完成订单列表中:{}'.format(order.vt_orderid, self.active_orders))

    def on_order_all_traded(self, order: OrderData):
        """
        订单全部成交
        :param order:
        :return:
        """
        self.write_log(u'{},委托单:{}全部完成'.format(order.time, order.vt_orderid))
        order_info = self.active_orders[order.vt_orderid]

        # 通过vt_orderid，找到对应的网格
        grid = order_info.get('grid', None)
        if grid is not None:
            # 移除当前委托单
            if order.vt_orderid in grid.order_ids:
                grid.order_ids.remove(order.vt_orderid)

            # 网格的所有委托单已经执行完毕
            if len(grid.order_ids) == 0:
                grid.order_status = False
                grid.traded_volume = 0

                # 平仓完毕（cover， sell）
                if order.offset != Offset.OPEN:
                    grid.open_status = False
                    grid.close_status = True
                    if grid.volume < order.traded:
                        self.write_log(f'网格平仓数量{grid.volume}，小于委托单成交数量:{order.volume}，修正为:{order.volume}')
                        grid.volume = order.traded

                    self.write_log(f'{grid.direction.value}单已平仓完毕,order_price:{order.price}'
                                   + f',volume:{order.volume}')

                    self.write_log(f'移除网格:{grid.to_json()}')
                    self.gt.remove_grids_by_ids(direction=grid.direction, ids=[grid.id])

                # 开仓完毕( buy, short)
                else:
                    grid.open_status = True
                    grid.open_time = self.cur_datetime
                    self.write_log(f'{grid.direction.value}单已开仓完毕,order_price:{order.price}'
                                   + f',volume:{order.volume}')

                # 网格的所有委托单部分执行完毕
            else:
                old_traded_volume = grid.traded_volume
                grid.traded_volume += order.volume
                grid.traded_volume = round(grid.traded_volume, 7)

                self.write_log(f'{grid.direction.value}单部分{order.offset}仓，'
                               + f'网格volume:{grid.volume}, traded_volume:{old_traded_volume}=>{grid.traded_volume}')

                self.write_log(f'剩余委托单号:{grid.order_ids}')

            self.gt.save()
        # 在策略得活动订单中，移除
        self.write_log(f'委托单{order.vt_orderid}完成，从活动订单中移除')
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

        # 直接更新“未完成委托单”，更新volume,retry次数
        old_order = self.active_orders[order.vt_orderid]
        self.write_log(u'{} 委托信息:{}'.format(order.vt_orderid, old_order))
        old_order['traded'] = order.traded

        grid = old_order.get('grid', None)

        pre_status = old_order.get('status', Status.NOTTRADED)
        if pre_status == Status.CANCELLED:
            self.write_log(f'当前状态已经是{Status.CANCELLED}，不做调整处理')
            return

        old_order.update({'status': Status.CANCELLED})
        self.write_log(u'委托单状态:{}=>{}'.format(pre_status, old_order.get('status')))
        if grid:
            if order.vt_orderid in grid.order_ids:
                grid.order_ids.remove(order.vt_orderid)
            if order.traded > 0:
                pre_traded_volume = grid.traded_volume
                grid.traded_volume = round(grid.traded_volume + order.traded, 7)
                self.write_log(f'撤单中部分开仓:{order.traded} + 原已成交:{pre_traded_volume}  => {grid.traded_volume}')
            if len(grid.order_ids) == 0:
                grid.order_status = False
                if grid.traded_volume > 0:
                    pre_volume = grid.volume
                    grid.volume = grid.traded_volume
                    grid.traded_volume = 0
                    grid.open_status = True
                    self.write_log(f'开仓完成，grid.volume {pre_volume} => {grid.volume}')

            self.gt.save()
        self.active_orders.update({order.vt_orderid: old_order})

        self.display_grids()

    def on_order_close_canceled(self, order: OrderData):
        """委托平仓单撤销"""
        self.write_log(u'委托平仓单撤销:{}'.format(order.__dict__))

        if order.vt_orderid not in self.active_orders:
            self.write_error(u'{}不在未完成的委托单中:{}。'.format(order.vt_orderid, self.active_orders))
            return

        # 直接更新“未完成委托单”，更新volume,Retry次数
        old_order = self.active_orders[order.vt_orderid]
        self.write_log(u'{} 订单信息:{}'.format(order.vt_orderid, old_order))
        old_order['traded'] = order.traded

        grid = old_order.get('grid', None)
        pre_status = old_order.get('status', Status.NOTTRADED)
        if pre_status == Status.CANCELLED:
            self.write_log(f'当前状态已经是{Status.CANCELLED}，不做调整处理')
            return

        old_order.update({'status': Status.CANCELLED})
        self.write_log(u'委托单状态:{}=>{}'.format(pre_status, old_order.get('status')))
        if grid:
            if order.vt_orderid in grid.order_ids:
                grid.order_ids.remove(order.vt_orderid)
            if order.traded > 0:
                pre_traded_volume = grid.traded_volume
                grid.traded_volume = round(grid.traded_volume + order.traded, 7)
                self.write_log(f'撤单中部分平仓成交:{order.traded} + 原已成交:{pre_traded_volume}  => {grid.traded_volume}')
            if len(grid.order_ids) == 0:
                grid.order_status = False
                if grid.traded_volume > 0:
                    pre_volume = grid.volume
                    grid.volume = round(grid.volume - grid.traded_volume, 7)
                    grid.traded_volume = 0
                    if grid.volume <= 0:
                        grid.volume = 0
                        grid.open_status = False
                        self.write_log(f'强制全部平仓完成')
                    else:
                        self.write_log(f'平仓委托中，撤单完成，部分成交，减少持仓grid.volume {pre_volume} => {grid.volume}')

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
        if self.entrust != 0:
            return

        if not self.trading and not self.inited:
            self.write_error(u'当前不允许交易')
            return

        # 多单网格逐一止损/止盈检查：
        long_grids = self.gt.get_opened_grids(direction=Direction.LONG)
        for g in long_grids:
            if g.stop_price > 0 and g.stop_price > self.cur_price and g.open_status and not g.order_status:
                # 调用平仓模块
                self.write_log(u'{} {}当前价:{} 触发多单止损线{},开仓价:{},v：{}'.
                               format(self.cur_datetime,
                                      self.vt_symbol,
                                      self.cur_price,
                                      g.stop_price,
                                      g.open_price,
                                      g.volume))
                if self.grid_sell(g):
                    self.write_log(u'多单止盈/止损委托成功')
                else:
                    self.write_error(u'多单止损委托失败')

        # 空单网格止损检查
        short_grids = self.gt.get_opened_grids(direction=Direction.SHORT)
        for g in short_grids:
            if g.stop_price > 0 and g.stop_price < self.cur_price and g.open_status and not g.order_status:
                # 网格止损
                self.write_log(u'{} {}当前价:{} 触发空单止损线:{}, 开仓价:{},v：{}'.
                               format(self.cur_datetime, self.vt_symbol, self.cur_price, g.stop_price,
                                      g.open_price, g.volume))
                if self.grid_cover(g):
                    self.write_log(u'空单止盈/止损委托成功')
                else:
                    self.write_error(u'委托空单平仓失败')

    def grid_buy(self, grid, **kwargs):
        """
        事务开多仓
        :return:
        """
        if self.backtesting:
            buy_price = self.cur_price + self.price_tick
        else:
            buy_price = self.cur_tick.ask_price_1

        vt_orderids = self.buy(vt_symbol=self.vt_symbol,
                               price=buy_price,
                               volume=grid.volume,
                               order_type=self.order_type,
                               order_time=self.cur_datetime,
                               grid=grid)
        if len(vt_orderids) > 0:
            self.write_log(u'创建{}事务多单,开仓价：{}，数量：{}，止盈价:{},止损价:{}'
                           .format(grid.type, grid.open_price, grid.volume, grid.close_price, grid.stop_price))
            self.gt.dn_grids.append(grid)
            self.gt.save()
            return True
        else:
            self.write_error(u'创建{}事务多单,委托失败，开仓价：{}，数量：{}，止盈价:{}'
                             .format(grid.type, grid.open_price, grid.volume, grid.close_price))
            return False

    def grid_short(self, grid, **kwargs):
        """
        事务开空仓
        :return:
        """
        if self.backtesting:
            short_price = self.cur_price - self.price_tick
        else:
            short_price = self.cur_tick.bid_price_1
        vt_orderids = self.short(vt_symbol=self.vt_symbol,
                                 price=short_price,
                                 volume=grid.volume,
                                 order_type=self.order_type,
                                 order_time=self.cur_datetime,
                                 grid=grid)
        if len(vt_orderids) > 0:
            self.write_log(u'创建{}事务空单,事务开空价：{}，当前价:{},数量：{}，止盈价:{},止损价:{}'
                           .format(grid.type, grid.open_price, self.cur_price, grid.volume, grid.close_price,
                                   grid.stop_price))
            self.gt.up_grids.append(grid)
            self.gt.save()
            return True
        else:
            self.write_error(u'创建{}事务空单,委托失败,开仓价：{}，数量：{}，止盈价:{}'
                             .format(grid.type, grid.open_price, grid.volume, grid.close_price))
            return False

    def grid_sell(self, grid, **kwargs):
        """
        事务平多单仓位
        1.来源自止损止盈平仓
        :param 平仓网格
        :return:
        """
        self.write_log(u'执行事务平多仓位:{}'.format(grid.to_json()))
        """
        self.account_pos = self.cta_engine.get_position(
            vt_symbol=self.vt_symbol,
            direction=Direction.NET)

        if self.account_pos is None:
            self.write_error(u'无法获取{}得持仓信息'.format(self.vt_symbol))
            return False
        """
        # 发出委托卖出单
        if self.backtesting:
            sell_price = self.cur_price - self.price_tick
        else:
            sell_price = self.cur_tick.bid_price_1

        # 发出平多委托
        if grid.traded_volume > 0:
            grid.volume -= grid.traded_volume
            grid.volume = round(grid.volume, 7)
            grid.traded_volume = 0

        """
        if self.account_pos.volume <= 0:
            self.write_error(u'当前{}的净持仓:{}，不能平多单'
                             .format(self.vt_symbol,
                                     self.account_pos.volume))
            return False
        if self.account_pos.volume < grid.volume:
            self.write_error(u'当前{}的净持仓:{}，不满足平仓目标:{}, 强制降低'
                             .format(self.vt_symbol,
                                     self.account_pos.volume,
                                     grid.volume))

            grid.volume = self.account_pos.volume
        """
        vt_orderids = self.sell(
            vt_symbol=self.vt_symbol,
            price=sell_price,
            volume=grid.volume,
            order_type=self.order_type,
            order_time=self.cur_datetime,
            grid=grid)
        if len(vt_orderids) == 0:
            if self.backtesting:
                self.write_error(u'多单平仓委托失败')
            else:
                self.write_error(u'多单平仓委托失败')
            return False
        else:
            self.write_log(u'多单平仓委托成功，编号:{}'.format(vt_orderids))

            return True

    def grid_cover(self, grid, **kwargs):
        """
        事务平空单仓位
        1.来源自止损止盈平仓
        :param 平仓网格
        :return:
        """
        self.write_log(u'执行事务平空仓位:{}'.format(grid.to_json()))
        """
        self.account_pos = self.cta_engine.get_position(
            vt_symbol=self.vt_symbol,
            direction=Direction.NET)
        if self.account_pos is None:
            self.write_error(u'无法获取{}得持仓信息'.format(self.vt_symbol))
            return False
        """
        # 发出委托单
        if self.backtesting:
            cover_price = self.cur_price + self.price_tick
        else:
            cover_price = self.cur_tick.ask_price_1

        # 发出cover委托
        if grid.traded_volume > 0:
            grid.volume -= grid.traded_volume
            grid.volume = round(grid.volume, 7)
            grid.traded_volume = 0

        """
        if self.account_pos.volume >= 0:
            self.write_error(u'当前{}的净持仓:{}，不能平空单'
                             .format(self.vt_symbol,
                                     self.account_pos.volume))
            return False
        if abs(self.account_pos.volume) < grid.volume:
            self.write_error(u'当前{}的净持仓:{}，不满足平仓目标:{}, 强制降低'
                             .format(self.vt_symbol,
                                     self.account_pos.volume,
                                     grid.volume))

            grid.volume = abs(self.account_pos.volume)
        """
        vt_orderids = self.cover(
            price=cover_price,
            vt_symbol=self.vt_symbol,
            volume=grid.volume,
            order_type=self.order_type,
            order_time=self.cur_datetime,
            grid=grid)

        if len(vt_orderids) == 0:
            if self.backtesting:
                self.write_error(u'空单平仓委托失败')
            else:
                self.write_error(u'空单平仓委托失败')
            return False
        else:
            self.write_log(u'空单平仓委托成功，编号:{}'.format(vt_orderids))
            return True

    def cancel_all_orders(self):
        """
        重载撤销所有正在进行得委托
        :return:
        """
        self.write_log(u'撤销所有正在进行得委托')
        self.tns_cancel_logic(dt=datetime.now(), force=True, reopen=False)

    def tns_cancel_logic(self, dt, force=False, reopen=False):
        "撤单逻辑"""
        if len(self.active_orders) < 1:
            self.entrust = 0
            return

        canceled_ids = []

        for vt_orderid in list(self.active_orders.keys()):
            order_info = self.active_orders[vt_orderid]
            order_vt_symbol = order_info.get('vt_symbol', self.vt_symbol)
            order_time = order_info['order_time']
            order_volume = order_info['volume'] - order_info['traded']
            order_grid = order_info['grid']
            order_status = order_info.get('status', Status.NOTTRADED)
            order_type = order_info.get('order_type', OrderType.LIMIT)
            over_seconds = (dt - order_time).total_seconds()

            # 只处理未成交的限价委托单
            if order_status in [Status.SUBMITTING, Status.NOTTRADED] and order_type == OrderType.LIMIT:
                if over_seconds > self.cancel_seconds or force:  # 超过设置的时间还未成交
                    self.write_log(u'{}超时{}秒未成交，取消委托单：vt_orderid:{},order:{}'
                                   .format(order_vt_symbol, over_seconds, vt_orderid, order_info))
                    order_info.update({'status': Status.CANCELLING})
                    self.active_orders.update({vt_orderid: order_info})
                    ret = self.cancel_order(str(vt_orderid))
                    if not ret:
                        self.write_log(f'{order_vt_symbol}撤单失败,更新状态为撤单成功')
                        order_info.update({'status': Status.CANCELLED})
                        self.active_orders.update({vt_orderid: order_info})
                        if order_grid and vt_orderid in order_grid.order_ids:
                            order_grid.order_ids.remove(vt_orderid)

                continue

            # 处理状态为‘撤销’的委托单
            elif order_status == Status.CANCELLED:
                self.write_log(u'委托单{}已成功撤单，删除{}'.format(vt_orderid, order_info))
                canceled_ids.append(vt_orderid)

                if reopen:
                    # 撤销的委托单，属于开仓类，需要重新委托
                    if order_info['offset'] == Offset.OPEN:
                        self.write_log(u'超时撤单后，重新开仓')
                        # 开空委托单
                        if order_info['direction'] == Direction.SHORT:
                            if self.backtesting:
                                short_price = self.cur_price - self.price_tick
                            else:
                                short_price = self.cur_tick.bid_price_1
                            if order_grid.volume != order_volume and order_volume > 0:
                                self.write_log(
                                    u'网格volume:{},order_volume:{}不一致，修正'.format(order_grid.volume, order_volume))
                                order_grid.volume = order_volume

                            self.write_log(u'重新提交{}开空委托,开空价{}，v:{}'.format(order_vt_symbol, short_price, order_volume))
                            vt_orderids = self.short(price=short_price,
                                                     volume=order_volume,
                                                     vt_symbol=order_vt_symbol,
                                                     order_type=order_type,
                                                     order_time=self.cur_datetime,
                                                     grid=order_grid)

                            if len(vt_orderids) > 0:
                                self.write_log(u'委托成功，orderid:{}'.format(vt_orderids))
                                order_grid.snapshot.update({'open_price': short_price})
                            else:
                                self.write_error(u'撤单后，重新委托开空仓失败')
                        else:
                            if self.backtesting:
                                buy_price = self.cur_price + self.price_tick
                            else:
                                buy_price = self.cur_tick.ask_price_1
                            if order_grid.volume != order_volume and order_volume > 0:
                                self.write_log(
                                    u'网格volume:{},order_volume:{}不一致，修正'.format(order_grid.volume, order_volume))
                                order_grid.volume = order_volume

                            self.write_log(u'重新提交{}开多委托,开多价{}，v:{}'.format(order_vt_symbol, buy_price, order_volume))
                            vt_orderids = self.buy(price=buy_price,
                                                   volume=order_volume,
                                                   vt_symbol=order_vt_symbol,
                                                   order_type=order_type,
                                                   order_time=self.cur_datetime,
                                                   grid=order_grid)

                            if len(vt_orderids) > 0:
                                self.write_log(u'委托成功，orderids:{}'.format(vt_orderids))
                                order_grid.snapshot.update({'open_price': buy_price})
                            else:
                                self.write_error(u'撤单后，重新委托开多仓失败')
                    else:
                        # 属于平多委托单
                        if order_info['direction'] == Direction.SHORT:
                            if self.backtesting:
                                sell_price = self.cur_price - self.price_tick
                            else:
                                sell_price = self.cur_tick.bid_price_1
                            self.write_log(u'重新提交{}平多委托,{}，v:{}'.format(order_vt_symbol, sell_price, order_volume))
                            vt_orderids = self.sell(price=sell_price,
                                                    volume=order_volume,
                                                    vt_symbol=order_vt_symbol,
                                                    order_type=order_type,
                                                    order_time=self.cur_datetime,
                                                    grid=order_grid)
                            if len(vt_orderids) > 0:
                                self.write_log(u'委托成功，orderids:{}'.format(vt_orderids))
                            else:
                                self.write_error(u'撤单后，重新委托平多仓失败')
                        # 属于平空委托单
                        else:
                            if self.backtesting:
                                cover_price = self.cur_price + self.price_tick
                            else:
                                cover_price = self.cur_tick.ask_price_1
                            self.write_log(u'重新提交{}平空委托,委托价{}，v:{}'.format(order_vt_symbol, cover_price, order_volume))
                            vt_orderids = self.cover(price=cover_price,
                                                     volume=order_volume,
                                                     vt_symbol=order_vt_symbol,
                                                     order_type=order_type,
                                                     order_time=self.cur_datetime,
                                                     grid=order_grid)
                            if len(vt_orderids) > 0:
                                self.write_log(u'委托成功，orderids:{}'.format(vt_orderids))
                            else:
                                self.write_error(u'撤单后，重新委托平空仓失败')
                else:
                    if order_info['offset'] == Offset.OPEN \
                            and order_grid \
                            and len(order_grid.order_ids) == 0 \
                            and not order_grid.open_status \
                            and not order_grid.order_status \
                            and order_grid.traded_volume == 0:
                        self.write_log(u'移除从未开仓成功的委托网格{}'.format(order_grid.__dict__))
                        order_info['grid'] = None
                        self.gt.remove_grids_by_ids(direction=order_grid.direction, ids=[order_grid.id])

        # 删除撤单的订单
        for vt_orderid in canceled_ids:
            self.write_log(f'活动订单撤单成功，移除{vt_orderid}')
            self.active_orders.pop(vt_orderid, None)

        if len(self.active_orders) == 0:
            self.entrust = 0

    def display_grids(self):
        """更新网格显示信息"""
        if not self.inited:
            return
        self.account_pos = self.cta_engine.get_position(vt_symbol=self.vt_symbol, direction=Direction.NET)
        if self.account_pos:
            self.write_log(
                f'账号{self.vt_symbol}持仓:{self.account_pos.volume}, 冻结:{self.account_pos.frozen}, 盈亏:{self.account_pos.pnl}')

        up_grids_info = ""
        for grid in list(self.gt.up_grids):
            if grid.open_status and grid.order_status:
                up_grids_info += f'平空中: [已平:{grid.traded_volume} => 目标:{grid.volume}, 委托时间:{grid.order_time}]\n'
                if len(grid.order_ids) > 0:
                    up_grids_info += f'委托单号:{grid.order_ids}'
                continue

            if grid.open_status and not grid.order_status:
                up_grids_info += f'持空中: [数量:{grid.volume}, 开仓时间:{grid.open_time}]\n'
                continue

            if not grid.open_status and grid.order_status:
                up_grids_info += f'开空中: [已开:{grid.traded_volume} => 目标:{grid.volume}, 委托时间:{grid.order_time}]\n'
                if len(grid.order_ids) > 0:
                    up_grids_info += f'委托单号:{grid.order_ids}'

        dn_grids_info = ""
        for grid in list(self.gt.dn_grids):
            if grid.open_status and grid.order_status:
                dn_grids_info += f'平多中: [已平:{grid.traded_volume} => 目标:{grid.volume}, 委托时间:{grid.order_time}]\n'
                if len(grid.order_ids) > 0:
                    dn_grids_info += f'委托单号:{grid.order_ids}'
                continue

            if grid.open_status and not grid.order_status:
                dn_grids_info += f'持多中: [数量:{grid.volume}, 开仓价:{grid.open_price},开仓时间:{grid.open_time}]\n'
                continue

            if not grid.open_status and grid.order_status:
                dn_grids_info += f'开多中: [已开:{grid.traded_volume} => 目标:{grid.volume}, 委托时间:{grid.order_time}]\n'
                if len(grid.order_ids) > 0:
                    dn_grids_info += f'委托单号:{grid.order_ids}'

        if len(up_grids_info) > 0:
            self.write_log(up_grids_info)
        if len(dn_grids_info) > 0:
            self.write_log(dn_grids_info)

    def display_tns(self):
        """显示事务的过程记录=》 log"""
        if not self.inited:
            return
        self.write_log(u'{} 当前 {}价格：{}, 委托状态:{}'
                       .format(self.cur_datetime, self.vt_symbol, self.cur_price, self.entrust))

        if len(self.active_orders) > 0:
            self.write_log('当前活动订单:{}'.format(self.active_orders))

        if hasattr(self, 'policy'):
            policy = getattr(self, 'policy')
            if policy:
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
            if 'margin' not in dist_data:
                dist_data.update({'margin': dist_data.get('price', 0) * dist_data.get('volume',
                                                                                      0) * self.cta_engine.get_margin_rate(
                    dist_data.get('symbol', self.vt_symbol))})
            if 'datetime' not in dist_data:
                dist_data.update({'datetime': self.cur_datetime})
            if self.position and 'long_pos' not in dist_data:
                dist_data.update({'long_pos': self.position.long_pos})
            if self.position and 'short_pos' not in dist_data:
                dist_data.update({'short_pos': self.position.short_pos})

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


class CtaSpotTemplate(CtaTemplate):
    """
    现货模板
    """

    asset_symbol = ""  # 资产币  BTCUSDT => BTC
    quote_symbol = ""  # 定价币  BTCUSDT => USDT

    price_tick = 0.01  # 商品的最小价格跳动
    symbol_size = 1  # 商品得合约乘数
    margin_rate = 1  # 商品的保证金
    volumn_tick = 0.01  # 商品最小成交数量

    # 委托类型
    order_type = OrderType.LIMIT
    cancel_seconds = 120  # 撤单时间(秒)
    activate_market = False

    # 资金相关
    max_invest_rate = 0.1  # 最大仓位(0~1)   asset / virtual_quote
    max_invest_margin = 0  # 资金上限 0，不限制 virtual_quote
    max_invest_pos = 0  # 单向头寸数量上限 0，不限制 asset

    # 是否回测状态
    backtesting = False

    # 逻辑过程日志
    dist_fieldnames = ['datetime', 'symbol', 'volume', 'price', 'margin',
                       'operation', 'signal', 'stop_price', 'target_price',
                       'long_pos']

    def __init__(self, cta_engine, strategy_name, vt_symbol, setting):
        """"""

        # vt_symbol => symbol, exchange
        self.symbol, self.exchange = extract_vt_symbol(vt_symbol)

        self.position = None  # 仓位组件
        self.policy = None  # 事务执行组件
        self.gt = None  # 网格交易组件
        self.klines = {}  # K线组件字典: kline_name: kline

        self.price_tick = 0.01  # 商品的最小价格跳动
        self.symbol_size = 1  # 商品得合约乘数
        self.margin_rate = 1  # 商品的保证金
        self.volumn_tick = 0.01  # 商品最小成交数量
        self.cancel_seconds = 120  # 撤单时间(秒)
        self.activate_market = False
        self.order_type = OrderType.LIMIT
        self.backtesting = False

        self.cur_datetime: datetime = None  # 当前Tick时间
        self.cur_tick: TickData = None  # 最新的合约tick( vt_symbol)
        self.cur_price = None  # 当前价（主力合约 vt_symbol)
        self.asset_pos = None  # 当前asset_symbol持仓信息
        self.quote_pos = None  # 当前quote_symbol的持仓信息

        self.last_minute = None  # 最后的分钟,用于on_tick内每分钟处理的逻辑
        self.display_bars = True

        super().__init__(
            cta_engine, strategy_name, vt_symbol, setting
        )

        # 增加仓位管理模块
        self.position = CtaPosition(strategy=self)
        self.position.maxPos = sys.maxsize
        # 增加网格持久化模块
        self.gt = CtaGridTrade(strategy=self)

        if 'backtesting' not in self.parameters:
            self.parameters.append('backtesting')

    def update_setting(self, setting: dict):
        """
        Update strategy parameter wtih value in setting dict.
        """
        for name in self.parameters:
            if name in setting:
                setattr(self, name, setting[name])

        self.price_tick = self.cta_engine.get_price_tick(self.vt_symbol)
        self.symbol_size = self.cta_engine.get_size(self.vt_symbol)
        self.margin_rate = self.cta_engine.get_margin_rate(self.vt_symbol)
        self.volumn_tick = self.cta_engine.get_volume_tick(self.vt_symbol)

        # 检查资产币+定价币是否与vt_symbol一致
        if self.symbol != f'{self.asset_symbol}{self.quote_symbol}':
            raise Exception(f'{self.vt_symbol}与{self.asset_symbol}+{self.quote_symbol}不匹配')

        if self.activate_market:
            self.write_log(f'{self.strategy_name}使用市价单委托方式')
            self.order_type = OrderType.MARKET


    def sync_data(self):
        """同步更新数据"""
        if not self.backtesting:
            self.write_log(u'保存k线缓存数据')
            self.save_klines_to_cache()

        if self.inited and self.trading:
            self.write_log(u'保存policy数据')
            self.policy.save()

    def save_klines_to_cache(self, kline_names: list = []):
        """
        保存K线数据到缓存
        :param kline_names: 一般为self.klines的keys
        :return:
        """
        if len(kline_names) == 0:
            kline_names = list(self.klines.keys())

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

    def load_klines_from_cache(self, kline_names: list = []):
        """
        从缓存加载K线数据
        :param kline_names:
        :return:
        """
        if len(kline_names) == 0:
            kline_names = list(self.klines.keys())

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

    def get_klines_snapshot(self):
        """返回当前klines的切片数据"""
        try:
            d = {
                'strategy': self.strategy_name,
                'datetime': datetime.now()}
            klines = {}
            for kline_name in sorted(self.klines.keys()):
                klines.update({kline_name: self.klines.get(kline_name).get_data()})
            kline_names = list(klines.keys())
            binary_data = zlib.compress(pickle.dumps(klines))
            d.update({'kline_names': kline_names, 'klines': binary_data, 'zlib': True})
            return d
        except Exception as ex:
            self.write_error(f'获取klines切片数据失败:{str(ex)}')
            return {}

    def init_policy(self):
        self.write_log(u'init_policy(),初始化执行逻辑')
        if self.policy:
            self.policy.load()

    def init_position(self):
        """
        初始化Positin
        使用网格的持久化，获取开仓状态的多空单，更新
        :return:
        """
        self.write_log(u'init_position(),初始化持仓')
        changed = False


        if len(self.gt.dn_grids) <= 0:
            # 加载已开仓的多数据，网格JSON
            self.position.long_pos = 0
            long_grids = self.gt.load(direction=Direction.LONG, open_status_filter=[True])
            if len(long_grids) == 0:
                self.write_log(u'没有持久化的多单数据')
                self.gt.dn_grids = []
            else:
                self.gt.dn_grids = long_grids
                for lg in long_grids:

                    if len(lg.order_ids) > 0 or lg.order_status:
                        self.write_log(f'重置委托状态:{lg.order_status},清除委托单：{lg.order_ids}')
                        lg.order_status = False
                        [self.cancel_order(vt_orderid) for vt_orderid in lg.order_ids]
                        lg.order_ids = []
                        changed = True

                    self.write_log(u'加载持仓多单[{},价格:{},数量:{}手, 开仓时间:{}'
                                   .format(lg.vt_symbol, lg.open_price, lg.volume, lg.open_time))
                    self.position.long_pos = round(self.position.long_pos + lg.volume, 7)

                self.write_log(f'持久化多单，共持仓:{self.position.long_pos}手')

        self.position.pos = round(self.position.long_pos + self.position.short_pos, 7)

        self.write_log(u'{}加载持久化数据完成，多单:{}，空单:{},共:{}手'
                       .format(self.strategy_name,
                               self.position.long_pos,
                               abs(self.position.short_pos),
                               self.position.pos))
        self.pos = self.position.pos
        if changed:
            self.gt.save()
        self.display_grids()

    def get_positions(self):
        """
        获取策略当前持仓(重构，使用主力合约）
        :return: [{'vt_symbol':symbol,'direction':direction,'volume':volume]
        """
        if not self.position:
            return []
        pos_list = []

        if self.position.long_pos > 0:
            for g in self.gt.get_opened_grids(direction=Direction.LONG):
                pos_list.append({'vt_symbol': f'{self.asset_symbol}.{self.exchange.value}',
                                 'direction': 'long',
                                 'volume': g.volume - g.traded_volume,
                                 'price': g.open_price})

        if self.cur_datetime and (datetime.now() - self.cur_datetime).total_seconds() < 10:
            self.write_log(u'{}当前持仓:{}'.format(self.strategy_name, pos_list))
        return pos_list

    def on_trade(self, trade: TradeData):
        """交易更新"""
        self.write_log(u'{},交易更新:{},当前持仓：{} '
                       .format(self.cur_datetime,
                               trade.__dict__,
                               self.position.pos))

        dist_record = dict()
        if self.backtesting:
            dist_record['datetime'] = trade.time
        else:
            dist_record['datetime'] = ' '.join([self.cur_datetime.strftime('%Y-%m-%d'), trade.time])
        dist_record['volume'] = trade.volume
        dist_record['price'] = trade.price
        dist_record['margin'] = trade.price * trade.volume * self.cta_engine.get_margin_rate(trade.vt_symbol)
        dist_record['symbol'] = trade.vt_symbol

        if trade.direction == Direction.LONG and trade.offset == Offset.OPEN:
            dist_record['operation'] = 'buy'
            self.position.open_pos(trade.direction, volume=trade.volume)
            dist_record['long_pos'] = self.position.long_pos
            dist_record['short_pos'] = self.position.short_pos

        if trade.direction == Direction.SHORT and trade.offset == Offset.OPEN:
            dist_record['operation'] = 'short'
            self.position.open_pos(trade.direction, volume=trade.volume)
            dist_record['long_pos'] = self.position.long_pos
            dist_record['short_pos'] = self.position.short_pos

        if trade.direction == Direction.LONG and trade.offset != Offset.OPEN:
            dist_record['operation'] = 'cover'
            self.position.close_pos(trade.direction, volume=trade.volume)
            dist_record['long_pos'] = self.position.long_pos
            dist_record['short_pos'] = self.position.short_pos

        if trade.direction == Direction.SHORT and trade.offset != Offset.OPEN:
            dist_record['operation'] = 'sell'
            self.position.close_pos(trade.direction, volume=trade.volume)
            dist_record['long_pos'] = self.position.long_pos
            dist_record['short_pos'] = self.position.short_pos

        self.save_dist(dist_record)
        self.pos = self.position.pos

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
                if order.offset == Offset.OPEN:
                    self.write_error(u'{}委托单开{}被拒，price:{},total:{},traded:{}，status:{}'
                                     .format(order.vt_symbol, order.direction, order.price, order.volume,
                                             order.traded, order.status))
                    self.on_order_open_canceled(order)
                else:
                    self.write_error(u'OnOrder({})委托单平{}被拒，price:{},total:{},traded:{}，status:{}'
                                     .format(order.vt_symbol, order.direction, order.price, order.volume,
                                             order.traded, order.status))
                    self.on_order_close_canceled(order)
            else:
                self.write_log(u'委托单未完成,total:{},traded:{},tradeStatus:{}'
                               .format(order.volume, order.traded, order.status))
        else:
            self.write_error(u'委托单{}不在策略的未完成订单列表中:{}'.format(order.vt_orderid, self.active_orders))

    def on_order_all_traded(self, order: OrderData):
        """
        订单全部成交
        :param order:
        :return:
        """
        self.write_log(u'{},委托单:{}全部完成'.format(order.time, order.vt_orderid))
        order_info = self.active_orders[order.vt_orderid]

        # 通过vt_orderid，找到对应的网格
        grid = order_info.get('grid', None)
        if grid is not None:
            # 移除当前委托单
            if order.vt_orderid in grid.order_ids:
                grid.order_ids.remove(order.vt_orderid)

            # 网格的所有委托单已经执行完毕
            if len(grid.order_ids) == 0:
                grid.order_status = False
                grid.traded_volume = 0

                # 平仓完毕（cover， sell）
                if order.offset != Offset.OPEN:
                    grid.open_status = False
                    grid.close_status = True
                    if grid.volume < order.traded:
                        self.write_log(f'网格平仓数量{grid.volume}，小于委托单成交数量:{order.volume}，修正为:{order.volume}')
                        grid.volume = order.traded

                    self.write_log(f'{grid.direction.value}单已平仓完毕,order_price:{order.price}'
                                   + f',volume:{order.volume}')

                    self.write_log(f'移除网格:{grid.to_json()}')
                    self.gt.remove_grids_by_ids(direction=grid.direction, ids=[grid.id])

                # 开仓完毕( buy, short)
                else:
                    grid.open_status = True
                    grid.open_time = self.cur_datetime
                    self.write_log(f'{grid.direction.value}单已开仓完毕,order_price:{order.price}'
                                   + f',volume:{order.volume}')

                # 网格的所有委托单部分执行完毕
            else:
                old_traded_volume = grid.traded_volume
                grid.traded_volume += order.volume
                grid.traded_volume = round(grid.traded_volume, 7)

                self.write_log(f'{grid.direction.value}单部分{order.offset}仓，'
                               + f'网格volume:{grid.volume}, traded_volume:{old_traded_volume}=>{grid.traded_volume}')

                self.write_log(f'剩余委托单号:{grid.order_ids}')

            self.gt.save()
        # 在策略得活动订单中，移除
        self.write_log(f'委托单{order.vt_orderid}完成，从活动订单中移除')
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

        # 直接更新“未完成委托单”，更新volume,retry次数
        old_order = self.active_orders[order.vt_orderid]
        self.write_log(u'{} 委托信息:{}'.format(order.vt_orderid, old_order))
        old_order['traded'] = order.traded

        grid = old_order.get('grid', None)

        pre_status = old_order.get('status', Status.NOTTRADED)
        if pre_status == Status.CANCELLED:
            self.write_log(f'当前状态已经是{Status.CANCELLED}，不做调整处理')
            return

        old_order.update({'status': Status.CANCELLED})
        self.write_log(u'委托单状态:{}=>{}'.format(pre_status, old_order.get('status')))
        if grid:
            if order.vt_orderid in grid.order_ids:
                grid.order_ids.remove(order.vt_orderid)
            if order.traded > 0:
                pre_traded_volume = grid.traded_volume
                grid.traded_volume = round(grid.traded_volume + order.traded, 7)
                self.write_log(f'撤单中部分开仓:{order.traded} + 原已成交:{pre_traded_volume}  => {grid.traded_volume}')
            if len(grid.order_ids) == 0:
                grid.order_status = False
                if grid.traded_volume > 0:
                    pre_volume = grid.volume
                    grid.volume = grid.traded_volume
                    grid.traded_volume = 0
                    grid.open_status = True
                    self.write_log(f'开仓完成，grid.volume {pre_volume} => {grid.volume}')

            self.gt.save()
        self.active_orders.update({order.vt_orderid: old_order})

        self.display_grids()

    def on_order_close_canceled(self, order: OrderData):
        """委托平仓单撤销"""
        self.write_log(u'委托平仓单撤销:{}'.format(order.__dict__))

        if order.vt_orderid not in self.active_orders:
            self.write_error(u'{}不在未完成的委托单中:{}。'.format(order.vt_orderid, self.active_orders))
            return

        # 直接更新“未完成委托单”，更新volume,Retry次数
        old_order = self.active_orders[order.vt_orderid]
        self.write_log(u'{} 订单信息:{}'.format(order.vt_orderid, old_order))
        old_order['traded'] = order.traded

        grid = old_order.get('grid', None)
        pre_status = old_order.get('status', Status.NOTTRADED)
        if pre_status == Status.CANCELLED:
            self.write_log(f'当前状态已经是{Status.CANCELLED}，不做调整处理')
            return

        old_order.update({'status': Status.CANCELLED})
        self.write_log(u'委托单状态:{}=>{}'.format(pre_status, old_order.get('status')))
        if grid:
            if order.vt_orderid in grid.order_ids:
                grid.order_ids.remove(order.vt_orderid)
            if order.traded > 0:
                pre_traded_volume = grid.traded_volume
                grid.traded_volume = round(grid.traded_volume + order.traded, 7)
                self.write_log(f'撤单中部分平仓成交:{order.traded} + 原已成交:{pre_traded_volume}  => {grid.traded_volume}')
            if len(grid.order_ids) == 0:
                grid.order_status = False
                if grid.traded_volume > 0:
                    pre_volume = grid.volume
                    grid.volume = round(grid.volume - grid.traded_volume, 7)
                    grid.traded_volume = 0
                    if grid.volume <= 0:
                        grid.volume = 0
                        grid.open_status = False
                        self.write_log(f'强制全部平仓完成')
                    else:
                        self.write_log(f'平仓委托中，撤单完成，部分成交，减少持仓grid.volume {pre_volume} => {grid.volume}')

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
        if self.entrust != 0:
            return

        if not self.trading and not self.inited:
            self.write_error(u'当前不允许交易')
            return

        # 多单网格逐一止损/止盈检查：
        long_grids = self.gt.get_opened_grids(direction=Direction.LONG)
        for g in long_grids:
            if g.stop_price > 0 and g.stop_price > self.cur_price and g.open_status and not g.order_status:
                # 调用平仓模块
                self.write_log(u'{} {}当前价:{} 触发多单止损线{},开仓价:{},v：{}'.
                               format(self.cur_datetime,
                                      g.vt_symbol,
                                      self.cur_price,
                                      g.stop_price,
                                      g.open_price,
                                      g.volume))
                if self.grid_sell(g):
                    self.write_log(u'多单止盈/止损委托成功')
                else:
                    self.write_error(u'多单止损委托失败')


    def grid_buy(self, grid):
        """
        事务开多仓
        :return:
        """
        if self.backtesting:
            buy_price = self.cur_price + self.price_tick
        else:
            buy_price = self.cur_tick.ask_price_1

        if self.quote_pos is None:
            self.write_error(u'无法获取{}得持仓信息'.format(self.quote_symbol))
            return False

        vt_orderids = self.buy(vt_symbol=self.vt_symbol,
                               price=buy_price,
                               volume=grid.volume,
                               order_type=self.order_type,
                               order_time=self.cur_datetime,
                               grid=grid)
        if len(vt_orderids) > 0:
            self.write_log(u'创建{}事务多单,开仓价：{}，数量：{}，止盈价:{},止损价:{}'
                           .format(grid.type, grid.open_price, grid.volume, grid.close_price, grid.stop_price))

            self.gt.save()
            return True
        else:
            self.write_error(u'创建{}事务多单,委托失败，开仓价：{}，数量：{}，止盈价:{}'
                             .format(grid.type, grid.open_price, grid.volume, grid.close_price))
            return False


    def grid_sell(self, grid):
        """
        事务平多单仓位
        1.来源自止损止盈平仓
        :param 平仓网格
        :return:
        """
        self.write_log(u'执行事务平多仓位:{}'.format(grid.to_json()))

        if self.asset_pos is None:
            self.write_error(u'无法获取{}得持仓信息'.format(self.asset_symbol))
            return False

        # 发出委托卖出单
        if self.backtesting:
            sell_price = self.cur_price - self.price_tick
        else:
            sell_price = self.cur_tick.bid_price_1

        # 发出平多委托
        if grid.traded_volume > 0:
            grid.volume -= grid.traded_volume
            grid.volume = round(grid.volume, 7)
            grid.traded_volume = 0

        if self.asset_pos.volume <= 0:
            self.write_error(u'当前{}的净持仓:{}，不能平多单'
                             .format(self.asset_symbol,
                                     self.asset_pos.volume))
            return False

        if self.asset_pos.volume < grid.volume:
            self.write_error(u'当前{}的净持仓:{}，不满足平仓目标:{}, 强制降低'
                             .format(self.asset_symbol,
                                     self.asset_pos.volume,
                                     grid.volume))

            grid.volume = self.asset_pos.volume

        vt_orderids = self.sell(
            vt_symbol=self.vt_symbol,
            price=sell_price,
            volume=grid.volume,
            order_type=self.order_type,
            order_time=self.cur_datetime,
            grid=grid)
        if len(vt_orderids) == 0:
            if self.backtesting:
                self.write_error(u'多单平仓委托失败')
            else:
                self.write_error(u'多单平仓委托失败')
            return False
        else:
            self.write_log(u'多单平仓委托成功，编号:{}'.format(vt_orderids))

            return True


    def cancel_all_orders(self):
        """
        重载撤销所有正在进行得委托
        :return:
        """
        self.write_log(u'撤销所有正在进行得委托')
        self.tns_cancel_logic(dt=datetime.now(), force=True, reopen=False)

    def tns_cancel_logic(self, dt, force=False, reopen=False):
        "撤单逻辑"""
        if len(self.active_orders) < 1:
            self.entrust = 0
            return

        canceled_ids = []

        for vt_orderid in list(self.active_orders.keys()):
            order_info = self.active_orders[vt_orderid]
            order_vt_symbol = order_info.get('vt_symbol', self.vt_symbol)
            order_time = order_info['order_time']
            order_volume = order_info['volume'] - order_info['traded']
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
                        self.write_log(u'撤单失败,更新状态为撤单成功')
                        order_info.update({'status': Status.CANCELLED})
                        self.active_orders.update({vt_orderid: order_info})
                        if order_grid and vt_orderid in order_grid.order_ids:
                            order_grid.order_ids.remove(vt_orderid)

                continue

            # 处理状态为‘撤销’的委托单
            elif order_status == Status.CANCELLED:
                self.write_log(u'委托单{}已成功撤单，删除{}'.format(vt_orderid, order_info))
                canceled_ids.append(vt_orderid)

                if order_info['offset'] == Offset.OPEN \
                        and order_grid \
                        and len(order_grid.order_ids) == 0 \
                        and not order_grid.open_status \
                        and not order_grid.order_status \
                        and order_grid.traded_volume == 0:
                    self.write_log(u'移除从未开仓成功的委托网格{}'.format(order_grid.__dict__))
                    order_info['grid'] = None
                    self.gt.remove_grids_by_ids(direction=order_grid.direction, ids=[order_grid.id])

        # 删除撤单的订单
        for vt_orderid in canceled_ids:
            self.write_log(f'活动订单撤单成功，移除{vt_orderid}')
            self.active_orders.pop(vt_orderid, None)

        if len(self.active_orders) == 0:
            self.entrust = 0

    def display_grids(self):
        """更新网格显示信息"""
        if not self.inited:
            return

        self.assett_pos = self.cta_engine.get_position(vt_symbol=f'{self.asset_symbol}.{self.exchange.value}', direction=Direction.NET)
        if self.asset_pos:
            self.write_log(
                f'账号{self.asset_symbol}持仓:{self.asset_pos.volume}, 冻结:{self.asset_pos.frozen}')

        self.quote_pos = self.cta_engine.get_position(vt_symbol=f'{self.quote_symbol}.{self.exchange.value}', direction=Direction.NET)
        if self.quote_pos:
            self.write_log(
                f'账号{self.quote_symbol}持仓:{self.quote_pos.volume}, 冻结:{self.quote_pos.frozen}')

        dn_grids_info = ""
        for grid in list(self.gt.dn_grids):
            if grid.close_status and not grid.open_status and grid.order_status:
                dn_grids_info += f'平多中: {grid.vt_symbol}[已平:{grid.traded_volume} => 目标:{grid.volume}, 平仓价格:{grid.close_price},委托时间:{grid.order_time}]\n'
                if len(grid.order_ids) > 0:
                    dn_grids_info += f'委托单号:{grid.order_ids}'
                continue

            if grid.open_status and not grid.order_status and not grid.close_status:
                dn_grids_info += f'持多中: {grid.vt_symbol}[数量:{grid.volume}, 开仓价格:{grid.open_price},开仓时间:{grid.open_time}]\n'
                continue

            if not grid.open_status and grid.order_status and not grid.close_status:
                dn_grids_info += f'开多中: {grid.vt_symbol}[已开:{grid.traded_volume} => 目标:{grid.volume}, 委托时间:{grid.order_time}]\n'
                if len(grid.order_ids) > 0:
                    dn_grids_info += f'委托单号:{grid.order_ids}'

        if len(dn_grids_info) > 0:
            self.write_log(dn_grids_info)

    def display_tns(self):
        """显示事务的过程记录=》 log"""
        if not self.inited:
            return
        self.write_log(u'{} 当前 {}价格：{}, 委托状态:{}'
                       .format(self.cur_datetime, self.vt_symbol, self.cur_price, self.entrust))

        if len(self.active_orders) > 0:
            self.write_log('当前活动订单数:{}'.format(len(self.active_orders))) #json.dumps(self.active_orders, indent=2, ensure_ascii=False)))

        if hasattr(self, 'policy'):
            policy = getattr(self, 'policy')
            if policy:
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
            if 'margin' not in dist_data:
                dist_data.update({'margin': dist_data.get('price', 0) * dist_data.get('volume',
                                                                                      0) * self.cta_engine.get_margin_rate(
                    dist_data.get('symbol', self.vt_symbol))})
            if 'datetime' not in dist_data:
                dist_data.update({'datetime': self.cur_datetime})
            if self.position and 'long_pos' not in dist_data:
                dist_data.update({'long_pos': self.position.long_pos})
            if self.position and 'short_pos' not in dist_data:
                dist_data.update({'short_pos': self.position.short_pos})

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


class MultiContractPolicy(CtaPolicy):
    """多合约Policy，记录持仓"""

    def __init__(self, strategy=None, **kwargs):
        super().__init__(strategy, **kwargs)
        self.debug = kwargs.get('debug', False)
        self.positions = {}  # vt_symbol: net_pos

    def from_json(self, json_data):
        """将数据从json_data中恢复"""
        super().from_json(json_data)

        self.positions = json_data.get('positions')

    def to_json(self):
        """转换至json文件"""
        j = super().to_json()
        j['positions'] = self.positions
        return j

    def on_trade(self, trade: TradeData):
        """更新交易"""
        pos = self.positions.get(trade.vt_symbol)

        if pos is None:
            pos = 0
        pre_pos = pos
        if trade.direction == Direction.LONG:
            pos = round(pos + trade.volume, 7)

        elif trade.direction == Direction.SHORT:
            pos = round(pos - trade.volume, 7)

        self.positions.update({trade.vt_symbol: pos})

        if self.debug and self.strategy:
            self.strategy.write_log(f'{trade.vt_symbol} pos:{pre_pos}=>{pos}')

        self.save()


class MultiContractTemplate(CtaTemplate):
    """多合约交易模板"""

    def __init__(self, cta_engine, strategy_name, vt_symbol, setting):

        self.policy = None
        self.cur_datetime = None
        super().__init__(cta_engine, strategy_name, vt_symbol, setting)

        self.policy = MultiContractPolicy(strategy=self, debug=True)

    def sync_data(self):
        """同步更新数据"""

        if self.inited and self.trading:
            self.write_log(u'保存policy数据')
            self.policy.save()

    def on_trade(self, trade: TradeData):
        """成交回报事件处理"""
        self.policy.on_trade(trade)

    def get_positions(self):
        """ 获取策略所有持仓详细"""
        pos_list = []

        for vt_symbol, pos in self.policy.positions.items():
            pos_list.append({'vt_symbol': vt_symbol,
                             'direction': 'long' if pos >= 0 else 'short',
                             'volume': pos})

        if self.cur_datetime and (datetime.now() - self.cur_datetime).total_seconds() < 10:
            self.write_log(u'{}当前持仓:{}'.format(self.strategy_name, pos_list))
        return pos_list

    def on_order(self, order: OrderData):
        pass

    def on_init(self):
        self.inited = True

    def on_start(self):
        self.trading = True

    def on_stop(self):
        self.trading = False
