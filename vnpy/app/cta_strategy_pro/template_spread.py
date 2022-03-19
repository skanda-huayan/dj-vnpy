# 套利模板
# 华富资产 @ 李来佳

import os
import traceback
from copy import copy
import bz2
import pickle
import zlib
from vnpy.trader.utility import append_data, extract_vt_symbol, get_months_diff
from .template import (
    CtaPosition,
    CtaGridTrade,
    CtaGrid,
    CtaTemplate,
    Direction,
    datetime,
    Offset,
    Exchange,
    TickData,
    OrderType,
    OrderData,
    TradeData,
    Status,
    StopOrder
)


class CtaSpreadTemplate(CtaTemplate):
    """CTA套利模板"""

    activate_fak = False  # 是否使用FAK得下单、追单方式
    order_type = OrderType.LIMIT  # 缺省下单方式，是使用限价单
    activate_lock = False  # 对某些日内平今手续费较高得合约，采用锁仓方式
    act_vt_symbol = ""  # 主动腿合约
    pas_vt_symbol = ""  # 被动腿合约
    act_symbol = ""
    pas_symbol = ""
    act_exchange = None
    pas_exchange = None
    act_vol_ratio = 1
    pas_vol_ratio = 1

    backtesting = False
    cancel_seconds = 120
    allow_trading_open = True  # 允许开仓
    force_trading_close = False  # 强制平仓
    history_orders = {}

    # 逻辑过程日志
    dist_fieldnames = ['datetime', 'symbol', 'volume', 'price',
                       'operation', 'signal', 'stop_price', 'target_price',
                       'long_pos', 'short_pos']

    def __init__(self, cta_engine, strategy_name, vt_symbol, setting):
        """"""
        super().__init__(cta_engine, strategy_name, vt_symbol, setting)

        if 'activate_fak' not in self.parameters:
            self.parameters.append('activate_fak')
        if 'activate_lock' not in self.parameters:
            self.parameters.append('activate_lock')
        if 'cancel_seconds' not in self.parameters:
            self.parameters.append('cancel_seconds')

        # 基础组件
        self.position = CtaPosition(strategy=self)
        self.gt = CtaGridTrade(strategy=self)
        self.klines = {}  # K线组件字典: kline_name: kline

        self.cur_datetime = None  # 当前Tick时间
        self.cur_mi_tick = None  # 最新的主力合约tick( vt_symbol)
        self.cur_99_tick = None  # 最新得指数合约tick( idx_symbol)

        self.cur_mi_price = None  # 当前价（主力合约 vt_symbol)
        self.cur_99_price = None  # 当前价（tick时，根据tick更新，onBar回测时，根据bar.close更新)

        self.cur_act_tick = None  # 最新的主动腿合约tick( act_vt_symbol)
        self.cur_pas_tick = None  # 最新得被动腿合约tick( pas_vt_symbol)
        self.cur_spd_tick = None  # 价差tick

        self.cur_act_price = None  # 当前价 主动腿合约tick( act_vt_symbol)
        self.cur_pas_price = None  # 当前价 被动腿合约tick( pas_vt_symbol)
        self.cur_spd_price = None  # 当前价差价格

        self.act_price_tick = None  # 主动合约价格跳动
        self.pas_price_tick = None  # 被动合约价格跳动
        self.act_symbol_size = None
        self.pas_symbol_size = None
        self.act_margin_rate = None
        self.pas_margin_rate = None

        self.diff_months = 0  # 主动腿和被动腿相隔月数量

        self.spd_pos = None  # 套利合约的holding pos
        self.act_pos = None  # 主动合约得holding pos
        self.pas_pos = None  # 被动合约得holding pos

        self.last_minute = None  # 最后的分钟,用于on_tick内每分钟处理的逻辑

        # 资金相关
        self.max_invest_rate = 0.1  # 最大仓位(0~1)
        self.max_invest_margin = 0  # 资金上限 0，不限制
        self.max_invest_pos = 0  # 单向头寸数量上限 0，不限制

    def update_setting(self, setting: dict):
        """更新配置参数"""
        super().update_setting(setting)

        self.act_symbol, self.act_exchange = extract_vt_symbol(self.act_vt_symbol)
        self.pas_symbol, self.pas_exchange = extract_vt_symbol(self.pas_vt_symbol)
        self.act_price_tick = self.cta_engine.get_price_tick(self.act_vt_symbol)
        self.pas_price_tick = self.cta_engine.get_price_tick(self.pas_vt_symbol)
        self.act_symbol_size = self.cta_engine.get_size(self.act_vt_symbol)
        self.pas_symbol_size = self.cta_engine.get_size(self.pas_vt_symbol)
        self.act_margin_rate = self.cta_engine.get_margin_rate(self.act_vt_symbol)
        self.pas_margin_rate = self.cta_engine.get_margin_rate(self.pas_vt_symbol)

        # 计算主动腿与被动腿得相隔月
        self.diff_months = get_months_diff(self.act_symbol, self.pas_symbol)

        # 实盘采用FAK
        if not self.backtesting and self.activate_fak:
            self.order_type = OrderType.FAK

    def display_grids(self):
        """更新网格显示信息"""
        if not self.inited:
            return

        up_grids_info = self.gt.to_str(direction=Direction.SHORT)
        if len(self.gt.up_grids) > 0:
            self.write_log(up_grids_info)

        dn_grids_info = self.gt.to_str(direction=Direction.LONG)
        if len(self.gt.dn_grids) > 0:
            self.write_log(dn_grids_info)

    def sync_data(self):
        """同步更新数据"""
        if not self.backtesting:
            self.write_log(u'保存k线缓存数据')
            self.save_klines_to_cache()

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
                if kline:
                    kline.strategy = None
                    kline.cb_on_bar = None
                    if kline.cb_on_period:
                        kline.cb_on_period = None
                    kline.cb_dict = {}
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

    def init_position(self, status_filter=[True]):
        """
        初始化Positin
        使用网格的持久化，获取开仓状态的多空单，更新
        :return:
        """
        self.write_log(u'init_position(),初始化持仓')
        pos_symbols = set()
        if len(self.gt.up_grids) <= 0:
            self.position.short_pos = 0
            # 加载反套单数据，网格JSON
            short_grids = self.gt.load(direction=Direction.SHORT, open_status_filter=status_filter)
            if len(short_grids) == 0:
                self.write_log(u'没有持久化的反套单数据')
                self.gt.up_grids = []

            else:
                self.gt.up_grids = short_grids
                for grid in short_grids:
                    if len(grid.order_ids) > 0 or grid.order_status:
                        self.write_log(f'重置委托状态:{grid.order_status},清除委托单：{grid.order_ids}')
                        grid.order_status = False
                        for vt_orderid in grid.order_ids:
                            self.cancel_order(vt_orderid)
                        grid.order_ids = []

                    if not grid.open_status:
                        continue

                    act_vt_symbol = grid.snapshot.get('act_vt_symbol', self.act_vt_symbol)
                    act_open_volume = grid.snapshot.get('act_open_volume', grid.volume * self.act_vol_ratio)
                    act_open_price = grid.snapshot.get('act_open_price')

                    pas_vt_symbol = grid.snapshot.get('pas_vt_symbol', self.pas_vt_symbol)
                    pas_open_volume = grid.snapshot.get('pas_open_volume', grid.volume * self.pas_vol_ratio)
                    pas_open_price = grid.snapshot.get('pas_open_price')
                    if act_vt_symbol != self.act_vt_symbol:
                        pos_symbols.add(act_vt_symbol)
                    if pas_vt_symbol != self.pas_vt_symbol:
                        pos_symbols.add(pas_vt_symbol)

                    self.write_log(u'加载反套单{}，数量:{},空单[{},价格:{},数量:{}],[多单:{},价格：{},数量:{}]'
                                   .format(grid.vt_symbol or self.vt_symbol, grid.volume,
                                           act_vt_symbol, act_open_price, act_open_volume,
                                           pas_vt_symbol, pas_open_price, pas_open_volume))

                    self.position.short_pos -= grid.volume

                self.write_log(u'加载持久化反套单，共持仓:{}手'.format(abs(self.position.short_pos)))

        if len(self.gt.dn_grids) <= 0:
            # 加载正套单数据，网格JSON
            self.position.long_pos = 0
            long_grids = self.gt.load(direction=Direction.LONG, open_status_filter=status_filter)
            if len(long_grids) == 0:
                self.write_log(u'没有持久化的正套单数据')
                self.gt.dn_grids = []
            else:
                self.gt.dn_grids = long_grids
                for grid in long_grids:
                    if len(grid.order_ids) > 0 or grid.order_status:
                        self.write_log(f'重置委托状态:{grid.order_status},清除委托单：{grid.order_ids}')
                        grid.order_status = False
                        for vt_orderid in grid.order_ids:
                            self.cancel_order(vt_orderid)
                        grid.order_ids = []

                    if not grid.open_status:
                        continue
                    act_vt_symbol = grid.snapshot.get('act_vt_symbol', self.act_vt_symbol)
                    act_open_volume = grid.snapshot.get('act_open_volume', grid.volume * self.act_vol_ratio)
                    act_open_price = grid.snapshot.get('act_open_price')

                    pas_vt_symbol = grid.snapshot.get('pas_vt_symbol', self.pas_vt_symbol)
                    pas_open_volume = grid.snapshot.get('pas_open_volume', grid.volume * self.pas_vol_ratio)
                    pas_open_price = grid.snapshot.get('pas_open_price')

                    if act_vt_symbol != self.act_vt_symbol:
                        pos_symbols.add(act_vt_symbol)
                    if pas_vt_symbol != self.pas_vt_symbol:
                        pos_symbols.add(pas_vt_symbol)

                    self.write_log(u'加载正套单{}，数量:{},多单[{},价格:{},数量:{}],[空单:{},价格：{},数量:{}]'
                                   .format(grid.vt_symbol or self.vt_symbol, grid.volume,
                                           act_vt_symbol, act_open_price, act_open_volume,
                                           pas_vt_symbol, pas_open_price, pas_open_volume))

                    self.position.long_pos += grid.volume

                self.write_log(f'加载持久化正套单，共持仓:{self.position.long_pos}手')

        self.position.pos = self.position.long_pos + self.position.short_pos

        self.write_log(u'{}加载持久化数据完成，正套单:{}，反套单:{},共:{}手'
                       .format(self.strategy_name,
                               self.position.long_pos,
                               abs(self.position.short_pos),
                               self.position.pos))
        self.pos = self.position.pos
        self.gt.save()
        self.display_grids()

        if not self.backtesting:
            # 如果持仓的合约，不在self.vt_symbol中，需要订阅
            for vt_symbol in list(pos_symbols):
                self.write_log(f'新增订阅合约:{vt_symbol}')
                self.cta_engine.subscribe_symbol(strategy_name=self.strategy_name, vt_symbol=vt_symbol)

    def get_positions(self):
        """
        获取策略当前持仓(重构，使用主力合约）
        :return: [{'vt_symbol':symbol,'direction':direction,'volume':volume]
        """
        if not self.position:
            return []
        pos_list = []

        for grid in self.gt.get_opened_grids(direction=Direction.LONG):
            act_vt_symbol = grid.snapshot.get('act_vt_symbol', self.act_vt_symbol)
            act_open_volume = grid.snapshot.get('act_open_volume', grid.volume * self.act_vol_ratio)
            act_open_price = grid.snapshot.get('act_open_price')

            pas_vt_symbol = grid.snapshot.get('pas_vt_symbol', self.pas_vt_symbol)
            pas_open_volume = grid.snapshot.get('pas_open_volume', grid.volume * self.pas_vol_ratio)
            pas_open_price = grid.snapshot.get('pas_open_price')

            pos_list.append({'vt_symbol': act_vt_symbol,
                             'direction': 'long',
                             'volume': act_open_volume,
                             'price': act_open_price})

            pos_list.append({'vt_symbol': pas_vt_symbol,
                             'direction': 'short',
                             'volume': pas_open_volume,
                             'price': pas_open_price})

        for grid in self.gt.get_opened_grids(direction=Direction.SHORT):
            act_vt_symbol = grid.snapshot.get('act_vt_symbol', self.act_vt_symbol)
            act_open_volume = grid.snapshot.get('act_open_volume', grid.volume * self.act_vol_ratio)
            act_open_price = grid.snapshot.get('act_open_price')

            pas_vt_symbol = grid.snapshot.get('pas_vt_symbol', self.pas_vt_symbol)
            pas_open_volume = grid.snapshot.get('pas_open_volume', grid.volume * self.pas_vol_ratio)
            pas_open_price = grid.snapshot.get('pas_open_price')

            pos_list.append({'vt_symbol': act_vt_symbol,
                             'direction': 'short',
                             'volume': act_open_volume,
                             'price': act_open_price})

            pos_list.append({'vt_symbol': pas_vt_symbol,
                             'direction': 'long',
                             'volume': pas_open_volume,
                             'price': pas_open_price})

        if self.cur_datetime and (datetime.now() - self.cur_datetime).total_seconds() < 10:
            self.write_log(u'当前持仓:{}'.format(pos_list))
        return pos_list

    def on_start(self):
        """启动策略（必须由用户继承实现）"""
        # 订阅主动腿/被动腿合约
        self.cta_engine.subscribe_symbol(strategy_name=self.strategy_name, vt_symbol=self.act_vt_symbol)
        self.cta_engine.subscribe_symbol(strategy_name=self.strategy_name, vt_symbol=self.pas_vt_symbol)

        self.write_log(u'启动')
        self.trading = True
        self.put_event()

    # ----------------------------------------------------------------------
    def on_stop(self):
        """停止策略（必须由用户继承实现）"""
        self.active_orders.clear()
        self.history_orders.clear()
        self.pos = 0
        self.entrust = 0

        self.write_log(u'停止')
        self.put_event()

    def on_trade(self, trade: TradeData):
        """交易更新"""
        self.write_log(u'{},交易更新:{}'
                       .format(self.cur_datetime,
                               trade.__dict__))

        # 找到委托单记录
        order_info = None
        # 优先从活动订单获取
        if trade.vt_orderid in self.active_orders.keys():
            order_info = self.active_orders.get(trade.vt_orderid)
        # 如果找不到，可能被移动到历史订单中，从历史订单获取
        if not order_info:
            if trade.vt_orderid in self.history_orders.keys():
                order_info = self.history_orders.get(trade.vt_orderid)
       # 找到委托记录
        if order_info is not None:
            # 委托单记录 =》 找到 Grid
            grid = order_info.get('grid')
            if grid :
                # 更新平均开仓/平仓得价格，数量
                self.update_grid_trade(order_info.get('offset', None), trade, grid)

    def update_pos(self, price, volume, operation, dt):
        """更新持仓组件得pos"""
        dist_record = dict()
        if isinstance(dt, datetime):
            dist_record['datetime'] = dt.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(dt, str):
            dist_record['datetime'] = ' '.join([self.cur_datetime.strftime('%Y-%m-%d'), dt])
        dist_record['volume'] = volume
        dist_record['price'] = price
        dist_record['symbol'] = self.vt_symbol
        dist_record['operation'] = operation
        symbol, exchange = extract_vt_symbol(self.vt_symbol)
        trade = TradeData(
            symbol=symbol,
            exchange=exchange,
            volume=volume,
            price=price,
            datetime=self.cur_datetime,
            orderid=self.cur_datetime.strftime('o_%Y%m%d%H%M%S%f'),
            tradeid=self.cur_datetime.strftime('t_%Y%m%d%H%M%S%f'),
            sys_orderid=self.cur_datetime.strftime('so_%Y%m%d%H%M%S%f'),
            time=self.cur_datetime.strftime('%Y-%m-%d %H:%M:%S'),
            strategy_name=self.strategy_name,
            gateway_name='-'
        )
        if operation == 'buy':
            self.position.open_pos(Direction.LONG, volume=volume)
            dist_record['long_pos'] = self.position.long_pos
            dist_record['short_pos'] = self.position.short_pos
            trade.offset=Offset.OPEN
            trade.direction=Direction.LONG

        if operation == 'short':
            self.position.open_pos(Direction.SHORT, volume=volume)
            dist_record['long_pos'] = self.position.long_pos
            dist_record['short_pos'] = self.position.short_pos
            trade.offset = Offset.OPEN
            trade.direction = Direction.SHORT
        if operation == 'cover':
            self.position.close_pos(Direction.LONG, volume=volume)
            dist_record['long_pos'] = self.position.long_pos
            dist_record['short_pos'] = self.position.short_pos
            trade.offset = Offset.CLOSE
            trade.direction = Direction.LONG
        if operation == 'sell':
            self.position.close_pos(Direction.SHORT, volume=volume)
            dist_record['long_pos'] = self.position.long_pos
            dist_record['short_pos'] = self.position.short_pos
            trade.offset = Offset.CLOSE
            trade.direction = Direction.SHORT

        self.save_dist(dist_record)
        self.pos = self.position.pos

        # 回测时，补充self.vt_symbol的交易记录
        if self.backtesting:
            self.cta_engine.append_trade(trade)


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

    def save_data(self):
        """保存过程数据"""
        if not self.backtesting:
            return

    def send_wechat(self, msg: str):
        """实盘时才发送微信"""
        if self.backtesting:
            return
        self.cta_engine.send_wechat(msg=msg, strategy=self)

    def fix_order(self, order: OrderData):
        """修正order被拆单得情况"""
        order_info = self.active_orders.get(order.vt_orderid, None)
        if order_info:
            volume = order_info.get('volume')  # 原始委托数量
            traded = order_info.get('traded')  # 原始委托中，已成交的数量
            if volume != order.volume:
                self.write_log(f'更新未完成订单{order.vt_orderid} {order.vt_symbol} 的委托数量:{volume}=>{order.volume}')
                order_info.update({'volume': order.volume})
            if traded != order.traded:
                self.write_log(f'更新未完成订单{order.vt_orderid} {order.vt_symbol} 的已成交数量 :{traded}=>{traded + order.traded}')
                order_info.update({'traded': traded + order.traded})

    def on_order(self, order: OrderData):
        """报单更新"""
        # 未执行的订单中，存在是异常，删除
        self.write_log(u'{}报单更新，{}'.format(self.cur_datetime, order.__dict__))

        # 修正order被拆单得情况"
        self.fix_order(order)

        if order.vt_orderid in self.active_orders:
            active_order = self.active_orders[order.vt_orderid]
            if order.volume == order.traded and order.status in [Status.ALLTRADED]:
                self.on_order_all_traded(order)

            elif active_order['offset'] == Offset.OPEN and order.status in [Status.CANCELLED]:
                # 开仓委托单被撤销
                self.on_order_open_canceled(order)

            elif active_order['offset'] != Offset.OPEN and order.status in [Status.CANCELLED]:
                # 平仓委托单被撤销
                self.on_order_close_canceled(order)

            elif order.status == Status.REJECTED:
                if active_order['offset'] == Offset.OPEN:
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

    def update_grid_trade(self, offset: Offset, trade: TradeData, grid: CtaGrid):
        """更新网格内，主动腿/被动腿得开平仓信息"""
        if offset == Offset.OPEN:
            # 更新开仓均价/数量
            if trade.vt_symbol == self.act_vt_symbol:
                opened_price = grid.snapshot.get('act_open_price', 0)
                opened_volume = grid.snapshot.get('act_open_volume')
                act_open_volume = opened_volume + trade.volume
                act_open_price = (opened_price * opened_volume + trade.price * trade.volume) / act_open_volume

                self.write_log(f'{trade.vt_symbol} 开仓均价{opened_price} => {act_open_price},'
                               f' 开仓手数:{opened_volume}=>{act_open_volume}')
                grid.snapshot.update({'act_open_price': act_open_price,
                                      'act_open_volume': act_open_volume,
                                      'act_vt_symbol': self.act_vt_symbol})

            elif trade.vt_symbol == self.pas_vt_symbol:
                opened_price = grid.snapshot.get('pas_open_price', 0)
                opened_volume = grid.snapshot.get('pas_open_volume', 0)
                pas_open_volume = opened_volume + trade.volume
                pas_open_price = (opened_price * opened_volume + trade.price * trade.volume) / pas_open_volume

                self.write_log(f'{trade.vt_symbol} 开仓均价{opened_price} => {pas_open_price},'
                               f' 开仓手数:{opened_volume}=>{pas_open_volume}')
                grid.snapshot.update({'pas_open_price': pas_open_price,
                                      'pas_open_volume': pas_open_volume,
                                      'pas_vt_symbol': self.pas_vt_symbol})

        else:
            # 更新平仓均价/数量
            if trade.vt_symbol == self.act_vt_symbol:
                closed_price = grid.snapshot.get('act_close_price', 0)
                closed_volume = grid.snapshot.get('act_close_volume',0)
                opened_volume = grid.snapshot.get('act_open_volume')

                act_close_volume = closed_volume + trade.volume
                opened_volume = opened_volume - trade.volume
                if opened_volume < 0:
                    debug =1
                act_close_price = (closed_price * closed_volume + trade.price * trade.volume) / act_close_volume

                self.write_log(f'{trade.vt_symbol} 平仓均价{closed_price} => {act_close_price},'
                               f' 平仓手数:{closed_volume}=>{act_close_volume}')
                grid.snapshot.update({'act_close_price': act_close_price,
                                      'act_close_volume': act_close_volume,
                                      'act_vt_symbol': self.act_vt_symbol,
                                      'act_open_volume':opened_volume})

            elif trade.vt_symbol == self.pas_vt_symbol:
                closed_price = grid.snapshot.get('pas_close_price', 0)
                closed_volume = grid.snapshot.get('pas_close_volume', 0)
                opened_volume = grid.snapshot.get('pas_open_volume')
                pas_closed_volume = closed_volume + trade.volume
                opened_volume = opened_volume - trade.volume
                if opened_volume < 0:
                    debug = 1
                pas_open_price = (closed_price * closed_volume + trade.price * trade.volume) / pas_closed_volume

                self.write_log(f'{trade.vt_symbol} 平仓均价{closed_price} => {pas_open_price},'
                               f' 平仓手数:{closed_volume}=>{pas_closed_volume}')
                grid.snapshot.update({'pas_close_price': pas_open_price,
                                      'pas_close_volume': pas_closed_volume,
                                      'pas_vt_symbol': self.pas_vt_symbol,
                                      'pas_open_volume': opened_volume})

        self.gt.save()

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
                if order_info.get("offset", None) != Offset.OPEN:
                    grid.open_status = False

                    self.write_log(f'{grid.direction.value}单已平仓完毕,手数:{grid.volume}, 详细:{grid.snapshot}')

                    self.update_pos(price=grid.close_price,
                                    volume=grid.volume,
                                    operation='cover' if grid.direction == Direction.SHORT else 'sell',
                                    dt=self.cur_datetime)

                    self.write_log(f'移除网格:{grid.to_json()}')
                    self.gt.remove_grids_by_ids(direction=grid.direction, ids=[grid.id])

                # 开仓完毕( buy, short)
                else:
                    grid.open_status = True
                    grid.open_time = self.cur_datetime
                    self.write_log(f'{grid.direction.value}单已开仓完毕,,手数:{grid.volume}, 详细:{grid.snapshot}')
                    self.update_pos(price=grid.open_price,
                                    volume=grid.volume,
                                    operation='short' if grid.direction == Direction.SHORT else 'buy',
                                    dt=self.cur_datetime)
                # 网格的所有委托单部分执行完毕
            else:
                self.write_log(f'剩余委托单号:{grid.order_ids}')

        # 在策略得活动订单中，移除
        self.history_orders[order.vt_orderid] = self.active_orders.pop(order.vt_orderid, None)
        self.gt.save()
        if len(self.active_orders) < 1:
            self.entrust = 0
            return

    def on_order_open_canceled(self, order: OrderData):
        """
        委托开仓单撤销
        如果是FAK模式，重新修改价格，再提交
        FAK用于实盘，需要增加涨跌停判断
        :param order:
        :return:
        """
        self.write_log(u'委托开仓单撤销:{}'.format(order.__dict__))

        if not self.trading:
            if not self.backtesting:
                self.write_error(u'当前不允许交易')
            return

        if order.vt_orderid not in self.active_orders:
            self.write_error(u'{}不在未完成的委托单中{}。'.format(order.vt_orderid, self.active_orders))
            return

        # 直接更新“未完成委托单”，更新volume,retry次数
        old_order = self.active_orders[order.vt_orderid]
        self.write_log(u'{} 委托信息:{}'.format(order.vt_orderid, old_order))
        old_order['traded'] = order.traded
        order_vt_symbol = copy(old_order['vt_symbol'])
        order_symbol, order_exchange = extract_vt_symbol(order_vt_symbol)

        order_volume = old_order['volume'] - old_order['traded']
        if order_volume <= 0:
            msg = u'{} {}{}需重新开仓数量为{}，不再开仓' \
                .format(self.strategy_name,
                        order.vt_orderid,
                        order_vt_symbol,
                        order_volume)
            self.write_error(msg)

            self.write_log(u'移除:{}'.format(order.vt_orderid))
            self.history_orders[order.vt_orderid] = self.active_orders.pop(order.vt_orderid, None)
            return

        order_price = old_order['price']
        order_type = old_order.get('order_type', OrderType.LIMIT)
        order_retry = old_order.get('retry', 0)
        grid = old_order.get('grid', None)
        if order_retry > 10:
            # 这里超过20次尝试失败后，不再尝试,发出告警信息
            msg = u'{}  {}/{}手， 重试开仓次数{}>10' \
                .format(self.strategy_name,
                        order_vt_symbol,
                        order_volume,
                        order_retry)
            self.write_error(msg)
            self.send_wechat(msg)

            if grid:
                if order.vt_orderid in grid.order_ids:
                    self.write_log(f'移除grid中order_ids:{order.vt_orderid}')
                    grid.order_ids.remove(order.vt_orderid)

                # 网格的所有委托单已经执行完毕
                #if len(grid.order_ids) == 0:
                #    grid.order_status = False

                self.gt.save()
                self.write_log(u'网格信息更新:{}'.format(grid.__dict__))

            self.write_log(u'移除:{}'.format(order.vt_orderid))
            self.history_orders[order.vt_orderid] = self.active_orders.pop(order.vt_orderid, None)
            return

        order_retry += 1

        # FAK 重新开单
        if old_order['direction'] == Direction.LONG and order_type == OrderType.FAK:
            # 更新网格交易器

            self.write_log(u'FAK模式，需要重新发送buy委托.grid:{}'.format(grid.__dict__))
            # 更新委托平仓价
            tick = self.cta_engine.get_tick(order_vt_symbol)
            price_tick = self.cta_engine.get_price_tick(order_vt_symbol)
            buy_price = max(tick.ask_price_1, tick.last_price, order_price) + price_tick
            # 不能超过涨停价
            if tick.limit_up > 0 and buy_price > tick.limit_up:
                buy_price = tick.limit_up
            if self.is_upper_limit(order_vt_symbol):
                self.write_log(f'{order_vt_symbol}涨停，不做buy')
                return

            # FAK发送委托追单
            vt_orderids = self.buy(price=buy_price,
                                   volume=order_volume,
                                   vt_symbol=order_vt_symbol,
                                   lock=order_exchange == Exchange.CFFEX or self.activate_lock,
                                   order_type=OrderType.FAK,
                                   order_time=self.cur_datetime if self.backtesting else datetime.now(),
                                   grid=grid)
            if not vt_orderids:
                self.write_error(u'重新提交{} {}手开多单,价格：{}，失败'.
                                 format(order_vt_symbol, order_volume, buy_price))
                return

            # 更新retry的次数
            for vt_orderid in vt_orderids:
                info = self.active_orders.get(vt_orderid, None)
                info.update({'retry': order_retry})

            if grid:
                if order.vt_orderid in grid.order_ids:
                    self.write_log(f'移除grid中order_ids:{order.vt_orderid}')
                    grid.order_ids.remove(order.vt_orderid)

            self.gt.save()
            # 删除旧的委托记录
            self.write_log(u'移除旧的委托记录:{}'.format(order.vt_orderid))
            self.history_orders[order.vt_orderid] = self.active_orders.pop(order.vt_orderid, None)

        elif old_order['direction'] == Direction.SHORT and order_type == OrderType.FAK:

            self.write_log(u'FAK模式，需要重新发送short委托.grid:{}'.format(grid.__dict__))
            tick = self.cta_engine.get_tick(order_vt_symbol)
            price_tick = self.cta_engine.get_price_tick(order_vt_symbol)
            short_price = min(tick.bid_price_1, tick.last_price, order_price) - price_tick
            # 不能超过跌停价
            if tick.limit_down > 0 and short_price < tick.limit_down:
                short_price = tick.limit_down

            if self.is_lower_limit(order_vt_symbol):
                self.write_log(f'{order_vt_symbol}跌停，不做short')
                return

            # 发送委托
            vt_orderids = self.short(price=short_price,
                                     volume=order_volume,
                                     vt_symbol=order_vt_symbol,
                                     lock=order_exchange == Exchange.CFFEX or self.activate_lock,
                                     order_type=OrderType.FAK,
                                     order_time=self.cur_datetime if self.backtesting else datetime.now(),
                                     grid=grid)

            if not vt_orderids:
                self.write_error(
                    u'重新提交{} {}手开空单,价格:{}, 失败'.format(self.vt_symbol, order_volume, short_price))
                return

            # 更新retry的次数
            for vt_orderid in vt_orderids:
                info = self.active_orders.get(vt_orderid, None)
                info.update({'retry': order_retry})

            if grid:
                if order.vt_orderid in grid.order_ids:
                    self.write_log(f'移除grid中order_ids:{order.vt_orderid}')
                    grid.order_ids.remove(order.vt_orderid)

            self.gt.save()
            # 删除旧的委托记录
            self.write_log(u'移除旧的委托记录:{}'.format(order.vt_orderid))
            self.history_orders[order.vt_orderid] = self.active_orders.pop(order.vt_orderid, None)
        else:
            pre_status = old_order.get('status', Status.NOTTRADED)
            old_order.update({'status': Status.CANCELLED})
            self.write_log(u'委托单状态:{}=>{}'.format(pre_status, old_order.get('status')))
            if grid:
                if order.vt_orderid in grid.order_ids:
                    self.write_log(f'移除grid中order_ids:{order.vt_orderid}')
                    grid.order_ids.remove(order.vt_orderid)

                #if not grid.order_ids:
                #    grid.order_status = False

                self.gt.save()
            self.active_orders.update({order.vt_orderid: old_order})

        self.display_grids()

    def on_order_close_canceled(self, order: OrderData):
        """委托平仓单撤销"""
        self.write_log(u'委托平仓单撤销:{}'.format(order.__dict__))

        if order.vt_orderid not in self.active_orders:
            self.write_error(u'{}不在未完成的委托单中:{}。'.format(order.vt_orderid, self.active_orders))
            return

        if not self.trading:
            self.write_error(f'{self.cur_datetime} 当前不允许交易')
            return

        # 直接更新“未完成委托单”，更新volume,Retry次数
        old_order = self.active_orders[order.vt_orderid]
        self.write_log(u'{} 订单信息:{}'.format(order.vt_orderid, old_order))
        old_order['traded'] = order.traded
        # order_time = old_order['order_time']
        order_vt_symbol = copy(old_order['vt_symbol'])
        order_symbol,order_exchange = extract_vt_symbol(order_vt_symbol)

        order_volume = old_order['volume'] - old_order['traded']
        if order_volume <= 0:
            msg = u'{} {}{}重新平仓数量为{}，不再平仓' \
                .format(self.strategy_name, order.vt_orderid, order_vt_symbol, order_volume)
            self.write_error(msg)
            self.send_wechat(msg)
            self.write_log(u'活动订单移除:{}'.format(order.vt_orderid))
            self.history_orders[order.vt_orderid] = self.active_orders.pop(order.vt_orderid, None)
            return

        order_price = old_order['price']
        order_type = old_order.get('order_type', OrderType.LIMIT)
        order_retry = old_order.get('retry',1)
        grid = old_order.get('grid', None)
        if order_retry > 10:
            msg = u'{} 平仓撤单 {}/{}手， 重试平仓次数{}>10' \
                .format(self.strategy_name, order_vt_symbol, order_volume, order_retry)
            self.write_error(msg)
            self.send_wechat(msg)
            if grid:
                if order.vt_orderid in grid.order_ids:
                    self.write_log(f'移除grid中order_ids:{order.vt_orderid}')
                    grid.order_ids.remove(order.vt_orderid)
                #if not grid.order_ids:
                #    grid.order_status = False
                self.gt.save()
                self.write_log(u'更新网格=>{}'.format(grid.__dict__))

            self.write_log(u'移除活动订单:{}'.format(order.vt_orderid))
            self.history_orders[order.vt_orderid] = self.active_orders.pop(order.vt_orderid, None)
            return

        order_retry += 1

        if old_order['direction'] == Direction.LONG and order_type == OrderType.FAK:
            self.write_log(u'FAK模式，需要重新发送cover委托.grid:{}'.format(grid.__dict__))
            # 更新委托平仓价
            tick = self.cta_engine.get_tick(order_vt_symbol)
            price_tick = self.cta_engine.get_price_tick(order_vt_symbol)
            cover_price = max(tick.ask_price_1, tick.last_price, order_price) + price_tick
            # 不能超过涨停价
            if tick.limit_up > 0 and cover_price > tick.limit_up:
                cover_price = tick.limit_up

            if self.is_upper_limit(order_vt_symbol):
                self.write_log(f'{order_vt_symbol}涨停，不做cover')
                return

            # 发送委托
            vt_orderids = self.cover(price=cover_price,
                                     volume=order_volume,
                                     vt_symbol=order_vt_symbol,
                                     lock=order_exchange == Exchange.CFFEX or self.activate_lock,
                                     order_type=OrderType.FAK,
                                     order_time=self.cur_datetime if self.backtesting else datetime.now(),
                                     grid=grid)
            if not vt_orderids:
                self.write_error(u'重新提交{} {}手平空单{}失败'.format(order_vt_symbol, order_volume, cover_price))
                return

            for vt_orderid in vt_orderids:
                info = self.active_orders.get(vt_orderid)
                info.update({'retry': order_retry})
            if grid:
                if order.vt_orderid in grid.order_ids:
                    self.write_log(f'移除grid中order_ids:{order.vt_orderid}')
                    grid.order_ids.remove(order.vt_orderid)
            self.gt.save()
            self.write_log(u'移除活动订单:{}'.format(order.vt_orderid))
            self.history_orders[order.vt_orderid] = self.active_orders.pop(order.vt_orderid, None)

        elif old_order['direction'] == Direction.SHORT and order_type == OrderType.FAK:
            self.write_log(u'FAK模式，需要重新发送sell委托.grid:{}'.format(grid.__dict__))
            tick = self.cta_engine.get_tick(order_vt_symbol)
            price_tick = self.cta_engine.get_price_tick(order_vt_symbol)
            sell_price = min(tick.bid_price_1, tick.last_price, order_price) - price_tick

            # 不能超过跌停价
            if tick.limit_down > 0 and sell_price < tick.limit_down:
                sell_price = tick.limit_down

            if self.is_lower_limit(order_vt_symbol):
                self.write_log(f'{order_vt_symbol}涨停，不做sell')
                return

            # 发送委托
            vt_orderids = self.sell(price=sell_price,
                                    volume=order_volume,
                                    vt_symbol=order_vt_symbol,
                                    lock=order_exchange == Exchange.CFFEX or self.activate_lock,
                                    order_type=OrderType.FAK,
                                    order_time=self.cur_datetime if self.backtesting else datetime.now(),
                                    grid=grid)

            if not vt_orderids:
                self.write_error(u'重新提交{} {}手平多单{}失败'.format(order_vt_symbol, order_volume, sell_price))
                return

            for vt_orderid in vt_orderids:
                info = self.active_orders.get(vt_orderid)
                info.update({'retry': order_retry})
            if grid:
                if order.vt_orderid in grid.order_ids:
                    self.write_log(f'移除grid中order_ids:{order.vt_orderid}')
                    grid.order_ids.remove(order.vt_orderid)
            self.gt.save()

            self.write_log(u'移除活动订单:{}'.format(order.vt_orderid))
            self.history_orders[order.vt_orderid] = self.active_orders.pop(order.vt_orderid, None)

        else:
            pre_status = old_order.get('status', Status.NOTTRADED)
            old_order.update({'status': Status.CANCELLED})
            self.write_log(u'委托单状态:{}=>{}'.format(pre_status, old_order.get('status')))
            if grid:
                if order.vt_orderid in grid.order_ids:
                    self.write_log(f'移除grid中order_ids:{order.vt_orderid}')
                    grid.order_ids.remove(order.vt_orderid)
                #if len(grid.order_ids) == 0:
                #    grid.order_status = False
                self.gt.save()
            self.active_orders.update({order.vt_orderid: old_order})

        self.display_grids()

    def on_stop_order(self, stop_order: StopOrder):
        self.write_log(f'停止单触发:{stop_order.__dict__}')

    def cancel_all_orders(self):
        """
        重载撤销所有正在进行得委托
        :return:
        """
        self.write_log(u'撤销所有正在进行得委托')
        self.cancel_logic(dt=datetime.now(), force=True, reopen=False)

    def cancel_logic(self, dt, force=False, reopen=False):
        "撤单逻辑"""
        if len(self.active_orders) < 1:
            self.entrust = 0
            return

        canceled_ids = []

        for vt_orderid in list(self.active_orders.keys()):
            order_info = self.active_orders.get(vt_orderid, None)
            if order_info is None:
                continue
            order_vt_symbol = order_info.get('vt_symbol', self.vt_symbol)
            order_symbol, order_exchange = extract_vt_symbol(order_vt_symbol)
            order_time = order_info['order_time']
            order_volume = order_info['volume'] - order_info['traded']
            order_grid = order_info['grid']
            order_status = order_info.get('status', Status.NOTTRADED)
            order_type = order_info.get('order_type', OrderType.LIMIT)
            over_seconds = (dt - order_time).total_seconds()

            # 只处理未成交的限价委托单
            if order_status in [Status.SUBMITTING, Status.NOTTRADED] and (order_type == OrderType.LIMIT):
                if over_seconds > self.cancel_seconds or force:  # 超过设置的时间还未成交
                    self.write_log(u'超时{}秒未成交，取消委托单：vt_orderid:{},order:{}'
                                   .format(over_seconds, vt_orderid, order_info))
                    order_info.update({'status': Status.CANCELLING})
                    self.active_orders.update({vt_orderid: order_info})
                    ret = self.cancel_order(str(vt_orderid))
                    if not ret:
                        self.write_log(f'{vt_orderid}撤单失败,更新状态为撤单成功')
                        order_info.update({'status': Status.CANCELLED})
                        self.active_orders.update({vt_orderid: order_info})
                    else:
                        self.write_log(f'{vt_orderid}撤单成功')
                        if order_grid:
                            if vt_orderid in order_grid.order_ids:
                                self.write_log(f'{vt_orderid}存在网格委托队列{order_grid.order_ids}中，移除')
                                order_grid.order_ids.remove(vt_orderid)
                            #if len(order_grid.order_ids) == 0:
                            #    order_grid.order_status = False
                continue

            # 处理状态为‘撤销’的委托单
            elif order_status == Status.CANCELLED:
                self.write_log(u'委托单{}已成功撤单，删除{}'.format(vt_orderid, order_info))
                canceled_ids.append(vt_orderid)

                if reopen and order_type == OrderType.LIMIT:
                    # 撤销的委托单，属于开仓类，需要重新委托
                    if order_info['offset'] == Offset.OPEN:
                        self.write_log(u'超时撤单后，重新开仓')
                        # 开空委托单
                        if order_info['direction'] == Direction.SHORT:
                            short_price = self.cta_engine.get_price(order_vt_symbol) \
                                          - self.cta_engine.get_price_tick(order_vt_symbol)  # noqa

                            self.write_log(u'重新提交{}开空委托,开空价{}，v:{}'
                                           .format(order_vt_symbol, short_price, order_volume))
                            vt_orderids = self.short(price=short_price,
                                                     volume=order_volume,
                                                     vt_symbol=order_vt_symbol,
                                                     lock=order_exchange == Exchange.CFFEX or self.activate_lock,
                                                     order_type=order_type,
                                                     order_time=self.cur_datetime if self.backtesting else datetime.now(),
                                                     grid=order_grid)

                            if len(vt_orderids) > 0:
                                self.write_log(u'委托成功，orderid:{}'.format(vt_orderids))
                                order_grid.snapshot.update({'open_price': short_price})
                            else:
                                self.write_error(u'撤单后，重新委托开空仓失败')
                        else:

                            buy_price = self.cta_engine.get_price(order_vt_symbol) \
                                        + self.cta_engine.get_price_tick(order_vt_symbol)  # noqa
                            self.write_log(u'重新提交{}开多委托,开多价{}，v:{}'
                                           .format(order_vt_symbol, buy_price, order_volume))
                            vt_orderids = self.buy(price=buy_price,
                                                   volume=order_volume,
                                                   vt_symbol=order_vt_symbol,
                                                   lock=order_exchange == Exchange.CFFEX or self.activate_lock,
                                                   order_type=order_type,
                                                   order_time=self.cur_datetime if self.backtesting else datetime.now(),
                                                   grid=order_grid)

                            if len(vt_orderids) > 0:
                                self.write_log(u'委托成功，orderids:{}'.format(vt_orderids))
                                order_grid.snapshot.update({'open_price': buy_price})
                            else:
                                self.write_error(u'撤单后，重新委托开多仓失败')
                    else:
                        # 属于平多委托单
                        if order_info['direction'] == Direction.SHORT:
                            sell_price = self.cta_engine.get_price(order_vt_symbol) \
                                         - self.cta_engine.get_price_tick(order_vt_symbol)  # noqa
                            self.write_log(u'重新提交{}平多委托,{}，v:{}'
                                           .format(order_vt_symbol, sell_price, order_volume))
                            vt_orderids = self.sell(price=sell_price,
                                                    volume=order_volume,
                                                    vt_symbol=order_vt_symbol,
                                                    lock=order_exchange == Exchange.CFFEX or self.activate_lock,
                                                    order_type=order_type,
                                                    order_time=self.cur_datetime if self.backtesting else datetime.now(),
                                                    grid=order_grid)
                            if len(vt_orderids) > 0:
                                self.write_log(u'委托成功，orderids:{}'.format(vt_orderids))
                            else:
                                self.write_error(u'撤单后，重新委托平多仓失败')
                        # 属于平空委托单
                        else:
                            cover_price = self.cta_engine.get_price(order_vt_symbol) \
                                          + self.cta_engine.get_price_tick(order_vt_symbol)  # noqa
                            self.write_log(u'重新提交{}平空委托,委托价{}，v:{}'
                                           .format(order_vt_symbol, cover_price, order_volume))
                            vt_orderids = self.cover(price=cover_price,
                                                     volume=order_volume,
                                                     vt_symbol=order_vt_symbol,
                                                     lock=order_exchange == Exchange.CFFEX or self.activate_lock,
                                                     order_type=order_type,
                                                     order_time=self.cur_datetime if self.backtesting else datetime.now(),
                                                     grid=order_grid)
                            if len(vt_orderids) > 0:
                                self.write_log(u'委托成功，orderids:{}'.format(vt_orderids))
                            else:
                                self.write_error(u'撤单后，重新委托平空仓失败')

        # 删除撤单的订单
        for vt_orderid in canceled_ids:
            self.write_log(u'删除orderID:{0}'.format(vt_orderid))
            self.history_orders[vt_orderid] = self.active_orders.pop(vt_orderid, None)

        if len(self.active_orders) == 0:
            self.entrust = 0

    def check_liquidity(self, direction=None, ask_volume=1, bid_volume=1):
        """实盘检查流动性缺失"""
        if self.backtesting:
            return True

        # 检查流动性缺失
        # if not self.cur_act_tick.bid_price_1 <= self.cur_act_tick.last_price <= self.cur_act_tick.ask_price_1 \
        #         and self.cur_act_tick.volume > 0:
        #     self.write_log(u'流动性缺失导致leg1最新价{0} /V:{1}超出买1 {2}卖1 {3}范围,'
        #                    .format(self.cur_act_tick.last_price, self.cur_act_tick.volume,
        #                            self.cur_act_tick.bid_price_1, self.cur_act_tick.ask_price_1))
        #     return False
        #
        # if not self.cur_pas_tick.bid_price_1 <= self.cur_pas_tick.last_price <= self.cur_pas_tick.ask_price_1 \
        #         and self.cur_pas_tick.volume > 0:
        #     self.write_log(u'流动性缺失导致leg2最新价{0} /V:{1}超出买1 {2}卖1 {3}范围,'
        #                    .format(self.cur_pas_tick.last_price, self.cur_pas_tick.volume,
        #                            self.cur_pas_tick.bid_price_1, self.cur_pas_tick.ask_price_1))
        #     return False

        # 如果设置了方向和volume，检查是否满足
        if direction==Direction.LONG:
            if self.cur_act_tick.ask_volume_1 < ask_volume:
                self.write_log(f'主动腿的卖1委量:{self.cur_act_tick.ask_volume_1}不满足：{ask_volume}')
                return False
            if self.cur_pas_tick.bid_volume_1 < bid_volume:
                self.write_log(f'被动腿的买1委量:{self.cur_pas_tick.bid_volume_1}不满足：{bid_volume}')
                return False
        elif direction == Direction.SHORT:
            if self.cur_act_tick.bid_volume_1 < bid_volume:
                self.write_log(f'主动腿的买1委量:{self.cur_act_tick.bid_volume_1}不满足：{bid_volume}')
                return False
            if self.cur_pas_tick.ask_volume_1 < ask_volume :
                self.write_log(f'被动腿的卖1委量:{self.cur_pas_tick.ask_volume_1}不满足：{ask_volume}')
                return False

        return True

    def check_near_up_nor_down(self):
        """检查当前价与涨跌停价格的距离是否太近"""
        if self.backtesting:
            return False

        # leg1 接近涨停价（10个minDiff以内)
        if self.cur_act_tick.limit_up > 0 \
                and self.cur_act_tick.ask_price_1 + 10 * self.act_price_tick > self.cur_act_tick.limit_up:
            self.write_log(u'主动腿 ask_price_1: {} 接近涨停价{}'
                           .format(self.cur_act_tick.ask_price_1, self.cur_act_tick.limit_up))
            return True

        # leg1 接近跌停价（10个minDiff 以内）
        if self.cur_act_tick.limit_down > 0 \
                and self.cur_act_tick.bid_price_1 - 10 * self.act_price_tick < self.cur_act_tick.limit_down:
            self.write_log(u'主动腿 bid_price_1{} 接近跌停价{}'
                           .format(self.cur_act_tick.bid_price_1, self.cur_act_tick.limit_up))
            return True

        # leg2 接近涨停价（10个minDiff以内)
        if self.cur_pas_tick.limit_up > 0 \
                and self.cur_pas_tick.ask_price_1 + 10 * self.pas_price_tick > self.cur_pas_tick.limit_up:
            self.write_log(u'被动腿 ask_price_1{} 接近涨停价{}'
                           .format(self.cur_pas_tick.ask_price_1, self.cur_pas_tick.limit_up))
            return True

        # leg2 接近跌停价（10个minDiff 以内）
        if self.cur_pas_tick.limit_down > 0 \
                and self.cur_pas_tick.bid_price_1 - 10 * self.pas_price_tick < self.cur_pas_tick.limit_down:
            self.write_log(u'被动腿 bid_price_1{} 接近跌停价{}'
                           .format(self.cur_pas_tick.bid_price_1, self.cur_pas_tick.limit_up))
            return True

        return False

    def spd_short(self, grid: CtaGrid, force: bool = False):
        """非标准合约的套利反套指令"""
        self.write_log(u'委托反套单,price={},volume={}'.format(grid.open_price, grid.volume))

        if self.entrust != 0:
            self.write_log(u'正在委托，不开仓')
            return []
        if not self.trading:
            self.write_log(u'停止状态，不开仓')
            return []
        if not self.allow_trading_open:
            self.write_log(f'{self.cur_datetime}不允许开仓')
            return []
        if self.force_trading_close:
            self.write_log(f'{self.cur_datetime}强制平仓日，不开仓')
            return []
        # 检查流动性缺失
        if not self.check_liquidity( direction=Direction.SHORT,
                ask_volume=grid.volume * self.pas_vol_ratio,
                bid_volume=grid.volume * self.act_vol_ratio
                ) and not force:
            return []
        # 检查涨跌停距离
        if self.check_near_up_nor_down():
            return []

        if self.cur_spd_tick.bid_price_1 < grid.open_price and not force:
            self.write_log(u'{}不满足开仓条件:{}'.format(self.cur_spd_tick.bid_price_1, grid.open_price))
            return []

        # 开空主动腿
        act_vt_orderids = self.short(vt_symbol=self.act_vt_symbol,
                                     lock=self.act_exchange == Exchange.CFFEX or self.activate_lock,
                                     price=self.cur_act_tick.bid_price_1,
                                     volume=grid.volume * self.act_vol_ratio,
                                     order_type=self.order_type,
                                     order_time=self.cur_datetime if self.backtesting else datetime.now(),
                                     grid=grid)
        if not act_vt_orderids:
            self.write_error(f'spd_short，{self.act_vt_symbol}开空仓{grid.volume * self.act_vol_ratio}手失败，'
                             f'委托价:{self.cur_act_tick.bid_price_1}')
            return []

        # 开多被动腿（FAK或者限价单）
        pas_vt_orderids = self.buy(vt_symbol=self.pas_vt_symbol,
                                   lock=self.pas_exchange == Exchange.CFFEX or self.activate_lock,
                                   price=self.cur_pas_tick.ask_price_1,
                                   volume=grid.volume * self.pas_vol_ratio,
                                   order_type=self.order_type,
                                   order_time=self.cur_datetime if self.backtesting else datetime.now(),
                                   grid=grid)
        if not pas_vt_orderids:
            self.write_error(f'spd_short，{self.pas_vt_symbol}开多仓{grid.volume * self.pas_vol_ratio}手失败，'
                             f'委托价:{self.cur_pas_tick.ask_price_1}')
            return []
        # WJ: update_grid_trade() 中会根据实际交易的数目更新 act_open_volume & pas_open_volume，
        # 所以这里必须设置为初始值0，否则grid中的 open_volume会是实际持仓的2倍，导致spd_sell & spd_cover时失败
        # grid.snapshot.update({"act_vt_symbol": self.act_vt_symbol, "act_open_volume": grid.volume * self.act_vol_ratio,
        #                       "pas_vt_symbol": self.pas_vt_symbol, "pas_open_volume": grid.volume * self.pas_vol_ratio})
        grid.snapshot.update({"act_vt_symbol": self.act_vt_symbol, "act_open_volume": 0,
                              "pas_vt_symbol": self.pas_vt_symbol, "pas_open_volume": 0})
        grid.order_status = True
        grid.order_time = self.cur_datetime

        vt_orderids = act_vt_orderids + pas_vt_orderids  # 不能用act_vt_orderids.extend(pas_vt_orderids)，它的返回值为 None，会导致没有vt_orderids
        self.write_log(u'spd short vt_order_ids：{0}'.format(vt_orderids))
        return vt_orderids

    # ----------------------------------------------------------------------
    def spd_buy(self, grid: CtaGrid, force: bool = False):
        """非标准合约的套利正套指令"""
        self.write_log(u'套利价差正套单,price={},volume={}'.format(grid.open_price, grid.volume))

        if self.entrust != 0:
            self.write_log(u'正在委托，不开仓')
            return []
        if not self.trading:
            self.write_log(u'停止状态，不开仓')
            return []
        if not self.allow_trading_open:
            self.write_log(f'{self.cur_datetime}不允许开仓')
            return []
        if self.force_trading_close:
            self.write_log(f'{self.cur_datetime}强制平仓日，不开仓')
            return []
        # 检查流动性缺失
        if not self.check_liquidity(
                direction=Direction.LONG,
                ask_volume=grid.volume * self.act_vol_ratio,
                bid_volume=grid.volume * self.pas_vol_ratio
                ) \
                and not force:
            return []
        # 检查涨跌停距离
        if self.check_near_up_nor_down():
            return []

        if self.cur_spd_tick.bid_price_1 > grid.open_price and not force:
            self.write_log(u'价差{}不满足:{}'.format(self.cur_spd_tick.bid_price_1, grid.open_price))
            return []

        # 开多主动腿（FAK 或者限价单）
        act_vt_orderids = self.buy(vt_symbol=self.act_vt_symbol,
                                   lock=self.act_exchange == Exchange.CFFEX or self.activate_lock,
                                   price=self.cur_act_tick.ask_price_1,
                                   volume=grid.volume * self.act_vol_ratio,
                                   order_type=self.order_type,
                                   order_time=self.cur_datetime if self.backtesting else datetime.now(),
                                   grid=grid)
        if not act_vt_orderids:
            self.write_error(f'spd_buy，{self.act_vt_symbol}开多仓{grid.volume * self.act_vol_ratio}手失败，'
                             f'委托价:{self.cur_act_tick.ask_price_1}')
            return []

        # 开空被动腿
        pas_vt_orderids = self.short(vt_symbol=self.pas_vt_symbol,
                                     lock=self.pas_exchange == Exchange.CFFEX or self.activate_lock,
                                     price=self.cur_pas_tick.bid_price_1,
                                     volume=grid.volume * self.pas_vol_ratio,
                                     order_type=self.order_type,
                                     order_time=self.cur_datetime if self.backtesting else datetime.now(),
                                     grid=grid)
        if not pas_vt_orderids:
            self.write_error(f'spd_buy，{self.pas_vt_symbol}开空仓{grid.volume * self.pas_vol_ratio}手失败，'
                             f'委托价:{self.cur_pas_tick.bid_price_1}')
            return []
        grid.snapshot.update({"act_vt_symbol": self.act_vt_symbol, "act_open_volume": 0,
                              "pas_vt_symbol": self.pas_vt_symbol, "pas_open_volume": 0})
        grid.order_status = True
        grid.order_time = self.cur_datetime
        vt_orderids = act_vt_orderids + pas_vt_orderids
        self.write_log(u'spd buy vt_ordderids：{}'.format(vt_orderids))
        return vt_orderids

    # ----------------------------------------------------------------------
    def spd_sell(self, grid: CtaGrid, force: bool = False):
        """非标准合约的套利平正套指令"""
        self.write_log(u'套利平正套单,price={},volume={}'.format(grid.close_price, grid.volume))
        if self.entrust != 0:
            self.write_log(u'正在委托，不平仓')
            return []
        if not self.trading:
            self.write_log(u'停止状态，不平仓')
            return []
        # 检查流动性缺失
        if not self.check_liquidity(
                direction=Direction.SHORT,
                ask_volume=grid.volume * self.pas_vol_ratio,
                bid_volume=grid.volume * self.act_vol_ratio
        ) and not force:
            return []
        # 检查涨跌停距离
        if self.check_near_up_nor_down():
            return []

        if self.cur_spd_tick.bid_price_1 < grid.close_price and not force:
            self.write_log(u'实际价差{}不满足:{}'.format(self.cur_spd_tick.bid_price_1, grid.close_price))
            return []

        # 获取账号持仓
        self.act_pos = self.cta_engine.get_position_holding(vt_symbol=self.act_vt_symbol)
        self.pas_pos = self.cta_engine.get_position_holding(vt_symbol=self.pas_vt_symbol)
        if not all([self.act_pos, self.pas_pos]):
            self.write_error('主动腿/被动腿的账号持仓数据不存在')
            return []

        # 获取需要平仓的主动腿、被动腿volume
        act_close_volume = grid.snapshot.get('act_open_volume', grid.volume * self.act_vol_ratio)
        pas_close_volume = grid.snapshot.get('pas_open_volume', grid.volume * self.pas_vol_ratio)

        # 检查账号持仓是否满足平仓目标
        if self.act_pos.long_pos < act_close_volume and not (self.act_exchange == Exchange.CFFEX or self.activate_lock):
            self.write_error(f'[正套]平仓，账号主动腿 {self.act_vt_symbol} 多单持仓{self.act_pos.long_pos}'
                             f'今仓{self.act_pos.long_td}/昨{self.act_pos.long_yd}, 不满足{act_close_volume}')
            return []
        if self.pas_pos.short_pos < pas_close_volume and not (
                self.pas_exchange == Exchange.CFFEX or self.activate_lock):
            self.write_error(f'[正套]平仓，账号被动腿 {self.pas_vt_symbol} 多单持仓{self.pas_pos.short_pos}'
                             f'今仓{self.pas_pos.short_td}/昨{self.pas_pos.short_yd}, 不满足{act_close_volume}')
            return []

        # 被动腿空单平仓
        pas_vt_orderids = self.cover(vt_symbol=self.pas_vt_symbol,
                                     lock=self.pas_exchange == Exchange.CFFEX or self.activate_lock,
                                     price=self.cur_pas_tick.ask_price_1,
                                     volume=grid.volume * self.pas_vol_ratio,
                                     order_type=self.order_type,
                                     order_time=self.cur_datetime if self.backtesting else datetime.now(),
                                     grid=grid)
        if not pas_vt_orderids:
            self.write_error(f'spd_sell，{self.pas_vt_symbol}空单平仓{grid.volume * self.pas_vol_ratio}手失败，'
                             f'委托价:{self.cur_pas_tick.ask_price_1}')
            return []

        # 主动腿多单平仓
        act_vt_orderids = self.sell(vt_symbol=self.act_vt_symbol,
                                    lock=self.act_exchange == Exchange.CFFEX or self.activate_lock,
                                    price=self.cur_act_tick.bid_price_1,
                                    volume=grid.volume * self.act_vol_ratio,
                                    order_type=self.order_type,
                                    order_time=self.cur_datetime if self.backtesting else datetime.now(),
                                    grid=grid)
        if not act_vt_orderids:
            self.write_error(f'spd_sell，{self.act_vt_symbol}多单平仓{grid.volume * self.act_vol_ratio}手失败，'
                             f'委托价:{self.cur_act_tick.bid_price_1}')
            return []

        grid.order_status = True
        grid.order_time = self.cur_datetime
        vt_orderids = act_vt_orderids + pas_vt_orderids
        self.write_log(f'spd sell vt_orderids：{vt_orderids}')
        return vt_orderids


    def spd_cover(self, grid: CtaGrid, force: bool = False):
        """非标准合约的套利平反套指令"""
        self.write_log(u'套利价差反套单平仓,price={},volume={}'.format(grid.close_price, grid.volume))
        if self.entrust != 0:
            self.write_log(u'正在委托，不平仓')
            return []
        if not self.trading:
            self.write_log(u'停止状态，不平仓')
            return []
        # 检查流动性缺失
        if not self.check_liquidity(
                direction=Direction.LONG,
                ask_volume=grid.volume * self.act_vol_ratio,
                bid_volume=grid.volume * self.pas_vol_ratio
        ) and not force:
            return []
        # 检查涨跌停距离
        if self.check_near_up_nor_down():
            return []

        if self.cur_spd_tick.ask_price_1 > grid.close_price and not force:
            self.write_log(u'实际价差{}不满足:{}'.format(self.cur_spd_tick.ask_price_1, grid.close_price))
            return []

        # 获取账号内主动腿和被动腿的持仓
        self.act_pos = self.cta_engine.get_position_holding(vt_symbol=self.act_vt_symbol)
        self.pas_pos = self.cta_engine.get_position_holding(vt_symbol=self.pas_vt_symbol)

        if not all([self.act_pos, self.pas_pos]):
            self.write_error('主动腿/被动退得持仓数据不存在')
            return []

        # 检查主动腿、被动腿，是否满足
        act_close_volume = grid.snapshot.get('act_open_volume', grid.volume * self.act_vol_ratio)
        pas_close_volume = grid.snapshot.get('pas_open_volume', grid.volume * self.pas_vol_ratio)
        if self.act_pos.short_pos < act_close_volume and not (
                self.act_exchange == Exchange.CFFEX or self.activate_lock):
            self.write_error(f'账号 {self.act_vt_symbol} 空单持仓{self.act_pos.short_pos}'
                             f'今仓{self.act_pos.short_td}/昨{self.act_pos.short_yd}, 不满足{act_close_volume}')
            return []
        if self.pas_pos.long_pos < pas_close_volume and not (self.pas_exchange == Exchange.CFFEX or self.activate_lock):
            self.write_error(f'账号 {self.pas_vt_symbol} 多单持仓{self.pas_pos.long_pos}'
                             f'今仓{self.pas_pos.long_td}/昨{self.pas_pos.long_yd}, 不满足{act_close_volume}')
            return []

        # 被动腿多单平仓
        pas_vt_orderids = self.sell(vt_symbol=self.pas_vt_symbol,
                                    lock=self.pas_exchange == Exchange.CFFEX or self.activate_lock,
                                    price=self.cur_pas_tick.bid_price_1,
                                    volume=grid.volume * self.pas_vol_ratio,
                                    order_type=self.order_type,
                                    order_time=self.cur_datetime if self.backtesting else datetime.now(),
                                    grid=grid)
        if not pas_vt_orderids:
            self.write_error(f'spd_cover，{self.pas_vt_symbol}多单平仓{grid.volume * self.pas_vol_ratio}手失败，'
                             f'委托价:{self.cur_pas_tick.bid_price_1}')
            return []

        # 主动腿空单平仓
        act_vt_orderids = self.cover(vt_symbol=self.act_vt_symbol,
                                     lock=self.act_exchange == Exchange.CFFEX or self.activate_lock,
                                     price=self.cur_act_tick.ask_price_1,
                                     volume=grid.volume * self.act_vol_ratio,
                                     order_type=self.order_type,
                                     order_time=self.cur_datetime if self.backtesting else datetime.now(),
                                     grid=grid)
        if not act_vt_orderids:
            self.write_error(f'spd_cover{self.act_vt_symbol}空单平仓{grid.volume * self.act_vol_ratio}手失败，'
                             f'委托价:{self.cur_act_tick.ask_price_1}')
            return []

        grid.order_status = True
        grid.order_time = self.cur_datetime
        vt_orderids = act_vt_orderids + pas_vt_orderids
        self.write_log(f'spd cover vt_orderids：{vt_orderids}')
        return vt_orderids


class CtaSpreadTemplateV2(CtaSpreadTemplate):
    """
    套利模板 v2
    改进方向：
    采用限价单方式
    针对非标准套利合约，分别判断两腿得运动动量，根据方向进行选择优先开仓得一腿
    设置时间撤单和价格运动偏移撤单逻辑

    网格组件开仓时, open_volume =0, target_volume = n, 当open_volume == target_volume时，开仓完成。
    网格组件平仓时，open_volume = n, target_volume = 0, 当open_volume == 0时，平仓完成
    """

    tick_window_len = 20  # 观测得tick数量
    spd_orders = {}  # 套利得委托单，用于跟踪主动腿和被动腿得成交情况

    def __init__(self, cta_engine, strategy_name, vt_symbol, setting):
        """"""
        super().__init__(cta_engine, strategy_name, vt_symbol, setting)

        self.spd_ask1_prices = []  # 价差得若干卖1价列表
        self.spd_bid1_prices = []  # 价差得若干买1价列表
        self.spd_last_prices = []  # 价差得最新价列表

        self.act_ask1_prices = []  # 主动腿若干卖1价列表
        self.act_bid1_prices = []  # 主动腿若干买1价列表
        self.act_last_prices = []  # 主动腿若干最新价列表

        self.pas_ask1_prices = []  # 被动腿若干卖1价列表
        self.pas_bid1_prices = []  # 被动腿若干买1价列表
        self.pas_last_prices = []  # 被动腿若干最新价列表

    def on_tick(self, tick: TickData):
        """
        更新tick价格
        :param tick:
        :return:
        """
        # 更新主动腿的持续买1、卖1价格、最新价
        if tick.symbol == self.act_symbol:
            if len(self.act_ask1_prices) > self.tick_window_len:
                self.act_ask1_prices.pop(0)
            if tick.ask_price_1 is not None and tick.ask_volume_1 > 0:
                self.act_ask1_prices.append(tick.ask_price_1)

            if len(self.act_bid1_prices) > self.tick_window_len:
                self.act_bid1_prices.pop(0)
            if tick.bid_price_1 is not None and tick.bid_volume_1 > 0:
                self.act_bid1_prices.append(tick.bid_price_1)

            if len(self.act_last_prices) > self.tick_window_len:
                self.act_last_prices.pop(0)
            if tick.last_price is not None and tick.volume > 0:
                self.act_last_prices.append(tick.last_price)

            # 更新被动腿的持续买1、卖1价格、最新价
            if tick.symbol == self.pas_symbol:
                if len(self.pas_ask1_prices) > self.tick_window_len:
                    self.pas_ask1_prices.pop(0)
                if tick.ask_price_1 is not None and tick.ask_volume_1 > 0:
                    self.pas_ask1_prices.append(tick.ask_price_1)

                if len(self.pas_bid1_prices) > self.tick_window_len:
                    self.pas_bid1_prices.pop(0)
                if tick.bid_price_1 is not None and tick.bid_volume_1 > 0:
                    self.pas_bid1_prices.append(tick.bid_price_1)

                if len(self.pas_last_prices) > self.tick_window_len:
                    self.pas_last_prices.pop(0)
                if tick.last_price is not None and tick.volume > 0:
                    self.pas_last_prices.append(tick.last_price)

        # 实时检查委托订单
        self.check_ordering_grids()

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

            # 平仓完毕（cover， sell）
            if order_info.get("offset", None) != Offset.OPEN:
                act_target_volume = grid.snapshot.get("act_target_volume")
                act_open_volume = grid.snapshot.get("act_open_volume")
                pas_target_volume = grid.snapshot.get("pas_target_volume")
                pas_open_volume = grid.snapshot.get("pas_open_volume")

                # 主动腿和被动腿都平仓完毕
                if pas_target_volume == pas_open_volume == 0 and act_target_volume == act_open_volume == 0:
                    grid.open_status = False
                    # grid.close_status = True

                    self.write_log(f'{grid.direction.value}单已平仓完毕,手数:{grid.volume}, 详细:{grid.snapshot}')

                    self.update_pos(price=grid.close_price,
                                    volume=grid.volume,
                                    operation='cover' if grid.direction == Direction.SHORT else 'sell',
                                    dt=self.cur_datetime)

                    self.write_log(f'移除网格:{grid.to_json()}')
                    self.gt.remove_grids_by_ids(direction=grid.direction, ids=[grid.id])

            # 开仓完毕( buy, short)
            else:
                act_target_volume = grid.snapshot.get("act_target_volume")
                act_open_volume = grid.snapshot.get("act_open_volume")
                pas_target_volume = grid.snapshot.get("pas_target_volume")
                pas_open_volume = grid.snapshot.get("pas_open_volume")
                act_open_price = grid.snapshot.get('act_open_price')
                pas_open_price = grid.snapshot.get('pas_open_price')

                # 主动腿和被动腿都开仓完毕
                if pas_target_volume == pas_open_volume > 0 and act_target_volume == act_open_volume > 0:
                    grid.order_status = False
                    grid.traded_volume = 0
                    grid.open_status = True
                    grid.open_time = self.cur_datetime

                    # 按照实际开仓开仓价进行更新
                    if grid.direction == Direction.LONG:
                        if act_open_price and pas_open_price and act_open_price - pas_open_price < grid.open_price:
                            real_open_price = act_open_price - pas_open_price
                            self.write_log(f'[正套{grid.open_price}=>{grid.close_price}] 调整:{real_open_price}=>{grid.close_price}')
                            grid.open_price = real_open_price
                    elif grid.direction == Direction.SHORT:
                        if act_open_price and pas_open_price and act_open_price - pas_open_price > grid.open_price:
                            real_open_price = act_open_price - pas_open_price
                            self.write_log(
                                f'[反套{grid.open_price}=>{grid.close_price}] 调整:{real_open_price}=>{grid.close_price}')
                            grid.open_price = real_open_price

                    self.write_log(f'{grid.direction.value}单已开仓完毕,,手数:{grid.volume}, 详细:{grid.snapshot}')
                    self.update_pos(price=grid.open_price,
                                    volume=grid.volume,
                                    operation='short' if grid.direction == Direction.SHORT else 'buy',
                                    dt=self.cur_datetime)

        # 在策略得活动订单中，移除
        self.history_orders[order.vt_orderid] = self.active_orders.pop(order.vt_orderid, None)
        self.gt.save()
        if len(self.active_orders) < 1:
            self.entrust = 0
            return

    def on_order_open_canceled(self, order: OrderData):
        """
        委托开仓单撤销
        :param order:
        :return:
        """
        self.write_log(u'委托开仓单撤销:{}'.format(order.__dict__))

        if not self.trading:
            if not self.backtesting:
                self.write_error(u'当前不允许交易')
            return

        if order.vt_orderid not in self.active_orders:
            self.write_error(u'{}不在未完成的委托单中{}。'.format(order.vt_orderid, self.active_orders))
            return

        # 直接更新“未完成委托单”，更新volume,retry次数
        old_order = self.active_orders[order.vt_orderid]
        self.write_log(u'{} 委托信息:{}'.format(order.vt_orderid, old_order))
        old_order['traded'] = order.traded
        order_vt_symbol = copy(old_order['vt_symbol'])
        order_symbol, order_exchange = extract_vt_symbol(order_vt_symbol)

        order_volume = old_order['volume'] - old_order['traded']
        if order_volume <= 0:
            msg = u'{} {}{}需重新开仓数量为{}，不再开仓' \
                .format(self.strategy_name,
                        order.vt_orderid,
                        order_vt_symbol,
                        order_volume)
            self.write_error(msg)

            self.write_log(u'移除:{}'.format(order.vt_orderid))
            self.history_orders[order.vt_orderid] = self.active_orders.pop(order.vt_orderid, None)
            return

        order_price = old_order['price']
        order_type = old_order.get('order_type', OrderType.LIMIT)

        grid = old_order.get('grid', None)

        pre_status = old_order.get('status', Status.NOTTRADED)
        old_order.update({'status': Status.CANCELLED})
        self.write_log(u'委托单状态:{}=>{}'.format(pre_status, old_order.get('status')))
        if grid:
            if order.vt_orderid in grid.order_ids:
                self.write_log(f'移除grid中order_ids:{order.vt_orderid}')
                grid.order_ids.remove(order.vt_orderid)

            # if not grid.order_ids:
            #    grid.order_status = False

            self.gt.save()
        self.active_orders.update({order.vt_orderid: old_order})

        self.display_grids()

    def on_order_close_canceled(self, order: OrderData):
        """委托平仓单撤销"""
        self.write_log(u'委托平仓单撤销:{}'.format(order.__dict__))

        if order.vt_orderid not in self.active_orders:
            self.write_error(u'{}不在未完成的委托单中:{}。'.format(order.vt_orderid, self.active_orders))
            return

        if not self.trading:
            self.write_error(f'{self.cur_datetime} 当前不允许交易')
            return

        # 直接更新“未完成委托单”，更新volume,Retry次数
        old_order = self.active_orders[order.vt_orderid]
        self.write_log(u'{} 订单信息:{}'.format(order.vt_orderid, old_order))
        old_order['traded'] = order.traded
        # order_time = old_order['order_time']
        order_vt_symbol = copy(old_order['vt_symbol'])
        order_symbol, order_exchange = extract_vt_symbol(order_vt_symbol)

        order_volume = old_order['volume'] - old_order['traded']
        if order_volume <= 0:
            msg = u'{} {}{}重新平仓数量为{}，不再平仓' \
                .format(self.strategy_name, order.vt_orderid, order_vt_symbol, order_volume)
            self.write_error(msg)
            self.send_wechat(msg)
            self.write_log(u'活动订单移除:{}'.format(order.vt_orderid))
            self.history_orders[order.vt_orderid] = self.active_orders.pop(order.vt_orderid, None)
            return

        order_price = old_order['price']
        order_type = old_order.get('order_type', OrderType.LIMIT)

        grid = old_order.get('grid', None)

        pre_status = old_order.get('status', Status.NOTTRADED)
        old_order.update({'status': Status.CANCELLED})
        self.write_log(u'委托单状态:{}=>{}'.format(pre_status, old_order.get('status')))
        if grid:
            if order.vt_orderid in grid.order_ids:
                self.write_log(f'移除grid中order_ids:{order.vt_orderid}')
                grid.order_ids.remove(order.vt_orderid)
            # if len(grid.order_ids) == 0:
            #    grid.order_status = False
            self.gt.save()
        self.active_orders.update({order.vt_orderid: old_order})

        self.display_grids()

    def cancel_logic(self, dt, force=False, reopen=False):
        "撤单逻辑"""
        if len(self.active_orders) < 1:
            self.entrust = 0

        canceled_ids = []

        for vt_orderid in list(self.active_orders.keys()):
            order_info = self.active_orders.get(vt_orderid, None)
            if order_info is None:
                continue
            order_vt_symbol = order_info.get('vt_symbol', self.vt_symbol)
            order_symbol, order_exchange = extract_vt_symbol(order_vt_symbol)
            order_time = order_info['order_time']
            order_volume = order_info['volume'] - order_info['traded']
            order_grid = order_info.get('grid', None)
            order_status = order_info.get('status', Status.NOTTRADED)
            order_type = order_info.get('order_type', OrderType.LIMIT)
            over_seconds = (dt - order_time).total_seconds()

            # 只处理未成交的限价委托单
            if order_status in [Status.SUBMITTING, Status.NOTTRADED] and (order_type == OrderType.LIMIT):
                if over_seconds > self.cancel_seconds or force:  # 超过设置的时间还未成交
                    self.write_log(u'超时{}秒未成交，取消委托单：vt_orderid:{},order:{}'
                                   .format(over_seconds, vt_orderid, order_info))
                    order_info.update({'status': Status.CANCELLING})
                    self.active_orders.update({vt_orderid: order_info})
                    ret = self.cancel_order(str(vt_orderid))
                    if not ret:
                        self.write_log(f'{vt_orderid}撤单失败,更新状态为撤单成功')
                        order_info.update({'status': Status.CANCELLED})
                        self.active_orders.update({vt_orderid: order_info})
                    else:
                        self.write_log(f'{vt_orderid}撤单成功')
                        if order_grid:
                            if vt_orderid in order_grid.order_ids:
                                self.write_log(f'{vt_orderid}存在网格委托队列{order_grid.order_ids}中，移除')
                                order_grid.order_ids.remove(vt_orderid)

                continue

            # 处理状态为‘撤销’的委托单
            elif order_status == Status.CANCELLED:
                self.write_log(u'委托单{}已成功撤单，删除{}'.format(vt_orderid, order_info))
                canceled_ids.append(vt_orderid)

        # 删除撤单的订单
        for vt_orderid in canceled_ids:
            self.write_log(u'删除orderID:{0}'.format(vt_orderid))
            self.history_orders[vt_orderid] = self.active_orders.pop(vt_orderid, None)

        if len(self.active_orders) == 0:
            self.entrust = 0

    def check_ordering_grids(self):
        """
        检查正在交易得网格
        :return:
        """

        # 扫描反套网格
        for grid in self.gt.up_grids:

            # 不是在委托得网格，不处理
            if not grid.order_status:
                continue
            act_target_volume = grid.snapshot.get('act_target_volume')
            pas_target_volume = grid.snapshot.get('pas_target_volume')
            act_open_volume = grid.snapshot.get('act_open_volume')
            pas_open_volume = grid.snapshot.get('pas_open_volume')

            # 正在委托反套单开仓状态
            if not grid.open_status and not grid.close_status:

                # 持平、未满足开仓目标
                if act_open_volume == pas_open_volume < act_target_volume == pas_target_volume:
                    # 价差满足,目前没有委托单
                    if self.cur_spd_tick.bid_price_1 >= grid.open_price and len(grid.order_ids) == 0:
                        # 买入被动腿数量
                        buy_pas_volume = pas_target_volume - pas_open_volume
                        # 开多被动腿（限价单）
                        pas_vt_orderids = self.buy(vt_symbol=self.pas_vt_symbol,
                                                   lock=self.pas_exchange == Exchange.CFFEX or self.activate_lock,
                                                   price=self.cur_pas_tick.ask_price_1,
                                                   volume=buy_pas_volume,
                                                   order_type=self.order_type,
                                                   order_time=self.cur_datetime if self.backtesting else datetime.now(),
                                                   grid=grid)
                        if not pas_vt_orderids:
                            self.write_error(f'spd_short，{self.pas_vt_symbol}开多仓{buy_pas_volume}手失败，'
                                             f'委托价:{self.cur_pas_tick.ask_price_1}')
                            continue

                # 主动腿缺腿，当前没有委托单
                if act_open_volume < pas_open_volume and len(grid.order_ids) == 0:
                    short_act_volume = pas_open_volume - act_open_volume
                    # 开空主动腿
                    act_vt_orderids = self.short(vt_symbol=self.act_vt_symbol,
                                                 lock=self.act_exchange == Exchange.CFFEX or self.activate_lock,
                                                 price=self.cur_act_tick.bid_price_1,
                                                 volume=short_act_volume,
                                                 order_type=self.order_type,
                                                 order_time=self.cur_datetime if self.backtesting else datetime.now(),
                                                 grid=grid)
                    if not act_vt_orderids:
                        self.write_error(f'spd_short，{self.act_vt_symbol}开空仓{short_act_volume}手失败，'
                                         f'委托价:{self.cur_act_tick.bid_price_1}')
                        continue

            # 正在委托得反套单平仓状态
            if grid.open_status and grid.close_status :
                # 持平、未满足平仓目标
                if act_open_volume == pas_open_volume > act_target_volume == pas_target_volume:
                    # 价差满足平仓,目前没有委托单
                    if self.cur_spd_tick.bid_price_1 <= grid.close_price and len(grid.order_ids) == 0:
                        pas_close_volume = pas_open_volume
                        # 被动腿多单平仓
                        pas_vt_orderids = self.sell(vt_symbol=self.pas_vt_symbol,
                                                    lock=self.pas_exchange == Exchange.CFFEX or self.activate_lock,
                                                    price=self.cur_pas_tick.bid_price_1,
                                                    volume=pas_close_volume,
                                                    order_type=self.order_type,
                                                    order_time=self.cur_datetime if self.backtesting else datetime.now(),
                                                    grid=grid)
                        if not pas_vt_orderids:
                            self.write_error(f'spd_cover，{self.pas_vt_symbol}多单平仓{pas_close_volume}手失败，'
                                             f'委托价:{self.cur_pas_tick.bid_price_1}')
                            continue

                # 当主动腿瘸腿时，降低主动腿
                if pas_target_volume <= pas_open_volume < act_open_volume and len(grid.order_ids) == 0:
                    act_close_volume = act_open_volume - pas_open_volume
                    # 主动腿空单平仓
                    act_vt_orderids = self.cover(vt_symbol=self.act_vt_symbol,
                                                 lock=self.act_exchange == Exchange.CFFEX or self.activate_lock,
                                                 price=self.cur_act_tick.ask_price_1,
                                                 volume=act_close_volume,
                                                 order_type=self.order_type,
                                                 order_time=self.cur_datetime if self.backtesting else datetime.now(),
                                                 grid=grid)
                    if not act_vt_orderids:
                        self.write_error(f'spd_cover{self.act_vt_symbol}空单平仓{act_close_volume}手失败，'
                                         f'委托价:{self.cur_act_tick.ask_price_1}')
                        continue

        # 扫描正套网格
        for grid in self.gt.dn_grids:

            # 不是在委托得网格，不处理
            if not grid.order_status:
                continue
            act_target_volume = grid.snapshot.get('act_target_volume')
            pas_target_volume = grid.snapshot.get('pas_target_volume')
            act_open_volume = grid.snapshot.get('act_open_volume')
            pas_open_volume = grid.snapshot.get('pas_open_volume')

            # 正在委托正套单开仓状态
            if not grid.open_status and not grid.close_status:
                # 持平、未满足开仓目标
                if act_open_volume == pas_open_volume < act_target_volume == pas_target_volume:
                    # 价差满足,目前没有委托单
                    if self.cur_spd_tick.ask_price_1 <= grid.open_price and len(grid.order_ids) == 0:
                        short_pas_volume = pas_target_volume - pas_open_volume
                        # 开空被动腿
                        pas_vt_orderids = self.short(vt_symbol=self.pas_vt_symbol,
                                                     lock=self.pas_exchange == Exchange.CFFEX or self.activate_lock,
                                                     price=self.cur_pas_tick.bid_price_1,
                                                     volume=short_pas_volume,
                                                     order_type=self.order_type,
                                                     order_time=self.cur_datetime if self.backtesting else datetime.now(),
                                                     grid=grid)
                        if not pas_vt_orderids:
                            self.write_error(f'[正套{grid.open_price}=>{grid.close_price}]，'
                                             f'被动腿{self.pas_vt_symbol}开空仓{short_pas_volume}手失败，'
                                             f'委托价:{self.cur_pas_tick.bid_price_1}')
                            continue
                        self.write_log(f'[正套{grid.open_price}=>{grid.close_price}]，'
                                         f'被动腿{self.pas_vt_symbol}开空仓{short_pas_volume}手，'
                                         f'委托价:{self.cur_pas_tick.bid_price_1}')
                        continue

                # 主动腿缺腿，当前没有委托单
                if act_open_volume < pas_open_volume and len(grid.order_ids) == 0:
                    buy_act_volume = pas_open_volume - act_open_volume
                    act_vt_orderids = self.buy(vt_symbol=self.act_vt_symbol,
                                               lock=self.act_exchange == Exchange.CFFEX or self.activate_lock,
                                               price=self.cur_act_tick.ask_price_1,
                                               volume=buy_act_volume,
                                               order_type=self.order_type,
                                               order_time=self.cur_datetime if self.backtesting else datetime.now(),
                                               grid=grid)
                    if not act_vt_orderids:
                        self.write_error(f'[正套{grid.open_price}=>{grid.close_price}]，'
                                         f'主动腿{self.act_vt_symbol}开多仓{buy_act_volume}手，'
                                         f'委托价:{self.cur_act_tick.ask_price_1}')

            # 正在委托得正套单平仓状态
            if grid.open_status and grid.close_status:
                # 持平、未满足平仓目标
                if act_open_volume == pas_open_volume > act_target_volume == pas_target_volume:
                    # 价差满足平仓,目前没有委托单
                    if self.cur_spd_tick.bid_price_1 >= grid.close_price and len(grid.order_ids) == 0:
                        pas_close_volume = pas_open_volume
                        # 被动腿空单平仓
                        pas_vt_orderids = self.cover(vt_symbol=self.pas_vt_symbol,
                                                     lock=self.pas_exchange == Exchange.CFFEX or self.activate_lock,
                                                     price=self.cur_pas_tick.ask_price_1,
                                                     volume=pas_close_volume,
                                                     order_type=self.order_type,
                                                     order_time=self.cur_datetime if self.backtesting else datetime.now(),
                                                     grid=grid)
                        if not pas_vt_orderids:
                            self.write_error(f'[正套{grid.open_price}=>{grid.close_price}]，'
                                             f'被动腿{self.pas_vt_symbol}空单平仓{pas_close_volume}手失败，'
                                             f'委托价:{self.cur_pas_tick.ask_price_1}')
                            continue
                        self.write_log(f'[正套{grid.open_price}=>{grid.close_price}]，'
                                         f'被动腿{self.pas_vt_symbol}空单平仓{pas_close_volume}手，'
                                         f'委托价:{self.cur_pas_tick.ask_price_1}')
                        continue

                if pas_target_volume <= pas_open_volume < act_open_volume  and len(grid.order_ids) == 0:
                    act_close_volume = act_open_volume - pas_open_volume
                    # 主动腿多单平仓
                    act_vt_orderids = self.sell(vt_symbol=self.act_vt_symbol,
                                                lock=self.act_exchange == Exchange.CFFEX or self.activate_lock,
                                                price=self.cur_act_tick.bid_price_1,
                                                volume=act_close_volume,
                                                order_type=self.order_type,
                                                order_time=self.cur_datetime if self.backtesting else datetime.now(),
                                                grid=grid)
                    if not act_vt_orderids:
                        self.write_error(f'[正套{grid.open_price}=>{grid.close_price}]，'
                                         f'主动腿{self.act_vt_symbol}多单平仓{act_close_volume}手失败，'
                                         f'委托价:{self.cur_act_tick.bid_price_1}')
                        continue
                    self.write_log(f'[正套{grid.open_price}=>{grid.close_price}]，'
                                     f'主动腿{self.act_vt_symbol}多单平仓{act_close_volume}手，'
                                     f'委托价:{self.cur_act_tick.bid_price_1}')

    def spd_buy(self, grid: CtaGrid, force: bool = False):
        """非标准合约的套利正套指令"""
        self.write_log(u'套利价差正套单,price={},volume={}'.format(grid.open_price, grid.volume))

        if self.entrust != 0:
            self.write_log(u'[正套]正在委托，不开仓')
            return []
        if not self.trading:
            self.write_log(u'[正套]停止状态，不开仓')
            return []
        if not self.allow_trading_open:
            self.write_log(f'[正套]{self.cur_datetime}不允许开仓')
            return []
        if self.force_trading_close:
            self.write_log(f'[正套]{self.cur_datetime}强制平仓日，不开仓')
            return []

        # 检查流动性缺失
        if not self.check_liquidity(
                direction=Direction.LONG,
                ask_volume=grid.volume * self.act_vol_ratio,
                bid_volume=grid.volume * self.pas_vol_ratio
        ) \
                and not force:
            return []

        # 检查涨跌停距离
        if self.check_near_up_nor_down():
            return []

        if self.cur_spd_tick.bid_price_1 > grid.open_price and not force:
            self.write_log(u'[正套]价差{}不满足:{}'.format(self.cur_spd_tick.bid_price_1, grid.open_price))
            return []

        # 判断主动腿、被动腿得动能方向，选择优先下单得合约
        # 主动腿目标、被动腿目标
        act_target_volume = grid.volume * self.act_vol_ratio
        pas_target_volume = grid.volume * self.pas_vol_ratio

        # 检查主动腿和被动腿需要得保证金，检查账号是否满足保证金
        # 主动腿保证金/被动腿保证金
        act_margin = act_target_volume * self.cur_act_tick.last_price * self.act_symbol_size * self.act_margin_rate
        pas_margin = pas_target_volume * self.cur_pas_tick.last_price * self.pas_symbol_size * self.pas_margin_rate

        # 当前净值,可用资金,资金占用比例,资金上限
        balance, avaliable, occupy_percent, percent_limit = self.cta_engine.get_account()

        # 同一品种套利
        invest_margin = max(act_margin, pas_margin)

        # 计划使用保证金
        target_margin = balance * (occupy_percent / 100) + invest_margin

        if 100 * (target_margin / balance) > percent_limit:
            self.write_error(u'[正套]委托后,预计当前资金占用:{},超过限定:{}比例,不能开仓'
                             .format(100 * (target_margin / balance), percent_limit))
            return []

        #
        # # 开多主动腿（FAK 或者限价单）
        # act_vt_orderids = self.buy(vt_symbol=self.act_vt_symbol,
        #                            lock=self.act_exchange == Exchange.CFFEX or self.activate_lock,
        #                            price=self.cur_act_tick.ask_price_1,
        #                            volume=grid.volume * self.act_vol_ratio,
        #                            order_type=self.order_type,
        #                            order_time=self.cur_datetime if self.backtesting else datetime.now(),
        #                            grid=grid)
        # if not act_vt_orderids:
        #     self.write_error(f'spd_buy，{self.act_vt_symbol}开多仓{grid.volume * self.act_vol_ratio}手失败，'
        #                      f'委托价:{self.cur_act_tick.ask_price_1}')
        #     return []

        # 开空被动腿
        pas_vt_orderids = self.short(vt_symbol=self.pas_vt_symbol,
                                     lock=self.pas_exchange == Exchange.CFFEX or self.activate_lock,
                                     price=self.cur_pas_tick.bid_price_1,
                                     volume=grid.volume * self.pas_vol_ratio,
                                     order_type=self.order_type,
                                     order_time=self.cur_datetime if self.backtesting else datetime.now(),
                                     grid=grid)
        if not pas_vt_orderids:
            self.write_error(f'[正套-被动腿]，{self.pas_vt_symbol}开空仓{grid.volume * self.pas_vol_ratio}手失败，'
                             f'委托价:{self.cur_pas_tick.bid_price_1}')
            return []
        self.write_log(f'[正套-被动腿]，{self.pas_vt_symbol}开空仓{grid.volume * self.pas_vol_ratio}手，'
                         f'委托价:{self.cur_pas_tick.bid_price_1}')

        # 利用网格得snapshort进行记录，当前持有仓位，目标仓位
        grid.snapshot.update(
            {"act_vt_symbol": self.act_vt_symbol, "act_open_volume": 0, 'act_target_volume': act_target_volume,
             "pas_vt_symbol": self.pas_vt_symbol, "pas_open_volume": 0, 'pas_target_volume': pas_target_volume})
        grid.order_status = True
        grid.order_time = self.cur_datetime
        vt_orderids = pas_vt_orderids
        self.write_log(u'[正套][被动腿] vt_ordderids：{}'.format(vt_orderids))

        # 添加正套得委托单跟踪，对象是网格id，内容是方向
        self.spd_orders.update({grid.id: {'direction': Direction.LONG}, 'offset': Offset.OPEN})

        return vt_orderids

    def spd_short(self, grid: CtaGrid, force: bool = False):
        """非标准合约的套利反套指令"""
        self.write_log(u'委托反套单,price={},volume={}'.format(grid.open_price, grid.volume))

        if grid.order_status:
            self.write_log(u'[反套]正在委托，不重复开仓')
            return []
        if not self.trading:
            self.write_log(u'[反套]停止状态，不开仓')
            return []
        if not self.allow_trading_open:
            self.write_log(f'[反套]{self.cur_datetime}不允许开仓')
            return []
        if self.force_trading_close:
            self.write_log(f'[反套]{self.cur_datetime}强制平仓日，不开仓')
            return []
        # 检查流动性缺失
        if not self.check_liquidity(direction=Direction.SHORT,
                                    ask_volume=grid.volume * self.pas_vol_ratio,
                                    bid_volume=grid.volume * self.act_vol_ratio
                                    ) and not force:
            return []
        # 检查涨跌停距离
        if self.check_near_up_nor_down():
            return []

        if self.cur_spd_tick.bid_price_1 < grid.open_price and not force:
            self.write_log(u'[反套]{}不满足开仓条件:{}'.format(self.cur_spd_tick.bid_price_1, grid.open_price))
            return []

        # 判断主动腿、被动腿得动能方向，选择优先下单得合约
        # 主动腿目标、被动腿目标
        act_target_volume = grid.volume * self.act_vol_ratio
        pas_target_volume = grid.volume * self.pas_vol_ratio

        # 检查主动腿和被动腿需要得保证金，检查账号是否满足保证金
        # 主动腿保证金/被动腿保证金
        act_margin = act_target_volume * self.cur_act_tick.last_price * self.act_symbol_size * self.act_margin_rate
        pas_margin = pas_target_volume * self.cur_pas_tick.last_price * self.pas_symbol_size * self.pas_margin_rate

        # 当前净值,可用资金,资金占用比例,资金上限
        balance, avaliable, occupy_percent, percent_limit = self.cta_engine.get_account()

        # 同一品种套利
        invest_margin = max(act_margin, pas_margin)

        # 计划使用保证金
        target_margin = balance * (occupy_percent / 100) + invest_margin

        if 100 * (target_margin / balance) > percent_limit:
            self.write_error(u'[反套]委托后,预计当前资金占用:{},超过限定:{}比例,不能开仓'
                             .format(100 * (target_margin / balance), percent_limit))
            return []
        # # 开空主动腿
        # act_vt_orderids = self.short(vt_symbol=self.act_vt_symbol,
        #                              lock=self.act_exchange == Exchange.CFFEX or self.activate_lock,
        #                              price=self.cur_act_tick.bid_price_1,
        #                              volume=grid.volume * self.act_vol_ratio,
        #                              order_type=self.order_type,
        #                              order_time=self.cur_datetime if self.backtesting else datetime.now(),
        #                              grid=grid)
        # if not act_vt_orderids:
        #     self.write_error(f'spd_short，{self.act_vt_symbol}开空仓{grid.volume * self.act_vol_ratio}手失败，'
        #                      f'委托价:{self.cur_act_tick.bid_price_1}')
        #     return []

        # 开多被动腿（FAK或者限价单）
        pas_vt_orderids = self.buy(vt_symbol=self.pas_vt_symbol,
                                   lock=self.pas_exchange == Exchange.CFFEX or self.activate_lock,
                                   price=self.cur_pas_tick.ask_price_1,
                                   volume=pas_target_volume,
                                   order_type=self.order_type,
                                   order_time=self.cur_datetime if self.backtesting else datetime.now(),
                                   grid=grid)
        if not pas_vt_orderids:
            self.write_error(f'[反套-被动腿]，{self.pas_vt_symbol}开多仓{pas_target_volume}手失败，'
                             f'委托价:{self.cur_pas_tick.ask_price_1}')
            return []
        self.write_log(f'[反套-被动腿]，{self.pas_vt_symbol}开多仓{pas_target_volume}手，'
                         f'委托价:{self.cur_pas_tick.ask_price_1}')

        # 设置实际开仓数量为0，用于不断实现目标
        grid.snapshot.update({"act_vt_symbol": self.act_vt_symbol,
                              "act_open_volume": 0,
                              'act_target_volume': act_target_volume,
                              "pas_vt_symbol": self.pas_vt_symbol,
                              "pas_open_volume": 0,
                              'pas_target_volume': pas_target_volume})

        grid.order_status = True
        grid.order_time = self.cur_datetime

        vt_orderids = pas_vt_orderids
        self.write_log(u'[反套-被动腿] vt_order_ids：{0}'.format(vt_orderids))
        return vt_orderids


    def spd_sell(self, grid: CtaGrid, force: bool = False):
        """非标准合约的套利平正套指令"""
        self.write_log(f'[正套:{grid.open_price}=>{grid.close_price}]平仓,volume={grid.volume}')
        if grid.order_status:
            self.write_log(f'[正套:{grid.open_price}=>{grid.close_price}]正在委托，不平仓')
            return []
        if not self.trading:
            self.write_log(f'[正套:{grid.open_price}=>{grid.close_price}]策略处于停止状态，不平仓')
            return []
        # 检查流动性缺失
        if not self.check_liquidity(
                direction=Direction.SHORT,
                ask_volume=grid.volume * self.pas_vol_ratio,
                bid_volume=grid.volume * self.act_vol_ratio
        ) and not force:
            return []

        # 检查涨跌停距离
        if self.check_near_up_nor_down():
            return []

        if self.cur_spd_tick.bid_price_1 < grid.close_price and not force:
            self.write_log(u'实际价差{}不满足:{}'.format(self.cur_spd_tick.bid_price_1, grid.close_price))
            return []

        # 获取账号持仓
        self.act_pos = self.cta_engine.get_position_holding(vt_symbol=self.act_vt_symbol)
        self.pas_pos = self.cta_engine.get_position_holding(vt_symbol=self.pas_vt_symbol)
        if not all([self.act_pos, self.pas_pos]):
            self.write_error(f'[正套:{grid.open_price}=>{grid.close_price}]主动腿/被动腿的账号持仓数据不存在')
            return []

        # 获取需要平仓的主动腿、被动腿volume
        act_close_volume = grid.snapshot.get('act_open_volume', grid.volume * self.act_vol_ratio)
        pas_close_volume = grid.snapshot.get('pas_open_volume', grid.volume * self.pas_vol_ratio)

        # 检查账号持仓是否满足平仓目标
        if self.act_pos.long_pos < act_close_volume and not (self.act_exchange == Exchange.CFFEX or self.activate_lock):
            self.write_error(f'[正套]平仓，账号主动腿 {self.act_vt_symbol} 多单持仓{self.act_pos.long_pos}'
                             f'今仓{self.act_pos.long_td}/昨{self.act_pos.long_yd}, 不满足{act_close_volume}')
            return []
        if self.pas_pos.short_pos < pas_close_volume and not (
                self.pas_exchange == Exchange.CFFEX or self.activate_lock):
            self.write_error(f'[正套]平仓，账号被动腿 {self.pas_vt_symbol} 多单持仓{self.pas_pos.short_pos}'
                             f'今仓{self.pas_pos.short_td}/昨{self.pas_pos.short_yd}, 不满足{act_close_volume}')
            return []

        # 更新主动腿和被动腿得目标持仓为0，即平掉仓位
        grid.snapshot.update({"act_target_volume": 0,"pas_target_volume": 0})

        # 被动腿空单平仓
        pas_vt_orderids = self.cover(vt_symbol=self.pas_vt_symbol,
                                     lock=self.pas_exchange == Exchange.CFFEX or self.activate_lock,
                                     price=self.cur_pas_tick.ask_price_1,
                                     volume=pas_close_volume,
                                     order_type=self.order_type,
                                     order_time=self.cur_datetime if self.backtesting else datetime.now(),
                                     grid=grid)
        if not pas_vt_orderids:
            self.write_error(f'[正套:{grid.open_price}=>{grid.close_price}]，{self.pas_vt_symbol}空单平仓{pas_close_volume}手失败，'
                             f'委托价:{self.cur_pas_tick.ask_price_1}')
            return []

        self.write_log(f'[正套:{grid.open_price}=>{grid.close_price}] {self.pas_vt_symbol}空单平仓{pas_close_volume}手，'
                         f'委托价:{self.cur_pas_tick.ask_price_1}')

        # 如果属于强制平仓得话，设置close价格低于当前价差。
        if force:
            new_close_price = self.cur_spd_tick.bid_price_1 - 10 * self.act_price_tick
            self.write_log(f'[正套:{grid.open_price}=>{grid.close_price}] 调整平仓价:{new_close_price}')
            grid.close_price = self.cur_spd_tick.bid_price_1 - 10 * self.act_price_tick

        #
        # # 主动腿多单平仓
        # act_vt_orderids = self.sell(vt_symbol=self.act_vt_symbol,
        #                             lock=self.act_exchange == Exchange.CFFEX or self.activate_lock,
        #                             price=self.cur_act_tick.bid_price_1,
        #                             volume=grid.volume * self.act_vol_ratio,
        #                             order_type=self.order_type,
        #                             order_time=self.cur_datetime if self.backtesting else datetime.now(),
        #                             grid=grid)
        # if not act_vt_orderids:
        #     self.write_error(f'spd_sell，{self.act_vt_symbol}多单平仓{grid.volume * self.act_vol_ratio}手失败，'
        #                      f'委托价:{self.cur_act_tick.bid_price_1}')
        #     return []

        grid.close_status = True
        grid.order_status = True
        grid.order_time = self.cur_datetime
        vt_orderids =  pas_vt_orderids
        self.write_log(f'[正套:{grid.open_price}=>{grid.close_price}] vt_orderids：{vt_orderids}')

        return vt_orderids

    # ----------------------------------------------------------------------
    def spd_cover(self, grid: CtaGrid, force: bool = False):
        """非标准合约的套利平反套指令"""
        self.write_log(u'套利价差反套单平仓,price={},volume={}'.format(grid.close_price, grid.volume))
        if grid.order_status:
            self.write_log(u'[反套] 正在委托平仓，不重复')
            return []
        if not self.trading:
            self.write_log(u'停止状态，不平仓')
            return []
        # 检查流动性缺失
        if not self.check_liquidity(
                direction=Direction.LONG,
                ask_volume=grid.volume * self.act_vol_ratio,
                bid_volume=grid.volume * self.pas_vol_ratio
        ) and not force:
            return []
        # 检查涨跌停距离
        if self.check_near_up_nor_down():
            return []

        if self.cur_spd_tick.ask_price_1 > grid.close_price and not force:
            self.write_log(u'实际价差{}不满足:{}'.format(self.cur_spd_tick.ask_price_1, grid.close_price))
            return []

        # 获取账号内主动腿和被动腿的持仓
        self.act_pos = self.cta_engine.get_position_holding(vt_symbol=self.act_vt_symbol)
        self.pas_pos = self.cta_engine.get_position_holding(vt_symbol=self.pas_vt_symbol)

        if not all([self.act_pos, self.pas_pos]):
            self.write_error('主动腿/被动退得持仓数据不存在')
            return []

        # 检查主动腿、被动腿，是否满足
        act_close_volume = grid.snapshot.get('act_open_volume', grid.volume * self.act_vol_ratio)
        pas_close_volume = grid.snapshot.get('pas_open_volume', grid.volume * self.pas_vol_ratio)
        if self.act_pos.short_pos < act_close_volume and not (
                self.act_exchange == Exchange.CFFEX or self.activate_lock):
            self.write_error(f'账号 {self.act_vt_symbol} 空单持仓{self.act_pos.short_pos}'
                             f'今仓{self.act_pos.short_td}/昨{self.act_pos.short_yd}, 不满足{act_close_volume}')
            return []
        if self.pas_pos.long_pos < pas_close_volume and not (self.pas_exchange == Exchange.CFFEX or self.activate_lock):
            self.write_error(f'账号 {self.pas_vt_symbol} 多单持仓{self.pas_pos.long_pos}'
                             f'今仓{self.pas_pos.long_td}/昨{self.pas_pos.long_yd}, 不满足{act_close_volume}')
            return []

        # 更新主动腿和被动腿得目标持仓为0，即平掉仓位
        grid.snapshot.update({"act_target_volume": 0, "pas_target_volume": 0})

        # 被动腿多单平仓
        pas_vt_orderids = self.sell(vt_symbol=self.pas_vt_symbol,
                                    lock=self.pas_exchange == Exchange.CFFEX or self.activate_lock,
                                    price=self.cur_pas_tick.bid_price_1,
                                    volume=pas_close_volume,
                                    order_type=self.order_type,
                                    order_time=self.cur_datetime if self.backtesting else datetime.now(),
                                    grid=grid)
        if not pas_vt_orderids:
            self.write_error(f'spd_cover，{self.pas_vt_symbol}多单平仓{pas_close_volume}手失败，'
                             f'委托价:{self.cur_pas_tick.bid_price_1}')
            return []

        # 如果属于强制平仓得话，设置close价格高于于当前价差10跳。
        if force:
            grid.close_price = self.cur_spd_tick.ask_price_1 + 10 * self.act_price_tick

        # # 主动腿空单平仓
        # act_vt_orderids = self.cover(vt_symbol=self.act_vt_symbol,
        #                              lock=self.act_exchange == Exchange.CFFEX or self.activate_lock,
        #                              price=self.cur_act_tick.ask_price_1,
        #                              volume=grid.volume * self.act_vol_ratio,
        #                              order_type=self.order_type,
        #                              order_time=self.cur_datetime if self.backtesting else datetime.now(),
        #                              grid=grid)
        # if not act_vt_orderids:
        #     self.write_error(f'spd_cover{self.act_vt_symbol}空单平仓{grid.volume * self.act_vol_ratio}手失败，'
        #                      f'委托价:{self.cur_act_tick.ask_price_1}')
        #     return []
        grid.close_status = True
        grid.order_status = True
        grid.order_time = self.cur_datetime
        vt_orderids = pas_vt_orderids
        self.write_log(f'spd cover vt_orderids：{vt_orderids}')
        return vt_orderids
