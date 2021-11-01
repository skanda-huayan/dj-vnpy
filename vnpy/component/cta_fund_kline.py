# encoding: UTF-8

# 资金曲线 - 华富资产
# 账号级别资金曲线（ 根据账号balance更新）
# 策略实例级别资金曲线（根据pnl计算）
# 使用小时级别KLine计算
# 针对策略实例，bar.open_interest 为平仓权益
# 2019/5/30
#   1. add load trade list function on init for strategy
#   2. add trade record function for strategy
# 2019/6/9
#   1. 增加renko bar的资金曲线

import os
from datetime import datetime
import pandas as pd
import traceback

from vnpy.component.cta_line_bar import CtaMinuteBar, CtaHourBar, CtaDayBar, CtaLineBar, get_cta_bar_type
from vnpy.component.cta_renko_bar import CtaRenkoBar
from vnpy.trader.object import BarData, TickData
from vnpy.trader.utility import get_folder_path, get_trading_date
from vnpy.trader.constant import Direction, Offset, Exchange


class FundKline(object):
    def __init__(self, cta_engine, setting, use_cache=False, load_trade=False):
        """
        初始化
        :param cta_engine:
        :param setting:
        """
        self.cta_engine = cta_engine
        self.kline = None
        self.setting = setting
        self.kline_name = setting.get('name')
        self.kline_file = None
        self.closed_profit = 0
        self.holding_profit = 0
        self.profit_list = []
        self.holding_list = []
        self.price_tick = self.setting.get('price_tick', 0.01)
        self.symbol = self.setting.get('symbol', None)
        if self.symbol is None:
            vt_symbol = self.setting.get('vt_symbol', 'fund')
            if '.' in vt_symbol:
                self.symbol, _ = vt_symbol.split('.')
            else:
                self.symbol = vt_symbol

        if self.kline_name is None:
            self.write_error(u'setting没有配置资金曲线name变量')
            return

        # onbar 回调函数
        self.onbar_callback = self.setting.pop('onbar_callback', None)

        self.use_renko = self.setting.pop('use_renko', False)
        if self.use_renko:
            self.write_log(u'使用CtaRenkoBar')
            self.kline = CtaRenkoBar(strategy=self, cb_on_bar=self.on_bar, setting=self.setting)
        else:
            kline_setting = self.setting
            kline_period = kline_setting.pop('kline_period', 'H1')  # 默认使用1小时K线
            kline_class, kline_bar_interval = get_cta_bar_type(kline_period)
            self.write_log(u'使用{}'.format(kline_period))
            kline_setting['bar_interval'] = kline_bar_interval  # X分钟K线, X秒K线，X小时K线
            self.kline = kline_class(strategy=self, cb_on_bar=self.on_bar, setting=kline_setting)
        self.inited = False

        self.long_pos_dict = {}
        self.short_pos_dict = {}

        # 记载历史k线
        if use_cache:
            self.load()
        else:
            self.inited = True
        # 加载历史交易记录，获取剩余持仓记录
        if (load_trade):
            self.load_trade_list()

    def write_log(self, content, strategy_name=None):
        """
        记录日志
        :param content:
        :return:
        """
        if self.cta_engine:
            self.cta_engine.write_log(content, strategy_name)

    def write_error(self, content, strategy_name=None):
        """
        记录充值日志
        :param content:
        :return:
        """
        if self.cta_engine:
            self.cta_engine.write_error(content, strategy_name)

    def load(self):
        """
        从本地csv文件恢复k线数据
        :return:
        """
        self.kline_file = str(get_folder_path('data').joinpath('fund_{}.csv'.format(self.kline_name)))

        # 如果数据文件存在，则加载数据
        if os.path.exists(self.kline_file):
            self.write_log(u'加载{}数据'.format(self.kline_name))
            df = pd.read_csv(self.kline_file)
            dt_now = datetime.now()
            df = df.set_index(pd.DatetimeIndex(df['datetime']))
            for dt, bar_data in df.iterrows():
                bar = BarData()

                bar.symbol = self.symbol
                bar.datetime = dt
                bar.open_price = float(bar_data['open'])
                bar.close_price = float(bar_data['close'])
                bar.high_price = float(bar_data['high'])
                bar.low_price = float(bar_data['low'])
                bar.date = dt.strftime('%Y-%m-%d')
                str_td = str(bar_data.get('trading_date', ''))
                if len(str_td) == 8:
                    bar.trading_day = str_td[0:4] + '-' + str_td[4:6] + '-' + str_td[6:8]
                elif len(str_td) == 0:
                    bar.trading_day = bar.date
                else:
                    bar.trading_day = get_trading_date(dt)
                bar.time = dt.strftime('%H:%M:%S')
                bar.open_interest = float(bar_data.get('open_interest', 0))

                if self.use_renko:
                    self.kline.add_bar(bar)
                else:
                    # bar得时间，与当前时间相隔超过一个小时，加入完整得bar
                    if (dt_now - dt).total_seconds() > 60 * 60:
                        self.kline.add_bar(bar, bar_is_completed=True, bar_freq=60)
                    # 可能是最后一根bar
                    else:
                        self.write_log(u'更新最后一根Bar:{},now:{}'.format(dt, dt_now))
                        self.kline.add_bar(bar, bar_is_completed=False, bar_freq=dt.minute)
        else:
            self.write_log(u'当前没有资金历史K线文件:{}'.format(self.kline_file))

        self.inited = True

        # 设置 kline的输出文件
        self.kline.export_filename = self.kline_file
        self.kline.export_fields = [
            {'name': 'datetime', 'source': 'bar', 'attr': 'datetime', 'type_': 'datetime'},
            {'name': 'open', 'source': 'bar', 'attr': 'open_price', 'type_': 'float'},
            {'name': 'high', 'source': 'bar', 'attr': 'high_price', 'type_': 'float'},
            {'name': 'low', 'source': 'bar', 'attr': 'low_price', 'type_': 'float'},
            {'name': 'close', 'source': 'bar', 'attr': 'close_price', 'type_': 'float'},
            {'name': 'turnover', 'source': 'bar', 'attr': 'turnover', 'type_': 'float'},
            {'name': 'volume', 'source': 'bar', 'attr': 'volume', 'type_': 'float'},
            {'name': 'open_interest', 'source': 'bar', 'attr': 'open_interest', 'type_': 'float'}
        ]

    def save(self):
        """保存数据"""
        for bar in self.kline.line_bar:
            self.kline.export_to_csv(bar)

    def load_trade_list(self):
        """
        加载策略的交易记录  data/xxxx_trade.csv
        :return:
        """
        trade_csv_file = str(get_folder_path('data').joinpath('{}_trade.csv'.format(self.kline_name)))

        if not os.path.exists(trade_csv_file):
            self.write_log('交易文件{} 不存在，不需要加载交易记录')
            return

        trade_df = pd.read_csv(trade_csv_file)
        # 对交易记录进行处理，计算平仓盈亏
        for _, row in trade_df.iterrows():
            try:
                direction = row.get('direction', None)
                offset = row.get('offset', None)
                price = row.get('price', None)
                vt_symbol = row.get('vt_symbol', None)
                volume = row.get('volume', None)
                trade_time = row.get('time', None)

                if 'SPD' in vt_symbol:
                    continue

                # 如果开仓类型，放入队列
                if direction == u'多' and offset == u'开仓':
                    exist_buy_list = self.long_pos_dict.get(vt_symbol, [])
                    exist_buy_list.append({'volume': volume, 'price': price, 'open_time': trade_time})
                    self.long_pos_dict.update({vt_symbol: exist_buy_list})
                    continue

                if direction == u'空' and offset == u'开仓':
                    exist_short_list = self.short_pos_dict.get(vt_symbol, [])
                    exist_short_list.append({'volume': volume, 'price': price, 'open_time': trade_time})
                    self.short_pos_dict.update({vt_symbol: exist_short_list})
                    continue

                if direction == u'空' and offset in [u'平仓', u'平今', u'平昨']:
                    sell_volume = volume
                    exist_buy_list = self.long_pos_dict.get(vt_symbol, [])

                    # 循环，一直到sell单被满足
                    while (sell_volume > 0):
                        if len(exist_buy_list) == 0:
                            self.write_log(u'{}没有足够的{}多单，数据不齐全,需要补全.{}'
                                           .format(self.kline_name, vt_symbol, row), strategy_name=self.kline_name)
                            break
                        buy_trade = exist_buy_list.pop(0)
                        buy_volume = buy_trade.get('volume', 0)
                        open_time = buy_trade.get('open_time', None)
                        trade_volume = 0

                        if sell_volume <= buy_volume:
                            trade_volume = sell_volume
                            buy_trade.update({'volume': buy_volume - sell_volume})
                            sell_volume = 0

                        else:
                            sell_volume -= buy_volume
                            trade_volume = buy_volume
                            buy_trade.update({'volume': 0})

                        self.write_log(f'{open_time} {trade_volume} => selled')

                        # 仍然有剩余的多单
                        if buy_trade.get('volume', 0) > 0:
                            exist_buy_list.insert(0, buy_trade)
                            self.long_pos_dict.update({vt_symbol: exist_buy_list})

                if direction == u'多' and offset in [u'平仓', u'平今', u'平昨']:
                    cover_volume = volume
                    exist_short_list = self.short_pos_dict.get(vt_symbol, [])

                    # 循环，一直到cover单被满足
                    while cover_volume > 0:
                        if len(exist_short_list) == 0:
                            self.write_error(u'{}没有足够的{}空单，数据不齐全,需要补全.{}'
                                             .format(self.kline_name, vt_symbol, row), strategy_name=self.kline_name)
                            break
                        short_trade = exist_short_list.pop(0)
                        short_volume = short_trade.get('volume', 0)
                        open_time = short_trade.get('open_time', None)
                        trade_volume = 0

                        if cover_volume <= short_volume:
                            trade_volume = cover_volume
                            short_trade.update({'volume': short_volume - cover_volume})
                            cover_volume = 0

                        else:
                            cover_volume -= short_volume
                            trade_volume = short_volume
                            short_trade.update({'volume': 0})

                        self.write_log(f'{open_time} {trade_volume} => covered')
                        # 仍然有剩余的空单
                        if short_trade.get('volume', 0) > 0:
                            exist_short_list.insert(0, short_trade)
                            self.short_pos_dict.update({vt_symbol: exist_short_list})

            except Exception as ex:
                self.write_error(u'{}发生异常:{}'.format(self.kline_name, str(ex)))
                pass

        self.write_log(u'{}加载历史交易数据完毕'.format(self.kline_name))
        if len(self.long_pos_dict) > 0:
            self.write_log(u'记录得持仓多单:{}'.format(self.long_pos_dict))
        if len(self.short_pos_dict) > 0:
            self.write_log(u'记录得持仓空单:{}'.format(self.short_pos_dict))

    def get_hold_pnl(self, log=False, update_list=False):
        """
        获取持仓收益
        :param: log 输出日志
        :param: update_list: 更新self.holding_list
        :return:
        """
        all_holding_profit = 0.0
        holded = False

        # 计算所有多单的持仓盈亏
        for vt_symbol, long_trade_list in self.long_pos_dict.items():
            cur_price = self.cta_engine.get_price(vt_symbol)
            if cur_price is None:
                continue
            cur_size = self.cta_engine.get_size(vt_symbol)
            long_holding_profit = 0
            traded = False
            for buy_trade in long_trade_list:
                open_price = buy_trade.get('price', 0)
                cur_profit = (cur_price - open_price) * cur_size * buy_trade.get('volume')
                long_holding_profit += cur_profit
                traded = True
                holded = True
                if update_list:
                    holding_record = {'open_time': buy_trade.get('open_time'),
                                      'vt_symbol': vt_symbol,
                                      'open_action': 'Buy',
                                      'volume': int(buy_trade.get('volume', 0)),
                                      'open_price': float(buy_trade.get('price', 0.0)),
                                      'cur_price': cur_price,
                                      'cur_profit': cur_profit,
                                      'holding_profit': long_holding_profit
                                      }
                    self.holding_list.append(holding_record)
            all_holding_profit += long_holding_profit
            if log and traded:
                self.write_log(u'{}多单持仓收益:{}'.format(vt_symbol, long_holding_profit), strategy_name=self.kline_name)

        # 计算所有空单的持仓盈亏
        for vt_symbol, short_trade_list in self.short_pos_dict.items():
            cur_price = self.cta_engine.get_price(vt_symbol)
            if cur_price is None:
                continue
            cur_size = self.cta_engine.get_size(vt_symbol)
            short_holding_profit = 0
            traded = False
            for short_trade in short_trade_list:
                open_price = short_trade.get('price', 0)
                cur_profit = (open_price - cur_price) * cur_size * short_trade.get('volume')
                short_holding_profit += cur_profit
                traded = True
                holded = True
                if update_list:
                    holding_record = {'open_time': short_trade.get('open_time'),
                                      'vt_symbol': vt_symbol,
                                      'open_action': 'Buy',
                                      'volume': int(short_trade.get('volume', 0)),
                                      'open_price': float(short_trade.get('price', 0.0)),
                                      'cur_price': cur_price,
                                      'cur_profit': cur_profit,
                                      'holding_profit': short_holding_profit
                                      }
                    self.holding_list.append(holding_record)
            all_holding_profit += short_holding_profit
            if log and traded:
                self.write_log(u'{}空单单持仓收益:{}'.format(vt_symbol, short_holding_profit), strategy_name=self.kline_name)
        return all_holding_profit, holded

    def on_bar(self, *args, **kwargs):
        if self.onbar_callback and (len(args) > 0 or len(kwargs) > 0):
            try:
                self.onbar_callback(*args, **kwargs)
            except Exception as ex:
                self.write_error(u'执行onbar回调函数异常:{},tb:{}'.format(str(ex), traceback.format_exc()))

    def update_account(self, dt, balance):
        """
        更新资金曲线
        :param dt:
        :param balance: 账号级别，直接使用账号得的balance；
        :return:
        """
        tick = TickData(
            gateway_name='Fund',
            symbol=self.symbol,
            exchange=Exchange.LOCAL,
            datetime=dt
        )
        tick.last_price = balance
        tick.volume = 1
        tick.ask_price_1 = balance
        tick.ask_volume_1 = 1
        tick.bid_price_1 = balance
        tick.bid_volume_1 = 1
        tick.date = tick.datetime.strftime('%Y-%m-%d')
        tick.time = tick.datetime.strftime('%H:%M:%S')
        tick.trading_day = get_trading_date(dt)
        tick.open_interest = balance

        if self.inited:
            self.kline.on_tick(tick)

        # 如果是从账号更新，无法更新持仓盈亏
        self.closed_profit = balance
        self.holding_profit = 0

    def update_trade(self, trade):
        """
        更新策略的交易记录
        :param trade:
        :return:
        """
        try:

            # 如果开仓类型，放入队列
            if trade.direction == Direction.LONG and trade.offset == Offset.OPEN:
                exist_buy_list = self.long_pos_dict.get(trade.vt_symbol, [])
                exist_buy_list.append({'volume': trade.volume, 'price': trade.price, 'open_time': trade.time})
                self.long_pos_dict.update({trade.vt_symbol: exist_buy_list})
                self.write_log(u'更新{}的持仓记录:{}'.format(trade.vt_symbol, exist_buy_list))
                return

            if trade.direction == Direction.SHORT and trade.offset == Offset.OPEN:
                exist_short_list = self.short_pos_dict.get(trade.vt_symbol, [])
                exist_short_list.append({'volume': trade.volume, 'price': trade.price, 'open_time': trade.time})
                self.short_pos_dict.update({trade.vt_symbol: exist_short_list})
                self.write_log(u'更新{}的持仓记录:{}'.format(trade.vt_symbol, exist_short_list))
                return

            if trade.direction == Direction.SHORT and trade.offset in [Offset.CLOSE, Offset.CLOSETODAY,
                                                                       Offset.CLOSEYESTERDAY]:
                sell_volume = trade.volume
                exist_buy_list = self.long_pos_dict.get(trade.vt_symbol, [])
                close_profit = 0
                # 循环，一直到sell单被满足
                while (sell_volume > 0):
                    if len(exist_buy_list) == 0:
                        self.write_error(
                            u'{}没有足够的{}多单记录，数据不齐全.{}'.format(self.kline_name, trade.vt_symbol, trade.__dict__))
                        return
                    buy_trade = exist_buy_list.pop(0)
                    buy_volume = buy_trade.get('volume', 0)
                    trade_volume = 0

                    if sell_volume <= buy_volume:
                        trade_volume = sell_volume
                        buy_trade.update({'volume': buy_volume - sell_volume})
                        sell_volume = 0

                    else:
                        sell_volume -= buy_volume
                        trade_volume = buy_volume
                        buy_trade.update({'volume': 0})

                    # 仍然有剩余的多单
                    if buy_trade.get('volume', 0) > 0:
                        exist_buy_list.insert(0, buy_trade)
                        self.long_pos_dict.update({trade.vt_symbol: exist_buy_list})

                    symbol_size = self.cta_engine.get_size(trade.vt_symbol)
                    cur_profit = (trade.price - float(buy_trade.get('price', 0.0))) * symbol_size * trade_volume
                    close_profit += cur_profit
                    self.closed_profit += cur_profit
                    profit_record = {'open_time': buy_trade.get('time'),
                                     'vt_symbol': trade.vt_symbol,
                                     'open_action': 'Buy',
                                     'volume': trade_volume,
                                     'open_price': buy_trade.get('price', 0.0),
                                     'close_time': trade.time,
                                     'close_price': trade.price,
                                     'close_action': 'Sell',
                                     'cur_profit': cur_profit,
                                     'closed_profit': self.closed_profit
                                     }
                    self.profit_list.append(profit_record)

                trade_dt = datetime.now()
                if len(trade.time) > 8 and ' ' in trade.time:
                    try:
                        trade_dt = datetime.strptime(trade.time, '%Y-%m-%d %H:%M:%S')
                    except Exception:
                        trade_dt = datetime.now()

                hold_pnl, _ = self.get_hold_pnl(log=True)

                self.update_strategy(dt=trade_dt, closed_pnl=close_profit, hold_pnl=hold_pnl)

                cur_openIntesting = self.kline.line_bar[-1].open_interest if len(
                    self.kline.line_bar) > 0 else close_profit
                self.write_log(u'{} 多单 {} => {}平仓，当前平仓收益:{}，策略收益:{}'
                               .format(self.kline_name, trade.vt_symbol, trade.time,
                                       close_profit, cur_openIntesting))

            if trade.direction == Direction.LONG and trade.offset in [Offset.CLOSE, Offset.CLOSETODAY,
                                                                      Offset.CLOSEYESTERDAY]:
                cover_volume = trade.volume
                exist_short_list = self.short_pos_dict.get(trade.vt_symbol, [])
                close_profit = 0
                # 循环，一直到cover单被满足
                while (cover_volume > 0):
                    if len(exist_short_list) == 0:
                        self.write_error(u'{}没有足够的{}空单，数据不齐全,数据需要补全.{}'
                                         .format(self.kline_name, trade.vt_symbol, trade.__dict__))
                        return
                    short_trade = exist_short_list.pop(0)
                    short_volume = short_trade.get('volume', 0)
                    open_time = short_trade.get('open_time', None)
                    trade_volume = 0

                    if cover_volume <= short_volume:
                        trade_volume = cover_volume
                        short_trade.update({'volume': short_volume - cover_volume})
                        cover_volume = 0

                    else:
                        cover_volume -= short_volume
                        trade_volume = short_volume
                        short_trade.update({'volume': 0})

                    # 仍然有剩余的空单
                    if short_trade.get('volume', 0) > 0:
                        exist_short_list.insert(0, short_trade)
                        self.short_pos_dict.update({trade.vt_symbol: exist_short_list})
                    symbol_size = self.cta_engine.get_size(trade.vt_symbol)
                    cur_profit = (float(short_trade.get('price', 0.0)) - trade.price) * symbol_size * trade_volume
                    close_profit += cur_profit
                    self.closed_profit += cur_profit
                    profit_record = {'open_time': short_trade.get('time'),
                                     'vt_symbol': trade.vt_symbol,
                                     'open_action': 'Short',
                                     'volume': trade_volume,
                                     'open_price': short_trade.get('price', 0.0),
                                     'close_time': trade.time,
                                     'close_price': trade.price,
                                     'close_action': 'Cover',
                                     'cur_profit': cur_profit,
                                     'closed_profit': self.closed_profit
                                     }
                    self.profit_list.append(profit_record)

                cur_openIntesting = self.kline.line_bar[-1].open_interest if len(
                    self.kline.line_bar) > 0 else close_profit
                self.write_log(f'{self.kline_name} {open_time} {trade.vt_symbol}空单 => '
                               f'{trade.time} 平仓，累计平仓收益:{close_profit}，策略收益:{cur_openIntesting}')
                trade_dt = datetime.now()
                if len(trade.time) > 8 and ' ' in trade.time:
                    try:
                        trade_dt = datetime.strptime(trade.time, '%Y-%m-%d %H:%M:%S')
                    except Exception:
                        trade_dt = datetime.now()

                hold_pnl, _ = self.get_hold_pnl(log=True)
                self.update_strategy(dt=trade_dt, closed_pnl=close_profit, hold_pnl=hold_pnl)

        except Exception as ex:
            self.write_error(u'更新策略{}交易记录异常:{}'.format(self.kline_name, str(ex)))
            self.write_error(traceback.format_exc())

    def update_strategy(self, dt, closed_pnl=0, hold_pnl=0):
        """
        更新资金曲线
        :param dt:
        :param closed_pnl: 策略提供的平仓盈亏
        :param hold_pnl: 策略提供的持仓盈亏
        :return:
        """
        # 获取当前bar的平仓权益
        open_interest = 0
        if len(self.kline.line_bar) > 0:
            open_interest = self.kline.line_bar[-1].open_interest

        if closed_pnl != 0:
            self.write_log(u'策略平仓收益:{}->{}'.format(open_interest, open_interest + closed_pnl))
            open_interest += closed_pnl

        tick = TickData(gateway_name='Fund',
                        symbol=self.symbol,
                        exchange=Exchange.LOCAL,
                        datetime=dt)

        tick.last_price = open_interest + hold_pnl
        tick.volume = 1
        tick.ask_price1 = open_interest + hold_pnl
        tick.ask_volume1 = 1
        tick.bid_price1 = open_interest + hold_pnl
        tick.bid_volume1 = 1
        tick.datetime = dt
        tick.open_interest = open_interest
        tick.date = tick.datetime.strftime('%Y-%m-%d')
        tick.time = tick.datetime.strftime('%H:%M:%S')
        tick.trading_day = get_trading_date(dt)

        if self.inited:
            self.kline.on_tick(tick)

        self.closed_profit = open_interest
        self.holding_profit = hold_pnl
