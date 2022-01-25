# encoding: UTF-8

'''
本文件中包含的是CTA模块的组合回测引擎，回测引擎的API和CTA引擎一致，
可以使用和实盘相同的代码进行回测。
华富资产 李来佳
'''
from __future__ import division

import sys
import os
import gc
import pandas as pd
import numpy as np
import traceback
import random
import bz2
import pickle

from datetime import datetime, timedelta
from time import sleep

from vnpy.trader.object import (
    TickData,
    BarData,
    RenkoBarData,
)
from vnpy.trader.constant import (
    Exchange,
)

from vnpy.trader.utility import (
    extract_vt_symbol,
    get_underlying_symbol,
    get_trading_date,
    import_module_by_str
)

from .back_testing import BackTestingEngine

# vnpy交易所，与淘宝数据tick目录得对应关系
VN_EXCHANGE_TICKFOLDER_MAP = {
    Exchange.SHFE.value: 'SQ',
    Exchange.DCE.value: 'DL',
    Exchange.CZCE.value: 'ZZ',
    Exchange.CFFEX.value: 'ZJ',
    Exchange.INE.value: 'SQ'
}

class PortfolioTestingEngine(BackTestingEngine):
    """
    CTA组合回测引擎, 使用回测引擎作为父类
    函数接口和策略引擎保持一样，
    从而实现同一套代码从回测到实盘。
    针对1分钟bar的回测 或者tick回测
    导入CTA_Settings

    """

    def __init__(self, event_engine=None):
        """Constructor"""
        super().__init__(event_engine)

        self.bar_csv_file = {}
        self.bar_df_dict = {}  # 历史数据的df，回测用
        self.bar_df = None  # 历史数据的df，时间+symbol作为组合索引
        self.bar_interval_seconds = 60  # bar csv文件，属于K线类型，K线的周期（秒数）,缺省是1分钟

        self.tick_path = None  # tick级别回测， 路径
        self.use_tq = False    # True:使用tq csv数据; False:使用淘宝购买的csv数据(19年之前)
        self.use_pkb2 = True  # 使用tdx下载的逐笔成交数据（pkb2压缩格式），模拟tick

    def load_bar_csv_to_df(self, vt_symbol, bar_file, data_start_date=None, data_end_date=None):
        """加载回测bar数据到DataFrame"""
        self.output(u'loading {} from {}'.format(vt_symbol, bar_file))
        if vt_symbol in self.bar_df_dict:
            return True

        if bar_file is None or not os.path.exists(bar_file):
            self.write_error(u'回测时，{}对应的csv bar文件{}不存在'.format(vt_symbol, bar_file))
            return False

        try:
            data_types = {
                "datetime": str,
                "open": float,
                "high": float,
                "low": float,
                "close": float,
                "open_interest": float,
                "volume": float,
                "instrument_id": str,
                "symbol": str,
                "total_turnover": float,
                "limit_down": float,
                "limit_up": float,
                "trading_day": str,
                "date": str,
                "time": str
            }
            if vt_symbol.startswith('future_renko'):
                data_types.update({
                    "color": str,
                    "seconds": int,
                    "high_seconds": int,
                    "low_seconds": int,
                    "height": float,
                    "up_band": float,
                    "down_band": float,
                    "low_time": str,
                    "high_time": str
                })
            # 加载csv文件 =》 dateframe
            symbol_df = pd.read_csv(bar_file, dtype=data_types)
            if len(symbol_df)==0:
                print(f'回测时加载{vt_symbol} csv文件{bar_file}失败。', file=sys.stderr)
                self.write_error(f'回测时加载{vt_symbol} csv文件{bar_file}失败。')
                return False

            first_dt = symbol_df.iloc[0]['datetime']
            if '.' in first_dt:
                datetime_format = "%Y-%m-%d %H:%M:%S.%f"
            else:
                datetime_format = "%Y-%m-%d %H:%M:%S"
            # 转换时间，str =》 datetime
            symbol_df["datetime"] = pd.to_datetime(symbol_df["datetime"], format=datetime_format)
            # 设置时间为索引
            symbol_df = symbol_df.set_index("datetime")

            # 裁剪数据
            symbol_df = symbol_df.loc[self.test_start_date:self.test_end_date]

            self.bar_df_dict.update({vt_symbol: symbol_df})
        except Exception as ex:
            self.write_error(u'回测时读取{} csv文件{}失败:{}'.format(vt_symbol, bar_file, ex))
            self.output(u'回测时读取{} csv文件{}失败:{}'.format(vt_symbol, bar_file, ex))
            return False

        return True

    def comine_bar_df(self):
        """
        合并所有回测合约的bar DataFrame =》集中的DataFrame
        把bar_df_dict =》bar_df
        :return:
        """
        self.output('comine_df')
        if len(self.bar_df_dict) == 0:
            print(f'{self.test_name}:无加载任何数据,请检查bar文件路径配置',file=sys.stderr)
            self.output(f'{self.test_name}:无加载任何数据,请检查bar文件路径配置')

        self.bar_df = pd.concat(self.bar_df_dict, axis=0).swaplevel(0, 1).sort_index()
        self.bar_df_dict.clear()

    def prepare_env(self, test_setting):
        self.output('portfolio prepare_env')
        super().prepare_env(test_setting)

        self.use_tq = test_setting.get('use_tq', False)
        self.use_pkb2 = test_setting.get('use_pkb2', True)
        if self.use_tq:
            self.use_pkb2 = False
            self.output(f'使用天勤数据')

        if self.use_pkb2:
            self.output(f'使用pkb2压缩格式')
        else:
            self.output(f'使用csv文件格式')

    def prepare_data(self, data_dict):
        """
        准备组合数据
        :param data_dict: 合约得配置参数
        :return:
        """
        # 调用回测引擎，跟新合约得数据
        super().prepare_data(data_dict)

        if len(data_dict) == 0:
            self.write_log(u'请指定回测数据和文件')
            return

        if self.mode == 'tick':
            return

        # 检查/更新bar文件
        for symbol, symbol_data in data_dict.items():
            self.write_log(u'配置{}数据:{}'.format(symbol, symbol_data))

            bar_file = symbol_data.get('bar_file', None)

            if bar_file is None:
                self.write_error(u'{}没有配置数据文件')
                continue

            if not os.path.isfile(bar_file):
                self.write_log(u'{0}文件不存在'.format(bar_file))
                continue

            self.bar_csv_file.update({symbol: bar_file})

    def run_portfolio_test(self, strategy_setting: dict = {}):
        """
        运行组合回测
        """
        if not self.strategy_start_date:
            self.write_error(u'回测开始日期未设置。')
            return

        if len(strategy_setting) == 0:
            self.write_error('未提供有效配置策略实例')
            return

        self.cur_capital = self.init_capital  # 更新设置期初资金
        if not self.data_end_date:
            self.data_end_date = datetime.today()
            self.test_end_date = datetime.now().strftime('%Y%m%d')

        # 保存回测脚本到数据库
        self.save_setting_to_mongo()

        self.write_log(u'开始组合回测')

        for strategy_name, strategy_setting in strategy_setting.items():
            self.load_strategy(strategy_name, strategy_setting)

        self.write_log(u'策略初始化完成')

        self.write_log(u'开始回放数据')

        self.write_log(u'开始回测:{} ~ {}'.format(self.data_start_date, self.data_end_date))

        if self.mode == 'bar':
            self.run_bar_test()
        else:
            self.run_tick_test()

    def run_bar_test(self):
        """使用bar进行组合回测"""
        testdays = (self.data_end_date - self.data_start_date).days

        if testdays < 1:
            self.write_log(u'回测时间不足')
            return

        # 加载数据
        for vt_symbol in self.symbol_strategy_map.keys():
            symbol, exchange = extract_vt_symbol(vt_symbol)
            self.load_bar_csv_to_df(vt_symbol, self.bar_csv_file.get(symbol))

            # 为套利合约提取主动 / 被动合约
            if exchange == Exchange.SPD:
                try:
                    active_symbol, active_rate, passive_symbol, passive_rate, spd_type = symbol.split('-')
                    active_vt_symbol = '.'.join([active_symbol, self.get_exchange(symbol=active_symbol).value])
                    passive_vt_symbol = '.'.join([passive_symbol, self.get_exchange(symbol=passive_symbol).value])
                    self.load_bar_csv_to_df(active_vt_symbol, self.bar_csv_file.get(active_symbol))
                    self.load_bar_csv_to_df(passive_vt_symbol, self.bar_csv_file.get(passive_symbol))
                except Exception as ex:
                    self.write_error(u'为套利合约提取主动/被动合约出现异常:{}'.format(str(ex)))

        # 合并数据
        self.comine_bar_df()

        last_trading_day = None
        bars_dt = None
        bars_same_dt = []

        gc_collect_days = 0

        try:
            for (dt, vt_symbol), bar_data in self.bar_df.iterrows():
                symbol, exchange = extract_vt_symbol(vt_symbol)
                if symbol.startswith('future_renko'):
                    bar_datetime = dt
                    bar = RenkoBarData(
                        gateway_name='backtesting',
                        symbol=symbol,
                        exchange=exchange,
                        datetime=bar_datetime
                    )
                    bar.seconds = float(bar_data.get('seconds', 0))
                    bar.high_seconds = float(bar_data.get('high_seconds', 0))  # 当前Bar的上限秒数
                    bar.low_seconds = float(bar_data.get('low_seconds', 0))  # 当前bar的下限秒数
                    bar.height = float(bar_data.get('height', 0))  # 当前Bar的高度限制
                    bar.up_band = float(bar_data.get('up_band', 0))  # 高位区域的基线
                    bar.down_band = float(bar_data.get('down_band', 0))  # 低位区域的基线
                    bar.low_time = bar_data.get('low_time', None)  # 最后一次进入低位区域的时间
                    bar.high_time = bar_data.get('high_time', None)  # 最后一次进入高位区域的时间
                else:
                    # 读取的bar是以bar结束时间作为datetime，vnpy是以bar开始时间作为bar datetime
                    bar_datetime = dt - timedelta(seconds=self.bar_interval_seconds)

                    bar = BarData(
                        gateway_name='backtesting',
                        symbol=symbol,
                        exchange=exchange,
                        datetime=bar_datetime
                    )

                bar.open_price = float(bar_data['open'])
                bar.close_price = float(bar_data['close'])
                bar.high_price = float(bar_data['high'])
                bar.low_price = float(bar_data['low'])
                bar.volume = int(bar_data['volume'])
                bar.open_interest = float(bar_data.get('open_interest', 0))
                bar.date = bar_datetime.strftime('%Y-%m-%d')
                bar.time = bar_datetime.strftime('%H:%M:%S')
                str_td = str(bar_data.get('trading_day', ''))
                if len(str_td) == 8:
                    bar.trading_day = str_td[0:4] + '-' + str_td[4:6] + '-' + str_td[6:8]
                elif len(str_td) == 10:
                    bar.trading_day = str_td
                else:
                    bar.trading_day = get_trading_date(bar_datetime)

                if last_trading_day != bar.trading_day:
                    self.output(u'回测数据日期:{},资金:{}'.format(bar.trading_day, self.net_capital))
                    if self.strategy_start_date > bar.datetime:
                        last_trading_day = bar.trading_day

                # bar时间与队列时间一致，添加到队列中
                if dt == bars_dt:
                    bars_same_dt.append(bar)
                    continue
                else:
                    # bar时间与队列时间不一致，先推送队列的bars
                    random.shuffle(bars_same_dt)
                    for _bar_ in bars_same_dt:
                        self.new_bar(_bar_)

                    # 创建新的队列
                    bars_same_dt = [bar]
                    bars_dt = dt

                # 更新每日净值
                if self.strategy_start_date <= dt <= self.data_end_date:
                    if last_trading_day != bar.trading_day:
                        if last_trading_day is not None:
                            self.saving_daily_data(datetime.strptime(last_trading_day, '%Y-%m-%d'), self.cur_capital,
                                                   self.max_net_capital, self.total_commission)
                        last_trading_day = bar.trading_day

                        # 第二个交易日,撤单
                        if not symbol.startswith('future_renko'):
                            self.cancel_orders()
                        # 更新持仓缓存
                        self.update_pos_buffer()

                        gc_collect_days += 1
                        if gc_collect_days >= 10:
                            # 执行内存回收
                            gc.collect()
                            sleep(1)
                            gc_collect_days = 0

                if self.net_capital < 0:
                    self.write_error(u'净值低于0，回测停止')
                    self.output(u'净值低于0，回测停止')
                    return

            self.write_log(u'bar数据回放完成')
            if last_trading_day is not None:
                self.saving_daily_data(datetime.strptime(last_trading_day, '%Y-%m-%d'), self.cur_capital,
                                       self.max_net_capital, self.total_commission)
        except Exception as ex:
            self.write_error(u'回测异常导致停止:{}'.format(str(ex)))
            self.write_error(u'{},{}'.format(str(ex), traceback.format_exc()))
            print(str(ex), file=sys.stderr)
            traceback.print_exc()
            return

    def load_csv_file(self, tick_folder, vt_symbol, tick_date):
        """从文件中读取tick，返回list[{dict}]"""

        # 使用天勤tick数据
        if self.use_tq:
            return self.load_tq_csv_file(tick_folder, vt_symbol, tick_date)

        # 使用淘宝下载的tick数据（2019年前）
        symbol, exchange = extract_vt_symbol(vt_symbol)
        underly_symbol = get_underlying_symbol(symbol)
        exchange_folder = VN_EXCHANGE_TICKFOLDER_MAP.get(exchange.value)

        if exchange == Exchange.INE:
            file_path = os.path.abspath(
                os.path.join(
                    tick_folder,
                    exchange_folder,
                    tick_date.strftime('%Y'),
                    tick_date.strftime('%Y%m'),
                    tick_date.strftime('%Y%m%d'),
                    '{}_{}.csv'.format(symbol.upper(), tick_date.strftime('%Y%m%d'))))
        else:
            file_path = os.path.abspath(
                os.path.join(
                    tick_folder,
                    exchange_folder,
                    tick_date.strftime('%Y'),
                    tick_date.strftime('%Y%m'),
                    tick_date.strftime('%Y%m%d'),
                    '{}{}_{}.csv'.format(underly_symbol.upper(), symbol[-2:], tick_date.strftime('%Y%m%d'))))

        ticks = []
        if not os.path.isfile(file_path):
            self.write_log(f'{file_path}文件不存在')
            return None

        df = pd.read_csv(file_path, encoding='gbk', parse_dates=False)
        df.columns = ['date', 'time', 'last_price', 'last_volume', 'volume', 'open_interest',
                      'bid_price_1', 'bid_volume_1', 'bid_price_2', 'bid_volume_2', 'bid_price_3', 'bid_volume_3',
                      'ask_price_1', 'ask_volume_1', 'ask_price_2', 'ask_volume_2', 'ask_price_3', 'ask_volume_3', 'BS']

        self.write_log(u'加载csv文件{}'.format(file_path))
        last_time = None
        for index, row in df.iterrows():
            # 日期, 时间, 成交价, 成交量, 总量, 属性(持仓增减), B1价, B1量, B2价, B2量, B3价, B3量, S1价, S1量, S2价, S2量, S3价, S3量, BS
            # 0    1      2      3       4      5               6     7    8     9     10     11    12    13    14   15    16   17    18

            tick = row.to_dict()
            tick.update({'symbol': symbol, 'exchange': exchange.value, 'trading_day': tick_date.strftime('%Y-%m-%d')})
            tick_datetime = datetime.strptime(tick['date'] + ' ' + tick['time'], '%Y-%m-%d %H:%M:%S')

            # 修正毫秒
            if tick['time'] == last_time:
                # 与上一个tick的时间（去除毫秒后）相同,修改为500毫秒
                tick_datetime = tick_datetime.replace(microsecond=500)
                tick['time'] = tick_datetime.strftime('%H:%M:%S.%f')
            else:
                last_time = tick['time']
                tick_datetime = tick_datetime.replace(microsecond=0)
                tick['time'] = tick_datetime.strftime('%H:%M:%S.%f')
            tick['datetime'] = tick_datetime

            # 排除涨停/跌停的数据
            if (float(tick['bid_price_1']) == float('1.79769E308') and int(tick['bid_volume_1']) == 0) \
                    or (float(tick['ask_price_1']) == float('1.79769E308') and int(tick['ask_volume_1']) == 0):
                continue

            ticks.append(tick)

        del df

        return ticks

    def load_tq_csv_file(self, tick_folder, vt_symbol, tick_date):
        """从天勤下载的csv文件中读取tick，返回list[{dict}]"""

        symbol, exchange = extract_vt_symbol(vt_symbol)
        underly_symbol = get_underlying_symbol(symbol)
        exchange_folder = VN_EXCHANGE_TICKFOLDER_MAP.get(exchange.value)

        file_path = os.path.abspath(
            os.path.join(
                tick_folder,
                tick_date.strftime('%Y%m'),
                '{}_{}.csv'.format(symbol, tick_date.strftime('%Y%m%d'))))

        ticks = []
        if not os.path.isfile(file_path):
            self.write_log(u'{}文件不存在'.format(file_path))
            file_path = os.path.abspath(
                os.path.join(
                    tick_folder,
                    tick_date.strftime('%Y%m'),
                    '{}_{}.csv'.format(symbol.lower(), tick_date.strftime('%Y%m%d'))))
            if not os.path.isfile(file_path):
                self.write_log(u'{}文件不存在'.format(file_path))
                return None
        try:
            df = pd.read_csv(file_path, parse_dates=False)
            # datetime,symbol,exchange,last_price,highest,lowest,volume,amount,open_interest,upper_limit,lower_limit,
            # bid_price_1,bid_volume_1,ask_price_1,ask_volume_1,
            # bid_price_2,bid_volume_2,ask_price_2,ask_volume_2,
            # bid_price_3,bid_volume_3,ask_price_3,ask_volume_3,
            # bid_price_4,bid_volume_4,ask_price_4,ask_volume_4,
            # bid_price_5,bid_volume_5,ask_price_5,ask_volume_5

            self.write_log(u'加载csv文件{}'.format(file_path))
            last_time = None
            for index, row in df.iterrows():

                tick = row.to_dict()
                tick['date'], tick['time'] = tick['datetime'].split(' ')
                tick.update({'trading_day': tick_date.strftime('%Y-%m-%d')})
                tick_datetime = datetime.strptime(tick['datetime'], '%Y-%m-%d %H:%M:%S.%f')

                # 修正毫秒
                if tick['time'] == last_time:
                    # 与上一个tick的时间（去除毫秒后）相同,修改为500毫秒
                    tick_datetime = tick_datetime.replace(microsecond=500)
                    tick['time'] = tick_datetime.strftime('%H:%M:%S.%f')
                else:
                    last_time = tick['time']
                    tick_datetime = tick_datetime.replace(microsecond=0)
                    tick['time'] = tick_datetime.strftime('%H:%M:%S.%f')
                tick['datetime'] = tick_datetime

                # 排除涨停/跌停的数据
                if (float(tick['bid_price_1']) == float('1.79769E308') and int(tick['bid_volume_1']) == 0) \
                        or (float(tick['ask_price_1']) == float('1.79769E308') and int(tick['ask_volume_1']) == 0):
                    continue

                ticks.append(tick)

            del df
        except Exception as ex:
            self.write_log(f'{file_path}文件读取不成功: {str(ex)}')
            return None
        return ticks

    def load_bz2_cache(self, cache_folder, cache_symbol, cache_date):
        """加载缓存数据"""
        if not os.path.exists(cache_folder):
            self.write_error('缓存目录:{}不存在,不能读取'.format(cache_folder))
            return None
        cache_folder_year_month = os.path.join(cache_folder, cache_date[:6])
        if not os.path.exists(cache_folder_year_month):
            self.write_error('缓存目录:{}不存在,不能读取'.format(cache_folder_year_month))
            return None

        cache_file = os.path.join(cache_folder_year_month, '{}_{}.pkb2'.format(cache_symbol, cache_date))
        if not os.path.isfile(cache_file):
            cache_file = os.path.join(cache_folder_year_month, '{}_{}.pkz2'.format(cache_symbol, cache_date))
            if not os.path.isfile(cache_file):
                self.write_error('缓存文件:{}不存在,不能读取'.format(cache_file))
                return None

        with bz2.BZ2File(cache_file, 'rb') as f:
            data = pickle.load(f)
            return data

        return None

    def get_day_tick_df(self, test_day):
        """获取某一天得所有合约tick"""
        tick_data_dict = {}

        for vt_symbol in list(self.symbol_strategy_map.keys()):
            symbol, exchange = extract_vt_symbol(vt_symbol)
            if self.use_pkb2:
                tick_list = self.load_bz2_cache(cache_folder=self.tick_path,
                                                cache_symbol=symbol,
                                                cache_date=test_day.strftime('%Y%m%d'))
            else:
                tick_list = self.load_csv_file(tick_folder=self.tick_path,
                                               vt_symbol=vt_symbol,
                                               tick_date=test_day)

            if not tick_list or len(tick_list) == 0:
                continue

            symbol_tick_df = pd.DataFrame(tick_list)
            # 缓存文件中，datetime字段，已经是datetime格式
            # 暂时根据时间去重，没有汇总volume
            symbol_tick_df.drop_duplicates(subset=['datetime'], keep='first', inplace=True)
            symbol_tick_df.set_index('datetime', inplace=True)

            tick_data_dict.update({vt_symbol: symbol_tick_df})

        if len(tick_data_dict) == 0:
            return None

        tick_df = pd.concat(tick_data_dict, axis=0).swaplevel(0, 1).sort_index()

        return tick_df

    def run_tick_test(self):
        """运行tick级别组合回测"""
        testdays = (self.data_end_date - self.data_start_date).days

        if testdays < 1:
            self.write_log(u'回测时间不足')
            return

        gc_collect_days = 0

        # 循环每一天
        for i in range(0, testdays):
            test_day = self.data_start_date + timedelta(days=i)

            combined_df = self.get_day_tick_df(test_day)

            if combined_df is None:
                continue

            try:
                for (dt, vt_symbol), tick_data in combined_df.iterrows():
                    symbol, exchange = extract_vt_symbol(vt_symbol)
                    last_price = tick_data.get('last_price',None)
                    if not last_price:
                        last_price = tick_data.get('price',None)
                    if not isinstance(last_price, float):
                        continue
                    if np.isnan(last_price):
                        continue
                    tick = TickData(
                        gateway_name='backtesting',
                        symbol=symbol,
                        exchange=exchange,
                        datetime=dt,
                        date=dt.strftime('%Y-%m-%d'),
                        time=dt.strftime('%H:%M:%S.%f'),
                        trading_day=test_day.strftime('%Y-%m-%d'),
                        last_price=last_price,
                        volume=tick_data['volume'],
                        ask_price_1=float(tick_data.get('ask_price_1',0)),
                        ask_volume_1=int(tick_data.get('ask_volume_1',0)),
                        bid_price_1=float(tick_data.get('bid_price_1',0)),
                        bid_volume_1=int(tick_data.get('bid_volume_1',0))
                    )
                    if tick_data.get('ask_price_5',0) > 0:
                        tick.ask_price_2 = float(tick_data.get('ask_price_2',0))
                        tick.ask_volume_2 = int(tick_data.get('ask_volume_2', 0))
                        tick.bid_price_2 = float(tick_data.get('bid_price_2', 0))
                        tick.bid_volume_2 = int(tick_data.get('bid_volume_2', 0))

                        tick.ask_price_3 = float(tick_data.get('ask_price_3', 0))
                        tick.ask_volume_3 = int(tick_data.get('ask_volume_3', 0)),
                        tick.bid_price_3 = float(tick_data.get('bid_price_3', 0)),
                        tick.bid_volume_3 = int(tick_data.get('bid_volume_3', 0))

                        tick.ask_price_4 = float(tick_data.get('ask_price_4', 0))
                        tick.ask_volume_4 = int(tick_data.get('ask_volume_4', 0)),
                        tick.bid_price_4 = float(tick_data.get('bid_price_4', 0)),
                        tick.bid_volume_4 = int(tick_data.get('bid_volume_4', 0))

                        tick.ask_price_5 = float(tick_data.get('ask_price_5', 0))
                        tick.ask_volume_5 = int(tick_data.get('ask_volume_5', 0)),
                        tick.bid_price_5 = float(tick_data.get('bid_price_5', 0)),
                        tick.bid_volume_5 = int(tick_data.get('bid_volume_5', 0))

                    self.new_tick(tick)

                # 结束一个交易日后，更新每日净值
                self.saving_daily_data(test_day,
                                       self.cur_capital,
                                       self.max_net_capital,
                                       self.total_commission)

                self.cancel_orders()
                # 更新持仓缓存
                self.update_pos_buffer()

                gc_collect_days += 1
                if gc_collect_days >= 10:
                    # 执行内存回收
                    gc.collect()
                    sleep(1)
                    gc_collect_days = 0

                if self.net_capital < 0:
                    self.write_error(u'净值低于0，回测停止')
                    self.output(u'净值低于0，回测停止')
                    return

            except Exception as ex:
                self.write_error(u'回测异常导致停止:{}'.format(str(ex)))
                self.write_error(u'{},{}'.format(str(ex), traceback.format_exc()))
                print(str(ex), file=sys.stderr)
                traceback.print_exc()
                return

        self.write_log(u'tick数据回放完成')


def single_test(test_setting: dict, strategy_setting: dict):
    """
    单一回测
    : test_setting, 组合回测所需的配置，包括合约信息，数据bar信息，回测时间，资金等。
    ：strategy_setting, dict, 一个或多个策略配置
    """
    # 创建组合回测引擎
    engine = PortfolioTestingEngine()

    engine.prepare_env(test_setting)
    try:
        engine.run_portfolio_test(strategy_setting)
        # 回测结果，保存
        engine.show_backtesting_result()

        # 保存策略得内部数据
        engine.save_strategy_data()

    except Exception as ex:
        print('组合回测异常{}'.format(str(ex)))
        traceback.print_exc()
        engine.save_fail_to_mongo(f'回测异常{str(ex)}')
        return False

    print('测试结束')
    return True
