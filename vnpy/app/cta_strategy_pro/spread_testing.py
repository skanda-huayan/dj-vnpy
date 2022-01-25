# encoding: UTF-8

'''
本文件中包含的是CTA模块的组合套利回测引擎，回测引擎的API和CTA引擎一致，
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
    TickData, BarData
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
from vnpy.trader.gateway import TickCombiner

from .back_testing import BackTestingEngine

# vnpy交易所，与淘宝数据tick目录得对应关系
VN_EXCHANGE_TICKFOLDER_MAP = {
    Exchange.SHFE.value: 'SQ',
    Exchange.DCE.value: 'DL',
    Exchange.CZCE.value: 'ZZ',
    Exchange.CFFEX.value: 'ZJ',
    Exchange.INE.value: 'SQ'
}


class SpreadTestingEngine(BackTestingEngine):
    """
    CTA套利组合回测引擎, 使用回测引擎作为父类
    函数接口和策略引擎保持一样，
    从而实现同一套代码从回测到实盘。

    tick回测:
        1,设置tick_path,

    """

    def __init__(self, event_engine=None):
        """Constructor"""
        super().__init__(event_engine)
        self.tick_path = None  # tick级别回测， 路径
        self.use_tq = False  # True:使用tq数据; False:使用淘宝购买的数据(19年之前)
        self.strategy_start_date_dict = {}
        self.strategy_end_date_dict = {}
        self.tick_combiner_dict = {}  # tick合成器
        self.symbol_combiner_dict = {} # symbol : [combiner]

        self.bar_csv_file = {}
        self.bar_df_dict = {}  # 历史数据的df，回测用
        self.bar_df = None  # 历史数据的df，时间+symbol作为组合索引
        self.bar_interval_seconds = 60  # bar csv文件，属于K线类型，K线的周期（秒数）,缺省是1分钟
        self.on_tick = self.new_tick  # 仿造 gateway的on_tick接口 => new_tick

    def prepare_env(self, test_setting):
        self.output('portfolio prepare_env')
        super().prepare_env(test_setting)

        self.use_tq = test_setting.get('use_tq', False)

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

    def load_strategy(self, strategy_name: str, strategy_setting: dict = None):
        """
        装载回测的策略
        setting是参数设置，包括
            class_name: str, 策略类名字
            vt_symbol: str, 缺省合约
            setting: {}, 策略的参数
            auto_init: True/False, 策略是否自动初始化
            auto_start: True/False， 策略是否自动启动
        """

        # 获取策略的类名
        class_name = strategy_setting.get('class_name', None)
        if class_name is None or strategy_name is None:
            self.write_error(u'setting中没有class_name')
            return

        # strategy_class => module.strategy_class
        if '.' not in class_name:
            module_name = self.class_module_map.get(class_name, None)
            if module_name:
                class_name = module_name + '.' + class_name
                self.write_log(u'转换策略为全路径:{}'.format(class_name))

        # 获取策略类的定义
        strategy_class = import_module_by_str(class_name)
        if strategy_class is None:
            self.write_error(u'加载策略模块失败:{}'.format(class_name))
            return

        # 处理 vt_symbol
        vt_symbol = strategy_setting.get('vt_symbol')
        symbol, exchange = extract_vt_symbol(vt_symbol)

        subscribe_symobls = [vt_symbol]
        # 属于自定义套利合约
        if exchange == Exchange.SPD:
            act_symbol, act_ratio, pas_symbol, pas_ratio, spread_type = symbol.split('-')
            act_underly = get_underlying_symbol(act_symbol).upper()
            pas_underly = get_underlying_symbol(pas_symbol).upper()
            act_exchange = self.get_exchange(f'{act_underly}99')
            pas_exchange = self.get_exchange(f'{pas_underly}99')
            act_contract = self.get_contract(f'{act_underly}99.{act_exchange.value}')
            if self.get_contract(f'{act_symbol}.{act_exchange.value}') is None:
                self.set_contract(symbol=act_symbol,
                                  exchange=act_exchange,
                                  product=act_contract.product,
                                  name=act_symbol,
                                  size=act_contract.size,
                                  price_tick=act_contract.pricetick,
                                  margin_rate=act_contract.margin_rate)
                self.write_log(f'设置主动腿指数合约信息{act_symbol}.{act_exchange.value}')
            if pas_underly != act_underly:
                pas_contract = self.get_contract(f'{pas_underly}99.{pas_exchange.value}')
            else:
                pas_contract = act_contract
            if self.get_contract(f'{pas_symbol}.{pas_exchange.value}') is None:
                self.set_contract(symbol=pas_symbol,
                              exchange=pas_exchange,
                              product=pas_contract.product,
                              name=act_symbol,
                              size=pas_contract.size,
                              price_tick=pas_contract.pricetick,
                              margin_rate=pas_contract.margin_rate)
                self.write_log(f'设置被动腿指数合约信息{pas_symbol}.{pas_exchange.value}')
            idx_spd_symbol=f'{act_underly}99-{act_ratio}-{pas_underly}99-{pas_ratio}-{spread_type}'

            if f'{idx_spd_symbol}.SPD' not in self.contract_dict:
                if spread_type == 'CJ':
                    if act_underly == pas_underly:
                        spd_price_tick = act_contract.pricetick
                        spd_size = act_contract.size
                        spd_margin_rate = act_contract.margin_rate
                    else:
                        spd_price_tick = min(act_contract.pricetick, pas_contract.pricetick)
                        spd_size = min(act_contract.size, pas_contract.size)
                        spd_margin_rate = max(act_contract.margin_rate, pas_contract.margin_rate)
                else:
                    spd_price_tick = 0.01
                    spd_size = 100
                    spd_margin_rate = 0.1

                self.set_contract(
                    symbol=idx_spd_symbol,
                    exchange=Exchange.SPD,
                    product=act_contract.product,
                    name=idx_spd_symbol,
                    size=spd_size,
                    price_tick=spd_price_tick,
                    margin_rate=spd_margin_rate
                )
                self.write_log(f'设置套利合约信息{idx_spd_symbol}.SPD')
            spd_contract =self.contract_dict.get(f'{idx_spd_symbol}.SPD')

            # subscribe_symobls.remove(vt_symbol)
            subscribe_symobls.append(f'{act_symbol}.{act_exchange.value}')
            subscribe_symobls.append(f'{pas_symbol}.{pas_exchange.value}')

            # 价差生成器
            combiner = self.tick_combiner_dict.get(vt_symbol, None)
            act_combiners = self.symbol_combiner_dict.get(act_symbol, [])
            pas_combiners = self.symbol_combiner_dict.get(pas_symbol, [])
            if combiner is None:
                combiner = TickCombiner(
                    gateway=self,
                    setting={
                        "symbol": symbol,
                        "leg1_symbol": act_symbol,
                        "leg1_ratio": int(act_ratio),
                        "leg2_symbol": pas_symbol,
                        "leg2_ratio": int(pas_ratio),
                        "price_tick": spd_contract.pricetick,
                        "is_spread": True if spread_type == "CJ" else False,
                        "is_ratio": True if spread_type == "BJ" else False}
                )
                self.tick_combiner_dict[vt_symbol] = combiner
                self.write_log(f'添加{vt_symbol} tick合成器')
            if combiner not in act_combiners:
                act_combiners.append(combiner)
                self.symbol_combiner_dict.update({act_symbol: act_combiners})
                self.write_log(f'添加{act_symbol} => {vt_symbol} 合成器映射关系')
            if combiner not in pas_combiners:
                pas_combiners.append(combiner)
                self.symbol_combiner_dict.update({pas_symbol: pas_combiners})
                self.write_log(f'添加{pas_symbol} => {vt_symbol} 合成器映射关系')

        # 取消自动启动
        if 'auto_start' in strategy_setting:
            strategy_setting.update({'auto_start': False})

        # 策略参数设置
        setting = strategy_setting.get('setting', {})

        # 强制更新回测为True
        setting.update({'backtesting': True})

        # 创建实例
        strategy = strategy_class(self, strategy_name, vt_symbol, setting)

        # 保存到策略实例映射表中
        self.strategies.update({strategy_name: strategy})

        # 更新vt_symbol合约与策略的订阅关系
        for sub_vt_symbol in subscribe_symobls:
            self.subscribe_symbol(strategy_name=strategy_name, vt_symbol=sub_vt_symbol)

        if strategy_setting.get('auto_init', False):
            self.write_log(u'自动初始化策略')
            strategy.on_init()

        if strategy_setting.get('auto_start', False):
            self.write_log(u'自动启动策略')
            strategy.on_start()

        if self.active_fund_kline:
            # 创建策略实例的资金K线
            self.create_fund_kline(name=strategy_name, use_renko=False)

    def run_portfolio_test(self, strategy_settings: dict = {}):
        """
        运行组合回测
        """
        if not self.strategy_start_date:
            self.write_error(u'回测开始日期未设置。')
            return

        if len(strategy_settings) == 0:
            self.write_error('未提供有效配置策略实例')
            return

        self.cur_capital = self.init_capital  # 更新设置期初资金
        if not self.data_end_date:
            self.data_end_date = datetime.today()

        self.write_log(u'开始套利组合回测')

        # 保存回测脚本到数据库
        self.save_setting_to_mongo()

        for strategy_name, strategy_setting in strategy_settings.items():
            # 策略得启动日期
            if 'start_date' in strategy_setting:
                start_date = strategy_setting.get('start_date')
                start_date = datetime.strptime(start_date, '%Y-%m-%d')
                self.strategy_start_date_dict.update({strategy_name, start_date})
            # 策略得结束日期
            if 'end_date' in strategy_setting:
                end_date = strategy_setting.get('end_date')
                end_date = datetime.strptime(end_date, '%Y-%m-%d')
                self.strategy_end_date_dict.update({strategy_name, end_date})

            self.load_strategy(strategy_name, strategy_setting)

        self.write_log(u'策略初始化完成')

        self.write_log(u'开始回放数据')

        self.write_log(u'开始回测:{} ~ {}'.format(self.data_start_date, self.data_end_date))

        if self.mode == 'tick':
            self.run_tick_test()
        else:
            self.run_bar_test()

    def load_csv_file(self, tick_folder, vt_symbol, tick_date):
        """从文件中读取tick，返回list[{dict}]"""
        if self.use_tq:
            return self.load_tq_csv_file(tick_folder, vt_symbol, tick_date)

        symbol, exchange = extract_vt_symbol(vt_symbol)
        underly_symbol = get_underlying_symbol(symbol)
        exchange_folder = VN_EXCHANGE_TICKFOLDER_MAP.get(exchange.value,'LOCAL')

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
        df.columns = ['date', 'time', 'last_price', 'volume', 'last_volume', 'open_interest',
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
                tick_folder, 'tq', 'future',
                tick_date.strftime('%Y%m'),
                '{}_{}.csv'.format(symbol, tick_date.strftime('%Y%m%d'))))

        ticks = []
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
        """
        加载缓存数据
        list[{dict}]
        """
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

            # 加载csv文件 =》 dateframe
            symbol_df = pd.read_csv(bar_file, dtype=data_types)
            if len(symbol_df) == 0:
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
            self.output(f'无加载任何数据,请检查bar文件路径配置')

        self.bar_df = pd.concat(self.bar_df_dict, axis=0).swaplevel(0, 1).sort_index()
        self.bar_df_dict.clear()

    def run_bar_test(self):
        """使用bar进行组合回测"""
        testdays = (self.data_end_date - self.data_start_date).days

        if testdays < 1:
            self.write_log(u'回测时间不足')
            return

        # 加载数据
        for vt_symbol in self.symbol_strategy_map.keys():
            symbol, exchange = extract_vt_symbol(vt_symbol)

            # 不读取SPD的bar文件
            if exchange == Exchange.SPD:
                continue
            self.load_bar_csv_to_df(vt_symbol, self.bar_csv_file.get(symbol))

        # 合并数据
        self.comine_bar_df()

        last_trading_day = None
        bars_dt = None
        bars_same_dt = []

        gc_collect_days = 0

        try:
            for (dt, vt_symbol), bar_data in self.bar_df.iterrows():
                symbol, exchange = extract_vt_symbol(vt_symbol)

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
                bar.open_interest = int(bar_data.get('open_interest', 0))
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
                    tick = TickData(
                        gateway_name='backtesting',
                        symbol=symbol,
                        exchange=exchange,
                        datetime=dt,
                        date=dt.strftime('%Y-%m-%d'),
                        time=dt.strftime('%H:%M:%S.%f'),
                        trading_day=tick_data['trading_day'],
                        last_price=float(tick_data['last_price']),
                        volume=int(tick_data['volume']),
                        ask_price_1=float(tick_data['ask_price_1']),
                        ask_volume_1=int(tick_data['ask_volume_1']),
                        bid_price_1=float(tick_data['bid_price_1']),
                        bid_volume_1=int(tick_data['bid_volume_1'])
                    )

                    if np.isnan(tick.ask_price_1) or np.isnan(tick.bid_price_1):
                        continue

                    self.new_tick(tick)

                    # 推送至所有tick combiner
                    [c.on_tick(tick) for c in self.symbol_combiner_dict.get(tick.symbol, [])]

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

    def new_bar(self, bar):
        """
        重载new_bar方法
        bar => tick => 合成器 => new_tick
        :param bar:
        :return:
        """
        tick = self.bar_to_tick(bar)
        self.new_tick(tick)
        # 推送至所有tick combiner
        [c.on_tick(tick) for c in self.symbol_combiner_dict.get(tick.symbol, [])]

    def bar_to_tick(self, bar):
        """ 通过bar分时bar转换为tick数据 """

        # tick =》 增加一分钟
        tick = TickData(
            gateway_name='backtesting',
            symbol=bar.symbol,
            exchange=bar.exchange,
            datetime=bar.datetime + timedelta(minutes=1)
        )
        tick.date = tick.datetime.strftime('%Y-%m-%d')
        tick.time = tick.datetime.strftime('%H:%M:%S.000')
        tick.trading_day = bar.trading_day if bar.trading_day else get_trading_date(tick.datetime)
        tick.volume = bar.volume
        tick.open_interest = bar.open_interest
        tick.last_price = bar.close_price
        tick.last_volume = bar.volume
        tick.limit_up = 0
        tick.limit_down = 0
        tick.open_price = 0
        tick.high_price = 0
        tick.low_price = 0
        tick.pre_close = 0
        tick.bid_price_1 = bar.close_price
        tick.ask_price_1 = bar.close_price
        tick.bid_volume_1 = bar.volume
        tick.ask_volume_1 = bar.volume
        return tick


def single_test(test_setting: dict, strategy_setting: dict):
    """
    单一回测
    : test_setting, 组合回测所需的配置，包括合约信息，数据tick信息，回测时间，资金等。
    ：strategy_setting, dict, 一个或多个策略配置
    """
    # 创建组合回测引擎
    engine = SpreadTestingEngine()

    engine.prepare_env(test_setting)
    try:
        engine.run_portfolio_test(strategy_setting)
        # 回测结果，保存
        engine.show_backtesting_result()

    except Exception as ex:
        print('组合回测异常{}'.format(str(ex)))
        engine.save_fail_to_mongo(f'回测异常{str(ex)}')
        traceback.print_exc()
        return False

    print('测试结束')
    return True
