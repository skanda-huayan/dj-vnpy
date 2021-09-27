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
import traceback
import random
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
)

from .back_testing import BackTestingEngine


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

    def load_bar_csv_to_df(self, vt_symbol, bar_file, data_start_date=None, data_end_date=None):
        """加载回测bar数据到DataFrame"""
        self.output(u'loading {} from {}'.format(vt_symbol, bar_file))
        if vt_symbol in self.bar_df_dict:
            return True

        if not os.path.exists(bar_file):
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
            # 转换时间，str =》 datetime
            symbol_df["datetime"] = pd.to_datetime(symbol_df["datetime"], format="%Y-%m-%d %H:%M:%S")
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
        self.bar_df = pd.concat(self.bar_df_dict, axis=0).swaplevel(0, 1).sort_index()
        self.bar_df_dict.clear()

    def prepare_env(self, test_setting):
        self.output('portfolio prepare_env')
        super().prepare_env(test_setting)

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

        # 保存回测设置/策略设置/任务ID至数据库
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
            self.write_error('目前仅实现bar回测')

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
                    # 如果数据源是bar时间end标识得，需要扣减bar得时间
                    # bar_datetime = dt - timedelta(seconds=self.bar_interval_seconds)
                    # 如果数据源是bar时间start标识，就不需要扣减bar时间
                    bar_datetime = dt

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

                bar.volume = int(bar_data['volume'])
                bar.date = dt.strftime('%Y-%m-%d')
                bar.time = dt.strftime('%H:%M:%S')
                str_td = str(bar_data.get('trading_day', ''))
                if len(str_td) == 8:
                    bar.trading_day = str_td[0:4] + '-' + str_td[4:6] + '-' + str_td[6:8]
                else:
                    bar.trading_day = bar.date

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

    except Exception as ex:
        print('组合回测异常{}'.format(str(ex)))
        traceback.print_exc()
        engine.save_fail_to_mongo(f'回测异常{str(ex)}')
        return False

    print('测试结束')
    return True
