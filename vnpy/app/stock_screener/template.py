""""""
import os
import bz2
import pickle
import traceback
import zlib

from abc import ABC
from copy import copy, deepcopy
from typing import Any, Callable
from logging import INFO, ERROR
from datetime import datetime, timedelta
from vnpy.trader.constant import Interval, Direction, Offset, Status, OrderType, Color, Exchange
from vnpy.trader.object import BarData, TickData, OrderData, TradeData
from vnpy.trader.utility import virtual, append_data, extract_vt_symbol, get_underlying_symbol
from vnpy.component.cta_grid_trade import CtaGrid, CtaGridTrade, LOCK_GRID


class ScreenerTemplate(ABC):
    """选股策略模板"""
    author = ""
    parameters = []  # 选股参数
    variables = []  # 选股运行变量
    results = []  # 选股结果

    def __init__(
            self,
            engine: Any,
            strategy_name: str,
            setting: dict,
    ):
        self.engine = engine
        self.strategy_name = strategy_name
        self.inited = False  # 是否初始化完毕
        self.running = False  # 是否开始执行选股
        self.completed = False  # 是否已经执行完毕

        self.klines = {}  # 所有K线

        self.update_setting(setting)

    def update_setting(self, setting: dict):
        """
        Update strategy parameter wtih value in setting dict.
        """
        for name in self.parameters:
            if name in setting:
                setattr(self, name, setting[name])

    def write_log(self, msg: str, level: int = INFO):
        """
        Write a log message.
        """
        self.engine.write_log(msg=msg, strategy_name=self.strategy_name, level=level)

    def write_error(self, msg: str):
        """write error log message"""
        self.write_log(msg=msg, level=ERROR)

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

    def check_adjust(self, vt_symbol):
        """
        检查股票的最新除权时间，是否在一周内
        :param vt_symbol:
        :return: True: 一周内没有发生除权； False：一周内发生过除权
        """
        last_adjust_factor = self.engine.get_adjust_factor(vt_symbol)
        if last_adjust_factor is None:
            return True
        last_adjust_date = last_adjust_factor.get('dividOperateDate', None)
        # 最后在除权出息日，发生在一周内
        if last_adjust_date and (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d') <= last_adjust_date:
            self.write_log(
                '{}[{}]发生除权除息，日期:{}'.format(vt_symbol, last_adjust_factor.get('name'), last_adjust_date))
            return False

        return True

    def save_klines_to_cache(self, symbol, kline_names: list = []):
        """
        保存K线数据到缓存
        :param kline_names: 一般为self.klines的keys
        :return:
        """
        if len(kline_names) == 0:
            kline_names = [s for s in list(self.klines.keys()) if s.startswith(symbol)]

        # 获取保存路径
        save_path = os.path.abspath(os.path.join(self.engine.get_data_path(), 'klines'))
        if not os.path.exists(save_path):
            os.makedirs(save_path)

        # 保存缓存的文件名（考虑到超多得股票，根据每个合约进行拆分）
        file_name = os.path.abspath(os.path.join(save_path, f'{self.strategy_name}_{symbol}_klines.pkb2'))
        with bz2.BZ2File(file_name, 'wb') as f:
            klines = {}
            for kline_name in kline_names:
                kline = self.klines.get(kline_name, None)
                # if kline:
                #    kline.strategy = None
                #    kline.cb_on_bar = None
                klines.update({kline_name: kline})
            pickle.dump(klines, f)

    def load_klines_from_cache(self, symbol, kline_names: list = []):
        """
        从缓存加载K线数据
        :param kline_names:
        :return:
        """
        if len(kline_names) == 0:
            kline_names = list(self.klines.keys())

        # 获取保存路径
        save_path = os.path.abspath(os.path.join(self.engine.get_data_path(), 'klines'))
        # 根据策略名称+股票合约进行读取
        file_name = os.path.abspath(os.path.join(save_path, f'{self.strategy_name}_{symbol}_klines.pkb2'))
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
