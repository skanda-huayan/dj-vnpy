# encoding: UTF-8

# 选股引擎

import os
import sys
import logging
import importlib
import traceback
import pandas as pd
import numpy as np

from collections import OrderedDict
from typing import List, Any, Callable
from datetime import datetime, timedelta
from functools import lru_cache

from concurrent.futures import ThreadPoolExecutor
# 华富资产
from vnpy.event import EventEngine, Event
from vnpy.trader.constant import Exchange, Interval  # noqa
from vnpy.trader.engine import BaseEngine, MainEngine
from vnpy.trader.event import EVENT_TIMER  # noqa

from vnpy.trader.util_wechat import send_wx_msg
from vnpy.trader.util_logger import setup_logger
from vnpy.trader.constant import Exchange, StockType
from vnpy.trader.object import LogData, BarData
from vnpy.data.tdx.tdx_common import get_tdx_market_code
from vnpy.data.common import stock_to_adj
from vnpy.trader.utility import load_json, save_json, get_csv_last_dt, extract_vt_symbol, get_folder_path, TRADER_DIR, \
    append_data
from vnpy.data.stock.stock_base import get_stock_base
from vnpy.data.stock.adjust_factor import get_all_adjust_factor
from vnpy.app.stock_screener.template import ScreenerTemplate

APP_NAME = 'StockScreenerEngine'
# 选股器日志
EVENT_SCR = 'eScrLog'


class StockScreenerEngine(BaseEngine):
    """
    选股引擎
    """

    # 策略配置文件
    setting_filename = "screener_setting.json"

    def __init__(self, main_engine: MainEngine, event_engine: EventEngine):
        """"""
        super().__init__(main_engine, event_engine, APP_NAME)

        self.main_engine = main_engine
        self.event_engine = event_engine

        self.strategies = {}  # 所有运行选股策略实例
        self.classes = {}  # 选股策略得类
        self.class_module_map = {}  # 策略模块与策略映射

        # 是否激活 write_log写入event bus(比较耗资源）
        self.event_log = False

        self.strategy_loggers = {}  # strategy_name: logger

        self.thread_executor = ThreadPoolExecutor(max_workers=1)
        self.thread_tasks = []

        self.create_logger(logger_name=APP_NAME)

        # 获取全量股票信息
        self.write_log(f'获取全量股票信息')
        self.symbol_dict = get_stock_base()
        self.write_log(f'共{len(self.symbol_dict)}个股票')
        # 除权因子
        self.write_log(f'获取所有除权因子')
        self.adjust_factor_dict = get_all_adjust_factor()
        self.write_log(f'共{len(self.adjust_factor_dict)}条除权信息')

        # 寻找数据文件所在目录
        vnpy_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
        self.write_log(f'项目所在目录:{vnpy_root}')
        self.bar_data_folder = os.path.abspath(os.path.join(vnpy_root, 'bar_data'))
        if os.path.exists(self.bar_data_folder):
            SSE_folder = os.path.abspath(os.path.join(vnpy_root, 'bar_data', 'SSE'))
            if os.path.exists(SSE_folder):
                self.write_log(f'上交所bar数据目录:{SSE_folder}')
            else:
                self.write_error(f'不存在上交所数据目录:{SSE_folder}')

            SZSE_folder = os.path.abspath(os.path.join(vnpy_root, 'bar_data', 'SZSE'))
            if os.path.exists(SZSE_folder):
                self.write_log(f'深交所bar数据目录:{SZSE_folder}')
            else:
                self.write_error(f'不存在深交所数据目录:{SZSE_folder}')
        else:
            self.write_error(f'不存在bar数据目录:{self.bar_data_folder}')
            self.bar_data_folder = None

    def get_all_vt_symbols(self, exchange: Exchange = None, stock_types: List[StockType] = [StockType.STOCK]):
        """
        获取所有股票列表
        :param exchange: 交易所过滤器
        :param stock_types: 合约类型：stock_cn, index_cn,etf_cn,bond_cn,cb_cn
        :return:
        """
        vt_symbols = []
        if len(stock_types) > 0:
            stock_type_values = [s.value for s in stock_types]
        else:
            stock_type_values = []
        for symbol_marketid, info in self.symbol_dict.items():

            if exchange:
                if info.get('exchange', None) != exchange.value:
                    continue

            if len(stock_type_values) > 0:
                if info.get('type', None) not in stock_type_values:
                    continue

            vt_symbols.append('{}.{}'.format(info.get('code'), info.get('exchange')))

        return vt_symbols

    def write_log(self, msg: str, strategy_name: str = '', level: int = logging.INFO):
        """
        Create cta engine log event.
        """
        if self.event_log:
            # 推送至全局CTA_LOG Event
            log = LogData(msg=f"{strategy_name}: {msg}" if strategy_name else msg,
                          gateway_name="Screener",
                          level=level)
            event = Event(type=EVENT_SCR, data=log)
            self.event_engine.put(event)

        # 保存单独的策略日志
        if strategy_name:
            strategy_logger = self.strategy_loggers.get(strategy_name, None)
            if not strategy_logger:
                log_path = get_folder_path('log')
                log_filename = str(log_path.joinpath(str(strategy_name)))
                print(u'create logger:{}'.format(log_filename))
                self.strategy_loggers[strategy_name] = setup_logger(file_name=log_filename,
                                                                    name=str(strategy_name))
                strategy_logger = self.strategy_loggers.get(strategy_name)
            if strategy_logger:
                strategy_logger.log(level, msg)
        else:
            if self.logger:
                self.logger.log(level, msg)

        # 如果日志数据异常，错误和告警，输出至sys.stderr
        if level in [logging.CRITICAL, logging.ERROR, logging.WARNING]:
            print(f"{strategy_name}: {msg}" if strategy_name else msg, file=sys.stderr)

    def write_error(self, msg: str, strategy_name: str = '', level: int = logging.ERROR):
        """写入错误日志"""
        self.write_log(msg=msg, strategy_name=strategy_name, level=level)

    @lru_cache()
    def get_data_path(self):
        data_path = os.path.abspath(os.path.join(TRADER_DIR, 'data'))
        return data_path

    @lru_cache()
    def get_logs_path(self):
        log_path = os.path.abspath(os.path.join(TRADER_DIR, 'log'))
        return log_path

    @lru_cache()
    def get_price_tick(self, vt_symbol: str):
        """查询价格最小跳动"""
        contract = self.main_engine.get_contract(vt_symbol)
        if contract is None:
            self.write_error(f'查询不到{vt_symbol}合约信息，缺省使用0.01作为价格跳动')
            return 0.01

        return contract.pricetick

    @lru_cache()
    def get_name(self, vt_symbol: str):
        """查询合约的name"""
        contract = self.main_engine.get_contract(vt_symbol)
        if contract is None:
            symbol_info = self.symbol_dict.get(vt_symbol, None)
            if symbol_info:
                name = symbol_info.get('name', None)
                if name:
                    return name
            self.write_error(f'查询不到{vt_symbol}合约信息')
            return vt_symbol
        return contract.name

    def get_bars(
            self,
            vt_symbol: str,
            days: int,
            interval: Interval,
            interval_num: int = 1
    ):
        """获取历史记录"""
        symbol, exchange = extract_vt_symbol(vt_symbol)
        end = datetime.now()
        start = end - timedelta(days)
        bars = []

        # 检查股票代码
        if vt_symbol not in self.symbol_dict:
            self.write_error(f'{vt_symbol}不在基础配置股票信息中')
            return bars

        # 检查数据文件目录
        if not self.bar_data_folder:
            self.write_error(f'没有bar数据目录')
            return bars
        # 按照交易所的存放目录
        bar_file_folder = os.path.abspath(os.path.join(self.bar_data_folder, f'{exchange.value}'))

        resample_min = False
        resample_hour = False
        resample_day = False
        file_interval_num = 1
        # 只有1,5,15,30分钟，日线数据
        if interval == Interval.MINUTE:
            # 如果存在相应的分钟文件，直接读取
            bar_file_path = os.path.abspath(os.path.join(bar_file_folder, f'{symbol}_{interval_num}m.csv'))
            if interval_num in [1, 5, 15, 30] and os.path.exists(bar_file_path):
                file_interval_num = interval
            # 需要resample
            else:
                resample_min = True
                if interval_num > 5:
                    file_interval_num = 5

        elif interval == Interval.HOUR:
            file_interval_num = 5
            resample_hour = True
            bar_file_path = os.path.abspath(os.path.join(bar_file_folder, f'{symbol}_{file_interval_num}m.csv'))
        elif interval == Interval.DAILY:
            bar_file_path = os.path.abspath(os.path.join(bar_file_folder, f'{symbol}_{interval_num}d.csv'))
            if not os.path.exists(bar_file_path):
                file_interval_num = 5
                resample_day = True
                bar_file_path = os.path.abspath(os.path.join(bar_file_folder, f'{symbol}_{file_interval_num}m.csv'))
        else:
            self.write_error(f'目前仅支持分钟,小时，日线数据')
            return bars

        bar_interval_seconds = interval_num * 60

        if not os.path.exists(bar_file_path):
            self.write_error(f'没有bar数据文件：{bar_file_path}')
            return bars

        try:
            data_types = {
                "datetime": str,
                "open": float,
                "high": float,
                "low": float,
                "close": float,
                "volume": float,
                "amount": float,
                "symbol": str,
                "trading_day": str,
                "date": str,
                "time": str
            }

            symbol_df = None
            qfq_bar_file_path = bar_file_path.replace('.csv', '_qfq.csv')
            use_qfq_file = False
            last_qfq_dt = get_csv_last_dt(qfq_bar_file_path)
            if last_qfq_dt is not None:
                last_dt = get_csv_last_dt(bar_file_path)

                if last_qfq_dt == last_dt:
                    use_qfq_file = True

            if use_qfq_file:
                self.write_log(f'使用前复权文件:{qfq_bar_file_path}')
                symbol_df = pd.read_csv(qfq_bar_file_path, dtype=data_types)
            else:
                # 加载csv文件 =》 dateframe
                self.write_log(f'使用未复权文件:{bar_file_path}')
                symbol_df = pd.read_csv(bar_file_path, dtype=data_types)

            # 转换时间，str =》 datetime
            symbol_df["datetime"] = pd.to_datetime(symbol_df["datetime"], format="%Y-%m-%d %H:%M:%S")
            # 设置时间为索引
            symbol_df = symbol_df.set_index("datetime")

            # 裁剪数据
            symbol_df = symbol_df.loc[start:end]

            if resample_day:
                self.write_log(f'{vt_symbol} resample:{file_interval_num}m => {interval}day')
                symbol_df = self.resample_bars(df=symbol_df, to_day=True)
            elif resample_hour:
                self.write_log(f'{vt_symbol} resample:{file_interval_num}m => {interval}hour')
                symbol_df = self.resample_bars(df=symbol_df, x_hour=interval_num)
            elif resample_min:
                self.write_log(f'{vt_symbol} resample:{file_interval_num}m => {interval}m')
                symbol_df = self.resample_bars(df=symbol_df, x_min=interval_num)

            if len(symbol_df) == 0:
                return bars

            if not use_qfq_file:
                # 复权转换
                adj_list = self.adjust_factor_dict.get(vt_symbol, [])
                # 按照结束日期，裁剪复权记录
                adj_list = [row for row in adj_list if
                            row['dividOperateDate'].replace('-', '') <= end.strftime('%Y%m%d')]

                if len(adj_list) > 0:
                    self.write_log(f'需要对{vt_symbol}进行前复权处理')
                    for row in adj_list:
                        row.update({'dividOperateDate': row.get('dividOperateDate')[:10] + ' 09:30:00'})
                    # list -> dataframe, 转换复权日期格式
                    adj_data = pd.DataFrame(adj_list)
                    adj_data["dividOperateDate"] = pd.to_datetime(adj_data["dividOperateDate"],
                                                                  format="%Y-%m-%d %H:%M:%S")
                    adj_data = adj_data.set_index("dividOperateDate")
                    # 调用转换方法，对open,high,low,close, volume进行复权, fore, 前复权， 其他，后复权
                    symbol_df = stock_to_adj(symbol_df, adj_data, adj_type='fore')

            for dt, bar_data in symbol_df.iterrows():
                bar_datetime = dt  # - timedelta(seconds=bar_interval_seconds)

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

                bar.volume = int(bar_data['volume']) if not np.isnan(bar_data['volume']) else 0
                bar.date = dt.strftime('%Y-%m-%d')
                bar.time = dt.strftime('%H:%M:%S')
                str_td = str(bar_data.get('trading_day', ''))
                if len(str_td) == 8:
                    bar.trading_day = str_td[0:4] + '-' + str_td[4:6] + '-' + str_td[6:8]
                else:
                    bar.trading_day = bar.date

                bars.append(bar)

        except Exception as ex:
            self.write_error(u'回测时读取{} csv文件{}失败:{}'.format(vt_symbol, bar_file_path, ex))
            self.write_error(traceback.format_exc())
            return bars

        return bars

    def resample_bars(self, df, x_min=None, x_hour=None, to_day=False):
        """
        重建x分钟K线（或日线）
        :param df: 输入分钟数
        :param x_min: 5, 15, 30, 60
        :param x_hour: 1, 2, 3, 4
        :param include_day: 重建日线, True得时候，不会重建分钟数
        :return:
        """
        # 设置df数据中每列的规则
        ohlc_rule = {
            'open': 'first',  # open列：序列中第一个的值
            'high': 'max',  # high列：序列中最大的值
            'low': 'min',  # low列：序列中最小的值
            'close': 'last',  # close列：序列中最后一个的值
            'volume': 'sum',  # volume列：将所有序列里的volume值作和
            'amount': 'sum',  # amount列：将所有序列里的amount值作和
            "symbol": 'first',
            "trading_date": 'first',
            "date": 'first',
            "time": 'first'
        }

        if isinstance(x_min, int) and not to_day:
            # 合成x分钟K线并删除为空的行 参数 closed：left类似向上取值既 09：30的k线数据是包含09：30-09：35之间的数据
            df_target = df.resample(f'{x_min}min', closed='left', label='left').agg(ohlc_rule).dropna(axis=0,
                                                                                                      how='any')
            return df_target
        if isinstance(x_hour, int) and not to_day:
            # 合成x小时K线并删除为空的行 参数 closed：left类似向上取值既 09：30的k线数据是包含09：30-09：35之间的数据
            df_target = df.resample(f'{x_hour}hour', closed='left', label='left').agg(ohlc_rule).dropna(axis=0,
                                                                                                        how='any')
            return df_target

        if to_day:
            # 合成x分钟K线并删除为空的行 参数 closed：left类似向上取值既 09：30的k线数据是包含09：30-09：35之间的数据
            df_target = df.resample(f'D', closed='left', label='left').agg(ohlc_rule).dropna(axis=0, how='any')
            return df_target

        return df

    def get_adjust_factor(self, vt_symbol, check_date=None):
        """
        获取[check_date前]除权因子
        :param vt_symbol:
        :param check_date: 某一指定日期
        :return:
        """
        stock_adjust_factor_list = self.adjust_factor_dict.get(vt_symbol, [])
        if len(stock_adjust_factor_list) == 0:
            return None
        stock_adjust_factor_list.reverse()
        if check_date is None:
            check_date = datetime.now().strftime('%Y-%m-%d')

        for d in stock_adjust_factor_list:
            if d.get("dividOperateDate","") < check_date:
                return d
        return None

    def register_event(self):
        """
        注册事件
        :return:
        """
        pass

    def register_funcs(self):
        """
        register the funcs to main_engine
        :return:
        """
        self.main_engine.get_all_completed_status = self.get_all_completed_status

    def init_engine(self):
        """
        """
        self.register_event()
        self.register_funcs()

        self.load_strategy_class()
        self.load_strategy_setting()

        self.write_log("CTA策略引擎初始化成功")

    def load_strategy_class(self):
        """
        Load strategy class from source code.
        """
        # 加载 vnpy/app/cta_strategy_pro/strategies的所有策略
        path1 = os.path.abspath(os.path.join(os.path.dirname(__file__), "strategies"))
        self.load_strategy_class_from_folder(
            path1, "vnpy.app.stock_screener.strategies")

    def load_strategy_class_from_folder(self, path: str, module_name: str = ""):
        """
        Load strategy class from certain folder.
        """
        for dirpath, dirnames, filenames in os.walk(path):
            for filename in filenames:
                if filename.endswith(".py"):
                    strategy_module_name = ".".join(
                        [module_name, filename.replace(".py", "")])
                elif filename.endswith(".pyd"):
                    strategy_module_name = ".".join(
                        [module_name, filename.split(".")[0]])
                elif filename.endswith(".so"):
                    strategy_module_name = ".".join(
                        [module_name, filename.split(".")[0]])
                else:
                    continue
                self.load_strategy_class_from_module(strategy_module_name)

    def load_strategy_class_from_module(self, module_name: str):
        """
        Load/Reload strategy class from module file.
        """
        try:
            module = importlib.import_module(module_name)

            for name in dir(module):
                value = getattr(module, name)
                if (isinstance(value, type) and issubclass(value, ScreenerTemplate) and value is not ScreenerTemplate):
                    class_name = value.__name__
                    if class_name not in self.classes:
                        self.write_log(f"加载策略类{module_name}.{class_name}")
                    else:
                        self.write_log(f"更新策略类{module_name}.{class_name}")
                    self.classes[class_name] = value
                    self.class_module_map[class_name] = module_name
            return True
        except:  # noqa
            msg = f"策略文件{module_name}加载失败，触发异常：\n{traceback.format_exc()}"
            self.write_log(msg=msg, level=logging.CRITICAL)
            return False

    def load_strategy_setting(self):
        """
        Load setting file.
        """

        # 读取策略得配置
        self.strategy_setting = load_json(self.setting_filename)

        for strategy_name, strategy_config in self.strategy_setting.items():
            self.add_strategy(
                class_name=strategy_config["class_name"],
                strategy_name=strategy_name,
                setting=strategy_config["setting"],
                auto_init=strategy_config.get('auto_init', False),
                auto_start=strategy_config.get('auto_start', False)
            )

    def update_strategy_setting(self, strategy_name: str, setting: dict, auto_init: bool = False,
                                auto_start: bool = False):
        """
        Update setting file.
        """
        strategy = self.strategies[strategy_name]
        # 原配置
        old_config = self.strategy_setting.get('strategy_name', {})
        new_config = {
            "class_name": strategy.__class__.__name__,
            "auto_init": auto_init,
            "auto_start": auto_start,
            "setting": setting
        }

        if old_config:
            self.write_log(f'{strategy_name} 配置变更:\n{old_config} \n=> \n{new_config}')

        self.strategy_setting[strategy_name] = new_config

        sorted_setting = OrderedDict()
        for k in sorted(self.strategy_setting.keys()):
            sorted_setting.update({k: self.strategy_setting.get(k)})

        save_json(self.setting_filename, sorted_setting)

    def remove_strategy_setting(self, strategy_name: str):
        """
        Update setting file.
        """
        if strategy_name not in self.strategy_setting:
            return
        self.write_log(f'移除CTA引擎{strategy_name}的配置')
        self.strategy_setting.pop(strategy_name)
        sorted_setting = OrderedDict()
        for k in sorted(self.strategy_setting.keys()):
            sorted_setting.update({k: self.strategy_setting.get(k)})

        save_json(self.setting_filename, sorted_setting)

    def call_strategy_func(
            self, strategy: ScreenerTemplate, func: Callable, params: Any = None
    ):
        """
        Call function of a strategy and catch any exception raised.
        """
        try:
            if params:
                func(params)
            else:
                func()
        except Exception:
            strategy.trading = False
            strategy.inited = False

            msg = f"触发异常已停止\n{traceback.format_exc()}"
            self.write_log(msg=msg,
                           strategy_name=strategy.strategy_name,
                           level=logging.CRITICAL)
            self.send_wechat(msg)

    def add_strategy(self,
                     class_name: str,
                     strategy_name: str,
                     setting: dict,
                     auto_init: bool = False,
                     auto_start: bool = False):
        """
        添加选股策略
        :return:
        """

        if strategy_name in self.strategies:
            msg = f"创建选股策略失败，存在重名{strategy_name}"
            self.write_log(msg=msg,
                           level=logging.CRITICAL)
            return False, msg

        strategy_class = self.classes.get(class_name, None)
        if not strategy_class:
            msg = f"创建选股策略失败，找不到策略类{class_name}"
            self.write_log(msg=msg,
                           level=logging.CRITICAL)
            return False, msg

        self.write_log(f'开始添加选股策略类{class_name}，实例名:{strategy_name}')
        strategy = strategy_class(self, strategy_name, setting)
        self.strategies[strategy_name] = strategy

        # Update to setting file.
        self.update_strategy_setting(strategy_name, setting, auto_init, auto_start)

        # 判断设置中是否由自动初始化和自动启动项目
        if auto_init:
            self.init_strategy(strategy_name, auto_start=auto_start)

        return True, f'成功添加{strategy_name}'

    def init_strategy(self, strategy_name: str, auto_start: bool = False):
        """
        初始化选股策略
        :return:
        """
        task = self.thread_executor.submit(self._init_strategy, strategy_name, auto_start)
        self.thread_tasks.append(task)

    def _init_strategy(self, strategy_name: str, auto_start: bool = False):
        """
        Init strategies in queue.
        """
        strategy = self.strategies[strategy_name]

        if strategy.inited:
            self.write_error(f"{strategy_name}已经完成初始化，禁止重复操作")
            return

        self.write_log(f"{strategy_name}开始执行初始化")

        # Call on_init function of strategy
        self.call_strategy_func(strategy, strategy.on_init)

        # Put event to update init completed status.
        strategy.inited = True
        self.write_log(f"{strategy_name}初始化完成")

        # 初始化后，自动启动策略交易
        if auto_start:
            self.start_strategy(strategy_name)

    def start_strategy(self, strategy_name: str):
        """
        启动选股策略
        :return:
        """
        strategy = self.strategies[strategy_name]
        if not strategy.inited:
            msg = f"策略{strategy.strategy_name}启动失败，请先初始化"
            self.write_error(msg)
            return False, msg

        if strategy.running:
            msg = f"{strategy_name}已经启动，请勿重复操作"
            self.write_log(msg)
            return False, msg

        self.call_strategy_func(strategy, strategy.on_start)
        strategy.running = True

        return True, f'成功启动选股策略{strategy_name}'

    def stop_strategy(self, strategy_name):
        """
        停止选股策略执行
        :return:
        """
        pass

    def get_all_completed_status(self):
        """检查所有选股是否运行完毕"""
        for strategy in self.strategies.values():
            if not strategy.completed:
                return False

        return True

    def send_wechat(self, msg: str, strategy: ScreenerTemplate = None):
        """
        send wechat message to default receiver
        :param msg:
        :param strategy:
        :return:
        """
        if strategy:
            subject = f"{strategy.strategy_name}"
        else:
            subject = "选股引擎"

        send_wx_msg(content=f'{subject}:{msg}')
