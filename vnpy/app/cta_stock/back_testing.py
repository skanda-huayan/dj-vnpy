# encoding: UTF-8

'''
本文件中包含的是CTA模块的组合回测引擎，回测引擎的API和CTA引擎一致，
可以使用和实盘相同的代码进行回测。
华富资产 李来佳
'''
from __future__ import division

import sys
import os
import importlib
import csv
import copy
import pandas as pd
import traceback
import numpy as np
import logging
import socket
import zlib
import pickle
from bson import binary

from collections import OrderedDict, defaultdict
from datetime import datetime, timedelta
from functools import lru_cache
from pathlib import Path

from .base import (
    EngineType,
    STOPORDER_PREFIX,
    StopOrder,
    StopOrderStatus
)
from .template import CtaTemplate

from vnpy.component.cta_fund_kline import FundKline

from vnpy.trader.object import (
    BarData,
    TickData,
    OrderData,
    TradeData,
    ContractData,
    PositionData
)
from vnpy.trader.constant import (
    Exchange,
    Direction,
    Offset,
    Status,
    OrderType,
    Product
)
from vnpy.trader.converter import PositionHolding

from vnpy.trader.utility import (
    get_underlying_symbol,
    round_to,
    extract_vt_symbol,
    format_number,
    import_module_by_str,
    get_stock_exchange
)
from vnpy.data.stock.adjust_factor import get_all_adjust_factor
from vnpy.data.common import stock_to_adj
from vnpy.trader.util_logger import setup_logger
from vnpy.data.mongo.mongo_data import MongoData
from uuid import uuid1


class BackTestingEngine(object):
    """
    CTA回测引擎
    函数接口和策略引擎保持一样，
    从而实现同一套代码从回测到实盘。
    针对1分钟bar的回测
    或者tick级别得回测
    提供对组合回测/批量回测得服务

    """

    def __init__(self, event_engine=None):
        """Constructor"""

        # 绑定事件引擎
        self.event_engine = event_engine

        self.mode = 'bar'  # 'bar': 根据1分钟k线进行回测， 'tick'，根据分笔tick进行回测

        # 引擎类型为回测
        self.engine_type = EngineType.BACKTESTING
        self.contract_type = 'stock'  # future, stock, digital

        # 回测策略相关
        self.classes = {}  # 策略类，class_name: stategy_class
        self.class_module_map = {}  # 策略类名与模块名映射 class_name: mudule_name
        self.strategies = {}  # 回测策略实例, key = strategy_name, value= strategy
        self.symbol_strategy_map = defaultdict(list)  # vt_symbol: strategy list

        self.test_name = 'stock_test_{}'.format(datetime.now().strftime('%M%S'))  # 回测策略组合的实例名字
        self.daily_report_name = ''  # 策略的日净值报告文件名称

        # 回测日期相关
        self.test_start_date = ''  # 组合回测启动得日期
        self.init_days = 0  # 初始化天数
        self.test_end_date = ''  # 组合回测结束日期

        self.data_start_date = None  # 回测数据开始日期，datetime对象 （用于截取数据）
        self.data_end_date = None  # 回测数据结束日期，datetime对象 （用于截取数据）
        self.strategy_start_date = None  # 策略启动日期（即前面的数据用于初始化），datetime对象

        # 回测数据相关
        self.adjust_factors = {}  # 复权因子
        self.slippage = {}  # 回测时假设的滑点
        self.commission_rate = {}  # 回测时假设的佣金比例（适用于百分比佣金）
        self.fix_commission = {}  # 每手固定手续费
        self.size = {}  # 合约大小，默认为1
        self.price_tick = {}  # 价格最小变动
        self.volume_tick = {}  # 合约委托单最小单位
        self.margin_rate = {}  # 回测合约的保证金比率
        self.price_dict = {}  # 登记vt_symbol对应的最新价
        self.contract_dict = {}  # 登记vt_symbol得对应合约信息
        self.symbol_exchange_dict = {}  # 登记symbol: exchange的对应关系

        self.stop_order_count = 0  # 本地停止单编号
        self.stop_orders = {}  # 本地停止单
        self.active_stop_orders = {}  # 活动本地停止单

        self.limit_order_count = 0  # 限价单编号
        self.limit_orders = OrderedDict()  # 限价单字典
        self.active_limit_orders = OrderedDict()  # 活动限价单字典，用于进行撮合用

        self.order_strategy_dict = {}  # orderid 与 strategy的映射

        self.trade_count = 0  # 成交编号
        self.trade_dict = OrderedDict()  # 用于统计成交收益时，还没处理得交易
        self.trades = OrderedDict()  # 记录所有得成交记录
        self.trade_pnl_list = []  # 交易记录列表

        self.long_position_list = []  # 多单持仓

        self.positions = {}  # 账号持仓，对象为PositionData

        # 当前最新数据，用于模拟成交用
        self.gateway_name = u'BackTest'

        self.last_bar = {}  # 最新的bar
        self.last_tick = {}  # 最新tick
        self.last_dt = None  # 最新时间

        # csvFile相关
        self.bar_interval_seconds = 60  # csv文件，属于K线类型，K线的周期（秒数）,缺省是1分钟

        # 费用风控情况
        self.percent = 0.0
        self.percent_limit = 30  # 投资仓位比例上限

        # 回测计算相关
        self.use_margin = True  # 使用保证金模式（期货使用，计算保证金时，按照开仓价计算。股票是按照当前价计算）

        self.init_capital = 1000000  # 期初资金
        self.cur_capital = self.init_capital  # 当前资金净值
        self.net_capital = self.init_capital  # 实时资金净值（每日根据capital和持仓浮盈计算）
        self.max_capital = self.init_capital  # 资金最高净值
        self.max_net_capital = self.init_capital
        self.available = self.init_capital

        self.max_pnl = 0  # 最高盈利
        self.min_pnl = 0  # 最大亏损
        self.max_occupy_rate = 0  # 最大保证金占比
        self.winning_result = 0  # 盈利次数
        self.losing_result = 0  # 亏损次数

        self.total_trade_count = 0  # 总成交数量
        self.total_winning = 0  # 总盈利
        self.total_losing = 0  # 总亏损
        self.total_turnover = 0  # 总成交金额（合约面值）
        self.total_commission = 0  # 总手续费
        self.total_slippage = 0  # 总滑点

        self.time_list = []  # 时间序列
        self.pnl_list = []  # 每笔盈亏序列
        self.capital_list = []  # 盈亏汇总的时间序列
        self.drawdown_list = []  # 回撤的时间序列
        self.drawdown_rate_list = []  # 最大回撤比例的时间序列(成交结算）

        self.max_net_capital_time = ''
        self.max_drawdown_rate_time = ''
        self.daily_max_drawdown_rate = 0  # 按照日结算价计算

        self.pnl_strategy_dict = {}  # 策略实例的平仓盈亏

        self.is_plot_daily = False
        self.daily_list = []  # 按日统计得序列
        self.daily_first_benchmark = None

        self.logger = None
        self.strategy_loggers = {}
        self.debug = False

        self.is_7x24 = False
        self.logs_path = None
        self.data_path = None

        self.fund_kline_dict = {}
        self.active_fund_kline = False

        # 回测任务/回测结果，保存在数据库中
        self.mongo_api = None
        self.task_id = None
        self.test_setting = None  # 回测设置
        self.strategy_setting = None  # 所有回测策略得设置

    def create_fund_kline(self, name, use_renko=False, extra_setting = {}):
        """
        创建资金曲线
        :param name: 账号名，或者策略名
        :param use_renko:是否使用砖图
        :param extra_setting: 扩展得k线设置，例如macd等
        :return:
        """
        setting = {}
        setting.update({'name': name})
        setting['para_ma1_len'] = 5
        setting['para_ma2_len'] = 10
        setting['para_ma3_len'] = 20
        setting['para_active_yb'] = True
        setting['price_tick'] = 0.01
        setting['underlying_symbol'] = 'fund'
        setting['is_7x24'] = self.is_7x24
        for k,v in extra_setting.items():
            if k not in setting:
                setting.update({k:v})
        if use_renko:
            # 使用砖图，高度是资金的千分之一
            setting['height'] = self.init_capital * 0.001
            setting['use_renko'] = True

        fund_kline = FundKline(cta_engine=self, setting=setting)
        self.fund_kline_dict.update({name: fund_kline})
        return fund_kline

    def get_fund_kline(self, name: str = None):
        # 指定资金账号/策略名
        if name:
            kline = self.fund_kline_dict.get(name, None)
            return kline

        # 没有指定账号，并且存在一个或多个资金K线
        if len(self.fund_kline_dict) > 0:
            # 优先找vt_setting中，配置了strategy_groud的资金K线
            kline = self.fund_kline_dict.get(self.test_name, None)

            # 找不到，返回第一个
            if kline is None:
                kline = self.fund_kline_dict.values()[0]
            return kline
        else:
            return None

    def get_account(self, vt_accountid: str = ""):
        """返回账号的实时权益，可用资金，仓位比例,投资仓位比例上限"""
        if self.net_capital == 0.0:
            self.percent = 0.0

        return self.net_capital, self.available, self.percent, self.percent_limit

    def set_test_start_date(self, start_date: str = '20100416', init_days: int = 10):
        """设置回测的启动日期"""
        self.test_start_date = start_date
        self.init_days = init_days

        self.data_start_date = datetime.strptime(start_date, '%Y%m%d')

        # 初始化天数
        init_time_delta = timedelta(init_days)

        self.strategy_start_date = self.data_start_date + init_time_delta
        self.write_log(u'设置：回测数据开始日期:{},初始化数据为{}天，策略自动启动日期:{}'
                       .format(self.data_start_date, self.init_days, self.strategy_start_date))

    def set_test_end_date(self, end_date: str = ''):
        """设置回测的结束日期"""
        self.test_end_date = end_date
        if end_date:
            self.data_end_date = datetime.strptime(end_date, '%Y%m%d')
            # 若不修改时间则会导致不包含dataEndDate当天数据
            self.data_end_date.replace(hour=23, minute=59)
        else:
            self.data_end_date = datetime.now()
        self.write_log(u'设置：回测数据结束日期:{}'.format(self.data_end_date))

    def set_init_capital(self, capital: float):
        """设置期初净值"""
        self.cur_capital = capital  # 资金
        self.net_capital = capital  # 实时资金净值（每日根据capital和持仓浮盈计算）
        self.max_capital = capital  # 资金最高净值
        self.max_net_capital = capital
        self.available = capital
        self.init_capital = capital

    def set_margin_rate(self, vt_symbol: str, margin_rate: float):
        """设置某个合约得保证金比率"""
        self.margin_rate.update({vt_symbol: margin_rate})

    @lru_cache()
    def get_margin_rate(self, vt_symbol: str):
        return self.margin_rate.get(vt_symbol, 0.05)

    def set_slippage(self, vt_symbol: str, slippage: float):
        """设置滑点点数"""
        self.slippage.update({vt_symbol: slippage})

    @lru_cache()
    def get_slippage(self, vt_symbol: str):
        """获取滑点"""
        return self.slippage.get(vt_symbol, 0)

    def set_size(self, vt_symbol: str, size: int):
        """设置合约大小"""
        self.size.update({vt_symbol: size})

    @lru_cache()
    def get_size(self, vt_symbol: str):
        """查询合约的size"""
        return self.size.get(vt_symbol, 10)

    @lru_cache()
    def get_name(self, vt_symbol: str):
        """查询中文名称"""
        contract = self.get_contract(vt_symbol)
        if contract:
            return contract.name
        else:
            return vt_symbol

    def set_price(self, vt_symbol: str, price: float):
        self.price_dict.update({vt_symbol: price})

    def get_price(self, vt_symbol: str):
        return self.price_dict.get(vt_symbol, None)

    def set_commission_rate(self, vt_symbol: str, rate: float):
        """设置佣金比例"""
        self.commission_rate.update({vt_symbol: rate})

        if rate >= 0.1:
            self.fix_commission.update({vt_symbol: rate})

    def get_commission_rate(self, vt_symbol: str):
        """ 获取保证金比例，缺省万分之一"""
        return self.commission_rate.get(vt_symbol, float(0.00001))

    def get_fix_commission(self, vt_symbol: str):
        return self.fix_commission.get(vt_symbol, 0)

    def set_price_tick(self, vt_symbol: str, price_tick: float):
        """设置价格最小变动"""
        self.price_tick.update({vt_symbol: price_tick})

    def get_price_tick(self, vt_symbol: str):
        return self.price_tick.get(vt_symbol, 0.01)

    def set_volume_tick(self, vt_symbol: str, volume_tick: float):
        """设置委托单最小单位"""
        self.volume_tick.update({vt_symbol: volume_tick})

    def get_volume_tick(self, vt_symbol: str):
        return self.volume_tick.get(vt_symbol, 1)

    def set_contract(self, symbol: str, exchange: Exchange, product: Product, name: str, size: int,
                     price_tick: float, volume_tick: float = 1, margin_rate: float = 0.1):
        """设置合约信息"""
        vt_symbol = '.'.join([symbol, exchange.value])
        if vt_symbol not in self.contract_dict:
            c = ContractData(
                gateway_name=self.gateway_name,
                symbol=symbol,
                exchange=exchange,
                name=name,
                product=product,
                size=size,
                pricetick=price_tick,
                min_volume=volume_tick,
                margin_rate=margin_rate
            )
            self.contract_dict.update({vt_symbol: c})
            self.set_size(vt_symbol, size)
            self.set_margin_rate(vt_symbol, margin_rate)
            self.set_price_tick(vt_symbol, price_tick)
            self.set_volume_tick(vt_symbol, volume_tick)
            self.symbol_exchange_dict.update({symbol: exchange})

    @lru_cache()
    def get_contract(self, vt_symbol):
        """获取合约配置信息"""
        return self.contract_dict.get(vt_symbol)

    @lru_cache()
    def get_exchange(self, symbol: str):
        return self.symbol_exchange_dict.get(symbol, Exchange.LOCAL)

    def get_position(self, vt_symbol: str, direction: Direction = Direction.NET, gateway_name: str = ''):
        """ 查询合约在账号的持仓"""
        if not gateway_name:
            gateway_name = self.gateway_name
        k = f'{gateway_name}.{vt_symbol}.{direction.value}'
        pos = self.positions.get(k, None)
        if not pos:
            contract = self.get_contract(vt_symbol)
            if not contract:
                self.write_log(f'{vt_symbol}合约信息不存在，构造一个')
                symbol, exchange = extract_vt_symbol(vt_symbol)
                if self.contract_type == 'future':
                    product = Product.FUTURES
                elif self.contract_type == 'stock':
                    product = Product.EQUITY
                else:
                    product = Product.SPOT
                contract = ContractData(gateway_name=gateway_name,
                                        name=vt_symbol,
                                        product=product,
                                        symbol=symbol,
                                        exchange=exchange,
                                        size=self.get_size(vt_symbol),
                                        pricetick=self.get_price_tick(vt_symbol),
                                        margin_rate=self.get_margin_rate(vt_symbol))
            pos = PositionData(
                gateway_name=gateway_name,
                symbol=contract.symbol,
                exchange=contract.exchange,
                direction=direction
            )
            self.positions[k] = pos
        return pos

    def set_name(self, test_name):
        """
        设置组合的运行实例名称
        :param test_name:
        :return:
        """
        self.test_name = test_name

    def set_daily_report_name(self, report_file):
        """
        设置策略的日净值记录csv保存文件名（含路径）
        :param report_file: 保存文件名（含路径）
        :return:
        """
        self.daily_report_name = report_file

    def prepare_env(self, test_setting):
        """
        根据配置参数，准备环境
        包括：
        回测名称 ，是否debug，数据目录/日志目录，
        资金/保证金类型/仓位控制
        回测开始/结束日期
        :param test_setting:
        :return:
        """
        self.test_setting = copy.copy(test_setting)

        self.output(f'回测引擎准备环境')
        if 'name' in test_setting:
            self.set_name(test_setting.get('name'))

        self.mode = test_setting.get('mode', 'bar')
        self.output(f'采用{self.mode}方式回测')

        self.contract_type = test_setting.get('contract_type', 'stock')
        self.output(f'测试合约主要为{self.contract_type}')

        self.debug = test_setting.get('debug', False)

        # 更新数据目录
        if 'data_path' in test_setting:
            self.data_path = test_setting.get('data_path')
        else:
            self.data_path = os.path.abspath(os.path.join(os.getcwd(), 'data'))

        print(f'数据输出目录:{self.data_path}')

        # 更新日志目录
        if 'logs_path' in test_setting:
            self.logs_path = os.path.abspath(os.path.join(test_setting.get('logs_path'), self.test_name))
        else:
            self.logs_path = os.path.abspath(os.path.join(os.getcwd(), 'log', self.test_name))
        print(f'日志输出目录:{self.logs_path}')

        # 创建日志
        self.create_logger(debug=self.debug)

        # 设置资金
        if 'init_capital' in test_setting:
            self.write_log(u'设置期初资金:{}'.format(test_setting.get('init_capital')))
            self.set_init_capital(test_setting.get('init_capital'))

        # 缺省使用保证金方式。（期货使用保证金/股票不使用保证金）
        self.use_margin = test_setting.get('use_margin', False)

        # 设置最大资金使用比例
        if 'percent_limit' in test_setting:
            self.write_log(u'设置最大资金使用比例:{}%'.format(test_setting.get('percent_limit')))
            self.percent_limit = test_setting.get('percent_limit')

        if 'start_date' in test_setting:
            if 'strategy_start_date' not in test_setting:
                init_days = test_setting.get('init_days', 10)
                self.write_log(u'设置回测开始日期:{}，数据加载日数:{}'.format(test_setting.get('start_date'), init_days))
                self.set_test_start_date(test_setting.get('start_date'), init_days)
            else:
                start_date = test_setting.get('start_date')
                strategy_start_date = test_setting.get('strategy_start_date')
                self.write_log(u'使用指定的数据开始日期：{}和策略启动日期:{}'.format(start_date, strategy_start_date))
                self.test_start_date = start_date
                self.data_start_date = datetime.strptime(start_date.replace('-', ''), '%Y%m%d')
                self.strategy_start_date = datetime.strptime(strategy_start_date.replace('-', ''), '%Y%m%d')

        if 'end_date' in test_setting:
            self.write_log(u'设置回测结束日期:{}'.format(test_setting.get('end_date')))
            self.set_test_end_date(test_setting.get('end_date'))

        # 准备数据
        if 'symbol_datas' in test_setting:
            self.write_log(u'准备数据')
            self.prepare_data(test_setting.get('symbol_datas'))

        if self.mode == 'tick':
            self.tick_path = test_setting.get('tick_path', None)

        # 设置bar文件的时间间隔秒数
        if 'bar_interval_seconds' in test_setting:
            self.write_log(u'设置bar文件的时间间隔秒数：{}'.format(test_setting.get('bar_interval_seconds')))
            self.bar_interval_seconds = test_setting.get('bar_interval_seconds')

        # 资金曲线
        self.active_fund_kline = test_setting.get('active_fund_kline', False)
        if self.active_fund_kline:
            # 创建资金K线
            self.create_fund_kline(self.test_name, use_renko=test_setting.get('use_renko', False))

        self.is_plot_daily = test_setting.get('is_plot_daily', False)

        # 加载所有本地策略class
        self.load_strategy_class()

    def prepare_data(self, data_dict):
        """
        准备组合数据
        :param data_dict:
        :return:
        """
        self.output('prepare_data')

        self.write_log(f'获取所有股票的复权因子')
        self.adjust_factors = get_all_adjust_factor()

        if len(data_dict) == 0:
            self.write_log(u'请指定回测数据和文件')
            return

        for vt_symbol, symbol_data in data_dict.items():
            self.write_log(u'配置{}数据:{}'.format(vt_symbol, symbol_data))
            self.set_price_tick(vt_symbol, symbol_data.get('price_tick', 0.01))
            self.set_volume_tick(vt_symbol, symbol_data.get('min_volume', 10))
            self.set_slippage(vt_symbol, symbol_data.get('slippage', 0))
            self.set_size(vt_symbol, symbol_data.get('symbol_size', 1))
            margin_rate = symbol_data.get('margin_rate', 1)
            self.set_margin_rate(vt_symbol, margin_rate)

            self.set_commission_rate(vt_symbol, symbol_data.get('commission_rate', float(0.0001)))
            symbol, exchange = extract_vt_symbol(vt_symbol)
            self.set_contract(
                symbol=symbol,
                name=symbol_data.get('name',vt_symbol),
                exchange=exchange,
                product=Product(symbol_data.get('product', "股票")),
                size=symbol_data.get('symbol_size', 1),
                price_tick=symbol_data.get('price_tick', 0.01),
                volume_tick=symbol_data.get('min_volume', 10),
                margin_rate=margin_rate
            )

    def new_tick(self, tick):
        """新得tick"""
        self.last_tick.update({tick.vt_symbol: tick})
        if self.last_dt is None or (tick.datetime and tick.datetime > self.last_dt):
            self.last_dt = tick.datetime

        self.set_price(tick.vt_symbol, tick.last_price)

        self.cross_stop_order(tick=tick)  # 撮合停止单
        self.cross_limit_order(tick=tick)  # 先撮合限价单

        # 更新账号级别资金曲线(只有持仓时，才更新)
        fund_kline = self.get_fund_kline(self.test_name)
        if fund_kline is not None and len(self.long_position_list) > 0:
            fund_kline.update_account(self.last_dt, self.net_capital)

        for strategy in self.symbol_strategy_map.get(tick.vt_symbol, []):
            # 更新策略的资金K线
            fund_kline = self.fund_kline_dict.get(strategy.strategy_name, None)
            if fund_kline:
                hold_pnl, _ = fund_kline.get_hold_pnl()
                if hold_pnl != 0:
                    fund_kline.update_strategy(dt=self.last_dt, hold_pnl=hold_pnl)

            # 推送tick到策略中
            strategy.on_tick({tick.vt_symbol: tick})  # 推送K线到策略中

            # 到达策略启动日期,启动策略
            if not strategy.trading and self.strategy_start_date < tick.datetime:
                strategy.trading = True
                strategy.on_start()
                self.output(u'{}策略启动交易'.format(strategy.strategy_name))

    def new_bar(self, bar):
        """新的K线"""
        self.last_bar.update({bar.vt_symbol: bar})
        if self.last_dt is None or (bar.datetime and bar.datetime > self.last_dt - timedelta(seconds=self.bar_interval_seconds)):
            self.last_dt = bar.datetime + timedelta(seconds=self.bar_interval_seconds)
        self.set_price(bar.vt_symbol, bar.close_price)
        self.cross_stop_order(bar=bar)  # 撮合停止单
        self.cross_limit_order(bar=bar)  # 先撮合限价单

        # 更新账号的资金曲线(只有持仓时，才更新)
        fund_kline = self.get_fund_kline(self.test_name)
        if fund_kline is not None and len(self.long_position_list) > 0:
            fund_kline.update_account(self.last_dt, self.net_capital)

        for strategy in self.symbol_strategy_map.get(bar.vt_symbol, []):
            # 更新策略的资金K线
            fund_kline = self.fund_kline_dict.get(strategy.strategy_name, None)
            if fund_kline:
                hold_pnl, _ = fund_kline.get_hold_pnl()
                if hold_pnl != 0:
                    fund_kline.update_strategy(dt=self.last_dt, hold_pnl=hold_pnl)

            # 推送K线到策略中
            strategy.on_bar({bar.vt_symbol: bar})  # 推送K线到策略中

            # 到达策略启动日期,启动策略
            if not strategy.trading and self.strategy_start_date < bar.datetime:
                strategy.trading = True
                strategy.on_start()
                self.output(u'{}策略启动交易'.format(strategy.strategy_name))

    def load_strategy_class(self):
        """
        Load strategy class from source code.
        """
        self.write_log('加载所有策略class')
        # 加载 vnpy/app/cta_strategy_pro/strategies的所有策略
        path1 = Path(__file__).parent.joinpath("strategies")
        self.load_strategy_class_from_folder(
            path1, "vnpy.app.cta_stock.strategies")

    def load_strategy_class_from_folder(self, path: Path, module_name: str = ""):
        """
        Load strategy class from certain folder.
        """
        for dirpath, dirnames, filenames in os.walk(str(path)):
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
                if (isinstance(value, type) and issubclass(value, CtaTemplate) and value is not CtaTemplate):
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
            self.write_error(msg)
            self.output(msg)
            return False

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

        vt_symbols = strategy_setting.get('vt_symbols', [])

        # 取消自动启动
        if 'auto_start' in strategy_setting:
            strategy_setting.update({'auto_start': False})

        # 策略参数设置
        setting = strategy_setting.get('setting', {})

        # 强制更新回测为True
        setting.update({'backtesting': True})

        # 创建实例
        strategy = strategy_class(self, strategy_name, vt_symbols, setting)

        # 保存到策略实例映射表中
        self.strategies.update({strategy_name: strategy})

        # 更新vt_symbol合约与策略的订阅关系
        for vt_symbol in vt_symbols:
            if '.' in vt_symbol:
                symbol, exchange = extract_vt_symbol(vt_symbol)
            else:
                symbol = vt_symbol
                exchange_str = get_stock_exchange(code=symbol)
                vt_symbol = '.'.join([symbol, exchange_str])

            self.subscribe_symbol(strategy_name=strategy_name, vt_symbol=vt_symbol)

        if strategy_setting.get('auto_init', False):
            self.write_log(u'自动初始化策略')
            strategy.on_init()

        if strategy_setting.get('auto_start', False):
            self.write_log(u'自动启动策略')
            strategy.on_start()

        if self.active_fund_kline:
            # 创建策略实例的资金K线
            self.create_fund_kline(name=strategy_name, use_renko=False)

    def subscribe_symbol(self, strategy_name: str, vt_symbol: str, gateway_name: str = '', is_bar: bool = False):
        """订阅合约"""
        strategy = self.strategies.get(strategy_name, None)
        if not strategy:
            return False

        # 添加 合约订阅 vt_symbol <=> 策略实例 strategy 映射.
        strategies = self.symbol_strategy_map[vt_symbol]
        strategies.append(strategy)
        return True

    # ---------------------------------------------------------------------
    def save_strategy_data(self):
        """保存策略数据"""
        for strategy in self.strategies.values():
            self.write_log(u'save strategy data')
            strategy.save_data()

    def send_order(self,
                   strategy: CtaTemplate,
                   vt_symbol: str,
                   direction: Direction,
                   offset: Offset,
                   price: float,
                   volume: float,
                   stop: bool,
                   order_type: OrderType = OrderType.LIMIT,
                   gateway_name: str = None):
        """发单"""
        price_tick = self.get_price_tick(vt_symbol)
        price = round_to(price, price_tick)

        if stop:
            return self.send_local_stop_order(
                strategy=strategy,
                vt_symbol=vt_symbol,
                direction=direction,
                offset=offset,
                price=price,
                volume=volume,
                gateway_name=gateway_name
            )
        else:
            return self.send_limit_order(
                strategy=strategy,
                vt_symbol=vt_symbol,
                direction=direction,
                offset=offset,
                price=price,
                volume=volume,
                gateway_name=gateway_name
            )

    def send_limit_order(self,
                         strategy: CtaTemplate,
                         vt_symbol: str,
                         direction: Direction,
                         offset: Offset,
                         price: float,
                         volume: float,
                         order_type: OrderType = OrderType.LIMIT,
                         gateway_name: str = None
                         ):

        """ 发限价单"""
        self.limit_order_count += 1
        order_id = str(self.limit_order_count)
        symbol, exchange = extract_vt_symbol(vt_symbol)
        if gateway_name is None:
            gateway_name = self.gateway_name
        order = OrderData(
            gateway_name=gateway_name,
            symbol=symbol,
            exchange=exchange,
            orderid=order_id,
            direction=direction,
            offset=offset,
            type=order_type,
            price=round_to(value=price, target=self.get_price_tick(vt_symbol)),
            volume=volume,
            status=Status.NOTTRADED,
            time=str(self.last_dt)
        )

        # 保存到限价单字典中
        self.active_limit_orders[order.vt_orderid] = order
        self.limit_orders[order.vt_orderid] = order
        self.order_strategy_dict.update({order.vt_orderid: strategy})

        self.write_log(f'创建限价单:{order.__dict__}')

        return [order.vt_orderid]

    def send_local_stop_order(
            self,
            strategy: CtaTemplate,
            vt_symbol: str,
            direction: Direction,
            offset: Offset,
            price: float,
            volume: float,
            gateway_name: str = None):

        """发出本地停止单"""
        self.stop_order_count += 1

        stop_order = StopOrder(
            vt_symbol=vt_symbol,
            direction=direction,
            offset=offset,
            price=price,
            volume=volume,
            stop_orderid=f"{STOPORDER_PREFIX}.{self.stop_order_count}",
            strategy_name=strategy.strategy_name,
        )
        self.write_log(f'创建本地停止单:{stop_order.__dict__}')
        self.order_strategy_dict.update({stop_order.stop_orderid: strategy})

        self.active_stop_orders[stop_order.stop_orderid] = stop_order
        self.stop_orders[stop_order.stop_orderid] = stop_order

        return [stop_order.stop_orderid]

    def cancel_order(self, strategy: CtaTemplate, vt_orderid: str):
        """撤单"""
        if vt_orderid.startswith(STOPORDER_PREFIX):
            return self.cancel_stop_order(strategy, vt_orderid)
        else:
            return self.cancel_limit_order(strategy, vt_orderid)

    def cancel_limit_order(self, strategy: CtaTemplate, vt_orderid: str):
        """限价单撤单"""
        if vt_orderid in self.active_limit_orders:
            order = self.active_limit_orders[vt_orderid]
            register_strategy = self.order_strategy_dict.get(vt_orderid, None)
            if register_strategy.strategy_name != strategy.strategy_name:
                return False
            order.status = Status.CANCELLED
            order.cancel_time = str(self.last_dt)
            self.active_limit_orders.pop(vt_orderid, None)
            strategy.on_order(order)
            return True
        return False

    def cancel_stop_order(self, strategy: CtaTemplate, vt_orderid: str):
        """本地停止单撤单"""
        if vt_orderid not in self.active_stop_orders:
            return False
        stop_order = self.active_stop_orders.pop(vt_orderid)

        stop_order.status = StopOrderStatus.CANCELLED
        strategy.on_stop_order(stop_order)
        return True

    def cancel_all(self, strategy):
        """撤销某个策略的所有委托单"""
        self.cancel_orders(strategy=strategy)

    def cancel_orders(self, vt_symbol: str = None, offset: Offset = None, strategy: CtaTemplate = None):
        """撤销所有单"""
        # Symbol参数:指定合约的撤单；
        # OFFSET参数:指定Offset的撤单,缺省不填写时，为所有
        # strategy参数： 指定某个策略的单子

        if len(self.active_limit_orders) > 0:
            self.write_log(u'从所有订单中,撤销：开平：{}，合约：{}，策略:{}'
                           .format(offset,
                                   vt_symbol if vt_symbol is not None else u'所有',
                                   strategy.strategy_name if strategy else None))

        for vt_orderid in list(self.active_limit_orders.keys()):
            order = self.active_limit_orders.get(vt_orderid, None)
            order_strategy = self.order_strategy_dict.get(vt_orderid, None)
            if order is None or order_strategy is None:
                continue

            if offset is None:
                offset_cond = True
            else:
                offset_cond = order.offset == offset

            if vt_symbol is None:
                symbol_cond = True
            else:
                symbol_cond = order.vt_symbol == vt_symbol

            if strategy is None:
                strategy_cond = True
            else:
                strategy_cond = strategy.strategy_name == order_strategy.strategy_name

            if offset_cond and symbol_cond and strategy_cond:
                self.write_log(u'撤销订单:{},{} {}@{}'
                               .format(vt_orderid, order.direction, order.price, order.volume))
                order.status = Status.CANCELLED
                order.cancel_time = str(self.last_dt)
                del self.active_limit_orders[vt_orderid]
                if order_strategy:
                    order_strategy.on_order(order)

        # 撤销本地停止单
        for stop_orderid in list(self.active_stop_orders.keys()):
            order = self.active_stop_orders.get(stop_orderid, None)
            order_strategy = self.order_strategy_dict.get(stop_orderid, None)
            if order is None or order_strategy is None:
                continue

            if offset is None:
                offset_cond = True
            else:
                offset_cond = order.offset == offset

            if vt_symbol is None:
                symbol_cond = True
            else:
                symbol_cond = order.vt_symbol == vt_symbol

            if strategy is None:
                strategy_cond = True
            else:
                strategy_cond = strategy.strategy_name == order_strategy.strategy_name

            if offset_cond and symbol_cond and strategy_cond:
                self.write_log(u'撤销本地停止单:{},{} {}@{}'
                               .format(stop_orderid, order.direction, order.price, order.volume))
                order.status = Status.CANCELLED
                order.cancel_time = str(self.last_dt)
                self.active_stop_orders.pop(stop_orderid, None)
                if strategy:
                    strategy.on_stop_order(order)

    def cross_stop_order(self, bar: BarData = None, tick: TickData = None):
        """
        本地停止单撮合
        Cross stop order with last bar/tick data.
        """
        vt_symbol = bar.vt_symbol if bar else tick.vt_symbol

        for stop_orderid in list(self.active_stop_orders.keys()):
            stop_order = self.active_stop_orders[stop_orderid]
            strategy = self.order_strategy_dict.get(stop_orderid, None)
            if stop_order.vt_symbol != vt_symbol or stop_order is None or strategy is None:
                continue

            # 若买入方向停止单价格高于等于该价格，则会触发
            if bar:
                long_cross_price = round_to(value=bar.low_price, target=self.get_price_tick(vt_symbol))
                long_cross_price -= self.get_price_tick(vt_symbol)
                # 若卖出方向停止单价格低于等于该价格，则会触发
                sell_cross_price = round_to(value=bar.high_price, target=self.get_price_tick(vt_symbol))
                sell_cross_price += self.get_price_tick(vt_symbol)
                # 在当前时间点前发出的买入委托可能的最优成交价
                long_best_price = round_to(value=bar.open_price,
                                           target=self.get_price_tick(vt_symbol)) + self.get_price_tick(vt_symbol)

                # 在当前时间点前发出的卖出委托可能的最优成交价
                sell_best_price = round_to(value=bar.open_price,
                                           target=self.get_price_tick(vt_symbol)) - self.get_price_tick(vt_symbol)
            else:
                long_cross_price = tick.last_price
                sell_cross_price = tick.last_price
                long_best_price = tick.last_price
                sell_best_price = tick.last_price

            # Check whether stop order can be triggered.
            long_cross = stop_order.direction == Direction.LONG and stop_order.price <= long_cross_price

            sell_cross = stop_order.direction == Direction.SHORT and stop_order.price >= sell_cross_price

            if not long_cross and not sell_cross:
                continue

            # Create order data.
            self.limit_order_count += 1
            symbol, exchange = extract_vt_symbol(vt_symbol)
            order = OrderData(
                symbol=symbol,
                exchange=exchange,
                orderid=str(self.limit_order_count),
                direction=stop_order.direction,
                offset=stop_order.offset,
                price=stop_order.price,
                volume=stop_order.volume,
                status=Status.ALLTRADED,
                gateway_name=self.gateway_name,
            )
            order.datetime = self.last_dt
            self.write_log(f'停止单被触发:\n{stop_order.__dict__}\n=>委托单{order.__dict__}')
            self.limit_orders[order.vt_orderid] = order

            # Create trade data.
            if long_cross:
                trade_price = max(stop_order.price, long_best_price)
            else:
                trade_price = min(stop_order.price, sell_best_price)

            self.trade_count += 1

            trade = TradeData(
                symbol=order.symbol,
                exchange=order.exchange,
                orderid=order.orderid,
                tradeid=str(self.trade_count),
                direction=order.direction,
                offset=order.offset,
                price=trade_price,
                volume=order.volume,
                time=self.last_dt.strftime("%Y-%m-%d %H:%M:%S"),
                datetime=self.last_dt,
                gateway_name=self.gateway_name,
            )
            trade.strategy_name = strategy.strategy_name
            trade.datetime = self.last_dt
            self.write_log(f'停止单触发成交:{trade.__dict__}')
            self.trade_dict[trade.vt_tradeid] = trade
            self.trades[trade.vt_tradeid] = copy.copy(trade)

            # Update stop order.
            stop_order.vt_orderids.append(order.vt_orderid)
            stop_order.status = StopOrderStatus.TRIGGERED

            self.active_stop_orders.pop(stop_order.stop_orderid)

            # Push update to strategy.
            strategy.on_stop_order(stop_order)
            strategy.on_order(order)
            self.append_trade(trade)

            # 更新持仓缓存数据
            pos = self.get_position(vt_symbol=trade.vt_symbol, direction=Direction.NET)
            pre_volume = pos.volume
            if trade.direction == Direction.LONG:
                pos.volume = round(pos.volume + trade.volume, 7)
            else:
                pos.volume = round(pos.volume - trade.volume, 7)
            self.write_log(f'{trade.vt_symbol} volume:{pre_volume} => {pos.volume}')

            strategy.on_trade(trade)

    def cross_limit_order(self, bar: BarData = None, tick: TickData = None):
        """基于最新数据撮合限价单"""

        vt_symbol = bar.vt_symbol if bar else tick.vt_symbol

        # 遍历限价单字典中的所有限价单
        for vt_orderid in list(self.active_limit_orders.keys()):
            order = self.active_limit_orders.get(vt_orderid, None)
            if order.vt_symbol != vt_symbol:
                continue

            strategy = self.order_strategy_dict.get(order.vt_orderid, None)
            if strategy is None:
                self.write_error(u'找不到vt_orderid:{}对应的策略'.format(order.vt_orderid))
                continue
            if bar:
                price_tick = self.get_price_tick(vt_symbol)

                buy_cross_price = round_to(value=bar.low_price, target=price_tick) + price_tick  # 若买入方向限价单价格高于该价格，则会成交
                sell_cross_price = round_to(value=bar.high_price,
                                            target=price_tick) - price_tick  # 若卖出方向限价单价格低于该价格，则会成交
                buy_best_cross_price = round_to(value=bar.open_price,
                                                target=price_tick) + price_tick  # 在当前时间点前发出的买入委托可能的最优成交价
                sell_best_cross_price = round_to(value=bar.open_price,
                                                 target=price_tick) - price_tick  # 在当前时间点前发出的卖出委托可能的最优成交价
            else:
                buy_cross_price = tick.last_price
                sell_cross_price = tick.last_price
                buy_best_cross_price = tick.last_price
                sell_best_cross_price = tick.last_price

            # 判断是否会成交
            buy_cross = order.direction == Direction.LONG and order.price >= buy_cross_price
            sell_cross = order.direction == Direction.SHORT and order.price <= sell_cross_price

            # 如果发生了成交
            if buy_cross or sell_cross:
                # 推送成交数据
                self.trade_count += 1  # 成交编号自增1

                trade_id = str(self.trade_count)
                symbol, exchange = extract_vt_symbol(vt_symbol)
                trade = TradeData(
                    gateway_name=self.gateway_name,
                    symbol=symbol,
                    exchange=exchange,
                    tradeid=trade_id,
                    orderid=order.orderid,
                    direction=order.direction,
                    offset=order.offset,
                    volume=order.volume,
                    time=self.last_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    datetime=self.last_dt
                )

                # 以买入为例：
                # 1. 假设当根K线的OHLC分别为：100, 125, 90, 110
                # 2. 假设在上一根K线结束(也是当前K线开始)的时刻，策略发出的委托为限价105
                # 3. 则在实际中的成交价会是100而不是105，因为委托发出时市场的最优价格是100
                if buy_cross:
                    trade_price = min(order.price, buy_best_cross_price)

                else:
                    trade_price = max(order.price, sell_best_cross_price)
                trade.price = trade_price

                # 记录该合约来自哪个策略实例
                trade.strategy_name = strategy.strategy_name

                # 更新持仓缓存数据
                pos = self.get_position(vt_symbol=trade.vt_symbol, direction=Direction.NET)
                pre_volume = pos.volume
                if trade.direction == Direction.LONG:
                    pos.volume = round(pos.volume + trade.volume, 7)
                else:
                    pos.volume = round(pos.volume - trade.volume, 7)
                self.write_log(f'{trade.vt_symbol} volume:{pre_volume} => {pos.volume}')

                self.trade_dict[trade.vt_tradeid] = trade
                self.trades[trade.vt_tradeid] = copy.copy(trade)
                self.write_log(u'vt_trade_id:{0}'.format(trade.vt_tradeid))

                self.write_log(u'{} : crossLimitOrder: TradeId:{}'.format(trade.strategy_name,
                                                                          trade.tradeid,
                                                                          ))

                # 写入交易记录
                self.append_trade(trade)

                strategy.on_trade(trade)

                # 更新资金曲线
                fund_kline = self.get_fund_kline(trade.strategy_name)
                if fund_kline:
                    fund_kline.update_trade(trade)

                # 推送委托数据
                order.traded = order.volume
                order.status = Status.ALLTRADED

                strategy.on_order(order)

                # 从字典中删除该限价单
                self.active_limit_orders.pop(vt_orderid, None)

        # 实时计算模式
        self.realtime_calculate()

    def update_position_yd(self):
        """更新持仓信息,把今仓=>昨仓"""

        for k, v in self.positions.items():
            if v.volume != v.yd_volume:
                self.write_log(f'调整{v.vt_symbol}持仓: 昨仓{v.yd_volume} => {v.volume}')
                v.yd_volume = v.volume

    def get_data_path(self):
        """
        获取数据保存目录
        :return:
        """
        if self.data_path is not None:
            data_folder = self.data_path
        else:
            data_folder = os.path.abspath(os.path.join(os.getcwd(), 'data'))
            self.data_path = data_folder
        if not os.path.exists(data_folder):
            os.makedirs(data_folder)
        return data_folder

    def get_logs_path(self):
        """
        获取日志保存目录
        :return:
        """
        if self.logs_path is not None:
            logs_folder = self.logs_path
        else:
            logs_folder = os.path.abspath(os.path.join(os.getcwd(), 'log'))
            self.logs_path = logs_folder

        if not os.path.exists(logs_folder):
            os.makedirs(logs_folder)

        return logs_folder

    def create_logger(self, strategy_name=None, debug=False):
        """
        创建日志
        :param strategy_name 策略实例名称
        :param debug:是否详细记录日志
        :return:
        """
        if strategy_name is None:
            filename = os.path.abspath(os.path.join(self.get_logs_path(), '{}'.format(
                self.test_name if len(self.test_name) > 0 else 'portfolio_test')))
            print(u'create logger:{}'.format(filename))
            self.logger = setup_logger(file_name=filename,
                                       name=self.test_name,
                                       log_level=logging.DEBUG if debug else logging.ERROR,
                                       backtesing=True)
        else:
            filename = os.path.abspath(
                os.path.join(self.get_logs_path(), '{}_{}'.format(self.test_name, str(strategy_name))))
            print(u'create logger:{}'.format(filename))
            self.strategy_loggers[strategy_name] = setup_logger(file_name=filename,
                                                                name=str(strategy_name),
                                                                log_level=logging.DEBUG if debug else logging.ERROR,
                                                                backtesing=True)

    def write_log(self, msg: str, strategy_name: str = None, level: int = logging.DEBUG):
        """记录日志"""
        # log = str(self.datetime) + ' ' + content
        # self.logList.append(log)

        if strategy_name is None:
            # 写入本地log日志
            if self.logger:
                self.logger.log(msg=msg, level=level)
            else:
                self.create_logger(debug=self.debug)
        else:
            if strategy_name in self.strategy_loggers:
                self.strategy_loggers[strategy_name].log(msg=msg, level=level)
            else:
                self.create_logger(strategy_name=strategy_name, debug=self.debug)

    def write_error(self, msg, strategy_name=None):
        """记录异常"""

        if strategy_name is None:
            if self.logger:
                self.logger.error(msg)
            else:
                self.create_logger(debug=self.debug)
        else:
            if strategy_name in self.strategy_loggers:
                self.strategy_loggers[strategy_name].error(msg)
            else:
                self.create_logger(strategy_name=strategy_name, debug=self.debug)
                try:
                    self.strategy_loggers[strategy_name].error(msg)
                except Exception as ex:
                    print('{}'.format(datetime.now()), file=sys.stderr)
                    print('could not create cta logger for {},excption:{},trace:{}'.format(strategy_name, str(ex),
                                                                                           traceback.format_exc()))
                    print(msg, file=sys.stderr)

    def output(self, content):
        """输出内容"""
        print(self.test_name + "\t" + content)

    def realtime_calculate(self):
        """实时计算交易结果
        支持多空仓位并存"""

        if len(self.trade_dict) < 1:
            return

        # 获取所有未处理得成交单
        vt_tradeids = list(self.trade_dict.keys())

        result_list = []  # 保存交易记录
        longid = ''

        # 对交易记录逐一处理
        for vt_tradeid in vt_tradeids:

            trade = self.trade_dict.pop(vt_tradeid, None)
            if trade is None:
                continue

            if trade.volume == 0:
                continue
            # buy trade 开多买入
            if trade.direction == Direction.LONG:
                self.write_log(f'{trade.vt_symbol} buy, price:{trade.price},volume:{trade.volume}')
                # 放入多单仓位队列
                self.long_position_list.append(trade)

            # sell trade
            elif trade.direction == Direction.SHORT:
                g_id = trade.vt_tradeid  # 交易组（多个平仓数为一组）
                g_result = None  # 组合的交易结果

                sell_volume = trade.volume

                while sell_volume > 0:
                    if len(self.long_position_list) == 0:
                        self.write_error(f'异常，没有{trade.vt_symbol}的多仓')
                        raise RuntimeError(u'realtimeCalculate2() Exception,没有开多单')
                        return

                    pop_indexs = [i for i, val in enumerate(self.long_position_list) if
                                  val.vt_symbol == trade.vt_symbol and val.strategy_name == trade.strategy_name]
                    if len(pop_indexs) < 1:
                        self.write_error(f'没有{trade.strategy_name}对应的symbol{trade.vt_symbol}多单数据,')
                        raise RuntimeError(
                            f'realtimeCalculate2() Exception,没有对应的symbol{trade.vt_symbol}多单数据,')
                        return

                    cur_long_pos_list = [s_pos.volume for s_pos in self.long_position_list]

                    self.write_log(u'{}当前多单:{}'.format(trade.vt_symbol, cur_long_pos_list))

                    pop_index = pop_indexs[0]
                    open_trade = self.long_position_list.pop(pop_index)
                    # 开多volume，不大于平仓volume
                    if sell_volume >= open_trade.volume:
                        self.write_log(f'{open_trade.vt_symbol},Sell Volume:{sell_volume} 满足:{open_trade.volume}')
                        sell_volume = sell_volume - open_trade.volume
                        sell_volume = round(sell_volume, 7)
                        self.write_log(f'{open_trade.vt_symbol},sell, price:{trade.price},volume:{open_trade.volume}')

                        result = TradingResult(open_price=open_trade.price,
                                               open_datetime=open_trade.datetime,
                                               exit_price=trade.price,
                                               close_datetime=trade.datetime,
                                               volume=open_trade.volume,
                                               rate=self.get_commission_rate(trade.vt_symbol),
                                               slippage=self.get_slippage(trade.vt_symbol),
                                               size=self.get_size(trade.vt_symbol),
                                               group_id=g_id,
                                               margin_rate=self.get_margin_rate(trade.vt_symbol),
                                               fix_commission=self.get_fix_commission(trade.vt_symbol))

                        t = OrderedDict()
                        t['gid'] = g_id
                        t['strategy'] = open_trade.strategy_name
                        t['vt_symbol'] = open_trade.vt_symbol
                        t['open_time'] = open_trade.time
                        t['open_price'] = open_trade.price
                        t['direction'] = u'Long'
                        t['close_time'] = trade.time
                        t['close_price'] = trade.price
                        t['volume'] = open_trade.volume
                        t['profit'] = result.pnl
                        t['commission'] = result.commission
                        self.trade_pnl_list.append(t)

                        # 更新策略实例的累加盈亏
                        self.pnl_strategy_dict.update(
                            {open_trade.strategy_name: self.pnl_strategy_dict.get(open_trade.strategy_name,
                                                                                  0) + result.pnl})

                        msg = u'gid:{} {}[{}:开多tid={}:{}]-[{}.平多tid={},{},vol:{}],净盈亏pnl={},手续费:{}' \
                            .format(g_id, open_trade.vt_symbol,
                                    open_trade.time, longid, open_trade.price,
                                    trade.time, vt_tradeid, trade.price,
                                    open_trade.volume, result.pnl, result.commission)

                        self.write_log(msg)
                        result_list.append(result)

                        if g_result is None:
                            if sell_volume > 0:
                                # 属于组合
                                g_result = copy.deepcopy(result)
                        else:
                            # 更新组合的数据
                            g_result.turnover = g_result.turnover + result.turnover
                            g_result.commission = g_result.commission + result.commission
                            g_result.slippage = g_result.slippage + result.slippage
                            g_result.pnl = g_result.pnl + result.pnl

                            if sell_volume == 0:
                                g_result.volume = abs(trade.volume)

                    # 开多volume,大于平仓volume，需要更新减少tradeDict的数量。
                    else:
                        remain_volume = open_trade.volume - sell_volume
                        remain_volume = round(remain_volume, 7)
                        self.write_log(f'{open_trade.vt_symbol}  pos: {open_trade.volume} => {remain_volume}')

                        result = TradingResult(open_price=open_trade.price,
                                               open_datetime=open_trade.datetime,
                                               exit_price=trade.price,
                                               close_datetime=trade.datetime,
                                               volume=sell_volume,
                                               rate=self.get_commission_rate(trade.vt_symbol),
                                               slippage=self.get_slippage(trade.vt_symbol),
                                               size=self.get_size(trade.vt_symbol),
                                               group_id=g_id,
                                               margin_rate=self.get_margin_rate(trade.vt_symbol),
                                               fix_commission=self.get_fix_commission(trade.vt_symbol))

                        t = OrderedDict()
                        t['gid'] = g_id
                        t['strategy'] = open_trade.strategy_name
                        t['vt_symbol'] = open_trade.vt_symbol
                        t['open_time'] = open_trade.time
                        t['open_price'] = open_trade.price
                        t['direction'] = u'Long'
                        t['close_time'] = trade.time
                        t['close_price'] = trade.price
                        t['volume'] = sell_volume
                        t['profit'] = result.pnl
                        t['commission'] = result.commission
                        self.trade_pnl_list.append(t)

                        # 更新策略实例的累加盈亏
                        self.pnl_strategy_dict.update(
                            {open_trade.strategy_name: self.pnl_strategy_dict.get(open_trade.strategy_name,
                                                                                  0) + result.pnl})

                        msg = u'Gid:{} {}[{}:开多tid={}:{}]-[{}.平多tid={},{},vol:{}],净盈亏pnl={},手续费:{}' \
                            .format(g_id, open_trade.vt_symbol, open_trade.time, longid, open_trade.price,
                                    trade.time, vt_tradeid, trade.price, sell_volume, result.pnl,
                                    result.commission)

                        self.write_log(msg)

                        # 减少开多volume,重新推进多单持仓列表中
                        open_trade.volume = remain_volume
                        self.long_position_list.append(open_trade)

                        sell_volume = 0
                        result_list.append(result)

                        if g_result is not None:
                            # 更新组合的数据
                            g_result.turnover = g_result.turnover + result.turnover
                            g_result.commission = g_result.commission + result.commission
                            g_result.slippage = g_result.slippage + result.slippage
                            g_result.pnl = g_result.pnl + result.pnl
                            g_result.volume = abs(trade.volume)

                if g_result is not None:
                    self.write_log(u'组合净盈亏:{0}'.format(g_result.pnl))

        # 计算仓位比例
        holding_cost = 0.0  # 持仓成本
        long_pos_dict = {}

        if len(self.long_position_list) > 0:
            for t in self.long_position_list:
                # 当前持仓的成本
                cur_holding_cost = t.price * t.volume
                holding_cost = holding_cost + cur_holding_cost

        # 可用资金 = 当前净值 - 占用保证金
        self.available = self.net_capital - holding_cost
        # 当前成本占比
        self.percent = round(float(holding_cost * 100 / self.net_capital), 2)
        # 更新最大成本占比
        self.max_occupy_rate = max(self.max_occupy_rate, self.percent)

        # 检查是否有平交易
        if len(result_list) == 0:
            msg = u''
            if len(self.long_position_list) > 0:
                msg += u'持多仓{0},'.format(str(long_pos_dict))

            msg += u'资金占用:{0},仓位:{1}%%'.format(holding_cost, self.percent)

            self.write_log(msg)
            return

        # 对交易结果汇总统计
        for result in result_list:
            if result.pnl > 0:
                self.winning_result += 1
                self.total_winning += result.pnl
            else:
                self.losing_result += 1
                self.total_losing += result.pnl
            self.cur_capital += result.pnl
            self.max_capital = max(self.cur_capital, self.max_capital)
            self.net_capital = max(self.net_capital, self.cur_capital)
            self.max_net_capital = max(self.net_capital, self.max_net_capital)
            # self.maxVolume = max(self.maxVolume, result.volume)
            drawdown = self.net_capital - self.max_net_capital
            drawdown_rate = round(float(drawdown * 100 / self.max_net_capital), 4)

            self.pnl_list.append(result.pnl)
            self.time_list.append(result.close_datetime)
            self.capital_list.append(self.cur_capital)
            self.drawdown_list.append(drawdown)
            self.drawdown_rate_list.append(drawdown_rate)

            self.total_trade_count += 1
            self.total_turnover += result.turnover
            self.total_commission += result.commission
            self.total_slippage += result.slippage

            msg = u'[gid:{}] {} 交易盈亏:{},交易手续费:{}回撤:{}/{},账号平仓权益:{},持仓权益：{}，累计手续费:{}' \
                .format(result.group_id, result.close_datetime, result.pnl, result.commission, drawdown,
                        drawdown_rate, self.cur_capital, self.net_capital, self.total_commission)

            self.write_log(msg)

        # 重新计算一次avaliable
        self.available = self.net_capital - holding_cost
        self.percent = round(float(holding_cost * 100 / self.net_capital), 2)

    def saving_daily_data(self, d, c, m, commission, benchmark=0):
        """保存每日数据"""
        data = {}
        data['date'] = d.strftime('%Y/%m/%d')  # 日期
        data['capital'] = c  # 当前平仓净值
        data['max_capital'] = m  # 之前得最高净值
        today_holding_profit = 0  # 持仓浮盈
        holding_cost = 0

        strategy_pnl = {}
        for strategy in self.strategies.keys():
            strategy_pnl.update({strategy: self.pnl_strategy_dict.get(strategy, 0)})

        positionMsg = ""
        for longpos in self.long_position_list:
            symbol = longpos.vt_symbol
            # 计算持仓浮盈浮亏/占用保证金
            holding_profit = 0
            last_price = self.get_price(symbol)
            if last_price is not None:
                holding_profit = (last_price - longpos.price) * longpos.volume
                holding_cost += last_price * abs(longpos.volume)

            # 账号的持仓盈亏
            today_holding_profit += holding_profit

            # 计算每个策略实例的持仓盈亏
            strategy_pnl.update({longpos.strategy_name: strategy_pnl.get(longpos.strategy_name, 0) + holding_profit})

            positionMsg += "\n[{},long,p={},v={},m={}]".format(symbol, longpos.price, longpos.volume, round(holding_profit,2))

        data['net'] = c + today_holding_profit  # 当日净值（含持仓盈亏）
        data['rate'] = (c + today_holding_profit) / self.init_capital
        data['holding_cost'] = holding_cost
        data['occupy_rate'] = data['holding_cost'] / data['capital']
        data['commission'] = commission

        data.update(self.price_dict)

        data.update(strategy_pnl)

        self.daily_list.append(data)

        # 更新每日浮动净值
        self.net_capital = data['net']

        # 更新最大初次持仓浮盈净值
        if data['net'] > self.max_net_capital:
            self.max_net_capital = data['net']
            self.max_net_capital_time = data['date']
        drawdown_rate = round((float(self.max_net_capital - data['net']) * 100) / self.max_net_capital, 4)
        if drawdown_rate > self.daily_max_drawdown_rate:
            self.daily_max_drawdown_rate = drawdown_rate
            self.max_drawdown_rate_time = data['date']

        msg = u'{}:  net={}, capital={} max={} margin={} commission={}， pos: {}' \
            .format(data['date'],
                    data['net'], c, m,
                    today_holding_profit,
                    commission,
                    positionMsg)

        if not self.debug:
            self.output(msg)
        else:
            self.write_log(msg)

    # ---------------------------------------------------------------------
    def export_trade_result(self):
        """
        导出交易结果（开仓-》平仓, 平仓收益）
        导出每日净值结果表
        :return:
        """
        if len(self.trade_pnl_list) == 0:
            self.write_log('no traded records')
            return

        s = self.test_name.replace('&', '')
        s = s.replace(' ', '')
        trade_list_csv_file = os.path.abspath(os.path.join(self.get_logs_path(), '{}_trade_list.csv'.format(s)))

        self.write_log(u'save trade records to:{}'.format(trade_list_csv_file))
        import csv
        csv_write_file = open(trade_list_csv_file, 'w', encoding='utf8', newline='')

        fieldnames = ['gid', 'strategy', 'vt_symbol', 'open_time', 'open_price', 'direction', 'close_time',
                      'close_price',
                      'volume', 'profit', 'commission']

        writer = csv.DictWriter(f=csv_write_file, fieldnames=fieldnames, dialect='excel')
        writer.writeheader()

        for row in self.trade_pnl_list:
            writer.writerow(row)

        # 导出每日净值记录表
        if not self.daily_list:
            return

        if self.daily_report_name == '':
            daily_csv_file = os.path.abspath(os.path.join(self.get_logs_path(), '{}_daily_list.csv'.format(s)))
        else:
            daily_csv_file = self.daily_report_name
        self.write_log(u'save daily records to:{}'.format(daily_csv_file))

        csv_write_file2 = open(daily_csv_file, 'w', encoding='utf8', newline='')
        fieldnames = ['date', 'capital', 'net', 'max_capital', 'rate', 'commission', 'holding_cost', 'occupy_rate', 'today_margin_long']
        # 添加合约的每日close价
        fieldnames.extend(sorted(self.price_dict.keys()))
        # 添加策略列表
        fieldnames.extend(sorted(self.strategies.keys()))
        writer2 = csv.DictWriter(f=csv_write_file2, fieldnames=fieldnames, dialect='excel')
        writer2.writeheader()

        for row in self.daily_list:
            writer2.writerow(row)

        if self.is_plot_daily:
            # 生成净值曲线图片
            df = pd.DataFrame(self.daily_list)
            df = df.set_index('date')
            from vnpy.trader.utility import display_dual_axis
            plot_file = os.path.abspath(os.path.join(self.get_logs_path(), '{}_plot.png'.format(s)))

            # 双坐标输出，左侧坐标是净值（比率），右侧是各策略的实际资金收益曲线
            display_dual_axis(df=df, columns1=['rate'], columns2=list(self.strategies.keys()), image_name=plot_file)

        return

    def get_result(self):
        # 返回回测结果
        d = {}
        d['init_capital'] = self.init_capital
        d['net_capital'] = self.net_capital
        d['profit'] = self.net_capital - self.init_capital
        d['max_capital'] = self.max_net_capital  # 取消原 maxCapital

        if len(self.pnl_list) == 0:
            return {}, [], []

        d['max_pnl'] = max(self.pnl_list)
        d['min_pnl'] = min(self.pnl_list)

        d['max_occupy_rate'] = self.max_occupy_rate
        d['total_trade_count'] = self.total_trade_count
        d['total_turnover'] = self.total_turnover
        d['total_commission'] = self.total_commission
        d['total_slippage'] = self.total_slippage
        d['time_list'] = self.time_list
        d['pnl_list'] = self.pnl_list
        d['capital_list'] = self.capital_list
        d['drawdown_list'] = self.drawdown_list
        d['drawdown_rate_list'] = self.drawdown_rate_list  # 净值最大回撤率列表
        d['winning_rate'] = round(100 * self.winning_result / len(self.pnl_list), 4)

        average_winning = 0  # 这里把数据都初始化为0
        average_losing = 0
        profit_loss_ratio = 0

        if self.winning_result:
            average_winning = self.total_winning / self.winning_result  # 平均每笔盈利
        if self.losing_result:
            average_losing = self.total_losing / self.losing_result  # 平均每笔亏损
        if average_losing:
            profit_loss_ratio = -average_winning / average_losing  # 盈亏比

        d['average_winning'] = average_winning
        d['average_losing'] = average_losing
        d['profit_loss_ratio'] = profit_loss_ratio

        # 计算Sharp
        if not self.daily_list:
            return {}, [], []

        capital_net_list = []
        capital_list = []
        for row in self.daily_list:
            capital_net_list.append(row['net'])
            capital_list.append(row['capital'])

        capital = pd.Series(capital_net_list)
        log_returns = np.log(capital).diff().fillna(0)
        sharpe = (log_returns.mean() * 252) / (log_returns.std() * np.sqrt(252))
        d['sharpe'] = sharpe

        return d, capital_net_list, capital_list

    def show_backtesting_result(self):
        """显示回测结果"""

        d, daily_net_capital, daily_capital = self.get_result()

        if len(d) == 0:
            self.output(u'无交易结果')
            return {}, ''

        # 导出交易清单
        self.export_trade_result()

        result_info = OrderedDict()

        # 输出
        self.output('-' * 30)
        result_info.update({u'第一笔交易': str(d['time_list'][0])})
        self.output(u'第一笔交易：\t%s' % d['time_list'][0])

        result_info.update({u'最后一笔交易': str(d['time_list'][-1])})
        self.output(u'最后一笔交易：\t%s' % d['time_list'][-1])

        result_info.update({u'总交易次数': d['total_trade_count']})
        self.output(u'总交易次数：\t%s' % format_number(d['total_trade_count']))

        result_info.update({u'期初资金': d['init_capital']})
        self.output(u'期初资金：\t%s' % format_number(d['init_capital']))

        result_info.update({u'期末资金': d['net_capital']})
        self.output(u'期末资金：\t%s' % format_number(d['net_capital']))

        result_info.update({u'总盈亏': d['profit']})
        self.output(u'总盈亏：\t%s' % format_number(d['profit']))

        result_info.update({u'资金最高净值': d['max_capital']})
        self.output(u'资金最高净值：\t%s' % format_number(d['max_capital']))

        result_info.update({u'资金最高净值时间': str(self.max_net_capital_time)})
        self.output(u'资金最高净值时间：\t%s' % self.max_net_capital_time)

        result_info.update({u'每笔最大盈利': d['max_pnl']})
        self.output(u'每笔最大盈利：\t%s' % format_number(d['max_pnl']))

        result_info.update({u'每笔最大亏损': d['min_pnl']})
        self.output(u'每笔最大亏损：\t%s' % format_number(d['min_pnl']))

        result_info.update({u'净值最大回撤': min(d['drawdown_list'])})
        self.output(u'净值最大回撤: \t%s' % format_number(min(d['drawdown_list'])))

        result_info.update({u'净值最大回撤率': self.daily_max_drawdown_rate})
        self.output(u'净值最大回撤率: \t%s' % format_number(self.daily_max_drawdown_rate))

        result_info.update({u'净值最大回撤时间': str(self.max_drawdown_rate_time)})
        self.output(u'净值最大回撤时间：\t%s' % self.max_drawdown_rate_time)

        result_info.update({u'胜率': d['winning_rate']})
        self.output(u'胜率：\t%s' % format_number(d['winning_rate']))

        result_info.update({u'盈利交易平均值': d['average_winning']})
        self.output(u'盈利交易平均值\t%s' % format_number(d['average_winning']))

        result_info.update({u'亏损交易平均值': d['average_losing']})
        self.output(u'亏损交易平均值\t%s' % format_number(d['average_losing']))

        result_info.update({u'盈亏比': d['profit_loss_ratio']})
        self.output(u'盈亏比：\t%s' % format_number(d['profit_loss_ratio']))

        result_info.update({u'最大资金占比': d['max_occupy_rate']})
        self.output(u'最大资金占比：\t%s' % format_number(d['max_occupy_rate']))

        result_info.update({u'平均每笔盈利': d['profit'] / d['total_trade_count']})
        self.output(u'平均每笔盈利：\t%s' % format_number(d['profit'] / d['total_trade_count']))

        result_info.update({u'平均每笔滑点成本': d['total_slippage'] / d['total_trade_count']})
        self.output(u'平均每笔滑点成本：\t%s' % format_number(d['total_slippage'] / d['total_trade_count']))

        result_info.update({u'平均每笔佣金': d['total_commission'] / d['total_trade_count']})
        self.output(u'平均每笔佣金：\t%s' % format_number(d['total_commission'] / d['total_trade_count']))

        result_info.update({u'Sharpe Ratio': d['sharpe']})
        self.output(u'Sharpe Ratio：\t%s' % format_number(d['sharpe']))

        # 保存回测结果/交易记录/日线统计 至数据库
        self.save_result_to_mongo(result_info)

        return result_info

    def save_setting_to_mongo(self):
        """ 保存测试设置到mongo中"""
        self.task_id = self.test_setting.get('task_id', str(uuid1()))

        # 保存到mongo得配置
        save_mongo = self.test_setting.get('save_mongo', {})
        if len(save_mongo) == 0:
            return

        if not self.mongo_api:
            self.mongo_api = MongoData(host=save_mongo.get('host', 'localhost'), port=save_mongo.get('port', 27017))

        # 移除含有"."的数据
        test_setting = {}
        if self.test_setting is not None:
            for k, v in self.test_setting.items():
                if k not in ['symbol_datas']:
                    test_setting[k] = v
        strategy_setting = {}
        if self.strategy_setting is not None:
            for k, v in self.strategy_setting.items():
                if k not in ['symbol_datas']:
                    strategy_setting[k] = v
        d = {
            'task_id': self.task_id,  # 单实例回测任务id
            'name': self.test_name,  # 回测实例名称， 策略名+参数+时间
            'group_id': self.test_setting.get('group_id', datetime.now().strftime('%y-%m-%d')),  # 回测组合id
            'status': 'start',
            'task_start_time': datetime.now(),  # 任务开始执行时间
            'run_host': socket.gethostname(),  # 任务运行得host主机
            'test_setting': test_setting,  # 回测参数
            'strategy_setting': strategy_setting,  # 策略参数
        }

        #  保存入数据库
        self.mongo_api.db_insert(
            db_name=self.gateway_name,
            col_name='tasks',
            d=d)

    def save_fail_to_mongo(self, fail_msg):
        # 保存到mongo得配置
        save_mongo = self.test_setting.get('save_mongo', {})
        if len(save_mongo) == 0:
            return

        if not self.mongo_api:
            self.mongo_api = MongoData(host=save_mongo.get('host', 'localhost'), port=save_mongo.get('port', 27017))

        # 更新数据到数据库回测记录中
        flt = {'task_id': self.task_id}

        d = self.mongo_api.db_query_one(
            db_name=self.gateway_name,
            col_name='tasks',
            flt=flt)

        if d:
            d.update({'status': 'fail'})  # 更新状态未完成
            d.update({'fail_msg': fail_msg})

            self.write_log(u'更新回测结果至数据库')

            self.mongo_api.db_update(
                db_name=self.gateway_name,
                col_name='tasks',
                filter_dict=flt,
                data_dict=d,
                replace=False)

    def save_result_to_mongo(self, result_info):

        # 保存到mongo得配置
        save_mongo = self.test_setting.get('save_mongo', {})
        if len(save_mongo) == 0:
            return

        if not self.mongo_api:
            self.mongo_api = MongoData(host=save_mongo.get('host', 'localhost'), port=save_mongo.get('port', 27017))

        # 更新数据到数据库回测记录中
        flt = {'task_id': self.task_id}

        d = self.mongo_api.db_query_one(
            db_name=self.gateway_name,
            col_name='tasks',
            flt=flt)

        if d:
            d.update({'status': 'finish'})  # 更新状态未完成
            d.update(result_info)  # 补充回测结果
            d.update({'task_finish_time': datetime.now()})  # 更新回测完成时间
            d.update({'trade_list': binary.Binary(zlib.compress(pickle.dumps(self.trade_pnl_list)))})  # 更新交易记录
            d.update({'daily_list': binary.Binary(zlib.compress(pickle.dumps(self.daily_list)))})  # 更新每日净值记录

            self.write_log(u'更新回测结果至数据库')

            self.mongo_api.db_update(
                db_name=self.gateway_name,
                col_name='tasks',
                filter_dict=flt,
                data_dict=d,
                replace=False)

    def put_strategy_event(self, strategy: CtaTemplate):
        """发送策略更新事件，回测中忽略"""
        pass

    def clear_backtesting_result(self):
        """清空之前回测的结果"""
        # 清空限价单相关
        self.limit_order_count = 0
        self.limit_orders.clear()
        self.active_limit_orders.clear()

        # 清空成交相关
        self.trade_count = 0
        self.trade_dict.clear()
        self.trades.clear()
        self.trade_pnl_list = []

    def append_trade(self, trade: TradeData):
        """
        根据策略名称，写入 logs\test_name_straetgy_name_trade.csv文件
        :param trade:
        :return:
        """
        strategy_name = getattr(trade, 'strategy_name', self.test_name)
        trade_fields = ['symbol', 'exchange', 'vt_symbol', 'tradeid',
                        'vt_tradeid', 'orderid', 'vt_orderid',
                        'direction',
                        'offset', 'price', 'volume', 'time']

        d = OrderedDict()
        try:
            for k in trade_fields:
                if k in ['exchange', 'direction', 'offset']:
                    d[k] = getattr(trade, k).value
                else:
                    d[k] = getattr(trade, k, '')

            trade_file = os.path.abspath(os.path.join(self.get_logs_path(), '{}_trade.csv'.format(strategy_name)))
            self.append_data(file_name=trade_file, dict_data=d)
        except Exception as ex:
            self.write_error(u'写入交易记录csv出错：{},{}'.format(str(ex), traceback.format_exc()))

    #  保存记录相关
    def append_data(self, file_name: str, dict_data: OrderedDict, field_names: list = None):
        """
        添加数据到csv文件中
        :param file_name:  csv的文件全路径
        :param dict_data:  OrderedDict
        :return:
        """
        if field_names is None or field_names == []:
            dict_fieldnames = list(dict_data.keys())
        else:
            dict_fieldnames = field_names

        try:
            if not os.path.exists(file_name):
                self.write_log(u'create csv file:{}'.format(file_name))
                with open(file_name, 'a', encoding='utf8', newline='') as csvWriteFile:
                    writer = csv.DictWriter(f=csvWriteFile, fieldnames=dict_fieldnames, dialect='excel')
                    self.write_log(u'write csv header:{}'.format(dict_fieldnames))
                    writer.writeheader()
                    writer.writerow(dict_data)
            else:
                with open(file_name, 'a', encoding='utf8', newline='') as csvWriteFile:
                    writer = csv.DictWriter(f=csvWriteFile, fieldnames=dict_fieldnames, dialect='excel',
                                            extrasaction='ignore')
                    writer.writerow(dict_data)
        except Exception as ex:
            self.write_error(u'append_data exception:{}'.format(str(ex)))


########################################################################
class TradingResult(object):
    """每笔交易的结果"""

    def __init__(self, open_price, open_datetime, exit_price, close_datetime, volume, rate, slippage, size, group_id,
                 margin_rate, fix_commission=0.0):
        """Constructor"""
        self.open_price = open_price  # 开仓价格
        self.exit_price = exit_price  # 平仓价格

        self.open_datetime = open_datetime  # 开仓时间datetime
        self.close_datetime = close_datetime  # 平仓时间

        self.volume = volume  # 交易数量（+/-代表方向）
        self.group_id = group_id  # 主交易ID（针对多手平仓）

        self.turnover = (self.open_price + self.exit_price) * abs(volume) * margin_rate  # 成交金额(实际保证金金额）
        if fix_commission > 0:
            self.commission = fix_commission * abs(self.volume)
        else:
            self.commission = abs(self.turnover * rate)  # 手续费成本
        self.slippage = slippage * 2 * abs(self.turnover)  # 滑点成本
        self.pnl = ((self.exit_price - self.open_price) * volume
                    - self.commission - self.slippage)  # 净盈亏
