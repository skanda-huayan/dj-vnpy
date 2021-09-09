# encoding: UTF-8
# 华富资产


from typing import Dict

from .utility import get_folder_path
from .util_logger import setup_logger
from .event import (
    EVENT_TRADE,
    EVENT_ORDER,
    EVENT_POSITION,
    EVENT_ACCOUNT,
    EVENT_LOG
)


########################################################################
class BasicMonitor(object):
    """
    基础监控

    headers中的值对应的字典格式如下
    {'display': u'中文名', 'cell': ""}

    """
    event_type: str = ""
    data_key: str = ""
    headers: Dict[str, dict] = {}

    # ----------------------------------------------------------------------
    def __init__(self, event_engine=None, monitor_name='BasicMonitor'):
        self.event_engine = event_engine

        self.logger = None

        self.create_logger(monitor_name)
        self.register_event()

    # ----------------------------------------------------------------------
    def register_event(self):
        if self.event_type:
            self.event_engine.register(self.event_type, self.update_event)

    # ----------------------------------------------------------------------
    def update_event(self, event):
        """收到事件更新"""
        data = event.data
        self.update_data(data)

    # ----------------------------------------------------------------------
    def update_data(self, data):
        """将数据更新到表格中"""
        s = []
        for header, value in self.headers.items():
            v = getattr(data, header)
            s.append('%s: %s' % (value['display'], str(v)))
        if self.logger is not None:
            self.logger.info(' '.join(s))

    def create_logger(self, monitor_name):
        """创建日志写入"""
        filename = str(get_folder_path('log').joinpath(monitor_name))
        print(u'create logger:{}'.format(filename))
        self.logger = setup_logger(file_name=filename, name=monitor_name)


class LogMonitor(BasicMonitor):
    """
    Monitor for log data.
    """

    event_type = EVENT_LOG
    data_key = ""

    headers = {
        "time": {"display": "时间", "update": False},
        "msg": {"display": "信息", "update": False},
        "gateway_name": {"display": "接口", "update": False},
    }

    def __init__(self, event_engine=None, monitor_name='LogMonitor'):
        super().__init__(event_engine, monitor_name)


class TradeMonitor(BasicMonitor):
    """
    Monitor for trade data.
    """

    event_type = EVENT_TRADE
    data_key = ""
    sorting = True

    headers: Dict[str, dict] = {
        "tradeid": {"display": "成交号 ", "update": False},
        "orderid": {"display": "委托号", "update": False},
        "symbol": {"display": "代码", "update": False},
        "name": {"display": "名称", "update": False},
        "exchange": {"display": "交易所", "update": False},
        "direction": {"display": "方向", "update": False},
        "offset": {"display": "开平", "update": False},
        "price": {"display": "价格", "update": False},
        "volume": {"display": "数量", "update": False},
        "time": {"display": "时间", "update": False},
        "gateway_name": {"display": "接口", "update": False},
    }

    def __init__(self, event_engine=None, monitor_name='TradeMonitor'):
        super().__init__(event_engine, monitor_name)


class OrderMonitor(BasicMonitor):
    """
    Monitor for order data.
    """

    event_type = EVENT_ORDER
    data_key = "vt_orderid"
    sorting = True

    headers: Dict[str, dict] = {
        "orderid": {"display": "委托号", "update": False},
        "symbol": {"display": "代码", "update": False},
        "name": {"display": "名称", "update": False},
        "exchange": {"display": "交易所", "update": False},
        "type": {"display": "类型", "update": False},
        "direction": {"display": "方向", "update": False},
        "offset": {"display": "开平", "update": False},
        "price": {"display": "价格", "update": False},
        "volume": {"display": "总数量", "update": True},
        "traded": {"display": "已成交", "update": True},
        "status": {"display": "状态", "update": True},
        "time": {"display": "时间", "update": True},
        "gateway_name": {"display": "接口", "update": False},
    }

    def __init__(self, event_engine=None, monitor_name='OrderMonitor'):
        super().__init__(event_engine, monitor_name)


class PositionMonitor(BasicMonitor):
    """
    Monitor for position data.
    """

    event_type = EVENT_POSITION
    data_key = "vt_positionid"
    sorting = True

    headers = {
        "symbol": {"display": "代码", "update": False},
        "name": {"display": "名称", "update": False},
        "exchange": {"display": "交易所", "update": False},
        "direction": {"display": "方向", "update": False},
        "volume": {"display": "数量", "update": True},
        "yd_volume": {"display": "昨仓", "update": True},
        "frozen": {"display": "冻结", "update": True},
        "price": {"display": "均价", "update": True},
        "pnl": {"display": "盈亏", "update": True},
        "gateway_name": {"display": "接口", "update": False},
    }

    def __init__(self, event_engine=None, monitor_name='PositionMonitor'):
        super().__init__(event_engine, monitor_name)


class AccountMonitor(BasicMonitor):
    """
    Monitor for account data.
    """

    event_type = EVENT_ACCOUNT
    data_key = "vt_accountid"
    sorting = True

    headers = {
        "accountid": {"display": "账号", "update": False},
        "pre_balance": {"display": "昨净值", "update": False},
        "balance": {"display": "净值", "update": True},
        "frozen": {"display": "冻结", "update": True},
        "margin": {"display": "保证金", "update": True},
        "available": {"display": "可用", "update": True},
        "commission": {"display": "手续费", "update": True},
        "close_profit": {"display": "平仓收益", "update": True},
        "holding_profit": {"display": "持仓收益", "update": True},
        "gateway_name": {"display": "接口", "update": False},
    }

    def __init__(self, event_engine=None, monitor_name='AccountMonitor'):
        super().__init__(event_engine, monitor_name)
