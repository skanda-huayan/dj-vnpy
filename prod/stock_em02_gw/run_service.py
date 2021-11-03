# flake8: noqa

import os
import sys
import multiprocessing
from time import sleep
from datetime import datetime, time
from logging import DEBUG

# 将repostory的目录i，作为根目录，添加到系统环境中。
ROOT_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if ROOT_PATH not in sys.path:
    sys.path.append(ROOT_PATH)
    print(f'append {ROOT_PATH} into sys.path')

from vnpy.event import EventEngine, EVENT_TIMER
from vnpy.trader.setting import SETTINGS
from vnpy.trader.engine import MainEngine
from vnpy.trader.utility import load_json
from vnpy.gateway.eastmoney import EastmoneyGateway
#from vnpy.app.cta_stock import CtaStockApp
#from vnpy.app.cta_crypto.base import EVENT_CTA_LOG
from vnpy.app.rpc_service import RpcServiceApp
#from vnpy.app.algo_broker import AlgoBrokerApp
from vnpy.app.account_recorder import AccountRecorderApp
from vnpy.trader.util_pid import update_pid
from vnpy.trader.util_monitor import OrderMonitor, TradeMonitor, PositionMonitor, AccountMonitor, LogMonitor

SETTINGS["log.active"] = True
SETTINGS["log.level"] = DEBUG
SETTINGS["log.console"] = True
SETTINGS["log.file"] = True

gateway_name = 'em02_gw'
gw_setting = load_json(f'connect_{gateway_name}.json')

import types
import traceback

def excepthook(exctype: type, value: Exception, tb: types.TracebackType) -> None:
    """
    Raise exception under debug mode
    """
    sys.__excepthook__(exctype, value, tb)

    msg = "".join(traceback.format_exception(exctype, value, tb))

    print(msg, file=sys.stderr)

class DaemonService(object):

    def __init__(self):
        self.event_engine = EventEngine()
        self.g_count = 0
        self.last_dt = datetime.now()
        # 创建账号/持仓/委托/交易/日志记录
        self.acc_monitor = AccountMonitor(self.event_engine)
        self.pos_monitor = PositionMonitor(self.event_engine)
        self.ord_monitor = OrderMonitor(self.event_engine)
        self.trd_monitor = TradeMonitor(self.event_engine)
        #self.log_monitor = LogMonitor(self.event_engine)

        # 创建主引擎
        self.main_engine = MainEngine(self.event_engine)

        self.save_data_time = None
        self.save_snapshot_time = None

        # 注册定时器，用于判断重连
        self.event_engine.register(EVENT_TIMER, self.on_timer)

    def on_timer(self, event):
        """定时器执行逻辑，每十秒执行一次"""

        # 60秒才执行一次检查
        self.g_count += 1
        if self.g_count <= 60:
            return

        # 强制写入一次gpid
        update_pid()

        self.g_count = 0
        dt = datetime.now()

        if dt.hour != self.last_dt.hour:
            self.last_dt = dt
            #print(u'run_server.py checkpoint:{0}'.format(dt))
            self.main_engine.write_log(u'run_server.py checkpoint:{0}'.format(dt))

        # ctp 短线重连得处理

         # 定时保存策略内数据
        if dt.strftime('%H:%M') in ['02:31', '15:16']:
            if self.save_data_time != dt.strftime('%H:%M'):
                self.main_engine.write_log(u'保存策略内数据')
                self.save_data_time = dt.strftime('%H:%M')
                try:
                    self.main_engine.save_strategy_data('ALL')
                except Exception as ex:
                    self.main_engine.write_error('保存策略内数据异常')

        if dt.strftime('%H:%M') in ['02:32', '10:16', '11:31', '15:17', '23:01']:
            if self.save_snapshot_time != dt.strftime('%H:%M'):
                self.main_engine.write_log(u'保存策略内K线切片数据')
                self.save_snapshot_time = dt.strftime('%H:%M')
                try:
                    self.main_engine.save_strategy_snapshot('ALL')
                except Exception as ex:
                    self.main_engine.write_error('保存策略内数据异常')

    def start(self):
        """
        Running in the child process.
        """
        SETTINGS["log.file"] = True

        timer_count = 0

        # 远程调用服务
        rpc_server = self.main_engine.add_app(RpcServiceApp)
        ret, msg = rpc_server.start()
        if not ret:
            self.main_engine.write_log(f"RPC服务未能启动:{msg}")
            return
        else:
            self.main_engine.write_log(f'RPC服务已启动')

        update_pid()

        # 添加账号同步app
        self.main_engine.add_app(AccountRecorderApp)

        # 接入网关
        self.main_engine.add_gateway(EastmoneyGateway, gateway_name)
        self.main_engine.write_log(f"连接{gateway_name}接口")
        self.main_engine.connect(gw_setting, gateway_name)

        sleep(5)

        # # 添加cta引擎
        # cta_engine = self.main_engine.add_app(CtaStockApp)
        # cta_engine.init_engine()

        # 添加算法引擎代理
        #self.main_engine.add_app(AlgoBrokerApp)

        self.main_engine.write_log("主引擎创建成功")

        while True:
            sleep(1)


if __name__ == "__main__":

    sys.excepthook = excepthook

    s = DaemonService()
    s.start()
