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
# from vnpy.gateway.gj import GjGateway
from vnpy.app.stock_screener import ScreenerApp
# from vnpy.app.cta_stock import CtaStockApp
# from vnpy.app.cta_crypto.base import EVENT_CTA_LOG
from vnpy.app.rpc_service import RpcServiceApp
# from vnpy.app.algo_broker import AlgoBrokerApp
from vnpy.app.account_recorder import AccountRecorderApp
from vnpy.trader.util_pid import update_pid

# from vnpy.trader.util_monitor import OrderMonitor, TradeMonitor, PositionMonitor, AccountMonitor, LogMonitor

SETTINGS["log.active"] = True
SETTINGS["log.level"] = DEBUG
SETTINGS["log.console"] = True
SETTINGS["log.file"] = True

screener_name = '日线选股'

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

        self.g_count = 0
        dt = datetime.now()

        # if dt.hour != self.last_dt.hour:
        self.last_dt = dt
        print(u'run_screener.py checkpoint:{0}'.format(dt))
        self.main_engine.write_log(u'run_screener.py checkpoint:{0}'.format(dt))
        if self.main_engine.get_all_completed_status():
            from vnpy.trader.util_wechat import send_wx_msg
            msg = f'{screener_name}完成所有选股任务'
            send_wx_msg(content=msg)
            self.main_engine.write_log(msg)
            sleep(10)
            os._exit(0)

    def start(self):
        """
        Running in the child process.
        """
        SETTINGS["log.file"] = True

        # 添加选股引擎
        screen_engine = self.main_engine.add_app(ScreenerApp)
        screen_engine.init_engine()

        self.main_engine.write_log("主引擎创建成功")

        while True:
            sleep(1)



if __name__ == "__main__":
    # from vnpy.trader.ui import create_qapp
    # qApp = create_qapp()
    # sys.excepthook = excepthook

    s = DaemonService()
    s.start()
