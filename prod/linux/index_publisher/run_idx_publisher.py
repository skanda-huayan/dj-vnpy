# flake8: noqa

import os
import sys
import multiprocessing
from time import sleep
from datetime import datetime, time
from logging import INFO,DEBUG


# 将repostory的目录i，作为根目录，添加到系统环境中。
ROOT_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
sys.path.append(ROOT_PATH)
print(f'append {ROOT_PATH} into sys.path')

from vnpy.event import EventEngine
from vnpy.trader.setting import SETTINGS
from vnpy.trader.engine import MainEngine
from vnpy.gateway.ctp import CtpGateway
from vnpy.app.index_tick_publisher import IndexTickPublisherApp
from vnpy.app.cta_strategy.base import EVENT_CTA_LOG
from vnpy.trader.util_pid import update_pid

SETTINGS["log.active"] = True
SETTINGS["log.level"] = DEBUG
SETTINGS["log.console"] = True


rabbit_setting = {
    "host": "192.168.1.211;192.168.1.212"
}
ctp_setting = {
     "用户名": "xxxx",
    "密码": "xxxx",
    "经纪商代码": "0187",
    "交易服务器": "tcp://114.80.225.2:41205",
    "行情服务器": "tcp://114.80.225.2:41213",
    "交易服务器2": "tcp://114.80.225.10:41205",
    "行情服务器2": "tcp://114.80.225.10:41213",
    "交易服务器_old": "tcp://124.74.247.179:41201",
    "行情服务器_old": "tcp://124.74.247.179:41212",
    "产品名称": "client_huafu_2.0.0",
    "授权编码": "xxxxx",
    "产品信息": ""
}

update_pid()


def run_child():
    """
    Running in the child process.
    """
    SETTINGS["log.file"] = True

    event_engine = EventEngine()
    main_engine = MainEngine(event_engine)
    #main_engine.add_gateway(CtpGateway)
    publisher_engine = main_engine.add_app(IndexTickPublisherApp)
    main_engine.write_log("主引擎创建成功")

    log_engine = main_engine.get_engine("log")
    event_engine.register(EVENT_CTA_LOG, log_engine.process_log_event)
    main_engine.write_log("注册日志事件监听")

    sleep(10)
    main_engine.write_log("启动连接行情 & rabbit")
    publisher_engine.connect(md_address=ctp_setting.get("行情服务器"),
                             userid=ctp_setting.get("用户名"),
                             password=ctp_setting.get("密码"),
                             brokerid=ctp_setting.get("经纪商代码"),
                             rabbit_config=rabbit_setting)
    # publisher_engine.connect(
    #                          rabbit_config=rabbit_setting)
    while True:
        sleep(1)


def run_parent():
    """
    Running in the parent process.
    """
    print("启动CTA策略守护父进程")

    # Chinese futures market trading period (day/night)
    DAY_START = time(8, 45)
    DAY_END = time(15, 30)

    NIGHT_START = time(20, 45)
    NIGHT_END = time(2, 45)

    child_process = None

    while True:
        current_time = datetime.now().time()
        trading = False

        # Check whether in trading period
        if (
            (current_time >= DAY_START and current_time <= DAY_END)
            or (current_time >= NIGHT_START)
            or (current_time <= NIGHT_END)
        ):
            trading = True

        # Start child process in trading period
        if trading and child_process is None:
            print("启动子进程")
            child_process = multiprocessing.Process(target=run_child)
            child_process.start()
            print("子进程启动成功")

        # 非记录时间则退出子进程
        if not trading and child_process is not None:
            print("关闭子进程")
            child_process.terminate()
            child_process.join()
            child_process = None
            print("子进程关闭成功")

        sleep(5)


if __name__ == "__main__":
    run_parent()
    #run_child()
