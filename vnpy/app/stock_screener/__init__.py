# encoding: UTF-8

import os
from pathlib import Path
from vnpy.trader.app import BaseApp
from .engine import StockScreenerEngine, APP_NAME


class ScreenerApp(BaseApp):
    """"""
    app_name = APP_NAME
    app_module = __module__
    app_path = Path(__file__).parent
    display_name = u'选股引擎'
    engine_class = StockScreenerEngine
