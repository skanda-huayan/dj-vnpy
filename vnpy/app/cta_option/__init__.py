from pathlib import Path

from vnpy.trader.app import BaseApp
from .base import APP_NAME

# 期权CTA策略引擎
from .engine import CtaOptionEngine

from .template import (
    Direction,
    Offset,
    Exchange,
    Status,
    Color,
    ContractData,
    HistoryRequest,
    TickData,
    BarData,
    TradeData,
    OrderData,
    CtaTemplate,
    CtaOptionTemplate,
    CtaOptionPolicy
    )  # noqa
from vnpy.trader.utility import BarGenerator, ArrayManager  # noqa


class CtaOptionApp(BaseApp):
    """期权引擎App"""

    app_name = APP_NAME
    app_module = __module__
    app_path = Path(__file__).parent
    display_name = "CTA期权策略"
    engine_class = CtaOptionEngine
    widget_name = "CtaOption"
    icon_name = "cta.ico"
