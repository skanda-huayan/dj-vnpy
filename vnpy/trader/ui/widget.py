"""
Basic widgets for VN Trader.
"""

import csv
from enum import Enum
from typing import Any, Dict
from copy import copy

from PyQt5 import QtCore, QtGui, QtWidgets

from vnpy.event import Event, EventEngine
from ..constant import Direction, Exchange, Offset, OrderType
from ..engine import MainEngine
from ..event import (
    EVENT_TICK,
    EVENT_TRADE,
    EVENT_ORDER,
    EVENT_POSITION,
    EVENT_ACCOUNT,
    EVENT_LOG
)
from ..object import OrderRequest, SubscribeRequest,LogData
from ..utility import load_json, save_json
from ..setting import SETTING_FILENAME, SETTINGS

COLOR_LONG = QtGui.QColor("red")
COLOR_SHORT = QtGui.QColor("green")
COLOR_BID = QtGui.QColor(255, 174, 201)
COLOR_ASK = QtGui.QColor(160, 255, 160)
COLOR_BLACK = QtGui.QColor("black")


class BaseCell(QtWidgets.QTableWidgetItem):
    """
    General cell used in tablewidgets.
    """

    def __init__(self, content: Any, data: Any):
        """"""
        super(BaseCell, self).__init__()
        self.setTextAlignment(QtCore.Qt.AlignCenter)
        self.set_content(content, data)

    def set_content(self, content: Any, data: Any) -> None:
        """
        Set text content.
        """
        self.setText(str(content))
        if isinstance(data, float):
            data = round(data, 7)
        self._data = data

    def get_data(self) -> Any:
        """
        Get data object.
        """
        return self._data


class EnumCell(BaseCell):
    """
    Cell used for showing enum data.
    """

    def __init__(self, content: str, data: Any):
        """"""
        super(EnumCell, self).__init__(content, data)

    def set_content(self, content: Any, data: Any) -> None:
        """
        Set text using enum.constant.value.
        """
        if content:
            if isinstance(content, str):
                super(EnumCell, self).set_content(content, data)
            else:
                super(EnumCell, self).set_content(content.value, data)


class DirectionCell(EnumCell):
    """
    Cell used for showing direction data.
    """

    def __init__(self, content: str, data: Any):
        """"""
        super(DirectionCell, self).__init__(content, data)

    def set_content(self, content: Any, data: Any) -> None:
        """
        Cell color is set according to direction.
        """
        super(DirectionCell, self).set_content(content, data)

        if content is Direction.SHORT:
            self.setForeground(COLOR_SHORT)
        else:
            self.setForeground(COLOR_LONG)


class BidCell(BaseCell):
    """
    Cell used for showing bid price and volume.
    """

    def __init__(self, content: Any, data: Any):
        """"""
        super(BidCell, self).__init__(content, data)

        self.setForeground(COLOR_BID)


class AskCell(BaseCell):
    """
    Cell used for showing ask price and volume.
    """

    def __init__(self, content: Any, data: Any):
        """"""
        super(AskCell, self).__init__(content, data)

        self.setForeground(COLOR_ASK)


class PnlCell(BaseCell):
    """
    Cell used for showing pnl data.
    """

    def __init__(self, content: Any, data: Any):
        """"""
        super(PnlCell, self).__init__(content, data)

    def set_content(self, content: Any, data: Any) -> None:
        """
        Cell color is set based on whether pnl is
        positive or negative.
        """
        super(PnlCell, self).set_content(content, data)

        if str(content).startswith("-"):
            self.setForeground(COLOR_SHORT)
        else:
            self.setForeground(COLOR_LONG)


class TimeCell(BaseCell):
    """
    Cell used for showing time string from datetime object.
    """

    def __init__(self, content: Any, data: Any):
        """"""
        super(TimeCell, self).__init__(content, data)

    def set_content(self, content: Any, data: Any) -> None:
        """
        Time format is 12:12:12.5
        """
        if content is None:
            return

        timestamp = content.strftime("%H:%M:%S")

        millisecond = int(content.microsecond / 1000)
        if millisecond:
            timestamp = f"{timestamp}.{millisecond}"

        self.setText(timestamp)
        self._data = data


class MsgCell(BaseCell):
    """
    Cell used for showing msg data.
    """

    def __init__(self, content: str, data: Any):
        """"""
        super(MsgCell, self).__init__(content, data)
        self.setTextAlignment(QtCore.Qt.AlignLeft | QtCore.Qt.AlignVCenter)


class BaseMonitor(QtWidgets.QTableWidget):
    """
    Monitor data update in VN Trader.
    """

    event_type: str = ""
    data_key: str = ""
    sorting: bool = False
    headers: Dict[str, dict] = {}

    signal: QtCore.pyqtSignal = QtCore.pyqtSignal(Event)

    def __init__(self, main_engine: MainEngine, event_engine: EventEngine):
        """"""
        super(BaseMonitor, self).__init__()

        self.main_engine: MainEngine = main_engine
        self.event_engine: EventEngine = event_engine
        self.cells: Dict[str, dict] = {}

        self.init_ui()
        self.register_event()

    def init_ui(self) -> None:
        """"""
        self.init_table()
        self.init_menu()

    def init_table(self) -> None:
        """
        Initialize table.
        """
        self.setColumnCount(len(self.headers))

        labels = [d["display"] for d in self.headers.values()]
        self.setHorizontalHeaderLabels(labels)

        self.verticalHeader().setVisible(False)
        self.setEditTriggers(self.NoEditTriggers)
        self.setAlternatingRowColors(True)
        self.setSortingEnabled(self.sorting)

    def init_menu(self) -> None:
        """
        Create right click menu.
        """
        self.menu = QtWidgets.QMenu(self)

        resize_action = QtWidgets.QAction("调整列宽", self)
        resize_action.triggered.connect(self.resize_columns)
        self.menu.addAction(resize_action)

        save_action = QtWidgets.QAction("保存数据", self)
        save_action.triggered.connect(self.save_csv)
        self.menu.addAction(save_action)

    def register_event(self) -> None:
        """
        Register event handler into event engine.
        """
        if self.event_type:
            self.signal.connect(self.process_event)
            self.event_engine.register(self.event_type, self.signal.emit)

    def process_event(self, event: Event) -> None:
        """
        Process new data from event and update into table.
        """
        # Disable sorting to prevent unwanted error.
        if self.sorting:
            self.setSortingEnabled(False)

        # Update data into table.
        data = event.data

        if not self.data_key:
            self.insert_new_row(data)
        else:
            key = data.__getattribute__(self.data_key)

            if key in self.cells:
                self.update_old_row(data)
            else:
                self.insert_new_row(data)

        # Enable sorting
        if self.sorting:
            self.setSortingEnabled(True)

    def insert_new_row(self, data: Any):
        """
        Insert a new row at the top of table.
        """
        self.insertRow(0)

        row_cells = {}
        for column, header in enumerate(self.headers.keys()):
            setting = self.headers[header]

            content = data.__getattribute__(header)
            cell = setting["cell"](content, data)
            self.setItem(0, column, cell)

            if setting["update"]:
                row_cells[header] = cell

        if self.data_key:
            key = data.__getattribute__(self.data_key)
            self.cells[key] = row_cells

    def update_old_row(self, data: Any) -> None:
        """
        Update an old row in table.
        """
        key = data.__getattribute__(self.data_key)
        row_cells = self.cells[key]

        for header, cell in row_cells.items():
            content = data.__getattribute__(header)
            cell.set_content(content, data)

    def resize_columns(self) -> None:
        """
        Resize all columns according to contents.
        """
        self.horizontalHeader().resizeSections(QtWidgets.QHeaderView.ResizeToContents)

    def save_csv(self) -> None:
        """
        Save table data into a csv file
        """
        path, _ = QtWidgets.QFileDialog.getSaveFileName(
            self, "保存数据", "", "CSV(*.csv)")

        if not path:
            return

        with open(path, "w") as f:
            writer = csv.writer(f, lineterminator="\n")

            writer.writerow(self.headers.keys())

            for row in range(self.rowCount()):
                row_data = []
                for column in range(self.columnCount()):
                    item = self.item(row, column)
                    if item:
                        row_data.append(str(item.text()))
                    else:
                        row_data.append("")
                writer.writerow(row_data)

    def contextMenuEvent(self, event: QtGui.QContextMenuEvent) -> None:
        """
        Show menu with right click.
        """
        self.menu.popup(QtGui.QCursor.pos())


class TickMonitor(BaseMonitor):
    """
    Monitor for tick data.
    """

    event_type = EVENT_TICK
    data_key = "vt_symbol"
    sorting = True

    headers = {
        "symbol": {"display": "代码", "cell": BaseCell, "update": False},
        "exchange": {"display": "交易所", "cell": EnumCell, "update": False},
        "name": {"display": "名称", "cell": BaseCell, "update": True},
        "last_price": {"display": "最新价", "cell": BaseCell, "update": True},
        "volume": {"display": "成交量", "cell": BaseCell, "update": True},
        "open_price": {"display": "开盘价", "cell": BaseCell, "update": True},
        "high_price": {"display": "最高价", "cell": BaseCell, "update": True},
        "low_price": {"display": "最低价", "cell": BaseCell, "update": True},
        "bid_price_1": {"display": "买1价", "cell": BidCell, "update": True},
        "bid_volume_1": {"display": "买1量", "cell": BidCell, "update": True},
        "ask_price_1": {"display": "卖1价", "cell": AskCell, "update": True},
        "ask_volume_1": {"display": "卖1量", "cell": AskCell, "update": True},
        "datetime": {"display": "时间", "cell": TimeCell, "update": True},
        "gateway_name": {"display": "接口", "cell": BaseCell, "update": False},
    }


class LogMonitor(BaseMonitor):
    """
    Monitor for log data.
    """

    event_type = EVENT_LOG
    data_key = ""
    sorting = False

    headers = {
        "time": {"display": "时间", "cell": TimeCell, "update": False},
        "msg": {"display": "信息", "cell": MsgCell, "update": False},
        "gateway_name": {"display": "接口", "cell": BaseCell, "update": False},
    }


class TradeMonitor(BaseMonitor):
    """
    Monitor for trade data.
    """

    event_type = EVENT_TRADE
    data_key = ""
    sorting = True

    headers: Dict[str, dict] = {
        "tradeid": {"display": "成交号 ", "cell": BaseCell, "update": False},
        "orderid": {"display": "委托号", "cell": BaseCell, "update": False},
        "symbol": {"display": "代码", "cell": BaseCell, "update": False},
        "name": {"display": "名称", "cell": BaseCell, "update": False},
        "exchange": {"display": "交易所", "cell": EnumCell, "update": False},
        "direction": {"display": "方向", "cell": DirectionCell, "update": False},
        "offset": {"display": "开平", "cell": EnumCell, "update": False},
        "price": {"display": "价格", "cell": BaseCell, "update": False},
        "volume": {"display": "数量", "cell": BaseCell, "update": False},
        "time": {"display": "时间", "cell": BaseCell, "update": False},
        "gateway_name": {"display": "接口", "cell": BaseCell, "update": False},
    }


class OrderMonitor(BaseMonitor):
    """
    Monitor for order data.
    """

    event_type = EVENT_ORDER
    data_key = "vt_orderid"
    sorting = True

    headers: Dict[str, dict] = {
        "orderid": {"display": "委托号", "cell": BaseCell, "update": False},
        "symbol": {"display": "代码", "cell": BaseCell, "update": False},
        "name": {"display": "名称", "cell": BaseCell, "update": False},
        "exchange": {"display": "交易所", "cell": EnumCell, "update": False},
        "type": {"display": "类型", "cell": EnumCell, "update": False},
        "direction": {"display": "方向", "cell": DirectionCell, "update": False},
        "offset": {"display": "开平", "cell": EnumCell, "update": False},
        "price": {"display": "价格", "cell": BaseCell, "update": False},
        "volume": {"display": "总数量", "cell": BaseCell, "update": True},
        "traded": {"display": "已成交", "cell": BaseCell, "update": True},
        "status": {"display": "状态", "cell": EnumCell, "update": True},
        "time": {"display": "时间", "cell": BaseCell, "update": True},
        "gateway_name": {"display": "接口", "cell": BaseCell, "update": False},
    }

    def init_ui(self):
        """
        Connect signal.
        """
        super(OrderMonitor, self).init_ui()

        self.setToolTip("双击单元格撤单")
        self.itemDoubleClicked.connect(self.cancel_order)

    def cancel_order(self, cell: BaseCell) -> None:
        """
        Cancel order if cell double clicked.
        """
        order = cell.get_data()
        req = order.create_cancel_request()
        self.main_engine.cancel_order(req, order.gateway_name)


class PositionMonitor(BaseMonitor):
    """
    Monitor for position data.
    """

    event_type = EVENT_POSITION
    data_key = "vt_positionid"
    sorting = True

    headers = {
        "name": {"display": "名称", "cell": BaseCell, "update": False},
        "symbol": {"display": "代码", "cell": BaseCell, "update": False},
        "exchange": {"display": "交易所", "cell": EnumCell, "update": False},
        "direction": {"display": "方向", "cell": DirectionCell, "update": False},
        "volume": {"display": "数量", "cell": BaseCell, "update": True},
        "yd_volume": {"display": "昨仓", "cell": BaseCell, "update": True},
        "frozen": {"display": "冻结", "cell": BaseCell, "update": True},
        "cur_price": {"display": "当前价", "cell": BaseCell, "update": True},
        "price": {"display": "均价", "cell": BaseCell, "update": True},
        "pnl": {"display": "盈亏", "cell": PnlCell, "update": True},
        "gateway_name": {"display": "接口", "cell": BaseCell, "update": False},
    }


class AccountMonitor(BaseMonitor):
    """
    Monitor for account data.
    """

    event_type = EVENT_ACCOUNT
    data_key = "vt_accountid"
    sorting = True

    headers = {
        "accountid": {"display": "账号", "cell": BaseCell, "update": False},
        "pre_balance": {"display": "昨净值", "cell": BaseCell, "update": False},
        "balance": {"display": "净值", "cell": BaseCell, "update": True},
        "frozen": {"display": "冻结", "cell": BaseCell, "update": True},
        "margin": {"display": "保证金", "cell": BaseCell, "update": True},
        "available": {"display": "可用", "cell": BaseCell, "update": True},
        "commission": {"display": "手续费", "cell": BaseCell, "update": True},
        "close_profit": {"display": "平仓收益", "cell": BaseCell, "update": True},
        "holding_profit": {"display": "持仓收益", "cell": BaseCell, "update": True},
        "gateway_name": {"display": "接口", "cell": BaseCell, "update": False},
    }


class ConnectDialog(QtWidgets.QDialog):
    """
    Start connection of a certain gateway.
    """

    def __init__(self, main_engine: MainEngine, gateway_name: str):
        """"""
        super().__init__()
        self.setting = {}
        self.main_engine: MainEngine = main_engine
        self.gateway_name: str = gateway_name
        self.filename: str = f"connect_{gateway_name.lower()}.json"

        self.widgets: Dict[str, QtWidgets.QWidget] = {}

        self.init_ui()

    def init_ui(self) -> None:
        """"""
        self.setWindowTitle(f"连接{self.gateway_name}")

        # Default setting provides field name, field data type and field default value.
        default_setting = self.main_engine.get_default_setting(
            self.gateway_name)

        # Saved setting provides field data used last time.
        loaded_setting = load_json(self.filename)

        self.setting.update(loaded_setting)

        # Initialize line edits and form layout based on setting.
        form = QtWidgets.QFormLayout()

        for field_name, field_value in default_setting.items():
            field_type = type(field_value)

            if field_type == list:
                widget = QtWidgets.QComboBox()
                widget.addItems(field_value)

                if field_name in loaded_setting:
                    saved_value = loaded_setting[field_name]
                    ix = widget.findText(saved_value)
                    widget.setCurrentIndex(ix)
            else:
                widget = QtWidgets.QLineEdit(str(field_value))

                if field_name in loaded_setting:
                    saved_value = loaded_setting[field_name]
                    widget.setText(str(saved_value))

                if "密码" in field_name:
                    widget.setEchoMode(QtWidgets.QLineEdit.Password)

            form.addRow(f"{field_name} <{field_type.__name__}>", widget)
            self.widgets[field_name] = (widget, field_type)

        button = QtWidgets.QPushButton("连接")
        button.clicked.connect(self.connect)
        form.addRow(button)

        self.setLayout(form)

    def connect(self) -> None:
        """
        Get setting value from line edits and connect the gateway.
        """
        setting = {}
        for field_name, tp in self.widgets.items():
            widget, field_type = tp
            if field_type == list:
                field_value = str(widget.currentText())
            else:
                field_value = field_type(widget.text())
            setting[field_name] = field_value

        self.setting.update(setting)

        save_json(self.filename, self.setting)

        self.main_engine.connect(self.setting, self.gateway_name)

        self.accept()


class TradingWidget(QtWidgets.QWidget):
    """
    General manual trading widget.
    """

    signal_tick = QtCore.pyqtSignal(Event)

    def __init__(self, main_engine: MainEngine, event_engine: EventEngine):
        """"""
        super().__init__()

        self.main_engine: MainEngine = main_engine
        self.event_engine: EventEngine = event_engine

        self.vt_symbol: str = ""

        self.init_ui()
        self.register_event()

    def init_ui(self) -> None:
        """"""
        self.setFixedWidth(300)

        # Trading function area
        exchanges = self.main_engine.get_all_exchanges()
        self.exchange_combo = QtWidgets.QComboBox()
        self.exchange_combo.addItems([exchange.value for exchange in exchanges])

        self.symbol_line = QtWidgets.QLineEdit()
        self.symbol_line.returnPressed.connect(self.set_vt_symbol)

        self.name_line = QtWidgets.QLineEdit()
        self.name_line.setReadOnly(True)

        self.direction_combo = QtWidgets.QComboBox()
        self.direction_combo.addItems(
            [Direction.LONG.value, Direction.SHORT.value])

        self.offset_combo = QtWidgets.QComboBox()
        self.offset_combo.addItems([offset.value for offset in Offset])

        self.order_type_combo = QtWidgets.QComboBox()
        self.order_type_combo.addItems(
            [order_type.value for order_type in OrderType])

        double_validator = QtGui.QDoubleValidator()
        # double_validator.setBottom(0)

        self.price_line = QtWidgets.QLineEdit()
        self.price_line.setValidator(double_validator)

        self.volume_line = QtWidgets.QLineEdit()
        self.volume_line.setValidator(double_validator)

        self.gateway_combo = QtWidgets.QComboBox()
        self.gateway_combo.addItems(self.main_engine.get_all_gateway_names())

        send_button = QtWidgets.QPushButton("委托")
        send_button.clicked.connect(self.send_order)

        cancel_button = QtWidgets.QPushButton("全撤")
        cancel_button.clicked.connect(self.cancel_all)

        algo_stop_button = QtWidgets.QPushButton("全停算法")
        algo_stop_button.clicked.connect(self.stop_algo)

        hbox_nomal = QtWidgets.QHBoxLayout()
        hbox_nomal.addWidget(send_button)
        hbox_nomal.addWidget(cancel_button)
        hbox_nomal.addWidget(algo_stop_button)

        algo_button = QtWidgets.QPushButton("算法单")
        algo_button.clicked.connect(self.send_algo)

        self.win_pips = QtWidgets.QLineEdit()
        self.win_pips.setText('10')
        self.stop_pips = QtWidgets.QLineEdit()
        self.stop_pips.setText('5')
        hbox_algo = QtWidgets.QHBoxLayout()
        win_lable = QtWidgets.QLabel("止盈跳")
        hbox_algo.addWidget(win_lable)
        hbox_algo.addWidget(self.win_pips)
        stop_lable = QtWidgets.QLabel("止损跳")
        hbox_algo.addWidget(stop_lable)
        hbox_algo.addWidget(self.stop_pips)
        hbox_algo.addWidget(algo_button)

        self.checkFixed = QtWidgets.QCheckBox("价格")  # 价格固定选择框

        form1 = QtWidgets.QFormLayout()
        form1.addRow("交易所", self.exchange_combo)
        form1.addRow("代码", self.symbol_line)
        form1.addRow("名称", self.name_line)
        form1.addRow("方向", self.direction_combo)
        form1.addRow("开平", self.offset_combo)
        form1.addRow("类型", self.order_type_combo)
        form1.addRow(self.checkFixed, self.price_line)
        form1.addRow("数量", self.volume_line)
        form1.addRow("接口", self.gateway_combo)
        form1.addRow(hbox_nomal)
        form1.addRow(hbox_algo)

        # Market depth display area
        bid_color = "rgb(255,174,201)"
        ask_color = "rgb(160,255,160)"

        self.bp1_label = self.create_label(bid_color)
        self.bp2_label = self.create_label(bid_color)
        self.bp3_label = self.create_label(bid_color)
        self.bp4_label = self.create_label(bid_color)
        self.bp5_label = self.create_label(bid_color)

        self.bv1_label = self.create_label(
            bid_color, alignment=QtCore.Qt.AlignRight)
        self.bv2_label = self.create_label(
            bid_color, alignment=QtCore.Qt.AlignRight)
        self.bv3_label = self.create_label(
            bid_color, alignment=QtCore.Qt.AlignRight)
        self.bv4_label = self.create_label(
            bid_color, alignment=QtCore.Qt.AlignRight)
        self.bv5_label = self.create_label(
            bid_color, alignment=QtCore.Qt.AlignRight)

        self.ap1_label = self.create_label(ask_color)
        self.ap2_label = self.create_label(ask_color)
        self.ap3_label = self.create_label(ask_color)
        self.ap4_label = self.create_label(ask_color)
        self.ap5_label = self.create_label(ask_color)

        self.av1_label = self.create_label(
            ask_color, alignment=QtCore.Qt.AlignRight)
        self.av2_label = self.create_label(
            ask_color, alignment=QtCore.Qt.AlignRight)
        self.av3_label = self.create_label(
            ask_color, alignment=QtCore.Qt.AlignRight)
        self.av4_label = self.create_label(
            ask_color, alignment=QtCore.Qt.AlignRight)
        self.av5_label = self.create_label(
            ask_color, alignment=QtCore.Qt.AlignRight)

        self.lp_label = self.create_label()
        self.return_label = self.create_label(alignment=QtCore.Qt.AlignRight)

        form2 = QtWidgets.QFormLayout()
        form2.addRow(self.ap5_label, self.av5_label)
        form2.addRow(self.ap4_label, self.av4_label)
        form2.addRow(self.ap3_label, self.av3_label)
        form2.addRow(self.ap2_label, self.av2_label)
        form2.addRow(self.ap1_label, self.av1_label)
        form2.addRow(self.lp_label, self.return_label)
        form2.addRow(self.bp1_label, self.bv1_label)
        form2.addRow(self.bp2_label, self.bv2_label)
        form2.addRow(self.bp3_label, self.bv3_label)
        form2.addRow(self.bp4_label, self.bv4_label)
        form2.addRow(self.bp5_label, self.bv5_label)

        # Overall layout
        vbox = QtWidgets.QVBoxLayout()
        vbox.addLayout(form1)
        vbox.addLayout(form2)
        self.setLayout(vbox)

    def create_label(
            self,
            color: str = "",
            alignment: int = QtCore.Qt.AlignLeft
    ) -> QtWidgets.QLabel:
        """
        Create label with certain font color.
        """
        label = QtWidgets.QLabel()
        if color:
            label.setStyleSheet(f"color:{color}")
        label.setAlignment(alignment)
        return label

    def register_event(self) -> None:
        """"""
        self.signal_tick.connect(self.process_tick_event)
        self.event_engine.register(EVENT_TICK, self.signal_tick.emit)

    def process_tick_event(self, event: Event) -> None:
        """"""
        tick = event.data
        if tick.vt_symbol != self.vt_symbol:
            return

        if not self.checkFixed.isChecked():
            self.price_line.setText(str(tick.last_price))

        self.lp_label.setText(str(round(tick.last_price, 7)))
        self.bp1_label.setText(str(round(tick.bid_price_1, 7)))
        self.bv1_label.setText(str(round(tick.bid_volume_1, 7)))
        self.ap1_label.setText(str(round(tick.ask_price_1, 7)))
        self.av1_label.setText(str(round(tick.ask_volume_1, 7)))

        if tick.pre_close:
            r = (tick.last_price / tick.pre_close - 1) * 100
            self.return_label.setText(f"{r:.2f}%")

        if tick.bid_price_2:
            self.bp2_label.setText(str(round(tick.bid_price_2, 7)))
            self.bv2_label.setText(str(round(tick.bid_volume_2, 7)))
            self.ap2_label.setText(str(round(tick.ask_price_2, 7)))
            self.av2_label.setText(str(round(tick.ask_volume_2, 7)))

            self.bp3_label.setText(str(round(tick.bid_price_3, 7)))
            self.bv3_label.setText(str(round(tick.bid_volume_3, 7)))
            self.ap3_label.setText(str(round(tick.ask_price_3, 7)))
            self.av3_label.setText(str(round(tick.ask_volume_3, 7)))

            self.bp4_label.setText(str(round(tick.bid_price_4, 7)))
            self.bv4_label.setText(str(round(tick.bid_volume_4, 7)))
            self.ap4_label.setText(str(round(tick.ask_price_4, 7)))
            self.av4_label.setText(str(round(tick.ask_volume_4, 7)))

            self.bp5_label.setText(str(round(tick.bid_price_5, 7)))
            self.bv5_label.setText(str(round(tick.bid_volume_5, 7)))
            self.ap5_label.setText(str(round(tick.ask_price_5, 7)))
            self.av5_label.setText(str(round(tick.ask_volume_5, 7)))

    def set_vt_symbol(self) -> None:
        """
        Set the tick depth data to monitor by vt_symbol.
        """
        symbol = str(self.symbol_line.text())
        if not symbol:
            return

        # Generate vt_symbol from symbol and exchange
        exchange_value = str(self.exchange_combo.currentText())
        vt_symbol = f"{symbol}.{exchange_value}"

        if vt_symbol == self.vt_symbol:
            return
        self.vt_symbol = vt_symbol

        # Update name line widget and clear all labels
        contract = self.main_engine.get_contract(vt_symbol)
        if not contract:
            self.name_line.setText("")
            gateway_name = self.gateway_combo.currentText()
        else:
            self.name_line.setText(contract.name)
            gateway_name = contract.gateway_name

            # Update gateway combo box.
            ix = self.gateway_combo.findText(gateway_name)
            self.gateway_combo.setCurrentIndex(ix)

        self.clear_label_text()

        # Subscribe tick data
        req = SubscribeRequest(
            symbol=symbol, exchange=Exchange(exchange_value)
        )

        if self.checkFixed.isChecked():
            self.checkFixed.setChecked(False)

        self.main_engine.subscribe(req, gateway_name)

    def clear_label_text(self) -> None:
        """
        Clear text on all labels.
        """
        self.lp_label.setText("")
        self.return_label.setText("")

        self.bv1_label.setText("")
        self.bv2_label.setText("")
        self.bv3_label.setText("")
        self.bv4_label.setText("")
        self.bv5_label.setText("")

        self.av1_label.setText("")
        self.av2_label.setText("")
        self.av3_label.setText("")
        self.av4_label.setText("")
        self.av5_label.setText("")

        self.bp1_label.setText("")
        self.bp2_label.setText("")
        self.bp3_label.setText("")
        self.bp4_label.setText("")
        self.bp5_label.setText("")

        self.ap1_label.setText("")
        self.ap2_label.setText("")
        self.ap3_label.setText("")
        self.ap4_label.setText("")
        self.ap5_label.setText("")

    def stop_algo(self) -> None:

        if not self.main_engine.algo_engine:
            QtWidgets.QMessageBox.critical(self, "算法引擎未启动", "请先启动算法引擎")
            return
        try:
            self.main_engine.algo_engine.stop_all()
        except Exception as ex:
            QtWidgets.QMessageBox.critical(self, f"算法引擎异常{str(ex)}", "请查看详细日志")


    def send_algo(self) ->None:
        """启动算法"""
        if not self.main_engine.algo_engine:
            QtWidgets.QMessageBox.critical(self, "算法引擎未启动", "请先启动算法引擎")
            return
        template_name = 'AutoStopWinAlgo'
        algo_template = self.main_engine.algo_engine.algo_templates.get(template_name,None)
        if algo_template is None:
            QtWidgets.QMessageBox.critical(self, f"算法[{template_name}]不存在", "请先部署算法")
            return
        symbol = str(self.symbol_line.text())
        if not symbol:
            QtWidgets.QMessageBox.critical(self, "委托失败", "请输入合约代码")
            return

        volume_text = str(self.volume_line.text())
        if not volume_text:
            QtWidgets.QMessageBox.critical(self, "委托失败", "请输入委托数量")
            return
        volume = float(volume_text)

        price_text = str(self.price_line.text())
        if not price_text:
            price = 0
        else:
            price = float(price_text)

        exchange = Exchange(str(self.exchange_combo.currentText()))

        win_pips = str(self.win_pips.text())
        if int(win_pips) <=0:
            QtWidgets.QMessageBox.critical(self, "止盈点数须大于0", "请输入正确止盈点数")
            return
        stop_pips = str(self.stop_pips.text())
        if int(stop_pips) <= 0:
            QtWidgets.QMessageBox.critical(self, "止损点数须大于0", "请输入正确止损点数")
            return
        offset = Offset(str(self.offset_combo.currentText()))
        if offset != Offset.OPEN:
            QtWidgets.QMessageBox.critical(self, "算法只支持开仓", "请选择开仓方式")
            return

        setting = {
            "vt_symbol": f"{symbol}.{exchange.value}",
            "direction": Direction(str(self.direction_combo.currentText())),
            "open_price": price,
            "win_pips": int(win_pips),
            "stop_pips": int(stop_pips),
            "volume": volume,
            "near_pips": 2,  # 价格接近多少个跳动才开始挂单（开仓）
            "offset": offset
        }
        algo = algo_template.new(self.main_engine.algo_engine, setting)
        algo.start()
        self.main_engine.algo_engine.algos[algo.algo_name] = algo

        self.main_engine.write_log(msg=f'算法{algo.algo_name}启动')

    def send_order(self) -> None:
        """
        Send new order manually.
        """
        symbol = str(self.symbol_line.text())
        if not symbol:
            QtWidgets.QMessageBox.critical(self, "委托失败", "请输入合约代码")
            return

        volume_text = str(self.volume_line.text())
        if not volume_text:
            QtWidgets.QMessageBox.critical(self, "委托失败", "请输入委托数量")
            return
        volume = float(volume_text)

        price_text = str(self.price_line.text())
        if not price_text:
            price = 0
        else:
            price = float(price_text)

        req = OrderRequest(
            symbol=symbol,
            exchange=Exchange(str(self.exchange_combo.currentText())),
            direction=Direction(str(self.direction_combo.currentText())),
            type=OrderType(str(self.order_type_combo.currentText())),
            volume=volume,
            price=price,
            offset=Offset(str(self.offset_combo.currentText())),
        )

        gateway_name = str(self.gateway_combo.currentText())

        self.main_engine.send_order(req, gateway_name)

    def cancel_all(self) -> None:
        """
        Cancel all active orders.
        """
        order_list = self.main_engine.get_all_active_orders()
        for order in order_list:
            req = order.create_cancel_request()
            self.main_engine.cancel_order(req, order.gateway_name)

    def auto_fill_symbol(self, cell):
        """根据行情信息自动填写交易组件"""
        try:
            # 读取行情数据，cell是一个表格中的单元格对象
            tick = cell.get_data()
            if tick is None:
                return

            if tick.symbol:
                self.symbol_line.setText(tick.symbol)

            if tick.exchange:
                self.exchange_combo.setCurrentText(tick.exchange.value)

            # 自动填写gateway信息
            if tick.gateway_name:
                self.gateway_combo.setCurrentText(tick.gateway_name)

            self.volume_line.setText(str(1))

            self.set_vt_symbol()

        except Exception as ex:
            self.main_engine.write_log(u'tradingWg.autoFillSymbol exception:{}'.format(str(ex)))

    def close_position(self, cell):
        """根据持仓信息自动填写交易组件"""
        try:
            # 读取持仓数据，cell是一个表格中的单元格对象
            pos = cell.get_data()
            if pos is None:
                return
            if pos.symbol:
                self.symbol_line.setText(pos.symbol)

            if pos.exchange:
                self.exchange_combo.setCurrentText(pos.exchange.value)

            if pos.gateway_name:
                self.gateway_combo.setCurrentText(pos.gateway_name)

            self.set_vt_symbol()

            self.order_type_combo.setCurrentText(OrderType.LIMIT.value)

            self.offset_combo.setCurrentText(Offset.CLOSE.value)

            self.volume_line.setText(str(abs(pos.volume)))
            if pos.direction == Direction.NET:
                if pos.volume >= 0:
                    self.direction_combo.setCurrentText(Direction.SHORT.value)
                else:
                    self.direction_combo.setCurrentText(Direction.LONG.value)
            elif pos.direction == Direction.LONG:

                self.direction_combo.setCurrentText(Direction.SHORT.value)
            else:
                self.direction_combo.setCurrentText(Direction.LONG.value)

        except Exception as ex:
            self.main_engine.write_log(u'tradingWg.closePosition exception:{}'.format(str(ex)))


class ActiveOrderMonitor(OrderMonitor):
    """
    Monitor which shows active order only.
    """

    def process_event(self, event) -> None:
        """
        Hides the row if order is not active.
        """
        super(ActiveOrderMonitor, self).process_event(event)

        order = event.data
        row_cells = self.cells[order.vt_orderid]
        row = self.row(row_cells["volume"])

        if order.is_active():
            self.showRow(row)
        else:
            self.hideRow(row)


class ContractManager(QtWidgets.QWidget):
    """
    Query contract data available to trade in system.
    """

    headers: Dict[str, str] = {
        "vt_symbol": "本地代码",
        "symbol": "代码",
        "exchange": "交易所",
        "name": "名称",
        "product": "合约分类",
        "size": "合约乘数",
        "pricetick": "价格跳动",
        "min_volume": "最小委托量",
        "gateway_name": "交易接口",
    }

    def __init__(self, main_engine: MainEngine, event_engine: EventEngine):
        super().__init__()

        self.main_engine: MainEngine = main_engine
        self.event_engine: EventEngine = event_engine

        self.init_ui()

    def init_ui(self) -> None:
        """"""
        self.setWindowTitle("合约查询")
        self.resize(1000, 600)

        self.filter_line = QtWidgets.QLineEdit()
        self.filter_line.setPlaceholderText("输入合约代码或者交易所，留空则查询所有合约")

        self.button_show = QtWidgets.QPushButton("查询")
        self.button_show.clicked.connect(self.show_contracts)

        labels = []
        for name, display in self.headers.items():
            label = f"{display}\n{name}"
            labels.append(label)

        self.contract_table = QtWidgets.QTableWidget()
        self.contract_table.setColumnCount(len(self.headers))
        self.contract_table.setHorizontalHeaderLabels(labels)
        self.contract_table.verticalHeader().setVisible(False)
        self.contract_table.setEditTriggers(self.contract_table.NoEditTriggers)
        self.contract_table.setAlternatingRowColors(True)

        hbox = QtWidgets.QHBoxLayout()
        hbox.addWidget(self.filter_line)
        hbox.addWidget(self.button_show)

        vbox = QtWidgets.QVBoxLayout()
        vbox.addLayout(hbox)
        vbox.addWidget(self.contract_table)

        self.setLayout(vbox)

    def show_contracts(self) -> None:
        """
        Show contracts by symbol
        """
        flt = str(self.filter_line.text()).lower()

        all_contracts = self.main_engine.get_all_contracts()
        if flt:
            contracts = [
                contract for contract in all_contracts if flt in contract.vt_symbol.lower()
            ]
        else:
            contracts = all_contracts

        self.contract_table.clearContents()
        self.contract_table.setRowCount(len(contracts))

        for row, contract in enumerate(contracts):
            for column, name in enumerate(self.headers.keys()):
                value = getattr(contract, name)
                if isinstance(value, Enum):
                    cell = EnumCell(value, contract)
                else:
                    cell = BaseCell(value, contract)
                self.contract_table.setItem(row, column, cell)

        self.contract_table.resizeColumnsToContents()


class AboutDialog(QtWidgets.QDialog):
    """
    About VN Trader.
    """

    def __init__(self, main_engine: MainEngine, event_engine: EventEngine):
        """"""
        super().__init__()

        self.main_engine: MainEngine = main_engine
        self.event_engine: EventEngine = event_engine

        self.init_ui()

    def init_ui(self) -> None:
        """"""
        self.setWindowTitle(f"关于VN Trader")

        text = f"""
            By Traders, For Traders.


            License：MIT

            Website：www.vnpy.com
            Github：www.github.com/vnpy/vnpy

            """

        label = QtWidgets.QLabel()
        label.setText(text)
        label.setMinimumWidth(500)

        vbox = QtWidgets.QVBoxLayout()
        vbox.addWidget(label)
        self.setLayout(vbox)


class GlobalDialog(QtWidgets.QDialog):
    """
    Start connection of a certain gateway.
    """

    def __init__(self):
        """"""
        super().__init__()

        self.widgets: Dict[str, Any] = {}

        self.init_ui()

    def init_ui(self) -> None:
        """"""
        self.setWindowTitle("全局配置")
        self.setMinimumWidth(800)

        settings = copy(SETTINGS)
        settings.update(load_json(SETTING_FILENAME))

        # Initialize line edits and form layout based on setting.
        form = QtWidgets.QFormLayout()

        for field_name, field_value in settings.items():
            field_type = type(field_value)
            widget = QtWidgets.QLineEdit(str(field_value))

            form.addRow(f"{field_name} <{field_type.__name__}>", widget)
            self.widgets[field_name] = (widget, field_type)

        button = QtWidgets.QPushButton("确定")
        button.clicked.connect(self.update_setting)
        form.addRow(button)

        self.setLayout(form)

    def update_setting(self) -> None:
        """
        Get setting value from line edits and update global setting file.
        """
        settings = {}
        for field_name, tp in self.widgets.items():
            widget, field_type = tp
            value_text = widget.text()

            if field_type == bool:
                if value_text == "True":
                    field_value = True
                else:
                    field_value = False
            else:
                field_value = field_type(value_text)

            settings[field_name] = field_value

        QtWidgets.QMessageBox.information(
            self,
            "注意",
            "全局配置的修改需要重启VN Trader后才会生效！",
            QtWidgets.QMessageBox.Ok
        )

        save_json(SETTING_FILENAME, settings)
        self.accept()
