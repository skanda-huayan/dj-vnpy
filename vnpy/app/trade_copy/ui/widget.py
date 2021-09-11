from datetime import datetime

from vnpy.event import Event, EventEngine
from vnpy.trader.engine import MainEngine
from vnpy.trader.ui import QtCore, QtWidgets, QtGui

from vnpy.trader.ui.widget import BaseCell, EnumCell, PnlCell, BaseMonitor, DirectionCell

from ..engine import (
    APP_NAME,
    EVENT_TRADECOPY_LOG,
    EVENT_TRADECOPY
)


class PositionCopyMonitor(BaseMonitor):
    """
    Monitor for position copy data.
    """

    event_type = EVENT_TRADECOPY
    data_key = "vt_positionid"
    sorting = True

    headers = {
        "name": {"display": "名称", "cell": BaseCell, "update": False},
        "symbol": {"display": "代码", "cell": BaseCell, "update": False},
        "direction": {"display": "方向", "cell": DirectionCell, "update": False},
        "src_volume": {"display": "源数量", "cell": BaseCell, "update": True},
        "target_volume": {"display": "目标", "cell": BaseCell, "update": True},
        "volume": {"display": "数量", "cell": BaseCell, "update": True},
        "yd_volume": {"display": "昨仓", "cell": BaseCell, "update": True},
        "diff": {"display": "偏差", "cell": PnlCell, "update": True},
        "cur_price": {"display": "当前价", "cell": BaseCell, "update": True},
        "price": {"display": "均价", "cell": BaseCell, "update": True},

    }

    def process_event(self, event: Event):
        super().process_event(event)


class TcManager(QtWidgets.QWidget):
    """跟单应用界面"""
    # qt日志事件
    signal_log = QtCore.pyqtSignal(Event)

    default_source_rpc_rep = 'tcp://localhost:2015'
    default_source_rpc_pub = 'tcp://localhost:2018'
    default_copy_ratio = 1
    default_copy_interval = 5

    def __init__(self, main_engine: MainEngine, event_engine: EventEngine):
        super().__init__()

        self.main_engine = main_engine
        self.event_engine = event_engine

        self.tc_engine = self.main_engine.get_engine(APP_NAME)

        self.init_ui()
        self.register_event()

    def init_ui(self):
        """"""
        self.setWindowTitle("跟单应用")
        self.resize(1098, 800)

        # 创建组件
        # 源帐号得rpc rep地址
        self.line_rep_addr = QtWidgets.QLineEdit(self.tc_engine.source_rpc_rep)
        # 源帐号得rpc pub地址
        self.line_pub_addr = QtWidgets.QLineEdit(self.tc_engine.source_rpc_pub)

        # 跟单比率
        validator = QtGui.QDoubleValidator()
        validator.setBottom(0)
        self.line_copy_ratio = QtWidgets.QLineEdit()
        self.line_copy_ratio.setValidator(validator)
        self.line_copy_ratio.setText(str(self.tc_engine.copy_ratio))

        # 更新频率
        validator2 = QtGui.QIntValidator()
        validator2.setBottom(1)
        self.line_interval = QtWidgets.QLineEdit()
        self.line_interval.setValidator(validator2)
        self.line_interval.setText(str(self.tc_engine.copy_interval))

        # 跟单按钮动作
        self.btn_start_copy = QtWidgets.QPushButton(u'连接&跟单')
        self.btn_start_copy.clicked.connect(self.start_copy)

        # 停止动作
        self.btn_stop_engine = QtWidgets.QPushButton(u'停止')
        self.btn_stop_engine.clicked.connect(self.stop_copy)
        self.btn_stop_engine.setEnabled(False)

        # 重置设置
        self.btn_reset_addr = QtWidgets.QPushButton(u'重置配置')
        self.btn_reset_addr.clicked.connect(self.reset_setting)

        # 仓位差异组件
        self.pos_monitor = PositionCopyMonitor(self.main_engine, self.event_engine)

        # 日志组件
        self.log_monitor = QtWidgets.QTextEdit()
        self.log_monitor.setReadOnly(True)

        self.widgetList = [
            self.line_copy_ratio,
            self.line_interval,
            self.line_rep_addr,
            self.line_pub_addr,
            self.btn_stop_engine,
            self.btn_start_copy,
            self.btn_reset_addr
        ]

        # 布局
        QLabel = QtWidgets.QLabel
        grid = QtWidgets.QGridLayout()
        grid.addWidget(QLabel(u'响应地址'), 0, 0)
        grid.addWidget(self.line_rep_addr, 0, 1)

        grid.addWidget(QLabel(u'发布地址'), 1, 0)
        grid.addWidget(self.line_pub_addr, 1, 1)

        grid.addWidget(QLabel(u'发布间隔（秒）'), 0, 2)
        grid.addWidget(self.line_interval, 0, 3)

        grid.addWidget(QLabel(u'复制比例（倍）'), 1, 2)
        grid.addWidget(self.line_copy_ratio, 1, 3)

        grid.addWidget(self.btn_start_copy, 2, 2, 1, 2)
        grid.addWidget(self.btn_stop_engine, 2, 0, 1, 2)
        grid.addWidget(self.btn_reset_addr, 3, 2, 1, 2)
        grid.addWidget(self.pos_monitor, 4, 0, 1, 4)
        grid.addWidget(self.log_monitor, 5, 0, 1, 4)

        self.setLayout(grid)

    def register_event(self):
        """注册事件绑定"""
        # qt信号 => 日志更新函数()
        self.signal_log.connect(self.process_log_event)

        self.event_engine.register(EVENT_TRADECOPY_LOG, self.signal_log.emit)

    def process_log_event(self, event: Event):
        """处理日志"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        msg = f"{timestamp}\t{event.data}"
        self.log_monitor.append(msg)

    def reset_setting(self):
        """
        重置配置
        :return:
        """
        self.line_rep_addr.setText(self.default_source_rpc_pub)
        self.line_pub_addr.setText(self.default_source_rpc_pub)
        self.line_copy_ratio.setText(self.default_copy_ratio)
        self.line_interval.setText(self.default_copy_interval)

    def start_copy(self):
        """
        连接源帐号（RPC）
        :return:
        """

        source_req_addr = str(self.line_rep_addr.text())
        source_pub_addr = str(self.line_pub_addr.text())
        copy_ratio = float(self.line_copy_ratio.text())
        copy_interval = float(self.line_interval.text())
        self.tc_engine.start_copy(source_req_addr, source_pub_addr, copy_ratio, copy_interval)

        for widget in self.widgetList:
            widget.setEnabled(False)
        self.btn_stop_engine.setEnabled(True)

    def stop_copy(self):

        if self.tc_engine:
            self.tc_engine.stop_copy()
