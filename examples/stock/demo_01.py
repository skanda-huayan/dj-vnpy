# flake8: noqa

# 示例代码
# 从本地股票数据加载，前复权，显示主图指标、副图指标、缠论

import os
import sys
import json

vnpy_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if vnpy_root not in sys.path:
    print(f'sys.path append({vnpy_root})')
    sys.path.append(vnpy_root)

os.environ["VNPY_TESTING"] = "1"

from vnpy.data.tdx.tdx_common import FakeStrategy
from vnpy.data.tdx.tdx_stock_data import *
from vnpy.component.cta_line_bar import CtaMinuteBar
from vnpy.trader.ui.kline.ui_snapshot import UiSnapshot
from vnpy.trader.ui import create_qapp
from vnpy.data.common import get_stock_bars

if __name__ == "__main__":

    # 创建一个假的策略
    t1 = FakeStrategy()

    # 股票代码.交易所
    vt_symbol = '000001.SZSE'
    # 数据周期
    bar_freq = '15m'
    # 一根bar代表的分钟数
    bar_interval = int(bar_freq.replace('m', ''))

    # 获取某个合约得的分时数据,周期是15分钟，返回数据类型是barData
    print('加载数据')
    bars, msg = get_stock_bars(vt_symbol=vt_symbol, freq=bar_freq,start_date='2021-03-01')

    # 创建一个15分钟bar的 kline对象
    setting = {}
    setting['name'] = f'{vt_symbol}_{bar_freq}'
    setting['bar_interval'] = bar_interval
    setting['para_ma1_len'] = 55  # 双均线
    setting['para_ma2_len'] = 89
    setting['para_macd_fast_len'] = 12  # 激活macd
    setting['para_macd_slow_len'] = 26
    setting['para_macd_signal_len'] = 9
    setting['para_active_chanlun'] = True  # 激活缠论
    setting['price_tick'] = 1
    setting['is_stock'] = True
    setting['underly_symbol'] = vt_symbol.split('.')[0]
    kline = CtaMinuteBar(strategy=t1, cb_on_bar=None, setting=setting)

    # 推送bar到kline中
    for bar in bars:
        kline.add_bar(bar, bar_is_completed=True, bar_freq=bar_interval)

    # 获取kline的切片数据
    data = kline.get_data()
    snapshot = {
        'strategy': "demo",
        'datetime': datetime.now(),
        "kline_names": [kline.name],
        "klines": {kline.name: data}}

    # 创建一个GUI界面应用app
    qApp = create_qapp()

    # 创建切片回放工具窗口
    ui = UiSnapshot()
    # 显示切片内容
    ui.show(snapshot_file="",
            d=snapshot)

    sys.exit(qApp.exec_())
