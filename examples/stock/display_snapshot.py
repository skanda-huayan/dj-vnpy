# flake8: noqa

# 示例代码
# 从策略保存K线数据中，读取某一K线，并显示

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

snapshot_file_name = 'prod/stock_pb/data/stock_clone_value_klines.pkb2'
kline_name = '601012.SSE_D1'
def get_klines(pkb2_files):
    """
    从缓存加载K线数据
    :param kline_names: 指定需要加载的k线名称列表
    :param vt_symbol: 指定股票代码,
        如果使用该选项，加载 data/klines/strategyname_vtsymbol_klines.pkb2
        如果空白，加载 data/strategyname_klines.pkb2
    :return:
    """
    if not os.path.exists(pkb2_files):
        return {}

    try:
        with bz2.BZ2File(pkb2_files, 'rb') as f:
            klines = pickle.load(f)

            return klines
    except Exception as ex:
        print(f'加载缓存K线数据失败:{str(ex)}')
    return {}

if __name__ == "__main__":

    # 创建一个假的策略
    t1 = FakeStrategy()

    file_name = os.path.abspath(os.path.join(vnpy_root,snapshot_file_name))
    klines = get_klines(file_name)

    print(f'kline names:{klines.keys()}')

    kline = klines.get(kline_name,None)
    if kline:
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
        ui.export(snapshot_file="",
                d=snapshot,
                export_file='s.png')
        #sys.exit(qApp.exec_())
    else:
        print(f'not found {kline_name}')
