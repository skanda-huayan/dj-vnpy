# flake8: noqa

# 示例代码
# 从本地bar_data目录下，读取某股票日线数据，前复权后，推送到K线，识别出顶、底分型，并识别出其强弱，在UI界面上展示出来

import os
import sys
import json
from datetime import datetime

vnpy_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if vnpy_root not in sys.path:
    print(f'sys.path append({vnpy_root})')
    sys.path.append(vnpy_root)

os.environ["VNPY_TESTING"] = "1"

from vnpy.data.tdx.tdx_common import FakeStrategy
from vnpy.component.cta_line_bar import CtaDayBar
from vnpy.trader.ui.kline.ui_snapshot import UiSnapshot
from vnpy.trader.utility import append_data
from vnpy.trader.ui import create_qapp
from vnpy.data.common import get_stock_bars

# 本示例中，输出的dist文件,主要用于图形显示一些逻辑
demo_03_dist = 'demo_03_dist.csv'


class DemoStrategy(FakeStrategy):
    # 输出csv的head
    dist_fieldnames = ['datetime', 'vt_symbol', 'volume', 'price',
                       'operation']

    def __init__(self, *args, **kwargs):

        super().__init__()

        # 最后一个找到的符合要求的分型index
        self.last_found_fx = None

        # 如果之前存在，移除
        if os.path.exists(demo_03_dist):
            self.write_log(f'移除{demo_03_dist}')
            os.remove(demo_03_dist)

        self.vt_symbol = kwargs.get('vt_symbol')

        # 创建一个日线bar的 kline对象
        setting = {}
        setting['name'] = f'{self.vt_symbol}_D1'
        setting['bar_interval'] = 1
        setting['para_ma1_len'] = 55  # 双均线
        setting['para_ma2_len'] = 89
        setting['para_macd_fast_len'] = 12  # 激活macd
        setting['para_macd_slow_len'] = 26
        setting['para_macd_signal_len'] = 9
        setting['para_active_chanlun'] = True  # 激活缠论
        setting['is_stock'] = True
        setting['price_tick'] = 1
        setting['underly_symbol'] = self.vt_symbol.split('.')[0]
        self.kline = CtaDayBar(strategy=self, cb_on_bar=self.on_bar, setting=setting)

    def on_bar(self, *args, **kwargs):
        """
        重构on_bar函数，实现demo的分型强弱判断
        :param args:
        :param kwargs:
        :return:
        """
        # 至少要有分型
        if len(self.kline.fenxing_list) == 0:
            return

        cur_fx = self.kline.fenxing_list[-1]

        # 如果的分型已经处理过了，就不再计算
        if cur_fx.index == self.last_found_fx:
            return

        # 分型是非实时的，已经走完的
        if cur_fx.is_rt:
            return

        # 分型前x根bar
        pre_bars = [bar for bar in self.kline.line_bar[-10:] if
                    bar.datetime.strftime('%Y-%m-%d %H:%M:%S') < cur_fx.index]

        if len(pre_bars) == 0:
            return
        pre_bar = pre_bars[-1]

        # 分型后x根bar
        extra_bars = \
            [bar for bar in self.kline.line_bar[-10:] if bar.datetime.strftime('%Y-%m-%d %H:%M:%S') > cur_fx.index]

        # 分型后，有三根bar
        if len(extra_bars) < 3:
            return

        # 处理顶分型
        if cur_fx.direction == 1:
            # 顶分型后第一根bar的低点，没有超过前bar的低点
            if extra_bars[0].low_price >= pre_bar.low_price:
                return

            # 找到正确形态，第二、第三根bar，都站在顶分型之下
            if pre_bar.low_price >= extra_bars[1].high_price > extra_bars[2].high_price:
                self.last_found_fx = cur_fx.index
                append_data(file_name=demo_03_dist,
                            field_names=self.dist_fieldnames,
                            dict_data={
                                'datetime': extra_bars[-1].datetime,
                                'vt_symbol': self.vt_symbol,
                                'volume': 0,
                                'price': extra_bars[-1].high_price,
                                'operation': '强顶分'
                            })

        # 处理底分型
        if cur_fx.direction == -1:
            # 底分型后第一根bar的高点，没有超过前bar的高点
            if extra_bars[0].high_price <= pre_bar.high_price:
                return

            # 找到正确形态，第二、第三根bar，都站在底分型之上
            if pre_bar.high_price <= extra_bars[1].low_price < extra_bars[2].low_price:
                self.last_found_fx = cur_fx.index
                append_data(file_name=demo_03_dist,
                            field_names=self.dist_fieldnames,
                            dict_data={
                                'datetime': extra_bars[-1].datetime,
                                'vt_symbol': self.vt_symbol,
                                'volume': 0,
                                'price': extra_bars[-1].low_price,
                                'operation': '强底分'
                            })


if __name__ == '__main__':

    # 股票代码.交易所
    vt_symbol = '600000.SSE'

    t1 = DemoStrategy(vt_symbol=vt_symbol)

    # 获取股票得日线数据,返回数据类型是barData
    print('加载数据')
    bars, msg = get_stock_bars(vt_symbol=vt_symbol, freq='1d', start_date='2019-01-01')

    if len(msg) > 0:
        print(msg)
        sys.exit(0)

    display_month = None
    # 推送bar到kline中
    for bar in bars:
        if bar.datetime.month != display_month:
            t1.write_log(f'推送:{bar.datetime.year}年{bar.datetime.month}月数据')
            display_month = bar.datetime.month
        t1.kline.add_bar(bar, bar_is_completed=True)

    # 获取kline的切片数据
    data = t1.kline.get_data()
    # 暂时不显示段、中枢等
    data.pop('duan_list', None)
    data.pop('bi_zs_list', None)
    data.pop('duan_zs_list', None)

    snapshot = {
        'strategy': "demo",
        'datetime': datetime.now(),
        "kline_names": [t1.kline.name],
        "klines": {t1.kline.name: data}}

    # 创建一个GUI界面应用app
    qApp = create_qapp()

    # 创建切片回放工具窗口
    ui = UiSnapshot()

    # 显示切片内容
    ui.show(snapshot_file="",
            d=snapshot,  # 切片数据
            dist_file=demo_03_dist, # 本地dist csv文件
            dist_include_list=['强底分','强顶分'])  # 指定输出的文字内容

    sys.exit(qApp.exec_())
