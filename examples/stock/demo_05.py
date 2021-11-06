# flake8: noqa

# 示例代码
# 从本地bar_data目录下，读取某股票日线数据，前复权后，推送到K线，识别出其中枢类型，标注在图上

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
demo_05_dist = 'demo_05_dist.csv'


class DemoStrategy(FakeStrategy):
    # 输出csv的head
    dist_fieldnames = ['datetime', 'vt_symbol', 'volume', 'price',
                       'operation']

    def __init__(self, *args, **kwargs):

        super().__init__()

        # 最后一个找到的符合要求的分笔位置
        self.last_found_bi = None

        # 最后一个处理得中枢开始位置
        self.last_found_zs = None

        # 最后一个中枢得判断类型
        self.last_found_type = None

        # 如果之前存在，移除
        if os.path.exists(demo_05_dist):
            self.write_log(f'移除{demo_05_dist}')
            os.remove(demo_05_dist)

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
        重构on_bar函数，实现demo的判断逻辑
        :param args:
        :param kwargs:
        :return:
        """
        if self.kline.cur_duan is None:
            return

        if self.kline.cur_bi_zs is None:
            return

        # 当前笔的start == 上一次判断过得
        if self.kline.cur_bi.start == self.last_found_bi:
            return

        # 当前笔中枢与上一个笔中枢得开始不同，可能是个新得笔中枢
        if self.kline.cur_bi_zs.start != self.last_found_zs:
            # 设置为最新判断中枢
            self.last_found_zs = self.kline.cur_bi_zs.start
            # 设置中枢得最后一笔开始时间，为最新判断时间
            self.last_found_bi = self.kline.cur_bi_zs.bi_list[-1].start
            # 设置中枢得类型为None
            self.last_found_type = None
            return

        # K线最后一笔得开始 = 中枢最后一笔得结束
        if self.kline.cur_bi.start == self.kline.cur_bi_zs.bi_list[-1].end:
            # 获得类型
            zs_type = self.kline.cur_bi_zs.get_type()

            # 记录下，这一笔已经执行过判断了
            self.last_found_bi = self.kline.cur_bi.start

            # 不一致时，才写入
            if zs_type != self.last_found_type:
                self.last_found_type = zs_type
                append_data(file_name=demo_05_dist,
                            field_names=self.dist_fieldnames,
                            dict_data={
                                'datetime': self.kline.cur_datetime,
                                'vt_symbol': self.vt_symbol,
                                'volume': 0,
                                'price': self.kline.cur_bi_zs.low,
                                'operation': zs_type
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
            dist_file=demo_05_dist,  # 本地dist csv文件
            dist_include_list=['close','enlarge','balance','attact', 'defend'])  # 指定输出的文字内容

    sys.exit(qApp.exec_())
