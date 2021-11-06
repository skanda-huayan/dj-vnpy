# flake8: noqa

# 示例代码
# 从本地bar_data目录下，读取某股票日线数据，前复权后，推送到K线，识别出其趋势线段、盘整线段、趋势背驰信号点，标注在图上

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
demo_06_dist = 'demo_06_dist.csv'


class DemoStrategy(FakeStrategy):
    # 输出csv的head
    dist_fieldnames = ['datetime', 'vt_symbol', 'volume', 'price',
                       'operation']

    def __init__(self, *args, **kwargs):

        super().__init__()

        # 最后一个执行检查的分笔开始位置
        self.last_check_bi = None

        # 最后一个检查线段
        self.last_found_duan = None

        # 最后一个判断类型（趋势、盘整）
        self.last_found_type = None

        # 最后一个判断背驰的分型信号
        self.last_found_beichi = None

        # 如果之前存在，移除
        if os.path.exists(demo_06_dist):
            self.write_log(f'移除{demo_06_dist}')
            os.remove(demo_06_dist)

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
        if self.kline.cur_duan is None or self.kline.cur_bi_zs is None:
            return

        cur_fx = self.kline.fenxing_list[-1]
        # 分型未结束，不做判断
        if cur_fx.is_rt:
            return

        # 当前段与上一次检查的段，开始时间不同，执行检查
        if self.kline.cur_duan.start != self.last_found_duan:

            # 判断条件：
            # 当前线段后出现一个分笔
            # 当前线段的结束时间比最后一个中枢晚
            if self.kline.cur_duan.end == self.kline.cur_bi.start \
                    and self.kline.cur_duan.end > self.kline.cur_bi_zs.end\
                    and self.kline.cur_bi.start != self.last_check_bi:

                # 判断是否盘整， 当前线段比上一线段短
                if self.kline.pre_duan and self.kline.pre_duan.height * 0.62 > self.kline.cur_duan.height:
                    # 获取该线段start -> end 之间，存在的中枢
                    zs_list = [zs for zs in self.kline.bi_zs_list if zs.start > self.kline.cur_duan.start and zs.end < self.kline.cur_duan.end]
                    # 存在的一个中枢
                    if len(zs_list) == 1:
                        # 记录该笔已经检查过
                        self.last_check_bi = self.kline.cur_bi.start

                        # 盘整名称，price位置
                        if self.kline.cur_duan.direction == 1:
                            self.last_found_type = '下跌盘整'
                            price = self.kline.cur_duan.high
                        else:
                            self.last_found_type = '上涨盘整'
                            price = self.kline.cur_duan.low

                        # 写入记录
                        append_data(file_name=demo_06_dist,
                                    field_names=self.dist_fieldnames,
                                    dict_data={
                                        'datetime': datetime.strptime(self.kline.cur_duan.end, '%Y-%m-%d %H:%M:%S'),
                                        'vt_symbol': self.vt_symbol,
                                        'volume': 0,
                                        'price': price,
                                        'operation': self.last_found_type
                                    })

                # 判断是否趋势
                if self.kline.is_contain_zs_inside_duan(direction=self.kline.cur_duan.direction,
                                                        cur_duan=self.kline.cur_duan,
                                                        zs_num=2):
                    # 记录检查的一笔
                    self.last_check_bi = self.kline.cur_bi.start
                    # 记录检查的线段
                    self.last_found_duan = self.kline.cur_duan.start

                    # 趋势名称、价格
                    if self.kline.cur_duan.direction == 1:
                        self.last_found_type = '上涨趋势'
                        price = self.kline.cur_duan.high
                    else:
                        self.last_found_type = '下跌趋势'
                        price = self.kline.cur_duan.low

                    # 记录数据
                    append_data(file_name=demo_06_dist,
                                field_names=self.dist_fieldnames,
                                dict_data={
                                    'datetime': datetime.strptime(self.kline.cur_duan.end, '%Y-%m-%d %H:%M:%S'),
                                    'vt_symbol': self.vt_symbol,
                                    'volume': 0,
                                    'price': price,
                                    'operation': self.last_found_type
                                })

        # 判断是否走势背驰
        if self.kline.is_zs_beichi_inside_duan(direction=self.kline.cur_duan.direction,
                                               cur_duan=self.kline.cur_duan):
            if cur_fx.index != self.last_found_beichi:
                self.last_found_beichi = cur_fx.index
                # 趋势名称、价格
                if self.kline.cur_duan.direction == 1:
                    self.last_found_type = '上涨背驰'
                    price = self.kline.cur_duan.high
                else:
                    self.last_found_type = '下跌背驰'
                    price = self.kline.cur_duan.low

                # 记录数据
                append_data(file_name=demo_06_dist,
                            field_names=self.dist_fieldnames,
                            dict_data={
                                'datetime': datetime.strptime(self.kline.cur_duan.end, '%Y-%m-%d %H:%M:%S'),
                                'vt_symbol': self.vt_symbol,
                                'volume': 0,
                                'price': price,
                                'operation': self.last_found_type
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
            dist_file=demo_06_dist,  # 本地dist csv文件
            dist_include_list=['上涨趋势','下跌趋势','上涨盘整','下跌盘整','上涨背驰','下跌背驰'])  # 指定输出的文字内容

    sys.exit(qApp.exec_())
