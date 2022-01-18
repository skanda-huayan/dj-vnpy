# -*- coding:UTF-8 -*-
# Author ：chenfeng
import traceback
from contextlib import closing

import os
from datetime import datetime, timedelta
from functools import lru_cache
from tqsdk import TqApi, TqSim,TqAuth
from vnpy.data.tq.downloader import DataDownloader
from vnpy.trader.constant import (
    Direction,
    Exchange,
    Product,
    Offset,
    Status,
    OptionType,
    OrderType,
    Interval,
)
from vnpy.trader.object import TickData, BarData
from vnpy.trader.utility import extract_vt_symbol, get_trading_date
import pandas as pd
import csv
from vnpy.data.tq.downloader import get_account_config

# pd.pandas.set_option('display.max_rows', None)  # 设置最大显示行数，超过该值用省略号代替，为None时显示所有行。
# pd.pandas.set_option('display.max_columns', None)  # 设置最大显示列数，超过该值用省略号代替，为None时显示所有列。
# pd.pandas.reset_option(‘参数名’, 参数值)	# 恢复默认相关选项

tick_csv_header = [
    "datetime", "symbol", "exchange", "last_price", "highest", "lowest", "volume", "amount", "open_interest",
    "upper_limit", "lower_limit", "bid_price1", "bid_volume1", "ask_price1",
    "ask_volume1", "bid_price2", "bid_volume2", "ask_price2", "ask_volume2",
    "bid_price3", "bid_volume3", "ask_price3", "ask_volume3", "bid_price4",
    "bid_volume4",
    "ask_price4", "ask_volume4",
    "bid_price5", "bid_volume5", "ask_price5", "ask_volume5"
]


@lru_cache(maxsize=9999)
def to_vt_symbol(tq_symbol: str) -> str:
    """"""
    if "KQ.m" in tq_symbol:
        ins_type, instrument = tq_symbol.split("@")
        exchange, symbol = instrument.split(".")
        return f"{symbol}88.{exchange}"
    elif "KQ.i" in tq_symbol:
        ins_type, instrument = tq_symbol.split("@")
        exchange, symbol = instrument.split(".")
        return f"{symbol}99.{exchange}"
    else:
        exchange, symbol = tq_symbol.split(".")
        return f"{symbol}.{exchange}"


@lru_cache(maxsize=9999)
def to_tq_symbol(symbol: str, exchange: Exchange) -> str:
    """
    TQSdk exchange first
    """
    count = 0
    for count, word in enumerate(symbol):
        if word.isdigit():
            break

    fix_symbol = symbol
    if exchange in [Exchange.INE, Exchange.SHFE, Exchange.DCE]:
        fix_symbol = symbol.lower()

    # Check for index symbol
    time_str = symbol[count:]

    if time_str in ["88"]:
        return f"KQ.m@{exchange.value}.{fix_symbol[:count]}"
    if time_str in ["99"]:
        return f"KQ.i@{exchange.value}.{fix_symbol[:count]}"

    return f"{exchange.value}.{fix_symbol}"


def generate_tick_from_dict(vt_symbol: str, data: dict) -> TickData:
    """
    生成TickData
    """
    symbol, exchange = extract_vt_symbol(vt_symbol)
    if '.' in data["datetime"]:
        time_format = "%Y-%m-%d %H:%M:%S.%f"
    else:
        time_format = "%Y-%m-%d %H:%M:%S"

    return TickData(
        symbol=symbol,
        exchange=exchange,
        datetime=datetime.strptime(data["datetime"][0:26], time_format),
        name=symbol,
        volume=int(data["volume"]),
        open_interest=data["open_interest"],
        last_price=float(data["last_price"]),
        # limit_up=float(data["upper_limit"]) if data["upper_limit"] !='#N/A' else None,
        # limit_down=float(data["lower_limit"]),
        high_price=float(data["highest"]),
        low_price=float(data["lowest"]),
        bid_price_1=float(data["bid_price1"]),
        bid_price_2=float(data["bid_price2"]),
        bid_price_3=float(data["bid_price3"]),
        bid_price_4=float(data["bid_price4"]),
        bid_price_5=float(data["bid_price5"]),
        ask_price_1=float(data["ask_price1"]),
        ask_price_2=float(data["ask_price2"]),
        ask_price_3=float(data["ask_price3"]),
        ask_price_4=float(data["ask_price4"]),
        ask_price_5=float(data["ask_price5"]),
        bid_volume_1=int(data["bid_volume1"]),
        bid_volume_2=int(data["bid_volume2"]),
        bid_volume_3=int(data["bid_volume3"]),
        bid_volume_4=int(data["bid_volume4"]),
        bid_volume_5=int(data["bid_volume5"]),
        ask_volume_1=int(data["ask_volume1"]),
        ask_volume_2=int(data["ask_volume2"]),
        ask_volume_3=int(data["ask_volume3"]),
        ask_volume_4=int(data["ask_volume4"]),
        ask_volume_5=int(data["ask_volume5"]),
        gateway_name='',
    )


class TqFutureData():

    def __init__(self, strategy=None):
        self.strategy = strategy  # 传进来策略实例，这样可以写日志到策略实例
        auth_dict = get_account_config()

        self.api = TqApi(TqSim(),auth=TqAuth(auth_dict['user_name'],auth_dict['password']))  # url="wss://u.shinnytech.com/t/md/front/mobile"

    def get_tick_serial(self, vt_symbol: str):
        # 获取最新的8964个数据 tick的话就相当于只有50分钟左右
        try:
            symbol, exchange = extract_vt_symbol(vt_symbol)
            tq_symbol = to_tq_symbol(symbol, exchange)
            # 使用with closing机制确保下载完成后释放对应的资源
            with closing(self.api):
                # 获得 pp2009 tick序列的引用
                ticks = self.api.get_tick_serial(symbol=tq_symbol, data_length=8964)  # 每个序列最大支持请求 8964 个数据
                return ticks  # 8964/3/60=49.8分钟
        except Exception as ex:
            print(u'获取历史tick数据出错：{},{}'.format(str(ex), traceback.format_exc()))
            return None

    def download_tick_history_to_csv(self, vt_symbol: str, cache_file: str, start_date: datetime, end_date: datetime):

        symbol, exchange = extract_vt_symbol(vt_symbol)
        tq_symbol = to_tq_symbol(symbol, exchange)
        td = DataDownloader(self.api, symbol_list=tq_symbol, dur_sec=0,  # Tick数据为dur_sec=0
                            start_dt=start_date, end_dt=end_date,
                            csv_file_name=cache_file)

        # 使用with closing机制确保下载完成后释放对应的资源
        # with closing(self.api):         # 不能这样关闭，套利要下两个腿，所以在策略中关闭
        #     while not td.is_finished():
        #         self.api.wait_update()
        #         print(f"progress:{vt_symbol}--{start_date}--{end_date}: {td.get_progress()}")
        # self.write_error(f"{vt_symbol}--{start_date}--{end_date}历史数据已经下载到csv")
        while not td.is_finished():
            self.api.wait_update()
            self.write_log(f"progress:{vt_symbol}--{start_date}--{end_date}: {td.get_progress()}")
        self.write_log(f"{vt_symbol}--{start_date}--{end_date}历史数据已经下载到csv")

    def close_api(self):
        # 关闭api,释放资源    download_tick_history_to_csv 中因为要下多个所以这里手动关闭
        self.api.close()

    def get_tick_from_cache(self, vt_symbol: str, trading_day: str):
        """从本地缓存文件读取， 返回[]"""
        if '-' in trading_day:
            trading_day = trading_day.replace('-', '')
        symbol, exchange = extract_vt_symbol(vt_symbol)

        vnpy_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
        ticks_file = os.path.abspath(os.path.join(vnpy_folder, 'tick_data', 'tq', 'future', trading_day[0:6],
                                                  f'{symbol}_{trading_day}.csv'))
        tick_dict_list = []
        if os.path.exists(ticks_file):
            try:
                with open(file=ticks_file, mode='r', encoding='utf-8', ) as f:
                    reader = csv.DictReader(f=f, fieldnames=tick_csv_header, delimiter=",")
                    for row in reader:
                        if str(row.get('last_price', 'nan')) not in ['nan', 'last_price']:
                            tick_dict_list.append(row)

                return tick_dict_list
            except Exception as ex:
                self.write_log(f'从缓存文件读取{vt_symbol}，交易日{trading_day}异常：{str(ex)}')

        return []

    def get_bars(self, vt_symbol: str, start_date: datetime = None, end_date: datetime = None):
        """
        获取历史bar（受限于最大长度8964根bar）
        :param vt_symbol:
        :param start_date:
        :param end_date:
        :return:
        """

        self.write_log(f"从天勤请求合约:{vt_symbol}开始时间：{start_date}的历史1分钟bar数据")
        symbol, exchange = extract_vt_symbol(vt_symbol)

        # 获取一分钟数据
        df = self.api.get_kline_serial(symbol=f'{exchange.value}.{symbol}', duration_seconds=60, data_length=8964)
        bars = []
        if df is None:
            self.write_error(f'返回空白dataframe')
            return []

        for index, row in df.iterrows():
            bar_datetime = datetime.strptime(self._nano_to_str(row['datetime']), "%Y-%m-%d %H:%M:%S.%f")
            if start_date:
                if bar_datetime < start_date:
                    continue
            if end_date:
                if bar_datetime > end_date:
                    continue
            bar = BarData(
                symbol=symbol,
                exchange=exchange,
                datetime=bar_datetime,
                open_price=row['open'],
                close_price=row['close'],
                high_price=row['high'],
                low_price=row['low'],
                volume=row['volume'],
                open_interest=row['close_oi'],
                trading_day=get_trading_date(bar_datetime),
                gateway_name='tq'
            )
            bars.append(bar)

        return bars

    def get_ticks(self, vt_symbol: str, start_date: datetime, end_date: datetime = None):
        """获取历史tick"""

        # 1.0从天勤接口下载指定日期，的合约的tick数据
        self.write_log(f"从天勤请求合约:{vt_symbol}开始时间：{start_date}的历史tick数据")
        symbol, exchange = extract_vt_symbol(vt_symbol)

        if end_date is None:
            end_date = datetime.now().replace(hour=16)

        n_days = (end_date - start_date).days

        if n_days <= 0:
            n_days = 1

        all_ticks = []
        # 轮询每一天，读取缓存数据
        for n in range(n_days + 1):
            trading_date = start_date + timedelta(days=n)
            if trading_date.isoweekday() in [6, 7]:
                continue
            trading_day = trading_date.strftime('%Y%m%d')
            day_ticks = self.get_tick_from_cache(vt_symbol=vt_symbol, trading_day=trading_day)

            if day_ticks:
                self.write_log(f'读取{vt_symbol} {trading_day}缓存数据{len(day_ticks)}条')
                all_ticks.extend(day_ticks)

        if all_ticks:
            last_tick_dt = all_ticks[-1].get('datetime')
            begin_dt = datetime.strptime(last_tick_dt[0:26], "%Y-%m-%d %H:%M:%S.%f")
            rt_ticks = self.get_runtime_ticks(vt_symbol=vt_symbol, begin_dt=begin_dt)
            if rt_ticks:
                all_ticks.extend(rt_ticks)
        return all_ticks

    def get_runtime_ticks(self, vt_symbol: str, begin_dt: datetime = None):
        """获取实时历史tick"""
        self.write_log(f"从天勤请求合约:{vt_symbol}的实时的8964条tick数据")
        symbol, exchange = extract_vt_symbol(vt_symbol)
        df = self.get_tick_serial(vt_symbol)
        ticks = []
        if df is None:
            return ticks

        self.write_log(f"从天勤或历史tick数据成功，开始清洗tick")
        # print(df.columns.values)
        # 给df 的各个列名按vnpy格式重置一下
        df.columns = ['datetime', 'id', 'last_price', 'average', 'highest', 'lowest', 'ask_price1',
                      'ask_volume11', 'bid_price1', 'bid_volume11', 'ask_price2', 'ask_volume12',
                      'bid_price2', 'bid_volume12', 'ask_price3', 'ask_volume13', 'bid_price3',
                      'bid_volume13', 'ask_price4', 'ask_volume14', 'bid_price4', 'bid_volume14',
                      'ask_price5', 'ask_volume15', 'bid_price5', 'bid_volume15', 'volume', 'amount',
                      'open_interest', 'symbol', 'duration']
        df.drop(['id', 'average', 'duration'], axis=1)

        for index, row in df.iterrows():
            # 日期时间, 成交价, 成交量, 总量, 属性(持仓增减), B1价, B1量, B2价, B2量, B3价, B3量, S1价, S1量, S2价, S2量, S3价, S3量, BS
            # 日期时间, 成交价,当日最高价,当日最低价, B1价, B1量，S1价, S1量，日内成交量，金额，持仓量
            #     0       1        2          3       4      5       6    7       8        9     10
            tick = row.to_dict()

            if str(tick['last_price']) == 'nan':
                continue
            # datetime: 自unix epoch(1970-01-01 00:00:00 GMT)以来的纳秒数
            # 1.0、转换读取的tick 时间文本 到 datetime格式
            # tick_datetime = datetime.strptime(tick['datetime'], "%Y-%m-%d %H:%M:%S.%f")
            tick_datetime = datetime.strptime(self._nano_to_str(tick['datetime']), "%Y-%m-%d %H:%M:%S.%f")
            if tick_datetime <= begin_dt:
                continue
            # 2.0、获取tick对应的交易日
            tick_tradingday = get_trading_date(tick_datetime)

            tick.update({'symbol': symbol, 'exchange': exchange.value, 'trading_day': tick_tradingday})
            tick['datetime'] = tick_datetime.strftime("%Y-%m-%d %H:%M:%S.%f")
            ticks.append(tick)

        del df
        return ticks

    @staticmethod
    def _nano_to_str(nano):
        # nano: 自unix epoch(1970-01-01 00:00:00 GMT)以来的纳秒数  9位为纳秒 6位为微秒，%f只用到微秒，所以[:6]
        dt = datetime.fromtimestamp(nano // 1000000000)
        s = dt.strftime('%Y-%m-%d %H:%M:%S')
        s += '.' + str(int(nano % 1000000000)).zfill(9)[:3]  # zfill() 方法返回指定长度的字符串，原字符串右对齐，前面填充0。
        return s

    def write_log(self, msg):
        if self.strategy is None:
            print(msg)
        else:
            self.strategy.write_log(msg)

    def write_error(self, msg):
        if self.strategy is None:
            print(msg)
        else:
            self.strategy.write_error(msg)


if __name__ == '__main__':
    # tqsdk = Query_tqsdk_data(strategy=self)   # 在策略中使用
    tqsdk = TqFutureData()
    # ticks = tqsdk.query_tick_current("pp2009.DCE")
    # tick_df = tqsdk.query_tick_history_data(vt_symbol="ni2009.SHFE", start_date=pd.to_datetime("2020-07-22"))
    # print(tick_df)

    # ticks = tqsdk.get_runtime_ticks("ni2009.SHFE")

    # print(ticks[0])

    # print(ticks[-1])
    bars = tqsdk.get_bars(vt_symbol='ni2011.SHFE')
    print(bars[0])
    print(bars[-1])
