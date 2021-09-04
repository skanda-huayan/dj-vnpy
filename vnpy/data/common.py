import os
import pandas as pd
import numpy as np
from typing import Union, List
from datetime import datetime

# 所有股票的复权因子
STOCK_ADJUST_FACTORS = {}

def get_bardata_folder(data_folder: str) -> str:
    """
    如果data_folder为空白，就返回bar_data的目录
    :param data_folder:
    :return:
    """
    if len(data_folder) == 0 or not os.path.exists(data_folder):
        vnpy_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
        data_folder = os.path.abspath(os.path.join(vnpy_root, 'bar_data'))
    return data_folder

def get_stock_bars(vt_symbol:str,
                 freq: str = "1d",
                 start_date: str = "",
                 fq_type:str ="qfq") -> (List, str):
    """
    获取本地文件的股票bar数据
    :param vt_symbol:
    :param freq:
    :param start_date: 20180101 或者 2018-01-01
    :param fq_type: qfq:前复权；hfq:后复权; 空白:不复权
    :return:
    """
    # 获取未复权的bar dataframe数据
    df, err_msg = get_stock_raw_data(vt_symbol=vt_symbol, freq=freq, start_date=start_date)
    bars = []
    if len(err_msg) > 0 or df is None:
        return bars, err_msg

    if fq_type != "":
        from vnpy.data.stock.adjust_factor import get_all_adjust_factor
        STOCK_ADJUST_FACTORS = get_all_adjust_factor()
        adj_list = STOCK_ADJUST_FACTORS.get(vt_symbol, [])

        if len(adj_list) > 0:

            for row in adj_list:
                row.update({'dividOperateDate': row.get('dividOperateDate')[:10] + ' 09:30:00'})
            # list -> dataframe, 转换复权日期格式
            adj_data = pd.DataFrame(adj_list)
            adj_data["dividOperateDate"] = pd.to_datetime(adj_data["dividOperateDate"], format="%Y-%m-%d %H:%M:%S")
            adj_data = adj_data.set_index("dividOperateDate")
            # 调用转换方法，对open,high,low,close, volume进行复权, fore, 前复权， 其他，后复权
            df = stock_to_adj(df, adj_data, adj_type='fore' if fq_type == 'qfw' else 'back')

        from vnpy.trader.object import BarData
        from vnpy.trader.constant import Exchange
        symbol, exchange = vt_symbol.split('.')

        for dt, bar_data in df.iterrows():
            bar_datetime = dt  # - timedelta(seconds=bar_interval_seconds)

            bar = BarData(
                gateway_name='backtesting',
                symbol=symbol,
                exchange=Exchange(exchange),
                datetime=bar_datetime
            )
            if 'open' in bar_data:
                bar.open_price = float(bar_data['open'])
                bar.close_price = float(bar_data['close'])
                bar.high_price = float(bar_data['high'])
                bar.low_price = float(bar_data['low'])
            else:
                bar.open_price = float(bar_data['open_price'])
                bar.close_price = float(bar_data['close_price'])
                bar.high_price = float(bar_data['high_price'])
                bar.low_price = float(bar_data['low_price'])

            bar.volume = int(bar_data['volume']) if not np.isnan(bar_data['volume']) else 0
            bar.date = dt.strftime('%Y-%m-%d')
            bar.time = dt.strftime('%H:%M:%S')
            str_td = str(bar_data.get('trading_day', ''))
            if len(str_td) == 8:
                bar.trading_day = str_td[0:4] + '-' + str_td[4:6] + '-' + str_td[6:8]
            else:
                bar.trading_day = bar.date

            bars.append(bar)

        return bars, ""

def get_stock_raw_data(vt_symbol: str,
                 freq: str = "1d",
                 start_date: str = "",
                 bar_data_folder: str = "") -> (Union[pd.DataFrame, None], str):
    """
    获取本地bar_data下的 交易所/股票代码_时间周期.csv原始bar数据（未复权）
    :param vt_symbol: 600001.SSE 或 600001
    :param freq: 1m,5m, 15m, 30m, 1h, 1d
    :param start_date: 开始日期
    :param bar_data_folder: 强制指定bar_data所在目录
    :return: DataFrame, err_msg
    """
    symbol, exchange = vt_symbol.split('.')
    # 1分钟 csv文件路径
    csv_file = os.path.abspath(os.path.join(
        get_bardata_folder(bar_data_folder),
        exchange,
        f'{symbol}_{freq}.csv'))

    if not os.path.exists(csv_file):
        err_msg = f'{csv_file} 文件不存在，不能读取'
        return None, err_msg
    try:
        # 载入原始csv => dataframe
        df = pd.read_csv(csv_file)

        datetime_format = "%Y-%m-%d %H:%M:%S"
        # 转换时间，str =》 datetime
        df["datetime"] = pd.to_datetime(df["datetime"], format=datetime_format)
        # 使用'datetime'字段作为索引
        df.set_index("datetime", inplace=True)
        if len(start_date) > 0:
            if len(start_date) == 8:
                _format = '%Y%m%d'
            else:
                _format = '%Y-%m-%d'
            start_date = datetime.strptime(start_date, _format)
            df = df.loc[start_date:]

        return df, ""

    except Exception as ex:
        err_msg = f'读取异常:{str(ex)}'
        return None, err_msg


def stock_to_adj(raw_data: pd.DataFrame,
                 adj_data: pd.DataFrame,
                 adj_type: str) -> pd.DataFrame:
    """
    股票数据复权转换
    :param raw_data: 不复权数据
    :param adj_data:  复权记录 ( 从barstock下载的复权记录列表=》df）
    :param adj_type: 复权类型, fore, 前复权； back,后复权
    :return:
    """

    if adj_type == 'fore':
        adj_factor = adj_data["foreAdjustFactor"]
        adj_factor = adj_factor / adj_factor.iloc[-1]  # 保证最后一个复权因子是1
    else:
        adj_factor = adj_data["backAdjustFactor"]
        adj_factor = adj_factor / adj_factor.iloc[0]  # 保证第一个复权因子是1

    # 把raw_data的第一个日期，插入复权因子df，使用后填充
    if adj_factor.index[0] != raw_data.index[0]:
        adj_factor.loc[raw_data.index[0]] = np.nan
    adj_factor.sort_index(inplace=True)
    adj_factor = adj_factor.ffill()

    adj_factor = adj_factor.reindex(index=raw_data.index)  # 按价格dataframe的日期索引来扩展索引
    adj_factor = adj_factor.ffill()  # 向前（向未来）填充扩展后的空单元格

    # 把复权因子，作为adj字段，补充到raw_data中
    raw_data['adj'] = adj_factor

    # 逐一复权高低开平和成交量
    for col in ['open', 'high', 'low', 'close']:
        raw_data[col] = raw_data[col] * raw_data['adj']  # 价格乘上复权系数
    raw_data['volume'] = raw_data['volume'] / raw_data['adj']  # 成交量除以复权系数

    return raw_data


def resample_bars_file(vt_symbol: str,
                       x_mins: List[str] = [],
                       include_day: bool = False,
                       bar_data_folder: str = "") -> (list, str):
    """
    重建x分钟K线（和日线）csv文件
    :param vt_symbol: 代码.交易所
    :param x_mins: [5, 15, 30, 60]
    :param include_day: 是否也重建日线
    :param vnpy_root: 项目所在根目录
    :return: out_files,err_msg
    """
    err_msg = ""
    out_files = []
    symbol, exchange = vt_symbol.split('.')

    # 1分钟 csv文件路径
    csv_file = os.path.abspath(os.path.join(get_bardata_folder(bar_data_folder), exchange, f'{symbol}_1m.csv'))

    if not os.path.exists(csv_file):
        err_msg = f'{csv_file} 文件不存在，不能转换'
        return out_files, err_msg

    # 载入1分钟csv => dataframe
    df_1m = pd.read_csv(csv_file)

    datetime_format = "%Y-%m-%d %H:%M:%S"
    # 转换时间，str =》 datetime
    df_1m["datetime"] = pd.to_datetime(df_1m["datetime"], format=datetime_format)
    # 使用'datetime'字段作为索引
    df_1m.set_index("datetime", inplace=True)

    # 设置df数据中每列的规则
    ohlc_rule = {
        'open': 'first',  # open列：序列中第一个的值
        'high': 'max',  # high列：序列中最大的值
        'low': 'min',  # low列：序列中最小的值
        'close': 'last',  # close列：序列中最后一个的值
        'volume': 'sum',  # volume列：将所有序列里的volume值作和
        'amount': 'sum',  # amount列：将所有序列里的amount值作和
        "symbol": 'first',
        "trading_date": 'first',
        "date": 'first',
        "time": 'first',
        # "pre_close": 'first',
        # "turnover_rate": 'last',
        # "change_rate": 'last'
    }

    for x_min in x_mins:
        # 目标文件
        target_file = os.path.abspath(
            os.path.join(get_bardata_folder(bar_data_folder), exchange, f'{symbol}_{x_min}m.csv'))
        # 合成x分钟K线并删除为空的行 参数 closed：left类似向上取值既 09：30的k线数据是包含09：30-09：35之间的数据
        # df_target = df_1m.resample(f'{x_min}min', how=ohlc_rule, closed='left', label='left').dropna(axis=0, how='any')
        df_target = df_1m.resample(
            f'{x_min}min',
            closed='left',
            label='left').agg(ohlc_rule).dropna(axis=0,
                                                how='any')
        # dropna(axis=0, how='any') axis参数0：针对行进行操作 1：针对列进行操作  how参数any：只要包含就删除 all：全是为NaN才删除

        if len(df_target) > 0:
            df_target.to_csv(target_file)
            print(f'生成[{x_min}分钟] => {target_file}')
            out_files.append(target_file)

    if include_day:
        # 目标文件
        target_file = os.path.abspath(
            os.path.join(get_bardata_folder(bar_data_folder), exchange, f'{symbol}_1d.csv'))
        # 合成x分钟K线并删除为空的行 参数 closed：left类似向上取值既 09：30的k线数据是包含09：30-09：35之间的数据
        # df_target = df_1m.resample(f'D', how=ohlc_rule, closed='left', label='left').dropna(axis=0, how='any')
        df_target = df_1m.resample(
            f'D',
            closed='left',
            label='left').agg(ohlc_rule).dropna(axis=0, how='any')
        # dropna(axis=0, how='any') axis参数0：针对行进行操作 1：针对列进行操作  how参数any：只要包含就删除 all：全是为NaN才删除

        if len(df_target) > 0:
            df_target.to_csv(target_file)
            print(f'生成[日线] => {target_file}')
            out_files.append(target_file)

    return out_files, err_msg
