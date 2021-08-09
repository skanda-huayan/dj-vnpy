import os
import pandas as pd


def resample_bars_file(vnpy_root, symbol, exchange, x_mins=[], include_day=False):
    """
    重建x分钟K线（和日线）csv文件
    :param symbol:
    :param x_mins: [5, 15, 30, 60]
    :param include_day: 是否也重建日线
    :return: out_files,err_msg
    """
    err_msg = ""
    out_files = []

    # 1分钟 csv文件路径
    csv_file = os.path.abspath(os.path.join(vnpy_root, 'bar_data', exchange.value, f'{symbol}_1m.csv'))

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
            os.path.join(vnpy_root, 'bar_data', exchange.value, f'{symbol}_{x_min}m.csv'))
        # 合成x分钟K线并删除为空的行 参数 closed：left类似向上取值既 09：30的k线数据是包含09：30-09：35之间的数据
        #df_target = df_1m.resample(f'{x_min}min', how=ohlc_rule, closed='left', label='left').dropna(axis=0, how='any')
        df_target = df_1m.resample(f'{x_min}min', closed='left', label='left').agg(ohlc_rule).dropna(axis=0,
                                                                                      how='any')
        # dropna(axis=0, how='any') axis参数0：针对行进行操作 1：针对列进行操作  how参数any：只要包含就删除 all：全是为NaN才删除

        if len(df_target) > 0:
            df_target.to_csv(target_file)
            print(f'生成[{x_min}分钟] => {target_file}')
            out_files.append(target_file)

    if include_day:
        # 目标文件
        target_file = os.path.abspath(
            os.path.join(vnpy_root, 'bar_data', exchange.value, f'{symbol}_1d.csv'))
        # 合成x分钟K线并删除为空的行 参数 closed：left类似向上取值既 09：30的k线数据是包含09：30-09：35之间的数据
        # df_target = df_1m.resample(f'D', how=ohlc_rule, closed='left', label='left').dropna(axis=0, how='any')
        df_target = df_1m.resample(f'D', closed='left', label='left').agg(ohlc_rule).dropna(axis=0, how='any')
        # dropna(axis=0, how='any') axis参数0：针对行进行操作 1：针对列进行操作  how参数any：只要包含就删除 all：全是为NaN才删除

        if len(df_target) > 0:
            df_target.to_csv(target_file)
            print(f'生成[日线] => {target_file}')
            out_files.append(target_file)

    return out_files,err_msg
