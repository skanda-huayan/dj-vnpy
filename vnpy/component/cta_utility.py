# K线常用得方法：
# 缠论得一些形态识别
# 部分代码来自 github czsc

from vnpy.trader.constant import ChanSignals, Direction
from vnpy.component.chanlun.pyChanlun import ChanBi, ChanDuan, ChanObject
from vnpy.component.cta_line_bar import CtaLineBar
from typing import List

# 所有底背驰信号集合
DI_BEICHI_SIGNALS = [ChanSignals.LA0.value, ChanSignals.LA1.value, ChanSignals.LA2.value, ChanSignals.LA3.value,
                     ChanSignals.LB0.value, ChanSignals.LB1.value, ChanSignals.LB2.value, ChanSignals.LB3.value]
# 所有顶背驰信号集合
DING_BEICHI_SIGNALS = [ChanSignals.SA0.value, ChanSignals.SA1.value, ChanSignals.SA2.value, ChanSignals.SA3.value,
                       ChanSignals.SB0.value, ChanSignals.SB1.value, ChanSignals.SB2.value, ChanSignals.SB3.value]


def duan_bi_is_end(duan: ChanDuan, direction: Direction) -> bool:
    """
    判断线段的最后一笔，是否为线段的最高位或者最低位
    （主要是为了过滤一些包含的特征因子分笔）
    :param duan:
    :param direction:
    :return:
    """
    if direction == Direction.LONG:
        max_high = max([x.high for x in duan.bi_list])
        if max_high == duan.bi_list[-1].high:
            return True
        else:
            return False
    else:
        min_low = min([x.low for x in duan.bi_list])
        if min_low == duan.bi_list[-1].low:
            return True
        else:
            return False


def check_duan_not_rt(kline: CtaLineBar, direction: Direction) -> bool:
    """
    检查某一个K线当前线段是否非实时
    :param kline:
    :param Direction:
    :return:
    """
    if direction == Direction.LONG:
        direction = 1
    else:
        direction = -1

    if not kline.cur_duan or kline.cur_duan.direction != direction or kline.fenxing_list[-1].is_rt:
        return False

    if kline.cur_duan.end != kline.cur_bi.end:
        return False

    return True


def check_bi_not_rt(kline: CtaLineBar, direction: Direction) -> bool:
    """
    检查某一个K线当前分笔是否非实时
    :param kline:
    :param Direction:
    :return:
    """
    if direction == Direction.LONG:
        direction = 1
    else:
        direction = -1

    if not kline.cur_bi or kline.cur_bi.direction != direction:
        return False

    if kline.cur_bi.direction == kline.fenxing_list[-1].direction:
        if not kline.fenxing_list[-1].is_rt:
            return True
        else:
            return False

    return True


def check_chan_xt(kline: CtaLineBar, bi_list: List[ChanObject]) -> str:
    """
    获取缠论得形态
    如果提供得是段内得bi_list，一般可以算出该线段是否有背驰，
    如果是提供若干bi_list，可以粗略找出三买、三买等信号
    :param kline: 指定得K线
    :param bi_list: 指定得笔列表， 这里可以是duan.bi_list,也可以是随意指定给与得bi_list
    :return:
    """
    v = ChanSignals.Other.value
    if len(bi_list) == 5:
        return check_chan_xt_five_bi(kline, bi_list)
    if len(bi_list) == 7:
        return check_chan_xt_seven_bi(kline, bi_list)
    if len(bi_list) == 9:
        return check_chan_xt_nine_bi(kline, bi_list)
    if len(bi_list) == 11:
        return check_chan_xt_eleven_bi(kline, bi_list)
    if len(bi_list) == 13:
        return check_chan_xt_thirteen_bi(kline, bi_list)

    return v


def check_chan_xt_three_bi(kline: CtaLineBar, bi_list: List[ChanObject]):
    """
    获取指定3分笔得形态
    （含有三笔）
    :param kline:
    :param bi_list:
    :return:
    """
    v = ChanSignals.Other.value

    if len(bi_list) != 3:
        return v

    bi_1, bi_2, bi_3 = bi_list

    # 最后一笔是下跌
    if bi_3.direction == -1:
        # X3LA0~向下不重合
        if bi_3.low > bi_1.high:
            v = ChanSignals.X3LA0.value

        # X3LB0~向下奔走型中枢
        if bi_2.low < bi_3.low < bi_1.high < bi_2.high:
            v = ChanSignals.X3LB0.value

        # X3LC0~向下三角收敛中枢
        if bi_1.high > bi_3.high and bi_1.low < bi_3.low:
            v = ChanSignals.X3LC0.value

        # 向下三角扩张中枢
        if bi_1.high < bi_3.high and bi_1.low > bi_3.low:
            v = ChanSignals.X3LD0.value

        # X3LE0~向下盘背中枢， X3LF0~向下无背中枢
        if bi_3.low < bi_1.low and bi_3.high < bi_1.high:
            if bi_3.height < bi_1.height:
                # X3LE0~向下盘背中枢
                v = ChanSignals.X3LE0.value
            else:
                # X3LF0~向下无背中枢
                v = ChanSignals.X3LF0.value

    # 上涨线段
    elif bi_3.direction == 1:
        # X3SA0~向上不重合
        if bi_3.high > bi_1.low:
            v = ChanSignals.X3SA0.value

        # X3SB0~向上奔走型中枢
        if bi_2.low < bi_1.low < bi_3.high < bi_2.high:
            v = ChanSignals.X3SB0.value

        # X3SC0~向上三角收敛中枢
        if bi_1.high > bi_3.high and bi_1.low < bi_3.low:
            v = ChanSignals.X3SC0.value

        # X3SD0~向上三角扩张中枢
        if bi_1.high < bi_3.high and bi_1.low > bi_3.low:
            v = ChanSignals.X3SD0.value
        # X3SE0~向上盘背中枢，X3SF0~向上无背中枢
        if bi_3.low > bi_1.low and bi_3.high > bi_1.high:
            if bi_3.height < bi_1.height:
                # X3SE0~向上盘背中枢
                v = ChanSignals.X3SE0.value
            else:
                # X3SF0~向上无背中枢
                v = ChanSignals.X3SF0.value

    return v


def check_chan_xt_five_bi(kline: CtaLineBar, bi_list: List[ChanObject]):
    """识别当前5笔形态
        :param cur_duan: 当前线段
        :return: str
        """
    v = ChanSignals.Other.value

    if len(bi_list) != 5:
        return v

    bi_1, bi_2, bi_3, bi_4, bi_5 = bi_list

    # 这里得方向判断，是依赖第一笔
    direction = bi_1.direction
    max_high = max([x.high for x in bi_list])
    min_low = min([x.low for x in bi_list])

    # 下跌线段，寻找背驰
    if direction == -1:
        # aAb式底背驰
        if min(bi_2.high, bi_4.high) > max(bi_2.low, bi_4.low) and max_high == bi_1.high and bi_5.height < bi_1.height:
            if (min_low == bi_3.low and bi_5.low < bi_1.low) or (min_low == bi_5.low):
                v = ChanSignals.LA0.value

        # 类趋势底背驰
        if max_high == bi_1.high and min_low == bi_5.low and bi_4.high < bi_2.low and bi_5.height < max(bi_3.height,
                                                                                                        bi_1.height):
            v = ChanSignals.LA0.value

        # 上颈线突破
        if (min_low == bi_1.low and bi_5.high > min(bi_1.high, bi_2.high) > bi_5.low > bi_1.low) \
                or (min_low == bi_3.low and bi_5.high > bi_3.high > bi_5.low > bi_3.low):
            v = ChanSignals.LG0.value

        # 五笔三买，要求bi_5.high是最高点
        if max(bi_1.low, bi_3.low) < min(bi_1.high, bi_3.high) < bi_5.low and bi_5.high == max_high:
            v = ChanSignals.LI0.value

        # 向上三角扩张中枢
        if bi_1.high < bi_3.high < bi_5.high and bi_1.low > bi_3.low > bi_5.low:
            v = ChanSignals.LJ0.value

        # 向上三角收敛中枢
        if bi_1.high > bi_3.high > bi_5.high and bi_1.low < bi_3.low < bi_5.low:
            v = ChanSignals.LK0.value

    # 上涨线段，寻找顶背驰
    elif direction == 1:
        # aAb式顶背驰
        if min(bi_2.high, bi_4.high) > max(bi_2.low, bi_4.low) and min_low == bi_1.low and bi_5.height < bi_1.height:
            if (max_high == bi_3.high and bi_5.high > bi_1.high) or (max_high == bi_5.high):
                v = ChanSignals.SA0.value

        # 类趋势顶背驰
        if min_low == bi_1.low and max_high == bi_5.high and bi_5.height < max(bi_1.height,
                                                                               bi_3.height) and bi_4.low > bi_2.high:
            v = ChanSignals.SA0.value

        # 下颈线突破
        if (max_high == bi_1.high and bi_5.low < max(bi_1.low, bi_2.low) < bi_5.high < max_high) \
                or (max_high == bi_3.high and bi_5.low < bi_3.low < bi_5.high < max_high):
            v = ChanSignals.SG0.value

        # 五笔三卖，要求bi_5.low是最低点
        if min(bi_1.high, bi_3.high) > max(bi_1.low, bi_3.low) > bi_5.high and bi_5.low == min_low:
            v = ChanSignals.SI0.value

        # 向下三角扩张中枢
        if bi_1.high < bi_3.high < bi_5.high and bi_1.low > bi_3.low > bi_5.low:
            v = ChanSignals.SJ0.value

        # 向下三角收敛中枢
        if bi_1.high > bi_3.high > bi_5.high and bi_1.low < bi_3.low < bi_5.low:
            v = ChanSignals.SK0.value

    return v


def check_chan_xt_seven_bi(kline: CtaLineBar, bi_list: List[ChanObject]):
    """
    识别当前7笔的形态
    :param cur_duan:
    :return:
    """
    v = ChanSignals.Other.value
    if len(bi_list) != 7:
        return v

    bi_1, bi_2, bi_3, bi_4, bi_5, bi_6, bi_7 = bi_list
    max_high = max([x.high for x in bi_list])
    min_low = min([x.low for x in bi_list])

    if bi_7.direction == -1:
        if bi_1.high == max_high and bi_7.low == min_low:
            # aAbcd式底背驰
            if min(bi_2.high, bi_4.high) > max(bi_2.low, bi_4.low) > bi_6.high and bi_7.height < bi_5.height:
                v = ChanSignals.LA0.value

            # abcAd式底背驰
            if bi_2.low > min(bi_4.high, bi_6.high) > max(bi_4.low, bi_6.low) and bi_7.height < (bi_1.high - bi_3.low):
                v = ChanSignals.LA0.value

            # aAb式底背驰
            if min(bi_2.high, bi_4.high, bi_6.high) > max(bi_2.low, bi_4.low, bi_6.low) and bi_7.height < bi_1.height:
                v = ChanSignals.LA0.value

            # 类趋势底背驰
            if bi_2.low > bi_4.high and bi_4.low > bi_6.high and bi_7.height < max(bi_5.height, bi_3.height,
                                                                                   bi_1.height):
                v = ChanSignals.LA0.value

        # 向上中枢完成
        if bi_4.low == min_low and min(bi_1.high, bi_3.high) > max(bi_1.low, bi_3.low) \
                and min(bi_5.high, bi_7.high) > max(bi_5.low, bi_7.low) \
                and max(bi_4.high, bi_6.high) > min(bi_3.high, bi_4.high):
            if max(bi_1.low, bi_3.low) < max(bi_5.high, bi_7.high):
                v = ChanSignals.LH0.value

        # 七笔三买，567回调
        if bi_5.high == max_high and bi_5.high > bi_7.high \
                and bi_5.low > bi_7.low > min(bi_1.high, bi_3.high) > max(bi_1.low, bi_3.low):
            v = ChanSignals.LI0.value

    elif bi_7.direction == 1:
        # 顶背驰
        if bi_1.low == min_low and bi_7.high == max_high:
            # aAbcd式顶背驰
            if bi_6.low > min(bi_2.high, bi_4.high) > max(bi_2.low, bi_4.low) and bi_7.height < bi_5.height:
                v = ChanSignals.SA0.value

            # abcAd式顶背驰
            if min(bi_4.high, bi_6.high) > max(bi_4.low, bi_6.low) > bi_2.high and bi_7.height < (bi_3.high - bi_1.low):
                v = ChanSignals.SA0.value

            # aAb式顶背驰
            if min(bi_2.high, bi_4.high, bi_6.high) > max(bi_2.low, bi_4.low, bi_6.low) and bi_7.height < bi_1.height:
                v = ChanSignals.SA0.value

            # 类趋势顶背驰
            if bi_2.high < bi_4.low and bi_4.high < bi_6.low and bi_7.height < max(bi_5.height, bi_3.height,
                                                                                   bi_1.height):
                v = ChanSignals.SA0.value

        # 向下中枢完成
        if bi_4.high == max_high and min(bi_1.high, bi_3.high) > max(bi_1.low, bi_3.low) \
                and min(bi_5.high, bi_7.high) > max(bi_5.low, bi_7.low) \
                and min(bi_4.low, bi_6.low) < max(bi_3.low, bi_4.low):
            if min(bi_1.high, bi_3.high) > min(bi_5.low, bi_7.low):
                v = ChanSignals.SH0.value

        # 七笔三卖，567回调
        if bi_5.low == min_low and bi_5.low < bi_7.low \
                and min(bi_1.high, bi_3.high) > max(bi_1.low, bi_3.low) > bi_7.high > bi_5.high:
            v = ChanSignals.SI0.value

    return v


def check_chan_xt_nine_bi(kline: CtaLineBar, bi_list: List[ChanObject]):
    """
    获取线段得形态（9分笔）
    :param cur_duan:
    :return:
    """
    v = ChanSignals.Other.value
    if len(bi_list) != 9:
        return v

    direction = bi_list[-1].direction
    bi_1, bi_2, bi_3, bi_4, bi_5, bi_6, bi_7, bi_8, bi_9 = bi_list
    max_high = max([x.high for x in bi_list])
    min_low = min([x.low for x in bi_list])

    # 依据最后一笔得方向进行判断
    if direction == -1:
        if min_low == bi_9.low and max_high == bi_1.high:
            # aAbBc式底背驰
            if min(bi_2.high, bi_4.high) > max(bi_2.low, bi_4.low) > bi_6.high \
                    and min(bi_6.high, bi_8.high) > max(bi_6.low, bi_8.low) \
                    and min(bi_2.low, bi_4.low) > max(bi_6.high, bi_8.high) \
                    and bi_9.height < bi_5.height:
                v = ChanSignals.LA0.value

            # aAb式底背驰
            if min(bi_2.high, bi_4.high, bi_6.high, bi_8.high) > max(bi_2.low, bi_4.low, bi_6.low, bi_8.low) \
                    and bi_9.height < bi_1.height and bi_3.low >= bi_1.low and bi_7.high <= bi_9.high:
                v = ChanSignals.LA0.value

            # aAbcd式底背驰
            if min(bi_2.high, bi_4.high, bi_6.high) > max(bi_2.low, bi_4.low, bi_6.low) > bi_8.high \
                    and bi_9.height < bi_7.height:
                v = ChanSignals.LA0.value

            # ABC式底背驰
            if bi_3.low < bi_1.low and bi_7.high > bi_9.high \
                    and min(bi_4.high, bi_6.high) > max(bi_4.low, bi_6.low) \
                    and (bi_1.high - bi_3.low) > (bi_7.high - bi_9.low):
                v = ChanSignals.LA0.value

        # 九笔三买
        if min_low == bi_1.low and max_high == bi_9.high \
                and bi_9.low > min([x.high for x in [bi_3, bi_5, bi_7]]) > max([x.low for x in [bi_3, bi_5, bi_7]]):
            v = ChanSignals.LI0.value

    elif direction == 1:
        if max_high == bi_9.high and min_low == bi_1.low:
            # aAbBc式顶背驰
            if bi_6.low > min(bi_2.high, bi_4.high) > max(bi_2.low, bi_4.low) \
                    and min(bi_6.high, bi_8.high) > max(bi_6.low, bi_8.low) \
                    and max(bi_2.high, bi_4.high) < min(bi_6.low, bi_8.low) \
                    and bi_9.height < bi_5.height:
                v = ChanSignals.SA0.value

            # aAb式顶背驰
            if min(bi_2.high, bi_4.high, bi_6.high, bi_8.high) > max(bi_2.low, bi_4.low, bi_6.low, bi_8.low) \
                    and bi_9.height < bi_1.height and bi_3.high <= bi_1.high and bi_7.low >= bi_9.low:
                v = ChanSignals.SA0.value

            # aAbcd式顶背驰
            if bi_8.low > min(bi_2.high, bi_4.high, bi_6.high) > max(bi_2.low, bi_4.low, bi_6.low) \
                    and bi_9.height < bi_7.height:
                v = ChanSignals.SA0.value

            # ABC式顶背驰
            if bi_3.high > bi_1.high and bi_7.low < bi_9.low \
                    and min(bi_4.high, bi_6.high) > max(bi_4.low, bi_6.low) \
                    and (bi_3.high - bi_1.low) > (bi_9.high - bi_7.low):
                v = ChanSignals.SA0.value

        # 九笔三卖
        if max_high == bi_1.high and min_low == bi_9.low \
                and bi_9.high < max([x.low for x in [bi_3, bi_5, bi_7]]) < min([x.high for x in [bi_3, bi_5, bi_7]]):
            v = ChanSignals.SI0.value

    return v


def check_chan_xt_eleven_bi(kline: CtaLineBar, bi_list: List[ChanObject]):
    """
    获取线段得背驰形态（含11个分笔）
    :param cur_duan:
    :return:
    """
    v = ChanSignals.Other.value
    if len(bi_list) != 11:
        return v

    direction = bi_list[-1].direction
    bi_1, bi_2, bi_3, bi_4, bi_5, bi_6, bi_7, bi_8, bi_9, bi_10, bi_11 = bi_list
    max_high = max([x.high for x in bi_list])
    min_low = min([x.low for x in bi_list])

    if direction == -1:
        if min_low == bi_11.low and max_high == bi_1.high:
            # aAbBc式底背驰，bi_2-bi_6构成A
            if min(bi_2.high, bi_4.high, bi_6.high) > max(bi_2.low, bi_4.low, bi_6.low) > bi_8.high \
                    and min(bi_8.high, bi_10.high) > max(bi_8.low, bi_10.low) \
                    and min(bi_2.low, bi_4.low, bi_6.low) > max(bi_8.high, bi_10.high) \
                    and bi_11.height < bi_7.height:
                v = ChanSignals.LA0.value

            # aAbBc式底背驰，bi_6-bi_10构成B
            if min(bi_2.high, bi_4.high) > max(bi_2.low, bi_4.low) > bi_6.high \
                    and min(bi_6.high, bi_8.high, bi_10.high) > max(bi_6.low, bi_8.low, bi_10.low) \
                    and min(bi_2.low, bi_4.low) > max(bi_6.high, bi_8.high, bi_10.high) \
                    and bi_11.height < bi_5.height:
                v = ChanSignals.LA0.value

            # ABC式底背驰，A5B3C3
            if bi_5.low == min([x.low for x in [bi_1, bi_3, bi_5]]) \
                    and bi_9.low > bi_11.low and bi_9.high > bi_11.high \
                    and bi_8.high > bi_6.low and bi_1.high - bi_5.low > bi_9.high - bi_11.low:
                v = ChanSignals.LA0.value
                # C内部背驰
                if bi_11.height < bi_9.height:
                    v = ChanSignals.LB0.value

            # ABC式底背驰，A3B3C5
            if bi_1.high > bi_3.high and bi_1.low > bi_3.low \
                    and bi_7.high == max([x.high for x in [bi_7, bi_9, bi_11]]) \
                    and bi_6.high > bi_4.low and bi_1.high - bi_3.low > bi_7.high - bi_11.low:
                v = ChanSignals.LA0.value
                # C内部背驰
                if bi_11.height < max(bi_9.height, bi_7.height):
                    v = ChanSignals.LB0.value

            # ABC式底背驰，A3B5C3
            if bi_1.low > bi_3.low and min(bi_4.high, bi_6.high, bi_8.high) > max(bi_4.low, bi_6.low, bi_8.low) \
                    and bi_9.high > bi_11.high and bi_1.high - bi_3.low > bi_9.high - bi_11.low:
                v = ChanSignals.LA0.value
                # C内部背驰
                if bi_11.height < max(bi_9.height, bi_7.height):
                    v = ChanSignals.LB0.value

    elif direction == 1:
        if max_high == bi_11.high and min_low == bi_1.low:
            # aAbBC式顶背驰，bi_2-bi_6构成A
            if bi_8.low > min(bi_2.high, bi_4.high, bi_6.high) >= max(bi_2.low, bi_4.low, bi_6.low) \
                    and min(bi_8.high, bi_10.high) >= max(bi_8.low, bi_10.low) \
                    and max(bi_2.high, bi_4.high, bi_6.high) < min(bi_8.low, bi_10.low) \
                    and bi_11.height < bi_7.height:
                v = ChanSignals.SA0.value

            # aAbBC式顶背驰，bi_6-bi_10构成B
            if bi_6.low > min(bi_2.high, bi_4.high) >= max(bi_2.low, bi_4.low) \
                    and min(bi_6.high, bi_8.high, bi_10.high) >= max(bi_6.low, bi_8.low, bi_10.low) \
                    and max(bi_2.high, bi_4.high) < min(bi_6.low, bi_8.low, bi_10.low) \
                    and bi_11.height < bi_7.height:
                v = ChanSignals.SA0.value

            # ABC式顶背驰，A5B3C3
            if bi_5.high == max([bi_1.high, bi_3.high, bi_5.high]) and bi_9.low < bi_11.low and bi_9.high < bi_11.high \
                    and bi_8.low < bi_6.high and bi_11.high - bi_9.low < bi_5.high - bi_1.low:
                v = ChanSignals.SA0.value
                # C内部背驰
                if bi_11.height < bi_9.height:
                    v = ChanSignals.SB0.value

            # ABC式顶背驰，A3B3C5
            if bi_7.low == min([bi_11.low, bi_9.low, bi_7.low]) and bi_1.high < bi_3.high and bi_1.low < bi_3.low \
                    and bi_6.low < bi_4.high and bi_11.high - bi_7.low < bi_3.high - bi_1.low:
                v = ChanSignals.SA0.value
                # C内部背驰
                if bi_11.height < max(bi_9.height, bi_7.height):
                    v = ChanSignals.SB0.value

            # ABC式顶背驰，A3B5C3
            if bi_1.high < bi_3.high and min(bi_4.high, bi_6.high, bi_8.high) > max(bi_4.low, bi_6.low, bi_8.low) \
                    and bi_9.low < bi_11.low and bi_3.high - bi_1.low > bi_11.high - bi_9.low:
                v = ChanSignals.SA0.value
                # C内部背驰
                if bi_11.height < max(bi_9.height, bi_7.height):
                    v = ChanSignals.SB0.value

    return v


def check_chan_xt_thirteen_bi(kline: CtaLineBar, bi_list: List[ChanObject]):
    """
    获取线段得背驰形态（含13个分笔）
    :param cur_duan:
    :return:
    """
    v = ChanSignals.Other.value
    if len(bi_list) != 13:
        return v

    direction = bi_list[-1].direction
    bi_1, bi_2, bi_3, bi_4, bi_5, bi_6, bi_7, bi_8, bi_9, bi_10, bi_11, bi_12, bi_13 = bi_list
    max_high = max([x.high for x in bi_list])
    min_low = min([x.low for x in bi_list])

    # 下跌线段时，判断背驰类型
    if direction == -1:
        if min_low == bi_13.low and max_high == bi_1.high:
            # aAbBc式底背驰，bi_2-bi_6构成A，bi_8-bi_12构成B
            if min(bi_2.high, bi_4.high, bi_6.high) > max(bi_2.low, bi_4.low, bi_6.low) > bi_8.high \
                    and min(bi_8.high, bi_10.high, bi_12.high) > max(bi_8.low, bi_10.low, bi_12.low) \
                    and min(bi_2.low, bi_4.low, bi_6.low) > max(bi_8.high, bi_10.high, bi_12.high) \
                    and bi_13.height < bi_7.height:
                v = ChanSignals.LA0.value

            # ABC式底背驰，A5B3C5
            if bi_5.low < min(bi_1.low, bi_3.low) and bi_9.high > max(bi_11.high, bi_13.high) \
                    and bi_8.high > bi_6.low and bi_1.high - bi_5.low > bi_9.high - bi_13.low:
                v = ChanSignals.LA0.value

                if bi_13.height < max(bi_11.height, bi_9.height):
                    v = ChanSignals.LB0.value

            # ABC式底背驰，A3B5C5
            if bi_3.low < min(bi_1.low, bi_5.low) and bi_9.high > max(bi_11.high, bi_13.high) \
                    and min(bi_4.high, bi_6.high, bi_8.high) > max(bi_4.low, bi_6.low, bi_8.low) \
                    and bi_1.high - bi_3.low > bi_9.high - bi_13.low:
                v = ChanSignals.LA0.value

                if bi_13.height < max(bi_11.height, bi_9.height):
                    v = ChanSignals.LB0.value

            # ABC式底背驰，A5B5C3
            if bi_5.low < min(bi_1.low, bi_3.low) and bi_11.high > max(bi_9.high, bi_13.high) \
                    and min(bi_6.high, bi_8.high, bi_10.high) > max(bi_6.low, bi_8.low, bi_10.low) \
                    and bi_1.high - bi_5.low > bi_11.high - bi_13.low:
                v = ChanSignals.LA0.value

                if bi_13.height < bi_11.height:
                    v = ChanSignals.LB0.value

    # 上涨线段时，判断背驰类型
    elif direction == 1:
        if max_high == bi_13.high and min_low == bi_1.low:
            # aAbBC式顶背驰，bi_2-bi_6构成A，bi_8-bi_12构成B
            if bi_8.low > min(bi_2.high, bi_4.high, bi_6.high) >= max(bi_2.low, bi_4.low, bi_6.low) \
                    and min(bi_8.high, bi_10.high, bi_12.high) >= max(bi_8.low, bi_10.low, bi_12.low) \
                    and max(bi_2.high, bi_4.high, bi_6.high) < min(bi_8.low, bi_10.low, bi_12.low) \
                    and bi_13.height < bi_7.height:
                v = ChanSignals.SA0.value

            # ABC式顶背驰，A5B3C5
            if bi_5.high > max(bi_3.high, bi_1.high) and bi_9.low < min(bi_11.low, bi_13.low) \
                    and bi_8.low < bi_6.high and bi_5.high - bi_1.low > bi_13.high - bi_9.low:
                v = ChanSignals.SA0.value
                # C内部顶背驰，形成双重顶背驰
                if bi_13.height < max(bi_11.height, bi_9.height):
                    v = ChanSignals.SB0.value

            # ABC式顶背驰，A3B5C5
            if bi_3.high > max(bi_5.high, bi_1.high) and bi_9.low < min(bi_11.low, bi_13.low) \
                    and min(bi_4.high, bi_6.high, bi_8.high) > max(bi_4.low, bi_6.low, bi_8.low) \
                    and bi_3.high - bi_1.low > bi_13.high - bi_9.low:
                v = ChanSignals.SA0.value
                # C内部顶背驰，形成双重顶背驰
                if bi_13.height < max(bi_11.height, bi_9.height):
                    v = ChanSignals.SB0.value

            # ABC式顶背驰，A5B5C3
            if bi_5.high > max(bi_3.high, bi_1.high) and bi_11.low < min(bi_9.low, bi_13.low) \
                    and min(bi_6.high, bi_8.high, bi_10.high) > max(bi_6.low, bi_8.low, bi_10.low) \
                    and bi_5.high - bi_1.low > bi_13.high - bi_11.low:
                v = ChanSignals.SA0.value
                # C内部顶背驰，形成双重顶背驰
                if bi_13.height < bi_11.height:
                    v = ChanSignals.SB0.value
    return v
