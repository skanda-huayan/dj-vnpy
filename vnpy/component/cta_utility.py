# K线常用得方法：
# 缠论得一些形态识别
# 部分代码来自 github czsc

from vnpy.trader.constant import ChanSignals, Direction
from vnpy.component.chanlun.pyChanlun import ChanBi, ChanDuan, ChanObject
# from vnpy.component.cta_line_bar import CtaLineBar
from typing import List, Union

# 所有底背驰信号集合
DI_BEICHI_SIGNALS = [ChanSignals.LA0.value,
                     ChanSignals.LB0.value]

# 所有顶背驰信号集合
DING_BEICHI_SIGNALS = [ChanSignals.SA0.value,
                       ChanSignals.SB0.value]


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


def check_duan_not_rt(kline, direction: Direction) -> bool:
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


def check_bi_not_rt(kline, direction: Direction) -> bool:
    """
    检查某一个K线当前分笔是否非实时并符合判断方向
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

    if not kline.cur_fenxing:
        return False

    if kline.cur_bi.direction == kline.cur_fenxing.direction:
        if not kline.cur_fenxing.is_rt:
            return True
        else:
            if direction == 1:
                # 判断还没走完的bar，是否满足顶分型
                if float(kline.cur_fenxing.high) == float(kline.high_array[-1]) \
                        and kline.cur_fenxing.index == kline.index_list[-1] \
                        and kline.line_bar[-1].datetime.strftime('%Y-%m-%d %H:%M:%S') > kline.cur_fenxing.index \
                        and kline.line_bar[-1].high_price < float(kline.cur_fenxing.high) \
                        and kline.line_bar[-1].low_price < kline.line_bar[-2].low_price:
                    return True

            else:
                # 判断还没走完的bar，是否满足底分型
                if float(kline.cur_fenxing.low) == float(kline.low_array[-1]) \
                        and kline.cur_fenxing.index == kline.index_list[-1] \
                        and kline.line_bar[-1].datetime.strftime('%Y-%m-%d %H:%M:%S') > kline.cur_fenxing.index \
                        and kline.line_bar[-1].low_price > float(kline.cur_fenxing.low) \
                        and kline.line_bar[-1].high_price > kline.line_bar[-2].high_price:
                    return True

            return False

    return True


def check_fx_power(kline, direction: Direction) -> str:
    """
    获取分型强弱
    :param kline: 本级别K线
    :param direction: 分型方向： 1：顶分型；-1：底分型
    :return: 强,普通,弱，不匹配
    """
    ret = '不匹配'

    # 不存在分型，或者分型还没结束，不做判断
    if not kline.cur_fenxing or kline.cur_fenxing.is_rt:
        return ret

    direction = 1 if direction == Direction.LONG else -1

    # 分型方向不一致
    if kline.cur_fenxing.direction != direction:
        return ret

    # 分型前x根bar
    pre_bars = [bar for bar in kline.line_bar[-10:] if
                bar.datetime.strftime('%Y-%m-%d %H:%M:%S') < kline.cur_fenxing.index]

    if len(pre_bars) == 0:
        return ret
    pre_bar = pre_bars[-1]

    # 分型后x根bar
    extra_bars = \
        [bar for bar in kline.line_bar[-10:] if bar.datetime.strftime('%Y-%m-%d %H:%M:%S') > kline.cur_fenxing.index]

    # 分型后，有三根bar
    if len(extra_bars) < 3:
        return ret

    # 处理顶分型
    if kline.cur_fenxing.direction == 1:
        # 顶分型后第一根bar的低点，没有超过前bar的低点
        if extra_bars[0].low_price >= pre_bar.low_price:
            return '普通'

        # 找到正确形态，第二、第三根bar，都站在顶分型之下
        if pre_bar.low_price >= extra_bars[1].high_price > extra_bars[2].high_price:
            return '强'

        return '普通'

    # 处理底分型
    if kline.cur_fenxing.direction == -1:
        # 底分型后第一根bar的高点，没有超过前bar的高点
        if extra_bars[0].high_price <= pre_bar.high_price:
            return '弱'

        # 找到正确形态，第二、第三根bar，都站在底分型之上
        if pre_bar.high_price <= extra_bars[1].low_price < extra_bars[2].low_price:
            return '强'

        return '普通'

    return ret


def check_chan_xt(kline, bi_list: List[ChanObject]) -> str:
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
    if len(bi_list) >= 13:
        return check_chan_xt_thirteen_bi(kline, bi_list[-13:])

    return v


def check_chan_xt_three_bi(kline, bi_list: List[ChanObject]):
    """
    获取指定3分笔得形态
    （含有三笔）
    :param kline: ctaLineBar对象
    :param bi_list: 笔列表
    :return:
    """
    v = ChanSignals.Other.value

    if len(bi_list) != 3:
        return v

    bi_1, bi_2, bi_3 = bi_list

    # 最后一笔是下跌
    if bi_3.direction == -1:
        # X3LA0~向下不重合
        #            ^
        #          /   \
        #         /     \
        #        /
        # \     /
        #  \   /
        #    v
        if bi_3.low > bi_1.high:
            v = ChanSignals.X3LA0.value

        # X3LB0~向下奔走型
        #              ^
        #            /   \
        #           /     \
        #  \       /       \
        #   \     /
        #    \   /
        #      v
        if bi_2.low < bi_3.low < bi_1.high < bi_2.high:
            v = ChanSignals.X3LB0.value

        # X3LC0~向下收敛
        # \
        #  \            ^
        #   \         /   \
        #    \       /     \
        #     \     /
        #      \   /
        #        v
        if bi_1.high > bi_3.high and bi_1.low < bi_3.low:
            v = ChanSignals.X3LC0.value

        # X3LD0~向下扩张
        #              ^
        #            /   \
        #  \       /      \
        #    \   /         \
        #      v            \
        #                    \
        if bi_1.high < bi_3.high and bi_1.low > bi_3.low:
            v = ChanSignals.X3LD0.value

        # X3LE0~向下盘背， X3LF0~向下无背
        if bi_3.low < bi_1.low and bi_3.high < bi_1.high:
            if bi_3.height < bi_1.height:
                # X3LE0~向下盘背
                # \
                #  \
                #   \           ^
                #    \       /   \
                #     \   /       \
                #       v          \
                #                   \
                v = ChanSignals.X3LE0.value
            else:
                # X3LF0~向下无背中枢
                # \
                #  \          ^
                #   \      /    \
                #    \   /       \
                #      v          \
                #                  \
                #                   \
                v = ChanSignals.X3LF0.value

    # 上涨线段
    elif bi_3.direction == 1:
        # X3SA0~向上不重合
        #    ^
        #  /  \
        # /    \
        #       \
        #        \     /
        #         \   /
        #           v
        if bi_3.high < bi_1.low:
            v = ChanSignals.X3SA0.value

        # X3SB0~向上奔走型
        #     ^
        #   /  \
        #  /    \       /
        # /      \     /
        #         \   /
        #           v
        if bi_2.low < bi_1.low < bi_3.high < bi_2.high:
            v = ChanSignals.X3SB0.value

        # X3SC0~向上收敛
        #        ^
        #      /  \
        #     /    \      /
        #    /      \   /
        #   /         v
        # /
        if bi_1.high > bi_3.high and bi_1.low < bi_3.low:
            v = ChanSignals.X3SC0.value

        # X3SD0~向上扩张
        #                       /
        #        ^            /
        #      /  \         /
        #     /    \      /
        #    /      \   /
        #             v
        if bi_1.high < bi_3.high and bi_1.low > bi_3.low:
            v = ChanSignals.X3SD0.value

        # X3SE0~向上盘背，X3SF0~向上无背
        if bi_3.low > bi_1.low and bi_3.high > bi_1.high:
            if bi_3.height < bi_1.height:
                # X3SE0~向上盘背
                #                   /
                #        ^         /
                #      /  \       /
                #     /    \     /
                #    /      \   /
                #   /         v
                #  /
                # /
                v = ChanSignals.X3SE0.value
            else:
                # X3SF0~向上无背
                #                    /
                #                   /
                #        ^         /
                #      /  \       /
                #     /    \     /
                #    /      \   /
                #   /         v
                #  /
                v = ChanSignals.X3SF0.value

    return v


def check_chan_xt_five_bi(kline, bi_list: List[ChanObject]):
    """识别当前5笔形态
    :param kline: ctaLineBar对象
    :param bi_list: 笔列表
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
                return ChanSignals.LA0.value  # "底背驰”五笔aAb式

        # 类趋势底背驰( 笔5 的强度比笔1、笔3低）
        if max_high == bi_1.high and min_low == bi_5.low and bi_4.high < bi_2.low \
                and bi_5.height < max(bi_3.height, bi_1.height) \
                and bi_5.atan < max(bi_3.atan, bi_1.atan):
            return ChanSignals.LA0.value  # "底背驰" 五笔类趋势

        # 上颈线突破
        if (min_low == bi_1.low and bi_5.high > min(bi_1.high, bi_2.high) > bi_5.low > bi_1.low) \
                or (min_low == bi_3.low and bi_5.high > bi_3.high > bi_5.low > bi_3.low):
            return ChanSignals.LG0.value  # 上颈线突破  五笔

        # 五笔三买，要求bi_5.high是最高点, 或者bi_4.height，超过笔2、笔3两倍
        if min_low < max(bi_1.low, bi_3.low) < min(bi_1.high, bi_3.high) < bi_5.low:
            if bi_5.high == max_high:  #  and max(bi_1.high, bi_3.high) < bi_5.low
                v = ChanSignals.LI0.value  # 类三买， 五笔
            elif bi_3.low == min_low and bi_1.high == max_high \
                    and bi_4.height > max(bi_1.height, bi_2.height, bi_3.height) \
                    and bi_4.height > 2 * max(bi_2.height, bi_3.height):
                v = ChanSignals.LI0.value  # 类三买， 五笔

        # # 向上三角扩张中枢
        # if bi_1.high < bi_3.high < bi_5.high and bi_1.low > bi_3.low > bi_5.low:
        #     v = ChanSignals.LJ0.value
        #
        # # 向上三角收敛中枢
        # if bi_1.high > bi_3.high > bi_5.high and bi_1.low < bi_3.low < bi_5.low:
        #     v = ChanSignals.LK0.value

    # 上涨线段，寻找顶背驰
    elif direction == 1:
        # aAb式顶背驰，类一卖
        if min(bi_2.high, bi_4.high) > max(bi_2.low, bi_4.low) and min_low == bi_1.low and bi_5.height < bi_1.height:
            if (max_high == bi_3.high and bi_5.high > bi_1.high) or (max_high == bi_5.high):
                return ChanSignals.SA0.value  # k3='基础形态', v1='顶背驰', v2='五笔aAb式'

        # 类趋势顶背驰，类一卖
        if min_low == bi_1.low and max_high == bi_5.high \
                and bi_5.height < max(bi_1.height, bi_3.height) \
                and bi_5.atan < max(bi_1.atan, bi_3.atan) \
                and bi_4.low > bi_2.high:
            return ChanSignals.SA0.value  # k3='基础形态', v1='顶背驰', v2='五笔类趋势')

        # 下颈线突破
        if (max_high == bi_1.high and bi_5.low < max(bi_1.low, bi_2.low) < bi_5.high < max_high) \
                or (max_high == bi_3.high and bi_5.low < bi_3.low < bi_5.high < max_high):
            return ChanSignals.SG0.value  # k3='基础形态', v1='下颈线突破', v2='五笔')

        # 五笔三卖，要求bi_5.low是最低点，中枢可能是1~3
        if min(bi_1.high, bi_3.high) > max(bi_1.low, bi_3.low) > bi_5.high:
            if bi_5.low == min_low: # and min(bi_1.low, bi_3.low) > bi_5.high
                return ChanSignals.SI0.value
            elif bi_3.high == max_high and bi_1.low == min_low \
                    and bi_4.height > max(bi_1.height, bi_2.height, bi_3.height) \
                    and bi_4.height > 2 * max(bi_2.height, bi_3.height):
                return ChanSignals.SI0.value  # k3='基础形态', v1='类三卖', v2='五笔')
            # elif bi_1.high == max_high and bi_1.low == min_low:

        # # 向下三角扩张中枢
        # if bi_1.high < bi_3.high < bi_5.high and bi_1.low > bi_3.low > bi_5.low:
        #     v = ChanSignals.SJ0.value
        #
        # # 向下三角收敛中枢
        # if bi_1.high > bi_3.high > bi_5.high and bi_1.low < bi_3.low < bi_5.low:
        #     v = ChanSignals.SK0.value

    return v


def check_chan_xt_seven_bi(kline, bi_list: List[ChanObject]):
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
            # aAbcd式底背驰, d.高度斜率 小于 b.高度斜率
            if min(bi_2.high, bi_4.high) > max(bi_2.low, bi_4.low) > bi_6.high \
                    and bi_7.height < bi_5.height and bi_7.atan <= bi_5.atan:
                v = ChanSignals.LA0.value  # k3='基础形态', v1='底背驰', v2='七笔aAbcd式')

            # abcAd式底背驰
            if bi_2.low > min(bi_4.high, bi_6.high) > max(bi_4.low, bi_6.low) \
                    and bi_7.height < (bi_1.high - bi_3.low) \
                    and bi_7.atan < (bi_1.atan + bi_3.atan) / 2:
                v = ChanSignals.LA0.value  # k3='基础形态', v1='底背驰', v2='七笔abcAd式')

            # aAb式底背驰
            if min(bi_2.high, bi_4.high, bi_6.high) > max(bi_2.low, bi_4.low, bi_6.low) \
                    and bi_7.height < bi_1.height and bi_7.atan <= bi_1.atan:
                v = ChanSignals.LA0.value  # k3='基础形态', v1='底背驰', v2='七笔aAb式'

            # 类趋势底背驰
            if bi_2.low > bi_4.high and bi_4.low > bi_6.high \
                    and bi_7.height < max(bi_5.height, bi_3.height, bi_1.height) \
                    and bi_7.atan < max(bi_5.atan, bi_3.atan, bi_1.atan):
                v = ChanSignals.LA0.value  # k3='基础形态', v1='底背驰', v2='七笔类趋势'

        # 向上中枢完成
        if bi_4.low == min_low and min(bi_1.high, bi_3.high) > max(bi_1.low, bi_3.low) \
                and min(bi_5.high, bi_7.high) > max(bi_5.low, bi_7.low) \
                and max(bi_4.high, bi_6.high) > min(bi_3.high, bi_4.high):
            if max(bi_1.low, bi_3.low) < max(bi_5.high, bi_7.high):
                v = ChanSignals.LH0.value  # k3='基础形态', v1='向上中枢完成', v2='七笔')

        # 七笔三买，567回调 ：1~3构成中枢，最低点在1~3，最高点在5~7，5~7的最低点大于1~3的最高点
        if bi_5.high == max_high and bi_5.high > bi_7.high \
                and bi_5.low > bi_7.low > min(bi_1.high, bi_3.high) > max(bi_1.low, bi_3.low):
            v = ChanSignals.LI0.value  # k3='基础形态', v1='类三买', v2='七笔'

    elif bi_7.direction == 1:
        # 顶背驰
        if bi_1.low == min_low and bi_7.high == max_high:
            # aAbcd式顶背驰
            if bi_6.low > min(bi_2.high, bi_4.high) > max(bi_2.low, bi_4.low) \
                    and bi_7.height < bi_5.height and bi_7.atan <= bi_5.atan:
                v = ChanSignals.SA0.value  # k3='基础形态', v1='顶背驰', v2='七笔aAbcd式'

            # abcAd式顶背驰
            if min(bi_4.high, bi_6.high) > max(bi_4.low, bi_6.low) > bi_2.high \
                    and bi_7.height < (bi_3.high - bi_1.low) \
                    and bi_7.atan < (bi_1.atan + bi_3.atan) / 2:
                v = ChanSignals.SA0.value  # k3='基础形态', v1='顶背驰', v2='七笔abcAd式'

            # aAb式顶背驰
            if min(bi_2.high, bi_4.high, bi_6.high) > max(bi_2.low, bi_4.low, bi_6.low) \
                    and bi_7.height < bi_1.height and bi_7.atan <= bi_1.atan:
                v = ChanSignals.SA0.value  # k3='基础形态', v1='顶背驰', v2='七笔aAb式'

            # 类趋势顶背驰
            if bi_2.high < bi_4.low and bi_4.high < bi_6.low \
                    and bi_7.height < max(bi_5.height, bi_3.height, bi_1.height) \
                    and bi_7.atan < max(bi_5.atan, bi_3.atan, bi_1.atan):
                v = ChanSignals.SA0.value  # k3='基础形态', v1='顶背驰', v2='七笔类趋势'

        # 向下中枢完成
        if bi_4.high == max_high and min(bi_1.high, bi_3.high) > max(bi_1.low, bi_3.low) \
                and min(bi_5.high, bi_7.high) > max(bi_5.low, bi_7.low) \
                and min(bi_4.low, bi_6.low) < max(bi_3.low, bi_4.low):
            if min(bi_1.high, bi_3.high) > min(bi_5.low, bi_7.low):
                v = ChanSignals.SH0.value  # k3='基础形态', v1='向下中枢完成', v2='七笔'

        # 七笔三卖，567回调 1~3构成中枢，最高点在1~3，最低点在5~7，5~7的最高点小于1~3的最低点
        if bi_5.low == min_low and bi_5.low < bi_7.low \
                and min(bi_1.high, bi_3.high) > max(bi_1.low, bi_3.low) > bi_7.high > bi_5.high:
            v = ChanSignals.SI0.value  # k3='基础形态', v1='类三卖', v2='七笔'

    return v


def check_chan_xt_nine_bi(kline, bi_list: List[ChanObject]):
    """
    获取线段得买卖点（9分笔）
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
            # aAb式类一买
            if min(bi_2.high, bi_4.high, bi_6.high, bi_8.high) > max(bi_2.low, bi_4.low, bi_6.low, bi_8.low) \
                    and bi_9.height < bi_1.height and bi_9.atan <= bi_1.atan \
                    and bi_3.low >= bi_1.low and bi_7.high <= bi_9.high:
                return ChanSignals.Q1L0.value  # k3='类买卖点', v1='类一买', v2='九笔aAb式'

            # aAbcd式类一买
            if min(bi_2.high, bi_4.high, bi_6.high) > max(bi_2.low, bi_4.low, bi_6.low) > bi_8.high \
                    and bi_9.height < bi_7.height and bi_9.atan <= bi_7.atan:
                return ChanSignals.Q1L0.value  # k3='类买卖点', v1='类一买', v2='九笔aAbcd式'

            # ABC式类一买
            if bi_3.low < bi_1.low and bi_7.high > bi_9.high \
                    and min(bi_4.high, bi_6.high) > max(bi_4.low, bi_6.low) \
                    and (bi_1.high - bi_3.low) > (bi_7.high - bi_9.low):
                return ChanSignals.Q1L0.value  # k3='类买卖点', v1='类一买', v2='九笔ABC式'

            # 类趋势一买
            if bi_8.high < bi_6.low < bi_6.high < bi_4.high < bi_2.low \
                    and bi_9.atan < max([bi_1.atan, bi_3.atan, bi_5.atan, bi_7.atan]):
                return ChanSignals.Q1L0.value  # k3='类买卖点', v1='类一买', v2='九笔类趋势'

        # 9笔 aAbBc式类一买（2~4构成中枢A，6~8构成中枢B，9背驰）
        if max_high == max(bi_1.high, bi_3.high) and min_low == bi_9.low \
                and min(bi_2.high, bi_4.high) > max(bi_2.low, bi_4.low) > bi_6.high \
                and min(bi_6.high, bi_8.high) > max(bi_6.low, bi_8.low) \
                and min(bi_2.low, bi_4.low) > max(bi_6.high, bi_8.high) \
                and (bi_9.height < bi_5.height or bi_9.atan <= bi_5.atan):
            return ChanSignals.Q1L0.value  # k3='类买卖点', v1='类一买', v2='九笔aAb式')

        # 九笔GG 类三买（1357构成中枢，最低点在3或5）
        if max_high == bi_9.high > bi_9.low \
                > max([x.high for x in [bi_1, bi_3, bi_5, bi_7]]) \
                > min([x.high for x in [bi_1, bi_3, bi_5, bi_7]]) \
                > max([x.low for x in [bi_1, bi_3, bi_5, bi_7]]) \
                > min([x.low for x in [bi_3, bi_5]]) == min_low:
            return ChanSignals.Q3L0.value  # Signal(k1=freq.value, k2=di_name, k3='类买卖点', v1='类三买', v2='九笔GG三买')

        # 类三买（357构成中枢，8的力度小于2，9回调不跌破GG构成三买）
        if bi_8.height < bi_2.height and max_high == bi_9.high > bi_9.low \
                > max([x.high for x in [bi_3, bi_5, bi_7]]) \
                > min([x.high for x in [bi_3, bi_5, bi_7]]) \
                > max([x.low for x in [bi_3, bi_5, bi_7]]) > bi_1.low == min_low:
            return ChanSignals.Q3L0.value  # Signal(k1=freq.value, k2=di_name, k3='类买卖点', v1='类三买', v2='九笔GG三买')

        # 类三买（357构成收敛中枢，8的力度小于2，9回调不跌破收敛中枢的末笔7，构成三买）
        if bi_8.height < bi_2.height and max_high == bi_9.high \
                > max([x.high for x in [bi_3, bi_5, bi_7]]) \
                > min([x.high for x in [bi_3, bi_5, bi_7]]) \
                > max([x.low for x in [bi_3, bi_5, bi_7]]) > bi_1.low == min_low:
            if bi_3.height > bi_5.height > bi_7.height \
                and bi_3.high > bi_5.high > bi_7.high:
                # 计算收敛三角的上切线，测算出bi_9对应的切线价格
                atan = (bi_3.high - bi_7.high) / (bi_3.bars + bi_4.bars + bi_5.bars + bi_6.bar - 3)
                p = bi_7.high - atan * (bi_7.bars + bi_8.bars + bi_9.bars - 2)
                if bi_9.low > p:
                    return ChanSignals.Q3L0.value  # Signal(k1=freq.value, k2=di_name, k3='类买卖点', v1='类三买', v2='九笔收敛突破三买')

        # # 九笔三买(789回调）中枢可能在3~7内
        # if min_low == bi_1.low and max_high == bi_9.high \
        #         and bi_9.low > min([x.high for x in [bi_3, bi_5, bi_7]]) > max([x.low for x in [bi_3, bi_5, bi_7]]):
        #     v = ChanSignals.Q3L0.value

        if min_low == bi_5.low and max_high == bi_1.high and bi_4.high < bi_2.low:  # 前五笔构成向下类趋势
            zd = max([x.low for x in [bi_5, bi_7]])
            zg = min([x.high for x in [bi_5, bi_7]])
            gg = max([x.high for x in [bi_5, bi_7]])
            if zg > zd and bi_8.high > gg:  # 567构成中枢，且8的高点大于gg
                if bi_9.low > zg:
                    return ChanSignals.Q3L0.value  # Signal(k1=freq.value, k2=di_name, k3='类买卖点', v1='类三买', v2='九笔ZG三买')

                # 参考双重底或者圆弧底， 在 bi_5.high => bi_7.high => p点 形成一条斜线，如果bi_9.low 在斜线之上，就是三买
                if gg == bi_5.high and zg == bi_7.high:
                    atan = (gg - zg) / (bi_5.bars + bi_6.bars - 1)
                    p = bi_7.high - atan * (bi_7.bars + bi_8.bars + bi_9.bars -2)
                    if bi_9.low > p:
                        return ChanSignals.Q3L0.value  # Signal(k1=freq.value, k2=di_name, k3='类买卖点', v1='类三买', v2='九笔ZG三买')

                # 类二买
                if bi_9.high > gg > zg > bi_9.low > zd:
                    return ChanSignals.Q2L0.value  # Signal(k1=freq.value, k2=di_name, k3='类买卖点', v1='类二买', v2='九笔')

        if min_low == bi_7.low and max_high == bi_1.high and bi_6.high < bi_2.low:  # 前7笔构成向下类趋势
            zd = max([x.low for x in [bi_5, bi_6]])
            zg = min([x.high for x in [bi_4, bi_6]])
            gg = max([x.high for x in [bi_4, bi_6]])
            if zg > zd and bi_8.high > gg == bi_4.high :  # 456构成中枢，7离开中枢，8反包且8的高点大于gg,4高 >6高点
                atan = (bi_4.high - bi_6.high) / (bi_5.bars + bi_6.bars - 1)
                p = bi_6.high - atan * (bi_7.bars + bi_8.bars + bi_9.bars - 2)
                if zd > bi_9.low > p:
                    return ChanSignals.Q2L0.value

    elif direction == 1:

        # 倒9笔是最高点，倒一笔是最低点
        if max_high == bi_9.high and min_low == bi_1.low:

            # aAb式类一卖
            if min(bi_2.high, bi_4.high, bi_6.high, bi_8.high) > max(bi_2.low, bi_4.low, bi_6.low, bi_8.low) \
                    and bi_9.height < bi_1.height and bi_9.atan <= bi_1.atan \
                    and bi_3.high <= bi_1.high and bi_7.low >= bi_9.low:
                return ChanSignals.Q1S0.value  # Signal(k1=freq.value, k2=di_name, k3='类买卖点', v1='类一卖', v2='九笔aAb式')

            # aAbcd式类一卖
            if bi_8.low > min(bi_2.high, bi_4.high, bi_6.high) > max(bi_2.low, bi_4.low, bi_6.low) \
                    and bi_9.height < bi_7.height and bi_9.atan <= bi_7.atan:
                return ChanSignals.Q1S0.value  # Signal(k1=freq.value, k2=di_name, k3='类买卖点', v1='类一卖', v2='九笔aAbcd式')

            # ABC式类一卖
            if bi_3.high > bi_1.high and bi_7.low < bi_9.low \
                    and min(bi_4.high, bi_6.high) > max(bi_4.low, bi_6.low) \
                    and (bi_3.high - bi_1.low) > (bi_9.high - bi_7.low):
                return ChanSignals.Q1S0.value  # Signal(k1=freq.value, k2=di_name, k3='类买卖点', v1='类一卖', v2='九笔ABC式')

            # 类趋势类一卖
            if bi_8.low > bi_6.high > bi_6.low > bi_4.high > bi_4.low > bi_2.high \
                    and bi_9.atan < max([bi_1.atan, bi_3.atan, bi_5.atan, bi_7.atan]):
                return ChanSignals.Q1S0.value  # Signal(k1=freq.value, k2=di_name, k3='类买卖点', v1='类一卖', v2='九笔类趋势')

        # aAbBc式类一卖
        if max_high == bi_9.high and min_low == min(bi_1.low, bi_3.low) \
                and bi_6.low > min(bi_2.high, bi_4.high) > max(bi_2.low, bi_4.low) \
                and min(bi_6.high, bi_8.high) > max(bi_6.low, bi_8.low) \
                and max(bi_2.high, bi_4.high) < min(bi_6.low, bi_8.low) \
                and (bi_9.height < bi_5.height or bi_9.atan <= bi_5.atan):
            return ChanSignals.Q1S0.value  # Signal(k1=freq.value, k2=di_name, k3='类买卖点', v1='类一卖', v2='九笔aAbBc式')

        # 九笔类三卖, 3/5/7形成中枢， 9笔回调不进中枢
        if max_high == bi_1.high and min_low == bi_9.low \
                and bi_9.high < max([x.low for x in [bi_3, bi_5, bi_7]]) < min([x.high for x in [bi_3, bi_5, bi_7]]):
            return ChanSignals.Q3S0.value  # Signal(k1=freq.value, k2=di_name, k3='类买卖点', v1='类三卖', v2='九笔')

        # 九笔类三卖 3、5、7形成收敛三角中枢， 9笔回调不进三角
        if max_high == bi_1.high and min_low == bi_9.low \
                and max([x.low for x in [bi_3, bi_5, bi_7]]) < min([x.high for x in [bi_3, bi_5, bi_7]]):
            if bi_3.height > bi_5.height > bi_7.height \
                and bi_3.low < bi_5.low < bi_7.low:
                # 计算收敛三角的下切线，测算出bi_9对应的切线价格
                atan = ( bi_7.low - bi_3.low ) / (bi_3.bars + bi_4.bars + bi_5.bars + bi_6.bars -3 )
                p = bi_7.low + atan * (bi_7.bars + bi_8.bars + bi_8.bars -2)
                if bi_9.high < p:
                    return ChanSignals.Q3S0.value  # Signal(k1=freq.value, k2=di_name, k3='类买卖点', v1='类三卖', v2='九笔')

        if min_low == bi_1.low and max_high == bi_5.high and bi_2.high < bi_4.low:  # 前五笔构成向上类趋势
            zd = max([x.low for x in [bi_5, bi_7]])
            zg = min([x.high for x in [bi_5, bi_7]])
            dd = min([x.low for x in [bi_5, bi_7]])
            if zg > zd and bi_8.low < dd:  # 567构成中枢，且8的低点小于dd
                if bi_9.high < zd:
                    return ChanSignals.Q3S0.value  # Signal(k1=freq.value, k2=di_name, k3='类买卖点', v1='类三卖', v2='九笔ZD三卖')

                # 参考双重顶或者圆弧顶， 在 bi_5.low => bi_7.low => p点 形成一条斜线，如果bi_9.high 在斜线之下，就是三卖
                if dd == bi_5.low and zd == bi_7.low:
                    atan = (zd - dd) / (bi_5.bars + bi_6.bars - 1)
                    p = bi_7.low + atan * (bi_7.bars + bi_8.bars + bi_9.bars - 2)
                    if bi_9.high < p:
                        return ChanSignals.Q3S0.value  # Signal(k1=freq.value, k2=di_name, k3='类买卖点', v1='类三卖', v2='九笔ZD三卖')

                # 类二卖
                if dd < zd <= bi_9.high < zg:
                    return ChanSignals.Q2S0.value  # Signal(k1=freq.value, k2=di_name, k3='类买卖点', v1='类二卖', v2='九笔')

        if min_low == bi_1.low and max_high == bi_7.high and bi_2.high < bi_6.low: # 前7笔形成上涨趋势
            zd = max([x.low for x in [bi_4, bi_6]])
            zg = min([x.high for x in [bi_4, bi_6]])
            dd = min([x.low for x in [bi_4, bi_6]])
            if zg > zd and bi_8.low < dd == bi_4.low:  # 456构成中枢，7离开中枢，8反包且8的低点小于dd,4低点< 6低点
                atan = (bi_6.low - bi_4.low) / (bi_5.bars + bi_6.bars -1)
                p = bi_6.low + atan * (bi_7.bars + bi_8.bars + bi_9.bars - 2)
                if zg < bi_9.high < p:
                    return ChanSignals.Q2S0.value

    return v


def check_chan_xt_eleven_bi(kline, bi_list: List[ChanObject]):
    """
    获取线段得的类买卖点（含11个分笔）
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

    # 11笔向下，寻找买点
    if direction == -1:
        # 1笔最高，11笔最低
        if min_low == bi_11.low and max_high == bi_1.high:

            # ABC式类一买，A5B3C3
            if bi_5.low == min([x.low for x in [bi_1, bi_3, bi_5]]) \
                    and bi_9.low > bi_11.low and bi_9.high > bi_11.high \
                    and bi_8.high > bi_6.low and bi_1.high - bi_5.low > bi_9.high - bi_11.low:
                return ChanSignals.Q1L0.value  # Signal(k1=freq.value, k2=di_name, k3='类买卖点', v1='类一买', v2="11笔A5B3C3式")

            # ABC式类一买，A3B3C5
            if bi_1.high > bi_3.high and bi_1.low > bi_3.low \
                    and bi_7.high == max([x.high for x in [bi_7, bi_9, bi_11]]) \
                    and bi_6.high > bi_4.low and bi_1.high - bi_3.low > bi_7.high - bi_11.low:
                return ChanSignals.Q1L0.value  # Signal(k1=freq.value, k2=di_name, k3='类买卖点', v1='类一买', v2="11笔A3B3C5式")

            # ABC式类一买，A3B5C3
            if bi_1.low > bi_3.low and min(bi_4.high, bi_6.high, bi_8.high) > max(bi_4.low, bi_6.low, bi_8.low) \
                    and bi_9.high > bi_11.high and bi_1.high - bi_3.low > bi_9.high - bi_11.low:
                v = ChanSignals.Q1L0.value  # Signal(k1=freq.value, k2=di_name, k3='类买卖点', v1='类一买', v2="11笔A3B5C3式")

            # a1Ab式类一买，a1（1~7构成的类趋势）
            if bi_2.low > bi_4.high > bi_4.low > bi_6.high > bi_5.low > bi_7.low and bi_10.high > bi_8.low:
                return ChanSignals.Q1L0.value  # Signal(k1=freq.value, k2=di_name, k3='类买卖点', v1='类一买', v2="11笔a1Ab式")

        # 类二买：1~9构成类趋势，11不创新低,斜率低于9
        if min_low == bi_9.low < bi_8.high < bi_6.low < bi_6.high < bi_4.low < bi_4.high < bi_2.low < bi_1.high == max_high \
                and bi_11.low > bi_9.low and bi_9.atan > bi_11.atan:
            return ChanSignals.Q2L0.value  # Signal(k1=freq.value, k2=di_name, k3='类买卖点', v1='类二买', v2="11笔")

        # 类二买（1~7构成盘整背驰，246构成下跌中枢，9/11构成上涨中枢，且上涨中枢ZG大于下跌中枢ZG）
        if bi_7.atan < bi_1.atan and min_low == bi_7.low < max([x.low for x in [bi_2, bi_4, bi_6]]) \
                < min([x.high for x in [bi_2, bi_4, bi_6]]) < max(
            [x.high for x in [bi_9, bi_11]]) < bi_1.high == max_high \
                and bi_11.low > min([x.low for x in [bi_2, bi_4, bi_6]]) \
                and min([x.high for x in [bi_9, bi_11]]) > max([x.low for x in [bi_9, bi_11]]):
            return ChanSignals.Q2L0.value  # Signal(k1=freq.value, k2=di_name, k3='类买卖点', v1='类二买', v2="11笔")

        # 类二买（1~7为区间极值，9~11构成上涨中枢，上涨中枢GG大于4~6的最大值，上涨中枢DD大于4~6的最小值）
        if max_high == bi_1.high and min_low == bi_7.low \
                and min(bi_9.high, bi_11.high) > max(bi_9.low, bi_11.low) \
                and max(bi_11.high, bi_9.high) > max(bi_4.high, bi_6.high) \
                and min(bi_9.low, bi_11.low) > min(bi_4.low, bi_6.low):
            return ChanSignals.Q2L0.value  # Signal(k1=freq.value, k2=di_name, k3='类买卖点', v1='类二买', v2="11笔")

        # 类三买（1~9构成大级别中枢，10离开，11回调不跌破GG）
        gg = max([x.high for x in [bi_1, bi_2, bi_3]])
        zg = min([x.high for x in [bi_1, bi_2, bi_3]])
        zd = max([x.low for x in [bi_1, bi_2, bi_3]])
        dd = min([x.low for x in [bi_1, bi_2, bi_3]])
        if max_high == bi_11.high and bi_11.low > zg > zd \
                and gg > bi_5.low and gg > bi_7.low and gg > bi_9.low \
                and dd < bi_5.high and dd < bi_7.high and dd < bi_9.high:
            return ChanSignals.Q3L0.value  # Signal(k1=freq.value, k2=di_name, k3='类买卖点', v1='类三买', v2="11笔GG三买")

    # 11笔向上，寻找卖点
    elif direction == 1:
        # 1笔最低，11笔最高
        if max_high == bi_11.high and min_low == bi_1.low:

            # ABC式顶背驰，A5B3C3
            if bi_5.high == max([bi_1.high, bi_3.high, bi_5.high]) and bi_9.low < bi_11.low and bi_9.high < bi_11.high \
                    and bi_8.low < bi_6.high and bi_11.high - bi_9.low < bi_5.high - bi_1.low:
                return ChanSignals.Q1S0.value  # Signal(k1=freq.value, k2=di_name, k3='类买卖点', v1='类一卖', v2="11笔A5B3C3式")

            # ABC式顶背驰，A3B3C5
            if bi_7.low == min([bi_11.low, bi_9.low, bi_7.low]) and bi_1.high < bi_3.high and bi_1.low < bi_3.low \
                    and bi_6.low < bi_4.high and bi_11.high - bi_7.low < bi_3.high - bi_1.low:
                return ChanSignals.Q1S0.value  # Signal(k1=freq.value, k2=di_name, k3='类买卖点', v1='类一卖', v2="11笔A3B3C5式")

            # ABC式顶背驰，A3B5C3
            if bi_1.high < bi_3.high and min(bi_4.high, bi_6.high, bi_8.high) > max(bi_4.low, bi_6.low, bi_8.low) \
                    and bi_9.low < bi_11.low and bi_3.high - bi_1.low > bi_11.high - bi_9.low:
                return ChanSignals.Q1S0.value

            # a1Ab式类一卖，a1（1~7构成的类趋势）
            if bi_2.high < bi_4.low < bi_4.high < bi_6.low < bi_5.high < bi_7.high and bi_10.low < bi_8.high:
                return ChanSignals.Q1S0.value  # Signal(k1=freq.value, k2=di_name, k3='类买卖点', v1='类一卖', v2="11笔a1Ab式")

        # 类二卖：1~9构成类趋势，11不创新高
        if max_high == bi_9.high > bi_8.low > bi_6.high > bi_6.low > bi_4.high > bi_4.low > bi_2.high > bi_1.low == min_low \
                and bi_11.high < bi_9.high and bi_9.atan > bi_11.atan:
            return ChanSignals.Q1S0.value  # Signal(k1=freq.value, k2=di_name, k3='类买卖点', v1='类二卖', v2="11笔")

        # 类二卖（1~7构成盘整背驰，246构成上涨中枢，9/11构成下跌中枢，且下跌中枢DD小于上涨中枢ZD）
        if bi_7.atan < bi_1.atan and max_high == bi_7.high \
                > min([x.high for x in [bi_2, bi_4, bi_6]]) \
                > max([x.low for x in [bi_2, bi_4, bi_6]]) \
                > min([x.low for x in [bi_9, bi_11]]) \
                > bi_1.low == min_low \
                and bi_11.high < max([x.high for x in [bi_2, bi_4, bi_6]]) \
                and max([x.low for x in [bi_9, bi_11]]) < min([x.high for x in [bi_9, bi_11]]):
            return ChanSignals.Q2S0.value  # Signal(k1=freq.value, k2=di_name, k3='类买卖点', v1='类二卖', v2="11笔")

        # 类二卖（1~7为区间极值，9~11构成下跌中枢，下跌中枢DD小于4~6的最小值，下跌中枢GG小于4~6的最大值）
        if min_low == bi_1.low and max_high == bi_7.high \
                and max(bi_9.low, bi_11.low) < min(bi_9.high, bi_11.high) \
                and min(bi_11.low, bi_9.low) < min(bi_4.low, bi_6.low) \
                and max(bi_9.high, bi_11.high) < max(bi_4.high, bi_6.high):
            return ChanSignals.Q2S0.value  # Signal(k1=freq.value, k2=di_name, k3='类买卖点', v1='类二卖', v2="11笔")

        # 类三卖（1~9构成大级别中枢，10离开，11回调不涨破DD）
        gg = min([x.low for x in [bi_1, bi_2, bi_3]])
        zg = max([x.low for x in [bi_1, bi_2, bi_3]])
        zd = min([x.high for x in [bi_1, bi_2, bi_3]])
        dd = max([x.high for x in [bi_1, bi_2, bi_3]])
        if min_low == bi_11.low and bi_11.high < zd < zg \
                and dd < bi_5.high and dd < bi_7.high and dd < bi_9.high \
                and gg > bi_5.low and gg > bi_7.low and gg > bi_9.low:
            return ChanSignals.Q3S0.value  # Signal(k1=freq.value, k2=di_name, k3='类买卖点', v1='类三卖', v2="11笔GG三买")

    return v


def check_chan_xt_thirteen_bi(kline, bi_list: List[ChanObject]):
    """
    获取线段得形态（含13个分笔）
    :param kline:
    :param bi_list: 由远及近的十三笔
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
            # ABC式类一买，A5B3C5
            if bi_5.low < max(bi_1.low, bi_3.low) and bi_9.high > max(bi_11.high, bi_13.high) \
                    and bi_8.high > bi_6.low and bi_1.high - bi_5.low > bi_9.high - bi_13.low:
                return ChanSignals.Q1L0.value  # Signal(k1=freq.value, k2=di_name, k3='类买卖点', v1='类一买', v2="13笔A5B3C5式")

            # ABC式类一买，A3B5C5
            if bi_3.low < min(bi_1.low, bi_5.low) and bi_9.high > max(bi_11.high, bi_13.high) \
                    and min(bi_4.high, bi_6.high, bi_8.high) > max(bi_4.low, bi_6.low, bi_8.low) \
                    and bi_1.high - bi_3.low > bi_9.high - bi_13.low:
                return ChanSignals.Q1L0.value  # Signal(k1=freq.value, k2=di_name, k3='类买卖点', v1='类一买', v2="13笔A3B5C5式")

            # ABC式底背驰，A5B5C3
            if bi_5.low < min(bi_1.low, bi_3.low) and bi_11.high > max(bi_9.high, bi_13.high) \
                    and min(bi_6.high, bi_8.high, bi_10.high) > max(bi_6.low, bi_8.low, bi_10.low) \
                    and bi_1.high - bi_5.low > bi_11.high - bi_13.low:
                return ChanSignals.Q1L0.value  # Signal(k1=freq.value, k2=di_name, k3='类买卖点', v1='类一买', v2="13笔A5B5C3式")
            # AB式底背驰， aAbBc

    # 上涨线段时，判断背驰类型
    elif direction == 1:
        if max_high == bi_13.high and min_low == bi_1.low:
            # ABC式顶背驰，A5B3C5
            if bi_5.high > min(bi_3.high, bi_1.high) and bi_9.low < min(bi_11.low, bi_13.low) \
                    and bi_8.low < bi_6.high and bi_5.high - bi_1.low > bi_13.high - bi_9.low:
                return ChanSignals.Q1S0.value  # Signal(k1=freq.value, k2=di_name, k3='类买卖点', v1='类一卖', v2="13笔A5B3C5式")

            # ABC式顶背驰，A3B5C5
            if bi_3.high > max(bi_5.high, bi_1.high) and bi_9.low < min(bi_11.low, bi_13.low) \
                    and min(bi_4.high, bi_6.high, bi_8.high) > max(bi_4.low, bi_6.low, bi_8.low) \
                    and bi_3.high - bi_1.low > bi_13.high - bi_9.low:
                return ChanSignals.Q1S0.value  # Signal(k1=freq.value, k2=di_name, k3='类买卖点', v1='类一卖', v2="13笔A3B5C5式")

            # ABC式顶背驰，A5B5C3
            if bi_5.high > max(bi_3.high, bi_1.high) and bi_11.low < min(bi_9.low, bi_13.low) \
                    and min(bi_6.high, bi_8.high, bi_10.high) > max(bi_6.low, bi_8.low, bi_10.low) \
                    and bi_5.high - bi_1.low > bi_13.high - bi_11.low:
                return ChanSignals.Q1S0.value  # Signal(k1=freq.value, k2=di_name, k3='类买卖点', v1='类一卖', v2="13笔A5B5C3式")

    return v


# def check_pzbc_1st(big_kline, small_kline: Union[CtaLineBar, None], signal_direction: Direction):
def check_pzbc_1st(big_kline, small_kline, signal_direction: Direction):
    """
    判断中枢盘整背驰1买/1卖信号
    big_kline当前线段为调整段，与信号方向相反，线段具有盘整一个中枢，
    进入中枢与离开中枢的一笔力度对比（高度、斜率）
    :param big_kline: 本级别K线
    :param small_kline: 次级别K线（可选，可以是None）
    :param signal_direction: 信号方向
    :return:
    """
    direction = 1 if signal_direction == Direction.LONG else -1

    # 排除
    # 没有前线段、没有笔中枢
    # 当前线段方向与判断方向一致、
    # 前线段比当前线段高度小
    if not big_kline.pre_duan \
            or not big_kline.cur_bi_zs \
            or big_kline.cur_duan.direction == direction \
            or big_kline.pre_duan.height < big_kline.cur_duan.height:
        return False

    # 如果有次级别K线时，也要判断方向
    if small_kline and (not small_kline.pre_duan or small_kline.cur_duan.direction == direction):
        return False

    # 当前线段必须有5笔
    if len(big_kline.cur_duan.bi_list) < 5:
        return False

    # 线段内，只允许有一个中枢
    if len([zs for zs in big_kline.bi_zs_list[-3:] if zs.start > big_kline.cur_duan.start]) > 1:
        return False

    # 当前笔中枢必须在当前线段之内
    if big_kline.cur_bi_zs.start < big_kline.cur_duan.start:
        return False

    # 当前线段的高低点,与最高、最低分笔一致（不会出现区间套）
    if signal_direction == Direction.LONG:
        # 当前最后一笔，就是线段的最后一笔
        if not duan_bi_is_end(big_kline.cur_duan, Direction.SHORT):
            return False
        # 当前的线段，已经具备底分型
        if not check_duan_not_rt(big_kline, Direction.SHORT):
            return False
        # 当前的笔，走完，具备底分型
        if not check_bi_not_rt(big_kline, Direction.SHORT):
            return False
    else:
        if not duan_bi_is_end(big_kline.cur_duan, Direction.LONG):
            return False
        if not check_duan_not_rt(big_kline, Direction.LONG):
            return False
        # 笔走完
        if not check_bi_not_rt(big_kline, Direction.LONG):
            return False

    # 中枢的进入笔、离开笔
    # 中枢的首笔与线段不同向，则选择中枢之前的一笔和最后的一笔
    if big_kline.cur_bi_zs.bi_list[0].direction != big_kline.cur_duan.direction:
        entry_bi_list = [bi for bi in big_kline.cur_duan.bi_list if bi.end <= big_kline.cur_bi_zs.start]
        exit_bi_list = [bi for bi in big_kline.cur_duan.bi_list if bi.end > big_kline.cur_bi_zs.end]
        if not (len(entry_bi_list) >= 1 and len(exit_bi_list) == 1):
            return False
        entry_bi = entry_bi_list[-1]
        exit_bi = exit_bi_list[0]

    # 中枢首笔跟线段同向
    else:
        entry_bi = big_kline.cur_bi_zs.bi_list[0]
        exit_bi = big_kline.cur_duan.bi_list[-1]

    # 进入笔的高度，要高于离开笔，或者，进入笔的斜率，要大于离开笔
    if entry_bi.height > exit_bi.height or entry_bi.atan > exit_bi.atan:

        # 分析次级别K线，判断其是否也发生线段背驰
        if small_kline:
            if len(small_kline.cur_duan.bi_list) > 1:
                if (small_kline.cur_duan.bi_list[0].height > small_kline.cur_duan.bi_list[-1].height \
                    and small_kline.cur_duan.bi_list[0].atan > small_kline.cur_duan.bi_list[-1].atan) \
                        or (small_kline.cur_duan.bi_list[-3].height > small_kline.cur_duan.bi_list[-1].height \
                            and small_kline.cur_duan.bi_list[-3].atan > small_kline.cur_duan.bi_list[-1].atan):
                    return True
        else:
            return True

    # 判断是否macd背驰
    if big_kline.is_macd_divergence(big_kline.cur_duan.direction, exit_bi.end, entry_bi.end):
        return True

    return False


# def check_qsbc_1st(big_kline, small_kline: Union[CtaLineBar, None], signal_direction: Direction):
def check_qsbc_1st(big_kline, small_kline, signal_direction: Direction):
    """
    判断趋势背驰1买/1卖信号
    big_kline当前线段为趋势，与信号方向相反，线段具有2个中枢，
    进入最后中枢与离开中枢的一笔力度对比（高度、斜率）
    :param big_kline: 本级别K线
    :param small_kline: 次级别K线（可选，可以是None）
    :param signal_direction: 信号方向
    :return:
    """
    direction = 1 if signal_direction == Direction.LONG else -1
    # 排除
    # 没有前线段、没有笔中枢
    # 当前线段方向与判断方向一致
    if not big_kline.pre_duan \
            or not big_kline.cur_bi_zs \
            or big_kline.cur_duan.direction == direction:
        return False

    # 如果有次级别K线时，也要判断方向
    if small_kline and (not small_kline.pre_duan or small_kline.cur_duan.direction == direction):
        return False

    # 线段内，至少有2个或以上中枢
    if len([zs for zs in big_kline.bi_zs_list[-4:] if zs.start > big_kline.cur_duan.start]) < 2:
        return False

    # 当前线段的高低点,与最高、最低分笔一致（不会出现区间套）
    if signal_direction == Direction.LONG:

        # 笔走完
        if not check_bi_not_rt(big_kline, Direction.SHORT):
            return False

        if not check_duan_not_rt(big_kline, Direction.SHORT):
            return False

        # 最后一笔
        if not duan_bi_is_end(big_kline.cur_duan, Direction.SHORT):
            return False

    else:
        # 笔走完
        if not check_bi_not_rt(big_kline, Direction.LONG):
            return False
        if not check_duan_not_rt(big_kline, Direction.LONG):
            return False
        # 最后一笔
        if not duan_bi_is_end(big_kline.cur_duan, Direction.LONG):
            return False

    # 中枢的进入笔、离开笔
    entry_bi_list = [bi for bi in big_kline.cur_duan.bi_list if bi.end <= big_kline.cur_bi_zs.start]
    exit_bi_list = [bi for bi in big_kline.cur_duan.bi_list if bi.end > big_kline.cur_bi_zs.end]
    if not (len(entry_bi_list) >= 1 and len(exit_bi_list) == 1):
        return False

    # 离开中枢的一笔，其middle必须也不在中枢内
    if signal_direction == Direction.LONG and exit_bi_list[0].middle > big_kline.cur_bi_zs.low:
        return False
    if signal_direction == Direction.SHORT and exit_bi_list[0].middle < big_kline.cur_bi_zs.high:
        return False

    # 进入中枢一笔，与离开中枢笔，方向必须相同
    if entry_bi_list[-1].direction != exit_bi_list[-1].direction:
        return False

    # 进入笔的高度，要高于离开笔，或者，进入笔的斜率，要大于离开笔
    if entry_bi_list[-1].height > exit_bi_list[0].height and entry_bi_list[-1].atan > exit_bi_list[0].atan:

        # 分析次级别K线，判断其是否也发生线段背驰
        if small_kline:
            if len(small_kline.cur_duan.bi_list) > 1:
                if (small_kline.cur_duan.bi_list[0].height > small_kline.cur_duan.bi_list[-1].height \
                    and small_kline.cur_duan.bi_list[0].atan > small_kline.cur_duan.bi_list[-1].atan) \
                        or (small_kline.cur_duan.bi_list[-3].height > small_kline.cur_duan.bi_list[-1].height \
                            and small_kline.cur_duan.bi_list[-3].atan > small_kline.cur_duan.bi_list[-1].atan):
                    return True
        else:
            return True

    return False


# def check_pz3bc_1st(big_kline, small_kline: Union[CtaLineBar, None], signal_direction: Direction):
def check_pz3bc_1st(big_kline, small_kline, signal_direction: Direction):
    """
    判断三卖后盘整背驰一买/三买后盘整背驰1卖信号
    big_kline当前线段与信号方向相反，线段具有盘整一个中枢，离开中枢的一笔力度与三买/卖信号后的一笔对比（高度、斜率）
    :param big_kline: 本级别K线
    :param small_kline: 次级别K线（可选，可以是None）
    :param signal_direction: 信号方向
    :return:
    """
    direction = 1 if signal_direction == Direction.LONG else -1

    # 排除
    # 没有前线段、没有笔中枢
    # 当前线段方向与判断方向一致、
    if not big_kline.pre_duan \
            or not big_kline.cur_bi_zs \
            or big_kline.cur_duan.direction == direction:
        return False

    # 如果有次级别K线时，也要判断方向
    if small_kline and (not small_kline.pre_duan or small_kline.cur_duan.direction == direction):
        return False

    # 当前线段必须有5笔
    if len(big_kline.cur_duan.bi_list) < 5:
        return False

    # 当前笔中枢必须在当前线段之内
    if big_kline.cur_bi_zs.start < big_kline.cur_duan.start:
        return False

    # 当前线段的高低点,与最高、最低分笔一致（不会出现区间套）
    if signal_direction == Direction.LONG:
        # 下跌线段与下跌笔为最低点
        if not duan_bi_is_end(big_kline.cur_duan, Direction.SHORT):
            return False
        # 下跌线段具有底分
        if not check_duan_not_rt(big_kline, Direction.SHORT):
            return False
        # 下跌笔具有底分
        if not check_bi_not_rt(big_kline, Direction.SHORT):
            return False
    else:
        # 上涨线段与上涨笔为最高点
        if not duan_bi_is_end(big_kline.cur_duan, Direction.LONG):
            return False
        # 上涨线段具有顶分
        if not check_duan_not_rt(big_kline, Direction.LONG):
            return False

        # 上涨笔具有顶分
        if not check_bi_not_rt(big_kline, Direction.LONG):
            return False

    # 中枢的离开笔,有三笔
    exit_bi_list = [bi for bi in big_kline.cur_duan.bi_list if bi.end > big_kline.cur_bi_zs.end]
    if len(exit_bi_list) != 3:
        return False

    # 离开中枢首笔的高度，要高于末笔笔，或者，斜率要大于末笔
    if exit_bi_list[0].height > exit_bi_list[-1].height and exit_bi_list[0].atan > exit_bi_list[-1].atan:

        # 分析次级别K线，判断其是否也发生线段背驰
        if small_kline:
            if len(small_kline.cur_duan.bi_list) > 1:
                if (small_kline.cur_duan.bi_list[0].height > small_kline.cur_duan.bi_list[-1].height \
                    and small_kline.cur_duan.bi_list[0].atan > small_kline.cur_duan.bi_list[-1].atan) \
                        or (small_kline.cur_duan.bi_list[-3].height > small_kline.cur_duan.bi_list[-1].height \
                            and small_kline.cur_duan.bi_list[-3].atan > small_kline.cur_duan.bi_list[-1].atan):
                    return True
        else:
            return True

    return False


# def check_qjt_1st(big_kline, small_kline: Union[CtaLineBar, None], signal_direction: Direction):
def check_qjt_1st(big_kline, small_kline, signal_direction: Direction):
    """
    判断区间套一买/区间套1卖信号
    big_kline当前线段与信号方向相反，线段具有盘整一个中枢，
    [一买信号为例]
    中枢前下跌一笔a，中枢后，存在两个下跌笔b、c，
    b比a力度小，c比b力度小（高度、斜率）
    :param big_kline: 本级别K线
    :param small_kline: 次级别K线（可选，可以是None）
    :param signal_direction: 信号方向
    :return:
    """
    direction = 1 if signal_direction == Direction.LONG else -1

    # 排除
    # 没有前线段、没有笔中枢
    # 当前线段方向与判断方向一致、
    if not big_kline.pre_duan \
            or not big_kline.cur_bi_zs \
            or big_kline.cur_duan.direction == direction:
        return False

    # 如果有次级别K线时，也要判断方向
    if small_kline and (not small_kline.pre_duan or small_kline.cur_duan.direction == direction):
        return False

    # 当前笔中枢必须在当前线段之内
    if big_kline.cur_bi_zs.start < big_kline.cur_duan.start:
        return False

    # 当前线段结束需要等于当前笔结束
    if big_kline.cur_duan.end != big_kline.cur_bi.end:
        return False

    # 寻找做多信号时，要求当前下跌笔底分型成立
    if signal_direction == Direction.LONG:
        # 笔走完
        if not check_bi_not_rt(big_kline, Direction.SHORT):
            return False

    # 寻找做空信号时，要求当前上涨笔顶分型成立
    else:
        # 笔走完
        if not check_bi_not_rt(big_kline, Direction.LONG):
            return False

    # 进入中枢前的一笔
    entry_bi_list = [bi for bi in big_kline.cur_duan.bi_list if bi.end <= big_kline.cur_bi_zs.start]

    # 中枢的离开笔,有三笔
    exit_bi_list = [bi for bi in big_kline.cur_duan.bi_list if bi.end > big_kline.cur_bi_zs.end]
    if len(entry_bi_list) < 1 or len(exit_bi_list) != 3:
        return False

    # c笔的高度，要高于b笔，高于进入笔a， c笔斜率要大于b笔> 进入笔a
    if exit_bi_list[0].height > exit_bi_list[-1].height > entry_bi_list[-1].height \
            and exit_bi_list[0].atan > exit_bi_list[-1].atan > entry_bi_list[-1].atan:

        # 分析次级别K线，判断其是否也发生线段背驰
        if small_kline:
            if len(small_kline.cur_duan.bi_list) > 1:
                if (small_kline.cur_duan.bi_list[0].height > small_kline.cur_duan.bi_list[-1].height \
                    and small_kline.cur_duan.bi_list[0].atan > small_kline.cur_duan.bi_list[-1].atan) \
                        or (small_kline.cur_duan.bi_list[-3].height > small_kline.cur_duan.bi_list[-1].height \
                            and small_kline.cur_duan.bi_list[-3].atan > small_kline.cur_duan.bi_list[-1].atan):
                    return True
        else:
            return True

    return False


# def check_qsbc_2nd(big_kline, small_kline: Union[CtaLineBar, None], signal_direction: Direction):
def check_qsbc_2nd(big_kline, small_kline, signal_direction: Direction):
    """
    判断趋势背驰1买/1卖后的二买、二卖信号
    big_kline当前线段为趋势，与信号方向相反，线段具有2个中枢，
    或者 big_kline的 tre_duan,pre_duan,cur_duan 为趋势，之间具有两个以上连续方向的中枢
    cur_duan的末端一笔，形成趋势背驰，或者末端一笔超长时，其次级别形成具有背驰信号
    big_kline当前段外具有两笔，最后一笔具有确认分型，斜率比cur_duan末笔的斜率小
    :param big_kline: 本级别K线
    :param small_kline: 次级别K线（可选，可以是None）
    :param signal_direction: 信号方向
    :return:
    """
    direction = 1 if signal_direction == Direction.LONG else -1
    # 排除
    # 没有前线段、没有笔中枢
    # 当前线段方向与判断方向一致
    if not big_kline.pre_duan \
            or not big_kline.cur_bi_zs \
            or big_kline.cur_duan.direction == direction:
        return False

    # 二买信号时，当前笔必须时下跌笔+底分型
    if signal_direction == Direction.LONG:
        # 若不是下跌笔，并且下跌笔没有底分型
        if not check_bi_not_rt(big_kline, Direction.SHORT):
            return False
    # 二卖信号时，当前笔必须是上涨笔+顶分型
    else:
        # 若不是上涨笔，并且上涨笔没有顶分型
        if not check_bi_not_rt(big_kline, Direction.LONG):
            return False

    # 当前线段内，至少有2个或以上中枢
    has_2_continue_zs = False
    bi_zs_in_cur_duan = [zs for zs in big_kline.bi_zs_list[-4:] if zs.start > big_kline.cur_duan.start]
    if len(bi_zs_in_cur_duan) >= 2:
        # 两个连续下跌的中枢，可以进一步判断是否满足二买做多
        if signal_direction == Direction.LONG and bi_zs_in_cur_duan[-2].low > bi_zs_in_cur_duan[-1].low:
            has_2_continue_zs = True
        # 两个连续上升的中枢，可以进一步判断是否满足二卖做空
        if signal_direction == Direction.SHORT and bi_zs_in_cur_duan[-2].high < bi_zs_in_cur_duan[-1].high:
            has_2_continue_zs = True

    # 当前线段内，不足两个中枢，判断前三个线段内，是否具有两个或两个以上中枢
    elif big_kline.tre_duan:
        # 找出三个线段内的所有中枢
        bi_zs_after_tre_duan = [zs for zs in big_kline.bi_zs_list[-4:] if zs.start > big_kline.tre_duan.start]
        if len(bi_zs_after_tre_duan) >= 2:
            if signal_direction == Direction.LONG \
                    and big_kline.tre_duan.high > big_kline.cur_duan.high \
                    and bi_zs_after_tre_duan[-2].low > bi_zs_after_tre_duan[-1].low:
                has_2_continue_zs = True
            if signal_direction == Direction.SHORT \
                    and big_kline.tre_duan.low < big_kline.cur_duan.low \
                    and bi_zs_after_tre_duan[-2].high < bi_zs_after_tre_duan[-1].high:
                has_2_continue_zs = True

    # 找不出两个连续同向的中枢，就不能进一步判断是否存在二买二卖
    if not has_2_continue_zs:
        return False

    # 当前线段外的两笔
    extra_bi_list = [bi for bi in big_kline.bi_list[-3:] if bi.start >= big_kline.cur_duan.end]
    if len(extra_bi_list) != 2:
        return False

    # 线段外一笔的高度，不能超过线段最后一笔高度
    if extra_bi_list[0].height > big_kline.cur_duan.bi_list[-1].height:
        return False

    # 最后一笔的高度，不能超过最后一段的高度的黄金分割38%
    if extra_bi_list[-1].height > big_kline.cur_duan.height * 0.38:
        return False

    # 二买情况下
    if direction == Direction.LONG:
        # 当前线段的第二低点
        if len(big_kline.cur_duan.bi_list) > 1:
            second_low = min([bi.low for bi in big_kline.cur_duan.bi_list[:-1]])
        else:
            second_low = min([bi.low for bi in big_kline.bi_list[-5:-1]])

        # 反抽上涨分笔，高度不能打破第二低点
        if extra_bi_list[0].high > second_low:
            return False
    else:
        # 当前线段的第二高点
        if len(big_kline.cur_duan.bi_list) > 1:
            second_high = max([bi.high for bi in big_kline.cur_duan.bi_list[:-1]])
        else:
            second_high = max([bi.high for bi in big_kline.bi_list[-5:-1]])
        # 反抽下跌分笔，低点不能打破第二高点
        if extra_bi_list[0].low < second_high:
            return False

    return True


# : Union[CtaLineBar, None]
def check_zs_3rd(big_kline,
                 small_kline,
                 signal_direction: Direction,
                 first_zs: bool = True,
                 all_zs: bool = True):
    """
    三买三卖信号
    :param big_kline: 本级别K线
    :param small_kline: 次级别K线
    :param signal_direction: 信号方向。Direction.LONG: 三买信号, Direction.SHORT, 三卖信号
    :param first_zs: 线段内得首个三买三卖（即第一个中枢后才有效）
    :param all_zs: True 中枢的开始，在线段开始点之后， False: 中枢结束在线段的开始点之后
    :return:
    """
    # Diection => 1/-1
    direction = 1 if signal_direction == Direction.LONG else -1

    if not big_kline.pre_duan or not big_kline.cur_bi_zs:
        return False

    # 排除，须满足：当前段的方向 == 信号方向， 当前笔的方向 != 信号方向
    if big_kline.cur_duan.direction != direction or big_kline.cur_bi.direction == direction:
        return False

    # 当前线段结束，与当前回调笔位置一致
    if big_kline.cur_duan.end != big_kline.cur_bi.start:
        return False

    zs_num = 0

    # 中枢与当前线段交集的判断
    if all_zs:

        # 信号线段，必须至少含有5个分笔（如果含有1个分笔的，可能是强二买信号）
        if len(big_kline.cur_duan.bi_list) < 3:
            return False

        # 当前中枢需要完全在当前线段内
        if big_kline.cur_bi_zs.start < big_kline.cur_duan.start:
            return False

        # 当前段之后的所有包含中枢
        zs_list = [zs for zs in big_kline.bi_zs_list[-3:] if zs.start >= big_kline.cur_duan.start]
        zs_num = len(zs_list)
        # 是否现在线段得首个中枢后的三买三卖
        if first_zs and zs_num > 1:
            return False
    else:
        # 中枢需要与当前线段有交集[部分交集、或中枢完全在当前段内形成]
        if big_kline.cur_bi_zs.end < big_kline.cur_duan.start:
            return False

        # 当前段之后的所有交集中枢
        zs_list = [zs for zs in big_kline.bi_zs_list[-3:] if zs.end > big_kline.cur_duan.start]
        zs_num = len(zs_list)
        # 是否现在线段得首个中枢后的三买三卖
        if first_zs and zs_num > 1:
            return False

    if not first_zs and zs_num > 1:
        # 中枢的进入笔、离开笔
        # 中枢的首笔与线段不同向，则选择中枢之前的一笔和最后的一笔
        if big_kline.cur_bi_zs.bi_list[0].direction != big_kline.cur_duan.direction:
            entry_bi_list = [bi for bi in big_kline.cur_duan.bi_list if bi.end <= big_kline.cur_bi_zs.start]
            exit_bi_list = [bi for bi in big_kline.cur_duan.bi_list if bi.end > big_kline.cur_bi_zs.end]
            if not (len(entry_bi_list) >= 1 and len(exit_bi_list) == 1):
                return False
            entry_bi = entry_bi_list[-1]
            exit_bi = exit_bi_list[0]

        # 中枢首笔跟线段同向
        else:
            entry_bi = big_kline.cur_bi_zs.bi_list[0]
            exit_bi = big_kline.cur_duan.bi_list[-1]
        #
        # # 防止属于中枢盘整
        if entry_bi.height > exit_bi.height and entry_bi.atan > exit_bi.atan:
            return False

    # 判断三买信号
    if signal_direction == Direction.LONG:

        # 本级别最后一笔，具有底分型
        if not check_bi_not_rt(big_kline, Direction.SHORT):
            return

        # 线段最后一笔，与中枢有交集，且笔的中心，不在中枢内
        if big_kline.cur_duan.bi_list[-1].low > big_kline.cur_bi_zs.high \
                or big_kline.cur_duan.bi_list[-1].middle <= big_kline.cur_bi_zs.high:
            return False

        # # 线段的最后一笔，长度不能超过平均长度的两倍
        # if big_kline.cur_duan.bi_list[-1].height > big_kline.bi_height_ma() * 2:
        #     return False

        # 下跌笔不落中枢，一般使用笔的底部必须在中枢上方。为了防止毛刺，这里用了分型的高位在中枢上方即可
        if big_kline.cur_fenxing.high <= big_kline.cur_bi_zs.high:
            return False

    # 判断三卖信号
    if signal_direction == Direction.SHORT:

        # 本级别最后一笔，具有顶分型
        if not check_bi_not_rt(big_kline, Direction.LONG):
            return

        # 线段最后一笔，与中枢有交集，且笔的中心，不在中枢内
        if big_kline.cur_duan.bi_list[-1].high < big_kline.cur_bi_zs.low \
                or big_kline.cur_duan.bi_list[-1].middle >= big_kline.cur_bi_zs.low:
            return False

        # # 线段的最后一笔，长度不能超过平均长度的两倍
        # if big_kline.cur_duan.bi_list[-1].height > big_kline.bi_height_ma() * 2:
        #     return False

        # 上涨分笔不回中枢，一般使用笔的顶部必须在中枢下方。为了防止毛刺，这里用了分型的低位在中枢下方即可
        if big_kline.cur_fenxing.low >= big_kline.cur_bi_zs.low:
            return False

    # 分析次级别K线，判断其是否也发生线段背驰
    if small_kline:
        if len(small_kline.cur_duan.bi_list) > 1:
            if small_kline.cur_duan.bi_list[0].height > small_kline.cur_duan.bi_list[-1].height \
                    or small_kline.cur_duan.bi_list[0].atan > small_kline.cur_duan.bi_list[-1].atan \
                    or small_kline.cur_duan.bi_list[-3].height > small_kline.cur_duan.bi_list[-1].height \
                    or small_kline.cur_duan.bi_list[-3].atan > small_kline.cur_duan.bi_list[-1].atan:
                return True
    else:
        return True
