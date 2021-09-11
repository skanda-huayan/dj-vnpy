"""
vnpy 2.x版跟单应用
华富资产，大佳

源帐号 => 跟单应用 => 目标帐号
配置步骤：
1、源帐号需要添加RpcServerApp，并启动。（无界面或有界面都可以）
2、目标帐号交易程序，添加TradeCopyApp，配置源账号的Rep/Pub地址

跟单规则：源帐号 仓位 * 倍率 => 目标帐号的目标仓位
"""
import os
import csv
from threading import Thread
from queue import Queue, Empty
from copy import copy
from collections import defaultdict, namedtuple
from datetime import datetime
import logging
from vnpy.event import Event, EventEngine
from vnpy.trader.engine import BaseEngine, MainEngine
from vnpy.trader.constant import Exchange
from vnpy.trader.object import (
    SubscribeRequest,
    OrderRequest,
    Offset,
    Direction,
    OrderType,
    TickData,
    ContractData
)

from vnpy.rpc import RpcClient
from vnpy.trader.event import EVENT_TICK, EVENT_CONTRACT, EVENT_POSITION, EVENT_TIMER

from vnpy.trader.utility import load_json, save_json, extract_vt_symbol
from vnpy.app.spread_trading.base import EVENT_SPREAD_DATA, SpreadData

APP_NAME = "TradeCopy"
EVENT_TRADECOPY_LOG = "eTradeCopyLog"
EVENT_TRADECOPY = 'eTradeCopy'


class TradeCopyEngine(BaseEngine):
    """
    跟单交易
    source ==> trade_copy ==> target
    """
    setting_filename = "trade_copy_setting.json"

    def __init__(self, main_engine: MainEngine, event_engine: EventEngine):
        """"""
        super().__init__(main_engine, event_engine, APP_NAME)

        # 是否激活跟单
        self.active = False

        # 持仓字典 合约+方向: vt_symbol, direction, src_volume,  target_volume, cur_volume, count
        self.pos_dict = {}

        # 被跟单得rpc server得请求端口、广播端口
        self.source_rpc_rep = 'tcp://localhost:2015'
        self.source_rpc_pub = 'tcp://localhost:2018'

        # 跟单比率(1 ~ n)
        # target_pos_volume = round(source_pos_volume * copy_ratio)
        # 简单得四舍五入
        self.copy_ratio = 1

        # 跟单检查间隔（缺省5秒）
        self.copy_interval = 5

        # 定时器
        self.timer_count = 0

        # RPC客户端，用于连接源帐号
        self.client = RpcClient()

        # 回调函数
        self.client.callback = self.client_callback

        # 接受本地position event更新
        self.accept_local = False

        # 加载配置
        self.load_setting()

        # 注册事件
        self.register_event()

    def load_setting(self):
        """
        加载配置
        :return:
        """
        try:
            setting = load_json(self.setting_filename)
            self.source_rpc_rep = setting.get("source_rpc_rep", "")
            self.source_rpc_pub = setting.get("source_rpc_pub", "")
            self.copy_ratio = setting.get('copy_ratio', 1)
            self.copy_interval = setting.get('copy_interval', 5)

        except Exception as ex:
            self.write_log(f'{APP_NAME}加载配置文件{self.setting_filename}异常{str(ex)}')

    def save_setting(self):
        """
        保存设置
        :return:
        """
        setting = {
            "source_rpc_rep": self.source_rpc_rep,
            "source_rpc_pub": self.source_rpc_pub,
            "copy_ratio": self.copy_ratio,
            'copy_interval': self.copy_interval
        }
        save_json(self.setting_filename, setting)
        self.write_log(f'保存设置完毕:{setting}')

    def register_event(self):
        """
        注册事件处理
        :return:
        """
        self.event_engine.register(EVENT_POSITION, self.process_position_event)
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)

    def client_callback(self, topic: str, event: Event):
        """
        rpc客户端（源帐号）得event回报
        :param topic:
        :param event:
        :return:
        """
        if event is None:
            print("none event", topic, event)
            return

        # 只处理持仓事件
        if event.type == EVENT_POSITION:
            src_pos = event.data

            # 不处理套利合约
            if ' ' in src_pos.symbol or '&' in src_pos.symbol:
                return

            # key = 合约+方向
            k = f'{src_pos.vt_symbol}.{src_pos.direction.value}'
            pos = self.pos_dict.get(k, {})

            # 更新持仓中属于source得部分, 计算出目标持仓
            target_volume = int(round(src_pos.volume * self.copy_ratio))
            pos.update({'vt_symbol': src_pos.vt_symbol,
                        'direction': src_pos.direction,
                        'name': src_pos.name,
                        'symbol': src_pos.symbol,
                        'src_volume': src_pos.volume,
                        'target_volume': target_volume,
                        'count': pos.get('count', 0) + 1,
                        'volume': pos.get('volume', 0),
                        'yd_volume': pos.get('yd_volume', 0),
                        'cur_price': pos.get('cur_price', 0),
                        'price': pos.get('price', 0),
                        'diff': pos.get('diff', target_volume - pos.get('volume', 0))
                        })

            self.pos_dict.update({k: pos})

    def process_position_event(self, event: Event):
        """
        处理本地帐号得持仓更新事件
        :param event:
        :return:
        """
        # 必须等到rpc的仓位都到齐了，才接收本地仓位
        if not self.accept_local:
            return

        cur_pos = event.data

        # 不处理套利合约
        if ' ' in cur_pos.symbol or '&' in cur_pos.symbol:
            return

        # key = 合约+方向
        k = f'{cur_pos.vt_symbol}.{cur_pos.direction.value}'
        pos = self.pos_dict.get(k, {})

        # 更新持仓中属于source得部分
        pos.update({
            'cur_positionid': cur_pos.vt_positionid,
            'volume': cur_pos.volume,
            'yd_volume': cur_pos.yd_volume,
            'price': cur_pos.price,
            'cur_price': cur_pos.cur_price,
            'diff': pos.get('target_volume', 0) - cur_pos.volume,
            'count': pos.get('count', 0) + 1
        })
        if 'name' not in pos:
            pos.update({'name': cur_pos.name})
        if 'vt_symbol' not in pos:
            pos.update({'vt_symbol': cur_pos.vt_symbol})
        if 'direction' not in pos:
            pos.update({'direction': cur_pos.direction})
        if 'symbol' not in pos:
            pos.update({'symbol': cur_pos.symbol})
        if 'src_volume' not in pos:
            pos.update({'src_volume': 0})
        if 'target_volume' not in pos:
            pos.update({'target_volume': 0})

        self.pos_dict.update({k: pos})

    def put_event(self):
        """
        更细监控表
        :return:
        """
        for key in self.pos_dict.keys():
            pos = self.pos_dict.get(key, {})
            # 补充key
            pos.update({'vt_positionid': key})
            # dict => object
            data = namedtuple("TradeCopy", pos.keys())(*pos.values())
            # 推送事件
            event = Event(
                EVENT_TRADECOPY,
                data
            )
            self.event_engine.put(event)

    def process_timer_event(self, event: Event):
        """定时执行"""
        self.timer_count += 1

        if self.timer_count % 2 == 0:
            self.put_event()

        if self.timer_count < self.copy_interval:
            return

        self.timer_count = 0

        # 未激活，不执行
        if not self.active:
            return

        # 执行跟单仓位比较
        for key in self.pos_dict.keys():
            pos = self.pos_dict.get(key)

            # 等到两次rpc pos后，才激活本地持仓更新
            if not self.accept_local and pos.get('count', 0) > 2:
                self.accept_local = True
                self.write_log(f'激活本地持仓更新')

            # 需要20次更新pos才算有效
            if pos.get('count', 0) <= 20:
                continue

            target_volume = pos.get('target_volume', 0)
            cur_volume = pos.get('volume', 0)
            direction = pos.get('direction')

            # 目标仓位 > 当前仓位， 需要开仓
            if target_volume > cur_volume >= 0:
                volume = target_volume - cur_volume
                self.open_pos(vt_symbol=pos.get('vt_symbol'),
                              direction=direction,
                              volume=volume)
                continue

            # 目标仓位 < 当前仓位， 需要减仓
            if 0 <= target_volume < cur_volume:
                # 减仓数量
                volume = cur_volume - target_volume

                # 平仓相反方向
                if direction == Direction.LONG:
                    direction = Direction.SHORT
                else:
                    direction = Direction.LONG

                self.close_pos(vt_symbol=pos.get('vt_symbol'),
                               direction=direction,
                               volume=volume,
                               vt_positionid=pos.get('cur_positionid'))

    def open_pos(self, vt_symbol, direction, volume):
        """
        买入、或做空
        :param vt_symbol:
        :param direction:
        :param volume:
        :return:
        """
        cur_tick = self.main_engine.get_tick(vt_symbol)
        contract = self.main_engine.get_contract(vt_symbol)
        symbol, exchange = extract_vt_symbol(vt_symbol)
        if contract is None:
            self.write_log(f'异常，{vt_symbol}的合约信息不存在')
            return

        if cur_tick is None:
            req = SubscribeRequest(
                symbol=symbol,
                exchange=exchange
            )
            self.main_engine.subscribe(req, contract.gateway_name)
            self.write_log(f'订阅合约{vt_symbol}')
            return

        dt_now = datetime.now()

        # 最新tick的时间，与当前的时间超过间隔，不处理（例如休盘时间）
        if (dt_now - cur_tick.datetime).total_seconds() > self.copy_interval:
            self.write_log(f'{vt_symbol} 最后tick时间{cur_tick.datetime}不满足开仓要求,当前时间:{dt_now}')
            return

        open_price = cur_tick.ask_price_1 if direction == Direction.LONG else cur_tick.bid_price_1

        order = OrderRequest(
            symbol=symbol,
            exchange=exchange,
            direction=direction,
            offset=Offset.OPEN,
            volume=volume,
            price=open_price,
            type=OrderType.FAK
        )
        self.write_log(f'发出委托开仓,{vt_symbol}, {direction.value},{volume},{open_price} ')
        self.main_engine.send_order(order, contract.gateway_name)

    def close_pos(self, vt_symbol, direction, volume, vt_positionid):
        """
        sell or cover
        :param vt_symbol:
        :param direction:
        :param volume:
        :return:
        """
        cur_tick = self.main_engine.get_tick(vt_symbol)
        contract = self.main_engine.get_contract(vt_symbol)
        cur_pos = self.main_engine.get_position(vt_positionid)
        if contract is None:
            self.write_log(f'异常，{vt_symbol}的合约信息不存在')
            return

        symbol, exchange = extract_vt_symbol(vt_symbol)
        if cur_tick is None:
            req = SubscribeRequest(
                symbol=symbol,
                exchange=exchange
            )
            self.main_engine.subscribe(req, contract.gateway_name)
            self.write_log(f'订阅合约{vt_symbol}')
            return

        if cur_pos is None:
            self.write_log(f'异常，{vt_positionid}的持仓信息不存在')
            return

        dt_now = datetime.now()

        # 最新tick的时间，与当前的时间超过间隔，不处理（例如休盘时间）
        if (dt_now - cur_tick.datetime).total_seconds() > self.copy_interval:
            self.write_log(f'{vt_symbol} 最后tick时间{cur_tick.datetime}不满足开仓要求,当前时间:{dt_now}')
            return

        close_price = cur_tick.ask_price_1 if direction == Direction.LONG else cur_tick.bid_price_1

        offset = Offset.CLOSE
        if exchange in [Exchange.SHFE, Exchange.CFFEX and Exchange.INE]:

            # 优先平昨仓
            if cur_pos.yd_volume > 0:
                # 平昨
                offset = Offset.CLOSEYESTERDAY

                # 如果平昨数量不够，平掉所有昨仓，剩余仓位，下次再平
                if cur_pos.yd_volume < volume:
                    self.write_log(f'{vt_symbol} 平仓数量:{volume} => {cur_pos.yd_volume}')
                    volume = cur_pos.yd_volume
            else:
                offset = Offset.CLOSETODAY

        order = OrderRequest(
            symbol=symbol,
            exchange=exchange,
            direction=direction,
            offset=offset,
            volume=volume,
            price=close_price,
            type=OrderType.FAK
        )
        self.write_log(f'发出委托开仓,{vt_symbol}, {direction.value},{volume},{close_price} ')
        self.main_engine.send_order(order, contract.gateway_name)

    def start_copy(self, source_req_addr, source_pub_addr, copy_ratio, copy_interval):
        """
        开始执行跟单
        :return:
        """
        # 订阅事件
        self.client.subscribe_topic("")

        if source_req_addr != self.source_rpc_rep:
            self.source_rpc_rep = source_req_addr
        if source_pub_addr != self.source_rpc_pub:
            self.source_rpc_pub = source_pub_addr
        if copy_ratio != self.copy_ratio:
            self.copy_ratio = copy_ratio
        if copy_interval != self.copy_interval and self.copy_interval >= 1:
            self.copy_interval = copy_interval
        self.write_log(f'保存设置')
        self.save_setting()

        # 连接rpc客户端
        self.write_log(f'开始连接rpc客户端')
        self.client.start(self.source_rpc_rep, self.source_rpc_pub)

        # 激活
        self.write_log(f'激活跟单')
        self.active = True

    def stop_copy(self):
        """
        停止跟单
        :return:
        """
        self.active = False
        self.write_log(f'停止跟单')

    def write_log(self, msg: str, source: str = "", level: int = logging.DEBUG):
        """
        更新日志
        :param msg:
        :param source:
        :param level:
        :return:
        """
        self.event_engine.put(Event(EVENT_TRADECOPY_LOG, msg))
        super().write_log(msg, source, level)
