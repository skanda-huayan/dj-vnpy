# encoding: UTF-8

# 通达信指数行情发布器
# 华富资产

import copy
import json
import traceback
from threading import Thread
from datetime import datetime, timedelta
from time import sleep
from logging import ERROR
from pytdx.exhq import TdxExHq_API
from copy import deepcopy

from vnpy.event import EventEngine
from vnpy.trader.constant import Exchange
from vnpy.trader.engine import BaseEngine, MainEngine
from vnpy.trader.event import EVENT_TIMER
from vnpy.trader.object import TickData, SubscribeRequest
from vnpy.trader.utility import get_trading_date, get_underlying_symbol, load_json, get_real_symbol_by_exchange
from vnpy.data.tdx.tdx_common import TDX_FUTURE_HOSTS, get_future_contracts
from vnpy.component.base import (
    NIGHT_MARKET_23,
    NIGHT_MARKET_SQ2,
    MARKET_DAY_ONLY)

from vnpy.amqp.producer import publisher
from vnpy.gateway.ctp.ctp_gateway import CtpMdApi, symbol_exchange_map

APP_NAME = 'Idx_Publisher'


class IndexTickPublisherV2(BaseEngine):
    """
    指数tick发布服务
    透过ctp 行情接口，获取所有合约，并根据合约的仓指，生成指数tick，发布至rabbitMQ
    """

    # ----------------------------------------------------------------------
    def __init__(self, main_engine: MainEngine, event_engine: EventEngine):
        """"""
        super(IndexTickPublisherV2, self).__init__(
            main_engine, event_engine, APP_NAME)

        self.main_engine = main_engine
        self.event_engine = event_engine
        self.create_logger(logger_name=APP_NAME)
        self.gateway_name = 'CTP'
        self.last_minute = None

        self.registerEvent()

        self.connection_status = False  # 连接状态

        #  ctp md api
        self.subscribed_symbols = set()  # 已订阅合约代码

        self.md_api = None  # API 的连接会话对象
        self.last_tick_dt = {}  # 记录该会话对象的最后一个tick时间

        self.instrument_count = 50000

        self.has_qry_instrument = False

        # vt_setting.json内rabbitmq配置项
        self.conf = {}
        self.pub = None

        self.status = {}
        self.subscribed_symbols = set()  # 已订阅合约代码
        self.ticks = {}

        self.dt = datetime.now()
        # 本地/vnpy/data/tdx/future_contracts.json
        self.all_contracts = get_future_contracts()
        # 需要订阅的短合约
        self.selected_underly_symbols = load_json('subscribe_symbols.json', auto_save=False)

        # 短合约 <=> 所有真实合约 的数量
        self.underly_symbols_num_dict = {}

    def write_error(self, content: str):
        self.write_log(msg=content, level=ERROR)

    def create_publisher(self, conf):
        """创建rabbitmq 消息发布器"""
        if self.pub:
            return
        try:
            self.write_log(f'创建发布器:{conf}')
            # 消息发布
            self.pub = publisher(host=conf.get('host', 'localhost'),
                                 port=conf.get('port', 5672),
                                 user=conf.get('user', 'admin'),
                                 password=conf.get('password', 'admin'),
                                 channel_number=conf.get('channel_number', 1),
                                 queue_name=conf.get('queue_name', ''),
                                 routing_key=conf.get('routing_key', 'default'),
                                 exchange=conf.get('exchange', 'x_fanout_idx_tick'))
            self.write_log(f'创建发布器成功')
        except Exception as ex:
            self.write_log(u'创建tick发布器异常:{}'.format(str(ex)))

    # ----------------------------------------------------------------------
    def registerEvent(self):
        """注册事件监听"""
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)

    def process_timer_event(self, event):
        """定时执行"""
        self.dt = datetime.now()

        if self.last_minute and self.dt.minute == self.last_minute:
            return

        self.last_minute = self.dt.minute

        self.check_status()

    def check_status(self):
        """定期检查状态"""
        if not self.md_api:
            self.status.update({'con': False})
            self.write_log(f'行情接口未连接')
            return
        dt_now = datetime.now()

        # 扫描合约配置文件
        for underly_symbol, info in self.all_contracts.items():
            # 如果本地subscribe_symbols内有合约的指定订阅清单，进行排除 ['RB','IF']
            if len(self.selected_underly_symbols) > 0 and underly_symbol not in self.selected_underly_symbols:
                continue

            # 日盘数据，夜盘期间不订阅
            if dt_now.hour < 4 or dt_now.hour > 20:
                if underly_symbol in MARKET_DAY_ONLY:
                    continue

            # 获取当前所有的合约列表
            symbols = info.get('symbols', {})
            # 获取交易所
            exchange = info.get('exchange', 'LOCAL')
            # 获取本地记录的tick dict
            tick_dict = self.ticks.get(underly_symbol, {})

            for symbol in symbols.keys():
                # 全路径合约 => 标准合约 ,如 ZC2109 => ZC109, RB2110 => rb2110
                vn_symbol = get_real_symbol_by_exchange(symbol, Exchange(exchange))

                if symbol.replace(underly_symbol, '') < dt_now.strftime('%Y%m%d'):
                    self.write_log(f'移除早于当月的合约{symbol}')
                    symbols.pop(symbol, None)
                    continue
                # 生成带交易所信息的合约
                vt_symbol = f'{vn_symbol}.{exchange}'
                # symbol_exchange_map是全局变量，ctp md api会使用到，所以需要更新其 合约与交易所的关系
                if vn_symbol not in symbol_exchange_map:
                    symbol_exchange_map.update({vn_symbol: Exchange(exchange)})

                # 该合约没有在行情中，重新发出订阅
                if vt_symbol not in tick_dict:
                    req = SubscribeRequest(
                        symbol=vn_symbol,
                        exchange=Exchange(exchange)
                    )
                    self.subscribe(req)

            # 等级短合约 <=> 真实合约数量
            self.underly_symbols_num_dict.update({underly_symbol: len(symbols.keys())})

    def connect(self, *args, **kwargs):
        """
        连接ctp行情，和rabbitmq推送
        :param args:
        :param kwargs:
        :return:
        """
        self.write_log(f'connect({kwargs}')

        # 连接ctp行情服务器
        md_address = kwargs.get('md_address')
        userid = kwargs.get('userid')
        password = kwargs.get('password')
        brokerid = kwargs.get('brokerid')
        if not self.md_api:
            self.write_log(f'创建ctp行情服务器{md_address}')
            self.md_api = CtpMdApi(gateway=self)
        self.md_api.connect(address=md_address,
                            userid=userid,
                            password=password,
                            brokerid=brokerid)

        # 连接rabbit MQ
        rabbit_config = kwargs.get('rabbit_config', {})
        self.write_log(f'创建rabbitMQ 消息推送桩,{rabbit_config}')
        self.conf.update(rabbit_config)
        self.create_publisher(self.conf)

    def subscribe(self, req: SubscribeRequest):
        """订阅合约"""
        self.write_log(f'engine:订阅合约: {req.vt_symbol}')

        if req.vt_symbol not in self.subscribed_symbols:
            self.subscribed_symbols.add(req.vt_symbol)

        if self.md_api:
            self.md_api.subscribe(req)

    def on_tick(self, tick):
        """ tick到达事件"""

        # 排除tick时间与当前时间不一致tick
        if abs((tick.datetime - self.dt).total_seconds()) > 20:
            return

        short_symbol = get_underlying_symbol(tick.symbol).upper()
        # 更新tick
        tick_dict = self.ticks.get(short_symbol, None)
        if tick_dict is None:
            tick_dict = {tick.symbol: tick}
            self.ticks.update({short_symbol: tick_dict})
            return

        # 与最后
        last_dt = self.last_tick_dt.get(short_symbol, tick.datetime)

        # 进行指数合成
        if last_dt and tick.datetime.second != last_dt.second:
            all_amount = 0
            all_interest = 0
            all_volume = 0
            all_ask1 = 0
            all_bid1 = 0
            last_price = 0
            ask_price_1 = 0
            bid_price_1 = 0
            mi_tick = None

            # 已经积累的行情tick数量，不足总数减1，不处理
            n = self.underly_symbols_num_dict.get(short_symbol, 1)
            if len(tick_dict) < min(n*0.8, 3) :
                self.write_log(f'{short_symbol}合约数据{len(tick_dict)}不足{n} 0.8,暂不合成指数')
                return

            # 计算所有合约的累加持仓量、资金、成交量、找出最大持仓量的主力合约
            for t in tick_dict.values():
                all_interest += t.open_interest
                all_amount += t.last_price * t.open_interest
                all_volume += t.volume
                all_ask1 += t.ask_price_1 * t.open_interest
                all_bid1 += t.bid_price_1 * t.open_interest
                if mi_tick is None or mi_tick.open_interest < t.open_interest:
                    mi_tick = t

            # 总量 > 0
            if all_interest > 0 and all_amount > 0:
                last_price = round(float(all_amount / all_interest), 4)
            # 卖1价
            if all_ask1 > 0 and all_interest > 0:
                ask_price_1 = round(float(all_ask1 / all_interest), 4)
            # 买1价
            if all_bid1 > 0 and all_interest > 0:
                bid_price_1 = round(float(all_bid1 / all_interest), 4)

            if mi_tick and last_price > 0:
                if self.pub:
                    d = copy.copy(mi_tick.__dict__)
                    # 时间 =》 字符串
                    if isinstance(mi_tick.datetime, datetime):
                        d.update({'datetime': mi_tick.datetime.strftime('%Y-%m-%d %H:%M:%S.%f')})
                    # 变量 => 字符串
                    d.update({'exchange': mi_tick.exchange.value})
                    d.update({'symbol': f'{short_symbol}99', 'vt_symbol': f'{short_symbol}99.{mi_tick.exchange.value}'})
                    # 更新未指数的持仓量、交易量，最后价格，ask1，bid1
                    d.update({'open_interest': all_interest, 'volume': all_volume,
                              'last_price': last_price, 'ask_price_1': ask_price_1, 'bid_price_1': bid_price_1})
                    #print('{} {}:{}'.format(d.get('datetime'), d.get("vt_symbol"), d.get('last_price')))
                    d = json.dumps(d)
                    self.pub.pub(d)

        # 更新时间
        self.last_tick_dt.update({short_symbol: tick.datetime})

        tick_dict.update({tick.symbol: tick})
        self.ticks.update({short_symbol: tick_dict})

    def on_custom_tick(self, tick):
        pass


class IndexTickPublisher(BaseEngine):
    # 指数tick发布服务
    # 通过通达信接口，获取指数行情tick，发布至rabbitMQ

    # ----------------------------------------------------------------------
    def __init__(self, main_engine: MainEngine, event_engine: EventEngine):
        """"""
        super(IndexTickPublisher, self).__init__(
            main_engine, event_engine, APP_NAME)

        self.main_engine = main_engine
        self.event_engine = event_engine
        self.create_logger(logger_name=APP_NAME)

        self.last_minute = None

        self.registerEvent()

        self.req_interval = 0.5  # 操作请求间隔500毫秒
        self.req_id = 0  # 操作请求编号
        self.connection_status = False  # 连接状态

        self.symbol_exchange_dict = {}  # tdx合约与vn交易所的字典
        self.symbol_market_dict = {}  # tdx合约与tdx市场的字典
        self.symbol_tick_dict = {}  # tdx合约与最后一个Tick得字典

        # self.queue = Queue()            # 请求队列
        self.pool = None  # 线程池
        self.req_thread = None  # 定时器线程

        self.ip_list = TDX_FUTURE_HOSTS

        #  tdx api
        self.fail_ip_dict = {}  # 失效得API 的连接服务器配置： IP_port: 分钟倒数
        self.best_ip = None
        self.best_port = None
        self.best_name = None
        self.api = None  # API 的连接会话对象
        self.last_tick_dt = None  # 记录该会话对象的最后一个tick时间

        self.last_sort_speed_dt = None
        self.instrument_count = 50000

        self.has_qry_instrument = False

        # vt_setting.json内rabbitmq配置项
        self.conf = {}
        self.pub = None

    def write_error(self, content: str):
        self.write_log(msg=content, level=ERROR)

    def create_publisher(self, conf):
        """创建rabbitmq 消息发布器"""
        if self.pub:
            return
        try:
            # 消息发布
            self.pub = publisher(host=conf.get('host', 'localhost'),
                                 port=conf.get('port', 5672),
                                 user=conf.get('user', 'admin'),
                                 password=conf.get('password', 'admin'),
                                 channel_number=conf.get('channel_number', 1),
                                 queue_name=conf.get('queue_name', ''),
                                 routing_key=conf.get('routing_key', 'default'),
                                 exchange=conf.get('exchange', 'x_fanout_idx_tick'))
        except Exception as ex:
            self.write_log(u'创建tick发布器异常:{}'.format(str(ex)))

    # ----------------------------------------------------------------------
    def registerEvent(self):
        """注册事件监听"""
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)

    def process_timer_event(self, event):
        """定时执行"""
        dt = datetime.now()

        if dt.minute == self.last_minute:
            return

        # 更新失效IP地址得counter
        for k in list(self.fail_ip_dict.keys()):
            c = self.fail_ip_dict.get(k, 0)
            if c <= 0:
                self.fail_ip_dict.pop(k, None)
            else:
                c -= 1
                self.fail_ip_dict.update({k: c})

        self.check_status()

    # ----------------------------------------------------------------------
    def ping(self, ip, port=7709):
        """
        ping行情服务器
        :param ip:
        :param port:
        :param type_:
        :return:
        """
        apix = TdxExHq_API()
        __time1 = datetime.now()
        try:
            with apix.connect(ip, port):
                if apix.get_instrument_count() > 10000:
                    _timestamp = (datetime.now() - __time1).total_seconds() * 1000
                    self.write_log('服务器{}:{},耗时:{}ms'.format(ip, port, _timestamp))
                    return _timestamp
                else:
                    self.write_log(u'该服务器IP {}无响应.'.format(ip))
                    return timedelta(seconds=10).total_seconds() * 1000
        except Exception as ex:
            self.write_error(u'tdx ping服务器{}，异常的响应{}'.format(ip, str(ex)))
            return timedelta(seconds=10).total_seconds() * 1000

    def sort_ip_speed(self):
        """
        对所有服务器进行速度排序
        :return:
        """
        speed_result = []
        for x in self.ip_list:
            speed = self.ping(x['ip'], x['port'])
            x.update({'speed': speed})
            speed_result.append(copy.copy(x))

        # 更新服务器，按照速度排序
        self.ip_list = sorted(speed_result, key=lambda s: s['speed'])
        self.write_log(u'服务器访问速度排序:{}'.format(self.ip_list))

    # ----------------------------------------------------------------------
    def select_best_ip(self):
        """
        选择行情服务器
        :return: IP地址， 端口， 服务器名称
        """
        self.write_log(u'选择通达信行情服务器')
        if self.last_sort_speed_dt is None or (datetime.now() - self.last_sort_speed_dt).total_seconds() > 60:
            self.sort_ip_speed()
            self.last_sort_speed_dt = datetime.now()

        valid_ip_list = [x for x in self.ip_list if x.get('speed', 10000) < 10000]

        if len(valid_ip_list) == 0:
            self.write_error(u'未能找到合适速度得行情服务器')
            return None, None, None

        for server in valid_ip_list:
            ip = server.get('ip')
            port = server.get('port')
            name = server.get('name', '{}:{}'.format(ip, port))
            if '{}:{}'.format(ip, port) in self.fail_ip_dict:
                self.write_log(u'{}:{}属于上次异常IP地址，忽略'.format(ip, port))
                continue
            return ip, port, name

        return None, None, None

    def connect(self, rabbit_config: dict):
        """
        连接通达讯行情服务器
        :param n:
        :return:
        """
        if self.connection_status:
            if self.api is not None or getattr(self.api, "client", None) is not None:
                self.write_log(u'当前已经连接,不需要重新连接')
                return

        self.write_log(u'开始通达信行情服务器')

        try:
            self.api = TdxExHq_API(heartbeat=True, auto_retry=True, raise_exception=True)
            # 选取最佳服务器
            self.best_ip, self.best_port, self.best_name = self.select_best_ip()

            if self.best_ip is None or self.best_port is None:
                self.write_error(u'未能选择到服务器')

            self.write_log(u'api 选择 {}: {}:{}'.format(self.best_name, self.best_ip, self.best_port))
            self.api.connect(self.best_ip, self.best_port)
            # 尝试获取市场合约统计
            c = self.api.get_instrument_count()
            if c is None or c < 10:
                err_msg = u'该服务器IP {}/{}无响应'.format(self.best_ip, self.best_port)
                self.fail_ip_dict.update({'{}:{}'.format(self.best_ip, self.best_port): 10})
                self.write_error(err_msg)
            else:
                self.write_log(u'创建tdx连接')
                self.last_tick_dt = datetime.now()
                self.connection_status = True
                self.instrument_count = c

        except Exception as ex:
            self.write_error(u'连接服务器tdx异常:{},{}'.format(str(ex), traceback.format_exc()))
            self.fail_ip_dict.update({'{}:{}'.format(self.best_ip, self.best_port): 10})
            return

        # 更新 symbol_exchange_dict , symbol_market_dict
        self.write_log(u'查询合约')
        self.qry_instrument()

        self.conf.update(rabbit_config)
        self.create_publisher(self.conf)

        self.req_thread = Thread(target=self.run)
        self.req_thread.start()

    def reconnect(self):
        """
        重连

        :return:
        """
        try:
            self.best_ip, self.best_port, self.best_name = self.select_best_ip()
            self.api = TdxExHq_API(heartbeat=True, auto_retry=True)
            self.api.connect(self.best_ip, self.best_port)
            # 尝试获取市场合约统计
            c = self.api.get_instrument_count()
            if c is None or c < 10:
                err_msg = u'该服务器IP {}/{}无响应'.format(self.best_ip, self.best_port)
                self.fail_ip_dict.update({'{}:{}'.format(self.best_ip, self.best_port): 10})
                self.write_error(err_msg)
            else:
                self.write_log(u'重新创建tdx连接')
            sleep(1)
        except Exception as ex:
            self.write_error(u'重新连接服务器异常:{},{}'.format(str(ex), traceback.format_exc()))
            self.fail_ip_dict.update({'{}:{}'.format(self.best_ip, self.best_port): 10})
            return

    def close(self):
        """退出API"""
        self.write_log(u'退出tdx API')
        self.connection_status = False

        if self.req_thread is not None:
            self.write_log(u'退出请求线程')
            self.req_thread.join()

        if self.pub:
            self.write_log(u'退出rabbitMQ 发布器')
            self.pub.exit()

    def check_status(self):
        self.write_log(u'检查tdx接口状态')
        if len(self.symbol_tick_dict) > 0:
            k = self.symbol_tick_dict.keys()[0]
            tick = self.symbol_tick_dict.get(k, None)
            if tick:
                self.write_log(f'{tick.vt_symbol}: {tick.datetime}, price:{tick.last_price}')
        else:
            self.write_log(f'目前没有收到tick')

        # 若还没有启动连接，就启动连接
        over_time = self.last_tick_dt is None or (datetime.now() - self.last_tick_dt).total_seconds() > 60
        if not self.connection_status or self.api is None or over_time:
            self.write_log(u'tdx还没有启动连接，就启动连接')
            self.close()
            self.api = None
            self.reconnect()
        else:
            self.write_log(u'tdx接口状态正常')

    def qry_instrument(self):
        """
        查询/更新合约信息
        :return:
        """
        if not self.connection_status:
            self.write_error(u'tdx连接状态为断开，不能查询和更新合约信息')
            return

        if self.has_qry_instrument:
            self.write_error(u'已经查询过一次合约信息，不再查询')
            return

        # 取得所有的合约信息
        num = self.api.get_instrument_count()
        if not isinstance(num, int):
            return

        all_contacts = sum(
            [self.api.get_instrument_info((int(num / 500) - i) * 500, 500) for i in range(int(num / 500) + 1)], [])
        # [{"category":category,"market": int,"code":sting,"name":string,"desc":string},{}]

        # 对所有合约处理，更新字典 指数合约-tdx市场，指数合约-交易所
        for tdx_contract in all_contacts:
            tdx_symbol = tdx_contract.get('code', None)
            if tdx_symbol is None or tdx_symbol[-2:] not in ['L9']:
                continue
            tdx_market_id = tdx_contract.get('market')
            self.symbol_market_dict[tdx_symbol] = tdx_market_id
            if tdx_market_id == 47:  # 中金所
                self.symbol_exchange_dict[tdx_symbol] = Exchange.CFFEX
            elif tdx_market_id == 28:  # 郑商所
                self.symbol_exchange_dict[tdx_symbol] = Exchange.CZCE
            elif tdx_market_id == 29:  # 大商所
                self.symbol_exchange_dict[tdx_symbol] = Exchange.DCE
            elif tdx_market_id == 30:  # 上期所+能源
                self.symbol_exchange_dict[tdx_symbol] = Exchange.SHFE
            elif tdx_market_id == 60:  # 主力合约
                self.write_log(u'主力合约:{}'.format(tdx_contract))
        self.has_qry_instrument = True

    def run(self):
        # 版本3 ：直接查询板块
        try:
            last_dt = datetime.now()
            self.write_log(u'开始运行tdx,{}'.format(last_dt))
            while self.connection_status:
                try:
                    self.process_index_req()
                except BrokenPipeError as bex:
                    self.write_error(u'BrokenPipeError{},重试重连tdx[{}]'.format(str(bex), 0))
                    self.fail_ip_dict.update({'{}:{}'.format(self.best_ip, self.best_port): 10})
                    self.reconnect()
                    sleep(5)
                    break
                except Exception as ex:
                    self.write_error(u'tdx exception:{},{}'.format(str(ex), traceback.format_exc()))
                    self.fail_ip_dict.update({'{}:{}'.format(self.best_ip, self.best_port): 10})
                    self.reconnect()

                sleep(self.req_interval)
                dt = datetime.now()
                if last_dt.minute != dt.minute:
                    self.write_log('tdxcheck point. {},last_tick_dt:{}'.format(dt, self.last_tick_dt))
                    last_dt = dt
        except Exception as ex:
            self.write_error(u'tdx pool.run exception:{},{}'.format(str(ex), traceback.format_exc()))

        self.write_error(u'tdx 线程 {}退出'.format(datetime.now()))

    def process_index_req(self):
        """处理板块获取指数行情tick"""

        # 获取通达信指数板块所有行情
        rt_list = self.api.get_instrument_quote_list(42, 3, 0, 100)

        if rt_list is None or len(rt_list) == 0:
            self.write_log(u'tdx:get_instrument_quote_list() rt_list为空')
            return

        # 记录该接口的行情最后更新时间
        self.last_tick_dt = datetime.now()

        for d in list(rt_list):
            tdx_symbol = d.get('code', None)
            if tdx_symbol.endswith('L9'):
                vn_symbol = tdx_symbol.replace('L9', '99').upper()
            else:
                vn_symbol = tdx_symbol.upper()

            tick_datetime = datetime.now()
            # 修正毫秒
            last_tick = self.symbol_tick_dict.get(vn_symbol, None)
            if (last_tick is not None) and tick_datetime.replace(microsecond=0) == last_tick.datetime:
                # 与上一个tick的时间（去除毫秒后）相同,修改为500毫秒
                tick_datetime = tick_datetime.replace(microsecond=500)
            else:
                tick_datetime = tick_datetime.replace(microsecond=0)

            # 通达信上能源的交易所为上期所，需要改正过来
            if vn_symbol in ['NR99', 'SC99']:
                exchange = Exchange.INE
            else:
                exchange = self.symbol_exchange_dict.get(tdx_symbol, Exchange.LOCAL)

            tick = TickData(
                gateway_name='tdx',
                symbol=vn_symbol,
                datetime=tick_datetime,
                exchange=exchange
            )

            tick.pre_close = float(d.get('ZuoJie', 0.0))
            tick.high_price = float(d.get('ZuiGao', 0.0))
            tick.open_price = float(d.get('JinKai', 0.0))
            tick.low_price = float(d.get('ZuiDi', 0.0))
            tick.last_price = float(d.get('MaiChu', 0.0))

            tick.volume = int(d.get('XianLiang', 0))
            tick.open_interest = d.get('ChiCangLiang')

            tick.time = tick.datetime.strftime('%H:%M:%S.%f')[0:12]
            tick.date = tick.datetime.strftime('%Y-%m-%d')

            tick.trading_day = get_trading_date(tick_datetime)

            # 指数没有涨停和跌停，就用昨日收盘价正负10%
            tick.limit_up = tick.pre_close * 1.1
            tick.limit_down = tick.pre_close * 0.9

            # CTP只有一档行情
            tick.bid_price_1 = float(d.get('MaiRuJia', 0.0))
            tick.bid_volume_1 = int(d.get('MaiRuLiang', 0))
            tick.ask_price_1 = float(d.get('MaiChuJia', 0.0))
            tick.ask_volume_1 = int(d.get('MaiChuLiang', 0))

            underlying_symbol = vn_symbol.replace('99', '').upper()

            # 排除非交易时间得tick
            if tick.exchange is Exchange.CFFEX:
                if tick.datetime.hour not in [9, 10, 11, 13, 14, 15]:
                    continue
                if tick.datetime.hour == 9 and tick.datetime.minute < 15:
                    continue
                # 排除早盘 11:30~12:00
                if tick.datetime.hour == 11 and tick.datetime.minute >= 30:
                    continue
                if tick.datetime.hour == 15 and tick.datetime.minute >= 15 and underlying_symbol in ['T', 'TF', 'TS']:
                    continue
                if tick.datetime.hour == 15 and underlying_symbol in ['IH', 'IF', 'IC']:
                    continue

            else:  # 大商所/郑商所，上期所，上海能源
                # 排除非开盘小时
                if tick.datetime.hour in [3, 4, 5, 6, 7, 8, 12, 15, 16, 17, 18, 19, 20]:
                    continue
                # 排除早盘 10:15~10:30
                if tick.datetime.hour == 10 and 15 <= tick.datetime.minute < 30:
                    continue
                # 排除早盘 11:30~12:00
                if tick.datetime.hour == 11 and tick.datetime.minute >= 30:
                    continue
                # 排除午盘 13:00 ~13:30
                if tick.datetime.hour == 13 and tick.datetime.minute < 30:
                    continue
                # 排除凌晨2:30~3:00
                if tick.datetime.hour == 2 and tick.datetime.minute >= 30:
                    continue

                # 排除大商所/郑商所/上期所夜盘数据上期所夜盘数据 23:00 收盘
                if underlying_symbol in NIGHT_MARKET_23:
                    if tick.datetime.hour in [23, 0, 1, 2]:
                        continue

                # 排除上期所夜盘数据 1:00 收盘
                if underlying_symbol in NIGHT_MARKET_SQ2:
                    if tick.datetime.hour in [1, 2]:
                        continue

            # 排除日盘合约在夜盘得数据
            if underlying_symbol in MARKET_DAY_ONLY and (tick.datetime.hour < 9 or tick.datetime.hour > 16):
                # self.write_log(u'排除日盘合约{}在夜盘得数据'.format(short_symbol))
                continue

            self.symbol_tick_dict[tick.symbol] = tick

            if self.pub:
                d = copy.copy(tick.__dict__)
                if isinstance(tick.datetime, datetime):
                    d.update({'datetime': tick.datetime.strftime('%Y-%m-%d %H:%M:%S.%f')})
                d.update({'exchange': tick.exchange.value})
                d = json.dumps(d)
                self.pub.pub(d)
