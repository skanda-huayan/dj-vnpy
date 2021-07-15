import sys
import traceback
import json
from copy import deepcopy
from uuid import uuid1
from datetime import datetime, timedelta
from time import sleep
from threading import Thread
from multiprocessing.dummy import Pool
from typing import Dict
import pandas as pd

from vnpy.event import Event
from vnpy.rpc import RpcClient
from vnpy.trader.gateway import BaseGateway
from vnpy.trader.object import (
    TickData,
    BarData,
    ContractData,
    SubscribeRequest,
    CancelRequest,
    OrderRequest
)
from vnpy.trader.event import (
    EVENT_TICK,
    EVENT_TRADE,
    EVENT_ORDER,
    EVENT_POSITION,
    EVENT_ACCOUNT,
    EVENT_CONTRACT,
    EVENT_LOG)
from vnpy.trader.constant import Exchange, Product
from vnpy.amqp.consumer import subscriber
from vnpy.amqp.producer import task_creator

from vnpy.data.tdx.tdx_common import get_stock_type_sz, get_stock_type_sh

STOCK_CONFIG_FILE = 'tdx_stock_config.pkb2'
from pytdx.hq import TdxHq_API
# 通达信股票行情
from vnpy.data.tdx.tdx_common import get_cache_config, get_tdx_market_code
from vnpy.trader.utility import get_stock_exchange
from pytdx.config.hosts import hq_hosts
from pytdx.params import TDXParams


class StockRpcGateway(BaseGateway):
    """
    股票交易得RPC接口
    交易使用RPC实现，
    行情1:
        使用RabbitMQ订阅获取
        需要启动单独得进程运行stock_tick_publisher
        Cta_Stock => 行情订阅 =》StockRpcGateway =》RabbitMQ (task)=》 stock_tick_publisher =》订阅(worker)
        stock_tick_publisher => restful接口获取股票行情 =》RabbitMQ(pub) => StockRpcGateway =>on_tick event
    行情2：
        使用tdx进行bar订阅
    """

    default_setting = {
        "主动请求地址": "tcp://127.0.0.1:2014",
        "推送订阅地址": "tcp://127.0.0.1:4102",
        "远程接口名称": "pb01"
    }

    exchanges = list(Exchange)

    def __init__(self, event_engine, gateway_name='StockRPC'):
        """Constructor"""
        super().__init__(event_engine, gateway_name)

        self.symbol_gateway_map = {}

        self.client = RpcClient()
        self.client.callback = self.client_callback
        self.rabbit_api = None
        self.tdx_api = None
        self.rabbit_dict = {}
        # 远程RPC端，gateway_name
        self.remote_gw_name = gateway_name

    def connect(self, setting: dict):
        """"""
        req_address = setting["主动请求地址"]
        pub_address = setting["推送订阅地址"]
        self.remote_gw_name = setting['远程接口名称']

        self.write_log(f'请求地址:{req_address},订阅地址:{pub_address},远程接口:{self.remote_gw_name}')

        # 订阅事件
        self.client.subscribe_topic("")
        # self.client.subscribe_topic(EVENT_TRADE)
        # self.client.subscribe_topic(EVENT_ORDER)
        # self.client.subscribe_topic(EVENT_POSITION)
        # self.client.subscribe_topic(EVENT_ACCOUNT)
        # self.client.subscribe_topic(EVENT_CONTRACT)
        # self.client.subscribe_topic(EVENT_LOG)

        self.client.start(req_address, pub_address)
        self.status.update({"con":True})

        self.rabbit_dict = setting.get('rabbit', {})
        if len(self.rabbit_dict) > 0:
            self.write_log(f'激活RabbitMQ行情接口.配置：\n{self.rabbit_dict}')
            self.rabbit_api = SubMdApi(gateway=self)
            self.rabbit_api.connect(self.rabbit_dict)
        else:
            self.write_log(f'激活tdx行情订阅接口')
            self.tdx_api = TdxMdApi(gateway=self)
            self.tdx_api.connect()

        self.write_log("服务器连接成功，开始初始化查询")

        self.query_all()

    def check_status(self):

        if self.client:
            pass

        if self.rabbit_api:
            self.rabbit_api.check_status()

        return True

    def subscribe(self, req: SubscribeRequest):
        """行情订阅"""

        if self.tdx_api:
            self.tdx_api.subscribe(req)
            return

        self.write_log(f'创建订阅任务=> rabbitMQ')
        host = self.rabbit_dict.get('host', 'localhost')
        port = self.rabbit_dict.get('port', 5672)
        user = self.rabbit_dict.get('user', 'admin')
        password = self.rabbit_dict.get('password', 'admin')
        exchange = 'x_work_queue'
        queue_name = 'subscribe_task_queue'
        routing_key = 'stock_subscribe'
        task = task_creator(
            host=host,
            port=port,
            user=user,
            password=password,
            exchange=exchange,
            queue_name=queue_name,
            routing_key=routing_key)

        mission = {}
        mission.update({'id': str(uuid1())})
        mission.update({'action': "subscribe"})
        mission.update({'vt_symbol': req.vt_symbol})
        mission.update({'is_stock': True})
        msg = json.dumps(mission)
        self.write_log(f'[=>{host}:{port}/{exchange}/{queue_name}/{routing_key}] create task :{msg}')
        task.pub(msg)
        task.close()
        # gateway_name = self.symbol_gateway_map.get(req.vt_symbol, "")
        # self.client.subscribe(req, gateway_name)
        if self.rabbit_api:
            self.rabbit_api.registed_symbol_set.add(req.vt_symbol)

    def send_order(self, req: OrderRequest):
        """
        RPC远程发单
        :param req:
        :return:
        """
        self.write_log(f'使用prc委托:{req.__dict__}')
        ref = self.client.send_order(req, self.remote_gw_name)

        local_ref = ref.replace(f'{self.remote_gw_name}.', f'{self.gateway_name}.')
        self.write_log(f'委托返回:{ref}=> {local_ref}')
        return local_ref

    def cancel_order(self, req: CancelRequest):
        """"""
        self.write_log(f'委托撤单:{req.__dict__}')
        # gateway_name = self.symbol_gateway_map.get(req.vt_symbol, "")
        self.client.cancel_order(req, self.remote_gw_name)

    def query_account(self):
        """"""
        pass

    def query_position(self):
        """"""
        pass

    def query_all(self):
        """"""
        contracts = self.client.get_all_contracts()
        for contract in contracts:
            self.symbol_gateway_map[contract.vt_symbol] = contract.gateway_name
            contract.gateway_name = self.gateway_name
            self.on_contract(contract)
        self.write_log("合约信息查询成功")

        accounts = self.client.get_all_accounts()
        for account in accounts:
            account.gateway_name = self.gateway_name
            self.on_account(account)
        self.write_log("资金信息查询成功")

        positions = self.client.get_all_positions()
        for position in positions:
            position.gateway_name = self.gateway_name
            # 更换 vt_positionid得gateway前缀
            position.vt_positionid = position.vt_positionid.replace(f'{position.gateway_name}.',
                                                                    f'{self.gateway_name}.')
            # 更换 vt_accountid得gateway前缀
            position.vt_accountid = position.vt_accountid.replace(f'{position.gateway_name}.', f'{self.gateway_name}.')

            self.on_position(position)
        self.write_log("持仓信息查询成功")

        orders = self.client.get_all_orders()
        for order in orders:
            # 更换gateway
            order.gateway_name = self.gateway_name
            # 更换 vt_orderid得gateway前缀
            order.vt_orderid = order.vt_orderid.replace(f'{self.remote_gw_name}.', f'{self.gateway_name}.')
            # 更换 vt_accountid得gateway前缀
            order.vt_accountid = order.vt_accountid.replace(f'{self.remote_gw_name}.', f'{self.gateway_name}.')

            self.on_order(order)
        self.write_log("委托信息查询成功")

        trades = self.client.get_all_trades()
        for trade in trades:
            trade.gateway_name = self.gateway_name
            # 更换 vt_orderid得gateway前缀
            trade.vt_orderid = trade.vt_orderid.replace(f'{self.remote_gw_name}.', f'{self.gateway_name}.')
            # 更换 vt_orderid得gateway前缀
            trade.vt_orderid = trade.vt_orderid.replace(f'{self.remote_gw_name}.', f'{self.gateway_name}.')
            # 更换 vt_accountid得gateway前缀
            trade.vt_accountid = trade.vt_accountid.replace(f'{self.remote_gw_name}.', f'{self.gateway_name}.')
            self.on_trade(trade)
        self.write_log("成交信息查询成功")

    def close(self):
        """"""
        self.client.stop()
        self.client.join()

    def client_callback(self, topic: str, event: Event):
        """"""
        if event is None:
            print("none event", topic, event)
            return
        if event.type == EVENT_TICK:
            return

        event = deepcopy(event)

        data = event.data

        if hasattr(data, "gateway_name"):
            data.gateway_name = self.gateway_name

            if hasattr(data, 'vt_orderid'):
                rpc_vt_orderid = data.vt_orderid.replace(f'{self.remote_gw_name}.', f'{self.gateway_name}.')
                self.write_log(f' vt_orderid :{data.vt_orderid} => {rpc_vt_orderid}')
                data.vt_orderid = rpc_vt_orderid

            if hasattr(data, 'vt_tradeid'):
                rpc_vt_tradeid = data.vt_tradeid.replace(f'{self.remote_gw_name}.', f'{self.gateway_name}.')
                self.write_log(f' vt_tradeid :{data.vt_tradeid} => {rpc_vt_tradeid}')
                data.vt_tradeid = rpc_vt_tradeid

            if hasattr(data, 'vt_accountid'):
                data.vt_accountid = data.vt_accountid.replace(f'{self.remote_gw_name}.', f'{self.gateway_name}.')
            if hasattr(data, 'vt_positionid'):
                data.vt_positionid = data.vt_positionid.replace(f'{self.remote_gw_name}.', f'{self.gateway_name}.')

            if event.type in [EVENT_ORDER, EVENT_TRADE]:
                self.write_log(f'{self.remote_gw_name} => {self.gateway_name} event:{data.__dict__}')

        self.event_engine.put(event)


# 代码 <=> 中文名称
symbol_name_map: Dict[str, str] = {}
# 代码 <=> 交易所
symbol_exchange_map: Dict[str, Exchange] = {}


class TdxMdApi(object):
    """通达信行情和基础数据"""

    def __init__(self, gateway: StockRpcGateway):
        """"""
        super().__init__()

        self.gateway: StockRpcGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.connect_status: bool = False
        self.login_status: bool = False

        self.req_interval = 0.5  # 操作请求间隔500毫秒
        self.req_id = 0  # 操作请求编号
        self.connection_status = False  # 连接状态

        self.symbol_exchange_dict = {}  # tdx合约与vn交易所的字典
        self.symbol_market_dict = {}  # tdx合约与tdx市场的字典
        self.symbol_vn_dict = {}  # tdx合约与vtSymbol的对应
        self.symbol_bar_dict = {}  # tdx合约与最后一个bar得字典
        self.registed_symbol_set = set()

        self.config = get_cache_config(STOCK_CONFIG_FILE)
        self.symbol_dict = self.config.get('symbol_dict', {})
        # 最佳IP地址
        self.best_ip = self.config.get('best_ip', {})
        # 排除的异常地址
        self.exclude_ips = self.config.get('exclude_ips', [])
        # 选择时间
        self.select_time = self.config.get('select_time', datetime.now() - timedelta(days=7))
        # 缓存时间
        self.cache_time = self.config.get('cache_time', datetime.now() - timedelta(days=7))

        self.commission_dict = {}
        self.contract_dict = {}

        # self.queue = Queue()            # 请求队列
        self.pool = None  # 线程池
        # self.req_thread = None          # 定时器线程

        # copy.copy(hq_hosts)

        self.ip_list = [{'ip': "180.153.18.170", 'port': 7709},
                        {'ip': "180.153.18.171", 'port': 7709},
                        {'ip': "180.153.18.172", 'port': 80},
                        {'ip': "202.108.253.130", 'port': 7709},
                        {'ip': "202.108.253.131", 'port': 7709},
                        {'ip': "202.108.253.139", 'port': 80},
                        {'ip': "60.191.117.167", 'port': 7709},
                        {'ip': "115.238.56.198", 'port': 7709},
                        {'ip': "218.75.126.9", 'port': 7709},
                        {'ip': "115.238.90.165", 'port': 7709},
                        {'ip': "124.160.88.183", 'port': 7709},
                        {'ip': "60.12.136.250", 'port': 7709},
                        {'ip': "218.108.98.244", 'port': 7709},
                        # {'ip': "218.108.47.69", 'port': 7709},
                        {'ip': "114.80.63.12", 'port': 7709},
                        {'ip': "114.80.63.35", 'port': 7709},
                        {'ip': "180.153.39.51", 'port': 7709},
                        # {'ip': '14.215.128.18', 'port': 7709},
                        # {'ip': '59.173.18.140', 'port': 7709}
                        ]

        self.best_ip = {'ip': None, 'port': None}
        self.api_dict = {}          # API 的连接会话对象字典
        self.last_bar_dt = {}       # 记录该合约的最后一个bar(结束）时间
        self.last_api_bar_dict = {} # 记录会话最后一个bar的时间
        self.security_count = 50000

        # 股票code name列表
        self.stock_codelist = None

    def ping(self, ip, port=7709):
        """
        ping行情服务器
        :param ip:
        :param port:
        :param type_:
        :return:
        """
        apix = TdxHq_API()
        __time1 = datetime.now()
        try:
            with apix.connect(ip, port):
                if apix.get_security_count(TDXParams.MARKET_SZ) > 9000:  # 0：深市 股票数量 = 9260
                    _timestamp = datetime.now() - __time1
                    self.gateway.write_log('服务器{}:{},耗时:{}'.format(ip, port, _timestamp))
                    return _timestamp
                else:
                    self.gateway.write_log(u'该服务器IP {}无响应'.format(ip))
                    return timedelta(9, 9, 0)
        except:
            self.gateway.write_error(u'tdx ping服务器，异常的响应{}'.format(ip))
            return timedelta(9, 9, 0)

    def select_best_ip(self, ip_list, proxy_ip="", proxy_port=0, exclude_ips=[]):
        """
        选取最快的IP
        :param ip_list:
        :param proxy_ip: 代理
        :param proxy_port: 代理端口
        :param exclude_ips: 排除清单
        :return:
        """
        from pytdx.util.best_ip import ping
        data = [ping(ip=x['ip'], port=x['port'], type_='stock', proxy_ip=proxy_ip, proxy_port=proxy_port) for x in
                ip_list if x['ip'] not in exclude_ips]
        results = []
        for i in range(len(data)):
            # 删除ping不通的数据
            if data[i] < timedelta(0, 9, 0):
                results.append((data[i], ip_list[i]))
            else:
                if ip_list[i].get('ip') not in self.exclude_ips:
                    self.exclude_ips.append(ip_list[i].get('ip'))

        # 按照ping值从小大大排序
        results = [x[1] for x in sorted(results, key=lambda x: x[0])]

        return results[0]

    def connect(self, n=3):
        """
        连接通达讯行情服务器
        :param n:
        :return:
        """
        if self.connection_status:
            for api in self.api_dict:
                if api is not None or getattr(api, "client", None) is not None:
                    self.gateway.write_log(u'当前已经连接,不需要重新连接')
                    return

        self.gateway.write_log(u'开始通达信行情服务器')

        if len(self.symbol_dict) == 0:
            self.gateway.write_error(f'本地没有股票信息的缓存配置文件')
        else:
            self.cov_contracts()

        # 选取最佳服务器
        if self.best_ip['ip'] is None and self.best_ip['port'] is None:
            self.best_ip = self.select_best_ip(ip_list=self.ip_list,
                                               proxy_ip="",
                                               proxy_port=0,
                                               exclude_ips=self.exclude_ips)

        # 创建n个api连接对象实例
        for i in range(n):
            try:
                api = TdxHq_API(heartbeat=True, auto_retry=True, raise_exception=True)
                api.connect(self.best_ip['ip'], self.best_ip['port'])
                # 尝试获取市场合约统计
                c = api.get_security_count(TDXParams.MARKET_SZ)
                if c is None or c < 10:
                    err_msg = u'该服务器IP {}/{}无响应'.format(self.best_ip['ip'], self.best_ip['port'])
                    self.gateway.write_error(err_msg)
                else:
                    self.gateway.write_log(u'创建第{}个tdx连接'.format(i + 1))
                    self.api_dict[i] = api
                    self.last_bar_dt[i] = datetime.now()
                    self.connection_status = True
                    self.security_count = c

                    # if len(symbol_name_map) == 0:
                    #    self.get_stock_list()

            except Exception as ex:
                self.gateway.write_error(u'连接服务器tdx[{}]异常:{},{}'.format(i, str(ex), traceback.format_exc()))
                self.gateway.status.update({"tdx_status":False, "tdx_error":str(ex)})
                return

        # 创建连接池，每个连接都调用run方法
        self.pool = Pool(n)
        self.pool.map_async(self.run, range(n))

        # 设置上层的连接状态
        self.gateway.status.update({"tdx_con":True, 'tdx_con_time':datetime.now().strftime('%H:%M:%S')})

    def reconnect(self, i):
        """
        重连
        :param i:
        :return:
        """
        try:
            self.best_ip = self.select_best_ip(ip_list=self.ip_list, exclude_ips=self.exclude_ips)
            api = TdxHq_API(heartbeat=True, auto_retry=True)
            api.connect(self.best_ip['ip'], self.best_ip['port'])
            # 尝试获取市场合约统计
            c = api.get_security_count(TDXParams.MARKET_SZ)
            if c is None or c < 10:
                err_msg = u'该服务器IP {}/{}无响应'.format(self.best_ip['ip'], self.best_ip['port'])
                self.gateway.write_error(err_msg)
            else:
                self.gateway.write_log(u'重新创建第{}个tdx连接'.format(i + 1))
                self.api_dict[i] = api

            sleep(1)
        except Exception as ex:
            self.gateway.write_error(u'重新连接服务器tdx[{}]异常:{},{}'.format(i, str(ex), traceback.format_exc()))
            self.gateway.status.update({"tdx_status":False, "tdx_error":str(ex)})
            return

    def close(self):
        """退出API"""
        self.connection_status = False

        # 设置上层的连接状态
        self.gateway.status.update({'tdx_con':False})

        if self.pool is not None:
            self.pool.close()
            self.pool.join()

    def subscribe(self, req):
        """订阅合约"""
        # 这里的设计是，如果尚未登录就调用了订阅方法
        # 则先保存订阅请求，登录完成后会自动订阅
        vn_symbol = str(req.symbol)
        if '.' in vn_symbol:
            vn_symbol = vn_symbol.split('.')[0]

        self.gateway.write_log(u'通达信行情订阅 {}'.format(str(vn_symbol)))

        tdx_symbol = vn_symbol  # [0:-2] + 'L9'
        tdx_symbol = tdx_symbol.upper()
        self.gateway.write_log(u'{}=>{}'.format(vn_symbol, tdx_symbol))
        self.symbol_vn_dict[tdx_symbol] = vn_symbol

        if tdx_symbol not in self.registed_symbol_set:
            self.registed_symbol_set.add(tdx_symbol)

        # 查询股票信息
        self.qry_instrument(vn_symbol)

        self.check_status()

    def check_status(self):
        """
        tdx行情接口状态监控
        :return:
        """
        self.gateway.write_log(u'检查tdx接口状态')
        try:
            # 一共订阅的数量
            self.gateway.status.update({"tdx_symbols_count":len(self.registed_symbol_set)})

            dt_now = datetime.now()
            if len(self.registed_symbol_set) > 0 and '0935' < dt_now.strftime("%H%M") < '1500':
                # 若还没有启动连接，就启动连接
                over_time = [((dt_now - dt).total_seconds() > 60) for dt in self.last_api_bar_dict.values()]
                if not self.connection_status or len(self.api_dict) == 0 or any(over_time):
                    self.gateway.write_log(u'tdx还没有启动连接，就启动连接')
                    self.close()
                    self.pool = None
                    self.api_dict = {}
                    pool_cout = getattr(self.gateway, 'tdx_pool_count', 3)
                    self.connect(pool_cout)

            api_bar_times = [f'{k}:{v.hour}:{v.minute}' for k,v in self.last_api_bar_dict.items()]
            if len(api_bar_times) > 0:

                self.gateway.status.update({"tdx_api_dt":api_bar_times,'tdx_status':True})

            #self.gateway.write_log(u'tdx接口状态正常')
        except Exception as ex:
            msg = f'检查tdx接口时异常:{str(ex)}' + traceback.format_exc()
            self.gateway.write_error(msg)

    def qry_instrument(self, symbol):
        """
        查询/更新股票信息
        :return:
        """
        if not self.connection_status:
            return

        api = self.api_dict.get(0)
        if api is None:
            self.gateway.write_log(u'取不到api连接，更新合约信息失败')
            return

        # TODO： 取得股票的中文名
        market_code = get_tdx_market_code(symbol)
        api.to_df(api.get_finance_info(market_code, symbol))

        # 如果有预定的订阅合约，提前订阅
        # if len(all_contacts) > 0:
        #     cur_folder =  os.path.dirname(__file__)
        #     export_file = os.path.join(cur_folder,'contracts.csv')
        #     if not os.path.exists(export_file):
        #         df = pd.DataFrame(all_contacts)
        #         df.to_csv(export_file)

    def cov_contracts(self):
        """转换本地缓存=》合约信息推送"""
        for symbol_marketid, info in self.symbol_dict.items():
            symbol, market_id = symbol_marketid.split('_')
            exchange = info.get('exchange', '')
            if len(exchange) == 0:
                continue

            vn_exchange_str = get_stock_exchange(symbol)

            # 排除通达信的指数代码
            if exchange != vn_exchange_str:
                continue

            exchange = Exchange(exchange)
            if info['stock_type'] == 'stock_cn':
                product = Product.EQUITY
            elif info['stock_type'] in ['bond_cn', 'cb_cn']:
                product = Product.BOND
            elif info['stock_type'] == 'index_cn':
                product = Product.INDEX
            elif info['stock_type'] == 'etf_cn':
                product = Product.ETF
            else:
                product = Product.EQUITY

            volume_tick = info['volunit']
            if symbol.startswith('688'):
                volume_tick = 200

            contract = ContractData(
                gateway_name=self.gateway_name,
                symbol=symbol,
                exchange=exchange,
                name=info['name'],
                product=product,
                pricetick=round(0.1 ** info['decimal_point'], info['decimal_point']),
                size=1,
                min_volume=volume_tick,
                margin_rate=1
            )

            if product != Product.INDEX:
                # 缓存 合约 =》 中文名
                symbol_name_map.update({contract.symbol: contract.name})

                # 缓存代码和交易所的印射关系
                symbol_exchange_map[contract.symbol] = contract.exchange

                self.contract_dict.update({contract.symbol: contract})
                self.contract_dict.update({contract.vt_symbol: contract})
                # 推送
                self.gateway.on_contract(contract)

    def get_stock_list(self):
        """股票所有的code&name列表"""

        api = self.api_dict.get(0)
        if api is None:
            self.gateway.write_log(u'取不到api连接，更新合约信息失败')
            return None

        self.gateway.write_log(f'查询所有的股票信息')

        data = pd.concat(
            [pd.concat([api.to_df(api.get_security_list(j, i * 1000)).assign(sse='sz' if j == 0 else 'sh').set_index(
                ['code', 'sse'], drop=False) for i in range(int(api.get_security_count(j) / 1000) + 1)], axis=0) for j
                in range(2)], axis=0)
        sz = data.query('sse=="sz"')
        sh = data.query('sse=="sh"')
        sz = sz.assign(sec=sz.code.apply(get_stock_type_sz))
        sh = sh.assign(sec=sh.code.apply(get_stock_type_sh))

        temp_df = pd.concat([sz, sh]).query('sec in ["stock_cn","etf_cn","bond_cn","cb_cn"]').sort_index().assign(
            name=data['name'].apply(lambda x: str(x)[0:6]))
        hq_codelist = temp_df.loc[:, ['code', 'name']].set_index(['code'], drop=False)

        for i in range(0, len(temp_df)):
            row = temp_df.iloc[i]
            if row['sec'] == 'etf_cn':
                product = Product.ETF
            elif row['sec'] in ['bond_cn', 'cb_cn']:
                product = Product.BOND
            else:
                product = Product.EQUITY

            volume_tick = 100 if product != Product.BOND else 10
            if row['code'].startswith('688'):
                volume_tick = 200

            contract = ContractData(
                gateway_name=self.gateway_name,
                symbol=row['code'],
                exchange=Exchange.SSE if row['sse'] == 'sh' else Exchange.SZSE,
                name=row['name'],
                product=product,
                pricetick=round(0.1 ** row['decimal_point'], row['decimal_point']),
                size=1,
                min_volume=volume_tick,
                margin_rate=1

            )
            # 缓存 合约 =》 中文名
            symbol_name_map.update({contract.symbol: contract.name})

            # 缓存代码和交易所的印射关系
            symbol_exchange_map[contract.symbol] = contract.exchange

            self.contract_dict.update({contract.symbol: contract})
            self.contract_dict.update({contract.vt_symbol: contract})
            # 推送
            self.gateway.on_contract(contract)

        return hq_codelist

    def run(self, i):
        """
        版本1：Pool内得线程，持续运行,每个线程从queue中获取一个请求并处理
        版本2：Pool内线程，从订阅合约集合中，取出符合自己下标 mode n = 0的合约，并发送请求
        :param i:
        :return:
        """
        # 版本2：
        try:
            api_count = len(self.api_dict)
            last_dt = datetime.now()
            last_minute = None
            self.gateway.write_log(u'开始运行tdx[{}],{}'.format(i, last_dt))
            while self.connection_status:
                dt = datetime.now()

                # 每个自然分钟的1~5秒，进行
                if last_minute == dt.minute or 1 < dt.second < 5:
                    continue
                last_minute = dt.minute

                symbols = set()
                for idx, tdx_symbol in enumerate(list(self.registed_symbol_set)):
                    # self.gateway.write_log(u'tdx[{}], api_count:{}, idx:{}, tdx_symbol:{}'.format(i, api_count, idx, tdx_symbol))
                    if idx % api_count == i:
                        try:
                            symbols.add(tdx_symbol)
                            self.processReq(tdx_symbol, i)
                        except BrokenPipeError as bex:
                            self.gateway.write_error(u'BrokenPipeError{},重试重连tdx[{}]'.format(str(bex), i))
                            self.reconnect(i)
                            sleep(5)
                            break
                        except Exception as ex:
                            self.gateway.write_error(
                                u'tdx[{}] exception:{},{}'.format(i, str(ex), traceback.format_exc()))

                            self.gateway.write_error(u'重试重连tdx[{}]'.format(i))
                            print(u'重试重连tdx[{}]'.format(i), file=sys.stderr)
                            self.reconnect(i)

                # self.gateway.write_log(u'tdx[{}] sleep'.format(i))
                sleep(self.req_interval)
                if last_dt.minute != dt.minute:
                    self.gateway.write_log('tdx[{}] check point. {}, process symbols:{}'.format(i, dt, symbols))
                    last_dt = dt
        except Exception as ex:
            self.gateway.write_error(u'tdx[{}] pool.run exception:{},{}'.format(i, str(ex), traceback.format_exc()))

        self.gateway.write_error(u'tdx[{}] {}退出'.format(i, datetime.now()))

    def processReq(self, req, i):
        """
        处理行情信息bar请求
        :param req:
        :param i:
        :return:
        """
        symbol = req
        if '.' in symbol:
            symbol, exchange = symbol.split('.')
            if exchange == 'SZSE':
                market_id = 0
            else:
                market_id = 1
        else:
            market_id = get_tdx_market_code(symbol)
            exchange = get_stock_exchange(symbol)

        exchange = Exchange(exchange)

        api = self.api_dict.get(i, None)
        if api is None:
            self.gateway.write_log(u'tdx[{}] Api is None'.format(i))
            raise Exception(u'tdx[{}] Api is None'.format(i))

        symbol_config = self.symbol_dict.get('{}_{}'.format(symbol, market_id), {})
        decimal_point = symbol_config.get('decimal_point', 2)

        rt_list = api.get_security_bars(
            category=8,
            market=market_id,
            code=symbol,
            start=0,
            count=1)

        if rt_list is None or len(rt_list) == 0:
            self.gateway.write_log(u'tdx[{}]: rt_list为空'.format(i))
            return
        data = rt_list[0]
        # tdx 返回bar的结束时间
        bar_dt = datetime.strptime(data.get('datetime'), '%Y-%m-%d %H:%M')

        # 更新api的获取bar结束时间
        self.last_api_bar_dict[i] = bar_dt

        if i in self.last_bar_dt:
            if self.last_bar_dt[i] < bar_dt:
                self.last_bar_dt[i] = bar_dt

        pre_bar = self.symbol_bar_dict.get(symbol)
        # 存在上一根Bar
        if pre_bar and (datetime.now() - bar_dt).total_seconds() > 60:
            return

        # vnpy bar开始时间
        bar_dt = bar_dt - timedelta(minutes=1)
        bar = BarData(
            gateway_name='tdx',
            symbol=symbol,
            exchange=exchange,
            datetime=bar_dt
        )
        bar.trading_day = bar_dt.strftime('%Y-%m-%d')
        bar.open_price = float(data['open'])
        bar.high_price = float(data['high'])
        bar.low_price = float(data['low'])
        bar.close_price = float(data['close'])
        bar.volume = float(data['vol'])

        self.symbol_bar_dict[symbol] = bar

        self.gateway.on_bar(deepcopy(bar))


class SubMdApi():
    """
    RabbitMQ Subscriber 数据行情接收API
    """

    def __init__(self, gateway):
        self.gateway = gateway
        self.gateway_name = gateway.gateway_name

        self.symbol_tick_dict = {}  # 合约与最后一个Tick得字典
        self.registed_symbol_set = set()  # 订阅的合约记录集
        self.last_tick_dt = None
        self.sub = None
        self.setting = {}
        self.connect_status = False
        self.thread = None  # 用线程运行所有行情接收

    def check_status(self):
        """接口状态的健康检查"""

        # 订阅的合约
        d = {'sub_symbols': sorted(self.symbol_tick_dict.keys())}

        # 合约的最后时间
        if self.last_tick_dt:
            d.update({"sub_tick_time": self.last_tick_dt.strftime('%Y-%m-%d %H:%M:%S')})

        if len(self.symbol_tick_dict) > 0:
            dt_now = datetime.now()
            hh_mm = dt_now.hour * 100 + dt_now.minute
            # 期货交易时间内
            if 930 <= hh_mm <= 1130 or 1301 <= hh_mm <= 1500:
                # 未有数据到达
                if self.last_tick_dt is None:
                    d.update({"sub_status": False, "sub_error": u"rabbitmq未有行情数据到达"})
                else:  # 有数据

                    # 超时5分钟以上
                    if (dt_now - self.last_tick_dt).total_seconds() > 60 * 5:
                        d.update({"sub_status": False,
                                  "sub_error": u"{}rabbitmq行情数据超时5分钟以上".format(hh_mm)})
                    else:
                        d.update({"sub_status": True})
                        self.gateway.status.pop("sub_error", None)

            # 非交易时间
            else:
                self.gateway.status.pop("sub_status", None)
                self.gateway.status.pop("sub_error", None)

        # 更新到gateway的状态中去
        self.gateway.status.update(d)

    def connect(self, setting={}):
        """连接"""
        self.setting = setting
        try:
            self.sub = subscriber(
                host=self.setting.get('host', 'localhost'),
                port=self.setting.get('port', 5672),
                user=self.setting.get('user', 'admin'),
                password=self.setting.get('password', 'admin'),
                exchange=self.setting.get('exchange', 'x_fanout_stock_tick'))

            self.sub.set_callback(self.on_message)
            self.thread = Thread(target=self.sub.start)
            self.thread.start()
            self.connect_status = True
            self.gateway.status.update({'sub_con': True, 'sub_con_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')})
        except Exception as ex:
            self.gateway.write_error(u'连接RabbitMQ {} 异常:{}'.format(self.setting, str(ex)))
            self.gateway.write_error(traceback.format_exc())
            self.connect_status = False

    def on_message(self, chan, method_frame, _header_frame, body, userdata=None):
        # print(" [x] %r" % body)
        try:
            str_tick = body.decode('utf-8')
            d = json.loads(str_tick)
            d.pop('rawData', None)

            symbol = d.pop('symbol', None)
            str_datetime = d.pop('datetime', None)

            if '.' in str_datetime:
                dt = datetime.strptime(str_datetime, '%Y-%m-%d %H:%M:%S.%f')
            else:
                dt = datetime.strptime(str_datetime, '%Y-%m-%d %H:%M:%S')

            tick = TickData(gateway_name=self.gateway_name,
                            exchange=Exchange(d.get('exchange')),
                            symbol=symbol,
                            datetime=dt)
            d.pop('gateway_name', None)
            d.pop('exchange', None)
            d.pop('symbol', None)
            tick.__dict__.update(d)

            self.symbol_tick_dict[symbol] = tick
            self.gateway.on_tick(tick)
            self.last_tick_dt = tick.datetime

        except Exception as ex:
            self.gateway.write_error(u'RabbitMQ on_message 异常:{}'.format(str(ex)))
            self.gateway.write_error(traceback.format_exc())

    def close(self):
        """退出API"""
        self.gateway.write_log(u'退出rabbit行情订阅API')
        self.connection_status = False

        try:
            if self.sub:
                self.gateway.write_log(u'关闭订阅器')
                self.sub.close()

            if self.thread is not None:
                self.gateway.write_log(u'关闭订阅器接收线程')
                self.thread.join()
        except Exception as ex:
            self.gateway.write_error(u'退出rabbitMQ行情api异常:{}'.format(str(ex)))
