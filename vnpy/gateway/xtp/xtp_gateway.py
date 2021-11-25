from typing import Any, Dict, List
from datetime import datetime
from functools import lru_cache

from vnpy.api.xtp import MdApi, TdApi
from vnpy.event import EventEngine
from vnpy.trader.event import EVENT_TIMER
from vnpy.trader.constant import (
    Exchange,
    Product,
    Direction,
    OrderType,
    Status,
    Offset
)
from vnpy.trader.gateway import BaseGateway
from vnpy.trader.object import (
    CancelRequest,
    OrderRequest,
    SubscribeRequest,
    TickData,
    ContractData,
    OrderData,
    TradeData,
    PositionData,
    AccountData
)
from vnpy.trader.utility import get_folder_path, print_dict, extract_vt_symbol

# 市场id <=> Exchange
MARKET_XTP2VT: Dict[int, Exchange] = {
    1: Exchange.SZSE,
    2: Exchange.SSE
}
MARKET_VT2XTP: Dict[Exchange, int] = {v: k for k, v in MARKET_XTP2VT.items()}

# 交易所id <=> Exchange
EXCHANGE_XTP2VT: Dict[int, Exchange] = {
    1: Exchange.SSE,
    2: Exchange.SZSE,
}
EXCHANGE_VT2XTP: Dict[Exchange, int] = {v: k for k, v in EXCHANGE_XTP2VT.items()}

# 方向  <=> Direction, Offset
DIRECTION_STOCK_XTP2VT: Dict[int, Any] = {
    1: (Direction.LONG, Offset.NONE),  # 买
    2: (Direction.SHORT, Offset.NONE),  # 卖
    21: (Direction.LONG, Offset.OPEN),  # 多，开
    22: (Direction.SHORT, Offset.OPEN),  # 空，开
    24: (Direction.LONG, Offset.CLOSE),  # 多，平
    23: (Direction.SHORT, Offset.CLOSE)  # 空， 平
}
DIRECTION_STOCK_VT2XTP: Dict[Any, int] = {v: k for k, v in DIRECTION_STOCK_XTP2VT.items()}

# 期权方向 <=> Direction
DIRECTION_OPTION_XTP2VT: Dict[int, Direction] = {
    1: Direction.LONG,
    2: Direction.SHORT
}
DIRECTION_OPTION_VT2XTP: Dict[Direction, int] = {v: k for k, v in DIRECTION_OPTION_XTP2VT.items()}

# 持仓方向 <=> Direction
POSITION_DIRECTION_XTP2VT = {
    0: Direction.NET,
    1: Direction.LONG,
    2: Direction.SHORT,
    3: Direction.SHORT
}

# 委托单类型
ORDERTYPE_XTP2VT: Dict[int, OrderType] = {
    1: OrderType.LIMIT,
    2: OrderType.MARKET,
    4: OrderType.MARKET
}
ORDERTYPE_VT2XTP: Dict[OrderType, int] = {v: k for k, v in ORDERTYPE_XTP2VT.items()}

# 协议类型
PROTOCOL_VT2XTP: Dict[str, int] = {
    "TCP": 1,
    "UDP": 2
}

# 状态 <=> Status
STATUS_XTP2VT: Dict[int, Status] = {
    0: Status.SUBMITTING,
    1: Status.ALLTRADED,
    2: Status.PARTTRADED,
    3: Status.CANCELLED,
    4: Status.NOTTRADED,
    5: Status.CANCELLED,
    6: Status.REJECTED,
    7: Status.SUBMITTING
}

# 合约类型 <=> Product
PRODUCT_XTP2VT: Dict[int, Product] = {
    0: Product.EQUITY,
    1: Product.INDEX,
    2: Product.FUND,
    3: Product.BOND,
    4: Product.OPTION,
    5: Product.EQUITY,
    6: Product.OPTION
}

# 开平仓 <=> Offset
OFFSET_VT2XTP: Dict[Offset, int] = {
    Offset.NONE: 0,
    Offset.OPEN: 1,
    Offset.CLOSE: 2,
    Offset.CLOSETODAY: 4,
    Offset.CLOSEYESTERDAY: 5
}
OFFSET_XTP2VT: Dict[int, Offset] = {v: k for k, v in OFFSET_VT2XTP.items()}

# 业务类型 <=> xtp
BUSINESS_VT2XTP: Dict[Any, int] = {
    "CASH": 0,
    Offset.NONE: 0,
    "MARGIN": 4,
    Offset.OPEN: 4,
    Offset.CLOSE: 4,
    "OPTION": 10,
}

# 代码 <=> 中文名称
symbol_name_map: Dict[str, str] = {}
# 代码 <=> 交易所
symbol_exchange_map: Dict[str, Exchange] = {}


@lru_cache()
def get_vt_symbol_name(vt_symbol):
    return symbol_name_map.get(vt_symbol, vt_symbol.split('.')[0])


class XtpGateway(BaseGateway):
    default_setting: Dict[str, Any] = {
        "账号": "",
        "密码": "",
        "客户号": 1,
        "行情地址": "",
        "行情端口": 0,
        "交易地址": "",
        "交易端口": 0,
        "行情协议": ["TCP", "UDP"],
        "授权码": ""
    }

    # 接口支持得交易所清单
    exchanges: List[Exchange] = list(EXCHANGE_VT2XTP.keys())

    def __init__(self, event_engine: EventEngine, gateway_name='XTP'):
        """"""
        super().__init__(event_engine, gateway_name=gateway_name)

        self.md_api = XtpMdApi(self)
        self.td_api = XtpTdApi(self)

    def connect(self, setting: dict) -> None:
        """"""
        userid = setting["账号"]
        password = setting["密码"]
        client_id = int(setting["客户号"])
        quote_ip = setting["行情地址"]
        quote_port = int(setting["行情端口"])
        trader_ip = setting["交易地址"]
        trader_port = int(setting["交易端口"])
        quote_protocol = setting["行情协议"]
        software_key = setting["授权码"]

        self.md_api.connect(userid, password, client_id, quote_ip, quote_port, quote_protocol)
        self.td_api.connect(userid, password, client_id, trader_ip, trader_port, software_key)
        self.init_query()

    def close(self) -> None:
        """"""
        self.md_api.close()
        self.td_api.close()

    def subscribe(self, req: SubscribeRequest) -> None:
        """"""
        self.md_api.subscrbie(req)

    def send_order(self, req: OrderRequest) -> str:
        """"""
        return self.td_api.send_order(req)

    def cancel_order(self, req: CancelRequest) -> None:
        """"""
        self.td_api.cancel_order(req)

    def query_account(self) -> None:
        """"""
        self.td_api.query_account()

    def query_position(self) -> None:
        """"""
        self.td_api.query_position()

    def process_timer_event(self, event) -> None:
        """"""
        self.count += 1
        if self.count < 5:
            return
        self.count = 0

        func = self.query_functions.pop(0)
        func()
        self.query_functions.append(func)

    def init_query(self) -> None:
        """"""
        self.count = 0
        self.query_functions = [self.query_account, self.query_position]
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)


class XtpMdApi(MdApi):

    def __init__(self, gateway: BaseGateway):
        """"""
        super().__init__()

        self.gateway: BaseGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.userid: str = ""
        self.password: str = ""
        self.client_id: int = 0
        self.server_ip: str = ""
        self.server_port: int = 0
        self.protocol: int = 0
        self.session_id: int = 0

        self.connect_status: bool = False
        self.login_status: bool = False

    def onDisconnected(self, reason: int) -> None:
        """"""
        self.connect_status = False
        self.login_status = False
        self.gateway.write_log(f"行情服务器连接断开, 原因{reason}")

        n = self.login(
            self.server_ip,
            self.server_port,
            self.userid,
            self.password,
            self.protocol)

        if n:
            self.session_id = n
            self.connect_status = True
            self.login_status = True

            self.gateway.write_log(f"交易服务器登录成功，会话编号：{self.session_id}")
        else:
            self.gateway.write_log("行情服务器登录失败")

    def onError(self, error: dict) -> None:
        """"""
        self.gateway.write_error("行情接口报错", error)

    def onSubMarketData(self, data: dict, error: dict, last: bool) -> None:
        """"""
        if not error or not error["error_id"]:
            return

        self.gateway.write_error("行情订阅失败", error)

    def onUnSubMarketData(self, data: dict, error: dict, last: bool) -> None:
        """"""
        pass

    def onDepthMarketData(self, data: dict) -> None:
        """深度行情回报"""
        timestamp = str(data["data_time"])
        dt = datetime.strptime(timestamp, "%Y%m%d%H%M%S%f")

        tick = TickData(
            symbol=data["ticker"],
            exchange=EXCHANGE_XTP2VT[data["exchange_id"]],
            datetime=dt,
            date=dt.strftime('%Y-%m-%d'),
            time=dt.strftime('%H:%M:%S.%f'),
            trading_day=dt.strftime('%Y-%m-%d'),
            volume=data["qty"],
            last_price=data["last_price"],
            limit_up=data["upper_limit_price"],
            limit_down=data["lower_limit_price"],
            open_price=data["open_price"],
            high_price=data["high_price"],
            low_price=data["low_price"],
            pre_close=data["pre_close_price"],
            gateway_name=self.gateway_name
        )

        tick.bid_price_1, tick.bid_price_2, tick.bid_price_3, tick.bid_price_4, tick.bid_price_5 = data["bid"][0:5]
        tick.ask_price_1, tick.ask_price_2, tick.ask_price_3, tick.ask_price_4, tick.ask_price_5 = data["ask"][0:5]
        tick.bid_volume_1, tick.bid_volume_2, tick.bid_volume_3, tick.bid_volume_4, tick.bid_volume_5 = data["bid_qty"][0:5]
        tick.ask_volume_1, tick.ask_volume_2, tick.ask_volume_3, tick.ask_volume_4, tick.ask_volume_5 = data["ask_qty"][0:5]

        tick.name = get_vt_symbol_name(tick.vt_symbol)
        # self.gateway.prices.update({tick.vt_symbol: tick.last_price})
        self.gateway.on_tick(tick)

    def onSubOrderBook(self, data: dict, error: dict, last: bool) -> None:
        """"""
        pass

    def onUnSubOrderBook(self, data: dict, error: dict, last: bool) -> None:
        """"""
        pass

    def onOrderBook(self, data: dict) -> None:
        """"""
        pass

    def onSubTickByTick(self, data: dict, error: dict, last: bool) -> None:
        """"""
        pass

    def onUnSubTickByTick(self, data: dict, error: dict, last: bool) -> None:
        """"""
        pass

    def onTickByTick(self, data: dict) -> None:
        """"""
        pass

    def onSubscribeAllMarketData(self, data: dict, error: dict) -> None:
        """"""
        pass

    def onUnSubscribeAllMarketData(self, data: dict, error: dict) -> None:
        """"""
        pass

    def onSubscribeAllOrderBook(self, data: dict, error: dict) -> None:
        """"""
        pass

    def onUnSubscribeAllOrderBook(self, data: dict, error: dict) -> None:
        """"""
        pass

    def onSubscribeAllTickByTick(self, data: dict, error: dict) -> None:
        """"""
        pass

    def onUnSubscribeAllTickByTick(self, data: dict, error: dict) -> None:
        """"""
        pass

    def onQueryAllTickers(self, data: dict, error: dict, last: bool) -> None:
        """合约信息回报"""
        contract = ContractData(
            symbol=data["ticker"],
            exchange=EXCHANGE_XTP2VT[data["exchange_id"]],
            name=data["ticker_name"],
            product=PRODUCT_XTP2VT[data["ticker_type"]],
            size=1,
            pricetick=data["price_tick"],
            min_volume=data["buy_qty_unit"],
            gateway_name=self.gateway_name
        )
        # if contract.symbol.startswith('1230'):
        #    self.gateway.write_log(msg=f'合约信息:{contract.__dict__}')
        self.gateway.on_contract(contract)

        # 更新最新价
        pre_close_price = float(data["pre_close_price"])
        vt_symbol = contract.vt_symbol
        if vt_symbol not in self.gateway.prices and pre_close_price > 0:
            self.gateway.prices.update({vt_symbol: pre_close_price})

        # 更新 symbol <=> 中文名称映射
        symbol_name_map[contract.vt_symbol] = contract.name

        # 更新 股票代码 <=> 交易所
        if contract.product != Product.INDEX:
            symbol_exchange_map[contract.symbol] = contract.exchange

        if last:
            self.gateway.write_log(f"{contract.exchange.value}合约信息查询成功")

    def onQueryTickersPriceInfo(self, data: dict, error: dict, last: bool) -> None:
        """查询最新价"""
        self.gateway.write_log('最新价:{}'.format(print_dict(data)))
        symbol = data.get('ticker')
        exchange_id = data.get('exchange_id')
        last_price = float(data.get('last_price', 0))

        if symbol and exchange_id and last_price > 0:
            exchange = EXCHANGE_XTP2VT[exchange_id]
            vt_symbol = f'{symbol}.{exchange.value}'
            self.gateway.prices.update({vt_symbol: last_price})
            self.gateway.write_log(f'{vt_symbol} 最新价: {last_price}')

    def onSubscribeAllOptionMarketData(self, data: dict, error: dict) -> None:
        """"""
        pass

    def onUnSubscribeAllOptionMarketData(self, data: dict, error: dict) -> None:
        """"""
        pass

    def onSubscribeAllOptionOrderBook(self, data: dict, error: dict) -> None:
        """"""
        pass

    def onUnSubscribeAllOptionOrderBook(self, data: dict, error: dict) -> None:
        """"""
        pass

    def onSubscribeAllOptionTickByTick(self, data: dict, error: dict) -> None:
        """"""
        pass

    def onUnSubscribeAllOptionTickByTick(self, data: dict, error: dict) -> None:
        """"""
        pass

    def connect(
            self,
            userid: str,
            password: str,
            client_id: int,
            server_ip: str,
            server_port: int,
            quote_protocol: int
    ) -> None:
        """"""
        self.userid = userid
        self.password = password
        self.client_id = client_id
        self.server_ip = server_ip
        self.server_port = server_port
        self.protocol = PROTOCOL_VT2XTP[quote_protocol]

        # Create API object
        if not self.connect_status:
            path = str(get_folder_path(self.gateway_name.lower()))
            self.createQuoteApi(self.client_id, path)

            self.login_server()

    def login_server(self) -> None:
        """"""
        n = self.login(
            self.server_ip,
            self.server_port,
            self.userid,
            self.password,
            self.protocol
        )

        if not n:
            self.connect_status = True
            self.login_status = True
            msg = "行情服务器登录成功"
            self.query_contract()
            self.init()
        else:
            msg = f"行情服务器登录失败，原因：{n}"

        self.gateway.write_log(msg)

    def close(self) -> None:
        """"""
        if self.connect_status:
            self.exit()

    def subscrbie(self, req: SubscribeRequest) -> None:
        """"""
        if self.login_status:
            xtp_exchange = EXCHANGE_VT2XTP.get(req.exchange, "")
            self.subscribeMarketData(req.symbol, 1, xtp_exchange)

    def query_contract(self) -> None:
        """查询合约明细"""
        for exchange_id in EXCHANGE_XTP2VT.keys():
            self.queryAllTickers(exchange_id)


class XtpTdApi(TdApi):

    def __init__(self, gateway: BaseGateway):
        """"""
        super().__init__()

        self.gateway: BaseGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.userid: str = ""
        self.password: str = ""
        self.client_id: str = ""
        self.server_ip: str = ""
        self.server_port: str = ""
        self.software_key: str = ""

        self.session_id: int = 0
        self.reqid: int = 0
        self.protocol: int = 0

        # 证券资产
        self.security_asset = None  # 未查询获取持仓时，是None，如果查询过，无持仓，为0
        self.security_volumes: Dict[str, int] = {}  # vt_symbol, volume

        # Whether current account supports margin or option
        self.margin_trading = False
        self.option_trading = False

        self.connect_status: bool = False
        self.login_status: bool = False

        self.short_positions: Dict[str, PositionData] = {}

    def onDisconnected(self, session: int, reason: int) -> None:
        """"""
        self.connect_status = False
        self.login_status = False
        self.gateway.write_log(f"交易服务器连接断开, 原因{reason}")

        self.login_server()

    def onError(self, error: dict) -> None:
        """"""
        self.gateway.write_error("交易接口报错", error)

    def onOrderEvent(self, data: dict, error: dict, session: int) -> None:
        """委托回报"""
        if error["error_id"]:
            self.gateway.write_error("交易委托失败", error)

        symbol = data["ticker"]
        if len(symbol) == 8:
            # 期权
            direction = DIRECTION_OPTION_XTP2VT[data["side"]]
            offset = OFFSET_XTP2VT[data["position_effect"]]
        else:
            # 股票
            direction, offset = DIRECTION_STOCK_XTP2VT[data["side"]]
        insert_time = str(data["insert_time"])
        dt = datetime.strptime(insert_time, '%Y%m%d%H%M%S%f')
        order = OrderData(
            accountid=self.userid,
            symbol=symbol,
            exchange=MARKET_XTP2VT[data["market"]],
            orderid=str(data["order_xtp_id"]),
            sys_orderid=str(data["order_xtp_id"]),
            type=ORDERTYPE_XTP2VT.get(data["price_type"], OrderType.LIMIT),
            direction=direction,
            offset=offset,
            price=data["price"],
            volume=data["quantity"],
            traded=data["qty_traded"],
            status=STATUS_XTP2VT[data["order_status"]],
            datetime=dt,
            time=dt.strftime('%H:%M:%S'),
            gateway_name=self.gateway_name
        )

        self.gateway.on_order(order)

    def onTradeEvent(self, data: dict, session: int) -> None:
        """"""
        symbol = data["ticker"]
        if len(symbol) == 8:
            direction = DIRECTION_OPTION_XTP2VT[data["side"]]
            offset = OFFSET_XTP2VT[data["position_effect"]]
        else:
            direction, offset = DIRECTION_STOCK_XTP2VT[data["side"]]

        trade_time = str(data["trade_time"])
        dt = datetime.strptime(trade_time, '%Y%m%d%H%M%S%f')

        trade = TradeData(
            accountid=self.userid,
            symbol=symbol,
            exchange=MARKET_XTP2VT[data["market"]],
            orderid=str(data["order_xtp_id"]),
            sys_orderid=str(data["order_xtp_id"]),
            tradeid=str(data["exec_id"]),
            direction=direction,
            offset=offset,
            price=data["price"],
            volume=data["quantity"],
            datetime=dt,
            time=dt.strftime('%H:%M:%S'),
            gateway_name=self.gateway_name
        )

        self.gateway.on_trade(trade)

    def onCancelOrderError(self, data: dict, error: dict, session: int) -> None:
        """"""
        if not error or not error["error_id"]:
            return

        self.gateway.write_error("撤单失败", error)

    def onQueryOrder(self, data: dict, error: dict, last: bool, session: int) -> None:
        """"""
        pass

    def onQueryTrade(self, data: dict, error: dict, last: bool, session: int) -> None:
        """"""
        pass

    def onQueryPosition(
            self,
            data: dict,
            error: dict,
            request: int,
            last: bool,
            session: int
    ) -> None:
        """普通账号持仓"""
        # self.gateway.write_log(f"------\n {print_dict(data)}")

        if data["market"] == 0:
            return
        vt_symbol = '{}.{}'.format(data["ticker"], MARKET_XTP2VT[data["market"]].value)
        position = PositionData(
            accountid=self.userid,
            symbol=data["ticker"],
            exchange=MARKET_XTP2VT[data["market"]],
            name=data["ticker_name"],
            direction=POSITION_DIRECTION_XTP2VT[data["position_direction"]],
            volume=data["total_qty"],
            frozen=data["locked_position"],
            price=data["avg_price"],
            pnl=data["unrealized_pnl"],
            yd_volume=data["yesterday_position"],
            gateway_name=self.gateway_name,
            cur_price=self.gateway.prices.get(vt_symbol, 0)
        )
        if position.volume > 0 and position.cur_price > 0:
            position.pnl = round(position.volume * (position.cur_price - position.price), 2)
        self.gateway.on_position(position)

        # 如果持仓>0 获取持仓对应的当前最新价
        if position.volume > 0 and vt_symbol not in self.gateway.prices:
            req = SubscribeRequest(symbol=position.symbol, exchange=position.exchange)
            self.gateway.subscribe(req)
            self.security_volumes.update({vt_symbol: data["total_qty"]})

    def update_security_asset(self):
        """更新资产净值"""
        # self.gateway.write_log(f'更新资产净值')
        total_asset = 0
        for vt_symbol, volume in self.security_volumes.items():
            price = self.gateway.prices.get(vt_symbol, None)
            # 获取不到股票的最新价，所以当前security_asset不可用
            if price is None:
                self.gateway.write_log(f'取不到:{vt_symbol}的价格')
                self.security_asset = None
                symbol, exchange = extract_vt_symbol(vt_symbol)
                req = SubscribeRequest(symbol=symbol, exchange=exchange)
                self.gateway.subscribe(req)
                return

            total_asset += volume * price
            # self.gateway.write_log(f'资产净值 => {total_asset}')

        self.security_asset = total_asset

    def onQueryAsset(
            self,
            data: dict,
            error: dict,
            request: int,
            last: bool,
            session: int
    ) -> None:
        """"""
        #  XTP_ACCOUNT_NORMAL = 0,	///<普通账户
        #  XTP_ACCOUNT_CREDIT,	1	///<信用账户
        #  XTP_ACCOUNT_DERIVE,	2	///<衍生品账户
        #  XTP_ACCOUNT_UNKNOWN	3	///<未知账户类型
        if data['account_type'] != 0:
            return

        # self.gateway.write_log(print_dict(data))
        self.update_security_asset()

        if self.security_asset is not None:
            cash_asset = data["total_asset"]
            balance = cash_asset + self.security_asset

            account = AccountData(
                accountid=self.userid,
                balance=balance,  # 总资产
                margin=self.security_asset,  # 证券资产
                frozen=data["withholding_amount"],
                gateway_name=self.gateway_name,
                trading_day=datetime.now().strftime('%Y-%m-%d')
            )
            # AccountData缺省的available 计算方法有误，这里直接取可用资金
            account.available = cash_asset

            self.gateway.on_account(account)

        if data["account_type"] == 1:
            self.margin_trading = True
        elif data["account_type"] == 2:
            self.option_trading = True

    def onQueryStructuredFund(self, data: dict, error: dict, last: bool, session: int) -> None:
        """"""
        pass

    def onQueryFundTransfer(self, data: dict, error: dict, last: bool, session: int) -> None:
        """"""
        pass

    def onFundTransfer(self, data: dict, session: int) -> None:
        """"""
        pass

    def onQueryETF(self, data: dict, error: dict, last: bool, session: int) -> None:
        """"""
        pass

    def onQueryETFBasket(self, data: dict, error: dict, last: bool, session: int) -> None:
        """"""
        pass

    def onQueryIPOInfoList(self, data: dict, error: dict, last: bool, session: int) -> None:
        """"""
        pass

    def onQueryIPOQuotaInfo(self, data: dict, error: dict, last: bool, session: int) -> None:
        """"""
        pass

    def onQueryOptionAuctionInfo(self, data: dict, error: dict, last: bool, session: int) -> None:
        """"""
        pass

    def onQueryCreditDebtInfo(
            self,
            data: dict,
            error: dict,
            request: int,
            last: bool,
            session: int
    ) -> None:
        """信用账号持仓"""
        self.gateway.write_log(f"------\n {print_dict(data)}")
        if data["debt_type"] == 1:
            symbol = data["ticker"]
            exchange = MARKET_XTP2VT[data["market"]]

            position = self.short_positions.get(symbol, None)
            if not position:
                position = PositionData(
                    accountid=self.userid,
                    symbol=symbol,
                    exchange=exchange,
                    direction=Direction.SHORT,
                    gateway_name=self.gateway_name,
                    cur_price=self.gateway.prices.get(f'{symbol}.{exchange.value}', 0.0)
                )
                self.short_positions[symbol] = position

            position.volume += data["remain_qty"]

        if last:
            for position in self.short_positions.values():
                self.gateway.on_position(position)

            self.short_positions.clear()

    def connect(
            self,
            userid: str,
            password: str,
            client_id: int,
            server_ip: str,
            server_port: int,
            software_key: str
    ) -> None:
        """"""

        self.userid = userid
        self.password = password
        self.client_id = client_id
        self.server_ip = server_ip
        self.server_port = server_port
        self.software_key = software_key
        self.protocol = PROTOCOL_VT2XTP["TCP"]

        # Create API object
        if not self.connect_status:
            path = str(get_folder_path(self.gateway_name.lower()))
            self.createTraderApi(self.client_id, path)

            self.setSoftwareKey(self.software_key)
            self.subscribePublicTopic(0)
            self.login_server()

    def login_server(self) -> None:
        """"""
        n = self.login(
            self.server_ip,
            self.server_port,
            self.userid,
            self.password,
            self.protocol
        )

        if n:
            self.session_id = n
            self.connect_status = True
            self.login_status = True
            msg = f"交易服务器登录成功, 会话编号：{self.session_id}"
            self.init()

        else:
            error = self.getApiLastError()
            msg = f"交易服务器登录失败，原因：{error['error_msg']}"

        self.gateway.write_log(msg)

    def close(self) -> None:
        """"""
        if self.connect_status:
            self.exit()

    def send_order(self, req: OrderRequest) -> str:
        """"""
        if req.exchange not in MARKET_VT2XTP:
            self.gateway.write_log(f"委托失败，不支持的交易所{req.exchange.value}")
            return ""

        if req.type not in ORDERTYPE_VT2XTP:
            self.gateway.write_log(f"委托失败，不支持的委托类型{req.type.value}")
            return ""

        # check for option type
        if len(req.symbol) == 8:
            xtp_req = {
                "ticker": req.symbol,
                "market": MARKET_VT2XTP[req.exchange],
                "price": req.price,
                "quantity": int(req.volume),
                "side": DIRECTION_OPTION_VT2XTP.get(req.direction, ""),
                "position_effect": OFFSET_VT2XTP[req.offset],
                "price_type": ORDERTYPE_VT2XTP[req.type],
                "business_type": BUSINESS_VT2XTP["OPTION"]
            }

        # stock type
        else:
            req.offset = Offset.NONE
            xtp_req = {
                "ticker": req.symbol,
                "market": MARKET_VT2XTP[req.exchange],
                "price": req.price,
                "quantity": int(req.volume),
                "side": DIRECTION_STOCK_VT2XTP.get((req.direction, req.offset), ""),
                "price_type": ORDERTYPE_VT2XTP[req.type],
                "business_type": BUSINESS_VT2XTP[req.offset]
            }

        orderid = self.insertOrder(xtp_req, self.session_id)

        order = req.create_order_data(str(orderid), self.gateway_name)
        order.accountid = self.userid
        if order.datetime is None:
            order.datetime = datetime.now()
            order.time = order.datetime.strftime('%H:%M:%S.%f')
        self.gateway.on_order(order)

        return order.vt_orderid

    def cancel_order(self, req: CancelRequest) -> None:
        """"""
        self.cancelOrder(int(req.orderid), self.session_id)
        return True

    def query_account(self) -> None:
        """"""
        if not self.connect_status:
            return

        self.reqid += 1
        self.queryAsset(self.session_id, self.reqid)

    def query_position(self) -> None:
        """"""
        if not self.connect_status:
            return

        self.reqid += 1
        self.queryPosition("", self.session_id, self.reqid)

        if self.margin_trading:
            self.reqid += 1
            self.queryCreditDebtInfo(self.session_id, self.reqid)
