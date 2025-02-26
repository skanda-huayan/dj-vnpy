""""""
from copy import copy
from typing import Dict, List

from .engine import MainEngine
from .object import (
    ContractData,
    OrderData,
    TradeData,
    PositionData,
    OrderRequest
)
from .constant import Direction, Offset, Exchange


class OffsetConverter:
    """
    仓位转换
    """

    def __init__(self, main_engine: MainEngine):
        """"""
        self.main_engine: MainEngine = main_engine
        self.holdings: Dict[str, "PositionHolding"] = {}

    def update_position(self, position: PositionData) -> None:
        """"""
        # if not self.is_convert_required(position.vt_symbol):
        #     return

        holding = self.get_position_holding(position.vt_symbol, position.gateway_name)
        if holding:
            holding.update_position(position)

    def update_trade(self, trade: TradeData) -> None:
        """"""
        if not self.is_convert_required(trade.vt_symbol):
            return

        holding = self.get_position_holding(trade.vt_symbol, trade.gateway_name)
        holding.update_trade(trade)

    def update_order(self, order: OrderData) -> None:
        """"""
        if not self.is_convert_required(order.vt_symbol):
            return

        holding = self.get_position_holding(order.vt_symbol, order.gateway_name)
        holding.update_order(order)

    def update_order_request(self, req: OrderRequest, vt_orderid: str, gateway_name: str = '') -> None:
        """"""
        if not self.is_convert_required(req.vt_symbol):
            return

        holding = self.get_position_holding(req.vt_symbol, gateway_name)
        holding.update_order_request(req, vt_orderid)

    def get_position_holding(self, vt_symbol: str, gateway_name: str = '') -> "PositionHolding":
        """获取持仓信息"""
        if gateway_name is None or len(gateway_name) == 0:
            if len(self.main_engine.gateways.keys()) == 1:
                gateway_name = list(self.main_engine.gateways.keys())[0]
            else:
                contract = self.main_engine.get_contract(vt_symbol)
            if contract:
                gateway_name = contract.gateway_name

        k = f'{gateway_name}.{vt_symbol}'
        holding = self.holdings.get(k, None)
        if not holding:
            contract = self.main_engine.get_contract(vt_symbol)
            if contract is None:
                return None
            holding = PositionHolding(contract)
            self.holdings[k] = holding
        return holding

    def convert_order_request(self, req: OrderRequest, lock: bool, gateway_name: str = '') -> List[OrderRequest]:
        """转换委托单"""
        # 合约是净仓，不具有多空，不需要转换
        if not self.is_convert_required(req.vt_symbol):
            return [req]

        # 获取当前持仓信息
        holding = self.get_position_holding(req.vt_symbol, gateway_name)

        if lock:
            # 锁仓转换
            return holding.convert_order_request_lock(req)

        # 平今/平昨拆分
        elif req.exchange in [Exchange.SHFE, Exchange.INE]:
            print(f'转换平今/平昨')
            return holding.convert_order_request_shfe(req)
        else:
            return [req]

    def is_convert_required(self, vt_symbol: str) -> bool:
        """
        Check if the contract needs offset convert.
        """
        contract = self.main_engine.get_contract(vt_symbol)

        # Only contracts with long-short position mode requires convert
        if not contract:
            return False
        elif contract.net_position:
            return False
        else:
            return True


class PositionHolding:
    """"""

    def __init__(self, contract: ContractData):
        """"""
        self.vt_symbol: str = contract.vt_symbol
        self.exchange: Exchange = contract.exchange

        self.active_orders: Dict[str, OrderData] = {}

        self.long_pos: float = 0
        self.long_yd: float = 0
        self.long_td: float = 0

        self.short_pos: float = 0
        self.short_yd: float = 0
        self.short_td: float = 0

        self.long_pos_frozen: float = 0
        self.long_yd_frozen: float = 0
        self.long_td_frozen: float = 0

        self.short_pos_frozen: float = 0
        self.short_yd_frozen: float = 0
        self.short_td_frozen: float = 0

    def update_position(self, position: PositionData) -> None:
        """"""
        if position.direction == Direction.LONG:
            self.long_pos = position.volume
            self.long_yd = position.yd_volume
            self.long_td = round(self.long_pos - self.long_yd, 7)
        else:
            self.short_pos = position.volume
            self.short_yd = position.yd_volume
            self.short_td = round(self.short_pos - self.short_yd, 7)

    def update_order(self, order: OrderData) -> None:
        """"""
        if order.is_active():
            self.active_orders[order.vt_orderid] = order
        else:
            if order.vt_orderid in self.active_orders:
                self.active_orders.pop(order.vt_orderid)

        self.calculate_frozen()

    def update_order_request(self, req: OrderRequest, vt_orderid: str) -> None:
        """"""
        gateway_name, orderid = vt_orderid.split(".")

        order = req.create_order_data(orderid, gateway_name)
        self.update_order(order)

    def update_trade(self, trade: TradeData) -> None:
        """更新交易"""

        if trade.direction == Direction.LONG:
            # 多，开仓 =》 增加今仓
            if trade.offset == Offset.OPEN:
                self.long_td += trade.volume
            # 多，平今 =》减少今仓
            elif trade.offset == Offset.CLOSETODAY:
                self.short_td -= trade.volume
            # 多，平昨 =》减少昨仓
            elif trade.offset == Offset.CLOSEYESTERDAY:
                self.short_yd -= trade.volume
            # 多，平仓 =》 减少
            elif trade.offset == Offset.CLOSE:
                if trade.exchange in [Exchange.SHFE, Exchange.INE] and self.short_yd >= trade.volume:
                    self.short_yd -= trade.volume
                else:
                    self.short_td -= trade.volume

                    if self.short_td < 0:
                        self.short_yd += self.short_td
                        self.short_td = 0
            self.short_yd = round(self.short_yd, 7)
            self.short_td = round(self.short_td, 7)

        else:
            if trade.offset == Offset.OPEN:
                self.short_td += trade.volume
            elif trade.offset == Offset.CLOSETODAY:
                self.long_td -= trade.volume
            elif trade.offset == Offset.CLOSEYESTERDAY:
                self.long_yd -= trade.volume
            elif trade.offset == Offset.CLOSE:
                if trade.exchange in [Exchange.SHFE, Exchange.INE] and self.long_yd >= trade.volume:
                    self.long_yd -= trade.volume
                else:
                    self.long_td -= trade.volume

                    if self.long_td < 0:
                        self.long_yd += self.long_td
                        self.long_td = 0
            self.long_td = round(self.long_td, 7)
            self.long_yd = round(self.long_yd, 7)

        self.long_pos = round(self.long_td + self.long_yd, 7)
        self.short_pos = round(self.short_td + self.short_yd, 7)

    def calculate_frozen(self) -> None:
        """"""
        self.long_pos_frozen = 0
        self.long_yd_frozen = 0
        self.long_td_frozen = 0

        self.short_pos_frozen = 0
        self.short_yd_frozen = 0
        self.short_td_frozen = 0

        for order in self.active_orders.values():
            # Ignore position open orders
            if order.offset == Offset.OPEN:
                continue

            frozen = round(order.volume - order.traded, 7)

            if order.direction == Direction.LONG:
                if order.offset == Offset.CLOSETODAY:
                    self.short_td_frozen += frozen
                elif order.offset == Offset.CLOSEYESTERDAY:
                    self.short_yd_frozen += frozen
                elif order.offset == Offset.CLOSE:
                    self.short_td_frozen += frozen

                    if self.short_td_frozen > self.short_td:
                        self.short_yd_frozen += (self.short_td_frozen
                                                 - self.short_td)
                        self.short_td_frozen = self.short_td
            elif order.direction == Direction.SHORT:
                if order.offset == Offset.CLOSETODAY:
                    self.long_td_frozen += frozen
                elif order.offset == Offset.CLOSEYESTERDAY:
                    self.long_yd_frozen += frozen
                elif order.offset == Offset.CLOSE:
                    self.long_td_frozen += frozen

                    if self.long_td_frozen > self.long_td:
                        self.long_yd_frozen += (self.long_td_frozen
                                                - self.long_td)
                        self.long_td_frozen = self.long_td

            self.long_pos_frozen = round(self.long_td_frozen + self.long_yd_frozen, 7)
            self.short_pos_frozen = round(self.short_td_frozen + self.short_yd_frozen, 7)

    def convert_order_request_shfe(self, req: OrderRequest) -> List[OrderRequest]:
        """上期所，委托单拆分"""
        if req.offset == Offset.OPEN:
            return [req]

        if req.direction == Direction.LONG:
            pos_available = self.short_pos - self.short_pos_frozen
            td_available = self.short_td - self.short_td_frozen
        else:
            pos_available = self.long_pos - self.long_pos_frozen
            td_available = self.long_td - self.long_td_frozen

        if req.volume > pos_available:
            print(f'{req.vt_symbol}没有可用仓位')
            return []
        elif req.volume <= td_available:
            req_td = copy(req)
            req_td.offset = Offset.CLOSETODAY
            print(f'{req.vt_symbol} 平仓=>平今')
            return [req_td]
        else:
            req_list = []

            if td_available > 0:
                req_td = copy(req)
                req_td.offset = Offset.CLOSETODAY
                req_td.volume = td_available
                print(f'{req.vt_symbol} 平仓 {req_td.volume}手 =>平今')
                req_list.append(req_td)

            req_yd = copy(req)
            req_yd.offset = Offset.CLOSEYESTERDAY
            req_yd.volume = req.volume - td_available
            print(f'{req.vt_symbol} 平仓 {req_yd.volume}手 =>平昨')
            req_list.append(req_yd)

            return req_list

    def convert_order_request_lock(self, req: OrderRequest) -> List[OrderRequest]:
        """"""
        if req.direction == Direction.LONG:
            td_volume = self.short_td
            yd_available = self.short_yd - self.short_yd_frozen
        else:
            td_volume = self.long_td
            yd_available = self.long_yd - self.long_yd_frozen

        # If there is td_volume, we can only lock position
        if td_volume:
            req_open = copy(req)
            req_open.offset = Offset.OPEN
            return [req_open]
        # If no td_volume, we close opposite yd position first
        # then open new position
        else:
            close_volume = min(req.volume, yd_available)
            open_volume = max(0, req.volume - yd_available)
            req_list = []

            if yd_available:
                req_yd = copy(req)
                if self.exchange in [Exchange.SHFE, Exchange.INE]:
                    req_yd.offset = Offset.CLOSEYESTERDAY
                else:
                    req_yd.offset = Offset.CLOSE
                req_yd.volume = close_volume
                req_list.append(req_yd)

            if open_volume:
                req_open = copy(req)
                req_open.offset = Offset.OPEN
                req_open.volume = open_volume
                req_list.append(req_open)

            return req_list

    def to_str(self):
        return f'Long:{self.long_pos},yd:{self.long_yd},td:{self.long_td}; Short:{self.short_pos},yd:{self.short_yd},td:{self.short_yd}'
