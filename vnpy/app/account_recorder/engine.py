# encoding: UTF-8

'''
账号相关数据记录
1. 监听 EVENT_ACCOUNT事件，数据 => mongodb Account.account_info 和 daily_info
2. 监听 EVENT_ORDER  事件，数据 => mongodb Account.today_orders
3. 监听 EVENT_TRADE  事件，数据 => mongodb Account.today_trades
4. 监听 EVENT_POSITION事件,数据 => mongodb Account.today_position
5. 监听股票接口的 EVENT_HISTORY_TRADE 事件， 数据 => history_orders
6. 监听股票接口的 EVENT_HISTORY_ORDER 事件， 数据 => history_trades
7. 监听股票接口的 EVENT_FUNDS_FLOW 事件， 数据 => funds_flow
8. 监听 EVENT_STRATEGY_POS事件，数据 =》 mongodb Account.today_strategy_pos
# 华富资产 李来佳

'''

import sys
import copy
import traceback

from datetime import datetime, timedelta
from queue import Queue
from threading import Thread
from time import time
from bson import binary

from vnpy.event import Event, EventEngine
from vnpy.trader.event import (
    EVENT_TIMER,
    EVENT_ACCOUNT,
    EVENT_ORDER,
    EVENT_TRADE,
    EVENT_POSITION,
    EVENT_HISTORY_TRADE,
    EVENT_HISTORY_ORDER,
    EVENT_FUNDS_FLOW,
    EVENT_STRATEGY_POS,
    EVENT_STRATEGY_SNAPSHOT,
    EVENT_ERROR,
    EVENT_WARNING,
    EVENT_CRITICAL,
)
from vnpy.trader.constant import Direction, Exchange, Status
from vnpy.trader.engine import BaseEngine, MainEngine
from vnpy.trader.utility import get_trading_date, load_json, save_json
from vnpy.data.mongo.mongo_data import MongoData

# 入库
ACCOUNT_DB_NAME = 'Account'
ACCOUNT_INFO_COL = 'account_info'
DAILY_INFO_COL = 'daily_info'
TODAY_ORDER_COL = 'today_orders'
TODAY_TRADE_COL = 'today_trades'
TODAY_STRATEGY_POS_COL = 'today_strategy_pos'
TODAY_POSITION_COL = 'today_positions'
HISTORY_ORDER_COL = 'history_orders'
HISTORY_TRADE_COL = 'history_trades'
HISTORY_STRATEGY_POS_COL = 'history_strategy_pos'
FUNDS_FLOW_COL = 'funds_flow'
STRATEGY_SNAPSHOT = 'strategy_snapshot'

ALERT_DB_NAME = "Alert"
GW_ERROR_COL_NAME = "gw_error_msg"
WARNING_COL_NAME = "warning_msg"
CRITICAL_COL_NAME = "critical_msg"

APP_NAME = "AccountRecorder"


########################################################################
class AccountRecorder(BaseEngine):
    """账号数据持久化"""
    setting_file_name = 'ar_setting.json'

    name = u'账户数据记录'

    # ----------------------------------------------------------------------
    def __init__(self, main_engine: MainEngine, event_engine: EventEngine):
        """Constructor"""

        super(AccountRecorder, self).__init__(
            main_engine, event_engine, APP_NAME)

        # 负责执行数据库插入的单独线程相关
        # 是否启动
        self.active = False
        self.queue = Queue()  # 队列
        self.thread = Thread(target=self.run)  # 线程

        # mongo数据库
        self.mongo_db = None

        # 账号的同步记录
        self.account_dict = {}  # gateway_name: setting

        self.is_7x24 = False  # 7 x 24 运行的账号( 数字货币)

        self.last_qry_dict = {}
        self.copy_history_orders = []  # 需要复制至历史成交的gateway名称
        self.copy_history_trades = []  # 需要复制至历史成交的gateway名称
        self.copy_history_strategypos = []  # 需要复制至历史策略持仓得gateway名称
        self.timer_count = 0

        self.event_list = []  # 只侦听处理的事件

        self.gw_name_acct_id = {}

        self.cur_trading_date = ""

        self.scaning_gw = []

        self.last_minute = None

        # 最近10条交易接口错误回报
        self.last_gw_error_msg = []

        # 最近10条系统严重错误信息
        self.last_critical_msg = []

        # 最近10条系统警告信息
        self.last_warning_msg = []

        # 加载配置文件
        self.load_setting()

        # 启动数据库写入线程
        self.start()

        # 启动事件侦听
        self.register_event()

    def close(self):
        print('AccountRecorder exit')
        self.stop()

    # ----------------------------------------------------------------------
    def load_setting(self):
        """读取配置"""
        self.write_log(f'{self.name}读取配置')
        try:
            d = load_json(self.setting_file_name)

            # mongo 数据库连接
            mongo_seetting = d.get('mongo_db', {})
            self.mongo_db = MongoData(host=mongo_seetting.get('host', 'localhost'),
                                      port=mongo_seetting.get('port', 27017))

            # 获取需要处理处理得账号配置
            self.account_dict = d.get('accounts', {})
            self.is_7x24 = d.get('is_7x24', False)

            self.event_list = d.get('event_list',[])

            # 识别配置，检查账号是否需要复制委托/成交到历史表
            for gateway_name, account_setting in self.account_dict.items():
                if account_setting.get('copy_history_orders', False):
                    self.copy_history_orders.append(gateway_name)

                if account_setting.get('copy_history_trades', False):
                    self.copy_history_trades.append(gateway_name)

                if account_setting.get('copy_history_strategypos', False):
                    self.copy_history_strategypos.append(gateway_name)

            self.write_log(f'{self.name}读取配置完成')
        except Exception as ex:
            self.main_engine.writeCritical(u'读取:{}异常:{}'.format(self.setting_file_name, str(ex)))

    def save_setting(self):
        try:
            save_json(self.setting_file_name, copy.copy(self.account_dict))
        except Exception as ex:
            self.main_engine.writeCritical(u'写入:{}异常:{}'.format(self.setting_file_name, str(ex)))

    # ----------------------------------------------------------------------
    def register_event(self):
        """注册事件监听"""
        self.event_engine.register(EVENT_TIMER, self.update_timer)

        if len(self.event_list) == 0 or EVENT_ACCOUNT in self.event_list:
            self.write_log(f'绑定EVENT_ACCOUNT事件 =>更新帐号净值')
            self.event_engine.register(EVENT_ACCOUNT, self.update_account)

        if len(self.event_list) == 0 or EVENT_ORDER in self.event_list:
            self.write_log(f'绑定EVENT_ORDER事件 =>更新委托事件')
            self.event_engine.register(EVENT_ORDER, self.update_order)

        if len(self.event_list) == 0 or EVENT_TRADE in self.event_list:
            self.write_log(f'绑定EVENT_TRADE事件 =>更新成交事件')
            self.event_engine.register(EVENT_TRADE, self.update_trade)

        if len(self.event_list) == 0 or EVENT_POSITION in self.event_list:
            self.write_log(f'绑定EVENT_POSITION事件 =>更新持仓事件')
            self.event_engine.register(EVENT_POSITION, self.update_position)

        if len(self.event_list) == 0 or EVENT_HISTORY_TRADE in self.event_list:
            self.write_log(f'绑定EVENT_HISTORY_TRADE事件 =>更新历史成交')
            self.event_engine.register(EVENT_HISTORY_TRADE, self.update_history_trade)

        if len(self.event_list) == 0 or EVENT_HISTORY_ORDER in self.event_list:
            self.write_log(f'绑定EVENT_HISTORY_ORDER事件 =>更新历史委托')
            self.event_engine.register(EVENT_HISTORY_ORDER, self.update_history_order)

        if len(self.event_list) == 0 or EVENT_FUNDS_FLOW in self.event_list:
            self.write_log(f'绑定EVENT_FUNDS_FLOW事件 =>更新资金流')
            self.event_engine.register(EVENT_FUNDS_FLOW, self.update_funds_flow)

        if len(self.event_list) == 0 or EVENT_STRATEGY_POS in self.event_list:
            self.write_log(f'绑定EVENT_STRATEGY_POS事件 =>更新策略持仓')
            self.event_engine.register(EVENT_STRATEGY_POS, self.update_strategy_pos)

        self.event_engine.register(EVENT_ERROR, self.process_gw_error)
        self.event_engine.register(EVENT_WARNING, self.process_warning)
        self.event_engine.register(EVENT_CRITICAL, self.process_critical)

        if len(self.event_list) == 0 or EVENT_STRATEGY_SNAPSHOT in self.event_list:
            self.write_log(f'绑定EVENT_STRATEGY_SNAPSHOT事件 =>更新策略切片')
            self.event_engine.register(EVENT_STRATEGY_SNAPSHOT, self.update_strategy_snapshot)

    # ----------------------------------------------------------------------
    def update_timer(self, event: Event):
        """更新定时器"""

        if len(self.account_dict) == 0 or self.active is False:
            return
        self.timer_count += 1

        if self.timer_count < 10:
            return

        self.timer_count = 0

        dt_now = datetime.now()

        if len(self.event_list) == 0 or 'eHistoryTrade.' in self.event_list:
            # 提交查询历史交易记录/历史委托/资金流水
            for data_type in [HISTORY_ORDER_COL, HISTORY_TRADE_COL, FUNDS_FLOW_COL]:
                dt = self.last_qry_dict.get(data_type, None)
                queryed = False
                for gw_name, account_info in self.account_dict.items():
                    if dt is None or (dt_now - dt).total_seconds() > 60 * 5:
                        begin_day = self.get_begin_day(gw_name, data_type).replace('-', '')
                        end_day = dt_now.strftime('%Y%m%d')
                        gw = self.main_engine.get_gateway(gw_name)
                        if gw is None:
                            self.write_log(f'Account_recorder找不到{gw_name}')
                        if hasattr(gw, 'qryHistory'):
                            self.write_log(u'向{}请求{}数据，{}~{}'.format(gw_name, data_type, begin_day, end_day))
                            gw.qryHistory(data_type, begin_day, end_day)
                        queryed = True

                if queryed:
                    self.last_qry_dict.update({data_type: dt})

    # ----------------------------------------------------------------------
    def update_account(self, event: Event):
        """更新账号资金"""
        account = event.data
        fld = {'account_id': account.accountid,
               'gateway_name': account.gateway_name,
               'trading_day': account.trading_day,
               'currency': account.currency}

        if len(account.accountid) == 0:
            return

        self.gw_name_acct_id.update({account.gateway_name: account.accountid})

        acc_data = copy.copy(account.__dict__)
        acc_data.update({'account_id': acc_data.pop('accountid')})
        # 更新至历史净值数据表
        self.update_data(db_name=ACCOUNT_DB_NAME, col_name=DAILY_INFO_COL, fld=copy.copy(fld),
                         data=acc_data)

        fld.pop('trading_day', None)
        # 更新至最新净值数据
        self.update_data(db_name=ACCOUNT_DB_NAME, col_name=ACCOUNT_INFO_COL, fld=fld, data=copy.copy(acc_data))

        self.remove_pre_trading_day_data(account.accountid, account.trading_day)

    def update_begin_day(self, gw_name, data_type, begin_day):
        """
        更新某一数据的开始日期
        :param gw_name: 网关名称
        :param data_type: 类型 history_trade, history_order, funds_flow
        :param begin_day: '%Y-%m-%d'格式字符串
        :return:
        """
        try:
            account_info = self.account_dict.get(gw_name, {})
            date_info = account_info.get(data_type, {})
            old_begin_day = date_info.get('begin_day', None)
            if old_begin_day is None or datetime.strptime(old_begin_day, '%Y-%m-%d') < datetime.strptime(begin_day,
                                                                                                         '%Y-%m-%d'):
                self.write_log(u'{}更新:{}:{}=>{}'.format(gw_name, data_type, old_begin_day, begin_day))
                date_info.update({'begin_day': begin_day})
                account_info.update({data_type: date_info})
                self.account_dict.update({gw_name: account_info})
                self.save_setting()

        except Exception as ex:
            self.main_engine.write_error(u'更新数据日期异常:{}'.format(str(ex)))
            self.write_log(traceback.format_exc())

    def get_begin_day(self, gw_name: str, data_type: str):
        """
        获取某一数据的开始日期
         :param gw_name: 网关名称
        :param data_type: 类型 history_trade, history_order, funds_flow
        :return:
        """
        account_info = self.account_dict.get(gw_name, {})
        date_info = account_info.get(data_type, {})
        begin_day = date_info.get('begin_day', None)
        if begin_day is None:
            begin_day = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')

        return begin_day

    def remove_pre_trading_day_data(self, account_id: str, trading_day: str):
        """
        移除非当前交易日得所有当日交易数据
        :param accountid:
        :param trading_day:
        :return:
        """
        if self.cur_trading_date == trading_day:
            return

        # 移除非当日得交易/持仓
        flt = {'account_id': account_id,
               'trade_date': {'$ne': trading_day}}
        self.write_log(f'移除非当日交易持仓:{flt}')
        self.mongo_db.db_delete(
            db_name=ACCOUNT_DB_NAME,
            col_name=TODAY_TRADE_COL,
            flt=flt)
        self.mongo_db.db_delete(
            db_name=ACCOUNT_DB_NAME,
            col_name=TODAY_POSITION_COL,
            flt=flt)

        # 移除非当日得委托
        flt = {'account_id': account_id,
               'order_date': {'$ne': trading_day}}
        self.write_log(f'移除非当日委托:{flt}')
        self.mongo_db.db_delete(
            db_name=ACCOUNT_DB_NAME,
            col_name=TODAY_ORDER_COL,
            flt=flt)

        # 移除非当日得持仓
        flt = {'account_id': account_id,
               'trading_day': {'$ne': trading_day}}
        self.write_log(f'移除非当日持仓:{flt}')
        self.mongo_db.db_delete(
            db_name=ACCOUNT_DB_NAME,
            col_name=TODAY_STRATEGY_POS_COL,
            flt=flt)

        self.cur_trading_date = trading_day

    def update_order(self, event: Event):
        """更新当日记录"""
        order = event.data
        # self.write_log(u'记录委托日志:{}'.format(order.__dict__))
        if len(order.sys_orderid) == 0:
            # 未有系统的委托编号，不做持久化
            order.sys_orderid = order.orderid
        if order.status in [Status.SUBMITTING]:
            return

        dt = getattr(order, 'datetime')
        if not dt:
            order_date = datetime.now().strftime('%Y-%m-%d')
            if len(order.time) > 0 and '.' not in order.time:
                dt = datetime.strptime(f'{order_date} {order.time}', '%Y-%m-%d %H:%M:%S')
                order.datetime = dt
        else:
            order_date = dt.strftime('%Y-%m-%d')

        # 数据库需要提前建立多重索引
        # db.today_orders.createIndex({'account_id':1,'vt_symbol':1,'sys_orderid':1,'order_date':1,'holder_id':1},{'name':'accountid_vtsymbol_sysorderid_order_date_holder_id','unique':true})

        fld = {'vt_symbol': order.vt_symbol,
               'account_id': order.accountid,
               'sys_orderid': order.sys_orderid,
               'order_date': order_date,
               'holder_id': getattr(order, 'holder_id', '')}

        data = copy.deepcopy(order.__dict__)
        data.update({'account_id': data.pop('accountid')})
        data.update({'order_date': order_date})
        data.update({'exchange': order.exchange.value})
        data.update({'direction': order.direction.value})
        data.update({'offset': order.offset.value})
        data.update({'type': order.type.value})
        data.update({'status': order.status.value})

        if order.exchange in [Exchange.SSE, Exchange.SZSE]:
            if hasattr(self.main_engine, 'get_name'):
                data.update({'name': self.main_engine.get_name(order.vt_symbol)})

        self.update_data(db_name=ACCOUNT_DB_NAME, col_name=TODAY_ORDER_COL, fld=fld, data=data)

        # 数据库需要提前建立多重索引
        # db.history_orders.createIndex({'account_id':1,'vt_symbol':1,'sys_orderid':1,'order_date':1,'holder_id':1},{'name':'history_accountid_vtsymbol_sysorderid_order_date_holder_id'})

        # 复制委托记录=》历史委托记录
        if order.gateway_name in self.copy_history_orders:
            history_data = copy.copy(data)
            fld2 = {'vt_symbol': order.vt_symbol,
                    'account_id': order.accountid,
                    'sys_orderid': order.sys_orderid,
                    'order_date': order_date,
                    'holder_id': getattr(order, 'holder_id', '')}

            self.update_data(db_name=ACCOUNT_DB_NAME, col_name=HISTORY_ORDER_COL, fld=fld2, data=history_data)

    def get_trading_date(self, dt: datetime):
        if self.is_7x24:
            return dt.strftime('%Y-%m-%d')
        else:
            return get_trading_date(dt)

    def update_trade(self, event: Event):
        """更新当日成交"""
        trade = event.data
        trade_date = self.get_trading_date(datetime.now())

        fld = {'vt_symbol': trade.vt_symbol,
               'account_id': trade.accountid,
               'vt_tradeid': trade.vt_tradeid,
               'trade_date': trade_date,
               'holder_id': trade.holder_id}

        # 提前创建索引
        # db.today_trades.createIndex({'account_id':1,'vt_symbol':1,'vt_tradeid':1,'trade_date':1,'holder_id':1},{'name':'accountid_vtSymbol_vt_tradeid_trade_date_holder_id','unique':true})

        data = copy.deepcopy(trade.__dict__)
        data.update({'account_id': data.pop('accountid')})
        data.update({'trade_date': trade_date})
        data.update({'exchange': trade.exchange.value})
        data.update({'direction': trade.direction.value})
        data.update({'offset': trade.offset.value})

        self.update_data(db_name=ACCOUNT_DB_NAME, col_name=TODAY_TRADE_COL, fld=fld, data=data)

        # db.history_trades.createIndex({'account_id':1,'vt_symbol':1,'vt_tradeid':1,'trade_date':1,'holder_id':1},{'name':'accountid_vtSymbol_vt_tradeid_trade_date_holder_id'})

        # 复制成交记录=》历史成交记录
        if trade.gateway_name in self.copy_history_trades:
            history_trade = copy.copy(data)
            fld2 = {'vt_symbol': trade.vt_symbol,
                    'account_id': trade.accountid,
                    'vt_tradeid': trade.vt_tradeid,
                    'trade_date': trade_date,
                    'holder_id': trade.holder_id}

            self.update_data(db_name=ACCOUNT_DB_NAME, col_name=HISTORY_TRADE_COL, fld=fld2, data=history_trade)

    def update_position(self, event: Event):
        """更新当日持仓"""
        pos = event.data
        trade_date = self.get_trading_date(datetime.now())

        # 不处理交易所返回得套利合约
        if pos.symbol.startswith('SP') and '&' in pos.symbol and ' ' in pos.symbol:
            return

        fld = {'vt_symbol': pos.vt_symbol,
               'direction': pos.direction.value,
               'account_id': pos.accountid,
               'trade_date': trade_date,
               'holder_id': pos.holder_id}

        #  db.today_positions.createIndex({'account_id':1,'vt_symbol':1,'direction':1,'trade_date':1,'holder_id':1},{'name':'accountid_vtsymbol_direction_trade_date_holder_id'})

        data = copy.copy(pos.__dict__)
        data.update({'account_id': data.pop('accountid')})
        data.update({'trade_date': trade_date})
        data.update({'exchange': pos.exchange.value})
        data.update({'direction': pos.direction.value})

        # 补充 当前价格
        if pos.cur_price == 0:
            price = self.main_engine.get_price(pos.vt_symbol)
            if price:
                data.update({'cur_price': price})

        self.update_data(db_name=ACCOUNT_DB_NAME, col_name=TODAY_POSITION_COL, fld=fld, data=data)

    def update_strategy_snapshot(self, event: Event):
        """更新策略切片"""
        snapshot = event.data
        # self.write_log(f"保存切片,{snapshot.get('account_id')},策略:{snapshot.get('strategy')}")
        klines = snapshot.pop('klines', None)
        if klines:
            self.write_log(f"转换 =>BSON.binary.Binary")
            snapshot.update({'klines': binary.Binary(klines)})
        fld = {
            'account_id': snapshot.get('account_id'),
            'strategy': snapshot.get('strategy'),
            'guid': snapshot.get('guid')
        }
        self.update_data(db_name=ACCOUNT_DB_NAME, col_name=STRATEGY_SNAPSHOT, fld=fld, data=snapshot)

    def update_history_trade(self, event: Event):
        """更新历史查询记录"""
        trade = event.data
        trade_date = trade.time.split(' ')[0]

        fld = {'vt_symbol': trade.vt_symbol,
               'account_id': trade.accountid,
               'vt_tradeid': trade.vt_tradeid,
               'trade_date': trade_date,
               'holder_id': trade.holder_id}

        data = copy.copy(trade.__dict__)
        data.update({'account_id': data.pop('accountid')})
        data.update({'trade_date': trade_date})
        self.update_data(db_name=ACCOUNT_DB_NAME, col_name=HISTORY_TRADE_COL, fld=fld, data=data)
        self.update_begin_day(trade.gateway_name, HISTORY_TRADE_COL, trade_date)

    def update_history_order(self, event: Event):
        """更新历史委托"""
        order = event.data
        order_date = order.time.split(' ')[0]
        fld = {'vt_symbol': order.vt_symbol,
               'account_id': order.accountid,
               'sys_orderid': order.sys_orderid,
               'order_date': order_date,
               'holder_id': order.holder_id}
        data = copy.copy(order.__dict__)
        data.update({'account_id': data.pop('accountid')})
        data.update({'order_date': order_date})
        self.update_data(db_name=ACCOUNT_DB_NAME, col_name=HISTORY_ORDER_COL, fld=fld, data=data)

        self.update_begin_day(order.gateway_name, HISTORY_ORDER_COL, order_date)

    def update_funds_flow(self, event: Event):
        """更新历史资金流水"""
        funds_flow = event.data
        data = copy.copy(funds_flow.__dict__)

        fld = {'account_id': funds_flow.accountid,
               'trade_date': funds_flow.trade_date,
               'trade_amount': funds_flow.trade_amount,
               'fund_remain': funds_flow.fund_remain,
               'contract_id': funds_flow.contract_id,
               'holder_id': funds_flow.holder_id}
        data.update({'account_id': data.pop('accountid')})
        self.update_data(db_name=ACCOUNT_DB_NAME, col_name=FUNDS_FLOW_COL, fld=fld, data=data)

        self.update_begin_day(funds_flow.gateway_name, HISTORY_ORDER_COL, funds_flow.trade_date)

    def update_strategy_pos(self, event: Event):
        """更新策略持仓事件"""
        data = event.data
        dt = data.get('datetime')

        #  db.today_strategy_pos.createIndex({'account_id':1,'strategy_group':1,'strategy_name':1,'date':1},{'name':'accountid_strategy_group_strategy_name_date'})

        account_id = data.get('accountid')
        fld = {
            'account_id': account_id,
            'strategy_group': data.get('strategy_group', '-'),
            'strategy_name': data.get('strategy_name')
        }
        pos_data = copy.copy(data)
        pos_data.update({'account_id': pos_data.get('accountid')})
        pos_data.update({'datetime': dt.strftime("%Y-%m-%d %H:%M:%S")})

        self.update_data(db_name=ACCOUNT_DB_NAME, col_name=TODAY_STRATEGY_POS_COL, fld=fld, data=pos_data)

    def process_gw_error(self, event: Event):
        """ 处理gw的回报错误日志"""
        try:
            data = event.data
            if data.errorMsg not in self.last_gw_error_msg:
                d = {}
                d.update({'gateway_name': data.gateway_name})
                d.update({'msg': data.msg})
                d.update({'additional_info': data.additional_info})
                d.update({'log_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S")})
                d.update({'trading_day': self.get_trading_date(datetime.now())})

                account_id = self.gw_name_acct_id.get(data.gateway_name, None)
                if account_id:
                    d.update({'account_id': account_id})

                    fld = copy.copy(d)

                    self.update_data(db_name=ALERT_DB_NAME, col_name=GW_ERROR_COL_NAME, fld=fld, data=d)

                    self.last_gw_error_msg.append(data.errorMsg)

            if len(self.last_gw_error_msg) >= 10:
                self.last_gw_error_msg.pop(0)

        except Exception as ex:
            print(u'处理gw告警日志异常:{}'.format(str(ex)), file=sys.stderr)

    def process_warning(self, event: Event):
        """处理日志"""
        try:
            data = event.data
            if data.msg not in self.last_warning_msg:
                d = {}
                d.update({'gateway_name': data.gateway_name})
                d.update({'msg': data.msg})
                d.update({'additional_info': data.additional_info})
                d.update({'log_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S")})
                d.update({'trading_day': get_trading_date()})

                account_id = self.gw_name_acct_id.get(data.gateway_name, None)
                if account_id:
                    d.update({'account_id': account_id})
                    fld = copy.copy(d)
                    self.update_data(db_name=ALERT_DB_NAME, col_name=WARNING_COL_NAME, fld=fld, data=d)

            if len(self.last_warning_msg) >= 10:
                self.last_warning_msg.pop(0)

        except Exception as ex:
            print(u'处理vn系统告警日志异常:{}'.format(str(ex)), file=sys.stderr)

    def process_critical(self, event: Event):
        """处理日志"""
        try:
            data = event.data
            if data.msg not in self.last_critical_msg:
                d = {}
                d.update({'gateway_name': data.gateway_name})
                d.update({'msg': data.msg})
                d.update({'additional_info': data.additional_info})
                d.update({'log_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S")})
                d.update({'trading_day': self.get_trading_date(datetime.now())})

                account_id = self.gw_name_acct_id.get(data.gateway_name, None)
                if account_id:
                    d.update({'account_id': account_id})
                    fld = copy.copy(d)
                    self.update_data(db_name=ALERT_DB_NAME, col_name=CRITICAL_COL_NAME, fld=fld, data=d)

            if len(self.last_critical_msg) >= 10:
                self.last_critical_msg.pop(0)

        except Exception as ex:
            print(u'处理vn系统告警日志异常:{}'.format(str(ex)), file=sys.stderr)

    # ----------------------------------------------------------------------
    def update_data(self, db_name, col_name, fld, data):
        """更新或插入数据到数据库"""
        self.queue.put((db_name, col_name, fld, data))

    # ----------------------------------------------------------------------
    def run(self):
        """运行插入线程"""
        while self.active:
            try:
                db_name, col_name, fld, d = self.queue.get(block=True, timeout=1)
                t1 = time()
                self.mongo_db.db_update(db_name=db_name,
                                        col_name=col_name,
                                        data_dict=d,
                                        filter_dict=fld,
                                        upsert=True)
                t2 = time()
                execute_ms = (int(round(t2 * 1000))) - (int(round(t1 * 1000)))
                if execute_ms > 200:
                    self.write_log(u'运行 {}.{} 更新 耗时:{}ms >200ms,数据:{}'
                                   .format(db_name, col_name, execute_ms, d))

            except Exception as ex:  # noqa
                pass

    # ----------------------------------------------------------------------
    def start(self):
        """启动"""
        self.active = True
        self.thread.start()
        self.write_log(f'账号记录引擎启动')

    # ----------------------------------------------------------------------
    def stop(self):
        """退出"""
        self.write_log(f'账号记录引擎退出')
        if self.mongo_db:
            self.mongo_db = None

        if self.active:
            self.active = False
            self.thread.join()
