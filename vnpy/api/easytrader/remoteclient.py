# -*- coding: utf-8 -*-
import requests

from .utils.misc import file2dict
from vnpy.rpc import RpcClient



TIMEOUT = 10
class RemoteClient:
    def __init__(self, broker, host, port=1430, **kwargs):
        self._s = requests.session()
        self._api = "http://{}:{}".format(host, port)
        self._broker = broker

    def prepare(
        self,
        config_path=None,
        user=None,
        password=None,
        exe_path=None,
        comm_password=None,
        **kwargs
    ):
        """
        登陆客户端
        :param config_path: 登陆配置文件，跟参数登陆方式二选一
        :param user: 账号
        :param password: 明文密码
        :param exe_path: 客户端路径类似 r'C:\\htzqzyb2\\xiadan.exe',
            默认 r'C:\\htzqzyb2\\xiadan.exe'
        :param comm_password: 通讯密码
        :return:
        """
        params = locals().copy()
        params.pop("self")
        # if exe_path is None:
        #     params['exe_path'] = 'C:\\THS\\xiadan.exe'
        if config_path is not None:
            account = file2dict(config_path)
            params["user"] = account["user"]
            params["password"] = account["password"]

        params["broker"] = self._broker

        # prepare需要启动同花顺客户端，需要的时间比较长，所以超时给长一些时间
        response = self._s.post(self._api + "/prepare", json=params, timeout=60)
        if response.status_code >= 300:
            raise Exception(response.json()["error"])
        return response.json()

    @property
    def balance(self):
        return self.common_get("balance")

    @property
    def position(self):
        return self.common_get("position")

    @property
    def today_entrusts(self):
        return self.common_get("today_entrusts")

    @property
    def today_trades(self):
        return self.common_get("today_trades")

    @property
    def cancel_entrusts(self):
        return self.common_get("cancel_entrusts")

    def auto_ipo(self):
        return self.common_get("auto_ipo")

    def exit(self):
        return self.common_get("exit")

    def common_get(self, endpoint):
        response = self._s.get(self._api + "/" + endpoint, timeout=TIMEOUT)
        if response.status_code >= 300:
            print(Exception(response.json()["error"]))
        return response.json()

    def buy(self, security, price, amount, **kwargs):
        params = locals().copy()
        params.pop("self")

        response = self._s.post(self._api + "/buy", json=params, timeout=TIMEOUT)
        if response.status_code >= 300:
            raise Exception(response.json()["error"])
        return response.json()

    def sell(self, security, price, amount, **kwargs):
        params = locals().copy()
        params.pop("self")

        response = self._s.post(self._api + "/sell", json=params, timeout=TIMEOUT)
        if response.status_code >= 300:
            raise Exception(response.json()["error"])
        return response.json()

    def cancel_entrust(self, entrust_no):
        params = locals().copy()
        params.pop("self")

        response = self._s.post(self._api + "/cancel_entrust", json=params, timeout=TIMEOUT)
        if response.status_code >= 300:
            raise Exception(response.json()["error"])
        return response.json()

###########
# written by 黄健威
# 以下是新增加的ZMQ Client
# 整个接口对外保持和原来的一致
# 通过对原requests接口的“鸭子类型替换”来实现透明化

def use(broker, host, port=1430, use_zmq=True, **kwargs):
    if use_zmq:
        return ZMQRemoteClient(broker, host, port)
    else:
        return RemoteClient(broker, host, port)

class ZMQResponse(object):
    # 这个类是模仿requests的返回结果
    def __init__(self, status_code, data) -> None:
        self.data = data
        self.status_code = status_code

    def json(self):
        return self.data

class MyRpcClient(RpcClient):
    # 这个类把vnpy原生的rpc组件中的超时输出去除
    # 原版rpc组件中，如果上一个请求后30秒内没有新的请求，会输出一段提示
    def on_disconnected(self):
        pass

class ZMQSession(object):
    # 这个类是模仿requests的Session
    def __init__(self, host, port) -> None:
        req_addr = "tcp://{}:{}".format(host, port)
        sub_addr = "tcp://{}:{}".format(host, port+1)
        
        self._rpc_client = MyRpcClient()
        self._rpc_client.start(req_addr, sub_addr)

    def post(self, url, json=None, timeout=10):
        name = url.split("/")[-1]
        data, status_code = self._rpc_client.call_func(name, json)
        resp = ZMQResponse(status_code, data)
        return resp

    def get(self, url, json=None, timeout=10):
        return self.post(url, json, timeout)

    def __del__(self):
        # 当进程开始销毁对象时，显式调用stop来杀死后台的zmq线程，避免死锁无法退出
        self._rpc_client.stop()
    
class ZMQRemoteClient(RemoteClient):
    # 对原RemoteClient的重载
    def __init__(self, broker, host, port=1430, **kwargs):
        self._broker = broker
        
        # api这个项目已经不需要了
        self._api = ""
        # 替换Session
        self._s = ZMQSession(host, port)

    def __del__(self):
        del self._s