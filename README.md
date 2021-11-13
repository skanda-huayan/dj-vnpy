<p align="center">
  <img src ="https://github.com/msincenselee/vnpy/blob/vnpy2/huafu_on_premise.jpg"/>
  <img src ="https://gitee.com/vnpy2/vnpy/blob/vnpy2/huafu_on_premise.jpg"/>
</p>


# “当你想放弃时，想想你为什么开始。埃隆·马斯克”

github 链接： https://github.com/msincenselee/vnpy
gitee  链接： https://gitee.com/vnpy2/vnpy

###Fork版本主要改进如下

18、CTA股票选股引擎

    -vnpy.app.stock_screener
        + 无界面选股引擎
    -prod.screener_d1
        + 每日执行一次选股所有选股策略脚本
        
17、东财股票接入

    - vnpy.api.eastmoney_api,
        + 自动登录
        
    - vnpy.gateway.eastmoney
        + 普通账号、信用账号、期权账号
        
    - vnpy.data.eastmoney 
        + 下载网页版数据
       
    - prod.stock_em02_gw + prod.stock_em02_rpc01 + prod.stock_em02_rpc02
       分布式引擎脚本示例
       
16、EasyTrade股票接入（国金证券）

    - vnpy.api.easytrader, 
        + 直接使用，无需pip install easytrader；
        + 任然需要安装组件 pip install -r vnpy/api/easytrader/requirement.txt
    - vnpy.gateway.gj 国金证券的gateway
        + 使用了t.d.x作为股票基础数据
        + 使用了天.勤.作为行情服务
        + 使用了easytrader的remote_client作为接入.
    - prod.stock_qj 运行例子
        + run_es_restful_server.py 放在A机器，安装国金全能客户端。
        + run_main_gj01.py 放在B机器，运行vn_trader客户端
        
15、天.勤.行情接入
    
    - vnpy.data.tq 定制downloder，扩展下载字段
    - prod.jobs.refill_tq_future.ticks， 下载tick
    - vnpy.gateway.ctp.ctp_gateway 扩展支持上期所的五档行情和指数行情。在配置文件中增加'tq':{} 即可。
    
14、GUI界面增强
    
    - 交易界面，恢复部分v1版本的快捷功能，如快速平仓
    - 策略运行界面，增加'保存’,'K线' 按钮，保存策略内部数据，保存切片，查看最新切片K线。
    - K线切片,支持同一策略内多周期、多品种K线。    
    - 修改接口连接配置，采用更新方式，替代覆盖。


13、 增加App: cta_stock, 包括：
    
    - 增加baostock数据源，可下载股票基本信息，复权因子，非复权5Min数据k线，满足大部分Cta策略的回测了。
    - 使用tdx的历史逐笔成交数据，可缓存每日数据=>pkb2文件，支持tick回测。
    - 独立的CTA引擎 cta_stock，运行股票CTA策略时，替代原版cta_strategy引擎
    - 提供股票专用模板，支持目标股票买入卖出，市场盘面算法交易，支持策略多股票持久化
    - 支持策略中获取账号资金/可用余额/当前仓位/风控仓位
    - 支持策略中获取账号所有股票持仓
    - 支持bar/tick方式回测/组合回测
    - 支持可转债日内交易回测，支持动态前复权。
    - 支持盘前复权信息事件【待更新】    

12、 增加App: cta_crypto，包括：
    
    - 增加币安合约交易vnpy.gateway.binancef，支持每个合约独立杠杆比率
    - 增肌币安合约数据接口 vnpy.data.binance.binance_future_data
    - 独立的CTA引擎 cta_crypto，运行数字货币时，替代原版cta_strategy引擎。
    - 支持bar方式回测/组合回测
    - 增强期货交易模板
    - 修正vnpy.gateway.binance现货网关，恢复position
  

11、算法引擎

    - 支持自定义套利合约得算法，及算法下单。
    - 可通过vnpy界面/cta_strategy_pro策略，直接发出套利单，由算法引擎执行
    
10、增加App: account_recorder， 包括：
    
    - 异步更新账号资金/委托/成交信息至Mongo数据库
    - 异步更新策略持仓数据至Mongo数据库
    - 异步查询股票历史委托/历史成交至Mongo数据库     

9、  增强主引擎，包括：

    - 支持同一类gateway，多个接入配置
    - 增加获取当前价格接口
    - 增加风控引擎入口 self.rm_engine
    - 增加算法引擎入口，支持自定义套利合约得手工/程序化交易转移至算法引擎实现


8、 增加App: cta_strategy_pro，包括：

  
    - 提供策略实例的单独日志记录文件
    - 去除统一的策略数据持久化功能，改为策略内部自行实现。
    - 去除加载k线/tick初始化服务，改为策略内部自行实现。
    - 提供单独重启某一策略实例功能，可在线更新策略源码后，重启某一策略实例，不影响其他运行实例。
    - 支持单策略多合约行情订阅，支持指数合约行情订阅
    - 提供组合回测引擎，能够直接加载cta_strategy_pro_setting.json文件进行组合回测。
    - 拆分组合回测引擎和回测引擎，组合回测引擎支持bar/tick级别的组合回测
    - 增加定时器，推动策略on_timer
    - 增加定时推送策略持仓event
    - 增加CtaPro模板，支持精细化策略持久模板，
    - 增加CtaPro期货模板，支持FAK委托，自动换月等
    - 增加CtaSpread模板，支持FAK正套/反套
    - 增加Spread组合引擎tick级别回测，支持多策略实例得套利共享账号回测。
    


7、 增加component组件，包括:


    - 提供cta_line_bar k线组件，支持国内文华/交易师/TB等分钟/小时的计算模式，支持任意秒/分钟/小时/天/周等周期，支持k线数据实时生成。
    - 提供cta_renko_bar k线组件，支持x跳动/千分比跳动 
    - 提供cta_fund_kline 资金曲线组件，策略实例/账号基本的实时资金曲线 
    - 提供cta_position 组件，支持多/空/净仓位记录，支持套利
    - 提供cta_policy 组件，持久化复杂的策略执行逻辑
    - 提供cta_period 组件，支持策略中‘周期’的逻辑
    - 提供cta_grid_trade组件，支持网格交易、复杂的策略持仓逻辑、持久化 
    

6、 增强ctp_gateway，包括:

    
    - 提供指数行情订阅
    - 使用RabbitMQ指数源，或t.d.x单一数据源    
    - 提供自定义合约功能，实时提供其合成后的tick行情
    - 增加天勤行情，实现上期所5档行情和指数行情
        

5、 增加App: index_tick_publisher, 订阅通达信指数行情=》rabbit_mq 推送

     
4、 增加App: tick_recorder, 直接异步写入csv文件


3、 增加t.d.x 免费数据源,包括
    
     - 提供主力合约/指数合约的信息获取
     - 提供期货/股票数据bar 和分笔成交数据下载
     - 提供每日增量更新期货数据=> csv文件，可配合NFS+Celery，实现分布式回测

2、 增加rabbitMQ通信组件

1、 事件引擎，增加运行效率调试功能



大佳
QQ/Wechat：28888502

系列在线课程

    2021 股票CTA实战课程：http://www.uquant.org/course/49

    2021 期货缠论高级课程：http://www.uquant.org/course/48

    2021 数字货币CTA课程：http://www.uquant.org/course/46

    2020 期货套利课程：http://www.uquant.org/course/43

--------------------------------------------------------------------------------------------
#  原版 vn.py - 基于python的开源交易平台开发框架
https://github.com/vnpy/vnpy
--------------------------------------------------------------------------------------------
### License
MIT
