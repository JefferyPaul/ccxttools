=====     =====     =====     =====     config     =====     =====     =====     =====
一个账号一个文件夹
\Config\Config.json
    {
        "name": "",
        "apiKey": "",
        "secret": "",
        "BaseCurrency": "BUSD"
    }
\Config\CoinContractSpotMapping.csv
    用于计算对冲时，匹配标的
    CoinM持仓币,Spot标的,USDM对冲合约


=====     =====     =====     =====     =====     =====     =====     =====     =====
cal_target_holding_coin.py
计算 未在USDM账户中对冲的 CoinM账户的盈亏，并生成订单。
    - 读取 \Config\Config.json
        api信息
        BaseCurrency
    - 读取 \Config\CoinContractSpotMapping.csv
        CoinM持仓币,Spot买卖转账标的,USDM对冲合约
    - 连接api，获取 CoinM 的数币持仓。
    - 连接api，获取 USDM 的合约持仓。
    - 检查CoinM的数币持仓 和 USDM的合约持仓，是否在Mapping中，若否则弹框报错并停止运行。
    - 检查CoinM的数币持仓 和 USDM的合约持仓，是否属于BaseCurrency，若是则剔除。
    - 计算未对冲的需要下单的量，并生成订单

    生成订单：
    - 赚钱，CoinM持币多于USDM对冲持仓：从CoinM转出至Spot，从Spot卖出。
    - 亏钱，CoinM持币少于USDM对冲持仓：从Spot买入币，转至CoinM。


=====     =====     =====     =====     基础     =====     =====     =====     =====
create_order.py
扫描文件，交易下单
    文件格式：
        账户, 订单类型, 标的, 方向, 数量, 价格
    eg:
        Spot,limit,BTC/BUSD,buy,0.001,20000

    账户：Spot / USDM / CoinM
    订单类型：limit / market
    market订单，忽略价格，可以不输入price；limit订单，必须输入price

transfer.py
扫描文件，转账
    文件格式：
        标的, 数量, From, to
    eg：
        BTC,0.0353,CoinM,Spot

cancel_order.py
取消订单
    输入：
        -e 指定交易所，必须输入
        --symbol 指定标的，必须输入
        --id 指定order id，选择输入
    只输入symbol，不输入id：撤销所有跟此symbol相关的order；
    输入symbol和id：只撤销该id的order


=====     =====     =====     =====     查询     =====     =====     =====     =====
fetch_account_commonly_used_data.py
查询常用数据
    输入文件：
        \Config\CoinMapping.csv，可以不存在此文件，如果存在则读取。
        用于计算账户总净资产。
        计算是只参考3个标的的价格：USDT / BTC / ETH，但是有一些小品种，需要特定指定。
        例如：
            BUSD=USDT
            ETHW=0, 代表不计算
        eg:
            BUSD,USDT,
            LDBUSD,USDT,
            ETHW,0,
    输出：
        \Output\AccountData\

fetch_opening_orders.py
查询成交中的订单。只能指定标的查询，无法查询所有。
    输入：
        -e 指定交易所，必须输入
        -s 指定标的，必须输入
        -o 指定输出路径

