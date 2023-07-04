
import os
import sys
from collections import defaultdict, Counter
from typing import Dict, List
import json
from dataclasses import dataclass
from datetime import datetime

import ccxt

PATH_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.append(PATH_ROOT)

from helper.simpleLogger import MyLogger
from helper.tp_WarningBoard import run_warning_board


# ==================    读取config，基础参数 ==============
PATH_CONFIG = os.path.join(PATH_ROOT, 'Config', 'Config.json')
# api
EXCHANGE_API_MAPPING = {
    "Spot": ccxt.binance,
    "USDM": ccxt.binanceusdm,
    "CoinM": ccxt.binancecoinm
}
# 运行等待时间
delay = 1
#
logger_obj = MyLogger('ccxttolls', output_root=os.path.join(PATH_ROOT, 'logs'))


# 读取本地配置
def read_api_config_file():
    _d = json.loads(open(PATH_CONFIG).read())
    api_config = {
        "apiKey": _d["apiKey"],
        "secret": _d["secret"],
        'trust_env': True
    }
    return api_config


# ==================   object =====================

@dataclass
class SimplePosition:
    api_name: str
    symbol: str
    amount: float

    def to_output_string(self):
        return ','.join([self.api_name, self.symbol, str(self.amount)])


@dataclass
class SimpleBalance:
    symbol: str
    total: float
    used: float
    free: float

    def __init__(self, symbol, total=0, used=0, free=0, *args, **kwargs):
        self.symbol = symbol
        self.total = total
        self.used = used
        self.free = free

    def to_output_string(self):
        return ','.join([self.symbol, str(self.total), str(self.used), str(self.free)])


@dataclass
class SimpleTrade:
    id: str
    symbol: str
    side: str
    price: float
    amount: float
    cost: float
    fee: float
    feeCurrency: str
    timestamp: int
    order: str

    def to_output_string(self):
        return ','.join([
            str(_) for _ in [
                self.id, self.symbol, self.side, self.price, self.amount,
                self.cost, '%s_%s' % (str(self.fee), self.feeCurrency),
                datetime.fromtimestamp(self.timestamp/1000).strftime('%Y-%m-%d %H:%M:%S'),
                self.order
            ]
        ])

    @classmethod
    def from_ccxt(cls, data):
        return cls(
            id=data['id'],
            symbol=data['symbol'],
            side=data['side'],
            price=data['price'],
            amount=data['amount'],
            cost=data['cost'],
            fee=data['fee']['cost'],
            feeCurrency=data['fee']['currency'],
            timestamp=data['timestamp'],
            order=data['order'],
        )

    def __str__(self):
        return str(self.__dict__)


# 成交中订单状态
@dataclass
class SimpleOpeningOrder:
    id: str
    status: str
    symbol: str
    type: str
    side: str
    price: float
    amount: float
    filled: float
    average: float
    timestamp: int

    def to_output_string(self):
        return ','.join([
            str(_) for _ in [
                self.id, self.status, self.symbol, self.type, self.side,
                self.price, self.amount, self.filled, self.average,
                datetime.fromtimestamp(self.timestamp/1000).strftime('%Y-%m-%d %H:%M:%S'),
            ]
        ])

    def __str__(self):
        return str(self.__dict__)

    @classmethod
    def from_ccxt(cls, data):
        return cls(
            id=data['id'],
            status=data['status'],
            symbol=data['symbol'],
            type=data['type'],
            side=data['side'],
            price=data['price'],
            amount=data['amount'],
            filled=data['filled'],
            average=data['average'],
            timestamp=data['timestamp'],
        )


@dataclass
class SimpleOrder:
    id: str
    symbol: str
    status: str
    type: str
    side: str
    price: float
    average: float
    amount: float
    filled: float
    cost: float
    fee: float
    feeCurrency: str
    timestamp: float

    def to_output_string(self):
        return ','.join([
            str(_) for _ in [
                self.id, self.status, self.symbol, self.type, self.side,
                self.price, self.amount, self.filled, self.average, self.cost,
                f'{str(self.fee)}_{self.feeCurrency}',
                datetime.fromtimestamp(self.timestamp/1000).strftime('%Y-%m-%d %H:%M:%S'),
            ]
        ])

    def __str__(self):
        return str(self.__dict__)

    @classmethod
    def from_ccxt(cls, data):
        return cls(
            id=data['id'],
            symbol=data['symbol'],
            status=data['status'],
            type=data['type'],
            side=data['side'],
            price=data['price'],
            average=data['average'],
            amount=data['amount'],
            filled=data['filled'],
            cost=data['cost'],
            fee=data['fee']['cost'],
            feeCurrency=data['fee']['currency'],
            timestamp=data['timestamp'],
        )


# 订单
class Order:
    def __init__(self, api_name, order_type, symbol, amount, price=None, ):
        assert api_name in ['Spot', 'USDM', 'CoinM']
        assert order_type in ['market', 'limit']
        if order_type == 'limit':
            assert (price is not None)

        self.api_name = api_name
        self.order_type = order_type
        self.symbol = symbol
        self.amount = amount
        self.price = price

    @property
    def side(self):
        if self.amount >= 0:
            return 'buy'
        else:
            return 'sell'

    def to_output_string(self):
        if not self.price:
            _price = ''
        else:
            _price = self.price
        return ','.join([self.api_name, self.order_type, self.symbol, self.side, str(abs(self.amount)), str(_price)])

    @classmethod
    def from_file_string(cls, line):
        line_split = line.split(',')
        if len(line_split) != 6:
            assert ValueError
        amount = float(line_split[4]) * {"buy": 1, "sell": -1}[line_split[3]]
        if line_split[5]:
            return cls(
                api_name=line_split[0],
                order_type=line_split[1],
                symbol=line_split[2],
                amount=float(amount),
                price=float(line_split[5])
            )
        else:
            return cls(
                api_name=line_split[0],
                order_type=line_split[1],
                symbol=line_split[2],
                amount=float(amount),
            )

    def to_order_dict(self) -> dict:
        d = {
            "type": self.order_type,
            "symbol": self.symbol,
            "side": self.side,
            "amount": abs(self.amount),
            "price": self.price,
        }
        if not self.price:
            d.pop('price')
        return d

    def __str__(self):
        return str({
            "api_name": self.api_name,
            "type": self.order_type,
            "symbol": self.symbol,
            "amount": self.amount,
            "price": self.price
        })


class TransferOrder:
    def __init__(self, symbol, amount, from_account, to_account):
        self.symbol = symbol
        self.amount = float(amount)
        self.from_account = from_account
        self.to_account = to_account

    @classmethod
    def from_file_string(cls, line):
        line_split = line.split(',')
        if len(line_split) != 4:
            assert ValueError
        return cls(
            symbol=line_split[0],
            amount=float(line_split[1]),
            from_account=line_split[2],
            to_account=line_split[3],
        )

    def to_order_dict(self):
        return {
            "symbol": self.symbol,
            "amount": self.amount,
            "fromAccount": self.from_account,
            "toAccount": self.to_account,
        }

    def to_output_string(self):
        return ','.join([self.symbol, str(self.amount), self.from_account, self.to_account])

    def __str__(self):
        return str(self.to_order_dict())


class CcxtTools:
    def __init__(self, api_key=None, secret=None, trust_env=None, logger=logger_obj):
        if not api_key:
            self.API_CONFIG = read_api_config_file()
        else:
            self.API_CONFIG = {
                "apiKey": api_key,
                "secret": secret,
                "trust_env": trust_env
            }
        self.logger = logger

    @classmethod
    def transfer_exchange_symbol(cls, s):
        return s.replace("/", "_").replace(":", "__")

    @classmethod
    def output_json(cls, data, output_file):
        if not os.path.isdir(os.path.dirname(output_file)):
            os.makedirs(os.path.dirname(output_file))
        with open(output_file, 'w') as f:
            f.write(json.dumps(data, indent=4))

    # =================    ccxt 数据整理、转换

    @classmethod
    def fetch_balance_all_to_holding(cls, balance):
        """
        exchange.fetch_balance() ->
        {
            'info':  { ... },    // the original untouched non-parsed reply with details
            'timestamp': 1499280391811, // Unix Timestamp in milliseconds (seconds * 1000)
            'datetime': '2017-07-05T18:47:14.692Z', // ISO8601 datetime string with milliseconds

            //-------------------------------------------------------------------------
            // indexed by availability of funds first, then by currency

            'free':  {           // money, available for trading, by currency
                'BTC': 321.00,   // floats...
                'USD': 123.00,
                ...
            },
            'used':  { ... },    // money on hold, locked, frozen, or pending, by currency
            'total': { ... },    // total (free + used), by currency

            //-------------------------------------------------------------------------
            // indexed by currency first, then by availability of funds

            'BTC':   {           // string, three-letter currency code, uppercase
                'free': 321.00   // float, money available for trading
                'used': 234.00,  // float, money on hold, locked, frozen or pending
                'total': 555.00, // float, total balance (free + used)
            },
            'USD':   {           // ...
                'free': 123.00   // ...
                'used': 456.00,
                'total': 579.00,
            },
            ...
        }
        """

        d_holding_balance = {}
        for _k, _v in balance.items():
            if _k == "info":
                d_holding_balance["info"] = {
                    _kj: _vj
                    for _kj, _vj in _v.items()
                    if _kj not in ["assets", "positions"]
                }
            elif _k == "free":
                d_holding_balance["free"] = _v.copy()
            elif _k == "used":
                d_holding_balance["used"] = {
                    _kj: _vj
                    for _kj, _vj in _v.items()
                    if _vj > 0
                }
            elif _k == "total":
                d_holding_balance["total"] = {
                    _kj: _vj
                    for _kj, _vj in _v.items()
                    if _vj > 0
                }
            elif type(_v) is dict:
                if 'total' in _v:
                    if _v['total'] > 0:
                        d_holding_balance[_k] = _v
                else:
                    d_holding_balance[_k] = _v
            else:
                d_holding_balance[_k] = _v
        return d_holding_balance

    @classmethod
    def fetch_balance_all_to_simple(cls, balance) -> List[SimpleBalance]:
        """将 ccxt.fetch_balance() 返回数据转换为 List[SimpleBalance] """
        d_balance = dict()
        for _item_name, _v in balance.items():
            if _item_name not in ['free', 'used', 'total']:
                continue
            d_balance[_item_name] = {
                _symbol: _amount
                for _symbol, _amount in _v.items()
                if _amount != 0
            }

        d_balance_gb_symbol = dict()
        for _item_name in ['free', 'used', 'total']:
            for _symbol, _amount in d_balance[_item_name].items():
                if _symbol not in d_balance_gb_symbol.keys():
                    d_balance_gb_symbol[_symbol] = {
                        "free": 0,
                        "used": 0,
                        "total": 0
                    }
                d_balance_gb_symbol[_symbol][_item_name] += float(_amount)

        l_simple_balance = list()
        for _symbol, _data in d_balance_gb_symbol.items():
            _simple_balance = SimpleBalance(
                symbol=_symbol, total=_data['total'], used=_data['used'], free=_data['free'])
            l_simple_balance.append(_simple_balance)
        return l_simple_balance

    @classmethod
    def fetch_positions_all_to_holding(cls, data):
        """
        exchange.fetch_positions() ->
        eg:
        {
           'info': { ... },             // json response returned from the exchange as is
           'id': '1234323',             // string, position id to reference the position, similar to an order id
           'symbol': 'BTC/USD',         // uppercase string literal of a pair of currencies
           'timestamp': 1607723554607,  // integer unix time since 1st Jan 1970 in milliseconds
           'datetime': '2020-12-11T21:52:34.607Z',  // ISO8601 representation of the unix time above
           'isolated': true,            // boolean, whether or not the position is isolated, as opposed to cross where margin is added automatically
           'hedged': false,             // boolean, whether or not the position is hedged, i.e. if trading in the opposite direction will close this position or make a new one
           'side': 'long',              // string, long or short
           'contracts': 5,              // float, number of contracts bought, aka the amount or size of the position
           'contractSize': 100,         // float, the size of one contract in quote units
           'entryPrice': 20000,         // float, the average entry price of the position
           'markPrice': 20050,          // float, a price that is used for funding calculations
           'notional': 100000,          // float, the value of the position in the settlement currency
           'leverage': 100,             // float, the leverage of the position, related to how many contracts you can buy with a given amount of collateral
           'collateral': 5300,          // float, the maximum amount of collateral that can be lost, affected by pnl
           'initialMargin': 5000,       // float, the amount of collateral that is locked up in this position
           'maintenanceMargin': 1000,   // float, the mininum amount of collateral needed to avoid being liquidated
           'initialMarginPercentage': 0.05,      // float, the initialMargin as a percentage of the notional
           'maintenanceMarginPercentage': 0.01,  // float, the maintenanceMargin as a percentage of the notional
           'unrealizedPnl': 300,        // float, the difference between the market price and the entry price times the number of contracts, can be negative
           'liquidationPrice': 19850,   // float, the price at which collateral becomes less than maintenanceMargin
           'marginMode': 'cross',       // string, can be cross or isolated
           'percentage': 3.32,          // float, represents unrealizedPnl / initialMargin * 100
        }

        """
        holding_position = [_position for _position in data if _position['contracts'] != 0]
        return holding_position

    @classmethod
    def fetch_positions_all_to_simple(cls, data, api_name='') -> List[SimplePosition]:
        """将 ccxt.fetch_positions() 返回数据转换为 List[SimplePosition] """
        _positions_simple = []
        for _position in data:
            if _position.get('contracts') == 0:
                continue
            else:
                _amount = _position.get('contracts') * {"long": 1, "short": -1}[_position.get('side')]
                _positions_simple.append(
                    SimplePosition(api_name=api_name, symbol=_position.get('symbol'), amount=_amount,))
        return _positions_simple

    @classmethod
    def fetch_orders_to_simple(cls, data) -> List[SimpleOrder]:
        l_data = []
        for _order_structure in data:
            l_data.append(SimpleOrder.from_ccxt(_order_structure))
        return l_data

    @classmethod
    def fetch_opening_orders_to_simple(cls, data) -> List[SimpleOpeningOrder]:
        l_data = []
        for _order_structure in data:
            l_data.append(SimpleOpeningOrder.from_ccxt(_order_structure))
        return l_data

    @classmethod
    def fetch_my_trades_to_simple(cls, data) -> List[SimpleTrade]:
        l_data = []
        for _trade_structure in data:
            # print(json.dumps(_trade_structure, indent=4))
            l_data.append(SimpleTrade.from_ccxt(_trade_structure))
        return l_data

    # ===========================   获取账户 基础数据
    # [1] 获取账户资产
    def get_accounts_balance(self, output_file=None, log=True) -> Dict[str, List[SimpleBalance]]:
        """

        :param log:
        :param output_file:
        :return:
        d_all_balance_exchange_coin
        { exchange:
            { CoinSymbol:
                {"free": ,
                "used": ,
                "total": }
        }}
        """
        d_balance_gb_exchange_symbol: Dict[str, List[SimpleBalance]] = defaultdict(list)
        for _exchange_name, _api in EXCHANGE_API_MAPPING.items():
            # [1] 连接交易所api
            exchange = _api(self.API_CONFIG)
            # [2] 查询账号资产情况
            if log:
                self.logger.info(f'Connected api, {_exchange_name}, fetch balance')
            # 整理数据
            l_exchange_balance: List[SimpleBalance] = self.fetch_balance_all_to_simple(exchange.fetch_balance())
            d_balance_gb_exchange_symbol[_exchange_name] = l_exchange_balance
        # 输出
        if output_file:
            if not os.path.isdir(os.path.dirname(output_file)):
                os.makedirs(os.path.dirname(output_file))
            l_output = [
                _simple_balance_obj.to_output_string()
                for _exchange, _exchange_data in d_balance_gb_exchange_symbol.items()
                for _simple_balance_obj in _exchange_data
            ]
            with open(output_file, 'w') as f:
                f.writelines('\n'.join(l_output))
        return d_balance_gb_exchange_symbol

    # [2] 获取账户 合约持仓
    def get_accounts_contract_positions(self, output_file=None, log=True) -> Dict[str, List[SimplePosition]]:
        """
        :param log:
        :param output_file:
        :return:
        {exchange: [SimplePosition], }
        """
        d_all_positions_simple: Dict[str, List[SimplePosition]] = defaultdict(list)
        for _name, _api in EXCHANGE_API_MAPPING.items():
            if _name == 'Spot':
                continue
            # [1] 连接交易所api
            if log:
                logger_obj.info(f'Connecting api, {_name}')
            exchange = _api(self.API_CONFIG)

            # [2] 查询账号资产情况
            if log:
                logger_obj.info(f'Connected api, {_name}, fetch position')
            _simple_position: List[SimplePosition] = self.fetch_positions_all_to_simple(
                exchange.fetch_positions(), api_name=_name)
            d_all_positions_simple[_name] = _simple_position

        if output_file:
            if not os.path.isdir(os.path.dirname(output_file)):
                os.makedirs(os.path.dirname(output_file))
            with open(output_file, 'w') as f:
                f.writelines('\n'.join([
                    _position.to_output_string()
                    for _api_name, _positions in d_all_positions_simple.items()
                    for _position in _positions
                ]))
        return d_all_positions_simple

    # [3]
    # 查询 订单
    def get_orders(self, api_name, symbol, since_timestamp=None) -> List[SimpleOrder]:
        exchange = EXCHANGE_API_MAPPING[api_name](self.API_CONFIG)
        l_orders = exchange.fetch_orders(symbol, since=since_timestamp)
        _data: List[SimpleOrder] = CcxtTools.fetch_orders_to_simple(l_orders)
        return _data

    def get_orders_by_list(self, checking_list, since_timestamp=None,  output_file=None, log=True) -> List[SimpleOrder]:
        if log:
            self.logger.info('getting orders by list')
        d_checking_symbol = defaultdict(list)
        for _api_name, _symbol in checking_list:
            d_checking_symbol[_api_name].append(_symbol)

        #
        l_orders: List[SimpleOrder] = []
        for _api_name, _l_symbol in d_checking_symbol.items():
            exchange = EXCHANGE_API_MAPPING[_api_name](self.API_CONFIG)
            for _symbol in _l_symbol:
                if log:
                    self.logger.info(f'getting orders, {_api_name}, {_symbol}')
                _l_orders: List[SimpleOrder] = CcxtTools.fetch_orders_to_simple(
                    exchange.fetch_orders(_symbol, since=since_timestamp))
                l_orders += _l_orders

        if output_file:
            if not os.path.isdir(os.path.dirname(output_file)):
                os.makedirs(os.path.dirname(output_file))
            with open(output_file, 'w') as f:
                f.writelines('\n'.join([_.to_output_string() for _ in l_orders]))

        return l_orders

    # 查询 未成交 订单
    def get_opening_orders(self, api_name, symbol) -> List[SimpleOpeningOrder]:
        exchange = EXCHANGE_API_MAPPING[api_name](self.API_CONFIG)
        l_orders = exchange.fetch_open_orders(symbol)
        _data: List[SimpleOpeningOrder] = CcxtTools.fetch_opening_orders_to_simple(l_orders)
        return _data

    def get_opening_orders_by_list(self, checking_list, output_file=None, log=True) -> List[SimpleOpeningOrder]:
        if log:
            self.logger.info('getting opening orders by list')
        d_checking_symbol = defaultdict(list)
        for _api_name, _symbol in checking_list:
            d_checking_symbol[_api_name].append(_symbol)

        #
        l_orders: List[SimpleOpeningOrder] = []
        for _api_name, _l_symbol in d_checking_symbol.items():
            exchange = EXCHANGE_API_MAPPING[_api_name](self.API_CONFIG)
            for _symbol in _l_symbol:
                if log:
                    self.logger.info(f'getting opening orders, {_api_name}, {_symbol}')
                _l_orders: List[SimpleOpeningOrder] = CcxtTools.fetch_opening_orders_to_simple(exchange.fetch_open_orders(_symbol))
                l_orders += _l_orders

        if output_file:
            if not os.path.isdir(os.path.dirname(output_file)):
                os.makedirs(os.path.dirname(output_file))
            with open(output_file, 'w') as f:
                f.writelines('\n'.join([_.to_output_string() for _ in l_orders]))

        return l_orders

    # [4] 查询 成交
    def get_my_trades(self, api_name, symbol, since_timestamp=None) -> List[SimpleTrade]:
        exchange = EXCHANGE_API_MAPPING[api_name](self.API_CONFIG)
        l_orders = exchange.fetch_my_trades(symbol, since=since_timestamp)
        _data: List[SimpleTrade] = CcxtTools.fetch_my_trades_to_simple(l_orders)
        return _data

    def get_my_trades_by_list(self, checking_list, since_timestamp=None, output_file=None, log=True) -> List[SimpleTrade]:
        if log:
            self.logger.info('getting trades by list')
        d_checking_symbol = defaultdict(list)
        for _api_name, _symbol in checking_list:
            d_checking_symbol[_api_name].append(_symbol)

        #
        l_trades: List[SimpleTrade] = []
        for _api_name, _l_symbol in d_checking_symbol.items():
            exchange = EXCHANGE_API_MAPPING[_api_name](self.API_CONFIG)
            for _symbol in _l_symbol:
                if log:
                    self.logger.info(f'getting trades, {_api_name}, {_symbol}, {str(since_timestamp)}')
                _l_trades: List[SimpleTrade] = CcxtTools.fetch_my_trades_to_simple(
                    exchange.fetch_my_trades(_symbol, since_timestamp))
                l_trades += _l_trades

        if output_file:
            if not os.path.isdir(os.path.dirname(output_file)):
                os.makedirs(os.path.dirname(output_file))
            with open(output_file, 'w') as f:
                f.writelines('\n'.join([_.to_output_string() for _ in l_trades]))

        return l_trades

    # [4] 获取最新价格
    def get_newest_price(self, api_name, symbol, log=True) -> float:
        if log:
            logger_obj.info(f'Connected api, {api_name}, fetch ticker price')
        exchange = EXCHANGE_API_MAPPING[api_name](self.API_CONFIG)
        _tick_data = exchange.fetch_ticker(symbol)
        return _tick_data['close']

    # 获取最新价格 BTC ETH BNB
    def get_newest_price_usually(self, output_file=None, log=True) -> dict:
        if log:
            logger_obj.info(f'fetch usually ticker price')
        _data = {
            'BTC/USDT': self.get_newest_price('Spot', 'BTC/USDT', log),
            'ETH/USDT': self.get_newest_price('Spot', 'ETH/USDT', log),
            'BNB/USDT': self.get_newest_price('Spot', 'BNB/USDT', log),
        }

        if output_file:
            if not os.path.isdir(os.path.dirname(output_file)):
                os.makedirs(os.path.dirname(output_file))
            with open(output_file, 'w') as f:
                f.writelines('\n'.join([
                    f'{_k},{str(_v)}'
                    for _k, _v in _data.items()
                ]))
        return _data

    # 获取账户基本配置
    def get_basic_config(self, log=True):
        """
        Margin Mode：Cross
        Position Mode：One-way Mode
        Leverage：5X，可调
        :return:
        """

        # d_balance_gb_exchange_symbol: Dict[str, List[SimpleBalance]] = defaultdict(list)
        for _exchange_name, _api in EXCHANGE_API_MAPPING.items():
            # [1] 连接交易所api
            exchange = _api(self.API_CONFIG)
            # [2] 查询账号资产情况
            if log:
                self.logger.info(f'Connected api, {_exchange_name}, fetch account basic config')
            # 整理数据
            l_exchange_config = exchange.fetch_
            d_balance_gb_exchange_symbol[_exchange_name] = l_exchange_balance
        pass


    # ===================  计算
    # [1] 计算 净资产总值.   按 USD BTC ETH BNB，分别计价
    def cal_total_equity(self, d_exchange_balance, d_price, ticker_transfer_mapping=None, output_file=None, log=True):
        # 获取 资产
        # { symbol: amount, }
        d_balance = defaultdict(float)
        for _exchange, _data in d_exchange_balance.items():
            for _simple_balance in _data:
                d_balance[_simple_balance.symbol] += _simple_balance.total

        # 转换
        if not ticker_transfer_mapping:
            ticker_transfer_mapping = {}

        total_balance_in_usdt = 0
        for _symbol, _amount in d_balance.items():
            # 转换symbol，处理特殊情况
            if _symbol in ticker_transfer_mapping.keys():
                _symbol = ticker_transfer_mapping[_symbol]
            # 计算
            if _symbol == 'USDT':
                total_balance_in_usdt += _amount * 1
            elif str(_symbol) + '/USDT' in d_price.keys():
                total_balance_in_usdt += _amount * d_price[_symbol + '/USDT']
            elif (type(_symbol) is float) or (type(_symbol) is int):
                try:
                    total_balance_in_usdt += _amount * _symbol
                except:
                    self.logger.error(f'{_symbol}, {_amount}, {_symbol}')
                    raise Exception
            else:
                self.logger.error(f'error, unknown symbol, {_symbol}')
                raise Exception

        d_total_balance = {
            "USDT": total_balance_in_usdt,
            "BTC": total_balance_in_usdt / d_price['BTC/USDT'],
            "ETH": total_balance_in_usdt / d_price['ETH/USDT'],
            "BNB": total_balance_in_usdt / d_price['BNB/USDT'],
        }

        if output_file:
            if not os.path.isdir(os.path.dirname(output_file)):
                os.makedirs(os.path.dirname(output_file))
            with open(output_file, 'w') as f:
                f.writelines('\n'.join([
                    f'{_k},{str(_v)}'
                    for _k, _v in d_total_balance.items()
                ]))
        return d_total_balance


# 反向合约
class ReversContract:
    @classmethod
    def is_revers_contract(cls, symbol):
        if str(symbol)[0] == '-':
            return True
        else:
            return False

    @classmethod
    def gen_normal_contract(cls, symbol):
        if str(symbol)[0] == '-':
            return str(symbol)[1:]
        else:
            return symbol

    @classmethod
    def gen_normal_trading_amount(cls, original_symbol, original_amount, api_config):
        _new_spot_ticker = cls.gen_normal_contract(original_symbol)
        exchange = EXCHANGE_API_MAPPING['Spot'](api_config)
        _price = exchange.fetch_ticker(_new_spot_ticker)['close']
        _new_amount = -original_amount / _price
        return _new_amount


# 转换
class CoinContractSpotTickerMapping:
    """
    提供转换方法
    检查是否有相同项目
    """
    def __init__(self, path_mapping_file):
        self._mapping = []
        self._read_file(path_mapping_file)
        self._check()

    @property
    def mapping(self):
        return self._mapping

    def _read_file(self, path):
        with open(path, encoding='utf-8') as f:
            l_lines = f.readlines()
        assert len(l_lines) > 1
        for line in l_lines[1:]:
            line = line.strip()
            if line == '':
                continue
            line_split = line.split(',')
            self._mapping.append({
                "coin": line_split[0],
                "spot": line_split[1],
                "contract": line_split[2],
            })

    def _check(self) -> Dict[str, Counter]:
        # 检查是否有重复项
        d_count = {
            "coin": Counter([_['coin'] for _ in self._mapping]),
            "spot": Counter([_['spot'] for _ in self._mapping]),
            "contract": Counter([_['contract'] for _ in self._mapping])
        }
        _error = False
        for _name, _counter in d_count.items():
            for _symbol, _count in _counter.items():
                if _count == 1:
                    continue
                else:
                    logger_obj.error(f'Mapping文件存在重复项, {_name}, {_symbol}')
                    _error = True
        if _error:
            raise Exception
        return d_count

    def coin_to_contract(self, coin):
        l_item = [_ for _ in self.mapping if _['coin'] == coin]
        if len(l_item) == 0:
            logger_obj.warning(f'Mapping not find spot = "{coin}"')
            return None
        else:
            return l_item[0]['contract']

    def coin_to_spot(self, coin):
        l_item = [_ for _ in self.mapping if _['coin'] == coin]
        if len(l_item) == 0:
            logger_obj.warning(f'Mapping not find spot = "{coin}"')
            return None
        else:
            return l_item[0]['spot']

    def contract_to_coin(self, contract):
        l_item = [_ for _ in self.mapping if _['contract'] == contract]
        if len(l_item) == 0:
            logger_obj.warning(f'Mapping not find contract = "{contract}"')
            return None
        else:
            return l_item[0]['coin']

    def contract_to_spot(self, contract):
        l_item = [_ for _ in self.mapping if _['contract'] == contract]
        if len(l_item) == 0:
            logger_obj.warning(f'Mapping not find contract = "{contract}"')
            return None
        else:
            return l_item[0]['spot']

    def spot_to_coin(self, spot):
        l_item = [_ for _ in self.mapping if _['spot'] == spot]
        if len(l_item) == 0:
            logger_obj.warning(f'Mapping not find spot = "{spot}"')
            return None
        else:
            return l_item[0]['coin']

    def spot_to_contract(self, spot):
        l_item = [_ for _ in self.mapping if _['spot'] == spot]
        if len(l_item) == 0:
            logger_obj.warning(f'Mapping not find spot = "{spot}"')
            return None
        else:
            return l_item[0]['contract']


def read_coin_transfer_mapping_file() -> dict:
    PATH_COIN_MAPPING = os.path.join(PATH_ROOT, 'Config', 'CoinMapping.csv')
    if os.path.isfile(PATH_COIN_MAPPING):
        with open(PATH_COIN_MAPPING) as f:
            l_lines = f.readlines()
        ticker_transfer_mapping = {}
        for line in l_lines:
            line = line.strip()
            if line == '':
                continue
            _key = line.split(',')[0]
            _value = line.split(',')[1]
            if _value.isdigit():
                _value = float(_value)
            ticker_transfer_mapping[_key] = _value
    else:
        ticker_transfer_mapping = {}
    return ticker_transfer_mapping


# ==========================   计算

# [1] 计算账户净资产价值， 分 USD BTC ETH，分别计价


# ===========================================

