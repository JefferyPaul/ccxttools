
import os
import sys
from collections import defaultdict
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
_d = json.loads(open(PATH_CONFIG).read())
API_CONFIG = {
    "apiKey": _d["apiKey"],
    "secret": _d["secret"],
    'trust_env': True
}
# api
EXCHANGE_API_MAPPING = {
    "Spot": ccxt.binance,
    "USDM": ccxt.binanceusdm,
    "CoinM": ccxt.binancecoinm
}
# 运行等待时间
delay = 1
#
logger = MyLogger('ccxttolls', output_root=os.path.join(PATH_ROOT, 'logs'))


# ==================

@dataclass
class SimplePosition:
    api_name: str
    symbol: str
    amount: float

    def to_output_string(self):
        return ','.join([self.api_name, self.symbol, str(self.amount)])


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


# 成交中订单状态
@dataclass
class TradingOrder:
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
                self.price, self.amount, self.filled, self.average, self.timestamp
            ]
        ])

    def __str__(self):
        str(self.__dict__)


class CcxtTools:
    @classmethod
    def transfer_exchange_symbol(cls, s):
        return s.replace("/", "_").replace(":", "__")

    @classmethod
    def output_json(cls, data, output_file):
        if not os.path.isdir(os.path.dirname(output_file)):
            os.makedirs(os.path.dirname(output_file))
        with open(output_file, 'w') as f:
            f.write(json.dumps(data, indent=4))

    # [2] 查询账号资产情况
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

    # [4] 查询市场行情，订单
    # @classmethod
    # def fetch_order_book(cls, output_root=None, n=5):
    #     """
    #     exchange.fetch_order_book() ->
    #     eg:
    #         {
    #             "symbol": "BCH/USDT:USDT",
    #             "bids": [],
    #             "asks": [],
    #             "timestamp": 1675156546939,
    #             "datetime": "2023-01-31T09:15:46.939Z",
    #             "nonce": 2443309673427
    #         }
    #     """
    #     d_all_order_book = dict()
    #     for symbol in list(self.exchange.markets.keys())[:n]:
    #         _order_book: dict = self.exchange.fetch_order_book(symbol)
    #         sleep(delay)  # rate limit
    #         d_all_order_book[symbol] = _order_book
    #
    #     if output_root:
    #         if not os.path.isdir(output_root):
    #             os.makedirs(output_root)
    #         for _symbol, _data in d_all_order_book.items():
    #             _s_order_book = json.dumps(_data, indent=4)
    #             with open(os.path.join(output_root, f'{_transfer_exchange_symbol(_symbol)}.json'), 'w') as f:
    #                 f.write(_s_order_book)
    #     return d_all_order_book
    #
    #     # [5] 查询账号持仓

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
    def fetch_open_orders_to_simple(cls, data) -> List[TradingOrder]:
        l_data = []
        for _order_structure in data:
            l_data.append(TradingOrder(
                id=_order_structure['id'],
                status=_order_structure['status'],
                symbol=_order_structure['symbol'],
                type=_order_structure['type'],
                side=_order_structure['side'],
                price=_order_structure['price'],
                amount=_order_structure['amount'],
                filled=_order_structure['filled'],
                average=_order_structure['average'],
                timestamp=_order_structure['timestamp'],
            ))
        return l_data


# ===========================   获取账户 基础数据
# [1] 获取账户资产
def get_binance_accounts_balance(output_root=None, log_out=True) -> (dict, dict):
    """

    :param output_root:
    :return:
    d_all_balance_exchange_coin
    { exchange:
        { CoinSymbol:
            {"free": ,
            "used": ,
            "total": }
    }}

    d_all_balance_at_coin
    {
    CoinSymbol:
        {"free": ,
        "used": ,
        "total": }
    }
    """
    d_all_balance = {}
    for _name, _api in EXCHANGE_API_MAPPING.items():
        # [1] 连接交易所api
        exchange = _api(API_CONFIG)

        # [2] 查询账号资产情况
        if log_out:
            logger.info(f'Connected api, {_name}, fetch balance')
        _balance = exchange.fetch_balance()
        _balance_simple = {}
        for _k, _v in _balance.items():
            if _k not in ['free', 'used', 'total']:
                continue
            _balance_simple[_k] = {
                __k: __v
                for __k, __v in _v.items()
                if __v != 0
            }
        d_all_balance[_name] = _balance_simple

    #
    d_all_balance_exchange_coin = defaultdict(dict)
    for _exchange, _data in d_all_balance.items():
        for _k in ['free', 'used', 'total']:
            for _symbol, _amount in _data[_k].items():
                if _symbol not in d_all_balance_exchange_coin[_exchange].keys():
                    d_all_balance_exchange_coin[_exchange][_symbol] = {
                        "free": 0,
                        "used": 0,
                        "total": 0
                    }
                d_all_balance_exchange_coin[_exchange][_symbol][_k] += float(_amount)

    #
    d_all_balance_at_coin = defaultdict(lambda: defaultdict(float))
    for _exchange, _exchange_data in d_all_balance_exchange_coin.items():
        for _symbol, _data in _exchange_data.items():
            d_all_balance_at_coin[_symbol]['total'] += _data['total']
            d_all_balance_at_coin[_symbol]['used'] += _data['used']
            d_all_balance_at_coin[_symbol]['free'] += _data['free']

    if output_root:
        if not os.path.isdir(output_root):
            os.makedirs(output_root)
        with open(os.path.join(output_root, 'balance_all.json'), 'w') as f:
            f.write(json.dumps(d_all_balance, indent=4))
        # group by coin
        l_output = [
            ','.join([str(_) for _ in [_symbol, _data['total'], _data['used'], _data['free']]])
            for _symbol, _data in d_all_balance_at_coin.items()
        ]
        with open(os.path.join(output_root, 'balance_gb_coin.csv'), 'w') as f:
            f.writelines('\n'.join(l_output))
        # group by account, coin
        l_output = [
            ','.join([str(_) for _ in [_exchange, _symbol, _data['total'], _data['used'], _data['free']]])
            for _exchange, _exchange_data in d_all_balance_exchange_coin.items()
            for _symbol, _data in _exchange_data.items()
        ]
        with open(os.path.join(output_root, 'balance_gb_account_coin.csv'), 'w') as f:
            f.writelines('\n'.join(l_output))

    return d_all_balance_exchange_coin, d_all_balance_at_coin


# [2] 获取最新价格 BTC ETH
def get_newest_price(output_file=None) -> dict:
    logger.info(f'Connected api, Spot, fetch ticker price')
    exchange = EXCHANGE_API_MAPPING['Spot'](API_CONFIG)
    _btc_tick = exchange.fetch_ticker('BTC/USDT')
    _eth_tick = exchange.fetch_ticker('ETH/USDT')
    _bnb_tick = exchange.fetch_ticker('BNB/USDT')
    _data = {
        'BTC/USDT': _btc_tick['close'],
        'ETH/USDT': _eth_tick['close'],
        'BNB/USDT': _bnb_tick['close'],
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


# [3] 计算账户净资产价值， 分 USD BTC ETH，分别计价
def cal_total_balance(d_balance, d_price, output_file=None):
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

    total_balance_in_usdt = 0
    for _symbol, _data in d_balance.items():
        _amount = _data['total']
        # 转换symbol，处理特殊情况
        if _symbol in ticker_transfer_mapping.keys():
            _symbol = ticker_transfer_mapping[_symbol]
        # 计算
        if _symbol == 'USDT':
            total_balance_in_usdt += _amount * 1
        elif str(_symbol) + '/USDT' in d_price.keys():
            total_balance_in_usdt += _amount * d_price[_symbol + '/USDT']
        elif type(_symbol) is float or int:
            try:
                total_balance_in_usdt += _amount * _symbol
            except :
                print(_symbol, _amount, _symbol)
        else:
            print(f'error, unknown symbol, {_symbol}')

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


# [4] 获取账户 合约持仓
def get_all_accounts_contract_positions(output_root=None, log_out=True) -> Dict[str, List[SimplePosition]]:
    """

    :param output_root:
    :return:
    {exchange: [SimplePosition], }
    """
    d_all_positions_simple: Dict[str, List[SimplePosition]] = {}
    for _name, _api in EXCHANGE_API_MAPPING.items():
        if _name == 'Spot':
            continue
        # [1] 连接交易所api
        if log_out:
            logger.info(f'Connecting api, {_name}')
        # exchange = ccxt.binance(config)
        exchange = _api(API_CONFIG)

        # [2] 查询账号资产情况
        if log_out:
            logger.info(f'Connected api, {_name}, fetch position')
        _positions: List[dict] = exchange.fetch_positions()
        d_all_positions_simple[_name] = CcxtTools.fetch_positions_all_to_simple(_positions, api_name=_name)

    if output_root:
        if not os.path.isdir(output_root):
            os.makedirs(output_root)
        with open(os.path.join(output_root, 'contract_position_gb_account.csv'), 'w') as f:
            f.writelines('\n'.join([
                _position.to_output_string()
                for _api_name, _positions in d_all_positions_simple.items()
                for _position in _positions
            ]))
    return d_all_positions_simple


# 查询 未成交 订单
def get_open_orders(api_name):
    exchange = EXCHANGE_API_MAPPING[api_name](API_CONFIG)
    l = exchange.fetch_open_orders()
    _data: List[TradingOrder] = CcxtTools.fetch_open_orders_to_simple(l)


# ==========================   计算

# [5] 计算对冲
def cal_hedge_order(p_balance, p_contract, output_file=None):
    """
    balance:
        CoinM,BTC,0.50414233
    contract:
        USDM,BTC/BUSD:BUSD,-0.6

    """
    # CoinM资产
    with open(p_balance) as f:
        l_lines = f.readlines()
    d_coin_m_balance = {}
    for line in l_lines:
        line = line.strip()
        if not line:
            continue
        _account, _coin, _amount = line.split(',')
        if _account != 'CoinM':
            continue
        else:
            d_coin_m_balance[_coin] = float(_amount)

    # USDM对冲合约
    d_usd_m_hedge_contract = {}
    with open(p_contract) as f:
        l_lines = f.readlines()
    for line in l_lines:
        line = line.strip()
        if line == '':
            continue
        _account, _contract, _amount = line.split(',')
        if _account != 'USDM':
            continue
        else:
            d_usd_m_hedge_contract[_contract] = float(_amount)

    #
    d_hedge_contract_short_name = {
        _.split('/')[0]: _
        for _ in list(d_usd_m_hedge_contract.keys())
    }
    d_new_hedge = defaultdict(float)
    new_coin_no_hedge_contract = False
    for _holding_coin, _amount in d_coin_m_balance.items():
        if _holding_coin not in d_hedge_contract_short_name.keys():
            # 新的需要对冲的 币
            d_new_hedge[_holding_coin] -= _amount
            new_coin_no_hedge_contract = True
        else:
            d_new_hedge[d_hedge_contract_short_name[_holding_coin]] -= _amount
    for _hedging_contract, _amount in d_usd_m_hedge_contract.items():
        d_new_hedge[_hedging_contract] -= _amount

    if output_file:
        if not os.path.isdir(os.path.dirname(output_file)):
            os.makedirs(os.path.dirname(output_file))
        with open(output_file, 'w') as f:
            f.writelines('\n'.join([
                f'{_contract},{str(_amount)}'
                for _contract, _amount in d_new_hedge.items()
            ]))
        if new_coin_no_hedge_contract:
            run_warning_board('没有匹配的合约用于对冲，请手动修改')
            os.system('pause')
    return d_new_hedge


def cal_target_positions_to_order(api_name, targets: List[SimplePosition]) -> List[Order]:
    exchange = EXCHANGE_API_MAPPING[api_name](API_CONFIG)
    holding_positions: List[SimplePosition] = CcxtTools.fetch_positions_all_to_simple(exchange.fetch_positions(), api_name)

    d_orders: Dict[tuple, Order] = dict()
    for target_position in targets:
        d_orders[(target_position.api_name, target_position.symbol)] = Order(
            api_name=target_position.api_name,
            order_type='market',
            symbol=target_position.symbol,
            amount=target_position.amount,
        )
    for holding_position in holding_positions:
        if (holding_position.api_name, holding_position.symbol) in d_orders.keys():
            d_orders[(holding_position.api_name, holding_position.symbol)].amount -= holding_position.amount
        else:
            d_orders[(holding_position.api_name, holding_position.symbol)] = Order(
                api_name=holding_position.api_name,
                order_type='market',
                symbol=holding_position.symbol,
                amount=-holding_position.amount,
            )
    return list(d_orders.values())


# ===========================================

