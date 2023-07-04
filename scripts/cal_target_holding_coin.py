"""
以 usdm 账户中的合约为 基准，检查 coinm 账户的持仓量。

"""

import os
import sys
from pprint import pprint
from typing import Dict, List
from collections import defaultdict, Counter
from datetime import datetime
import json
import logging
import math

PATH_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(PATH_ROOT)

from ccxttools import (
    EXCHANGE_API_MAPPING, CcxtTools, SimplePosition,  SimpleBalance, SimpleOrder,
    Order, TransferOrder, ReversContract,
    logger_obj, CoinContractSpotTickerMapping
)
from helper.tp_WarningBoard import run_warning_board


def gen_order(orders: list, reverse=False):
    for _data in orders:
        _trading_spot_contract_symbol = _data['TradingSpotContractSymbol']
        _trading_spot_contract_amount = _data['TradingSpotContractAmount']
        _transfer_coin_symbol = _data['TransferCoinSymbol']
        _transfer_coin_amount = _data['TransferCoinAmount']

        # 交易单
        if _trading_spot_contract_amount != 0:
            if (_trading_spot_contract_amount > 0) == (not reverse):
                # Spot买入， (亏钱)
                _order = Order(
                    api_name='Spot',
                    order_type='market',
                    symbol=_trading_spot_contract_symbol,
                    amount=_trading_spot_contract_amount
                )
                l_output_string_buy.append(_order.to_output_string())
            else:
                # 卖出  (赚钱)
                _order = Order(
                    api_name='Spot',
                    order_type='market',
                    symbol=_trading_spot_contract_symbol,
                    amount=_trading_spot_contract_amount
                )
                l_output_string_sell.append(_order.to_output_string())
            logger_obj.info(f'生成交易订单, {str(_order)}')

        # 转账单
        if _transfer_coin_amount != 0:
            if _transfer_coin_amount > 0:
                # 转入CoinM
                _transfer_order = TransferOrder(
                    symbol=_transfer_coin_symbol,
                    amount=abs(_transfer_coin_amount),
                    from_account='Spot',
                    to_account='CoinM',
                )
                l_output_string_transfer_in.append(_transfer_order.to_output_string())
            else:
                # CoinM转出至Spot
                _transfer_order = TransferOrder(
                    symbol=_transfer_coin_symbol,
                    amount=abs(_transfer_coin_amount),
                    from_account='CoinM',
                    to_account='Spot',
                )
                l_output_string_transfer_out.append(_transfer_order.to_output_string())
            logger_obj.info(f'生成转账订单, {str(_transfer_order)}')


class AccountsHolding:
    def __init__(self, _logger: logging.Logger):
        self._spot_balance = {}
        self._coinm_balance = {}
        self._usdm_contract = {}
        
        self.fetch_data()

        if _logger:
            self.logger = _logger
        else:
            self.logger = logging.Logger(name='AccountsHolding')

    @property
    def SpotBalance(self):
        return self._spot_balance

    @property
    def CoinMBalance(self):
        return self._coinm_balance

    @property
    def USDMContract(self):
        return self._usdm_contract

    def fetch_data(self):
        _d_all_balance_exchange_coin: Dict[str, List[SimpleBalance]] = ccxt_obj.get_accounts_balance()
        _d_all_positions: Dict[str, List[SimplePosition]] = ccxt_obj.get_accounts_contract_positions()
        # spot 数币持仓. {symbol: amount}
        self._spot_balance = {
            _balance.symbol: float(_balance.total)
            for _balance in _d_all_balance_exchange_coin.get('Spot')
        }
        # coinM 保证金数币持仓. {symbol: amount}
        self._coinm_balance = {
            _balance.symbol: float(_balance.total)
            for _balance in _d_all_balance_exchange_coin.get('CoinM')
        }
        # USDM 对冲合约持仓. {symbol: amount}
        self._usdm_contract = {
            position.symbol: position.amount
            for position in _d_all_positions['USDM']
        }
    
    def check_with_mapping(self, mapping_obj: CoinContractSpotTickerMapping):
        _error = False        
        mapping_coin = [_["coin"] for _ in mapping_obj.mapping]
        for _coin in self.CoinMBalance.keys():
            if _coin not in mapping_coin:
                self.logger.error(f'请检查mapping表,缺少coin,{_coin}')
                _error = True
                
        mapping_contract = [_["contract"] for _ in mapping_obj.mapping]
        for _contract in self.USDMContract.keys():
            if _contract not in mapping_contract:
                self.logger.error(f'请检查mapping表,缺少contract,{_contract}')
                _error = True
        if _error:
            run_warning_board('请检查mapping表')
            raise Exception


def print_dict_log(name, dict_data, output_file=None):
    # 输出 记录
    _l_output_string = []
    for _k, _v in dict_data.items():
        _ = f'{name},{str(_k)},{str(_v)}'
        _l_output_string.append(_)
        logger_obj.info(_)

    if output_file:
        if _l_output_string:
            with open(output_file, 'a+') as f:
                f.writelines('\n'.join(_l_output_string))
    

if __name__ == '__main__':
    # [0] 读取 config,  BaseCurrency,需要剔除
    AMOUNT_ROUNDING_SIZE = 4
    TRANSFER_RATIO = 0.99
    PATH_CONFIG = os.path.join(PATH_ROOT, '../Config', 'Config.json')
    d_config = json.loads(open(PATH_CONFIG, encoding='utf-8').read())
    BASE_CURRENCY = d_config['BaseCurrency']

    ccxt_obj = CcxtTools()

    # 读取mapping
    # [{ "coin": "", "spot": "", "contract": ""}]
    path_contracts_mapping = os.path.join(PATH_ROOT, '../Config', 'CoinContractSpotMapping.csv')
    coin_contract_spot_mapping = CoinContractSpotTickerMapping(path_contracts_mapping)
    # BASE_CURRENCY_USDM_CONTRACT,需要剔除
    if BASE_CURRENCY in [_['coin'] for _ in coin_contract_spot_mapping.mapping]:
        BASE_CURRENCY_USDM_CONTRACT = coin_contract_spot_mapping.coin_to_contract(BASE_CURRENCY)
    else:
        BASE_CURRENCY_USDM_CONTRACT = ''

    PATH_OUTPUT_ROOT = os.path.join(PATH_ROOT, 'Output', 'CalTargetHoldingCoin')
    if not os.path.isdir(PATH_OUTPUT_ROOT):
        os.makedirs(PATH_OUTPUT_ROOT)
    PATH_CAL_LOG_FILE = os.path.join(PATH_OUTPUT_ROOT, 'calculate_log_%s.csv' % datetime.now().strftime('%Y%m%d_%H%M%S'))

    # [1] 获取最新 持仓和账户情况
    accounts_holding_obj = AccountsHolding(logger_obj)
    print('\n全部持仓：')
    print_dict_log("Spot持仓", accounts_holding_obj.SpotBalance, PATH_CAL_LOG_FILE,) 
    print_dict_log("CoinM持仓", accounts_holding_obj.CoinMBalance, PATH_CAL_LOG_FILE,) 
    print_dict_log("USDM对冲合约持仓", accounts_holding_obj.USDMContract, PATH_CAL_LOG_FILE,) 
    
    # 检查 mapping表是否有缺失
    accounts_holding_obj.check_with_mapping(coin_contract_spot_mapping)

    # [2] 计算 需要买卖多少币
    # 剔除 BaseCurrency
    d_target_usdm_contract = {
        _contract: _v
        for _contract, _v in accounts_holding_obj.USDMContract.items()
        if _contract != BASE_CURRENCY_USDM_CONTRACT
    }
    d_target_coinm_balance = {
        _coin: _v
        for _coin, _v in accounts_holding_obj.CoinMBalance.items()
        if _coin != BASE_CURRENCY
    }
    # 用于对冲的合约，转换为，目标持仓  【目标是对准 对冲合约持仓】
    hedging_contract_coin = [
        coin_contract_spot_mapping.contract_to_coin(_contract) for _contract in d_target_usdm_contract.keys()]
    d_target_spot_balance = {
        _k: _v
        for _k, _v in accounts_holding_obj.SpotBalance.items()
        if _k in hedging_contract_coin
    }
    print('\n进行对冲的币和合约（剔除本位币）：')
    print_dict_log("USDM对冲合约（目标）", d_target_usdm_contract, PATH_CAL_LOG_FILE, )
    print_dict_log("Spot持仓", d_target_spot_balance, PATH_CAL_LOG_FILE, )
    print_dict_log("CoinM持仓", d_target_coinm_balance, PATH_CAL_LOG_FILE, )

    # 计算 交易订单 和 转账订单
    d_trading_order = defaultdict(float)
    d_transfer_order = defaultdict(float)
    for _contract, _amount in d_target_usdm_contract.items():
        _spot_ticker = coin_contract_spot_mapping.contract_to_spot(_contract)
        d_trading_order[_spot_ticker] -= _amount
        d_transfer_order[_spot_ticker] -= _amount
    for _coin, _amount in d_target_coinm_balance.items():
        _spot_ticker = coin_contract_spot_mapping.coin_to_spot(_coin)
        d_trading_order[_spot_ticker] -= _amount
        d_transfer_order[_spot_ticker] -= _amount
    # 交易订单需要多 考虑 spot中已有的持仓
    for _coin, _amount in d_target_spot_balance.items():
        _spot_ticker = coin_contract_spot_mapping.coin_to_spot(_coin)
        d_trading_order[_spot_ticker] -= _amount

    # amount rounding
    # d_trading_order = {
    #     _k: round(_v, AMOUNT_ROUNDING_SIZE)
    #     for _k, _v in d_trading_order.items()
    # }
    # # 转账数量 * 0.999，保留（考虑手续费）
    # d_transfer_order = {
    #     _k: round(_v * TRANSFER_RATIO, AMOUNT_ROUNDING_SIZE)
    #     for _k, _v in d_transfer_order.items()
    # }
            
    # 下单占比
    d_trading_order_percentage = defaultdict(float)
    for _spot_ticker, _amount in d_trading_order.items():
        _usdm_contract = coin_contract_spot_mapping.spot_to_contract(_spot_ticker)
        d_trading_order_percentage[_spot_ticker] = -_amount / d_target_usdm_contract[_usdm_contract]

    print('\n需要下单：')
    print_dict_log("Spot下单", d_trading_order, PATH_CAL_LOG_FILE, )
    print_dict_log("Spot转账", d_transfer_order, PATH_CAL_LOG_FILE, )
    print('\n：')
    print_dict_log(
        "Spot下单占比", d_trading_order_percentage, os.path.join(PATH_OUTPUT_ROOT, '_order_amount_percentage.csv'))

    # [3] 生成order 单
    # order 格式转换
    # 反向合约处理
    l_order = []
    l_reverse_order = []
    for _spot_ticker, _trading_amount in d_trading_order.items():
        _transfer_amount = d_transfer_order[_spot_ticker]
        if _transfer_amount > 0:
            # spot 转至 conim。 少转进。
            _transfer_amount = math.floor(
                _transfer_amount * TRANSFER_RATIO * (10 ** AMOUNT_ROUNDING_SIZE)) / (10 ** AMOUNT_ROUNDING_SIZE)
        else:
            # 多转出
            _transfer_amount = math.ceil(
                _transfer_amount / TRANSFER_RATIO * (10 ** AMOUNT_ROUNDING_SIZE)) / (10 ** AMOUNT_ROUNDING_SIZE)

        if not ReversContract.is_revers_contract(_spot_ticker):
            if _trading_amount > 0:
                _trading_amount = math.ceil(_trading_amount * (10 ** AMOUNT_ROUNDING_SIZE)) / (10 ** AMOUNT_ROUNDING_SIZE)
            else:
                _trading_amount = math.floor(_trading_amount * (10 ** AMOUNT_ROUNDING_SIZE)) / (10 ** AMOUNT_ROUNDING_SIZE)
            l_order.append({
                "TradingSpotContractSymbol": _spot_ticker,
                "TradingSpotContractAmount": _trading_amount,
                "TransferCoinSymbol": coin_contract_spot_mapping.spot_to_coin(_spot_ticker),
                "TransferCoinAmount": _transfer_amount,
            })
        else:
            # 反向合约处理
            exchange = EXCHANGE_API_MAPPING['Spot'](ccxt_obj.API_CONFIG)
            _new_spot_ticker = ReversContract.gen_normal_contract(symbol=_spot_ticker)
            _price = exchange.fetch_ticker(_new_spot_ticker)['close']
            _new_amount = ReversContract.gen_normal_trading_amount(_spot_ticker, _trading_amount, ccxt_obj.API_CONFIG)
            if _new_amount < 0:
                _new_amount = math.ceil(_new_amount * (10 ** AMOUNT_ROUNDING_SIZE)) / (10 ** AMOUNT_ROUNDING_SIZE)
            else:
                _new_amount = math.floor(_new_amount * (10 ** AMOUNT_ROUNDING_SIZE)) / (10 ** AMOUNT_ROUNDING_SIZE)
            l_reverse_order.append({
                "TradingSpotContractSymbol": _new_spot_ticker,
                "TradingSpotContractAmount": _new_amount,
                "TransferCoinSymbol": coin_contract_spot_mapping.spot_to_coin(_spot_ticker),
                "TransferCoinAmount": _transfer_amount,
            })
            print("\n")
            logger_obj.info(
                f'反向合约,{_spot_ticker},{_trading_amount},'
                f'新合约{_new_spot_ticker},最新价格{str(_price)},下单数量{str(_new_amount)}')

    # 生成order 单
    l_output_string_buy = []
    l_output_string_transfer_in = []
    l_output_string_sell = []
    l_output_string_transfer_out = []
    gen_order(l_order)
    if l_reverse_order:
        gen_order(l_reverse_order, reverse=True)

    with open(os.path.join(PATH_OUTPUT_ROOT, '_order.csv'), 'w') as f:
        f.writelines('\n'.join(l_output_string_buy+l_output_string_sell))
    #
    with open(os.path.join(PATH_OUTPUT_ROOT, '_order_buy.csv'), 'w') as f:
        f.writelines('\n'.join(l_output_string_buy))
    with open(os.path.join(PATH_OUTPUT_ROOT, '_transfer_in.csv'), 'w') as f:
        f.writelines('\n'.join(l_output_string_transfer_in))
    #
    with open(os.path.join(PATH_OUTPUT_ROOT, '_order_sell.csv'), 'w') as f:
        f.writelines('\n'.join(l_output_string_sell))
    with open(os.path.join(PATH_OUTPUT_ROOT, '_transfer_out.csv'), 'w') as f:
        f.writelines('\n'.join(l_output_string_transfer_out))
