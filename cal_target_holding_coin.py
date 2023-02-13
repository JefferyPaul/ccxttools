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

PATH_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.append(PATH_ROOT)

from ccxttools import (
    EXCHANGE_API_MAPPING, API_CONFIG, CcxtTools, SimplePosition,
    Order, TransferOrder,
    get_binance_accounts_balance, get_all_accounts_contract_positions,
    logger,
)
from helper.tp_WarningBoard import run_warning_board


def gen_order(orders: list, reverse=False):
    for _data in orders:
        _spot_symbol = _data['SpotSymbol']
        _spot_amount = _data['SpotAmount']
        _coinm_symbol = _data['CoinMSymbol']
        _coinm_amount = _data['CoinMAmount']
        _order = Order(
            api_name='Spot',
            order_type='market',
            symbol=_spot_symbol,
            amount=_spot_amount
        )
        logger.info(f'生成订单, {_spot_symbol}, {str(_spot_amount)}, {_coinm_symbol}, {str(_coinm_amount)}')
        if (_spot_amount > 0) == (not reverse):
            # (_amount > 0) and (not reverse) ;(_amount < 0) and (reverse)
            # Spot买入，转入CoinM  (亏钱)
            l_output_string_buy.append(_order.to_output_string())
            _transfer_order = TransferOrder(
                symbol=_coinm_symbol,
                amount=abs(_coinm_amount),
                from_account='Spot',
                to_account='CoinM',
            )
            l_output_string_transfer_in.append(_transfer_order.to_output_string())
        else:
            # (_amount < 0) and (not reverse) ;(_amount > 0) and (reverse)
            # CoinM转出至Spot，卖出  (赚钱)
            l_output_string_sell.append(_order.to_output_string())
            _transfer_order = TransferOrder(
                symbol=_coinm_symbol,
                amount=abs(_coinm_amount),
                from_account='CoinM',
                to_account='Spot',
            )
            l_output_string_transfer_out.append(_transfer_order.to_output_string())


class CoinContractSpotTickerMapping:
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
                    logger.error(f'Mapping文件存在重复项, {_name}, {_symbol}')
                    _error = True
        if _error:
            raise Exception
        return d_count

    def coin_to_contract(self, coin):
        l_item = [_ for _ in self.mapping if _['coin'] == coin]
        if len(l_item) == 0:
            logger.warning(f'Mapping not find spot = "{coin}"')
            return None
        else:
            return l_item[0]['contract']

    def coin_to_spot(self, coin):
        l_item = [_ for _ in self.mapping if _['coin'] == coin]
        if len(l_item) == 0:
            logger.warning(f'Mapping not find spot = "{coin}"')
            return None
        else:
            return l_item[0]['spot']

    def contract_to_coin(self, contract):
        l_item = [_ for _ in self.mapping if _['contract'] == contract]
        if len(l_item) == 0:
            logger.warning(f'Mapping not find contract = "{contract}"')
            return None
        else:
            return l_item[0]['coin']

    def contract_to_spot(self, contract):
        l_item = [_ for _ in self.mapping if _['contract'] == contract]
        if len(l_item) == 0:
            logger.warning(f'Mapping not find contract = "{contract}"')
            return None
        else:
            return l_item[0]['spot']

    def spot_to_coin(self, spot):
        l_item = [_ for _ in self.mapping if _['spot'] == spot]
        if len(l_item) == 0:
            logger.warning(f'Mapping not find spot = "{spot}"')
            return None
        else:
            return l_item[0]['coin']

    def spot_to_contract(self, spot):
        l_item = [_ for _ in self.mapping if _['spot'] == spot]
        if len(l_item) == 0:
            logger.warning(f'Mapping not find spot = "{spot}"')
            return None
        else:
            return l_item[0]['contract']


if __name__ == '__main__':
    # [0] 读取 config,  BaseCurrency,需要剔除
    PATH_CONFIG = os.path.join(PATH_ROOT, 'Config', 'Config.json')
    d_config = json.loads(open(PATH_CONFIG, encoding='utf-8').read())
    BASE_CURRENCY = d_config['BaseCurrency']
    # 读取mapping
    # [{ "coin": "", "spot": "", "contract": ""}]
    path_contracts_mapping = os.path.join(PATH_ROOT, 'Config', 'CoinContractSpotMapping.csv')
    coin_contract_spot_mapping = CoinContractSpotTickerMapping(path_contracts_mapping)
    # BASE_CURRENCY_USDM_CONTRACT,需要剔除
    if BASE_CURRENCY in [_['coin'] for _ in coin_contract_spot_mapping.mapping]:
        BASE_CURRENCY_USDM_CONTRACT = coin_contract_spot_mapping.coin_to_contract(BASE_CURRENCY)
    else:
        BASE_CURRENCY_USDM_CONTRACT = ''

    PATH_OUTPUT_ROOT = os.path.join(PATH_ROOT, 'Output', 'CalTargetHoldingCoin')
    if not os.path.isdir(PATH_OUTPUT_ROOT):
        os.makedirs(PATH_OUTPUT_ROOT)

    # [1] 获取最新 持仓和账户情况
    # 持仓
    # [[_name, _symbol, _amount], ]
    d_all_balance_exchange_coin, d_all_balance_coin = get_binance_accounts_balance()
    # {symbol: amount}
    d_coinm_balance = {
        _symbol: float(_symbol_data['total'])
        for _symbol, _symbol_data in d_all_balance_exchange_coin['CoinM'].items()
    }
    # 合约持仓
    d_all_positions = get_all_accounts_contract_positions()
    # {symbol: amount}
    d_usdm_positions = {
        position.symbol: position.amount
        for position in d_all_positions['USDM']
    }

    # 输出 记录
    path_cal_log = os.path.join(PATH_OUTPUT_ROOT, 'calculate_log_%s.csv' % datetime.now().strftime('%Y%m%d_%H%M%S'))
    l_output_string = []
    print('\n')
    for _symbol, _amount in d_coinm_balance.items():
        _ = f'CoinM保证金持仓,{_symbol},{str(_amount)}'
        l_output_string.append(_)
        logger.info(_)
    for _symbol, _amount in d_usdm_positions.items():
        _ = f'USDM对冲合约持仓,{_symbol},{str(_amount)}'
        l_output_string.append(_)
        logger.info(_)
    with open(path_cal_log, 'a+') as f:
        f.writelines('\n'.join(l_output_string))

    # 检查 mapping表是否有缺失
    _error = False
    mapping_coin = [_["coin"] for _ in coin_contract_spot_mapping.mapping]
    for _coin in d_coinm_balance.keys():
        if _coin not in mapping_coin:
            logger.error(f'请检查mapping表,缺少coin,{_coin}')
            _error = True
    mapping_contract = [_["contract"] for _ in coin_contract_spot_mapping.mapping]
    for _contract in d_usdm_positions.keys():
        if _contract not in mapping_contract:
            logger.error(f'请检查mapping表,缺少contract,{_contract}')
            _error = True
    if _error:
        run_warning_board('请检查mapping表')

    # 剔除 BaseCurrency
    d_coinm_balance = {
        _coin: _v
        for _coin, _v in d_coinm_balance.items()
        if _coin != BASE_CURRENCY
    }
    d_usdm_positions = {
        _contract: _v
        for _contract, _v in d_usdm_positions.items()
        if _contract != BASE_CURRENCY_USDM_CONTRACT
    }

    # 输出 记录
    l_output_string = []
    print('\n')
    for _symbol, _amount in d_coinm_balance.items():
        _ = f'剔除本位币后CoinM保证金持仓,{_symbol},{str(_amount)}'
        l_output_string.append(_)
        logger.info(_)
    for _symbol, _amount in d_usdm_positions.items():
        _ = f'剔除本位币后USDM对冲合约持仓,{_symbol},{str(_amount)}'
        l_output_string.append(_)
        logger.info(_)
    with open(path_cal_log, 'a+') as f:
        f.writelines('\n'.join(l_output_string))

    # [2] 计算 需要买卖多少币
    # 计算 挂单
    d_order = defaultdict(float)
    for _contract, _amount in d_usdm_positions.items():
        _spot_ticker = coin_contract_spot_mapping.contract_to_spot(_contract)
        d_order[_spot_ticker] -= round(_amount, 4)
    for _coin, _amount in d_coinm_balance.items():
        _spot_ticker = coin_contract_spot_mapping.coin_to_spot(_coin)
        d_order[_spot_ticker] -= round(_amount, 4)
    # 下单占比
    d_order_percentage = defaultdict(float)
    for _spot_ticker, _amount in d_order.items():
        _usdm_contract = coin_contract_spot_mapping.spot_to_contract(_spot_ticker)
        d_order_percentage[_spot_ticker] = -_amount / d_usdm_positions[_usdm_contract]

    # [3] 输出
    # 输出 log
    print('\n')
    for _symbol, _amount in d_order.items():
        _ = f'Spot下单,{_symbol},{str(_amount)}'
        l_output_string.append(_)
        logger.info(_)
    with open(path_cal_log, 'w') as f:
        f.writelines('\n'.join(l_output_string))
    # 下单量占比
    l_output_string = []
    for _symbol, _amount in d_order_percentage.items():
        l_output_string.append(f'{_symbol},{str(_amount)}')
    with open(os.path.join(PATH_OUTPUT_ROOT, '_order_amount_percentage.csv'), 'w') as f:
        f.writelines('\n'.join(l_output_string))

    # [4] 生成order 单
    # order 格式转换
    # 反向合约处理
    l_order = []
    l_reverse_order = []
    for _spot_ticker, _amount in d_order.items():
        if _amount == 0:
            continue
        if _spot_ticker[0] != '-':
            l_order.append({
                "SpotSymbol": _spot_ticker,
                "SpotAmount": _amount,
                "CoinMSymbol": coin_contract_spot_mapping.spot_to_coin(_spot_ticker),
                "CoinMAmount": _amount,
            })
        else:
            # 反向合约处理
            print("\n")
            exchange = EXCHANGE_API_MAPPING['Spot'](API_CONFIG)
            _new_spot_ticker = _spot_ticker[1:]
            _price = exchange.fetch_ticker(_new_spot_ticker)['close']
            _new_amount = round(-_amount / _price, 4)
            l_reverse_order.append({
                "SpotSymbol": _new_spot_ticker,
                "SpotAmount": _new_amount,
                "CoinMSymbol": coin_contract_spot_mapping.spot_to_coin(_spot_ticker),
                "CoinMAmount": _amount,
            })
            logger.info(f'反向合约,{_spot_ticker},{_amount},新合约{_new_spot_ticker},最新价格{str(_price)},下单数量{str(_new_amount)}')

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


