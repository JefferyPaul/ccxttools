"""
以 usdm 账户中的合约为 基准，检查 coinm 账户的持仓量。

"""

import os
import sys
from pprint import pprint
from typing import Dict, List
from collections import defaultdict
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


def read_mapping() -> List[dict]:
    l_mapping = []
    with open(PATH_CONTRACTS_MAPPING, encoding='utf-8') as f:
        l_lines = f.readlines()
    assert len(l_lines) > 1
    for line in l_lines[1:]:
        line = line.strip()
        if line == '':
            continue
        line_split = line.split(',')
        l_mapping.append({
            "coin": line_split[0],
            "spot": line_split[1],
            "contract": line_split[2],
        })
    return l_mapping


if __name__ == '__main__':
    # [0] 读取mapping
    # [{ "coin": "", "spot": "", "contract": ""}]
    PATH_CONTRACTS_MAPPING = os.path.join(PATH_ROOT, 'Config', 'CoinContractSpotMapping.csv')
    L_COIN_CONTRACT_SPOT_MAPPING: List[dict] = read_mapping()
    PATH_CONFIG = os.path.join(PATH_ROOT, 'Config', 'Config.json')
    d_config = json.loads(open(PATH_CONFIG, encoding='utf-8').read())
    BASE_CURRENCY = d_config['BaseCurrency']
    if BASE_CURRENCY in [_['coin'] for _ in L_COIN_CONTRACT_SPOT_MAPPING]:
        BASE_CURRENCY_USDM_CONTRACT = [_ for _ in L_COIN_CONTRACT_SPOT_MAPPING if _['coin'] == BASE_CURRENCY][0]['contract']
    else:
        BASE_CURRENCY_USDM_CONTRACT = ''

    PATH_OUTPUT_ROOT = os.path.join(PATH_ROOT, 'Output', 'CalTargetHoldingCoin')
    if not os.path.isdir(PATH_OUTPUT_ROOT):
        os.makedirs(PATH_OUTPUT_ROOT)

    # [1] 获取最新 持仓和账户情况
    # 持仓
    # [[_name, _symbol, _amount], ]
    d_all_balance_exchange_coin, d_all_balance_coin = get_binance_accounts_balance()
    d_coinm_balance = {
        _symbol: float(_symbol_data['total'])
        for _symbol, _symbol_data in d_all_balance_exchange_coin['CoinM'].items()
    }
    # 合约持仓
    # Dict[str, List[SimplePosition]]
    d_all_positions = get_all_accounts_contract_positions()
    d_usdm_positions = {
        position.symbol: position.amount
        for position in d_all_positions['USDM']
    }

    # 检查 是否mapping表有缺失
    _error = False
    mapping_coin = [_["coin"] for _ in L_COIN_CONTRACT_SPOT_MAPPING]
    for _coin in d_coinm_balance.keys():
        if _coin not in mapping_coin:
            logger.error(f'请检查mapping表,缺少coin,{_coin}')
            _error = True
    mapping_contract = [_["contract"] for _ in L_COIN_CONTRACT_SPOT_MAPPING]
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

    # [2] 计算 需要买卖多少币
    # 计算 挂单
    d_order = defaultdict(float)
    for _contract, _amount in d_usdm_positions.items():
        _spot_ticker = [_ for _ in L_COIN_CONTRACT_SPOT_MAPPING if _['contract'] == _contract][0]['spot']
        d_order[_spot_ticker] -= _amount
    for _coin, _amount in d_coinm_balance.items():
        _spot_ticker = [_ for _ in L_COIN_CONTRACT_SPOT_MAPPING if _['coin'] == _coin][0]['spot']
        d_order[_spot_ticker] -= _amount
    # 下单占比
    d_order_percentage = defaultdict(float)
    for _spot_ticker, _amount in d_order.items():
        _usdm_contract = [_ for _ in L_COIN_CONTRACT_SPOT_MAPPING if _['spot'] == _spot_ticker][0]['contract']
        d_order_percentage[_spot_ticker] = -_amount / d_usdm_positions[_usdm_contract]

    # [3] 输出
    # 输出 log
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
    print('\n')
    for _symbol, _amount in d_order.items():
        _ = f'Spot下单,{_symbol},{str(_amount)}'
        l_output_string.append(_)
        logger.info(_)
    with open(os.path.join(PATH_OUTPUT_ROOT, 'calculate_log_%s.csv' % datetime.now().strftime('%Y%m%d_%H%M%S')), 'w') as f:
        f.writelines('\n'.join(l_output_string))

    # 生成order 单
    l_output_string_buy = []
    l_output_string_transfer_in = []
    l_output_string_sell = []
    l_output_string_transfer_out = []
    for _symbol, _amount in d_order.items():
        _order = Order(
            api_name='Spot',
            order_type='market',
            symbol=_symbol,
            amount=_amount
        )
        if _amount >= 0:
            # Spot买入，转入CoinM  (亏钱)
            l_output_string_buy.append(_order.to_output_string())
            _transfer_order = TransferOrder(
                symbol=[_ for _ in L_COIN_CONTRACT_SPOT_MAPPING if _['spot'] == _symbol][0]['coin'],
                amount=abs(_amount),
                from_account='Spot',
                to_account='CoinM',
            )
            l_output_string_transfer_in.append(_transfer_order.to_output_string())
        else:
            # CoinM转出至Spot，卖出  (赚钱)
            l_output_string_sell.append(_order.to_output_string())
            _transfer_order = TransferOrder(
                symbol=[_ for _ in L_COIN_CONTRACT_SPOT_MAPPING if _['spot'] == _symbol][0]['coin'],
                amount=abs(_amount),
                from_account='CoinM',
                to_account='Spot',
            )
            l_output_string_transfer_out.append(_transfer_order.to_output_string())

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

    # 下单量占比
    l_output_string = []
    for _symbol, _amount in d_order_percentage.items():
        l_output_string.append(f'{_symbol},{str(_amount)}')
    with open(os.path.join(PATH_OUTPUT_ROOT, '_order_amount_percentage.csv'), 'w') as f:
        f.writelines('\n'.join(l_output_string))
