import os
import sys
from pprint import pprint
import json

PATH_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.append(PATH_ROOT)

from ccxttools import (
    get_binance_accounts_balance, get_newest_price, cal_total_balance,
    get_all_accounts_contract_positions, cal_hedge_order, logger
)


if __name__ == '__main__':
    # [1] 净资产值, 获取净资产-多币种持仓
    _, d_all_balance = get_binance_accounts_balance(os.path.join(PATH_ROOT, 'Output', 'AccountData'))
    # [2] 获取 BTC ETH 价格
    newest_price = get_newest_price(os.path.join(PATH_ROOT, 'Output', 'AccountData', 'ticker_newest_price.csv'))
    # [3] 计算净资产-统一币种  USDT BTC ETH
    cal_total_balance(
        d_all_balance, newest_price, os.path.join(PATH_ROOT, 'Output', 'AccountData', 'balance_value.csv'))
    # [4] 持仓
    get_all_accounts_contract_positions(os.path.join(PATH_ROOT, 'Output', 'AccountData'))
    # pprint(get_all_accounts_contract_positions(), indent=4)
