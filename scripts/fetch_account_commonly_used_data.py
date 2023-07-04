import os
import sys
from pprint import pprint
import json
from datetime import datetime, timedelta
from typing import Dict, List

PATH_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(PATH_ROOT)

from ccxttools import (
    CcxtTools, logger_obj, SimplePosition, SimpleBalance, read_coin_transfer_mapping_file,
)


if __name__ == '__main__':
    ccxt_obj = CcxtTools()

    # [1] 资产,
    d_balance: Dict[str, List[SimpleBalance]] = ccxt_obj.get_accounts_balance(
        os.path.join(PATH_ROOT, 'Output', 'AccountData', 'balance.csv'))

    # [2] 合约持仓
    d_position: Dict[str, List[SimplePosition]] = ccxt_obj.get_accounts_contract_positions(
        os.path.join(PATH_ROOT, 'Output', 'AccountData', 'contract_position.csv'))

    # [3] 行情价格，BTC ETH BNB
    d_newest_price: dict = ccxt_obj.get_newest_price_usually(
        os.path.join(PATH_ROOT, 'Output', 'AccountData', 'ticker_newest_price.csv'))

    # [4] 计算净资产-统一币种  USDT BTC ETH
    ccxt_obj.cal_total_equity(
        d_exchange_balance=d_balance, d_price=d_newest_price,
        ticker_transfer_mapping=read_coin_transfer_mapping_file(),
        output_file=os.path.join(PATH_ROOT, 'Output', 'AccountData', 'total_equity.csv')
    )

    # [5] opening orders
    with open(os.path.join(PATH_ROOT, './Config', 'fetching_symbols.csv')) as f:
        l_lines = f.readlines()
    l_symbols = []
    for line in l_lines:
        line = line.strip()
        if line == '':
            continue
        l_symbols.append([line.split(',')[0], line.split(',')[1]])
    #
    ccxt_obj.get_opening_orders_by_list(
        checking_list=l_symbols, output_file=os.path.join(PATH_ROOT, 'Output', 'AccountData', 'opening_orders.csv'))

    # [6] trades
    ccxt_obj.get_my_trades_by_list(
        checking_list=l_symbols, since_timestamp=(datetime.now() - timedelta(days=1)).timestamp() * 1000,
        output_file=os.path.join(PATH_ROOT, 'Output', 'AccountData', 'trades.csv')
    )

    # TODO output csv需要表头
