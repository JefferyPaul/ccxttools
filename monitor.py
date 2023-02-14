"""
------------------------------------------------------------
|       TestFund    2020-1-1 12:00:05
|       持仓：
|       Spot:                   CoinM               USDM
|           BNB       1             BTC                 BTC/USDT
|           USDT      1             ETH                 ETH/USDT
|           BTC
|
|
|

"""

import sys
from time import sleep
from datetime import datetime
import argparse
import os
import json
from collections import defaultdict
from typing import List, Dict

PATH_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.append(PATH_ROOT)

from ccxttools import (
    get_binance_accounts_balance, get_newest_price, cal_total_balance,
    get_all_accounts_contract_positions, SimplePosition, cal_hedge_order, logger
)

arg_parser = argparse.ArgumentParser()
arg_parser.add_argument('--interval', default=60)
args = arg_parser.parse_args()
FLUSH_INTERVAL = float(args.interval)


def _gen_split_row():
    return ' -' * 50


def _rounding_in_length(value, length):
    pass


def _fetch_balance_string() -> list:
    """
    获取balance，转成 string

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
    d_all_balance_exchange_coin, d_all_balance_at_coin = get_binance_accounts_balance(log_out=False)

    d_l_string = defaultdict(list)
    for _name in ["Spot", "CoinM", "USDM"]:
        for _symbol in sorted(list(d_all_balance_exchange_coin[_name].keys())):
            _total = d_all_balance_exchange_coin[_name][_symbol]['total']
            _used = d_all_balance_exchange_coin[_name][_symbol]['used']
            _used_ratio = _used / _total
            d_l_string[_name].append(
                f"{str(_symbol).center(12)}{(str(_total) + ' (' + '{:.1%}'.format(_used_ratio) + ') ').ljust(23, ' ')}"
            )

    # 转换为 各行的字符串
    l_output_string = []
    l_output_string.append(f"|{'** Balance **'.center(100)}")
    l_output_string.append(f"|{'Spot'.center(35)}{'CoinM'.center(35)}{'USDM'.center(35)}")
    for n in range(max([len(d_l_string["Spot"]), len(d_l_string["CoinM"]), len(d_l_string["USDM"])])):
        row_string = ""
        row_string += "|"
        for _name in ["Spot", "CoinM", "USDM"]:
            if n <= len(d_l_string[_name])-1:
                _string = d_l_string[_name][n]
                row_string += _string
            else:
                row_string += ' ' * 35
        l_output_string.append(row_string)
    return l_output_string


def _fetch_contract_string():
    """
    获取 contract position，返回打印字符串

    {
    "Spot": [SimplePosition(api_name, symbol, amount), ]
    }
    """
    d_all_contracts: Dict[str, List[SimplePosition]] = get_all_accounts_contract_positions(log_out=False)

    d_l_string = defaultdict(list)
    for _name in ["CoinM", "USDM"]:
        for _position_data in d_all_contracts[_name]:
            _symbol = _position_data.symbol
            _amount = _position_data.amount
            d_l_string[_name].append(
                f"{str(_symbol).center(20)}{str(_amount).ljust(15, ' ')}"
            )
    d_l_string["Spot"] = []

    # 转换为 各行的字符串
    l_output_string = []
    l_output_string.append(f"|{'** ContractPosition **'.center(100)}")
    l_output_string.append(f"|{' '*35}{'CoinM'.center(35)}{'USDM'.center(35)}")
    for n in range(max([len(d_l_string["CoinM"]), len(d_l_string["USDM"])])):
        row_string = ""
        row_string += "|"
        for _name in ["Spot", "CoinM", "USDM"]:
            if n <= len(d_l_string[_name])-1:
                _string = d_l_string[_name][n]
                row_string += _string
            else:
                row_string += ' ' * 35
        l_output_string.append(row_string)
    return l_output_string


def main():
    # 表头
    s_dt_now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    L_OUTPUT_STRING.append(_gen_split_row())
    L_OUTPUT_STRING.append(f'|{str(fund_name).center(25, " ")}{str(s_dt_now).center(25, " ")}')
    L_OUTPUT_STRING.append(_gen_split_row())

    # Balance
    l_balance_string = _fetch_balance_string()
    for _ in l_balance_string:
        L_OUTPUT_STRING.append(_)
    L_OUTPUT_STRING.append(_gen_split_row())

    # Contract Position
    l_contract_position_string = _fetch_contract_string()
    for _ in l_contract_position_string:
        L_OUTPUT_STRING.append(_)
    L_OUTPUT_STRING.append(_gen_split_row())




if __name__ == '__main__':
    # 读取config
    PATH_CONFIG = os.path.join(PATH_ROOT, 'Config', 'Config.json')
    d_config = json.loads(open(PATH_CONFIG, encoding='utf-8').read())
    fund_name = d_config['name']

    while True:
        L_OUTPUT_STRING = []

        main()

        # 清屏
        os.system('cls')
        print('\n'.join(L_OUTPUT_STRING))
        # print('\r' + '\r\n'.join(L_OUTPUT_STRING), end='', flush=True)
        # sys.stdout.write('\r' + '\n'.join(L_OUTPUT_STRING))
        # sys.stdout.writelines(['\r'] + L_OUTPUT_STRING)
        # sys.stdout.flush()

        sleep(FLUSH_INTERVAL)

