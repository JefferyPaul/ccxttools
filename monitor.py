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
from datetime import datetime, time
import argparse
import os
import json
from collections import defaultdict
from typing import List, Dict

PATH_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.append(PATH_ROOT)

from ccxttools import (
    CcxtTools, read_coin_transfer_mapping_file,
    SimplePosition, SimpleBalance,
)

arg_parser = argparse.ArgumentParser()
arg_parser.add_argument('--interval', default=60)
args = arg_parser.parse_args()
FLUSH_INTERVAL = float(args.interval)


def _gen_split_row():
    return ' -' * 50


class CalPnl:
    s_settle_time = "17:00:00"

    def __init__(self):
        self.last_date_total_equity = 0
        self.last_dt = datetime.strptime('2000-1-1', '%Y-%m-%d')

    def cal(self, total_equity: float) -> (float, datetime):
        checking_dt = datetime.now()
        if self.last_dt == datetime.strptime('2000-1-1', '%Y-%m-%d'):
            self.last_date_total_equity = total_equity
            self.last_dt = checking_dt
        else:
            split_dt = datetime.strptime(
                datetime.now().strftime('%Y/%m/%d') + ' ' + self.s_settle_time, '%Y/%m/%d %H:%M:%S')
            if (checking_dt > split_dt) and (self.last_dt < split_dt):
                self.last_date_total_equity = total_equity
                self.last_dt = checking_dt

        return total_equity - self.last_date_total_equity, self.last_dt


def gen_total_equity_str(d_total_equity):
    # 转换为 各行的字符串
    l_output_string = []
    l_output_string.append(f"|{'** TotalEquity **'.center(100)}")
    if BASE_CURRENCY in d_total_equity:
        _te = d_total_equity.pop(BASE_CURRENCY)
        # 记录盈亏
        _pnl, _last_dt = CAL_PNL_OBJ.cal(_te)
        if (_te - _pnl) == 0:
            _ror = 0
        else:
            _ror = _pnl / (_te - _pnl)
        l_output_string.append(
            f'|{" "*5}*{BASE_CURRENCY} {str(_te)}{" "*3}' +
            f'( pnl:{str(round(_pnl, 5))}{" "*3}' + "ror:{:.3%}".format(_ror) + f'{" "*3}{_last_dt.strftime("%m/%d %H:%M")} )')

    # 其他
    _s = ''
    for _currency in ["USDT", "BTC", "ETH"]:
        if _currency in d_total_equity:
            _te = d_total_equity.get(_currency)
            _s += f'{" "*5} {_currency} {str(_te)}'.center(30)
    if _s:
        l_output_string.append('|' + _s)
    return l_output_string


def gen_balance_str(d_balance: Dict[str, List[SimpleBalance]]) -> list:
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
    d_l_string = defaultdict(list)
    for _name in ["Spot", "CoinM", "USDM"]:
        for _simple_balance in sorted(d_balance[_name], key=lambda x: x.symbol):
            _symbol = _simple_balance.symbol
            _total = _simple_balance.total
            _used = _simple_balance.used
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


def gen_holding_contract_str(d_contracts: Dict[str, List[SimplePosition]]):
    """
    获取 contract position，返回打印字符串

    {
    "Spot": [SimplePosition(api_name, symbol, amount), ]
    }
    """
    d_l_string = defaultdict(list)
    for _name in ["CoinM", "USDM"]:
        for _position_data in sorted(d_contracts[_name], key=lambda x: x.symbol):
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
    # 获取数据，避免重复获取
    d_balance_gb_exchange: Dict[str, List[SimpleBalance]] = CCXT_OBJ.get_accounts_balance(log=False)
    d_newest_price = CCXT_OBJ.get_newest_price_usually(log=False)
    d_total_equity = CCXT_OBJ.cal_total_equity(
        d_exchange_balance=d_balance_gb_exchange, d_price=d_newest_price,
        ticker_transfer_mapping=D_COIN_TRANSFER_MAPPING,
    )
    d_holding_contracts: Dict[str, List[SimplePosition]] = CCXT_OBJ.get_accounts_contract_positions(log=False)

    # =========================================
    # 组织内容

    # [1] 表头
    s_dt_now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    L_OUTPUT_STRING.append(_gen_split_row())
    L_OUTPUT_STRING.append(f'|{str(FUND_NAME).center(25, " ")}{str(s_dt_now).center(25, " ")}')
    L_OUTPUT_STRING.append(_gen_split_row())

    # [2] Total Equity    净资产总值
    for _ in gen_total_equity_str(d_total_equity):
        L_OUTPUT_STRING.append(_)
    L_OUTPUT_STRING.append(_gen_split_row())

    # [3] Balance    资产
    for _ in gen_balance_str(d_balance_gb_exchange):
        L_OUTPUT_STRING.append(_)
    L_OUTPUT_STRING.append(_gen_split_row())

    # [4] Contract Position 合约持仓
    for _ in gen_holding_contract_str(d_holding_contracts):
        L_OUTPUT_STRING.append(_)
    L_OUTPUT_STRING.append(_gen_split_row())


if __name__ == '__main__':
    # 读取config
    PATH_CONFIG = os.path.join(PATH_ROOT, 'Config', 'Config.json')
    D_CONFIG = json.loads(open(PATH_CONFIG, encoding='utf-8').read())
    FUND_NAME = D_CONFIG['name']
    D_COIN_TRANSFER_MAPPING = read_coin_transfer_mapping_file()
    BASE_CURRENCY = D_CONFIG['BaseCurrency']
    if BASE_CURRENCY in D_COIN_TRANSFER_MAPPING:
        BASE_CURRENCY = D_COIN_TRANSFER_MAPPING[BASE_CURRENCY]
    CAL_PNL_OBJ = CalPnl()

    CCXT_OBJ = CcxtTools()

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

