import os
import sys
from pprint import pprint
import argparse
from typing import Dict, List
from time import sleep
from datetime import datetime

PATH_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.append(PATH_ROOT)

from ccxttools import (logger, EXCHANGE_API_MAPPING, API_CONFIG, TransferOrder)

arg_parser = argparse.ArgumentParser()
arg_parser.add_argument('-i', '--input')
args = arg_parser.parse_args()
PATH_INPUT = os.path.abspath(args.input)


def read_transfer_order_file() -> List[TransferOrder]:
    # 读取 订单
    l_orders = []
    with open(PATH_INPUT) as f:
        l_lines = f.readlines()
    for line in l_lines:
        line = line.strip()
        if line == '':
            continue
        order: TransferOrder = TransferOrder.from_file_string(line)
        l_orders.append(order)
    return l_orders


def log(s):
    if not os.path.isdir(os.path.dirname(OUTPUT_FILE)):
        os.makedirs(os.path.dirname(OUTPUT_FILE))
    with open(OUTPUT_FILE, 'a+', encoding='utf-8') as f:
        f.write(s + '\n')


if __name__ == '__main__':
    OUTPUT_FILE = os.path.join(
        PATH_ROOT, 'Output', 'TransferLog', 'transfer_%s.csv' % datetime.now().strftime('%Y%m%d_%H%M%S'))

    # 读取文件
    l_transfer_orders = read_transfer_order_file()
    # 下单，记录订单状况
    l_trading_orders_info = []
    for _transfer_order in l_transfer_orders:
        logger.info('transfer order, %s' % str(_transfer_order))
        # 检查 from to Account
        _error = False
        if _transfer_order.from_account not in ['Spot', 'USDM', 'CoinM']:
            logger.error(f'Account Error,{_transfer_order.from_account},not (Spot, USDM, CoinM)')
            _error = True
        if _transfer_order.to_account not in ['Spot', 'USDM', 'CoinM']:
            logger.error(f'Account Error,{_transfer_order.to_account},not (Spot, USDM, CoinM)')
            _error = True
        if 'Spot' not in [_transfer_order.to_account, _transfer_order.from_account]:
            logger.error(f'No Spot Account in this Order')
            _error = True
        if _error:
            raise ValueError

        #
        if _transfer_order.from_account == 'Spot':
            # 转入期货账户
            exchange = EXCHANGE_API_MAPPING[_transfer_order.to_account](API_CONFIG)
            _rtn = exchange.transfer_in(code=_transfer_order.symbol, amount=_transfer_order.amount)
            print(_rtn)
        elif _transfer_order.to_account == 'Spot':
            # 转出期货账户
            exchange = EXCHANGE_API_MAPPING[_transfer_order.from_account](API_CONFIG)
            _rtn = exchange.transfer_out(code=_transfer_order.symbol, amount=_transfer_order.amount)
            print(_rtn)
        else:
            logger.error(f'No Spot Account in this Order')
            continue

        # log
        log(_transfer_order.to_output_string())
