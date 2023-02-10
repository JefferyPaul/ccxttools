import os
import sys
from pprint import pprint
import json
import argparse
from dataclasses import dataclass
from typing import Dict, List
from time import sleep
from datetime import datetime
import shutil

PATH_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.append(PATH_ROOT)

from ccxttools import (logger, EXCHANGE_API_MAPPING, API_CONFIG, Order, CcxtTools, TradingOrder)

arg_parser = argparse.ArgumentParser()
arg_parser.add_argument('-e', '--exchange', )
arg_parser.add_argument('-o', '--output', )
arg_parser.add_argument('-s', '--symbol', )
args = arg_parser.parse_args()
CHECKING_EXCHANGE = args.exchange
CHECKING_SYMBOL = args.symbol
PATH_OUTPUT_ROOT = os.path.abspath(args.output)
assert CHECKING_EXCHANGE in ['Spot', 'USDM', 'CoinM']
if not os.path.isdir(PATH_OUTPUT_ROOT):
    os.makedirs(PATH_OUTPUT_ROOT)


if __name__ == '__main__':
    exchange = EXCHANGE_API_MAPPING[CHECKING_EXCHANGE](API_CONFIG)
    logger.info(f'Connected api, {CHECKING_EXCHANGE}, fetch opening orders')

    _ = exchange.fetch_open_orders(symbol=CHECKING_SYMBOL)
    l_opening_orders: List[TradingOrder] = CcxtTools.fetch_open_orders_to_simple(_)
    for _opening_order in l_opening_orders:
        print('\n')
        pprint(_opening_order.__dict__, indent=4)

    #
    with open(os.path.join(PATH_OUTPUT_ROOT, 'opening_orders.csv'), 'w') as f:
        f.writelines('\n'.join([_.to_output_string() for _ in l_opening_orders]))
    shutil.copyfile(
        src=os.path.join(PATH_OUTPUT_ROOT, 'opening_orders.csv'),
        dst=os.path.join(PATH_OUTPUT_ROOT, 'opening_orders_%s.csv' % datetime.now().strftime('%Y%m%d_%H%M%S'))
    )
