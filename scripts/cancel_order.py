import os
import sys
from pprint import pprint
import json
import argparse
from dataclasses import dataclass
from typing import Dict, List
from time import sleep
from datetime import datetime

PATH_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(PATH_ROOT)

from ccxttools import (logger_obj, EXCHANGE_API_MAPPING, read_api_config_file, Order)

arg_parser = argparse.ArgumentParser()
arg_parser.add_argument('-e', '--exchange', )
arg_parser.add_argument('--symbol', )
arg_parser.add_argument('--id', default='')
args = arg_parser.parse_args()
CHECKING_EXCHANGE = args.exchange
CHECKING_SYMBOL = args.symbol
CHECKING_ID = args.id
assert CHECKING_EXCHANGE in ['Spot', 'USDM', 'CoinM']

#
API_CONFIG = read_api_config_file()


if __name__ == '__main__':
    exchange = EXCHANGE_API_MAPPING[CHECKING_EXCHANGE](API_CONFIG)
    logger_obj.info(f'Connected api, {CHECKING_EXCHANGE}, cancel orders, {CHECKING_SYMBOL}, {CHECKING_ID}')
    if CHECKING_ID:
        _o = exchange.cancel_order(id=CHECKING_ID, symbol=CHECKING_SYMBOL)
        pprint(_o, indent=4)
    else:
        _o = exchange.cancel_all_orders(symbol=CHECKING_SYMBOL)
        pprint(_o, indent=4)
