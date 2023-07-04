"""
账户基础配置

Config.json:
{
  "items": [
    {
      "exchange": "USDM",
      "symbols": ["BTC/USDT:USDT", "ETH/USDT:USDT", "BNB/USDT:USDT"]
    },
    {
      "exchange": "CoinM",
      "symbols": ["BTC/USD:BTC", "ETH/USD:ETH"]
    }
  ],
  "config": {
    "margin_mode": "cross",
    "leverage": 5,
    "position_mode_use_hedge": false
  },
  "_备注": [
    "margin_mode 可选: cross / isolated",
    "leverage: 数字, 1-125",
    "position_mode_use_hedge: true or false, 是否使用对冲(双向)模式. 若否则为 单向模式"
  ]
}
"""

import os
import sys
from pprint import pprint
import json
import argparse
from typing import Dict, List
from datetime import datetime
import shutil

PATH_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(PATH_ROOT)

from ccxttools import (logger_obj, EXCHANGE_API_MAPPING, read_api_config_file)

arg_parser = argparse.ArgumentParser()
arg_parser.add_argument('-i', '--input', )
args = arg_parser.parse_args()
PATH_INPUT_FILE = os.path.abspath(args.input)
assert os.path.isfile(PATH_INPUT_FILE)

#
API_CONFIG = read_api_config_file()


if __name__ == '__main__':

    # 读取输入
    d_config = json.loads(open(PATH_INPUT_FILE, encoding='utf-8').read())

    # 参数
    margin_mode = d_config['config']['margin_mode']
    leverage = int(d_config['config']['leverage'])
    position_mode_use_hedge = d_config['config']['position_mode_use_hedge']
    assert margin_mode in ['cross', 'isolated']
    assert type(position_mode_use_hedge) is bool

    # 修改
    for _item_info in d_config['items']:
        _exchange = _item_info['exchange']
        _symbols = _item_info['symbols']

        exchange = EXCHANGE_API_MAPPING[_exchange](API_CONFIG)
        logger_obj.info(f'\n\nConnected api, {_exchange}')

        for _symbol in _symbols:
            #
            # r = exchange.set_margin_mode('cross', CHECKING_SYMBOL)
            try:
                logger_obj.info(f'set margin mode, {_symbol}, {margin_mode}')
                r = exchange.set_margin_mode(margin_mode, _symbol)
            except Exception as e:
                print('\n=========  set_margin_mode ERROR  =========')
                print(e)
                print('\n')
            else:
                pprint(r, indent=4)

            #
            # r = exchange.set_leverage(5, CHECKING_SYMBOL)
            try:
                logger_obj.info(f'set leverage, {_symbol}, {leverage}')
                r = exchange.set_leverage(leverage=leverage, symbol=_symbol)
            except Exception as e:
                print('\n=========  set_leverage ERROR  =========')
                print(e)
                print('\n')
            else:
                pprint(r, indent=4)

            # # set hedged to True or False for a market
            # try:
            #     logger_obj.info(f'set position mode, {_symbol}, {["One-way", "Hedge"][int(position_mode_use_hedge)]}')
            #     r = exchange.set_position_mode(hedged=position_mode_use_hedge, symbol=_symbol)
            # except Exception as e:
            #     if "No need to change position side" in str(e):
            #         pprint("No need to change position side", indent=4)
            #     else:
            #         print('\n=========  set_position_mode ERROR  =========')
            #         print(e)
            #         print('\n')
            # else:
            #     pprint(r, indent=4)

        # set hedged to True or False for a market
        try:
            logger_obj.info(
                f'set position mode, {["One-way", "Hedge"][int(position_mode_use_hedge)]}')
            r = exchange.set_position_mode(hedged=str(position_mode_use_hedge).lower())
        except Exception as e:
            if "No need to change position side" in str(e):
                pprint("No need to change position side", indent=4)
            else:
                print('\n=========  set_position_mode ERROR  =========')
                print(e)
                print('\n')
        else:
            pprint(r, indent=4)

    logger_obj.info('Finished')
