import os
import sys
from collections import defaultdict, Counter
from typing import Dict, List
import json
from dataclasses import dataclass
from time import sleep
from datetime import datetime
import argparse
import shutil

import ccxt

PATH_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.append(PATH_ROOT)

from helper.simpleLogger import MyLogger
from helper.tp_WarningBoard import run_warning_board
from ccxttools import CcxtTools, read_coin_transfer_mapping_file

arg_parser = argparse.ArgumentParser()
arg_parser.add_argument('-i', '--input', help='json file')
arg_parser.add_argument('-o', '--output',)
args = arg_parser.parse_args()
PATH_INPUT_FILE = os.path.abspath(args.input)
PATH_OUTPUT_ROOT = os.path.abspath(args.output)

assert os.path.isfile(PATH_INPUT_FILE)
if os.path.isdir(PATH_OUTPUT_ROOT):
    shutil.rmtree(PATH_OUTPUT_ROOT)
    sleep(0.1)
os.makedirs(PATH_OUTPUT_ROOT)


"""
{
    "name": "LiKeKe",
    "apiKey": "W7cxiFwwOJrWxe6Vwk5BgM7XRRSVzNZX809jQipj3MXEQ3frjp7UVFYAsvVO34qR",
    "secret": "yBk9S3qDwARVl5iE6hMAUfJNFeRWPtlmS5MCtoIQvpLtvSFKPhSZ3pbVNQLsGIFs",
    "BaseCurrency": "BUSD"
}
"""


if __name__ == '__main__':
    logger = MyLogger('GetTotalEquity', output_root=os.path.join(PATH_ROOT, 'logs'))

    # 读取config，json，直接读取
    l_d_config: List[dict] = json.loads(open(PATH_INPUT_FILE, encoding='utf-8').read())
    # mapping
    d_coin_transfer_mapping = read_coin_transfer_mapping_file()
    
    #
    d_total_equity = {}
    for _product_config in l_d_config:
        _name = _product_config['name']
        _api_key = _product_config['apiKey']
        _api_secret = _product_config['secret']
        _base_currency = _product_config['BaseCurrency']
        
        logger.info(f'checking {_name}')
        ccxt_tools_obj = CcxtTools(_api_key, _api_secret, logger=logger)
        _balance = ccxt_tools_obj.get_accounts_balance(log=False)
        _price = ccxt_tools_obj.get_newest_price_usually(log=False)
        # print(_balance)
        # print(_price)
        """
        d_total_balance = {
            "USDT": total_balance_in_usdt,
            "BTC": total_balance_in_usdt / d_price['BTC/USDT'],
            "ETH": total_balance_in_usdt / d_price['ETH/USDT'],
            "BNB": total_balance_in_usdt / d_price['BNB/USDT'],
        }
        d_price = {
            'BTC/USDT': self.get_newest_price('Spot', 'BTC/USDT', log),
            'ETH/USDT': self.get_newest_price('Spot', 'ETH/USDT', log),
            'BNB/USDT': self.get_newest_price('Spot', 'BNB/USDT', log),
        }
        """
        d_te = ccxt_tools_obj.cal_total_equity(
            d_exchange_balance=_balance, d_price=_price,
            ticker_transfer_mapping=d_coin_transfer_mapping,
        )
        if _base_currency in d_coin_transfer_mapping.keys():
            _total_equity = d_te[d_coin_transfer_mapping[_base_currency]]
        else:
            _total_equity = d_te[_base_currency]
        d_total_equity[_name] = _total_equity

    #
    p_output = os.path.join(PATH_OUTPUT_ROOT, 'TotalEquity.csv')
    with open(p_output, 'w') as f:
        f.writelines("FundName,Balance\n")
        f.writelines('\n'.join([f'{_k},{str(_v)}' for _k, _v in d_total_equity.items()]))
    p_output_bak = os.path.join(PATH_OUTPUT_ROOT, 'TotalEquity_%s.csv' % datetime.now().strftime('%Y%m%d_%H%M%S'))
    shutil.copyfile(p_output, p_output_bak)
    logger.info('Finished')
