import os
import sys
from pprint import pprint
import json
import argparse
from typing import Dict, List
from time import sleep
from datetime import datetime

PATH_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.append(PATH_ROOT)

from ccxttools import (logger, EXCHANGE_API_MAPPING, API_CONFIG, Order)

arg_parser = argparse.ArgumentParser()
# arg_parser.add_argument('-t', '--type', choices=['target', '', 'all'])
arg_parser.add_argument('-i', '--input')
args = arg_parser.parse_args()
# API = args.api
PATH_INPUT = os.path.abspath(args.input)


def read_order_file() -> List[Order]:
    # 读取 订单
    l_orders = []
    with open(PATH_INPUT) as f:
        l_lines = f.readlines()
    for line in l_lines:
        line = line.strip()
        if line == '':
            continue
        order: Order = Order.from_file_string(line)
        l_orders.append(order)
    return l_orders


def log(s):
    if not os.path.isdir(os.path.dirname(OUTPUT_FILE)):
        os.makedirs(os.path.dirname(OUTPUT_FILE))
    with open(OUTPUT_FILE, 'a+', encoding='utf-8') as f:
        f.write(s + '\n')


if __name__ == '__main__':
    # 输出文件
    OUTPUT_FILE = os.path.join(
        PATH_ROOT, 'Output', 'CreateOrderLogs', 'orders_%s.csv' % datetime.now().strftime('%Y%m%d_%H%M%S'))

    # 读取文件
    l_orders = read_order_file()
    # 下单，记录订单状况
    for _order in l_orders:
        logger.info('creating order, %s' % str(_order))
        _api_name = _order.api_name
        exchange = EXCHANGE_API_MAPPING[_api_name](API_CONFIG)
        _order_structure = exchange.create_order(**_order.to_order_dict())

        # ({
        #     "id": _order_structure.id,
        #     "symbol": _order_structure.symbol,
        #     "type": _order_structure.type,
        #     "side": _order_structure.side,
        #     "amount": _order_structure.amount,
        #     "price": _order_structure.amount,
        #     "timestamp": _order_structure.timestamp
        # })

        # log
        # 查询订单状态，记录
        _order_structure = exchange.fetch_order(_order_structure["id"], _order_structure["symbol"])
        _order_status = {
            "exchange": exchange.id,
            "id": _order_structure["id"],
            "symbol": _order_structure["symbol"],
            "type": _order_structure["type"],
            "side": _order_structure["side"],
            "amount": _order_structure["amount"],
            "price": _order_structure["price"],
            "timestamp": datetime.fromtimestamp(_order_structure["timestamp"] / 1000),
            "filled": _order_structure["filled"],
            "average": _order_structure["average"],
        }
        # pprint(_order_status, indent=4)
        log(",".join([str(_) for _ in _order_status.values()]))
