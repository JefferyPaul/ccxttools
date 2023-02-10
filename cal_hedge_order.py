import os
import sys
from pprint import pprint
from typing import Dict, List

PATH_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.append(PATH_ROOT)

from ccxttools import (
    EXCHANGE_API_MAPPING, API_CONFIG, CcxtTools, SimplePosition,
    cal_hedge_order, cal_target_positions_to_order,
    logger,
)


if __name__ == '__main__':
    # d = cal_hedge_order(
    #     os.path.join(PATH_ROOT, 'Output', 'AccountData', 'balance_gb_account_coin.csv'),
    #     os.path.join(PATH_ROOT, 'Output', 'AccountData', 'contract_position_gb_account.csv'),
    #     os.path.join(PATH_ROOT, 'Output', '_order_hedge.csv')
    # )
    # pprint(d, indent=4)

    exchange = EXCHANGE_API_MAPPING['USDM'](API_CONFIG)
    # 对冲合约，目标持仓
    hedge_contracts: List[SimplePosition] = CcxtTools.fetch_positions_all_to_simple(exchange.fetch_positions(), 'USDM')
    pprint(hedge_contracts)
    orders = cal_target_positions_to_order(api_name='CoinM', targets=hedge_contracts)
    with open(os.path.join(PATH_ROOT, 'Output', 'order_test.csv'), 'w') as f:
        f.writelines(
            '\n'.join([order.to_output_string() for order in orders])
        )
