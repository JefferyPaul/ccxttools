import os
import sys
from pprint import pprint
import json
from time import sleep
from datetime import datetime, timedelta
from typing import Dict, List
import argparse
import shutil

PATH_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(PATH_ROOT)

from ccxttools import (
    CcxtTools, CoinContractSpotTickerMapping,
    logger_obj, SimplePosition, SimpleBalance, read_coin_transfer_mapping_file,
    Order, TransferOrder, ReversContract
)
from helper.simpleLogger import MyLogger


arg_parser = argparse.ArgumentParser()
arg_parser.add_argument('-o', '--output')
args = arg_parser.parse_args()
PATH_OUTPUT_ROOT = os.path.abspath(args.output)
if os.path.isdir(PATH_OUTPUT_ROOT):
    shutil.rmtree(PATH_OUTPUT_ROOT)
    sleep(0.1)
os.makedirs(PATH_OUTPUT_ROOT)

D_PATH_OUTPUT_FILES = {
    # 平 coinM合约持仓
    "0": os.path.join(PATH_OUTPUT_ROOT, '0_Order_FlatCoinMContract.csv'),
    # [1] 转 CoinM保证金至Spot
    "1": os.path.join(PATH_OUTPUT_ROOT, '1_Transfer_CoinmToSpot.csv'),
    # [2] 卖 Spot卖出[1]中转出的coin
    "2": os.path.join(PATH_OUTPUT_ROOT, '2_Order_SellCoinFromCoinM.csv'),
    # [3] 平 USDM合约持仓
    "3": os.path.join(PATH_OUTPUT_ROOT, '3_Order_FlatUSDMConTract.csv'),
    # [4] 转 USDM转出保证金至Spot
    "4": os.path.join(PATH_OUTPUT_ROOT, '4_Transfer_USDMToSpot.csv'),
    # [5] 卖 Spot卖出[4]中的coin
    "5": os.path.join(PATH_OUTPUT_ROOT, '5_Order_SellCoinFromUSDM.csv'),
    # [6] 卖 Spot中的coin
    "6": os.path.join(PATH_OUTPUT_ROOT, '6_Order_SellCoinFromSpot.csv')
}


def _gen_amount_at_close_ratio(amount, exchange, type) -> float or int:
    # coinm position, 0
    # usdm position, 2

    # coinm balance, 3
    # usdm balance, 3
    # spot balance, 3
    if close_ratio == 1:
        return amount
    else:
        if type == 'position':
            if exchange == 'CoinM':
                return round(amount * close_ratio, 0)
            if exchange == 'USDM':
                return round(amount * close_ratio, 2)
        elif type == 'balance':
            return round(amount * close_ratio, 3)
    raise KeyError


def main():
    # base currency
    path_config_file = os.path.join(PATH_ROOT, 'Config', 'Config.json')
    d_config = json.loads(open(path_config_file, encoding='utf-8').read())
    base_currency = d_config['BaseCurrency']
    print(f"BaseCurrency: {base_currency}")

    # coin contract mapping
    path_mapping_file = os.path.join(PATH_ROOT, 'Config', 'CoinContractSpotMapping.csv')
    coin_contract_spot_mapping = CoinContractSpotTickerMapping(path_mapping_file=path_mapping_file)

    # 获取 spot资产 coinm资产 coinm持仓 usdm资产 usdm持仓
    ccxt_obj = CcxtTools(logger=logger)
    d_balance: Dict[str, List[SimpleBalance]] = ccxt_obj.get_accounts_balance(log=False)
    d_position: Dict[str, List[SimplePosition]] = ccxt_obj.get_accounts_contract_positions(log=False)
    print('\n\nBalance:')
    pprint(d_balance, indent=4)
    print('\n\nPosition:')
    pprint(d_position, indent=4)
    print('\n\n')

    # 如果coinM还有合约持仓，需要人为判断是否有误，因为可能是交易仍未暂停
    _is_coinm_position = False
    if 'CoinM' in d_position.keys():
        if len(d_position['CoinM']) > 0:
            _is_coinm_position = True
            logger.warning('CoinM中仍有持仓，是否继续并平仓？')
            os.system('pause')
            print('\n')
    # 生成订单
    # [0] 平 coinM合约持仓
    if _is_coinm_position:
        logger.info('0,平coinM合约持仓')
        l_transfer_orders = []
        for _position in d_position['CoinM']:
            l_transfer_orders.append(
                Order(api_name='CoinM', order_type='market',
                      symbol=_position.symbol,
                      amount=-_gen_amount_at_close_ratio(amount=_position.amount, exchange='CoinM', type='position'))
            )
        s_output = '\n'.join([_.to_output_string() for _ in l_transfer_orders])
        with open(D_PATH_OUTPUT_FILES["0"], 'w') as f:
            f.writelines(s_output)
        print(s_output + '\n')

    # [1] 转 CoinM保证金至Spot
    # [2] 卖 Spot卖出[1]中转出的coin
    logger.info('1,转 CoinM保证金至Spot')
    logger.info('2,卖 Spot卖出[1]中转出的coin')
    l_transfer_orders = []
    l_orders = []
    for _balance in d_balance['CoinM']:
        # BTC / ETH
        # 1 转出
        _amount = _gen_amount_at_close_ratio(amount=_balance.total, exchange='CoinM', type='balance')
        l_transfer_orders.append(
            TransferOrder(symbol=_balance.symbol, amount=_amount,
                          from_account='CoinM', to_account='Spot')
        )
        # 2 卖掉
        if _balance.symbol == base_currency:
            continue
        _spot_trading_symbol = coin_contract_spot_mapping.coin_to_spot(_balance.symbol)
        if not _spot_trading_symbol:
            logger.warning(f'找不到此coin的spot交易对, {_balance.symbol}')
        else:
            if not ReversContract.is_revers_contract(_spot_trading_symbol):
                l_orders.append(
                    Order(api_name='Spot', order_type='market', symbol=_spot_trading_symbol, amount=-_amount,)
                )
            else:
                l_orders.append(
                    Order(
                        api_name='Spot', order_type='market',
                        symbol=ReversContract.gen_normal_contract(_spot_trading_symbol),
                        amount=_gen_amount_at_close_ratio(
                            amount=ReversContract.gen_normal_trading_amount(
                                original_symbol=_spot_trading_symbol,
                                original_amount=-_amount,
                                api_config=ccxt_obj.API_CONFIG,
                            ),
                            exchange='spot',
                            type='balance'
                        ),
                    )
                )

    s_output = '\n'.join([_.to_output_string() for _ in l_transfer_orders])
    with open(D_PATH_OUTPUT_FILES["1"], 'w') as f:
        f.writelines(s_output)
    # logger.info('1,转 CoinM保证金至Spot')
    print(s_output + '\n')
    s_output = '\n'.join([_.to_output_string() for _ in l_orders])
    with open(D_PATH_OUTPUT_FILES["2"], 'w') as f:
        f.writelines(s_output)
    # logger.info('2,卖 Spot卖出[1]中转出的coin')
    print(s_output + '\n')

    # [3] 平 USDM合约持仓
    logger.info('3,平 USDM合约持仓')
    l_orders = []
    for _position in d_position["USDM"]:
        _amount = _gen_amount_at_close_ratio(amount=_position.amount, exchange='USDM', type='position')
        l_orders.append(
            Order(api_name='USDM', order_type='market', symbol=_position.symbol, amount=-_amount)
        )
    s_output = '\n'.join([_.to_output_string() for _ in l_orders])
    with open(D_PATH_OUTPUT_FILES["3"], 'w') as f:
        f.writelines(s_output)
    print(s_output + '\n')

    # [4] 转 USDM转出保证金至Spot
    # [5] 卖 Spot卖出[4]中的coin
    logger.info('4,转 USDM转出保证金至Spot')
    logger.info('5, 卖 Spot卖出[4]中的coin')

    l_transfer_orders = []
    l_orders = []
    for _balance in d_balance["USDM"]:
        _amount = _gen_amount_at_close_ratio(amount=_balance.total, exchange='USDM', type='balance')
        # 4，转出
        l_transfer_orders.append(
            TransferOrder(symbol=_balance.symbol, amount=_amount,
                          from_account='USDM', to_account='Spot')
        )
        # 5，卖掉
        if _balance.symbol == base_currency:
            continue
        _spot_trading_symbol = coin_contract_spot_mapping.coin_to_spot(_balance.symbol)
        if not _spot_trading_symbol:
            logger.warning(f'找不到此coin的spot交易对, {_balance.symbol}')
        else:
            if not ReversContract.is_revers_contract(_spot_trading_symbol):
                l_orders.append(
                    Order(api_name='Spot', order_type='market', symbol=_spot_trading_symbol, amount=-_amount,)
                )
            else:
                l_orders.append(
                    Order(
                        api_name='Spot', order_type='market',
                        symbol=ReversContract.gen_normal_contract(_spot_trading_symbol),
                        amount=_gen_amount_at_close_ratio(
                            amount=ReversContract.gen_normal_trading_amount(
                                original_symbol=_spot_trading_symbol,
                                original_amount=-_amount,
                                api_config=ccxt_obj.API_CONFIG,
                            ),
                            exchange='spot',
                            type='balance'
                        ),
                    )
                )

    s_output = '\n'.join([_.to_output_string() for _ in l_transfer_orders])
    with open(D_PATH_OUTPUT_FILES["4"], 'w') as f:
        f.writelines(s_output)
    # logger.info('4,转 USDM转出保证金至Spot')
    print(s_output + '\n')
    s_output = ''.join([_.to_output_string() for _ in l_orders])
    with open(D_PATH_OUTPUT_FILES["5"], 'w') as f:
        f.writelines(s_output)
    # logger.info('5, 卖 Spot卖出[4]中的coin')
    print(s_output + '\n')

    #  [6] 卖 Spot中的coin
    logger.info('6, 卖 Spot中的coin')
    l_orders = []
    for _balance in d_balance["Spot"]:
        if _balance.symbol == base_currency:
            continue
        _spot_trading_symbol = coin_contract_spot_mapping.coin_to_spot(_balance.symbol)
        if not _spot_trading_symbol:
            logger.warning(f'找不到此coin的spot交易对, {_balance.symbol}')
        else:
            _amount = _gen_amount_at_close_ratio(amount=_balance.total, exchange='Spot', type='balance')
            if not ReversContract.is_revers_contract(_spot_trading_symbol):
                l_orders.append(
                    Order(api_name='Spot', order_type='market', symbol=_spot_trading_symbol, amount=-_amount)
                )
            else:
                l_orders.append(
                    Order(
                        api_name='Spot', order_type='market',
                        symbol=ReversContract.gen_normal_contract(_spot_trading_symbol),
                        amount=_gen_amount_at_close_ratio(
                            amount=ReversContract.gen_normal_trading_amount(
                                original_symbol=_spot_trading_symbol,
                                original_amount=-_amount,
                                api_config=ccxt_obj.API_CONFIG,
                            ),
                            exchange='spot',
                            type='balance'
                        ),
                    )
                )
    s_output = '\n'.join([_.to_output_string() for _ in l_orders])
    with open(D_PATH_OUTPUT_FILES["6"], 'w') as f:
        f.writelines(s_output)
    print(s_output + '\n')


if __name__ == '__main__':
    logger = MyLogger('ClosePosition', output_root=os.path.join(PATH_ROOT, 'logs'))
    # 输入 调整比例
    close_ratio = abs(float(input('请输入平仓比例（输入1 全部平仓）：')))
    assert close_ratio > 0
    assert close_ratio <= 1

    main()
