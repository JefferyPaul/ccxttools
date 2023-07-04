import os
import shutil
from time import sleep
import argparse
from pprint import pprint

# 输入参数
arg_parser = argparse.ArgumentParser()
arg_parser.add_argument('--base_currency',)
arg_parser.add_argument('--equity',)
args = arg_parser.parse_args()
base_currency = args.base_currency
total_equity = float(args.equity)
# BaseCurrency / total_equity
print(f'Input Argument at Bat:')
print(f'Base Currency: {base_currency},\tAdded Equity: {str(total_equity)}')
print('请检查后，按回车，开始计算...')
os.system('pause')


PATH_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# price read file
print('\n\nreading newest price:')
path_price_file = os.path.join(PATH_ROOT, 'Output', 'AccountData', 'ticker_newest_price.csv')
with open(path_price_file) as f:
    l_lines = f.readlines()
d_price = {}
for line in l_lines:
    line = line.strip()
    _symbol = line.split(',')[0]
    try:
        _price = float(line.split(',')[1])
    except:
        print('价格文件有误')
        raise ValueError
    assert _price > 0
    d_price[_symbol] = _price
pprint(d_price, indent=4)

# d
if base_currency == 'USDT':
    path_output_folder = os.path.join(PATH_ROOT, 'Output', 'Hedge', 'USDT')

    if os.path.isdir(path_output_folder):
        shutil.rmtree(path_output_folder)
        sleep(0.1)
    os.makedirs(path_output_folder)

    # 1 transfer from spot to usdm, TE * 10%
    print('\n1 transfer from spot to usdm, TE * 10%')
    amount = total_equity * 0.2
    s_output = f'USDT,{str(amount)},Spot,USDM'
    path_1 = os.path.join(path_output_folder, '1.TransferFromSpotToUSDM.csv')
    with open(path_1, 'w') as f:
        f.write(s_output)
    print(s_output)

    # 2 Order, spot 买入币，usdm对冲
    print('\n2 Order, spot 买入币，usdm对冲')
    amount_btc = total_equity * 0.1 / d_price['BTC/USDT']
    amount_eth = total_equity * 0.1 / d_price['ETH/USDT']
    amount_btc_usdt = total_equity * 0.1 / d_price['BTC/USDT']
    amount_eth_usdt = total_equity * 0.1 / d_price['ETH/USDT']
    l_output = [
        f"Spot,market,BTC/USDT,buy,{str(amount_btc)},",
        f"Spot,market,ETH/USDT,buy,{str(amount_eth)},",
        f"USDM,market,BTC/USDT:USDT,sell,{str(amount_btc_usdt)},",
        f"USDM,market,ETH/USDT:USDT,sell,{str(amount_eth_usdt)},",
    ]
    path_2 = os.path.join(path_output_folder, '2.BuyCoinAndHedge.csv')
    with open(path_2, 'w') as f:
        f.writelines('\n'.join(l_output))
    print('\n'.join(l_output))

    # 3 transfer from spot to coinm
    print('\n3 transfer from spot to coinm')
    l_output = [
        f'BTC,{str(amount_btc)},Spot,CoinM',
        f'ETH,{str(amount_eth)},Spot,CoinM',
    ]
    path_3 = os.path.join(path_output_folder, '3.TransferInCoinM.csv')
    with open(path_3, 'w') as f:
        f.writelines('\n'.join(l_output))
    print('\n'.join(l_output))

elif base_currency == 'ETH':
    path_output_folder = os.path.join(PATH_ROOT, 'Output', 'Hedge', 'ETH')

    if os.path.isdir(path_output_folder):
        shutil.rmtree(path_output_folder)
        sleep(0.1)
    os.makedirs(path_output_folder)

    # 1  order , spot sell 15% ETH
    amount_eth = total_equity * 0.1
    l_output = [
        f'Spot,market,ETH/USDT,sell,{str(amount_eth)},',
    ]
    path_1 = os.path.join(path_output_folder, '1.SellETHForHedge.csv')
    with open(path_1, 'w') as f:
        f.writelines('\n'.join(l_output))
    print('\n'.join(l_output))

    # 2 transfer 15% USDT for hedge
    amount = total_equity * 0.1 * 0.99 * d_price['ETH/USDT']
    s_output = f'USDT,{str(amount)},Spot,USDM'
    path_2 = os.path.join(path_output_folder, '2.TransferFromSpotToUSDM.csv')
    with open(path_2, 'w') as f:
        f.write(s_output)
    print(s_output)

    # 3 USDM buy 30% ETHUSDT and sell 15% BTCUSDT , spot sell 15% ETH/BTC
    amount_eth_btc = total_equity * 0.1
    amount_eth_usdt = total_equity * 0.2
    amount_btc_usdt = total_equity * 0.1 * d_price['ETH/USDT'] / d_price['BTC/USDT']
    l_output = [
        f"USDM,market,ETH/USDT:USDT,buy,{str(amount_eth_usdt)},",
        f"Spot,market,ETH/BTC,sell,{str(amount_eth_btc)},",
        f"USDM,market,BTC/USDT:USDT,sell,{str(amount_btc_usdt)},",
    ]
    path_3 = os.path.join(path_output_folder, '3.BuyBTCandHedge.csv')
    with open(path_3, 'w') as f:
        f.writelines('\n'.join(l_output))
    print('\n'.join(l_output))

    # 4 from spot to coinm
    l_output = [
        f'BTC,{str(amount_btc_usdt)},Spot,CoinM',
        f'ETH,{str(amount_eth_usdt)},Spot,CoinM',
    ]
    path_4 = os.path.join(path_output_folder, '4.TransferFromSpotToUSDM.csv')
    with open(path_4, 'w') as f:
        f.writelines('\n'.join(l_output))
    print('\n'.join(l_output))

elif base_currency == 'BTC':
    path_output_folder = os.path.join(PATH_ROOT, 'Output', 'Hedge', 'BTC')

    if os.path.isdir(path_output_folder):
        shutil.rmtree(path_output_folder)
        sleep(0.1)
    os.makedirs(path_output_folder)

    # 1  order , spot sell 15% ETH
    amount_btc = total_equity * 0.1
    l_output = [
        f'Spot,market,BTC/USDT,sell,{str(amount_btc)},',
    ]
    path_1 = os.path.join(path_output_folder, '1.SellBTCForHedge.csv')
    with open(path_1, 'w') as f:
        f.writelines('\n'.join(l_output))
    print('\n'.join(l_output))

    # 2 transfer 15% USDT for hedge
    amount = total_equity * 0.1 * 0.99 * d_price['BTC/USDT']
    s_output = f'USDT,{str(amount)},Spot,USDM'
    path_2 = os.path.join(path_output_folder, '2.TransferFromSpotToUSDM.csv')
    with open(path_2, 'w') as f:
        f.write(s_output)
    print(s_output)

    # 3 USDM buy 30% ETHUSDT and sell 15% BTCUSDT , spot sell 15% ETH/BTC
    amount_eth_btc = total_equity * 0.1 * d_price['BTC/USDT'] / d_price['ETH/USDT']
    amount_eth_usdt = total_equity * 0.1 * d_price['BTC/USDT'] / d_price['ETH/USDT']
    amount_btc_usdt = total_equity * 0.2
    l_output = [
        f"USDM,market,BTC/USDT:USDT,buy,{str(amount_btc_usdt)},",
        f"Spot,market,ETH/BTC,buy,{str(amount_eth_btc)},",
        f"USDM,market,ETH/USDT:USDT,sell,{str(amount_eth_usdt)},",
    ]
    path_3 = os.path.join(path_output_folder, '3.BuyETHandHedge.csv')
    with open(path_3, 'w') as f:
        f.writelines('\n'.join(l_output))
    print('\n'.join(l_output))

    # 4 from spot to coinm
    l_output = [
        f'ETH,{str(amount_eth_btc)},Spot,CoinM',
        f'BTC,{str(amount_btc_usdt)},Spot,CoinM',
    ]
    path_4 = os.path.join(path_output_folder, '4.TransferFromSpotToUSDM.csv')
    with open(path_4, 'w') as f:
        f.writelines('\n'.join(l_output))
    print('\n'.join(l_output))
else:
    print('BaseCurrency Error (BTC / ETH / USDT)')
    raise Exception

