import talib
import threading
from binance.client import Client
from binance import ThreadedWebsocketManager
from binance.enums import *

api_key = None
secret_key = None
binance_client = Client(api_key, secret_key)


def get_assets():
    exchange_info = binance_client.get_exchange_info()

    assets_info = dict()

    for s in exchange_info['symbols']:
        bull = True

        is_spot_trading_allowed = s['isSpotTradingAllowed']
        quote_asset = (s['quoteAsset'] == 'USDT')
        alive = (s['status'] == 'TRADING')
        symbol = s['symbol']

        bull *= is_spot_trading_allowed * quote_asset * alive

        if bull:
            assets_info[symbol] = s

    return assets_info


assets_info = get_assets()
trade_assets = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'NEOUSDT', 'LTCUSDT']

binance_websocket = ThreadedWebsocketManager(api_key=api_key, api_secret=secret_key)
binance_websocket.start()


streams = list()
for asset in trade_assets:
    s = asset.lower()
    stream = f'{s}@kline_1m'
    streams.append(stream)


def call_back(message):
    print(message)


binance_websocket.start_multiplex_socket(callback=call_back, streams=streams)
print('start')


#binances_websocket_s.join()
