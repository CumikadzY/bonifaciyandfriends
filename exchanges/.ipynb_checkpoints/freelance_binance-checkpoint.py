import talib
import threading
from binance.client import Client
from binance import ThreadedWebsocketManager
from binance.enums import *
import pandas as pd


class Data:
    trade_assets = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'NEOUSDT', 'LTCUSDT']
    df = pd.DataFrame(columns=trade_assets)
