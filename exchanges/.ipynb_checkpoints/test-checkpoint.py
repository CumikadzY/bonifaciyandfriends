from VictoraAndBillySecrets import binance_api_key, binance_secret_key
import bisect
import decimal
import enum
from typing import Tuple, Dict

import threading
from time import sleep
import json, time, math
from binance.client import Client
from binance import ThreadedWebsocketManager
from numba import jit
from enum import Enum
from math import ceil

import time
from binance.client import Client

class Binance:
    def __init__(self, public_key = '', secret_key = '', sync = False):
        self.time_offset = 0
        self.b = Client(public_key, secret_key)

        if sync:
            self.time_offset = self._get_time_offset()

    def _get_time_offset(self):
        res = self.b.get_server_time()
        return res['serverTime'] - int(time.time() * 1000)

    def synced(self, fn_name, **args):
        args['timestamp'] = int(time.time() - self.time_offset)
        return getattr(self.b, fn_name)(**args)

binance = Binance(public_key = binance_api_key, secret_key = binance_secret_key, sync=True)
binance.synced('get_all_orders',  symbol='BTCUSDT')

