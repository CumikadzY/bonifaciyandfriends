import bisect
import decimal
from typing import Tuple, Dict
import threading
import json, time, math
from binance.client import Client
from binance.exceptions import BinanceAPIException
from numba import jit
from enum import Enum
from math import ceil, floor
from VictoraAndBillySecrets import binance_api_key, binance_secret_key
from binance.enums import *
# pip install unicorn-binance-websocket-api
from unicorn_binance_websocket_api.unicorn_binance_websocket_api_manager import BinanceWebSocketApiManager
import time
import binance.helpers as utils


class column(dict):
    def __init__(self):
        self._list = []

    def __setitem__(self, key, value):
        bid_f, ask_f, bid_s, ask_s = value[2:]

        try:
            margin_f = self.calculate_spread(bid_f, ask_s)  # sell F, buy M
            margin_s = self.calculate_spread(bid_s, ask_f)  # sell M, buy F
        except:
            return None

        value[0] = max(margin_f, margin_s)
        value[1] = margin_f > margin_s

        old = self.get(key)
        if old is None:

            bisect.insort(self._list, (value, key))
            dict.__setitem__(self, key, value)
        else:

            old_pos = bisect.bisect_left(self._list, (old, key))
            new_pos = bisect.bisect_left(self._list, (value, key))
            if new_pos > old_pos:

                self._list.insert(new_pos, (value, key))
                del self._list[old_pos]
            else:

                del self._list[old_pos]
                self._list.insert(new_pos, (value, key))

            dict.__setitem__(self, key, value)

    def __delitem__(self, key):
        old = self.get(key)

        if old is not None:
            old_pos = bisect.bisect(self._list, (old, key))
            del self._list[old_pos]
        dict.__delitem__(self, key)

    def sort(self):
        try:
            values, keys = zip(*self._list)
            return dict(zip(keys, values))

        except:
            return None

    def last(self):
        return self._list[-1]

    @jit(forceobj=True)
    def calculate_spread(self, bid, ask):
        return (bid - ask) / ask * 100


class Trans(Enum):
    _FUTURES_2_OFFSHORE = 'UMFUTURE_MAIN'  # UMFUTURE_C2C USDⓈ-M Futures account transfer to C2C account
    _MARGIN_2_OFFSHORE = 'MARGIN_MAIN'  # MARGIN_C2C Margin（cross）account transfer to C2C account
    _OFFSHORE_2_MARGIN = 'MAIN_MARGIN'  # C2C_MARGIN C2C account transfer to Margin(cross) account
    _OFFSHORE_2_FUTURES = 'MAIN_UMFUTURE'  # C2C_UMFUTURE C2C account transfer to USDⓈ-M Futures account


class BinanceStockExchange(threading.Thread):
    timer = 0
    maximum = None
    supports_spot = True
    supports_futures = True
    send_message = lambda x: x

    tradable_assets = {'BTCUSDT': 'BTC', 'ETHUSDT': 'ETH', 'BNBUSDT': 'BNB', 'NEOUSDT': 'NEO', 'LTCUSDT': 'LTC',
                       'QTUMUSDT': 'QTUM', 'ADAUSDT': 'ADA', 'XRPUSDT': 'XRP', 'EOSUSDT': 'EOS', 'IOTAUSDT': 'IOTA',
                       'XLMUSDT': 'XLM', 'ONTUSDT': 'ONT', 'TRXUSDT': 'TRX', 'ETCUSDT': 'ETC', 'VETUSDT': 'VET',
                       'LINKUSDT': 'LINK', 'WAVESUSDT': 'WAVES', 'BTTUSDT': 'BTT', 'ZILUSDT': 'ZIL', 'BATUSDT': 'BAT',
                       'XMRUSDT': 'XMR', 'ZECUSDT': 'ZEC', 'IOSTUSDT': 'IOST', 'DASHUSDT': 'DASH', 'OMGUSDT': 'OMG',
                       'THETAUSDT': 'THETA', 'ENJUSDT': 'ENJ', 'MATICUSDT': 'MATIC', 'ATOMUSDT': 'ATOM',
                       'FTMUSDT': 'FTM', 'ALGOUSDT': 'ALGO', 'DOGEUSDT': 'DOGE', 'ANKRUSDT': 'ANKR', 'CHZUSDT': 'CHZ',
                       'XTZUSDT': 'XTZ', 'RVNUSDT': 'RVN', 'KAVAUSDT': 'KAVA', 'IOTXUSDT': 'IOTX', 'BCHUSDT': 'BCH',
                       'SOLUSDT': 'SOL', 'LRCUSDT': 'LRC', 'COMPUSDT': 'COMP', 'SNXUSDT': 'SNX', 'SXPUSDT': 'SXP',
                       'MANAUSDT': 'MANA', 'YFIUSDT': 'YFI', 'CRVUSDT': 'CRV', 'SANDUSDT': 'SAND', 'DOTUSDT': 'DOT',
                       'LUNAUSDT': 'LUNA', 'SUSHIUSDT': 'SUSHI', 'YFIIUSDT': 'YFII', 'KSMUSDT': 'KSM',
                       'EGLDUSDT': 'EGLD', 'UNIUSDT': 'UNI', 'AVAXUSDT': 'AVAX', 'AAVEUSDT': 'AAVE', 'NEARUSDT': 'NEAR',
                       'FILUSDT': 'FIL', 'AXSUSDT': 'AXS', 'UNFIUSDT': 'UNFI', 'GRTUSDT': 'GRT', '1INCHUSDT': '1INCH',
                       'ALICEUSDT': 'ALICE', 'TLMUSDT': 'TLM', 'BAKEUSDT': 'BAKE', 'ICPUSDT': 'ICP', 'ARUSDT': 'AR',
                       'C98USDT': 'C98', 'DYDXUSDT': 'DYDX', 'GALAUSDT': 'GALA', 'ENSUSDT': 'ENS'}

    _ticks = tradable_assets.values()
    main_column = column().fromkeys(_ticks, [1, ] * 6)
    blinds = (0.75, 0.1)

    def __init__(self, api_key=binance_api_key, secret_key=binance_secret_key):
        threading.Thread.__init__(self)
        del self.tradable_assets['EGLDUSDT']
        del self.tradable_assets['KAVAUSDT']
        del self.tradable_assets['MANAUSDT']
        self.thresholds = (0.18, 0.2)
        self.max_spread = 10.0

        self.api_key = api_key
        self.secret_key = secret_key

        self.binance_client = Client(api_key, secret_key)

        self.futures_precision = self.get_futures_precision()
        self.spot_precision = self.get_spot_precision()
        self.step_sizes = self.get_step_size()
        # self.tradable_assets = self.get_tradable_asset()
        self.sp = {'debug': True, 'alive': False, 'ticker': None, 'side': None,
                   'order_amount_futures': None, 'order_amount_margin': None}

        self.start()

    def suck_sockets(self):
        trade_markets = list(map(str.lower, self.tradable_assets.keys()))
        binance_websocket_api_manager_margin = BinanceWebSocketApiManager(exchange="binance.com-margin")
        binance_websocket_api_manager_margin.create_stream(['bookTicker'], trade_markets)

        binance_websocket_api_manager_futures = BinanceWebSocketApiManager(exchange="binance.com-futures")
        binance_websocket_api_manager_futures.create_stream(['bookTicker'], trade_markets)

        while True:
            oldest_stream_data_from_stream_buffer_margin = binance_websocket_api_manager_margin.pop_stream_data_from_stream_buffer()
            oldest_stream_data_from_stream_buffer_futures = binance_websocket_api_manager_futures.pop_stream_data_from_stream_buffer()

            try:
                if oldest_stream_data_from_stream_buffer_margin:
                    self.update_spot(self.conv_dict(oldest_stream_data_from_stream_buffer_margin))

                if oldest_stream_data_from_stream_buffer_futures:
                    self.update_futures(self.conv_dict(oldest_stream_data_from_stream_buffer_futures))

            except Exception as e:
                pass

    def set_blinds(self, lil_blind, big_blind):
        if lil_blind + big_blind > 0.5:
            raise Exception()

        self.blinds = (big_blind, lil_blind)

    def set_thresholds(self, lower_thresh, upper_thresh):
        self.thresholds = (lower_thresh, upper_thresh)

    def set_debug(self, debug):
        self.sp['debug'] = debug

    def conv_dict(self, s):
        json_acceptable_string = s.replace("'", "\"")
        d = json.loads(json_acceptable_string)
        return d

    def get_step_size(self):
        exchange_info = self.binance_client.get_exchange_info()

        assets_info = dict()

        for s in exchange_info['symbols']:
            symbol = s['symbol']
            filter_types = s['filters']
            for fil_type in filter_types:
                if fil_type['filterType'] == 'LOT_SIZE':
                    assets_info[symbol] = fil_type['stepSize']
                    break

        return assets_info

    def get_futures_precision(self):
        exchange_info = self.binance_client.futures_exchange_info()
        symbols_precision = {}
        for item in exchange_info['symbols']:
            symbols_precision[item['symbol']] = item['quantityPrecision']
        return symbols_precision

    def get_spot_precision(self):
        exchange_info = self.binance_client.get_exchange_info()

        assets_info = dict()

        for s in exchange_info['symbols']:
            bull = True

            is_spot_trading_allowed = s['isSpotTradingAllowed']
            is_margin_trading_allowed = s['isMarginTradingAllowed']
            quote_asset = (s['quoteAsset'] == 'USDT')
            alive = (s['status'] == 'TRADING')
            symbol = s['symbol']

            bull *= is_spot_trading_allowed * is_margin_trading_allowed * quote_asset * alive

            if bull:
                assets_info[symbol] = s

        return assets_info

    def get_tradable_asset(self):
        exchange_info = self.binance_client.get_exchange_info()
        futures_exchange_info = self.binance_client.futures_exchange_info()

        assets_info = dict()
        symbols_precision = dict()

        for item in futures_exchange_info['symbols']:
            symbols_precision[item['symbol']] = item['quantityPrecision']

        for s in exchange_info['symbols']:
            bull = True
            is_spot_trading_allowed = s['isSpotTradingAllowed']
            is_margin_trading_allowed = s['isMarginTradingAllowed']
            quote_asset = (s['quoteAsset'] == 'USDT')
            alive = (s['status'] == 'TRADING')
            symbol = s['symbol']
            is_futures_trading_allowed = symbol in symbols_precision
            bull *= is_spot_trading_allowed * is_margin_trading_allowed * quote_asset * alive * is_futures_trading_allowed

            if bull:
                assets_info[symbol] = s['baseAsset']

        return assets_info

    def update_spot(self, message):
        message = message['data']

        if message['s'] in self.tradable_assets.keys():
            ass = self.tradable_assets[message['s']]
            copy = self.main_column[ass].copy()
            copy[4:6] = float(message['b']), float(message['a'])
            self.main_column[ass] = copy

    def update_futures(self, message):
        message = message['data']

        if message['s'] in self.tradable_assets.keys():
            ass = self.tradable_assets[message['s']]
            copy = self.main_column[ass].copy()
            copy[2:4] = float(message['b']), float(message['a'])
            self.main_column[ass] = copy

    def log_action(self, message, console=True, telegram=True):
        if console:
            print(message)

        if telegram:
            try:
                self.send_message(message)
            except Exception as e:
                print("Telegram logging error")
                print(e)

    def open_binance_order(self, is_futures, is_buy, asset, amount):
        self.log_action(f'''
                    Trying to open {'BUY' if is_buy else 'SELL'} order on {'FUTURES' if is_futures else 'MARGIN'} stock exchange.
                    Asset: {asset}
                    Amount: {amount}
                    ''')

        if is_futures:
            self.binance_client.futures_create_order(symbol=f'{asset}USDT', side=('BUY', 'SELL')[is_buy], type='MARKET',
                                                    quantity=amount, leverage=1)
        else:
            if is_buy:
                self.binance_client.create_margin_order(symbol=f"{asset}USDT", quantity=amount, side='BUY', type="MARKET")
            else:
                self.binance_client.create_margin_order(symbol=f"{asset}USDT", quantity=amount, side="SELL", type="MARKET",
                                                        sideEffectType="MARGIN_BUY")

    def close_binance_order(self, is_futures, is_buy, asset, amount):
        self.log_action(f'''
                            Trying to close {'BUY' if is_buy else 'SELL'} order on {'FUTURES' if is_futures else 'MARGIN'} stock exchange.
                            Asset: {asset}
                            Amount: {amount}
                            ''')

        if is_futures:
            self.binance_client.futures_create_order(symbol=f'{asset}USDT', side=('BUY', 'SELL')[is_buy], type='MARKET',
                                                     quantity=amount, leverage=1, reduceOnly=True)
        else:
            if is_buy:
                data = \
                list(filter(lambda x: x['asset'] == asset, self.binance_client.get_margin_account()['userAssets']))[0]
                to_add_rounded = decimal.Decimal(data["borrowed"]) + decimal.Decimal(data["interest"])
                decimal.getcontext().prec = int(self.spot_precision[f"{asset}USDT"]['baseAssetPrecision'])
                repay_amount = to_add_rounded
                repay_amount = repay_amount * decimal.Decimal("1.004")
                self.binance_client.create_margin_order(symbol=f"{asset}USDT", side="BUY", type="MARKET",
                                                        quantity=repay_amount,
                                                        sideEffectType="AUTO_REPAY")
            else:
                self.binance_client.create_margin_order(symbol=f"{asset}USDT", quantity=amount, side="SELL",
                                                        type="MARKET")

    def make_trans(self, type_, amount):
        asset = 'USDT'
        amount = utils.round_step_size(amount, 8)
        self.binance_client.make_universal_transfer(type=type_, asset=asset, amount=amount)

    def get_margin_balance(self, asset='USDT'):
        m_bal = self.binance_client.get_margin_account()
        for bal in m_bal['userAssets']:
            if bal['asset'] == asset:
                m_bal = float(bal['free'])
                return m_bal

    def get_balance(self):
        bal = self.binance_client.get_asset_balance("USDT")
        return float(bal['free'])

    def get_futures_balance(self):
        f_bal = self.binance_client.futures_account_balance()
        for bal in f_bal:

            if bal['asset'] == 'USDT':
                f_bal = float(bal['balance'])
                return f_bal

    def do_fucking_lot_size(self, asset, amount):
        prec = self.spot_precision[asset]['baseAssetPrecision']
        min_q = decimal.Decimal(self.spot_precision[asset]['filters'][2]['minQty'])

        amount_rounded = decimal.Decimal(amount)  # decimal.Decimal(str(to_add)[:str(to_add).index(".") + 6])
        decimal.getcontext().prec = int(prec)

        while amount_rounded < min_q:
            amount_rounded += min_q

        return float(amount_rounded)

    def run(self):
        time.sleep(10)

        try:
            maxos = 0

            while True:
                session_params = self.sp
                top_leader = self.main_column.last()
                args, ticker = top_leader
                spread = args[0]
                if self.maximum is None or self.maximum[0] < self.main_column[ticker][0]:
                    self.maximum = self.main_column[ticker]
                if self.timer % (60 * 60 * 10) == 0:
                    self.log_action(str(ticker))
                    self.log_action(str(self.maximum))
                    self.maximum = None
                self.timer += 1

                if spread < self.max_spread:
                    maxos = max(maxos, spread)

                print(maxos, top_leader)
                # print(self.main_column.sort())

                time.sleep(0.1)

                lower_thresh, upper_thresh = self.thresholds

                if not session_params['debug']:
                    if session_params['alive']:
                        ticker = session_params['ticker']
                        tracked_pair = self.main_column[ticker]
                        spread = tracked_pair[0]

                        if spread <= lower_thresh:
                            self.log_action('ПОГНАЛ ЗАКРЫВАТЬ ПОЗИЦИИ')
                            session_params['alive'] = False
                            order_amount_futures = session_params['order_amount_futures']
                            order_amount_margin = session_params['order_amount_margin']

                            try:
                                if not session_params['side']: # M > F
                                    self.close_binance_order(asset=ticker, is_buy=False, is_futures=True, amount=order_amount_futures)
                                    self.close_binance_order(asset=ticker, is_buy=True, is_futures=False, amount=order_amount_margin)
                                else: # F > M
                                    self.close_binance_order(asset=ticker, is_buy=False, is_futures=False,amount=order_amount_margin)
                                    self.close_binance_order(asset=ticker, is_buy=True, is_futures=True, amount=order_amount_futures)

                            except Exception as e:
                                self.log_action("Ошибка закрытия позиции")
                                self.log_action(str(e))

                            self.log_action('ВСЕ ЗАКРЫЛ, ФИКСИРУЕМ ПРИБЫЛЬ')

                            f_balance = self.get_futures_balance()
                            m_balance = self.get_margin_balance()

                            self.make_trans(Trans._FUTURES_2_OFFSHORE.value, f_balance)
                            self.make_trans(Trans._MARGIN_2_OFFSHORE.value, m_balance)

                            self.log_action('БАБКИ ВЕРНУЛ')

                    else:
                        top_leader = self.main_column.last()

                        balance = self.get_balance()
                        amount_to_send = balance // 2
                        big_blind, lil_blind = self.blinds

                        args, ticker = top_leader
                        spread = args[0]
                        side = args[1]

                        if spread > self.max_spread:
                            continue

                        # open
                        if spread >= upper_thresh:
                            # self.log_action(f"WOW! {ticker} has {spread}!")
                            # continue
                            self.log_action(f'ПОГНАЛ ОТКРЫВАТЬ ПОЗИЦИИ\n {ticker}')

                            session_params['alive'] = True
                            session_params['ticker'] = ticker
                            session_params['side'] = side

                            self.make_trans(Trans._OFFSHORE_2_FUTURES.value, amount_to_send)
                            self.make_trans(Trans._OFFSHORE_2_MARGIN.value, amount_to_send)

                            self.log_action('ПЕРЕВЕЛ БАБКИ')

                            symbol = ticker + 'USDT'

                            order_amount_1 = order_amount_2 = floor(amount_to_send * big_blind)

                            try:
                                if not side:  # M > F
                                    self.log_action('ЗАШЕЛ ОТКРЫВАТЬ M > F')

                                    tracked_pair = self.main_column[ticker]
                                    price1, price2 = tracked_pair[3], tracked_pair[4]

                                    precision_1 = self.futures_precision[symbol]
                                    precision_2 = self.spot_precision[symbol]['baseAssetPrecision']

                                    precise_order_amount_futures = "{:0.0{}f}".format(order_amount_1 / price1, precision_1)  #
                                    precise_order_amount_margin = "{:0.0{}f}".format(order_amount_2 / price2, precision_2)  #

                                    step_size = self.step_sizes[symbol]
                                    precise_order_amount_futures = utils.round_step_size(float(precise_order_amount_futures),
                                                                                   float(step_size))
                                    precise_order_amount_margin = utils.round_step_size(float(precise_order_amount_margin),
                                                                                   float(step_size))

                                    session_params['order_amount_futures'] = precise_order_amount_futures
                                    session_params['order_amount_margin'] = precise_order_amount_margin

                                    if float(precise_order_amount_futures) * price1 < 10.0 \
                                            or float(precise_order_amount_margin) * price2 < 10.0 \
                                            or float(self.spot_precision[f'{ticker}USDT']['filters'][2]['minQty']) \
                                            > min(float(precision_1), float(precision_2)):
                                        self.log_action(f"Invalid amounts {precise_order_amount_futures} {precise_order_amount_margin}")
                                        self.do_all_from_white_blank()
                                        continue

                                    self.open_binance_order(asset=ticker, is_buy=False, is_futures=False, amount=precise_order_amount_margin)
                                    self.open_binance_order(asset=ticker, is_buy=True, is_futures=True, amount=precise_order_amount_futures)

                                else:  # F > M
                                    self.log_action('ЗАШЕЛ ОТКРЫВАТЬ F > M')
                                    tracked_pair = self.main_column[ticker]

                                    price1, price2 = tracked_pair[2], tracked_pair[5]

                                    precision_1 = self.futures_precision[symbol]
                                    precision_2 = self.spot_precision[symbol]['baseAssetPrecision']

                                    precise_order_amount_futures = "{:0.0{}f}".format(order_amount_1 / price1, precision_1)  #
                                    precise_order_amount_margin = "{:0.0{}f}".format(order_amount_2 / price2, precision_2)  #

                                    step_size = self.step_sizes[symbol]
                                    precise_order_amount_futures = utils.round_step_size(float(precise_order_amount_futures),
                                                                                   float(step_size))
                                    precise_order_amount_margin = utils.round_step_size(float(precise_order_amount_margin),
                                                                                   float(step_size))

                                    session_params['order_amount_futures'] = precise_order_amount_futures
                                    session_params['order_amount_margin'] = precise_order_amount_margin

                                    if float(precise_order_amount_futures) * price1 < 10.0 \
                                                or float(precise_order_amount_margin) * price2 < 10.0 \
                                            or float(self.spot_precision[f'{ticker}USDT']['filters'][2]['minQty']) \
                                            > min(float(precision_1), float(precision_2)):
                                        self.log_action(
                                        f"Invalid amounts {precise_order_amount_futures} {precise_order_amount_margin}")

                                        self.do_all_from_white_blank()
                                        continue

                                    self.open_binance_order(asset=ticker, is_buy=False, is_futures=True,
                                                            amount=precise_order_amount_futures)
                                    self.open_binance_order(asset=ticker, is_buy=True, is_futures=False,
                                                            amount=precise_order_amount_margin)

                            except BinanceAPIException as e:
                                        print(e)
                                        if "Not a valid margin" in e.message:
                                            items = list(self.tradable_assets.items())
                                            for (t, n) in items:
                                                if n == ticker:
                                                    del self.tradable_assets[t]
                                                    del self.main_column[ticker]
                                                    self.do_all_from_white_blank()
                                                    continue

        except Exception as e:
            print(e)
            self.do_all_from_white_blank()

    def do_all_from_white_blank(self):
        self.log_action('СЛУЧАЕТСЯ, ПЕРЕЗАПУСКАЮСЬ')
        self.sp['alive'] = False

        try:
            f_balance = self.get_futures_balance()
            m_balance = self.get_margin_balance()

            if f_balance != 0.0:
                self.make_trans(Trans._FUTURES_2_OFFSHORE.value, f_balance)
            if m_balance != 0.0:
                self.make_trans(Trans._MARGIN_2_OFFSHORE.value, m_balance)
        except Exception as e:
            print(e)

        time.sleep(10)

# binance_stock_exchange = BinanceStockExchange()
# binance_stock_exchange.start()
