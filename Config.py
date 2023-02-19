from Bot import Bot
from VictoraAndBillySecrets import binance_api_key, binance_secret_key, bitmex_api_key, bitmex_secret_key, kraken_api_key, kraken_secret_key
from exchanges.trans_binance import BinanceStockExchange

binance_stock_exchange = BinanceStockExchange(binance_api_key, binance_secret_key)
binance_stock_exchange.start()

bot = Bot(binance_stock_exchange)



def start_cumtrading():
    ''',

        BitmexStockExchange(
            bitmex_api_key,
            bitmex_secret_key,
            "wss://www.bitmex.com/realtime"
        ),

        KrakenStockExchange(
            kraken_api_key,
            kraken_secret_key,
            "wss://ws.kraken.com/"
        )


    globals()['analyzer_func'] = Thread(target=analyzer.start_analyze())
    globals()['analyzer_func'].start()
    '''
