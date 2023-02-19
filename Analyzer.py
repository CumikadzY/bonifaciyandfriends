from typing import List, Tuple, Dict
import pandas as pd
from exchanges.StockExchange import StockExchange, StockExchangeType
import time
import threading
from numba import jit
import numpy as np

import multiprocessing
global common_spreads

right_pairs = {"BTC", "ADA", "BCH", "DOGE", "DOT", "EOS", "ETH", "LINK", "LTC", "UNI", "TRX", "XRP", "XLM"}
class Query(threading.Thread):

    chart = list()
    work_chart = [None]*2*len(right_pairs)
    def __init__(self):
        threading.Thread.__init__(self)
        for pair in right_pairs:
            self.chart.append(Currency('F_' + pair))
            self.chart.append(Currency('S_' + pair))
        self.start()
    def run(self) -> None:
        self.chart= sorted(self.chhart,
                           key = lambda x: np.array(x.get_spreads()).max(),
                           reverse=True)
        for order in work_chart:
            # засунуть сюда аналайзер
            pass

    def start_track(self,ind):
        #проверка на наличие баланса и риски ликвидации
        work_chart.insert(ind,chart.pop(ind))

        return work_chart

    def stop_track(self,ind):
        chart.append(work_chart.pop(ind))
        return work_chart


class Currency:
    open_orders = None

    def __init__(self,name,):
        self.name = name

    def get_spreads(self):
        return common_spreads[self.name]

class Analyzer(threading.Thread):
    manager = multiprocessing.Manager()
    saved_spreads: Dict[str, List[List[float]]] = manager.dict()  # pair : spreads

    def __init__(self, oracle):
        threading.Thread.__init__(self)
        self.oracle = oracle

        self.start()

    def run(self) -> None:
        self.start_analyze()


    @jit(forceobj=True)
    def calculate_spread(self, bid, ask):
        return (ask - bid) / bid * 100


    def start_analyze(self):
        global right_pairs

        while True:
            stock_exchanges = list(self.oracle.stock_exchanges)

            f_s_t = time.time()

            #Futures
            futures_stock_exchanges = list(filter(lambda x: x.supports_futures, stock_exchanges))

            if len(futures_stock_exchanges) >= 2:
                for pair in map(lambda x: "F_" + x, right_pairs):
                    process = multiprocessing.Process(target=self.analyze_spread_for_pair, args=(futures_stock_exchanges, pair))
                    process.start()

                print("futures analyzed", time.time() - f_s_t)

            # Spot
            f_s_t = time.time()

            spot_stock_exchanges = list(filter(lambda x: x.supports_spot, stock_exchanges))

            if len(spot_stock_exchanges) >= 2:
                for pair in map(lambda x: "S_" + x, right_pairs):
                    process = multiprocessing.Process(target=self.analyze_spread_for_pair, args=(spot_stock_exchanges, pair))
                    process.start()

                print("spots analyzed", time.time() - f_s_t)

            time.sleep(0.3)


    def analyze_spread_for_pair(self, stock_exchanges, pair):
        try:
            spreads = []

            for i in range(len(stock_exchanges)):
                spreads.append([])
                for j in range(len(stock_exchanges)):
                    if i == j:  # means the same stock exchange
                        spreads[i].append(0.0)
                    else:
                        ask = stock_exchanges[j].pairs[pair][1]
                        bid = stock_exchanges[i].pairs[pair][0]

                        if (ask is not None) and (bid is not None):
                            spreads[i].append(self.calculate_spread(bid, ask))
                        else:
                            spreads[i].append(0.0)

            self.saved_spreads[pair] = spreads

            self.oracle.got_new_analyzed_data(self.analyze_spreads(pair, spreads))
            common_spreads = spreads
            return True

        except Exception as e:
            return False

    def print_pretty(self, pair, spreads):
        df = pd.DataFrame(spreads)
        print(pair)
        print(df)

    def analyze_spreads(self, pair, spreads: List[List[float]]) -> (str, float):  # spread, buy, sell
        result: List[Tuple[float, int, int]] = list()
        m = 0

        for i in range(len(spreads)):
            for j in range(len(spreads)):
                if spreads[i][j] > self.oracle.threshold:
                    result.append((spreads[i][j], i, j))
                    m = max(m, spreads[i][j])

        return (pair, m)
