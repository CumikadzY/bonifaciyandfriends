from typing import List, Tuple

from exchanges.StockExchange import StockExchange


class Oracle:
    stock_exchanges: List[StockExchange] = list()
    threshold = 1.0

    send_message = lambda x: print()

    def got_new_analyzed_data(self, analyzed):
        if analyzed[1] != 0.0:
            self.send_message(f"Внимание! У криптовалюты *{analyzed[0]}* появился высокий спред {analyzed[1]} %")
