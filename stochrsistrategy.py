#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import time
import warnings
from datetime import datetime
from enum import Enum

import arrow
import numpy as np
import pandas as pd
import ta
from openapi_client import openapi
from openapi_genclient import ApiException
from python3_gearman import GearmanWorker
from ta import add_all_ta_features
from ta import momentum
from tqdm import tqdm

warnings.filterwarnings('ignore')

SLEEP_TIME = 0.25
SELL = 1
BUY = -1

# Значения для WithFirstOrder
WithFirstOrder = 1 << 0  # По направлению первого заказа
AgainstFirstOrder = 1 << 1  # Против направления первого заказа
InAnyDirection = WithFirstOrder | AgainstFirstOrder  # В любом направлении


def get_trace(e: Exception):
    trace = []
    tb = e.__traceback__
    while tb is not None:
        trace.append({
            "filename": tb.tb_frame.f_code.co_filename,
            "name": tb.tb_frame.f_code.co_name,
            "lineno": tb.tb_lineno
        })
        tb = tb.tb_next
    return trace


class ExtendedEnum(Enum):

    @classmethod
    def list(cls):
        return list(map(lambda c: c.value, cls))


# Типы стратегий
class StrategyEnum(ExtendedEnum):
    RSI = "RSI"
    MACD = "MACD"
    BBANDS = "BBANDS"


# Типы брокеров
class BrokerEnum(ExtendedEnum):
    Tinkoff = "Tinkoff"
    Just2Trade = "Just2Trade"


# Типы действий с заказами
class ActionEnum(ExtendedEnum):
    Open = "Open"
    Close = "Close"
    Update = "Update"
    Error = "Error"
    Unknown = "Unknown"


# Типы операций м заказами
class ReasonEnum(ExtendedEnum):
    Buy = "Buy"
    Sell = "Sell"
    StopLoss = "StopLoss"
    TakeProfit = "TakeProfit"
    Manual = "Manual"
    Unknown = "Unknown"


# Типы заказов
class OrderTypeEnum(ExtendedEnum):
    SellMarket = "SellMarket"
    SellStop = "SellStop"
    SellLimit = "SellLimit"
    BuyMarket = "BuyMarket"
    BuyStop = "BuyStop"
    BuyLimit = "BuyLimit"
    Unknown = "Unknown"


class DirectionEnum(ExtendedEnum):
    Sell = "Sell"
    Buy = "Buy"
    SellOrBuy = "SellOrBuy"


# Класс настройки сетки
class Grid:
    def __init__(self, max_orders: int = 0, lots_per_order: int = 0, step: float = 0.0, with_first_order: int = 1):
        self.max_orders = max_orders
        self.lots_per_order = lots_per_order
        self.step = step
        self.with_first_order = with_first_order


# Класс настройки заказов
class Order:
    def __init__(self, uid: str = "", order_type: OrderTypeEnum = OrderTypeEnum.Unknown, value: float = 0):
        self.OrderUID = uid
        self.Type = order_type.name
        self.Value = value


# Класс дополнительных настроек
class Extra:
    def __init__(self, key: str, value: str):
        self.Key = key
        self.Value = value


# Класс данных для возврата выполненных работ 
class Return:
    def __init__(self, robot_uid: str, instrument: str, lots: int,
                 action: ActionEnum, reason: ReasonEnum,
                 order: Order = None, extra=None):
        self.RobotUID = robot_uid
        self.Instrument = instrument
        self.Lots = lots
        self.Action = action.name
        self.Reason = reason.name
        self.Order = order
        self.Extra = extra


# Класс настроек брокера (общий)
class Broker:
    def __init__(self, sandbox: bool, orders):
        self.sandbox = sandbox
        self.orders = orders

    def connect(self, currency: str = "USD", balance: float = 50000):
        return None


# Класс настроек брокера Тинькофф
class TinkoffBroker(Broker):
    def __init__(self, sandbox: bool, brk, orders):
        super().__init__(sandbox, orders)
        self.token = brk['Token']
        self.client = None

    def connect(self, currency="USD", balance=50000):

        if self.sandbox:
            print("Брокер работает в режиме песочницы")
            self.client = openapi.sandbox_api_client(self.token)
            self.client.sandbox.sandbox_register_post()
            self.client.sandbox.sandbox_currencies_balance_post({
                "currency": currency,
                "balance": balance
            })
        else:
            print("Брокер работает в боевом режиме")
            self.client = openapi.api_client(self.token)

        return self.client


# Класс настроек брокера Just2Trade
class Just2TradeBroker(Broker):
    def __init__(self, sandbox: bool, brk, orders):
        super().__init__(sandbox, orders)
        self.key = brk['Key']
        self.secret = brk['Secret']


# Класс стратегии (общий)
class TradeStrategy:

    def __init__(self, worker: GearmanWorker, job, brk, robot_settings, stop_loss: float, take_profit: float,
                 lots: int, ticker: str, interval: str, grid: Grid):
        self.worker = worker
        self.job = job
        self.broker = brk

        self.robot_settings = robot_settings
        self.stop_loss = -stop_loss
        self.take_profit = take_profit
        self.lots = lots
        self.ticker = ticker
        self.interval = interval
        self.grid = grid


# Класс стратегии StochasticRSI
class StochasticRSIStrategy(TradeStrategy):
    def __init__(self, worker: GearmanWorker, job, brk: Broker, robot_settings, stop_loss: float, take_profit: float,
                 lots: int, ticker: str, interval: str,
                 rsi_upper_margin: float, rsi_lower_margin: float, rsi_length: int, rsi_k: int, rsi_d: int, grid: Grid):
        super().__init__(worker, job, brk, robot_settings, stop_loss, take_profit, lots, ticker, interval, grid)

        self.dataframe = None
        self.rsi_upper_margin = rsi_upper_margin
        self.rsi_lower_margin = rsi_lower_margin
        self.rsi_length = rsi_length
        self.rsi_k = rsi_k
        self.rsi_d = rsi_d

        self.broker.client = self.broker.connect()

    def cancel_order(self, order_id):
        self.broker.client.orders.orders_cancel_post(order_id, async_req=False)

    def check_if_equity_exists(self):
        exists = [a.average_position_price.value for a in
                  self.broker.client.portfolio.portfolio_get(async_req=False).payload.positions
                  if a.figi == self.ticker]
        return exists

    def check_if_order_exists(self):
        orders = self.broker.client.orders.orders_get(async_req=False)
        exists = [a.order_id for a in orders.payload if a.figi == self.ticker]
        return exists

    def get_last_price(self):
        return self.broker.client.market.market_orderbook_get(self.ticker, depth=20, async_req=False).payload.last_price

    def is_stop_loss(self, base_price):
        market_price = self.get_last_price()
        if market_price <= base_price * (float(100 + self.stop_loss) / 100.0):
            return True
        else:
            return False

    def is_take_profit(self, base_price):
        market_price = self.get_last_price()
        if market_price >= base_price * (float(100 + self.take_profit) / 100.0):
            return True
        else:
            return False

    # Метод исполнения стратегии
    def execute(self):
        # Проверяем если есть позиции в портфеле
        last_price = self.get_last_price()

        # Есть позиции и передан(ы) номер(а) заказа(ов)
        if len(self.broker.orders) != 0:
            print(
                f"Уже есть открытый заказ {self.broker.orders[0]['OrderUID']} с ценой {self.broker.orders[0]['OpenPrice']}, продолжаю его сопровождение")
            # Проверка SL/TP по сравнению цены у открывшегося заказа
            order_open_price = float(self.broker.orders[0]["OpenPrice"])
            order_direction = self.broker.orders[0]["Direction"]
            stop_loss = self.is_stop_loss(order_open_price)
            take_profit = self.is_take_profit(order_open_price)
            print(
                f"Stop loss значение: {self.stop_loss}, множитель: {(float(100 + self.stop_loss) / 100.0)}, цена: {order_open_price * (float(100 + self.stop_loss) / 100.0)}")

            print(
                f"Take profit значение {self.take_profit}, множитель: {(float(100 + self.take_profit) / 100.0)}, цена: {order_open_price * (float(100 + self.take_profit) / 100.0)}")

            if stop_loss:
                if order_direction == DirectionEnum.Buy.value:
                    print(
                        f"!! Сигнал закрытия заказа {self.broker.orders[0]['OrderUID']} через SL получен, закрываю через заказ на продажу !!")
                    result = self.place_market_order(DirectionEnum.Sell.value)
                    return self.return_sl_tp(result, order_open_price, OrderTypeEnum.SellMarket)
                elif order_direction == DirectionEnum.Sell.value:
                    print(
                        f"!! Сигнал закрытия заказа {self.broker.orders[0]['OrderUID']} через SL получен, закрываю через заказ на покупку !!")
                    result = self.place_market_order(DirectionEnum.Buy.value)
                    return self.return_sl_tp(result, order_open_price, OrderTypeEnum.BuyMarket)
            elif take_profit:
                if order_direction == DirectionEnum.Buy.value:
                    print(
                        f"!! Сигнал закрытия заказа {self.broker.orders[0]['OrderUID']} через TP получен, закрываю через заказ на продажу !!")
                    result = self.place_market_order(DirectionEnum.Sell.value)
                    return self.return_sl_tp(result, order_open_price, OrderTypeEnum.SellMarket)
                elif order_direction == DirectionEnum.Sell.value:
                    print(
                        f"!! Сигнал закрытия заказа {self.broker.orders[0]['OrderUID']} через TP получен, закрываю через заказ на покупку !!")
                    result = self.place_market_order(DirectionEnum.Buy.value)
                    return self.return_sl_tp(result, order_open_price, OrderTypeEnum.BuyMarket)
            else:
                print(f"Сигнал для закрытия заказа {self.broker.orders[0]['OrderUID']} по SL/TP не обнаружен")
        else:
            print(
                f"Нет открытых заказов, продолжаю исполнение с поиском сигнала на открытие заказа с типом {self.robot_settings['Direction']}")

        # Если нет сигнала на закрытие по SL/TP, отрабатываем сигналы на покупку/продажу
        data = self.load_data(2)
        dn, up = self.calculate_indicators(data)

        up_signal = up[-1]
        down_signal = dn[-1]

        if self.robot_settings["Direction"] == DirectionEnum.Buy.value:
            if up_signal == BUY and len(self.broker.orders) == 0:
                print("!!! Сигнал открытия заказа на покупку обнаружен, выполняю !!!")
                result = self.place_market_order(DirectionEnum.Buy.value)
                return self.return_buy_sell(result, last_price, OrderTypeEnum.BuyMarket)

            elif down_signal == SELL and len(self.broker.orders) > 0:
                print(
                    f"Сигнал закрытия заказа {self.broker.orders[0]['OrderUID']} на покупку через продажу обнаружен, выполняю")
                result = self.place_market_order(DirectionEnum.Sell.value)
                return self.return_buy_sell(result, last_price, OrderTypeEnum.SellMarket)

            else:
                return Return(self.robot_settings["RobotUID"], self.ticker, self.lots, ActionEnum.Unknown,
                              ReasonEnum.Unknown, Order("", OrderTypeEnum.Unknown, last_price),
                              [Extra("Message", "Сигнала для покупки или продажи не обнаружено"),
                               Extra("Description", "Сигнала для покупки или продажи не обнаружено")])

        elif self.robot_settings["Direction"] == DirectionEnum.Sell.value:
            if down_signal == SELL and len(self.broker.orders) == 0:
                print("!!! Сигнал открытия заказа на продажу обнаружен, выполняю !!!")
                result = self.place_market_order(DirectionEnum.Sell.value)
                return self.return_buy_sell(result, last_price, OrderTypeEnum.BuyMarket)

            elif up_signal == BUY and len(self.broker.orders) > 0:
                print(
                    f"Сигнал закрытия заказа {self.broker.orders[0]['OrderUID']} на продажу через покупку обнаружен, выполняю")
                result = self.place_market_order(DirectionEnum.Buy.value)
                return self.return_buy_sell(result, last_price, OrderTypeEnum.SellMarket)

            else:
                return Return(self.robot_settings["RobotUID"], self.ticker, self.lots, ActionEnum.Unknown,
                              ReasonEnum.Unknown, Order("", OrderTypeEnum.Unknown, last_price),
                              [Extra("Message", "Сигнала для покупки или продажи не обнаружено"),
                               Extra("Description", "Сигнала для покупки или продажи не обнаружено")])

        else:
            return Return(self.robot_settings["RobotUID"], self.ticker, self.lots, ActionEnum.Unknown,
                          ReasonEnum.Unknown, Order("", OrderTypeEnum.Unknown, last_price),
                          [Extra("Message", "Неправильное направление робота, может быть только Sell/Buy/SellOrBuy"),
                           Extra("Description",
                                 "Неправильное направление робота, может быть только Sell/Buy/SellOrBuy")])

    # Возвращает результаты открытия закрытия позиций, открытых ранее
    def return_sl_tp(self, result, order_open_price, order_type: OrderTypeEnum):
        extra = [Extra("tracking_id", result.tracking_id),
                 Extra("ParentOrderUID", str(self.broker.orders[0]['OrderUID']))]

        if result.status == "Ok":
            return Return(self.robot_settings["RobotUID"], self.ticker, self.lots,
                          ActionEnum.Close, ReasonEnum.TakeProfit,
                          Order(result.payload.order_id, order_type,
                                order_open_price * (float(100 + self.stop_loss) / 100.0)), extra)

        extra.extend([Extra("Message", result.payload.message), Extra("Description", result.payload.reject_reason)])
        return Return(self.robot_settings["RobotUID"], self.ticker, self.lots,
                      ActionEnum.Error, ReasonEnum.TakeProfit,
                      Order("", order_type, order_open_price * (float(100 + self.stop_loss) / 100.0)), extra)

    # Возвращает результаты работы заказов на продажу или покупку
    def return_buy_sell(self, result, last_price, order_type: OrderTypeEnum):
        stoch_rsi = self.dataframe['stochrsi'][-1]
        stoch_rsi_d = self.dataframe['stochrsi_d'][-1]
        stoch_rsi_k = self.dataframe['stochrsi_k'][-1]

        extra = [Extra("tracking_id", result.tracking_id)]

        if result.status == "Ok":
            extra.extend(
                [Extra("RSI", str(stoch_rsi)), Extra("RSI_D", str(stoch_rsi_d)), Extra("RSI_K", str(stoch_rsi_k))])
            return Return(self.robot_settings["RobotUID"], self.ticker, self.lots, ActionEnum.Open, ReasonEnum.Buy,
                          Order(result.payload.order_id, order_type, last_price), extra)

        extra.extend([Extra("Message", result.payload.message), Extra("Description", result.payload.reject_reason)])
        return Return(self.robot_settings["RobotUID"], self.ticker, self.lots,
                      ActionEnum.Error, ReasonEnum.Buy,
                      Order("", order_type, last_price),
                      extra)

    # Загружает данные цены (свечей) от брокера
    def load_data(self, days):
        result = []
        for i in tqdm(range(days)):
            end = arrow.now().shift(days=-i).format("YYYY-MM-DDTHH:mm:ssZZ")
            start = arrow.now().shift(days=-i - 1).format("YYYY-MM-DDTHH:mm:ssZZ")
            try:
                response = self.broker.client.market.market_candles_get(figi=self.ticker,
                                                                        _from=start,
                                                                        to=end,
                                                                        interval=self.interval, async_req=False)
                print("Исторические данные успешно получены")
            except Exception as e:
                print(str({
                    "type": type(e).__name__,
                    "message": str(e),
                    "trace": get_trace(e)
                }))
            else:
                print(f"Попытка получения исторических данных через {SLEEP_TIME} секунд")
                result.extend(response.payload.candles)
                time.sleep(SLEEP_TIME)
        return result

    def get_orders(self):
        return self.broker.client.orders.orders_get(async_req=False)

    def get_operations(self, period):
        end = arrow.now().format("YYYY-MM-DDTHH:mm:ssZZ")
        start = arrow.now().shift(days=-period).format("YYYY-MM-DDTHH:mm:ssZZ")
        return self.broker.client.operations.operations_get(start, end, async_req=False)

    # Создание лимитного заказа
    def place_limit_order(self, operation: str, price: float):
        result = self.broker.client.orders.orders_limit_order_post(self.ticker, {
            "lots": self.lots,
            "operation": operation,
            "price": price}, async_req=False)
        return result

    # Создание рыночного заказа
    def place_market_order(self, operation: str):
        result = self.broker.client.orders.orders_market_order_post(self.ticker, {
            "lots": self.lots,
            "operation": operation}, async_req=False)
        return result

    # Расчёт индикаторов
    def calculate_indicators(self, data):
        # building dataframe
        dataframe = pd.DataFrame.from_records([a.to_dict() for a in data])

        # setting datetime index for dataframe
        dataframe['datetime'] = pd.to_datetime(dataframe.time)
        dataframe.set_index('datetime', inplace=True)
        dataframe.sort_values(by='datetime', inplace=True)

        # using average signal (middle of hi and lo)
        dataframe['avg'] = (dataframe['h'] + dataframe['l']) / 2

        # calculating Stoch RSI
        indicator = ta.momentum.StochRSIIndicator(dataframe.avg, window=self.rsi_length,
                                                  smooth1=self.rsi_k,
                                                  smooth2=self.rsi_d, fillna=True)

        dataframe = add_all_ta_features(dataframe, "o", "h", "l", "c", "v", fillna=True)
        dataframe['stochrsi'] = indicator.stochrsi()
        dataframe['stochrsi_d'] = indicator.stochrsi_d()
        dataframe['stochrsi_k'] = indicator.stochrsi_k()

        up = self.rsi_upper_margin
        down = self.rsi_lower_margin

        # checking if all three indicators if out of upper-lower margins
        # and k, and d are between Stoch RSI and margin

        up_filters = (dataframe['stochrsi_d'] > up) & (dataframe['stochrsi_k'] > up) & (
                dataframe['stochrsi'] > dataframe['stochrsi_d']) & (dataframe['stochrsi'] > dataframe['stochrsi_k'])
        down_filters = (dataframe['stochrsi_d'] < down) & (dataframe['stochrsi_k'] < down) & (
                dataframe['stochrsi'] < dataframe['stochrsi_d']) & (dataframe['stochrsi'] < dataframe['stochrsi_k'])

        # setting signals
        dataframe['down_signal'] = np.where(up_filters, SELL, np.nan)
        dataframe['up_signal'] = np.where(down_filters, BUY, np.nan)

        # markers for graph
        dataframe['down_marker'] = dataframe['down_signal'] * dataframe.c
        dataframe['up_marker'] = (dataframe['up_signal'] + 1) * dataframe.c

        self.dataframe = dataframe
        # returning signals
        return dataframe['down_signal'], dataframe['up_signal']


broker = None
strategy = None


#  Обработка стратегии RSI через диспетчер задач
def run_strategy_rsi(gearman_worker: GearmanWorker, gearman_job):
    global broker
    global strategy

    data = json.loads(gearman_job.data)

    strategy_settings = data['Strategy']
    order_settings = data['Order']
    broker_settings = data['Broker']
    robot_settings = data['Robot']

    now = datetime.now()
    print(f"Запуск задачи для робота {robot_settings['RobotUID']} в ", now.strftime("%d/%m/%Y %H:%M:%S"))

    strategy_name = strategy_settings['Name']
    broker_name = broker_settings['Name']

    # Parse order parameters
    ticker = order_settings['Ticker']
    interval = order_settings['Interval']
    stop_loss = order_settings['StopLoss']
    take_profit = order_settings['TakeProfit']
    lots = order_settings['Lots']
    orders = order_settings['Orders']

    if strategy_name not in StrategyEnum.list():
        return json.dumps(
            Return(robot_settings['RobotUID'], ticker, lots, ActionEnum.Error, ReasonEnum.Unknown, Order(),
                   [Extra("Message", "Неверная или пустая стратегия передана для работы"),
                    Extra("Description", "Неверная или пустая стратегия передана для работы")]).__dict__,
            default=lambda o: o.__dict__)

    if broker_name not in BrokerEnum.list():
        return json.dumps(
            Return(robot_settings['RobotUID'], ticker, lots, ActionEnum.Error, ReasonEnum.Unknown, Order(),
                   [Extra("Message", "Неверный или пустой брокер передан для работы"),
                    Extra("Description", "Неверный или пустой брокер передан для работы")]).__dict__,
            default=lambda o: o.__dict__)

    # Parse broker parameters
    sandbox = broker_settings['Sandbox']

    if broker_name == BrokerEnum.Tinkoff.name:
        broker = TinkoffBroker(sandbox, broker_settings, orders)
    else:
        print("Неверный или пустой брокер, выход")
        return json.dumps(
            Return(robot_settings['RobotUID'], ticker, lots, ActionEnum.Error, ReasonEnum.Unknown, Order(),
                   [Extra("Message", "Неверный или пустой брокер передан для работы"),
                    Extra("Description", "Неверный или пустой брокер передан для работы")]).__dict__,
            default=lambda o: o.__dict__)

    # Parse strategy parameters
    if strategy_name == "RSI":
        rsi_upper_margin = strategy_settings['UpperMargin']
        rsi_lower_margin = strategy_settings['LowerMargin']
        rsi_length = strategy_settings['Length']
        rsi_k = strategy_settings['K']
        rsi_d = strategy_settings['D']
        grid_settings = strategy_settings['Grid']

        max_orders = grid_settings['MaxOrders']
        lots_per_order = grid_settings['LotsPerOrder']
        step = float(grid_settings['Step'])
        with_first_order = grid_settings['WithFirstOrder']
        grid = Grid(max_orders, lots_per_order, step, with_first_order)
        try:
            strategy = StochasticRSIStrategy(gearman_worker, gearman_job, broker, robot_settings,
                                             stop_loss, take_profit, lots, ticker, interval,
                                             rsi_upper_margin, rsi_lower_margin, rsi_length, rsi_k, rsi_d, grid)
        except ApiException as e:
            print("Ошибка инициализации стратегии: ", e.reason, ", трассировка : ", get_trace(e))
            return json.dumps(
                Return(robot_settings['RobotUID'], ticker, lots, ActionEnum.Error, ReasonEnum.Unknown, Order(),
                       [Extra("Message", "Ошибка инициализации стратегии"),
                        Extra("Description", e.reason)]).__dict__, default=lambda o: o.__dict__)

    else:
        print("Неверная или пустая стратегия, выход")
        return json.dumps(
            Return(robot_settings['RobotUID'], ticker, lots, ActionEnum.Error, ReasonEnum.Unknown, Order(),
                   [Extra("Message", "Ошибка инициализации робота"),
                    Extra("Description", "Неверная или пустая стратегия")]).__dict__,
            default=lambda o: o.__dict__)

    print(f"Исполняется стратегия {strategy_name} для робота {robot_settings['RobotUID']}")
    try:
        ret = strategy.execute()
        print(f"Стратегия для робота {robot_settings['RobotUID']} выполнена успешно")
    except ApiException as e:
        print("Исключение во время исполнения запроса у брокера", e.__str__(), ", трассировка: ", get_trace(e))
        ret = Return(robot_settings['RobotUID'], ticker, lots, ActionEnum.Error, ReasonEnum.Unknown, Order(),
                     [Extra("Message", "Исключение во время исполнения запроса у брокера"),
                      Extra("Description", e.__str__())])
    except Exception as ex:
        print("Общее исключение во время работы робота", ex.__str__(), ", трассировка: ", get_trace(ex))
        ret = Return(robot_settings['RobotUID'], ticker, lots, ActionEnum.Error, ReasonEnum.Unknown, Order(),
                     [Extra("Message", "Общее исключение во время работы робота"),
                      Extra("Description", ex.__str__())])

    return json.dumps(ret, default=lambda o: o.__dict__)


def main():
    gm_worker = GearmanWorker(["0.0.0.0:4730"])
    gm_worker.register_task(StrategyEnum.RSI.name, run_strategy_rsi)

    print("Запуск рабочего процесса для обработки стратегий\n")

    gm_worker.work()


if __name__ == "__main__":
    main()
