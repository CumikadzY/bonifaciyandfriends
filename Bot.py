from telegram import Update, ForceReply, ReplyMarkup, MessageEntity
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, CallbackContext
from VictoraAndBillySecrets import tg_bot_token, binance_api_key, binance_secret_key, bitmex_api_key, bitmex_secret_key
import threading

class Bot(threading.Thread):
    admins = {
    }

    def say_hello(self, update: Update, context):
        if update.message.from_user.name in self.admins.keys():
            self.admins[update.message.from_user.name] = update.message.chat_id

            update.message.reply_text(
                f"Привет, @{update.message.from_user.name}!\nАрбитражный бот v.1.0 alpha готов к работе, пропишите /launch для старта")

    def set_thresh(self, update: Update, context):
        if update.message.from_user.name in self.admins.keys():
            self.admins[update.message.from_user.name] = update.message.chat_id
            try:
                splitted = update.message.text.split(" ")
                self.binance_stock_exchange.set_thresholds(float(splitted[1]), float(splitted[2]))

                update.message.reply_text(
                    f"Порог обновлён успешно: {self.binance_stock_exchange.threshold}")
            except:
                update.message.reply_text(
                    f"ACHTUNG!!!")

    def set_blinds(self, update: Update, context):
        if update.message.from_user.name in self.admins.keys():
            self.admins[update.message.from_user.name] = update.message.chat_id
            try:
                splitted = update.message.text.split(" ")
                self.binance_stock_exchange.set_blinds(float(splitted[1]), float(splitted[2]))

                update.message.reply_text(
                    f"Отношения обновлены успешно: {self.binance_stock_exchange.blinds}")
            except:
                update.message.reply_text(
                    f"ACHTUNG!!!")

    def enable_trading(self, update: Update, context):
        if update.message.from_user.name in self.admins.keys():
            self.admins[update.message.from_user.name] = update.message.chat_id
            try:
                self.binance_stock_exchange.set_debug(False)

                update.message.reply_text(f"Трейдинг запущен")
            except:
                update.message.reply_text(
                    f"ACHTUNG!!!")

    def disable_trading(self, update: Update, context):
        if update.message.from_user.name in self.admins.keys():
            self.admins[update.message.from_user.name] = update.message.chat_id
            try:
                self.binance_stock_exchange.set_debug(True)

                update.message.reply_text(f"Трейдинг отключен")
            except:
                update.message.reply_text(
                    f"ACHTUNG!!!")

    def get_balance(self, update: Update, context):
        if update.message.from_user.name in self.admins.keys():
            self.admins[update.message.from_user.name] = update.message.chat_id
            try:
                update.message.reply_text(f"Общий баланс: {self.binance_stock_exchange.get_balance()}")
            except:
                update.message.reply_text(
                    f"ACHTUNG!!!")

    def launch(self, update: Update, context):
        if update.message.from_user.name in self.admins.keys():
            self.admins[update.message.from_user.name] = update.message.chat_id
            '''self.oracle.stock_exchanges = [
                BinanceStockExchange(binance_api_key, binance_secret_key),
                BitmexStockExchange(bitmex_api_key, bitmex_secret_key),
                PoloniexStockExchange("", "")
            ]'''

            update.message.reply_text(f"Бот успешно стартовал")

    '''
    def get_spread_by_pairname(self, update: Update, context):
        if update.message.from_user.name in self.admins.keys():
            self.admins[update.message.from_user.name] = update.message.chat_id
            splitted = update.message.text.split(" ")

            if len(splitted) == 2 and splitted[1] in self.analyzer.saved_spreads.keys():
                spreads = pd.DataFrame(self.analyzer.saved_spreads[splitted[1].upper()])


                update.message.reply_text(str(spreads))
            else:
                update.message.reply_text("ACHTUNG!")
    '''

    def send_notification(self, message):
        for admin_chat_id in self.admins.values():
            try:
                self.updater.bot.send_message(text=message.replace(".", "\.").replace("!", "\!").replace("_", "\_"), chat_id=admin_chat_id, parse_mode="MarkdownV2")
            except Exception as e:
                print(e)
                pass


    def __init__(self, binance_stock_exchange):
        threading.Thread.__init__(self)

        self.binance_stock_exchange = binance_stock_exchange
        self.binance_stock_exchange.send_message = self.send_notification

        self.start()


    def run(self) -> None:
        self.updater = Updater(tg_bot_token)

        self.updater.dispatcher.add_handler(CommandHandler("start", self.say_hello))
        self.updater.dispatcher.add_handler(CommandHandler("launch", self.launch))
        #self.updater.dispatcher.add_handler(CommandHandler("getSpread", self.get_spread_by_pairname))
        #self.updater.dispatcher.add_handler(CommandHandler("listSpreads", self.list_spreads))
        self.updater.dispatcher.add_handler(CommandHandler("setThresholds", self.set_thresh))
        self.updater.dispatcher.add_handler(CommandHandler("setBlinds", self.set_blinds))
        self.updater.dispatcher.add_handler(CommandHandler("enableTrading", self.enable_trading))
        self.updater.dispatcher.add_handler(CommandHandler("disableTrading", self.disable_trading))
        self.updater.dispatcher.add_handler(CommandHandler("getBalance", self.get_balance))


        self.updater.start_polling()


