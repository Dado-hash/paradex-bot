import asyncio
import logging
import os
import sys
from decimal import Decimal

from dotenv import load_dotenv

from app.bots.parallel_market_maker_bot import ParallelMarketMakerBot
from app.bots.single_market_maker_bot import SingleMarketMakerBot
from app.exchanges.backpack import BackpackExchange
from app.exchanges.base_exchange import BaseExchange
from app.exchanges.hibachi import HibachiExchange
from app.exchanges.paradex import ParadexExchange
from app.log_setup import log_setup, get_market

if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

load_dotenv()
log_setup()


async def main():
    logging.warning("Running for MARKET: " + get_market())

    strategy = os.getenv("STRATEGY_TYPE")
    bot = None

    if strategy == '1':
        exchange_type = os.getenv("EXCHANGE_TYPE")
        exchange: BaseExchange

        if exchange_type == '1':
            exchange: BaseExchange = ParadexExchange(os.getenv("L1_ADDRESS"), os.getenv("L2_PRIVATE_KEY"))
        elif exchange_type == '2':
            exchange: BaseExchange = BackpackExchange(os.getenv("API_KEY"), os.getenv("API_SECRET"))
        else:  # exchange_type == '3' for Hibachi
            exchange: BaseExchange = HibachiExchange(os.getenv("HIBACHI_API_KEY"), os.getenv("HIBACHI_PRIVATE_KEY"))
        await exchange.setup()

        check_balance(exchange, Decimal(os.getenv("MAX_POSITION_SIZE")))

        bot = SingleMarketMakerBot(exchange)

    elif strategy == '2':
        exchange1: BaseExchange
        exchange2: BaseExchange

        exchange_type_1 = os.getenv("EXCHANGE_TYPE_1")
        logging.info(f"DEBUG: EXCHANGE_TYPE_1 = {exchange_type_1}")
        if exchange_type_1 == '1':
            logging.info("Creating Paradex exchange1")
            exchange1: BaseExchange = ParadexExchange(os.getenv("L1_ADDRESS_1"), os.getenv("L2_PRIVATE_KEY_1"))
        elif exchange_type_1 == '2':
            logging.info("Creating Backpack exchange1")
            exchange1: BaseExchange = BackpackExchange(os.getenv("API_KEY_1"), os.getenv("API_SECRET_1"))
        else:  # exchange_type_1 == '3' for Hibachi
            logging.info("Creating Hibachi exchange1")
            exchange1: BaseExchange = HibachiExchange(os.getenv("HIBACHI_API_KEY_1"), os.getenv("HIBACHI_PRIVATE_KEY_1"))

        exchange_type_2 = os.getenv("EXCHANGE_TYPE_2")
        logging.info(f"DEBUG: EXCHANGE_TYPE_2 = {exchange_type_2}")
        if exchange_type_2 == '1':
            logging.info("Creating Paradex exchange2")
            exchange2: BaseExchange = ParadexExchange(os.getenv("L1_ADDRESS_2"), os.getenv("L2_PRIVATE_KEY_2"))
        elif exchange_type_2 == '2':
            logging.info("Creating Backpack exchange2")
            exchange2: BaseExchange = BackpackExchange(os.getenv("API_KEY_2"), os.getenv("API_SECRET_2"))
        else:  # exchange_type_2 == '3' for Hibachi
            logging.info("Creating Hibachi exchange2")
            hibachi_api_key = os.getenv("HIBACHI_API_KEY_2")
            hibachi_private_key = os.getenv("HIBACHI_PRIVATE_KEY_2")
            logging.info(f"DEBUG: HIBACHI_API_KEY_2 = {'***' if hibachi_api_key else 'None'}")
            logging.info(f"DEBUG: HIBACHI_PRIVATE_KEY_2 = {'***' if hibachi_private_key else 'None'}")
            exchange2: BaseExchange = HibachiExchange(hibachi_api_key, hibachi_private_key)

        await exchange1.setup()
        await exchange2.setup()

        check_balance(exchange1, Decimal(os.getenv("DEFAULT_ORDER_SIZE")))
        check_balance(exchange2, Decimal(os.getenv("DEFAULT_ORDER_SIZE")))

        bot = ParallelMarketMakerBot(exchange1, exchange2)

    if bot is None:
        raise RuntimeError("Stratage type incorrect")

    await bot.trading_loop()


def check_balance(exchange: BaseExchange, max_size: Decimal):
    # Skip balance check if order book is not available yet
    if not exchange.buy_orders_list or len(exchange.buy_orders_list) == 0:
        logging.warning(f"Skipping balance check for {exchange.exchange_type.value} - order book not loaded yet")
        return

    min_balance = exchange.buy_orders_list[0][0] * Decimal("1.05") * max_size / Decimal(
        os.getenv("MAX_LEVERAGE"))

    if exchange.balance is None:
        logging.warning(f"Skipping balance check for {exchange.exchange_type.value} - balance not loaded yet")
        return

    if min_balance > exchange.balance:
        raise RuntimeError(
            f"Not enough money | Min: {min_balance} USDC | Max Leverage: {os.getenv('MAX_LEVERAGE')}")

    order_size = Decimal(os.getenv("DEFAULT_ORDER_SIZE"))
    logging.info(f"DEBUG Balance Check:")
    logging.info(f"  Exchange: {exchange.exchange_type}")
    logging.info(f"  Balance: {exchange.balance}")
    logging.info(f"  Order size: {order_size}")
    logging.info(f"  Mark price: {exchange.mark_price}")
    logging.info(f"  Required: {order_size * exchange.mark_price / 10}")


if __name__ == '__main__':
    asyncio.run(main())
