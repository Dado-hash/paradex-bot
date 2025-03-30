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
        else:
            exchange: BaseExchange = BackpackExchange(os.getenv("API_KEY"), os.getenv("API_SECRET"))
        await exchange.setup()

        check_balance(exchange, Decimal(os.getenv("MAX_POSITION_SIZE")))

        bot = SingleMarketMakerBot(exchange)

    elif strategy == '2':
        exchange1: BaseExchange
        exchange2: BaseExchange

        exchange_type_1 = os.getenv("EXCHANGE_TYPE_1")
        if exchange_type_1 == '1':
            exchange1: BaseExchange = ParadexExchange(os.getenv("L1_ADDRESS_1"), os.getenv("L2_PRIVATE_KEY_1"))
        else:
            exchange1: BaseExchange = BackpackExchange(os.getenv("API_KEY_1"), os.getenv("API_SECRET_1"))

        exchange_type_2 = os.getenv("EXCHANGE_TYPE_2")
        if exchange_type_2 == '1':
            exchange2: BaseExchange = ParadexExchange(os.getenv("L1_ADDRESS_2"), os.getenv("L2_PRIVATE_KEY_2"))
        else:
            exchange2: BaseExchange = BackpackExchange(os.getenv("API_KEY_2"), os.getenv("API_SECRET_2"))

        await exchange1.setup()
        await exchange2.setup()

        check_balance(exchange1, Decimal(os.getenv("DEFAULT_ORDER_SIZE")))
        check_balance(exchange2, Decimal(os.getenv("DEFAULT_ORDER_SIZE")))

        bot = ParallelMarketMakerBot(exchange1, exchange2)

    if bot is None:
        raise RuntimeError("Stratage type incorrect")

    await bot.trading_loop()


def check_balance(exchange: BaseExchange, max_size: Decimal):
    min_balance = exchange.buy_orders_list[0][0] * Decimal("1.05") * max_size / Decimal(
        os.getenv("MAX_LEVERAGE"))
    if min_balance > exchange.balance:
        raise RuntimeError(
            f"Not enough money | Min: {min_balance} USDC | Max Leverage: {os.getenv('MAX_LEVERAGE')}")


if __name__ == '__main__':
    asyncio.run(main())
