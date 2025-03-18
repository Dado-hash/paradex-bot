import asyncio
import logging
import os
import sys
from decimal import Decimal

from dotenv import load_dotenv

from app.bots.parallel_market_maker_bot import ParallelMarketMakerBot
from app.bots.single_market_maker_bot import SingleMarketMakerBot
from app.exchanges.paradex import ParadexExchange
from log_setup import log_setup

if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

load_dotenv()
log_setup()


async def main():
    logging.warning("Running for MARKET: " + os.getenv("MARKET"))

    strategy = os.getenv("STRATEGY_TYPE")
    bot = None

    if strategy == '1':
        exchange = ParadexExchange(os.getenv("L1_ADDRESS"), os.getenv("L2_PRIVATE_KEY"))
        await exchange.setup()

        check_balance(exchange, Decimal(os.getenv("MAX_POSITION_SIZE")))

        bot = SingleMarketMakerBot(exchange)

    elif strategy == '2':
        exchange1 = ParadexExchange(os.getenv("L1_ADDRESS_1"), os.getenv("L2_PRIVATE_KEY_1"))
        exchange2 = ParadexExchange(os.getenv("L1_ADDRESS_2"), os.getenv("L2_PRIVATE_KEY_2"))

        await exchange1.setup()
        await exchange2.setup()

        check_balance(exchange1, Decimal(os.getenv("DEFAULT_ORDER_SIZE")))
        check_balance(exchange2, Decimal(os.getenv("DEFAULT_ORDER_SIZE")))

        bot = ParallelMarketMakerBot(exchange1, exchange2)

    if bot is None:
        raise RuntimeError("Stratage type incorrect")

    await bot.trading_loop()


def check_balance(exchange: ParadexExchange, max_size: Decimal):
    min_balance = exchange.buy_orders_list[0][0] * Decimal("1.05") * max_size / Decimal(
        os.getenv("MAX_LEVERAGE"))
    if min_balance > exchange.balance:
        raise RuntimeError(
            f"Not enough money | Min: {min_balance} USDC | Max Leverage: {os.getenv('MAX_LEVERAGE')}")


if __name__ == '__main__':
    asyncio.run(main())
