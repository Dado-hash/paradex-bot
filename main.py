import asyncio
import logging
import os
import time
import uuid
from decimal import Decimal

from dotenv import load_dotenv
from paradex_py.common.order import OrderType, OrderSide, Order

from log_setup import log_setup
from paradex_api import Api
from utils import get_random_position_size

load_dotenv()
log_setup()

CLOSE_EXPIRE_ORDER_TIME = 5 * 60


async def trading(order_side: OrderSide):
    balance = Api.get_USDC_balance()
    order_size = get_random_position_size(balance)

    order_id = await open_limit_order(order_side, order_size)

    if isinstance(order_id, bool):
        return

    while True:
        await asyncio.sleep(0.5)

        answer = await move_limit_order(order_side, order_size, order_id)

        if isinstance(answer, bool):
            break
        if answer is not None:
            order_id = answer

    reduce_order_side = OrderSide.Buy if order_side == OrderSide.Sell else OrderSide.Sell
    position_size = Api.get_position_size()
    order_id = await open_limit_order(reduce_order_side, position_size, True)
    order_open_time = time.time()
    while True:
        await asyncio.sleep(0.5)

        position_size = Api.get_position_size()
        if position_size == Decimal(0):
            return

        current_time = time.time()

        if current_time - CLOSE_EXPIRE_ORDER_TIME > order_open_time:
            logging.critical("Order Time EXPIRED")
            return await Api.critical_close()

        answer = await move_limit_order(reduce_order_side, position_size, order_id, True)

        if isinstance(answer, bool):
            return await Api.critical_close()
        if answer is not None:
            order_id = answer


async def move_limit_order(order_side: OrderSide, init_position_size: Decimal, open_order_client_id: str,
                           is_reduce: bool = False) -> bool | str:
    try:
        order = await Api.fetch_order_by_client_id(open_order_client_id)
        if isinstance(order, bool):
            return True
    except Exception as e:
        logging.critical(e)
        return True

    remaining_size = Decimal(order["remaining_size"])

    if remaining_size != init_position_size or remaining_size == Decimal(0):
        if remaining_size == Decimal(0):
            logging.warning(
                str(order_side) + " order " + ("REDUCE_" if is_reduce else "") + "FILLED: " + open_order_client_id)
            return True
        else:
            if is_reduce:
                await asyncio.sleep(0.1)
            else:
                await asyncio.sleep(5)

            canceled_order = await Api.cancel_order_by_client_id(open_order_client_id)
            logging.warning(
                str(order_side) + " order " + (
                    "REDUCE_" if is_reduce else "") + "CANCELED (PARTIALLY FILLED): " + open_order_client_id)
            if isinstance(canceled_order, bool):
                return True

        if not is_reduce:
            return True

        return await open_limit_order(order_side, remaining_size, is_reduce)

    order_price: Decimal = Decimal(order["price"])

    best_price = Api.get_best_price(order_side, remaining_size, order_price)

    logging.warning(str(order_side) + " BEST PRICE - " + str(best_price))

    if order_price != best_price:
        canceled_order = await Api.cancel_order_by_client_id(open_order_client_id)
        logging.warning(
            str(order_side) + " order " + (
                "REDUCE_" if is_reduce else "") + "CANCELED (BEST PRICE CHANGED): " + open_order_client_id)
        if isinstance(canceled_order, bool):
            return True

        return await open_limit_order(order_side, remaining_size, is_reduce)


async def open_limit_order(order_side: OrderSide, position_size: Decimal, is_reduce: bool = False,
                           retry: int = 0) -> str | bool:
    if position_size <= Decimal(0):
        return True
    client_id = str(uuid.uuid4())
    order_price = Api.get_best_price(order_side, Decimal(0))

    open_order = Order(
        client_id=client_id,
        market=os.getenv("COIN"),
        order_type=OrderType.Limit,
        order_side=order_side,
        size=position_size,
        limit_price=order_price,
        instruction="POST_ONLY",
        reduce_only=is_reduce
    )
    order = Api.paradex.api_client.submit_order(order=open_order)
    logging.warning(
        str(order_side) + " order " + ("REDUCE_" if is_reduce else "") + "OPENED: " + order["client_id"])
    try:
        checked_order = await Api.fetch_order_by_client_id(order["client_id"])
        logging.warning(
            str(order_side) + " order CHECK " + ("REDUCE_" if is_reduce else "") + "OPENED: " + order["client_id"])
        if isinstance(checked_order, bool):
            return True
    except Exception as e:
        if retry > 3:
            raise Exception("Can't open order")
        logging.critical(e)
        await asyncio.sleep(0.1)
        return await open_limit_order(order_side, position_size, is_reduce, retry + 1)
    return order["client_id"]


async def main():
    logging.warning("Running for coin: " + os.getenv("COIN"))

    # Setup for trading
    await Api.init_paradex()
    await Api.init_price_websocket()
    await Api.critical_close()

    while True:
        try:
            # Wait for prices to be updated
            if not Api.last_buy_size or not Api.last_sell_size:
                await asyncio.sleep(1)
                continue

            balance = Api.get_USDC_balance()
            min_balance = Api.max_buy_price * Decimal(str(1.1)) * Decimal(os.getenv("MIN_SIZE")) / Decimal(
                os.getenv("MAX_LEVERAGE"))
            if balance < min_balance:
                raise RuntimeError("Not enough money | Min: " + str(min_balance))
            else:
                logging.info("Balance: " + str(balance) + " USDC | " +
                             str(round(balance / Api.max_buy_price, int(os.getenv("SIZE_ROUND")))) + " " + os.getenv(
                    "COIN"))

            side = OrderSide.Buy if Api.last_sell_size > Api.last_buy_size else OrderSide.Sell

            await trading(side)

            await asyncio.sleep(1)

        except Exception as e:
            await Api.critical_close()
            logging.critical(e)
            return


if __name__ == '__main__':
    asyncio.run(main())
