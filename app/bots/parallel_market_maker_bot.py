import asyncio
import logging
import os
import time
from decimal import Decimal
from typing import Any

from paradex_py.common.order import OrderSide

from app.bots.base_bot import BaseBot
from app.exchanges.paradex import ParadexExchange
from app.helpers.orders import get_best_order_price


def get_market_order_size(main_position: dict | None, other_position: dict | None) -> tuple[Decimal, bool] | tuple[
    None, None]:
    main_size = abs(Decimal(main_position["size"])) if main_position else 0
    other_size = abs(Decimal(other_position["size"])) if other_position else 0
    diff = main_size - other_size

    if diff > 0:
        return diff, True
    elif diff < 0:
        return abs(diff), False
    return None, None


def is_time_to_close_position(position1: dict, position2: dict) -> bool:
    positions = [pos for pos in [position1, position2] if pos is not None]
    for position in positions:
        if "created_at" in position and float(
                position["created_at"]) / 1000 + float(os.getenv("POSITION_TIME_THRESHOLD_SECONDS")) < time.time():
            return True
    return False


def get_unfilled_size(position: dict | None) -> Decimal | None:
    if position is None:
        return Decimal(os.getenv("DEFAULT_ORDER_SIZE"))

    main_not_filled_size = Decimal(os.getenv("DEFAULT_ORDER_SIZE")) - abs(Decimal(position["size"]))

    if main_not_filled_size < 0:
        return main_not_filled_size

    if main_not_filled_size < Decimal(os.getenv("MARKET_MIN_ORDER_SIZE")):
        return None

    return main_not_filled_size


def get_limit_order_size(main_order_side: OrderSide, main_position: dict | None,
                         other_position: dict | None) -> tuple[Decimal | None, OrderSide] | tuple[None, None]:
    if is_time_to_close_position(main_position, other_position):
        if main_position is None:
            return None, None

        return abs(Decimal(main_position["size"])), main_order_side.opposite_side()

    if main_position is None and other_position is None:
        return Decimal(os.getenv("DEFAULT_ORDER_SIZE")), main_order_side

    main_unfilled_size = get_unfilled_size(main_position)

    if other_position is None:
        if main_unfilled_size is not None and main_unfilled_size < 0:
            return abs(main_unfilled_size), main_order_side.opposite_side()
        return main_unfilled_size, main_order_side

    other_unfilled_size = get_unfilled_size(other_position)

    if other_unfilled_size is None and other_position is not None and main_position is not None:
        main_unfilled_size = min(abs(Decimal(other_position["size"])) - abs(Decimal(main_position["size"])),
                                 Decimal(os.getenv("DEFAULT_ORDER_SIZE")) - abs(Decimal(main_position["size"])))
        if main_unfilled_size < 0:
            return abs(main_unfilled_size), main_order_side.opposite_side()

    if main_unfilled_size < 0:
        return abs(main_unfilled_size), main_order_side.opposite_side()
    return abs(main_unfilled_size), main_order_side


def get_depth(main_position, other_position) -> int:
    if main_position and other_position and (main_position["size"] != other_position["size"]):
        return 0

    if main_position is None and other_position is not None:
        return 0

    if main_position is not None and other_position is None:
        return 0

    return int(os.getenv("DEFAULT_DEPTH_ORDER_BOOK_ANALYSIS"))


class ParallelMarketMakerBot(BaseBot):
    _exchange1: ParadexExchange = None
    _exchange2: ParadexExchange = None

    def __init__(self, exchange1: Any, exchange2: Any):
        self._exchange1 = exchange1
        self._exchange2 = exchange2

    async def trading_loop(self):
        for _ in range(3):
            tasks = []
            try:
                logging.critical("Trade START")
                tasks = [
                    asyncio.create_task(self.__side_trading(OrderSide.Buy, self._exchange1, self._exchange2)),
                    asyncio.create_task(self.__side_trading(OrderSide.Sell, self._exchange2, self._exchange1)),
                ]
                await asyncio.gather(*tasks)
                logging.critical("Trade END")
            except Exception as e:
                logging.exception(e)
                for task in tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                await self._exchange1.critical_close_all()
                await self._exchange2.critical_close_all()

    async def __side_trading(self, main_order_side: OrderSide, main_account: ParadexExchange,
                             other_account: ParadexExchange):
        while True:
            open_order = None
            if len(main_account.open_orders) > 1:
                main_account.cancel_all_orders()
                await asyncio.sleep(float(os.getenv("PING_SECONDS")))
                continue
            elif len(main_account.open_orders) == 1:
                open_order = main_account.open_orders[0]

            main_position = next(iter(main_account.open_positions), None)
            other_position = next(iter(other_account.open_positions), None)

            order_size, order_side = get_limit_order_size(main_order_side, main_position, other_position)
            is_reduce = True if order_side != main_order_side else False

            if order_size is None or order_size <= 0:
                if open_order is not None:
                    main_account.cancel_all_orders()
                await asyncio.sleep(1)
                continue

            if os.getenv("TRADING_MODE") == "2":
                market_size, market_is_reduce = get_market_order_size(main_position, other_position)

                if market_size and order_size and market_is_reduce == is_reduce:
                    main_account.cancel_all_orders()
                    await asyncio.sleep(float(os.getenv("PING_SECONDS")))
                    await main_account.open_market_order(order_side, market_size, is_reduce)
                    await asyncio.sleep(float(os.getenv("PING_SECONDS")))
                    continue

            depth = get_depth(main_position, other_position)

            best_price = get_best_order_price(main_account, order_side, Decimal(os.getenv("MAX_PRICE_STEPS_GAP")),
                                              depth,
                                              (Decimal(open_order["price"]), Decimal(
                                                  open_order["size"])) if open_order is not None else None, 0)

            if open_order is None:
                await main_account.open_limit_order(order_side, order_size, best_price, is_reduce)
            else:
                if best_price != Decimal(open_order["price"]):
                    await main_account.modify_limit_order(open_order["id"], order_side, order_size, best_price,
                                                          is_reduce)

            await asyncio.sleep(float(os.getenv("PING_SECONDS")))
