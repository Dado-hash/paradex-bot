import asyncio
import logging
import os
import time
from decimal import Decimal
from typing import Any

from paradex_py.common.order import OrderSide

from app.bots.base_bot import BaseBot
from app.exchanges.paradex import ParadexExchange
from app.helpers.orders import get_unrealized_pnl_percent, get_best_order_price
from app.helpers.utils import get_random_size
from app.models.position_side import PositionSide


def get_order_size_for_open(same_side_position: dict | None, other_side_position: dict | None) -> Decimal | None:
    max_order_size = Decimal(os.getenv("MAX_ORDER_SIZE"))
    if same_side_position is not None:
        max_order_size = min(
            Decimal(os.getenv("MAX_POSITION_SIZE")) - abs(Decimal(same_side_position["size"])),
            Decimal(os.getenv("MAX_ORDER_SIZE"))
        )

    if max_order_size < Decimal(os.getenv("MARKET_MIN_ORDER_SIZE")):
        return None

    min_order_size = 0
    if other_side_position is not None:
        min_order_size = abs(Decimal(other_side_position["size"]))

    min_order_size = max(
        min_order_size,
        min(max_order_size, Decimal(os.getenv("MIN_ORDER_SIZE"))),
        Decimal(os.getenv("MARKET_MIN_ORDER_SIZE"))
    )

    return get_random_size(min_order_size, max_order_size)


class SingleMarketMakerBot(BaseBot):
    _exchange: ParadexExchange = None

    def __init__(self, exchange: Any):
        self._exchange = exchange

    async def trading_loop(self):
        for _ in range(3):
            tasks = []
            try:
                logging.critical("Trade START")
                tasks = [
                    asyncio.create_task(self.__side_trading(OrderSide.Buy)),
                    asyncio.create_task(self.__side_trading(OrderSide.Sell))
                ]
                await asyncio.gather(*tasks)
                logging.critical("Trade END")
            except Exception as e:
                logging.exception(e)
                for task in tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                await self._exchange.critical_close_all()

    async def __side_trading(self, order_side: OrderSide):
        while True:
            positions = self._exchange.open_positions or []
            same_side_position: dict | None = next(filter(
                lambda x: PositionSide[x["side"]].is_same_side_with_order(
                    order_side), positions), None)
            other_side_position: dict | None = next(filter(
                lambda x: not PositionSide[x["side"]].is_same_side_with_order(
                    order_side), positions), None)

            if same_side_position is not None:
                pnl_percent = get_unrealized_pnl_percent(self._exchange, same_side_position)

                if pnl_percent > Decimal(os.getenv("TAKE_PROFIT_TAKER_THRESHOLD")):
                    logging.critical("Max PROFIT limit reached")
                    await self._exchange.critical_close_all()
                    await asyncio.sleep(1)
                    continue
                elif pnl_percent < -Decimal(os.getenv("STOP_LOSS_TAKER_THRESHOLD")):
                    logging.critical("Max LOSS limit reached")
                    await self._exchange.critical_close_all()
                    await asyncio.sleep(1)
                    continue
                elif "created_at" in same_side_position and float(
                        same_side_position["created_at"]) / 1000 + float(
                    os.getenv("MAX_TAKER_POSITION_TIME_SECONDS")) < time.time():
                    logging.critical("Max position time limit reached")
                    await self._exchange.critical_close_all()
                    await asyncio.sleep(1)
                    continue

            elif other_side_position is not None:
                if "created_at" in other_side_position and float(
                        other_side_position["created_at"]) / 1000 + float(
                    os.getenv("MIN_POSITION_TIME_SECONDS")) > time.time():
                    self._exchange.cancel_all_orders()
                    # Sleep until min position time is coming
                    await asyncio.sleep(float(
                        other_side_position["created_at"]) / 1000 + float(
                        os.getenv("MIN_POSITION_TIME_SECONDS")) - time.time())
                    continue

            open_orders = list(filter(lambda x: OrderSide(x["side"]) == order_side, self._exchange.open_orders or []))
            open_order = None
            if len(open_orders) > 1:
                self._exchange.cancel_all_orders()
                await asyncio.sleep(float(os.getenv("PING_SECONDS")))
                continue
            elif len(open_orders) == 1:
                open_order = open_orders[0]

            depth = int(os.getenv("DEFAULT_DEPTH_ORDER_BOOK_ANALYSIS"))
            if other_side_position is not None:
                pnl_percent = get_unrealized_pnl_percent(self._exchange, other_side_position)
                if ("created_at" in other_side_position and float(
                        other_side_position["created_at"]) / 1000 + float(
                    os.getenv("MAX_MAKER_POSITION_TIME_SECONDS")) < time.time()) or (
                        pnl_percent > Decimal(os.getenv("TAKE_PROFIT_MAKER_THRESHOLD"))) or (
                        pnl_percent < -Decimal(os.getenv("STOP_LOSS_MAKER_THRESHOLD"))):
                    depth = 0

            best_price = get_best_order_price(self._exchange, order_side, Decimal(os.getenv("MAX_PRICE_STEPS_GAP")),
                                              depth,
                                              (Decimal(open_order["price"]), Decimal(
                                                  open_order["size"])) if open_order is not None else None, 0)
            order_size = get_order_size_for_open(same_side_position, other_side_position)

            if order_size is None:
                if open_order is not None:
                    self._exchange.cancel_all_orders()
                await asyncio.sleep(1)
                continue

            if open_order is None:
                await self._exchange.open_limit_order(order_side, order_size, best_price)
            else:
                if best_price != Decimal(open_order["price"]):
                    await self._exchange.modify_limit_order(open_order["id"], order_side, order_size, best_price)

            await asyncio.sleep(float(os.getenv("PING_SECONDS")))
