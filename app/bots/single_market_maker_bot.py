import asyncio
import logging
import os
import time
from decimal import Decimal
from typing import Optional

from app.bots.base_bot import BaseBot
from app.exchanges.base_exchange import BaseExchange
from app.helpers.config import get_market_min_order_size_by_exchange
from app.helpers.orders import get_unrealized_pnl_percent, get_best_order_price
from app.helpers.utils import get_random_size, get_attribute
from app.models.data_order import DataOrder
from app.models.data_position import DataPosition
from app.models.exchange_type import ExchangeType
from app.models.generic_order_side import GenericOrderSide, OrderSideEnum


def get_order_size_for_open(same_side_position: DataPosition | None,
                            other_side_position: DataPosition | None, exchange_type: ExchangeType) -> Decimal | None:
    max_order_size = Decimal(os.getenv("MAX_ORDER_SIZE"))
    if same_side_position is not None:
        max_order_size = min(
            Decimal(os.getenv("MAX_POSITION_SIZE")) - abs(Decimal(same_side_position.size)),
            Decimal(os.getenv("MAX_ORDER_SIZE"))
        )

    if max_order_size < get_market_min_order_size_by_exchange(exchange_type):
        return None

    min_order_size = 0
    if other_side_position is not None:
        min_order_size = abs(Decimal(other_side_position.size))

    min_order_size = max(
        min_order_size,
        min(max_order_size, Decimal(os.getenv("MIN_ORDER_SIZE"))),
        get_market_min_order_size_by_exchange(exchange_type)
    )

    return get_random_size(min_order_size, max_order_size, exchange_type)


class SingleMarketMakerBot(BaseBot):
    _exchange: BaseExchange

    def __init__(self, exchange: BaseExchange):
        self._exchange = exchange

    async def trading_loop(self):
        for _ in range(3):
            tasks = []
            try:
                logging.critical("Trade START")
                tasks = [
                    asyncio.create_task(
                        self.__side_trading(GenericOrderSide(OrderSideEnum.BUY, self._exchange.exchange_type))),
                    asyncio.create_task(
                        self.__side_trading(GenericOrderSide(OrderSideEnum.SELL, self._exchange.exchange_type)))
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

    async def __side_trading(self, order_side: GenericOrderSide):
        while True:
            positions = self._exchange.open_positions or []
            same_side_position: DataPosition | None = next(filter(
                lambda x: x.side.is_same_side_with_order(
                    order_side), positions), None)
            other_side_position: DataPosition | None = next(filter(
                lambda x: not x.side.is_same_side_with_order(
                    order_side), positions), None)
            same_created_at = get_attribute(same_side_position, "created_at")
            other_created_at = get_attribute(other_side_position, "created_at")

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
                elif same_created_at and (float(same_created_at) / 1000 + float(
                        os.getenv("MAX_TAKER_POSITION_TIME_SECONDS")) < time.time()):
                    logging.critical("Max position time limit reached")
                    await self._exchange.critical_close_all()
                    await asyncio.sleep(1)
                    continue

            elif other_side_position is not None:
                if other_created_at and (float(other_created_at) / 1000 + float(
                        os.getenv("MIN_POSITION_TIME_SECONDS")) > time.time()):
                    self._exchange.cancel_all_orders()
                    # Sleep until min position time is coming
                    await asyncio.sleep(float(other_created_at) / 1000 + float(
                        os.getenv("MIN_POSITION_TIME_SECONDS")) - time.time())
                    continue

            open_orders = list(filter(lambda x: x.side == order_side, self._exchange.open_orders or []))
            open_order: Optional[DataOrder] = None
            if len(open_orders) > 1:
                self._exchange.cancel_all_orders()
                await asyncio.sleep(float(os.getenv("PING_SECONDS")))
                continue
            elif len(open_orders) == 1:
                open_order = open_orders[0]

            depth = int(os.getenv("DEFAULT_DEPTH_ORDER_BOOK_ANALYSIS"))
            if other_side_position is not None:
                pnl_percent = get_unrealized_pnl_percent(self._exchange, other_side_position)
                if (other_created_at and (float(other_created_at) / 1000 + float(
                        os.getenv("MAX_MAKER_POSITION_TIME_SECONDS")) < time.time())) or (
                        pnl_percent > Decimal(os.getenv("TAKE_PROFIT_MAKER_THRESHOLD"))) or (
                        pnl_percent < -Decimal(os.getenv("STOP_LOSS_MAKER_THRESHOLD"))):
                    depth = 0

            best_price = get_best_order_price(self._exchange, order_side, Decimal(os.getenv("MAX_PRICE_STEPS_GAP")),
                                              depth,
                                              (Decimal(open_order.price), Decimal(
                                                  open_order.size)) if open_order is not None else None, 0)
            order_size = get_order_size_for_open(same_side_position, other_side_position, self._exchange.exchange_type)

            if order_size is None:
                if open_order is not None:
                    self._exchange.cancel_all_orders()
                await asyncio.sleep(1)
                continue

            if open_order is None:
                self._exchange.open_limit_order(order_side, order_size, best_price)
            else:
                if best_price != Decimal(open_order.price):
                    self._exchange.modify_limit_order(open_order.id, order_side, order_size, best_price)

            await asyncio.sleep(float(os.getenv("PING_SECONDS")))
