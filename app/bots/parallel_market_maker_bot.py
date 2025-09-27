import asyncio
import logging
import os
import time
from decimal import Decimal

from app.bots.base_bot import BaseBot
from app.exchanges.base_exchange import BaseExchange
from app.helpers.config import get_market_min_order_size_by_exchange
from app.helpers.orders import get_best_order_price
from app.helpers.utils import get_attribute
from app.models.data_position import DataPosition
from app.models.exchange_type import ExchangeType
from app.models.generic_order_side import GenericOrderSide, OrderSideEnum


def get_market_order_size(main_position: DataPosition | None, other_position: DataPosition | None) -> tuple[
                                                                                                          Decimal, bool] | \
                                                                                                      tuple[None, None]:
    main_size = abs(Decimal(main_position.size)) if main_position else 0
    other_size = abs(Decimal(other_position.size)) if other_position else 0
    diff = main_size - other_size

    if diff > 0:
        return diff, True
    elif diff < 0:
        return abs(diff), False
    return None, None


def is_time_to_close_position(position1: DataPosition | None, position2: DataPosition | None) -> bool:
    positions = [pos for pos in [position1, position2] if pos is not None]
    for position in positions:
        created_at = get_attribute(position, "created_at")

        if created_at and (
                float(created_at) / 1000 + float(os.getenv("POSITION_TIME_THRESHOLD_SECONDS")) < time.time()):
            return True
    return False


def get_unfilled_size(position: DataPosition | None, exchange_type: ExchangeType) -> Decimal | None:
    if position is None:
        return Decimal(os.getenv("DEFAULT_ORDER_SIZE"))

    main_not_filled_size = Decimal(os.getenv("DEFAULT_ORDER_SIZE")) - abs(Decimal(position.size))

    if main_not_filled_size < 0:
        return main_not_filled_size

    if main_not_filled_size < get_market_min_order_size_by_exchange(exchange_type) or main_not_filled_size == 0:
        return None

    return main_not_filled_size


def get_limit_order_size(main_order_side: GenericOrderSide, main_position: DataPosition | None,
                         other_position: DataPosition | None, exchange_type: ExchangeType) -> tuple[
                                                                                                  Decimal | None, GenericOrderSide] | \
                                                                                              tuple[
                                                                                                  None, None]:
    if is_time_to_close_position(main_position, other_position):
        if main_position is None:
            return None, None

        return abs(Decimal(main_position.size)), main_order_side.opposite_side()

    if main_position is None and other_position is None:
        return Decimal(os.getenv("DEFAULT_ORDER_SIZE")), main_order_side

    main_unfilled_size = get_unfilled_size(main_position, exchange_type)

    if other_position is None:
        if main_unfilled_size is not None and main_unfilled_size < 0:
            return abs(main_unfilled_size), main_order_side.opposite_side()
        return main_unfilled_size, main_order_side

    other_unfilled_size = get_unfilled_size(other_position, exchange_type)

    if other_unfilled_size is None and other_position is not None and main_position is not None:
        main_unfilled_size = min(abs(Decimal(other_position.size)) - abs(Decimal(main_position.size)),
                                 Decimal(os.getenv("DEFAULT_ORDER_SIZE")) - abs(Decimal(main_position.size)))
        if main_unfilled_size < 0:
            return abs(main_unfilled_size), main_order_side.opposite_side()

    if main_unfilled_size is None:
        return None, None

    if main_unfilled_size < 0:
        return abs(main_unfilled_size), main_order_side.opposite_side()
    return abs(main_unfilled_size), main_order_side


def get_depth(main_position: DataPosition | None, other_position: DataPosition | None) -> int:
    if main_position and other_position and (main_position.size != other_position.size):
        return 0

    if main_position is None and other_position is not None:
        return 0

    if main_position is not None and other_position is None:
        return 0

    return int(os.getenv("DEFAULT_DEPTH_ORDER_BOOK_ANALYSIS"))


class ParallelMarketMakerBot(BaseBot):
    _exchange1: BaseExchange
    _exchange2: BaseExchange

    def __init__(self, exchange1: BaseExchange, exchange2: BaseExchange):
        self._exchange1 = exchange1
        self._exchange2 = exchange2

    def _validate_trading_directions(self):
        """Validate trading direction configuration"""
        exchange1_side = os.getenv("EXCHANGE1_SIDE", "BUY").upper()
        exchange2_side = os.getenv("EXCHANGE2_SIDE", "SELL").upper()
        allow_same_direction = os.getenv("ALLOW_SAME_DIRECTION", "False").lower() == "true"

        # Validate values
        valid_sides = ["BUY", "SELL"]
        if exchange1_side not in valid_sides:
            raise ValueError(f"EXCHANGE1_SIDE must be BUY or SELL, got: {exchange1_side}")
        if exchange2_side not in valid_sides:
            raise ValueError(f"EXCHANGE2_SIDE must be BUY or SELL, got: {exchange2_side}")

        # Check if same direction is allowed
        if not allow_same_direction and exchange1_side == exchange2_side:
            raise ValueError(f"Same direction trading not allowed. Both exchanges set to {exchange1_side}. Set ALLOW_SAME_DIRECTION=True to enable.")

        # Log configuration
        strategy_type = "HEDGING" if exchange1_side != exchange2_side else "DIRECTIONAL"
        logging.warning(f"Trading Strategy: {strategy_type}")
        logging.warning(f"  - {self._exchange1.exchange_type.value}: {exchange1_side} ({'LONG' if exchange1_side == 'BUY' else 'SHORT'})")
        logging.warning(f"  - {self._exchange2.exchange_type.value}: {exchange2_side} ({'LONG' if exchange2_side == 'BUY' else 'SHORT'})")

        if strategy_type == "DIRECTIONAL":
            logging.warning("⚠️  WARNING: Both exchanges in same direction - this is NOT market neutral!")
        else:
            logging.info("✅ Market neutral hedging strategy configured")

    async def trading_loop(self):
        # Validate trading directions configuration
        self._validate_trading_directions()

        for _ in range(3):
            tasks = []
            try:
                logging.critical("Trade START")

                # Read configurable trading directions from .env
                exchange1_side_str = os.getenv("EXCHANGE1_SIDE", "BUY").upper()
                exchange2_side_str = os.getenv("EXCHANGE2_SIDE", "SELL").upper()

                # Convert to enum
                exchange1_side = OrderSideEnum.BUY if exchange1_side_str == "BUY" else OrderSideEnum.SELL
                exchange2_side = OrderSideEnum.BUY if exchange2_side_str == "BUY" else OrderSideEnum.SELL

                logging.info(f"Trading configuration: {self._exchange1.exchange_type.value}={exchange1_side_str}, {self._exchange2.exchange_type.value}={exchange2_side_str}")

                tasks = [
                    asyncio.create_task(
                        self.__side_trading(GenericOrderSide(exchange1_side, self._exchange1.exchange_type),
                                            self._exchange1, self._exchange2)),
                    asyncio.create_task(
                        self.__side_trading(GenericOrderSide(exchange2_side, self._exchange2.exchange_type),
                                            self._exchange2, self._exchange1)),
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

    async def __side_trading(self, main_order_side: GenericOrderSide, main_account: BaseExchange,
                             other_account: BaseExchange):
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

            # HEDGE IMMEDIATO: se l'altra gamba ha già una posizione e questa no, opzionalmente apri subito market
            if os.getenv("IMMEDIATE_HEDGE_ON_FILL", "false").lower() == "true":
                if main_position is None and other_position is not None and len(main_account.open_orders) == 0:
                    try:
                        target_size = min(Decimal(os.getenv("DEFAULT_ORDER_SIZE")), abs(Decimal(other_position.size)))
                        if target_size > 0:
                            logging.info(f"⚡ Immediate hedge: opening market {main_order_side.value} {target_size} on {main_account.exchange_type.value}")
                            main_account.open_market_order(main_order_side, target_size, is_reduce=False)
                            await asyncio.sleep(float(os.getenv("PING_SECONDS")))
                            continue
                    except Exception as hedge_e:
                        logging.error(f"Immediate hedge error: {hedge_e}")

            order_size, order_side = get_limit_order_size(main_order_side, main_position, other_position,
                                                          main_account.exchange_type)
            is_reduce = True if order_side != main_order_side else False

            if order_size is None or order_size <= 0:
                main_account.cancel_all_orders()
                await asyncio.sleep(1)
                continue

            if os.getenv("TRADING_MODE") == "2":
                market_size, market_is_reduce = get_market_order_size(main_position, other_position)

                if market_size and order_size and market_is_reduce == is_reduce:
                    main_account.cancel_all_orders()
                    main_account.open_market_order(order_side, market_size, is_reduce)
                    await asyncio.sleep(float(os.getenv("PING_SECONDS")))
                    continue

            depth = get_depth(main_position, other_position)

            best_price = get_best_order_price(main_account, order_side, Decimal(os.getenv("MAX_PRICE_STEPS_GAP")),
                                              depth,
                                              (Decimal(open_order.price), Decimal(
                                                  open_order.size)) if open_order is not None else None, 0)

            if open_order is None:
                main_account.open_limit_order(order_side, order_size, best_price, is_reduce)
            else:
                if best_price != Decimal(open_order.price):
                    main_account.modify_limit_order(open_order.id, order_side, order_size, best_price,
                                                    is_reduce)

            await asyncio.sleep(float(os.getenv("PING_SECONDS")))
