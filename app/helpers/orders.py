import logging
import os
from decimal import Decimal
from math import inf

from app.exchanges.base_exchange import BaseExchange
from app.helpers.config import get_price_step_by_exchange
from app.models.data_position import DataPosition
from app.models.generic_order_side import GenericOrderSide, OrderSideEnum
from app.models.generic_position_side import PositionSideEnum


def remove_exist_order_for_orders_list(orders: list[(Decimal, Decimal)], exist_order: (Decimal, Decimal)):
    if exist_order is not None:
        return list(filter(lambda x: x[1] > Decimal(0),
                           map(lambda x: x if x[0] != exist_order[0] else (x[0], x[1] - exist_order[1]),
                               orders)))
    return orders


def get_unrealized_pnl_percent(exchange: BaseExchange, position: DataPosition) -> Decimal:
    average_entry_price = Decimal(position.average_entry_price)
    leverage = Decimal(os.getenv("MAX_LEVERAGE"))

    if position.side == PositionSideEnum.LONG:
        current_price = Decimal(exchange.buy_orders_list[0][0])
        pnl_percent = (current_price - average_entry_price) / average_entry_price * leverage
        logging.warning(
            f"Side: {position.side} | Pnl percent: {pnl_percent} | Average: {average_entry_price} | Current: {current_price}")
        return pnl_percent
    else:
        current_price = Decimal(exchange.sell_orders_list[0][0])
        pnl_percent = (average_entry_price - current_price) / average_entry_price * leverage
        logging.warning(
            f"Side: {position.side} | Pnl percent: {pnl_percent} | Average: {average_entry_price} | Current: {current_price}")
        return pnl_percent


def get_best_order_price(exchange: BaseExchange, side: GenericOrderSide, max_steps_gap: Decimal = Decimal(3),
                         depth: int = 3,
                         exist_order: (Decimal, Decimal) = None, from_order=0):
    step = get_price_step_by_exchange(exchange.exchange_type)

    if side == OrderSideEnum.BUY:
        orders = remove_exist_order_for_orders_list(exchange.buy_orders_list, exist_order)
        if not orders:
            # Fallback se non abbiamo ancora order book: usa mark_price o 0
            base_price = exchange.mark_price or Decimal('0')
            return base_price
        depth = min(depth, len(orders) - 1)
        # Opposite side safety
        if not exchange.sell_orders_list:
            opp_best = (exchange.mark_price or orders[0][0]) + step
            return min(orders[0][0] + step, opp_best)

        for i in range(from_order, depth):
            price_1, _ = orders[i]
            price_2, _ = orders[i + 1]

            step_gap = (price_1 - price_2) / step

            if step_gap >= max_steps_gap:
                return Decimal(
                    min(price_2 + step, exchange.sell_orders_list[0][0] - step,
                        (exchange.mark_price - step * Decimal(
                            os.getenv("MIN_MARK_PRICE_PRICE_GAPS"))) if exchange.mark_price else inf))

        return min(orders[from_order][0] + step, exchange.sell_orders_list[0][0] - step,
                   (exchange.mark_price - step * Decimal(
                       os.getenv("MIN_MARK_PRICE_PRICE_GAPS"))) if exchange.mark_price else inf)
    else:
        orders = remove_exist_order_for_orders_list(exchange.sell_orders_list, exist_order)
        if not orders:
            base_price = exchange.mark_price or Decimal('0')
            return base_price
        depth = min(depth, len(orders) - 1)
        if not exchange.buy_orders_list:
            opp_best = (exchange.mark_price or orders[0][0]) - step
            return max(orders[0][0] - step, opp_best)

        for i in range(from_order, depth):
            price_1, _ = orders[i]
            price_2, _ = orders[i + 1]

            step_gap = (price_2 - price_1) / step

            if step_gap >= max_steps_gap:
                # Safety check for empty order book
                best_bid = exchange.buy_orders_list[0][0] if exchange.buy_orders_list else exchange.mark_price or Decimal('0')
                return Decimal(
                    max(price_2 - step, best_bid + step,
                        (exchange.mark_price + step * Decimal(
                            os.getenv("MIN_MARK_PRICE_PRICE_GAPS"))) if exchange.mark_price else 0))

        # Safety check for empty order book
        best_bid = exchange.buy_orders_list[0][0] if exchange.buy_orders_list else exchange.mark_price or Decimal('0')
        return max(orders[from_order][0] - step, best_bid + step,
                   (exchange.mark_price + step * Decimal(
                       os.getenv("MIN_MARK_PRICE_PRICE_GAPS"))) if exchange.mark_price else 0)
