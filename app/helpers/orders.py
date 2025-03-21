import logging
import os
from decimal import Decimal

from paradex_py.common.order import OrderSide

from app.exchanges.base_exchange import BaseExchange
from app.models.position_side import PositionSide


def remove_exist_order_for_orders_list(orders: list[(Decimal, Decimal)], exist_order: (Decimal, Decimal)):
    if exist_order is not None:
        return list(filter(lambda x: x[1] > Decimal(0),
                           map(lambda x: x if x[0] != exist_order[0] else (x[0], x[1] - exist_order[1]),
                               orders)))
    return orders


def get_unrealized_pnl_percent(exchange: BaseExchange, position: dict) -> Decimal:
    side = PositionSide(position.get("side"))
    average_entry_price = Decimal(position.get("average_entry_price"))
    leverage = Decimal(os.getenv("MAX_LEVERAGE"))

    if side == PositionSide.LONG:
        current_price = Decimal(exchange.buy_orders_list[0][0])
        pnl_percent = (current_price - average_entry_price) / average_entry_price * leverage
        logging.warning(
            f"Pnl percent: {pnl_percent} | Average: {average_entry_price} | Current: {current_price}")
        return pnl_percent
    else:
        current_price = Decimal(exchange.sell_orders_list[0][0])
        pnl_percent = (average_entry_price - current_price) / average_entry_price * leverage
        logging.warning(
            f"Side: {side} | Pnl percent: {pnl_percent} | Average: {average_entry_price} | Current: {current_price}")
        return pnl_percent


def get_best_order_price(exchange: BaseExchange, side: OrderSide, max_steps_gap: Decimal = Decimal(3),
                         depth: int = 3,
                         exist_order: (Decimal, Decimal) = None, from_order=0):
    step = Decimal(os.getenv("PRICE_STEP"))

    if side == OrderSide.Buy:
        orders = remove_exist_order_for_orders_list(exchange.buy_orders_list, exist_order)

        depth = min(depth, len(orders) - 1)

        for i in range(from_order, depth):
            price_1, _ = orders[i]
            price_2, _ = orders[i + 1]

            step_gap = (price_1 - price_2) / step

            if step_gap >= max_steps_gap:
                return Decimal(
                    min(price_2 + step, exchange.sell_orders_list[0][0] - step,
                        exchange.mark_price - step * Decimal(os.getenv("MIN_MARK_PRICE_PRICE_GAPS"))))

        return min(orders[from_order][0] + step, exchange.sell_orders_list[0][0] - step,
                   exchange.mark_price - step * Decimal(os.getenv("MIN_MARK_PRICE_PRICE_GAPS")))
    else:
        orders = remove_exist_order_for_orders_list(exchange.sell_orders_list, exist_order)

        depth = min(depth, len(orders) - 1)

        for i in range(from_order, depth):
            price_1, _ = orders[i]
            price_2, _ = orders[i + 1]

            step_gap = (price_2 - price_1) / step

            if step_gap >= max_steps_gap:
                return Decimal(
                    max(price_2 - step, exchange.buy_orders_list[0][0] + step,
                        exchange.mark_price + step * Decimal(os.getenv("MIN_MARK_PRICE_PRICE_GAPS"))))

        return max(orders[from_order][0] - step, exchange.buy_orders_list[0][0] + step,
                   exchange.mark_price + step * Decimal(os.getenv("MIN_MARK_PRICE_PRICE_GAPS")))
