import os
from decimal import Decimal

from app.models.exchange_type import ExchangeType


def get_size_round_by_exchange(exchange_type: ExchangeType) -> int:
    if exchange_type == ExchangeType.PARADEX:
        return int(os.getenv("SIZE_ROUND"))
    elif exchange_type == ExchangeType.BACKPACK:
        return int(os.getenv("BACKPACK_SIZE_ROUND"))
    else:  # HIBACHI
        return int(os.getenv("HIBACHI_SIZE_ROUND"))


def get_market_min_order_size_by_exchange(exchange_type: ExchangeType) -> Decimal:
    if exchange_type == ExchangeType.PARADEX:
        return Decimal(os.getenv("MARKET_MIN_ORDER_SIZE"))
    elif exchange_type == ExchangeType.BACKPACK:
        return Decimal(os.getenv("BACKPACK_MARKET_MIN_ORDER_SIZE"))
    else:  # HIBACHI
        return Decimal(os.getenv("HIBACHI_MARKET_MIN_ORDER_SIZE"))


def get_price_step_by_exchange(exchange_type: ExchangeType) -> Decimal:
    if exchange_type == ExchangeType.PARADEX:
        return Decimal(os.getenv("PRICE_STEP"))
    elif exchange_type == ExchangeType.BACKPACK:
        return Decimal(os.getenv("BACKPACK_PRICE_STEP"))
    else:  # HIBACHI
        return Decimal(os.getenv("HIBACHI_PRICE_STEP"))
