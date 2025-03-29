import random
import time
from decimal import Decimal
from typing import Any

from app.helpers.config import get_size_round_by_exchange
from app.models.exchange_type import ExchangeType


def random_decimal(min_val: Decimal, max_val: Decimal) -> Decimal:
    return min_val + (max_val - min_val) * Decimal(random.uniform(0, 1))


def get_random_size(min_size: Decimal, max_size: Decimal, exchange: ExchangeType) -> Decimal:
    size_round = get_size_round_by_exchange(exchange)

    random_size = Decimal(str(round(random_decimal(min_size, max_size), size_round)))

    return max(random_size, min_size)


def get_attribute(data: dict | Any | None, key: str):
    if data is None:
        return None
    return getattr(data, key, None) if hasattr(data, key) else data.get(key, None)


def get_time_now_milliseconds() -> str:
    return str(round(time.time() * 1000, 0))
