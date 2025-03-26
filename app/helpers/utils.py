import dataclasses
import os
import random
from decimal import Decimal
from typing import Any


def random_decimal(min_val: Decimal, max_val: Decimal) -> Decimal:
    return min_val + (max_val - min_val) * Decimal(random.uniform(0, 1))


def get_random_size(min_size: Decimal, max_size: Decimal) -> Decimal:
    size_round = int(os.getenv("SIZE_ROUND"))

    random_size = Decimal(str(round(random_decimal(min_size, max_size), size_round)))

    return max(random_size, min_size)


def get_attribute(data: dict | Any, key: str):
    return getattr(data, key, None) if hasattr(data, key) else data.get(key, None)
