import os
import random
from decimal import Decimal

from paradex_api import Api


def random_decimal(min_val: Decimal, max_val: Decimal) -> Decimal:
    return min_val + (max_val - min_val) * Decimal(random.uniform(0, 1))


def get_random_position_size(balance: Decimal) -> Decimal:
    min_size = Decimal(os.getenv("MIN_SIZE"))
    max_size = Decimal(os.getenv("SIZE_LIMIT"))
    size_round = int(os.getenv("SIZE_ROUND"))

    max_position = balance * Decimal("0.9") / Api.max_buy_price
    max_position = min(max_position, max_size)

    lower_bound = max(min_size, max_position / Decimal(2))
    upper_bound = max(min_size, max_position)

    random_size = Decimal(str(round(random_decimal(lower_bound, upper_bound), size_round)))

    return max(random_size, min_size)
