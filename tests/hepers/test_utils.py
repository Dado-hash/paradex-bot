import os
from decimal import Decimal

import pytest

from app.helpers.utils import random_decimal, get_random_size
from app.models.exchange_type import ExchangeType


@pytest.fixture(scope="module", autouse=True)
def set_env():
    os.environ["SIZE_ROUND"] = "2"


def test_random_decimal():
    min_val = Decimal("1.5")
    max_val = Decimal("3.5")

    for _ in range(100):
        result = random_decimal(min_val, max_val)
        assert min_val <= result <= max_val, f"Value {result} out of range"


def test_get_random_size():
    os.environ["SIZE_ROUND"] = "2"
    min_size = Decimal("1.23")
    max_size = Decimal("5.67")

    for _ in range(100):
        result = get_random_size(min_size, max_size, ExchangeType.PARADEX)
        assert min_size <= result <= max_size, f"Value {result} out of range"
