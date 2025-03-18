import os
from decimal import Decimal

import pytest

from app.bots.parallel_market_maker_bot import get_unfilled_size


@pytest.fixture(scope="module", autouse=True)
def mock_env():
    os.environ["DEFAULT_ORDER_SIZE"] = "100"
    os.environ["MARKET_MIN_ORDER_SIZE"] = "10"


def test_none_position():
    result = get_unfilled_size(None)
    assert result == Decimal('100')


def test_positive_main_not_filled_size():
    position = {'size': '15'}
    result = get_unfilled_size(position)
    assert result == Decimal('85')


def test_negative_main_not_filled_size_over():
    position = {'size': '110'}
    result = get_unfilled_size(position)
    assert result == Decimal('-10')


def test_below_min_order_size_1():
    position = {'size': '95'}
    result = get_unfilled_size(position)
    assert result is None


def test_below_min_order_size_2():
    position = {'size': '91'}
    result = get_unfilled_size(position)
    assert result is None


def test_below_min_order_size_3():
    position = {'size': '100'}
    result = get_unfilled_size(position)
    assert result is None
