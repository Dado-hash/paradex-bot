import os
from decimal import Decimal

import pytest

from app.bots.parallel_market_maker_bot import get_unfilled_size
from app.models.data_position import DataPosition
from app.models.exchange_type import ExchangeType


@pytest.fixture(scope="module", autouse=True)
def mock_env():
    os.environ["DEFAULT_ORDER_SIZE"] = "100"
    os.environ["MARKET_MIN_ORDER_SIZE"] = "10"


def test_none_position():
    result = get_unfilled_size(None, ExchangeType.PARADEX)
    assert result == Decimal('100')


def test_positive_main_not_filled_size():
    position = DataPosition(size=Decimal(15))
    result = get_unfilled_size(position, ExchangeType.PARADEX)
    assert result == Decimal('85')


def test_negative_main_not_filled_size_over():
    position = DataPosition(size=Decimal(110))
    result = get_unfilled_size(position, ExchangeType.PARADEX)
    assert result == Decimal('-10')


def test_below_min_order_size_1():
    position = DataPosition(size=Decimal(95))
    result = get_unfilled_size(position, ExchangeType.PARADEX)
    assert result is None


def test_below_min_order_size_2():
    position = DataPosition(size=Decimal(91))
    result = get_unfilled_size(position, ExchangeType.PARADEX)
    assert result is None


def test_below_min_order_size_3():
    position = DataPosition(size=Decimal(100))
    result = get_unfilled_size(position, ExchangeType.PARADEX)
    assert result is None
