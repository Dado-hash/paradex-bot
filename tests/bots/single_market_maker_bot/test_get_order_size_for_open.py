import os
from decimal import Decimal

import pytest

from app.bots.single_market_maker_bot import get_order_size_for_open
from app.models.data_position import DataPosition
from app.models.exchange_type import ExchangeType


@pytest.fixture(scope="module", autouse=True)
def mock_env():
    os.environ["MAX_ORDER_SIZE"] = "100"
    os.environ["MAX_POSITION_SIZE"] = "200"
    os.environ["MIN_ORDER_SIZE"] = "10"
    os.environ["MARKET_MIN_ORDER_SIZE"] = "5"
    os.environ["SIZE_ROUND"] = "0"


def test_no_positions():
    for _ in range(100):
        result = get_order_size_for_open(None, None, ExchangeType.PARADEX)
        assert Decimal("10") <= result <= Decimal("100")


def test_same_side_full_position():
    same_side_position = DataPosition(size=Decimal(196))
    result = get_order_size_for_open(same_side_position, None, ExchangeType.PARADEX)
    assert result is None


def test_other_side_full_position():
    other_side_position = DataPosition(size=Decimal(200))
    result = get_order_size_for_open(None, other_side_position, ExchangeType.PARADEX)
    assert result == Decimal(200)


def test_almost_full_position():
    same_side_position = DataPosition(size=Decimal(195))
    result = get_order_size_for_open(same_side_position, None, ExchangeType.PARADEX)
    assert result == Decimal("5")


def test_same_side_position():
    same_side_position = DataPosition(size=Decimal(50))
    for _ in range(100):
        result = get_order_size_for_open(same_side_position, None, ExchangeType.PARADEX)
        assert Decimal("10") <= result <= Decimal("100")


def test_other_side_position_1():
    other_side_position = DataPosition(size=Decimal(20))
    for _ in range(100):
        result = get_order_size_for_open(None, other_side_position, ExchangeType.PARADEX)
        assert Decimal("20") <= result <= Decimal("100")
