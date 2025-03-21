import os
from decimal import Decimal

import pytest

from app.bots.single_market_maker_bot import get_order_size_for_open


@pytest.fixture(scope="module", autouse=True)
def mock_env():
    os.environ["MAX_ORDER_SIZE"] = "100"
    os.environ["MAX_POSITION_SIZE"] = "200"
    os.environ["MIN_ORDER_SIZE"] = "10"
    os.environ["MARKET_MIN_ORDER_SIZE"] = "5"
    os.environ["SIZE_ROUND"] = "0"


def test_no_positions():
    for _ in range(100):
        result = get_order_size_for_open(None, None)
        assert Decimal("10") <= result <= Decimal("100")


def test_same_side_full_position():
    same_side_position = {"size": "196"}
    result = get_order_size_for_open(same_side_position, None)
    assert result is None


def test_other_side_full_position():
    other_side_position = {"size": "200"}
    result = get_order_size_for_open(None, other_side_position)
    assert result == Decimal(200)


def test_almost_full_position():
    same_side_position = {"size": "195"}
    result = get_order_size_for_open(same_side_position, None)
    assert result == Decimal("5")


def test_same_side_position():
    same_side_position = {"size": "50"}
    for _ in range(100):
        result = get_order_size_for_open(same_side_position, None)
        assert Decimal("10") <= result <= Decimal("100")


def test_other_side_position_1():
    other_side_position = {"size": "20"}
    for _ in range(100):
        result = get_order_size_for_open(None, other_side_position)
        assert Decimal("20") <= result <= Decimal("100")
