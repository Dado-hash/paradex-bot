import os
from decimal import Decimal
from unittest.mock import patch

import pytest

from app.bots.parallel_market_maker_bot import get_limit_order_size
from app.models.data_position import DataPosition
from app.models.exchange_type import ExchangeType
from app.models.generic_order_side import GenericOrderSide, OrderSideEnum

paradex_order_side_buy = GenericOrderSide(OrderSideEnum.BUY, ExchangeType.PARADEX)
paradex_order_side_sell = GenericOrderSide(OrderSideEnum.SELL, ExchangeType.PARADEX)


@pytest.fixture(scope="module", autouse=True)
def set_env():
    os.environ["DEFAULT_ORDER_SIZE"] = "10"
    os.environ["MARKET_MIN_ORDER_SIZE"] = "5"
    os.environ["POSITION_TIME_THRESHOLD_SECONDS"] = "3600"


def test_get_limit_order_size_no_positions():
    result = get_limit_order_size(
        paradex_order_side_buy,
        None,
        None,
        ExchangeType.PARADEX
    )
    assert result == (Decimal('10'), paradex_order_side_buy)  # DEFAULT_ORDER_SIZE = 10


@patch("time.time", return_value=1700000000)
def test_get_limit_order_size_only_main_position(mock_time):
    main_position = DataPosition(size=Decimal(5), created_at="1700000000000")
    result = get_limit_order_size(
        paradex_order_side_buy,
        main_position,
        None,
        ExchangeType.PARADEX
    )
    assert result == (Decimal('5'), paradex_order_side_buy)  # 10 - 5 = 5


@patch("time.time", return_value=1700000000)
def test_get_limit_order_size_only_other_position(mock_time):
    other_position = DataPosition(size=Decimal(3), created_at="1700000000000")
    result = get_limit_order_size(
        paradex_order_side_sell,
        None,
        other_position,
        ExchangeType.PARADEX
    )
    assert result == (Decimal('10'), paradex_order_side_sell)  # DEFAULT_ORDER_SIZE = 10


@patch("time.time", return_value=1700000000)
def test_get_limit_order_size_both_positions(mock_time):
    main_position = DataPosition(size=Decimal(4), created_at="1700000000000")
    other_position = DataPosition(size=Decimal(6), created_at="1700000000000")
    result = get_limit_order_size(
        paradex_order_side_buy,
        main_position,
        other_position,
        ExchangeType.PARADEX
    )
    assert result == (Decimal('2'), paradex_order_side_buy)  # 6 - 4 = 2


@patch("time.time", return_value=1700000000)
def test_get_limit_order_size_time_other_to_close(mock_time):
    main_position = DataPosition(size=Decimal(7), created_at="1699996300000")
    other_position = DataPosition(size=Decimal(3), created_at="1699996400001")
    result = get_limit_order_size(
        paradex_order_side_sell,
        main_position,
        other_position,
        ExchangeType.PARADEX
    )
    assert result == (Decimal('7'), paradex_order_side_buy)  # Close the position


@patch("time.time", return_value=1700000000)
def test_get_limit_order_size_time_main_to_close_1(mock_time):
    main_position = DataPosition(size=Decimal(5), created_at="1699996400001")
    other_position = DataPosition(size=Decimal(8), created_at="1699996300000")
    result = get_limit_order_size(
        paradex_order_side_sell,
        main_position,
        other_position,
        ExchangeType.PARADEX
    )
    assert result == (Decimal('5'), paradex_order_side_buy)


@patch("time.time", return_value=1700000000)
def test_get_limit_order_size_time_main_to_close_2(mock_time):
    main_position = None
    other_position = DataPosition(size=Decimal(8), created_at="1699996300000")
    result = get_limit_order_size(
        paradex_order_side_sell,
        main_position,
        other_position,
        ExchangeType.PARADEX
    )
    assert result == (None, None)


@patch("time.time", return_value=1700000000)
def test_get_limit_order_size_unfilled_size_minus_1(mock_time):
    main_position = DataPosition(size=Decimal(12), created_at="1700000000000")
    other_position = None
    result = get_limit_order_size(
        paradex_order_side_sell,
        main_position,
        other_position,
        ExchangeType.PARADEX
    )
    assert result == (Decimal(2), paradex_order_side_buy)


@patch("time.time", return_value=1700000000)
def test_get_limit_order_size_unfilled_size_minus_2(mock_time):
    main_position = DataPosition(size=Decimal(12), created_at="1700000000000")
    other_position = DataPosition(size=Decimal(5), created_at="1700000000000")
    result = get_limit_order_size(
        paradex_order_side_sell,
        main_position,
        other_position,
        ExchangeType.PARADEX
    )
    assert result == (Decimal(2), paradex_order_side_buy)


@patch("time.time", return_value=1700000000)
def test_get_limit_order_size_unfilled_size_minus_3(mock_time):
    main_position = DataPosition(size=Decimal(12), created_at="1700000000000")
    other_position = DataPosition(size=Decimal(10), created_at="1700000000000")
    result = get_limit_order_size(
        paradex_order_side_sell,
        main_position,
        other_position,
        ExchangeType.PARADEX
    )
    assert result == (Decimal(2), paradex_order_side_buy)


@patch("time.time", return_value=1700000000)
def test_get_limit_order_size_main_unfilled_size_none(mock_time):
    main_position = DataPosition(size=Decimal(9), created_at="1700000000000")
    other_position = DataPosition(size=Decimal(5), created_at="1700000000000")
    result = get_limit_order_size(
        paradex_order_side_sell,
        main_position,
        other_position,
        ExchangeType.PARADEX
    )
    assert result == (None, None)
