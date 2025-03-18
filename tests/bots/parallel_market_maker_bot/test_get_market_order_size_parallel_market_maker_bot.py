from decimal import Decimal
from unittest.mock import patch

from app.bots.parallel_market_maker_bot import get_market_order_size


def test_get_market_order_size_for_open_no_positions():
    result = get_market_order_size(
        None,
        None
    )
    assert result == (None, None)


@patch("time.time", return_value=1700000000)
def test_get_market_order_size_for_open_only_main_position(mock_time):
    main_position = {"size": "5", "created_at": "1700000000000"}
    result = get_market_order_size(
        main_position,
        None
    )
    assert result == (Decimal('5'), True)


@patch("time.time", return_value=1700000000)
def test_get_market_order_size_for_open_both_positions(mock_time):
    main_position = {"size": "4", "created_at": "1700000000000"}
    other_position = {"size": "6", "created_at": "1700000000000"}
    result = get_market_order_size(
        main_position,
        other_position
    )
    assert result == (Decimal('2'), False)  # 6 - 4 = 2


@patch("time.time", return_value=1700000000)
def test_get_market_order_size_for_open_time_other_to_close(mock_time):
    main_position = {"size": "7", "created_at": "1699996300000"}
    other_position = {"size": "3", "created_at": "1699996400001"}
    result = get_market_order_size(
        main_position,
        other_position
    )
    assert result == (Decimal('4'), True)  # 7 - 3 = 4


@patch("time.time", return_value=1700000000)
def test_get_market_order_size_for_open_time_main_to_close(mock_time):
    main_position = {"size": "5", "created_at": "1699996400001"}
    other_position = {"size": "8", "created_at": "1699996300000"}
    result = get_market_order_size(
        main_position,
        other_position
    )
    assert result == (Decimal('3'), False)  # 8 - 5 = 3
