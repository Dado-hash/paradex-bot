import os
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from app.exchanges.paradex import ParadexExchange
from app.helpers.orders import get_unrealized_pnl_percent
from app.models.position_side import PositionSide


@pytest.fixture(scope="module", autouse=True)
def mock_env():
    os.environ["MAX_LEVERAGE"] = "5"


@pytest.fixture
def mock_exchange():
    exchange = MagicMock(spec=ParadexExchange)
    exchange.buy_orders_list = [[Decimal('100')]]
    exchange.sell_orders_list = [[Decimal('90')]]
    return exchange


def test_get_unrealized_pnl_percent_long(mock_exchange):
    position = {
        "side": PositionSide.LONG.value,
        "average_entry_price": "80"
    }
    result = get_unrealized_pnl_percent(mock_exchange, position)
    expected = (Decimal('100') - Decimal('80')) / Decimal('80') * Decimal('5')
    assert result == expected


def test_get_unrealized_pnl_percent_short(mock_exchange):
    position = {
        "side": PositionSide.SHORT.value,
        "average_entry_price": "100"
    }
    result = get_unrealized_pnl_percent(mock_exchange, position)
    expected = (Decimal('100') - Decimal('90')) / Decimal('100') * Decimal('5')
    assert result == expected
