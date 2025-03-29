import os
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from app.exchanges.paradex import ParadexExchange
from app.helpers.orders import get_unrealized_pnl_percent
from app.models.data_position import DataPosition
from app.models.exchange_type import ExchangeType
from app.models.generic_position_side import GenericPositionSide, PositionSideEnum


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
    position = DataPosition(side=GenericPositionSide(PositionSideEnum.LONG, ExchangeType.PARADEX),
                            average_entry_price=Decimal(80))
    result = get_unrealized_pnl_percent(mock_exchange, position)
    expected = (Decimal('100') - Decimal('80')) / Decimal('80') * Decimal('5')
    assert result == expected


def test_get_unrealized_pnl_percent_short(mock_exchange):
    position = DataPosition(side=GenericPositionSide(PositionSideEnum.SHORT, ExchangeType.PARADEX),
                            average_entry_price=Decimal(100))
    result = get_unrealized_pnl_percent(mock_exchange, position)
    expected = (Decimal('100') - Decimal('90')) / Decimal('100') * Decimal('5')
    assert result == expected
