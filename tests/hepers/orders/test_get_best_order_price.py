import os
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from app.exchanges.paradex import ParadexExchange
from app.helpers.orders import get_best_order_price
from app.models.exchange_type import ExchangeType
from app.models.generic_order_side import GenericOrderSide, OrderSideEnum

paradex_order_side_buy = GenericOrderSide(OrderSideEnum.BUY, ExchangeType.PARADEX)
paradex_order_side_sell = GenericOrderSide(OrderSideEnum.SELL, ExchangeType.PARADEX)


@pytest.fixture(scope="module", autouse=True)
def mock_env():
    os.environ["PRICE_STEP"] = "0.1"
    os.environ["MIN_MARK_PRICE_PRICE_GAPS"] = "3"


class TestBestOrderPriceDefault:
    @pytest.fixture
    def mock_exchange(self):
        exchange = MagicMock(spec=ParadexExchange)
        exchange.exchange_type = ExchangeType.PARADEX
        exchange.buy_orders_list = [
            (Decimal('100.0'), Decimal('1')),
            (Decimal('99.9'), Decimal('2')),
            (Decimal('99.8'), Decimal('3'))
        ]
        exchange.sell_orders_list = [
            (Decimal('101.0'), Decimal('1')),
            (Decimal('101.1'), Decimal('2')),
            (Decimal('101.2'), Decimal('3'))
        ]
        exchange.mark_price = Decimal(100.5)
        return exchange

    def test_get_best_order_price_buy(self, mock_exchange):
        result = get_best_order_price(mock_exchange, paradex_order_side_buy)
        assert result == Decimal('100.1')

    def test_get_best_order_price_sell(self, mock_exchange):
        result = get_best_order_price(mock_exchange, paradex_order_side_sell)
        assert result == Decimal('100.9')


class TestBestOrderMaxStepsGap:
    @pytest.fixture
    def mock_exchange(self):
        exchange = MagicMock(spec=ParadexExchange)
        exchange.exchange_type = ExchangeType.PARADEX
        exchange.buy_orders_list = [
            (Decimal('100.0'), Decimal('1')),
            (Decimal('99.5'), Decimal('2')),
            (Decimal('98.0'), Decimal('3'))
        ]
        exchange.sell_orders_list = [
            (Decimal('101.0'), Decimal('1')),
            (Decimal('101.1'), Decimal('2')),
            (Decimal('102.0'), Decimal('3'))
        ]
        exchange.mark_price = Decimal(100.5)
        return exchange

    def test_get_best_order_price_buy_1(self, mock_exchange):
        result = get_best_order_price(mock_exchange, paradex_order_side_buy, Decimal(3))
        assert result == Decimal('99.6')

    def test_get_best_order_price_buy_2(self, mock_exchange):
        result = get_best_order_price(mock_exchange, paradex_order_side_buy, Decimal(10))
        assert result == Decimal('98.1')

    def test_get_best_order_price_sell(self, mock_exchange):
        result = get_best_order_price(mock_exchange, paradex_order_side_sell, Decimal(3))
        assert result == Decimal('101.9')
