from decimal import Decimal
from unittest.mock import patch

import pytest
from paradex_py.common.order import OrderSide

from paradex_api import Api


@pytest.mark.parametrize(
    "side, order_size, order_price, pre_max_buy_price, max_buy_price, pre_last_buy_size, last_buy_size, min_sell_price, expected",
    [
        # Проверяем случай, когда ордер уже на первом месте и единственный
        (OrderSide.Buy, Decimal('1.0'), Decimal('100'), Decimal('99'), Decimal('100'), Decimal('1.0'),
         Decimal('1.0'), Decimal('105'), Decimal('100')),
        # Проверяем случай, когда есть разрыв между первым и вторым ордером
        (
                OrderSide.Buy, Decimal('1.0'), Decimal('99'), Decimal('99'), Decimal('102'), Decimal('1.0'),
                Decimal('1.5'),
                Decimal('105'), Decimal('100')),
        # Проверяем случай, когда нельзя пересекать спред
        (OrderSide.Buy, Decimal('1.0'), Decimal('100'), Decimal('99'), Decimal('100'), Decimal('1.0'),
         Decimal('1.0'), Decimal('100.5'), Decimal('100')),
        # Проверяем sell-логику с аналогичными кейсами
        (OrderSide.Sell, Decimal('1.0'), Decimal('105'), Decimal('106'), Decimal('105'), Decimal('1.0'),
         Decimal('1.0'), Decimal('105'), Decimal('105')),
        (OrderSide.Sell, Decimal('1.0'), Decimal('106'), Decimal('106'), Decimal('103'), Decimal('1.0'),
         Decimal('1.5'), Decimal('100'), Decimal('105')),
        (OrderSide.Sell, Decimal('1.0'), Decimal('105'), Decimal('106'), Decimal('105'), Decimal('1.0'),
         Decimal('1.0'), Decimal('100.5'), Decimal('105')),
    ]
)
def test_get_best_price(
        side, order_size, order_price, pre_max_buy_price, max_buy_price, pre_last_buy_size, last_buy_size,
        min_sell_price, expected
):
    with patch.object(Api, 'pre_max_buy_price', pre_max_buy_price), \
            patch.object(Api, 'max_buy_price', max_buy_price), \
            patch.object(Api, 'pre_last_buy_size', pre_last_buy_size), \
            patch.object(Api, 'last_buy_size', last_buy_size), \
            patch.object(Api, 'min_sell_price', min_sell_price), \
            patch('os.getenv', return_value='0.1'):
        result = Api.get_best_price(side, order_size, order_price)
        assert result == expected
