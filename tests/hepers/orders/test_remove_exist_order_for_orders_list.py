from decimal import Decimal

from app.helpers.orders import remove_exist_order_for_orders_list


def test_remove_existing_order():
    orders = [(Decimal('1.0'), Decimal('10.0')), (Decimal('2.0'), Decimal('20.0'))]
    exist_order = (Decimal('1.0'), Decimal('5.0'))
    expected = [(Decimal('1.0'), Decimal('5.0')), (Decimal('2.0'), Decimal('20.0'))]
    result = remove_exist_order_for_orders_list(orders, exist_order)
    assert result == expected


def test_remove_non_existing_order():
    orders = [(Decimal('1.0'), Decimal('10.0')), (Decimal('2.0'), Decimal('20.0'))]
    exist_order = (Decimal('3.0'), Decimal('5.0'))
    expected = orders
    result = remove_exist_order_for_orders_list(orders, exist_order)
    assert result == expected


def test_remove_order_with_zero_quantity():
    orders = [(Decimal('1.0'), Decimal('10.0')), (Decimal('2.0'), Decimal('20.0'))]
    exist_order = (Decimal('1.0'), Decimal('10.0'))
    expected = [(Decimal('2.0'), Decimal('20.0'))]
    result = remove_exist_order_for_orders_list(orders, exist_order)
    assert result == expected


def test_exist_order_is_none():
    orders = [(Decimal('1.0'), Decimal('10.0')), (Decimal('2.0'), Decimal('20.0'))]
    exist_order = None
    expected = orders
    result = remove_exist_order_for_orders_list(orders, exist_order)
    assert result == expected
