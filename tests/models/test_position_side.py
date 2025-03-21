from paradex_py.common.order import OrderSide

from app.models.position_side import PositionSide


def test_opposite_side():
    assert PositionSide.LONG.opposite_side() == PositionSide.SHORT
    assert PositionSide.SHORT.opposite_side() == PositionSide.LONG


def test_opposite_order_side():
    assert PositionSide.LONG.opposite_order_side() == OrderSide.Sell
    assert PositionSide.SHORT.opposite_order_side() == OrderSide.Buy


def test_is_same_side_with_order():
    assert PositionSide.LONG.is_same_side_with_order(OrderSide.Buy)
    assert not PositionSide.LONG.is_same_side_with_order(OrderSide.Sell)
    assert PositionSide.SHORT.is_same_side_with_order(OrderSide.Sell)
    assert not PositionSide.SHORT.is_same_side_with_order(OrderSide.Buy)
