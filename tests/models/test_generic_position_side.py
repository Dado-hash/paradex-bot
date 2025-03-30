from enums.RequestEnums import OrderSide as BackPackOrderSide
from paradex_py.common.order import OrderSide as ParadexOrderSide

from app.models.exchange_type import ExchangeType
from app.models.generic_order_side import GenericOrderSide
from app.models.generic_position_side import PositionSideEnum, GenericPositionSide


def test_opposite_side_paradex():
    assert GenericPositionSide(PositionSideEnum.LONG.value[ExchangeType.PARADEX],
                               ExchangeType.PARADEX).opposite_side() == GenericPositionSide(
        PositionSideEnum.SHORT.value[ExchangeType.PARADEX], ExchangeType.PARADEX)
    assert GenericPositionSide(PositionSideEnum.SHORT.value[ExchangeType.PARADEX],
                               ExchangeType.PARADEX).opposite_side() == GenericPositionSide(
        PositionSideEnum.LONG.value[ExchangeType.PARADEX], ExchangeType.PARADEX)


def test_opposite_side_backpack():
    assert GenericPositionSide(PositionSideEnum.LONG.value[ExchangeType.BACKPACK],
                               ExchangeType.BACKPACK).opposite_side() == GenericPositionSide(
        PositionSideEnum.SHORT.value[ExchangeType.BACKPACK], ExchangeType.BACKPACK)
    assert GenericPositionSide(PositionSideEnum.SHORT.value[ExchangeType.BACKPACK],
                               ExchangeType.BACKPACK).opposite_side() == GenericPositionSide(
        PositionSideEnum.LONG.value[ExchangeType.BACKPACK], ExchangeType.BACKPACK)


def test_opposite_order_side_paradex():
    assert GenericPositionSide(PositionSideEnum.LONG.value[ExchangeType.PARADEX],
                               ExchangeType.PARADEX).opposite_order_side() == GenericOrderSide(
        ParadexOrderSide.Sell.value, ExchangeType.PARADEX)
    assert GenericPositionSide(PositionSideEnum.SHORT.value[ExchangeType.PARADEX],
                               ExchangeType.PARADEX).opposite_order_side() == GenericOrderSide(
        ParadexOrderSide.Buy.value, ExchangeType.PARADEX)


def test_opposite_order_side_backpack():
    assert GenericPositionSide(PositionSideEnum.LONG.value[ExchangeType.BACKPACK],
                               ExchangeType.BACKPACK).opposite_order_side() == GenericOrderSide(
        BackPackOrderSide.ASK.value, ExchangeType.BACKPACK)
    assert GenericPositionSide(PositionSideEnum.SHORT.value[ExchangeType.BACKPACK],
                               ExchangeType.BACKPACK).opposite_order_side() == GenericOrderSide(
        BackPackOrderSide.BID.value, ExchangeType.BACKPACK)


def test_is_same_side_with_order_paradex():
    assert GenericPositionSide(PositionSideEnum.LONG.value[ExchangeType.PARADEX],
                               ExchangeType.PARADEX).is_same_side_with_order(GenericOrderSide(
        ParadexOrderSide.Buy.value, ExchangeType.PARADEX))
    assert GenericPositionSide(PositionSideEnum.SHORT.value[ExchangeType.PARADEX],
                               ExchangeType.PARADEX).is_same_side_with_order(GenericOrderSide(
        ParadexOrderSide.Sell.value, ExchangeType.PARADEX))


def test_is_same_side_with_order_backpack():
    assert GenericPositionSide(PositionSideEnum.LONG.value[ExchangeType.BACKPACK],
                               ExchangeType.BACKPACK).is_same_side_with_order(GenericOrderSide(
        BackPackOrderSide.BID.value, ExchangeType.BACKPACK))
    assert GenericPositionSide(PositionSideEnum.SHORT.value[ExchangeType.BACKPACK],
                               ExchangeType.BACKPACK).is_same_side_with_order(GenericOrderSide(
        BackPackOrderSide.ASK.value, ExchangeType.BACKPACK))
