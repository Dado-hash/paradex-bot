from enums.RequestEnums import OrderSide as BackPackOrderSide
from paradex_py.common.order import OrderSide as ParadexOrderSide

from app.models.exchange_type import ExchangeType
from app.models.generic_order_side import GenericOrderSide, OrderSideEnum


def test_opposite_side_paradex():
    assert GenericOrderSide(ParadexOrderSide.Buy.value, ExchangeType.PARADEX).opposite_side() == GenericOrderSide(
        ParadexOrderSide.Sell.value, ExchangeType.PARADEX)
    assert GenericOrderSide(ParadexOrderSide.Sell.value, ExchangeType.PARADEX).opposite_side() == GenericOrderSide(
        ParadexOrderSide.Buy.value, ExchangeType.PARADEX)


def test_get_value_paradex():
    assert GenericOrderSide(ParadexOrderSide.Buy.value, ExchangeType.PARADEX).value == OrderSideEnum.BUY.value[
        ExchangeType.PARADEX]
    assert GenericOrderSide(ParadexOrderSide.Sell.value, ExchangeType.PARADEX).value == OrderSideEnum.SELL.value[
        ExchangeType.PARADEX]


def test_opposite_side_backpack():
    assert GenericOrderSide(BackPackOrderSide.BID.value, ExchangeType.BACKPACK).opposite_side() == GenericOrderSide(
        BackPackOrderSide.ASK.value, ExchangeType.BACKPACK)
    assert GenericOrderSide(BackPackOrderSide.ASK.value, ExchangeType.BACKPACK).opposite_side() == GenericOrderSide(
        BackPackOrderSide.BID.value, ExchangeType.BACKPACK)


def test_get_value_backpack():
    assert GenericOrderSide(BackPackOrderSide.BID.value, ExchangeType.BACKPACK).value == OrderSideEnum.BUY.value[
        ExchangeType.BACKPACK]
    assert GenericOrderSide(BackPackOrderSide.ASK.value, ExchangeType.BACKPACK).value == OrderSideEnum.SELL.value[
        ExchangeType.BACKPACK]
