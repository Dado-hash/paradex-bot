from enum import Enum

from paradex_py.common.order import OrderSide


class PositionSide(Enum):
    LONG = "LONG"
    SHORT = "SHORT"

    def opposite_side(self) -> "PositionSide":
        return PositionSide.SHORT if self == PositionSide.LONG else PositionSide.LONG

    def opposite_order_side(self) -> "OrderSide":
        return OrderSide.Sell if self == PositionSide.LONG else OrderSide.Buy

    def is_same_side_with_order(self, order_side: OrderSide):
        return True if (self == PositionSide.LONG and order_side == OrderSide.Buy) or (
                self == PositionSide.SHORT and order_side == OrderSide.Sell) else False
