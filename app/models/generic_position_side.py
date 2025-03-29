from enum import Enum
from typing import Any

from app.models.exchange_type import ExchangeType
from app.models.generic_order_side import GenericOrderSide, OrderSideEnum


class PositionSideEnum(Enum):
    LONG = {ExchangeType.PARADEX: "LONG", ExchangeType.BACKPACK: True}
    SHORT = {ExchangeType.PARADEX: "SHORT", ExchangeType.BACKPACK: False}


class GenericPositionSide:
    def __init__(self, value: Any | PositionSideEnum, exchange_type: ExchangeType):
        if isinstance(value, PositionSideEnum):
            self.exchange_type = exchange_type
            self.__side = value
        else:
            for side in PositionSideEnum:
                if value == side.value[exchange_type]:
                    self.exchange_type = exchange_type
                    self.__side = side
                    return
            raise ValueError(f"Unknown PositionSide: {value}")

    def __eq__(self, other):
        if isinstance(other, GenericPositionSide):
            return self.value == other.value
        elif isinstance(other, PositionSideEnum):
            return self.__side == other
        return False

    def __str__(self):
        return f"{self.exchange_type} - {self.value}"

    @property
    def value(self):
        return self.__side.value[self.exchange_type]

    def __get_object(self, side: PositionSideEnum):
        return GenericPositionSide(side.value[self.exchange_type], self.exchange_type)

    def opposite_side(self) -> "GenericPositionSide":
        return self.__get_object(PositionSideEnum.SHORT) if self.__side == PositionSideEnum.LONG else self.__get_object(
            PositionSideEnum.LONG)

    def opposite_order_side(self) -> "GenericOrderSide":
        return GenericOrderSide(OrderSideEnum.SELL,
                                self.exchange_type) if self.__side == PositionSideEnum.LONG else GenericOrderSide(
            OrderSideEnum.BUY, self.exchange_type)

    def is_same_side_with_order(self, order_side: GenericOrderSide):
        return True if (self.__side == PositionSideEnum.LONG and order_side == OrderSideEnum.BUY) or (
                self.__side == PositionSideEnum.SHORT and order_side == OrderSideEnum.SELL) else False
