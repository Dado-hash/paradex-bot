from enum import Enum
from typing import Any

from enums.RequestEnums import OrderSide as BackPackOrderSide
from paradex_py.common.order import OrderSide as ParadexOrderSide

from app.models.exchange_type import ExchangeType


class OrderSideEnum(Enum):
    BUY = {ExchangeType.PARADEX: ParadexOrderSide.Buy.value, ExchangeType.BACKPACK: BackPackOrderSide.BID.value, ExchangeType.HIBACHI: "BID"}
    SELL = {ExchangeType.PARADEX: ParadexOrderSide.Sell.value, ExchangeType.BACKPACK: BackPackOrderSide.ASK.value, ExchangeType.HIBACHI: "ASK"}


class GenericOrderSide:
    def __init__(self, value: Any | OrderSideEnum, exchange_type: ExchangeType):
        if isinstance(value, OrderSideEnum):
            self.exchange_type = exchange_type
            self.__side = value
        else:
            for side in OrderSideEnum:
                if value == side.value[exchange_type]:
                    self.exchange_type = exchange_type
                    self.__side = side
                    return
            raise ValueError(f"Unknown OrderSide: {value}")

    def __eq__(self, other):
        if isinstance(other, GenericOrderSide):
            return self.value == other.value
        elif isinstance(other, OrderSideEnum):
            return self.__side == other
        return False

    def __str__(self):
        return f"{self.exchange_type} - {self.value}"

    @property
    def value(self):
        return self.__side.value[self.exchange_type]

    def __get_object(self, side: OrderSideEnum):
        return GenericOrderSide(side.value[self.exchange_type], self.exchange_type)

    def opposite_side(self) -> "GenericOrderSide":
        return self.__get_object(OrderSideEnum.SELL) if self.__side == OrderSideEnum.BUY else self.__get_object(
            OrderSideEnum.BUY)
