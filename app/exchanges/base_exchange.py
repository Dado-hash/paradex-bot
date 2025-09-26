from abc import ABC, abstractmethod
from decimal import Decimal
from typing import List, Tuple, Optional

from app.models.data_order import DataOrder
from app.models.data_position import DataPosition
from app.models.exchange_type import ExchangeType
from app.models.generic_order_side import GenericOrderSide


class BaseExchange(ABC):
    exchange_type: ExchangeType

    buy_orders_list: List[Tuple[Decimal, Decimal]] = []
    sell_orders_list: List[Tuple[Decimal, Decimal]] = []

    open_orders: List[DataOrder] = []
    open_positions: List[DataPosition] = []

    mark_price: Optional[Decimal] = None

    balance: Optional[Decimal] = None

    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    async def setup(self):
        pass

    @abstractmethod
    def modify_limit_order(self, order_id: str, order_side: GenericOrderSide, order_size: Decimal, price: Decimal,
                           is_reduce: bool = False) -> dict | None:
        pass

    @abstractmethod
    def open_limit_order(self, order_side: GenericOrderSide, order_size: Decimal, price: Decimal,
                         is_reduce: bool = False) -> dict | None:
        pass

    @abstractmethod
    def open_market_order(self, order_side: GenericOrderSide, order_size: Decimal, is_reduce: bool = False):
        pass

    @abstractmethod
    def cancel_all_orders(self) -> None:
        pass

    @abstractmethod
    def close_all_positions(self) -> None:
        pass

    @abstractmethod
    async def critical_close_all(self) -> None:
        pass
