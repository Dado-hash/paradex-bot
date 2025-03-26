from dataclasses import dataclass
from decimal import Decimal

from paradex_py.common.order import OrderSide


@dataclass
class DataOrder:
    id: str = None
    side: OrderSide = None
    price: Decimal = None
    size: Decimal = None

    def __getitem__(self, key):
        if hasattr(self, key):
            return getattr(self, key)
        else:
            return None
