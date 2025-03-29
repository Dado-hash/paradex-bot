from dataclasses import dataclass
from decimal import Decimal

from app.models.generic_order_side import GenericOrderSide


@dataclass
class DataOrder:
    id: str = None
    side: GenericOrderSide = None
    price: Decimal = None
    size: Decimal = None

    def __getitem__(self, key):
        if hasattr(self, key):
            return getattr(self, key)
        else:
            return None
