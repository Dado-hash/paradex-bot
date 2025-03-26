import logging
from dataclasses import dataclass
from decimal import Decimal

from app.models.position_side import PositionSide


@dataclass
class DataPosition:
    id: str = None
    market: str = None
    size: Decimal = None
    side: PositionSide = None
    average_entry_price: Decimal = None
    created_at: str = None

    def __getitem__(self, key):
        if hasattr(self, key):
            return getattr(self, key)
        else:
            return None