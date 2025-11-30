from __future__ import annotations
from typing import NamedTuple

class InventoryEvent(NamedTuple):
    event_type: str
    inventory_id: str
    product_id: str
    warehouse_id: str
    quantity_change: int
    reason: str
    timestamp: str
