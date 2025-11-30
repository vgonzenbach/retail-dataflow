from __future__ import annotations
from typing import NamedTuple

from apache_beam import DoFn, pvalue

class InventoryEvent(NamedTuple):
    event_type: str
    inventory_id: str
    product_id: str
    warehouse_id: str
    quantity_change: int
    reason: str
    timestamp: str


class InventoryEventDQValidatorDoFn(DoFn):
    def process(self, event: InventoryEvent):
        errors = []

        valid_reasons = {'restock', 'sale', 'return', 'damage'}
        if event.reason not in valid_reasons:
            errors.append(f"Value of field 'reason' is not in set of valid reasons: {valid_reasons!r}.")
        
        if not event.quantity_change < 100 or not event.quantity_change > -100:
            errors.append(f"Value of field 'quantity_change' is not within range [-100, 100]")
        
        if errors:
            yield pvalue.TaggedOutput("invalid", {"errors": errors, "event": event._asdict()})
        else:
            yield event
        