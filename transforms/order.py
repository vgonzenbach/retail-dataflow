from __future__ import annotations
from datetime import datetime
from typing import NamedTuple, List
from decimal import Decimal

from apache_beam import DoFn, pvalue

# EVENT Fields
class OrderShippingAddress(NamedTuple):
    street: str
    city: str
    country: str

class OrderItem:
    product_id: str
    product_name: str
    quantity: int
    price: Decimal

# Event schemas
class OrderEvent(NamedTuple):
    event_type: str
    order_id: str
    order_date: str
    customer_id: str
    status: str
    shipping_address: OrderShippingAddress
    items: List[OrderItem]
    total_amount: Decimal


class FactOrderHeader(NamedTuple):
    order_id: str
    order_date: str
    order_ts: str
    customer_id: str
    order_id: datetime
    status: str
    shipping_address_street: str
    shipping_address_city: str
    shipping_address_country: str
    total_amount: Decimal

    @staticmethod
    def from_event(ev: OrderEvent) -> FactOrderHeader:
        """
        For Header:
        - drop items array
        - flatten shipping_address
        """
        order_dt: datetime = datetime.fromisoformat(ev.order_date) # even
        order_ts: str = order_dt.isoformat()
        order_date: str = order_dt.date().isoformat()
        return FactOrderHeader(
            order_id                    =   ev.order_id,
            customer_id                 =   ev.customer_id,
            order_date                  =   order_date,
            order_ts                    =   order_ts,
            status                      =   ev.status,
            shipping_address_street     =   ev.shipping_address['street'],
            shipping_address_city       =   ev.shipping_address['city'],
            shipping_address_country    =   ev.shipping_address['country'],
            total_amount                =   ev.total_amount,
        )


class FactOrderItem(NamedTuple):
    order_id: str
    order_date: str
    order_ts: str
    product_id: str
    product_name: str
    quantity: int
    price: Decimal
    total_amount: Decimal

    @staticmethod
    def from_event(ev: OrderEvent) -> FactOrderItem:
        # TODO: extract common logic
        order_dt: datetime = datetime.fromisoformat(ev.order_date) # even
        order_ts: str = order_dt.isoformat()
        order_date: str = order_dt.date().isoformat()
    
        for item in ev.items: # item is the name of the field
            yield FactOrderItem(
                order_id        =   ev.order_id,
                order_date      =   order_date,
                order_ts        =   order_ts,
                product_id      =   item['product_id'],
                product_name    =   item['product_name'],
                quantity        =   item['quantity'],
                price           =   item['price'],
                total_amount    =   item['quantity'] * item['price']
            ) 

    def to_dict(self) -> dict:
        return self._asdict()

class OrderEventDQValidatorDoFn(DoFn):
    def process(self, event: OrderEvent):
            # N. B. Not a field (do NOT annotate)
        errors = []
        valid_states = {'pending', 'processing', 'shipped', 'delivered'}
        if event.status not in valid_states:
            errors.append(f"Value of field 'status' is not in set of valid states: {valid_states!r}.")

        computed_total = sum(item['price'] * item['quantity'] for item in event.items)
        if event.total_amount != computed_total:
            errors.append(f"Value of field 'total_amount' != sum(price * quantity) for all items.")

        if errors:
            yield pvalue.TaggedOutput("invalid", {"errors": errors, "event": event._asdict()})
        else:
            yield event
