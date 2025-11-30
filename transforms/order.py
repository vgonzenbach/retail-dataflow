from __future__ import annotations
from copy import deepcopy
from datetime import datetime, timezone
from typing import NamedTuple, List
from decimal import Decimal

from apache_beam import DoFn

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
    # N. B. Not a field (do NOT annotate)
    VALID_STATES = {'pending', 'processing', 'shipped', 'delivered'}
        
    def validate(self):
        errors = []
        if self.status not in self.VALID_STATES:
            errors.append(f"Value of field 'status' is not a member of OrderEvent.VALID_STATES={self.VALID_STATES!r}.")

        if self.total_amount != sum(item['price'] * item['quantity'] for item in self.items):
            errors.append(f"Value of field 'total_amount' != sum(price * quantity) for all items.")

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

class ToFactOrderItemDoFn(DoFn):
    """
    Splits order events into header and items. 
    """
    def process(self, element: dict):
        """
        For Items:
        - add order_id from header
        - add order_date from header
        """
        event: dict = deepcopy(element) # inmutability
        items = event['items']
        for item in items:
            order_dt: datetime = datetime.fromisoformat(event['order_date']) # even
            order_ts: str = order_dt.isoformat()
            order_date: str = order_dt.date().isoformat()
            item_row = item | {
                'order_id': event['order_id'],
                'order_date': order_date,
                'order_ts': order_ts,
                'quantity': order_ts,
                'price'
                'total_amount': int(item['quantity']) * Decimal(item['price']) # add total_amount ($) of the line item
            }
            yield item_row

