from copy import deepcopy
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import NamedTuple, List
from decimal import Decimal

from apache_beam import DoFn
#from apache_beam.coders import RowCoder
#from apache_beam.coders.registry import register_coder # register_coder(ShippingAdress, RowCoder)

# TODO: create status enum

class ShippingAddress(NamedTuple):
    street: str
    city: str
    country: str

class Item:
    product_id: str
    product_name: str
    quantity: int
    price: float

class OrderEvent(NamedTuple):
    event_type: str
    order_id: str
    order_date: str
    customer_id: str
    order_id: datetime
    status: str
    shipping_address: ShippingAddress
    items: List[Item]
    total_amount: float # not precise 

class ToFactOrderHeaderDoFn(DoFn):
    """
    Splits order events into header and items. 
    """
    def process(self, element: dict):
        """
        For Header:
        - drop items array
        - flatten shipping_address
        """
        event: dict = deepcopy(element) # inmutability
        # drop items array
        del event['items']
        # flatten shipping_address 
        shipping_address =  event.pop('shipping_address')
        shipping_address_flat = {f'shipping_address_{k}': v for k, v in shipping_address.items()}
        header_row = event | shipping_address_flat
        yield header_row

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

