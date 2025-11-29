from copy import deepcopy
from apache_beam import DoFn, pvalue

class SplitOrderDoFn(DoFn):
    """
    Splits order events into header and items. 
    """
    def process(self, element: dict):
        """
        For Header:
        - drop items array
        - flatten shipping_address
        """
        event: dict = deepcopy(element) # ensure inmutability
        # drop nested fields
        items = event.pop('items')
        shipping_address =  event.pop('shipping_address')
        # join flat shipping_address 
        shipping_address_flat = {f'shipping_address_{k}': v for k, v in shipping_address.items()}
        header_row = event | shipping_address_flat
        yield pvalue.TaggedOutput('header', header_row)

        """
        For Items:
        - add order_id from header
        - add order_date from header
        """
        for item in items:
            item_row = item | {
                'order_id': header_row['order_id'],
                'order_date': header_row['order_date']  
            }
            yield pvalue.TaggedOutput('items', item_row)