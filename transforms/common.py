from __future__ import annotations
from typing import Union, ClassVar, Sequence
from apache_beam import DoFn, pvalue
from apache_beam.io.gcp.bigquery import BigQueryDisposition, WriteToBigQuery

from transforms.order import OrderEvent
 # deprecated
class SplitAndCastEventsDoFn(DoFn):
    """
    Extracts the event_type field from an event and splits into different outputs based on its value.
    """
    def process(self, event: dict):
        event_type = event.get('event_type', None)

        if event_type == 'order':
            yield pvalue.TaggedOutput("order", OrderEvent(**event))
        """ TODO: Implement additional types
        elif event_type == 'inventory':
            yield pvalue.TaggedOutput("inventory", InventoryEvent(**event))

        elif event_type == 'user_activity':
            yield pvalue.TaggedOutput("user_activity", UserActivityEvent(**event))
        """
        yield pvalue.TaggedOutput(
            "unknown", 
            {
                "error": {
                    "reason": "unknown", 
                    "errors": ["Value of 'event_type' is unknown."]
                }, 
                "event": event
            }
        )


class EventDQValidatorDoFn(DoFn):
    def process(self, event: Union[OrderEvent, "InventoryEvent", "UserActivityEvent"]):
        errors = event.validate()
        if errors:
            yield pvalue.TaggedOutput(
                "invalid", 
                {
                    "error": {
                        "reason": "invalid", 
                        "errors": errors
                    }, 
                    "event": event._asdict()
                }
            )
        else:
            yield event

class WriteFactToBigQuery(WriteToBigQuery):
    """
    Wrapper for configuring write to BigQuery.
    """
    def __init__(self, table: str):
        super().__init__(
            table=table,
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            create_disposition=BigQueryDisposition.CREATE_NEVER
        )
