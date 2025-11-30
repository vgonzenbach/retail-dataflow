from __future__ import annotations
from typing import Union, ClassVar, Sequence
from abc import abstractmethod
from copy import deepcopy
from apache_beam import DoFn, pvalue
from apache_beam.io.gcp.bigquery import BigQueryDisposition, WriteToBigQuery

from transforms.order import OrderEvent
 # deprecated
class SplitEventsByTypeDoFn(DoFn):
    KNOWN_EVENT_TYPES: ClassVar[Sequence[str]] = ('order', 'inventory', 'user_activity')
    """
    Extracts the event_type field from an event and splits into different outputs based on its value.
    """
    def process(self, element: dict):
        event: dict = deepcopy(element)
        event_type = event.pop('event_type', None)

        if event_type not in self.KNOWN_EVENT_TYPES:
            yield pvalue.TaggedOutput('unknown', {'error': f"'event_type' {event_type!r} is not known. Known event types: {self.KNOWN_EVENT_TYPES}", 'event': event})
            return
        print(f"{event_type=!r}")
        print(f"{event=!r}")
        yield pvalue.TaggedOutput(event_type, event)


class DQEvent(DoFn):
    def process(self, event: Union[OrderEvent, "InventoryEvent", "UserActivityEvent"]):
        errors = event.validate()
        if errors:
            yield pvalue.TaggedOutput(
                "invalid", {"errors": errors, "event": event._asdict()}
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
