from typing import ClassVar, Sequence
from copy import deepcopy
from apache_beam import DoFn, pvalue
from apache_beam.io.gcp.bigquery import BigQueryDisposition, WriteToBigQuery

class SplitEventsByTypeDoFn(DoFn):
    KNOWN_EVENT_TYPES: ClassVar[Sequence[str]] = ('order', 'inventory', 'user_activity')
    """
    Extracts the event_type field from an event and splits into different outputs based on its value.
    """
    def process(self, element: dict):
        event: dict = deepcopy(element)
        event_type = event.pop('event_type', None)

        if not event_type:
            yield pvalue.TaggedOutput('invalid', {'error': "Event does not contain required field 'event_type'", 'event': event})
            return 

        if event_type not in self.KNOWN_EVENT_TYPES:
            yield pvalue.TaggedOutput('unknown_type', {'error': f"'event_type' {event_type!r} is not known. Known event types: {self.KNOWN_EVENT_TYPES}", 'event': event})
            return

        yield pvalue.TaggedOutput(event_type, event)


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
