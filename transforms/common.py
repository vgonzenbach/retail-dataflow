from copy import deepcopy
from apache_beam import DoFn, pvalue

class SplitEventsByTypeDoFn(DoFn):
    """
    Extracts the event_type field from an event and splits into different outputs based on its value.
    """
    def process(self, element: dict):
        event: dict = deepcopy(element)
        event_type = event.pop('event_type')
        if event_type == 'order':
            yield pvalue.TaggedOutput('order', event)
