from __future__ import annotations
from typing import Callable, Any
import logging
import json
from datetime import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.window import FixedWindows, TimestampedValue
from apache_beam.utils.timestamp import Timestamp

from apache_beam.io import ReadFromPubSub
from apache_beam.io.fileio import WriteToFiles

from transforms.common import SplitEventsByTypeDoFn, WriteFactToBigQuery
from transforms.order import OrderEvent, FactOrderHeader, FactOrderItem

logging.getLogger().setLevel(logging.DEBUG)

KNOWN_TYPES = ('order', 'inventory', 'user_activity', 'unknown')

class MyOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--input_subscription',
            dest='input_subscription',
            help='Input PubSub subscription to pull from.')
        parser.add_argument(
            '--output_gcs',
            dest='output_gcs',
            required=True,
            help='Output file to write results to.')
# set options
opts = PipelineOptions()
my_opts = opts.view_as(MyOptions)
opts.view_as(StandardOptions).streaming = True

def assign_event_time(ev: dict) -> TimestampedValue:
    timestamp: str = ev['order_date'] or ev['timestamp']
    timestamp: datetime = datetime.fromisoformat(timestamp)
    timestamp: Timestamp = Timestamp.from_utc_datetime(timestamp)
    return TimestampedValue(ev, timestamp)

def name_file(window, pane, shard_index, total_shards, compression, destination):
    timestamp: datetime = window.start.to_utc_datetime()
    filepath = f"{destination}/{timestamp:%Y/%m/%d/%H/%M}"
    filename = f"{destination}_{timestamp:%Y%m%d%H%M}.json"
    return filepath + "/" + filename

def partition_by_type(event, num_partitions):
    T = event.get("event_type")
    if T in KNOWN_TYPES[:-1]:
        return KNOWN_TYPES.index(T)
    return len(KNOWN_TYPES) - 1

def camelcase(snakecase: str) -> str:
    return "".join(part.capitalize() for part in snakecase.split("_"))

with beam.Pipeline(options=opts) as pipeline:

    events = (
        pipeline 
        | 'ReadPubSub' >> ReadFromPubSub(subscription=my_opts.input_subscription)
        | 'ParseJSON' >> beam.Map(lambda b: json.loads(b.decode('utf-8')))
        | 'TimestampEvent' >> beam.Map(assign_event_time)
    )

#    ( # write raw events to GCS
#        events
#        | 'WindowInto1Min' >> beam.WindowInto(FixedWindows(60))
#        | 'ToText' >> beam.Map(json.dumps) #TODO use ToString.Element
#        | 'WriteToGCS' >> WriteToFiles(
#            path=my_opts.output_gcs,
#            destination=lambda ev: json.loads(ev)['event_type'],
#            file_naming=name_file)
#    )
    # TEST
    # events | beam.Map(debug_print)
    order, inventory, user_activity, unknown = events | beam.Partition(partition_by_type, len(KNOWN_TYPES))
        #| beam.ParDo(SplitEventsByTypeDoFn()).with_outputs('inventory', 'user_activity', main='order')

    # split events by type for later validation + ingestion
    # events_split = events | 'SplitByType' >> beam.ParDo(SplitEventsByTypeDoFn()).with_outputs('order') # TODO: output other types + unknown
    
    # TEST
    order: beam.PCollection[OrderEvent] = order | beam.Map(lambda ev: OrderEvent(**ev)).with_output_types(OrderEvent)

    fact_order_header: beam.PCollection[FactOrderHeader] = order | beam.Map(FactOrderHeader.from_event).with_output_types(FactOrderHeader) 
    fact_order_item: beam.PCollection[FactOrderHeader] = order | beam.FlatMap(FactOrderItem.from_event).with_output_types(FactOrderItem)

    #order_split = (
    #    order
    #    | ''
    #    | 'SplitOrderEvents' >> beam.ParDo(SplitOrderDoFn()).with_outputs('items', 'header')
    #)
    #order_header, order_items = order_split.header, order_split.items

    for table, pcoll in [
        ('fact_order_header', fact_order_header,),
        ('fact_order_item', fact_order_item),
    ]:
        tag = camelcase(table)
        ( pcoll 
            | f"{tag}ToDict" >> beam.Map(lambda f: f.to_dict()) 
            | f"{tag}ToBQ" >> WriteFactToBigQuery(table=f'events.{table}') 
        )


    #    pcoll | f"{tag}ToDict" >> beam.Map(lambda f: f.to_dict()) 
    #    | 'WriteOrderHeaderToBQ' >> WriteFactToBigQuery(table='events.fact_order_header')
    #fact_order_item | "ToItemDict" >> beam.Map(lamda)
    # order_items | 'WriteOrderItemsToBQ' >> WriteFactToBigQuery(table='events.fact_order_items')