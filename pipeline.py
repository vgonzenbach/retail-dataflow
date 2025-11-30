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

from transforms.common import SplitAndCastEventsDoFn, WriteFactToBigQuery
from transforms.order import OrderEvent, OrderEventDQValidatorDoFn, FactOrderHeader, FactOrderItem
from transforms.inventory import InventoryEvent, InventoryEventDQValidatorDoFn, FactInventory

logging.getLogger().setLevel(logging.DEBUG)

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
            help='GCS Bucket to write results to.')
# set options
opts = PipelineOptions()
my_opts = opts.view_as(MyOptions)
opts.view_as(StandardOptions).streaming = True

def assign_event_time(ev: dict) -> TimestampedValue:
    timestamp: str = ev.get('order_date', None) or ev.get('timestamp', None)
    timestamp: datetime = datetime.fromisoformat(timestamp)
    timestamp: Timestamp = Timestamp.from_utc_datetime(timestamp)
    return TimestampedValue(ev, timestamp)

def build_output_destination(event):
    return

def name_file(window, pane, shard_index, total_shards, compression, destination):
    timestamp: datetime = window.start.to_utc_datetime()
    filepath = f"{destination}/{timestamp:%Y/%m/%d/%H/%M}"
    filename = f"{destination}_{timestamp:%Y%m%d%H%M}.json"
    return filepath + "/" + filename

def camelcase(snakecase: str) -> str:
    return "".join(part.capitalize() for part in snakecase.split("_"))

with beam.Pipeline(options=opts) as pipeline:

    events = (
        pipeline 
        | 'ReadPubSub' >> ReadFromPubSub(subscription=my_opts.input_subscription)
        | 'ParseJSON' >> beam.Map(lambda b: json.loads(b.decode('utf-8')))
        | 'TimestampEvent' >> beam.Map(assign_event_time)
        | 'WindowInto1Min' >> beam.WindowInto(FixedWindows(60))
    )
    # inventory, user_activity, unknown
    order, inventory, unknown = events | beam.ParDo(SplitAndCastEventsDoFn()).with_outputs('order', 'inventory', 'unknown')
    # hints:
    order: beam.PCollection[OrderEvent]
    inventory: beam.PCollection[InventoryEvent]
    
    #
    order, invalid_order = (
        order | beam.ParDo(OrderEventDQValidatorDoFn()).with_outputs('invalid', main='main')
    )
    inventory, invalid_inventory = (
        inventory | beam.ParDo(InventoryEventDQValidatorDoFn()).with_outputs('invalid', main='main')
    )
    # hints:
    order: beam.PCollection[OrderEvent]
    inventory: beam.PCollection[InventoryEvent]

    # write valid events to output/
    for event_type, event in [
        ('order', order)
        # TODO: add inventory, user_activity
    ]:
        tag = event_type.capitalize()
        (
        event
        | '{tag}ToText' >> beam.Map(json.dumps) #TODO use ToString.Element
        | '{tag}ToGCS' >> WriteToFiles(
            path=my_opts.output_gcs,
            destination='output',
            file_naming=name_file)
    )
    
    # create fact records for BQ
    fact_order_header: beam.PCollection[FactOrderHeader] = (
        order | "ToOrderHeaderFact" >> beam.Map(FactOrderHeader.from_event).with_output_types(FactOrderHeader) 
    )
    fact_order_item: beam.PCollection[FactOrderHeader] = (
        order | "ToOrderItemFact" >> beam.FlatMap(FactOrderItem.from_event).with_output_types(FactOrderItem)
    )
    fact_inventory: beam.PCollection[FactInventory] = (
        inventory | "ToInventoryFact" >>beam.Map(FactInventory.from_event).with_output_types(FactInventory)
    )

    # write to BQ
    for table, fact in [
        ('fact_order_header', fact_order_header,),
        ('fact_order_item', fact_order_item),
        ('fact_inventory', fact_inventory)
    ]:
        tag = camelcase(table)
        ( fact 
            | f"{tag}ToDict" >> beam.Map(lambda f: f._asdict()) 
            | f"{tag}ToBQ" >> WriteFactToBigQuery(table=f'events.{table}') 
        )
