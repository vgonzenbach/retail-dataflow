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

from transforms.orders import SplitOrderDoFn
from transforms.common import SplitEventsByTypeDoFn, WriteFactToBigQuery

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

# TODO: remove
#def build_gcs_destination(event):
#    ts: str = event["order_date"] or event["timestamp"]
#    ts = datetime.fromisoformat(ts)
#    return f"{event_type}/{ts:%Y/%m/%d/%H/%M}/{event_type}_{ts:%Y%m%d%H%M}.json"

def make_file_namer(destination: str):
    ...

def name_file(window, pane, shard_index, total_shards, compression, destination):
    print(f"{window=!r}")
    timestamp: datetime = window.start.to_utc_datetime()
    print(f"{timestamp=!r}")
    filepath = f"{destination}/{timestamp:%Y/%m/%d/%H/%M}"
    filename = f"{destination}_{timestamp:%Y%m%d%H%M}.json"
    return filepath + "/" + filename

def debug_print(x):
    print(x)
    return x

with beam.Pipeline(options=opts) as pipeline:

    events = (
        pipeline 
        | 'Read' >> ReadFromPubSub(subscription=my_opts.input_subscription)
        | 'Decode' >> beam.Map(lambda b: b.decode('utf-8'))
        | 'ToDict' >> beam.Map(json.loads)
        | 'Times' >> beam.Map(assign_event_time)
        | 'WindowInto1Min' >> beam.WindowInto(FixedWindows(60)) 
        | 'SplitEventsByType' >> beam.ParDo(SplitEventsByTypeDoFn()).with_outputs('order', 'invalid', 'unknown_types')
    )

    # --- write events to GCS staging layer ---
    events_named = [
        ('order', events.order)
        # ("inventory", events.inventory),
        # ("user_activity", events.user_activity),
    ]
    for name, ev in events_named:
        tag = name.capitalize()
        (
            ev
            | f'Stringify{tag}' >> beam.Map(json.dumps)
            | f'Debug{tag}' >> beam.Map(debug_print)
            | f'Write{tag}ToGCS' >> WriteToFiles(
                path=f"{my_opts.output_gcs}/output/{name}/",
                destination=lambda _: name,
                file_naming=name_file)
        )
    # --- write errors to GCS ---


    # split orders into header and items
    orders_split = events.order | 'SplitOrderEvents' >> beam.ParDo(SplitOrderDoFn()).with_outputs('items', 'header')
    order_headers = orders_split.header
    order_items = orders_split.items

    order_headers | 'WriteOrderHeaderToBQ' >> WriteFactToBigQuery(table='events.fact_order_header')
    order_items | 'WriteOrderItemsToBQ' >> WriteFactToBigQuery(table='events.fact_order_items')
