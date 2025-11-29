from __future__ import annotations
from typing import Callable, Any
import logging
import json
from datetime import datetime

import apache_beam as beam
from apache_beam import window
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io import ReadFromPubSub
from apache_beam.pvalue import TaggedOutput
from apache_beam.io.fileio import WriteToFiles
from apache_beam.io.gcp.bigquery import BigQueryDisposition, WriteToBigQuery

from transforms.orders import SplitOrderDoFn
from transforms.common import SplitEventsByTypeDoFn

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
            required=False, # TODO: change
            help='Output file to write results to.')
# set options
opts = PipelineOptions()
my_opts = opts.view_as(MyOptions)
opts.view_as(StandardOptions).streaming = True

def build_gcs_destination(data):
    event = json.loads(data)
    event_type = event["event_type"]
    ts: str = event["order_date"] or event["timestamp"]
    ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    return f"{event_type}/{ts:%Y/%m/%d/%H/%M}/{event_type}_{ts:%Y%m%d%H%M}"

def debug_print(x):
    print(x)
    return x

with beam.Pipeline(options=opts) as pipeline:

    events = (
        pipeline 
        | 'Read' >> ReadFromPubSub(subscription=my_opts.input_subscription)
        | 'Decode' >> beam.Map(lambda b: b.decode('utf-8'))
    )

#    ( # side output into gcs
#        events_str
#        | 'WindowInto1Min' >> beam.WindowInto(window.FixedWindows(60)) 
#        | 'WriteToGCS' >> WriteToFiles(
#            path=my_opts.output_gcs, 
#            destination=build_gcs_destination,
#            file_naming=lambda window, pane, shard_index, total_shards, compression, destination: f"{destination}.json")
#    )

    # split event types into their own PCollection
    events_split: tuple[Any, dict] = (
        events
        | 'ToDict' >> beam.Map(json.loads)
        | 'SplitEventsByType' >> beam.ParDo(SplitEventsByTypeDoFn()).with_outputs('order')
    )
    # validate schema for each event type
    events_split | 'DebugEventSplit' >> beam.Map(debug_print)
    orders = events_split.order # TODO: add validation PTransform

    # split orders into header and items
    orders_split = orders | 'SplitOrderEvents' >> beam.ParDo(SplitOrderDoFn()).with_outputs('items', 'header')
    order_headers = orders_split.header
    order_items = orders_split.items

    order_headers | 'DebugHeaders' >> beam.Map(debug_print)
    order_items | 'DebugItems' >> beam.Map(debug_print)

        #| 'WriteToBQ' >> WriteToBigQuery(
        #    table='events.fact_order_header',
        #    write_disposition=BigQueryDisposition.WRITE_APPEND,
        #    create_disposition=BigQueryDisposition.CREATE_NEVER
        #)
