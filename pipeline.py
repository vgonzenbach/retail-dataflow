import argparse
import logging
import json
from datetime import datetime

import apache_beam as beam
from apache_beam import window
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io import ReadFromPubSub
from apache_beam.io.fileio import WriteToFiles


logging.getLogger().setLevel(logging.INFO)


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
my_opts = PipelineOptions().view_as(MyOptions)
pipeline_options=PipelineOptions()
pipeline_options.view_as(StandardOptions).streaming = True

def build_destination(data):
    event = json.loads(data)
    event_type = event["event_type"]
    ts: str = event["order_date"] or event["timestamp"]
    ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    return f"{event_type}/{ts:%Y/%m/%d/%H/%M}/{event_type}_{ts:%Y%m%d%H%M}"

with beam.Pipeline(options=pipeline_options) as pipeline:

    events = (
        pipeline 
        | 'Read' >> ReadFromPubSub(subscription=my_opts.input_subscription)
        | 'Decode' >> beam.Map(lambda b: b.decode('utf-8'))
        | 'WindowInto1Min' >> beam.WindowInto(window.FixedWindows(60)) # preparing for WriteToFiles
    )

    events | 'WriteToGCS' >> WriteToFiles(
        path=my_opts.output_gcs, 
        destination=build_destination,
        file_naming=lambda window, pane, shard_index, total_shards, compression, destination: f"{destination}.json"
    )

    
    
