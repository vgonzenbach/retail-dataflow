import argparse
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

from apache_beam.io import ReadFromPubSub


logging.getLogger().setLevel(logging.INFO)

class RetailPipelineOptions(PipelineOptions):
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
retail_pipeline_options = PipelineOptions().view_as(RetailPipelineOptions)
input_subscription, output_gcs = retail_pipeline_options.input_subscription, retail_pipeline_options.output_gcs
pipeline_options=PipelineOptions()
pipeline_options.view_as(StandardOptions).streaming = True

with beam.Pipeline(options=pipeline_options) as pipeline:

    # check topic or subscription
    events = (
        pipeline 
        | 'Read' >> ReadFromPubSub(subscription=input_subscription)
        | 'Decode' >> beam.Map(lambda b: b.decode('utf-8'))
        | 'PrintToStdout' >> beam.Map(print)
    )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)