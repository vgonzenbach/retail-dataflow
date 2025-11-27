import argparse
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.runners.runner import PipelineResult

from apache_beam.io import ReadFromPubSub, WriteToText


class PrintElement(beam.DoFn):
    def process(self, element):
        print(element)
        yield element


def run(argv=None, save_main_session=True) -> PipelineResult:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        help='Input subscription to pull from.')
    parser.add_argument(
        '--output',
        dest='output',
        required=False, # TODO: change
        help='Output file to write results to.')
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view(StandardOptions).streaming = True
    pipeline = beam.Pipeline(options=pipeline_options)
    # check topic or subscription
    events = (
        pipeline 
        | 'Read' >> ReadFromPubSub(subscription=known_args.input)
        | 'Decode' >> beam.Map(lambda b: b.decode('utf-8'))
        | 'PrintToStdout' >> beam.ParDo(PrintElement())
    )

    result = pipeline.run()
    result.wait_until_finish()
    return result

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()