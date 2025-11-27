import argparse
import json
import logging
from google.cloud import pubsub_v1

logger = logging.getLogger(__file__)

def load_event(path: str) -> bytes:
    """Helper function to load specific events from a file."""
    with open(path, mode='rb') as f:
        logger.info('Reading json event from file...')
        event: dict = json.load(f)
        event: str = json.dumps(event)
        event: bytes = bytes(event, encoding='utf-8')
    return event

def publish_event(topic: str, data: bytes):
    with pubsub_v1.PublisherClient() as publisher:
        logger.info(f"Publishing event to topic '{topic}'...")
        future = publisher.publish(topic=topic, data=data)
        message_id = future.result()
        # TODO: try catch or case-switch
        logger.info(f"Published message ID: {message_id}")

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        required=True,
        help='Input file.'
    )
    parser.add_argument(
        '--topic',
        dest='topic',
        required=True,
        help='Pubsub Topic to which to publish.'
    )
    args  = parser.parse_args()
    event = load_event(args.input)
    publish_event(topic=args.topic, data=event)
