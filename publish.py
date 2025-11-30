import argparse
import json
import logging
from google.cloud import pubsub_v1
from data.fake import make_order_event, make_inventory_event, make_invalid_order_event,make_invalid_inventory_event

logger = logging.getLogger(__file__)

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
        '--topic',
        dest='topic',
        required=True,
        help='Pubsub Topic to which to publish.'
    )
    args  = parser.parse_args()
    for make_event in (make_order_event, make_invalid_order_event, make_inventory_event, make_invalid_inventory_event):
        event: dict = make_event()
        event: str = json.dumps(event, indent=2)
        logger.info(event)
        event: bytes = bytes(event, encoding='utf-8')
        publish_event(topic=args.topic, data=event)
