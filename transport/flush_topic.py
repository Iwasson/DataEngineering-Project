"""
Consumer whose only purpose is to consume all messages in a topic
for the purposes of debugging.
"""
import sys
from confluent_kafka import Consumer
from loguru import logger

import kafka_api as kafka

logger.remove()
logger.add(sys.stderr, level='INFO')

if __name__ == '__main__':
  config, args = kafka.parse_config(is_consumer=True)
  consumer = Consumer(config)

  # Subscribe to topic
  topic = "sensor-data"
  kafka.subscribe(topic, consumer, args)

  # Poll for new messages from Kafka and print them.
  data = []
  try:
    data = kafka.consume_events(topic, consumer, logger)
  except KeyboardInterrupt:
    pass
  finally:
    # Leave group and commit final offsets
    consumer.close()
    logger.info(f'Flushed {topic} of {len(data)} events')