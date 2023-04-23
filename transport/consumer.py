"""
Primary Kafka consumer that collects single-record messages from
the sensor-data topic, organizing them into a list of dictionaries
which is then written to a file in the snapshots folder.

Snapshot filenames are derived from the date the consumer is run.

Give the --flush flag to flush the topic without storing the data.
"""
import sys
from confluent_kafka import Consumer
from loguru import logger

import kafka_api as kafka

logger.remove()
logger.add(sys.stderr, level='INFO')

if __name__ == '__main__':
  # Create Consumer instance
  config, args = kafka.parse_config(is_consumer=True)
  consumer = Consumer(config)

  # Subscribe to topic
  topic = 'sensor-data'
  kafka.subscribe(topic, consumer, args)

  # Consume events
  count = kafka.consume_events(topic, consumer, args.flush, logger)

  consumer.close()
  logger.info(f'Total rows consumed from {topic}: {count}')
