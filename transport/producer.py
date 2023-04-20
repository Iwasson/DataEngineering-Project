"""
Primary Kafka producer that collects the JSON breadcrumb records
retrieved by the snapshot.py script and sends each record individually
to the sensor-data topic.

Note: Kafka can only produce 100000 events before its buffer needs to
be flushed.
"""
import sys
import json
from confluent_kafka import Producer
from loguru import logger

import kafka_api as kafka

logger.remove()
logger.add(sys.stderr, level='INFO')

if __name__ == '__main__':
  # Create Producer instance
  config, args = kafka.parse_config()
  producer = Producer(config)

  topic = 'sensor-data'
  # data = get_snapshot()

  with open('../snapshots/trimet_test_data.json', 'r') as f:
    data = json.load(f)

  count = kafka.produce_events(topic, data, producer, logger)
  logger.info(f'Size of original data: {len(data)}... Total records transmitted: {count}')