"""
Primary Kafka producer that collects the JSON breadcrumb records
retrieved by the snapshot.py script and sends each record individually
to the sensor-data topic.

Note: Kafka can only queue 100000 events before its buffer needs to
be flushed.
"""
import sys
import os
from datetime import date
from confluent_kafka import Producer
from loguru import logger

from snapshot import get_snapshot
import kafka_api as kafka

logger.remove()
logger.add(sys.stderr, level='INFO')

if __name__ == '__main__':
  # Create Producer instance
  config, args = kafka.parse_config()
  producer = Producer(config)

  # Gather data from API
  topic = 'sensor-data'
  data = get_snapshot()

  # Produce events with Kafka
  count = kafka.produce_events(topic, data, producer, logger)
  msg = f'{date.today()}: Size of original data: {len(data)}. Total records transmitted: {count}'
  logger.info(msg)
  with open(f'{os.path.dirname(__file__)}../log.txt', 'a') as f:
    f.write(msg)

