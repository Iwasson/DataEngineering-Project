"""
Primary Kafka producer that collects the JSON breadcrumb records
retrieved by the snapshot.py script and sends each record individually
to the sensor-data topic.

Note: Kafka can only queue 100000 events before its buffer needs to
be flushed.
"""
import sys
from confluent_kafka import Producer
from loguru import logger

from gather_data import gather_data
import kafka_api as kafka

logger.remove()
logger.add(sys.stderr, level='INFO')

if __name__ == '__main__':
  # Create Producer instance
  config, args = kafka.parse_config()
  producer = Producer(config)

  # Gather data from API
  topic = 'sensor-data'
  data = gather_data()

  # Produce events with Kafka
  count = kafka.produce_events(topic, data, producer, logger)
  logger.info(f'Size of original data: {len(data)}... Total records transmitted: {count}')
