"""
Primary Kafka producer that collects the JSON breadcrumb records
retrieved by the snapshot.py script and sends each record individually
to the sensor-data topic.

Note: Kafka can only queue 100000 events before its buffer needs to
be flushed.
"""
import sys
import json
import os
from datetime import datetime
from confluent_kafka import Producer
from argparse import ArgumentParser
from configparser import ConfigParser
from confluent_kafka import Producer, Message
from loguru import logger
from typing import Tuple, List

from webscraper import get_webdata
from snapshot import get_snapshot

logger.remove()
logger.add(sys.stderr, level='INFO')


def parse_config(is_consumer: bool = False) -> Tuple:
  """
  Parse the command line for configs to create a consumer

  Returns a tuple
  """
  parser = ArgumentParser()
  parser.add_argument('--reset', action='store_true')
  parser.add_argument('--flush', action='store_true')
  parser.add_argument('--trip', action='store_true')
  args = parser.parse_args()

  # Parse the configuration.
  # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
  config_parser = ConfigParser()
  with open(f'{os.path.dirname(__file__)}/kafka.ini', 'r') as config_file:
    config_parser.read_file(config_file)
  config = dict(config_parser['default'])

  if is_consumer: config.update(config_parser['consumer'])
  return config, args


def produce_events(topic: str, data: List[dict], producer: Producer, logger) -> int:
  """
  Transmit each record to the topic, filling up the producer queue
  and flushing when it's filled. Send update messages about every
  five seconds.

  Returns an int
  """
  
  def delivery_callback(err: str, msg: Message) -> None:
    """
    Per-message delivery callback (triggered by poll() or flush())
    when a message has been successfully delivered or permanently
    failed delivery (after retries).

    Returns None
    """
    if err:
      logger.error(f'ERROR: Message failed delivery: {err}')
    else:
      logger.debug(f'Produced event to topic {topic}: key = {msg.key()}')

  buffer_size = 100000
  count = 0 

  # Transmit each record individually to the topic
  for row in data:
    if topic == 'sensor-data':
      key = f'{row["VEHICLE_ID"]} | {row["OPD_DATE"]}'
    else: key = f'{row["date"]} | {row["event_number"]}' 

    producer.produce(topic, json.dumps(row), key, callback=delivery_callback)
    count += 1
    buffer_size -= 1

    # Flush the buffer to prevent overflow
    if buffer_size == 0:
      producer.poll(buffer_size)
      producer.flush()
      buffer_size = 100000
      logger.info(f'Flushing buffer. Number of records transmitted: {count}')

  if count > 0:
    # Block until the messages are sent.
    producer.poll(buffer_size)
    producer.flush()
  return count


if __name__ == '__main__':
  # Create Producer instance
  config, args = parse_config()
  producer = Producer(config)

  # Gather data from API
  if args.trip:
    topic = 'trip-data'
    data = get_webdata()
  else:
    topic = 'sensor-data'
    data = get_snapshot()

  # Produce events with Kafka
  count = produce_events(topic, data, producer, logger)
  msg = f'{datetime.now()}: Size of original data: {len(data)}. Total records transmitted to {topic}: {count}\n'
  logger.info(msg)
  with open(f'{os.path.dirname(__file__)}/../log.txt', 'a') as f:
    f.write(msg)
