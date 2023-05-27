"""
Primary Kafka consumer that collects single-record messages from
the sensor-data topic, organizing them into a list of dictionaries
which is then written to a file in the snapshots folder.

Snapshot filenames are derived from the date the consumer is run.

Give the --flush flag to flush the topic without storing the data.
"""
import sys
import os
import json
from datetime import datetime
from confluent_kafka import Consumer
from argparse import Namespace
from loguru import logger
from time import perf_counter
from confluent_kafka import OFFSET_BEGINNING, Consumer, Message

from producer import parse_config
from transform_breadcrumbs import transform_breadcrumbs
from transform_trips import transform_trips
from postgres import save_df_to_postgres
from postgres import save_trip_df_to_postgres

logger.remove()
logger.add(sys.stderr, level='INFO')


def subscribe(topic: str, consumer: Consumer, args: Namespace) -> None:
  """
  Subscribe to the topic

  Returns None
  """
  def reset_offset(consumer, partitions):
    if args.reset:
      for p in partitions:
        p.offset = OFFSET_BEGINNING
      consumer.assign(partitions)

  try:
    consumer.subscribe([topic], on_assign=reset_offset)
  except KeyError as exc:
    raise KeyError(f'Unable to subscribe to {topic}: {exc}')


def parse_row(msg: Message, topic: str) -> None:
  """
  Decode message, convert to dict

  Returns data
  """
  key = msg.key().decode('utf-8')
  value = msg.value().decode('utf-8')
  data = json.loads(value)
  logger.debug(f"Consumed event from topic {topic}: key = {key}")
  return data


def store_data(data: dict, file) -> None:
  """
  Store data in a snapshot file if any has been retrieved

  Returns None
  """
  if not data:
    logger.info(f'No data to store')
    return None

  json.dump(data, file)
  file.write("\n")
  logger.debug(f'data appended to {file.name}')


def consume_events(topic: str, consumer: Consumer) -> int:
  """
  Poll the topic, consuming events until an interrupt.
  Save captured data to a file unless flush flag given.

  Returns an int
  """
  failed_polls = 0
  start_time = perf_counter()
  try:
    data = []
    while True:
      msg = consumer.poll(1.0)
      if msg is None:
        logger.info('Waiting...')
        failed_polls += 1

        # Exit if consumer has finished reading messages
        if failed_polls >= 10:
          break
      elif msg.error():
        logger.error(f'ERROR: {msg.error()}')
      else:
        data.append(parse_row(msg, topic))

        # Send update messages every ~5 seconds
        end_time = perf_counter()
        if end_time - start_time > 5:
          logger.info(f'Number of events consumed: {len(data)}')
          start_time = perf_counter()
  except KeyboardInterrupt:
    pass
  finally:
    msg = f'{datetime.now()}: Consumed {len(data)} records from {topic}.\n'
    with open(f'{os.path.dirname(__file__)}/../log.txt', 'a') as log:
      log.write(msg)
    logger.info(msg)
    return data

if __name__ == '__main__':
  # Create Consumer instance
  config, args = parse_config(is_consumer=True)
  consumer = Consumer(config)

  # Subscribe to topic
  if args.trip:
    topic = 'trip-data'
  else: topic = 'sensor-data'
  subscribe(topic, consumer, args)

  # Consume events
  data = consume_events(topic, consumer)
  consumer.close()

  # Validate and transform data if not flushing
  if args.flush: logger.info('Flushed data.')
  elif topic == 'sensor-data':
    df = transform_breadcrumbs(data)
    save_df_to_postgres(df)
  else:
    df = transform_trips(data)
    save_trip_df_to_postgres(df)
