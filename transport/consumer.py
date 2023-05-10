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
from datetime import date
from confluent_kafka import Consumer
from argparse import Namespace
from loguru import logger
from time import perf_counter
from producer import parse_config
from confluent_kafka import OFFSET_BEGINNING, Consumer, Message


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


def parse_row(msg: Message, topic: str, logger) -> None:
  """
  Decode message, convert to dict

  Returns data
  """
  key = msg.key().decode('utf-8')
  value = msg.value().decode('utf-8')
  data = json.loads(value)
  logger.debug(f"Consumed event from topic {topic}: key = {key}")
  return data


def store_data(data: dict, file, logger) -> None:
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


def consume_events(topic: str, consumer: Consumer, flush: bool, logger) -> int:
  """
  Poll the topic, consuming events until an interrupt.
  Save captured data to a file unless flush flag given.

  Returns an int
  """
  data_count = 0
  failed_polls = 0
  start_time = perf_counter()
  try:
    # Create snapshot file
    if not flush:
      path = f'{os.path.dirname(__file__)}/../snapshots'
      file = f'{date.today()}.json'
      f = open(f'{path}/{file}', 'a')

    while True:
      msg = consumer.poll(1.0)
      if msg is None:
        logger.info('Waiting...')
        failed_polls += 1

        # Exit if consumer has failed 30 times
        if flush and failed_polls >= 30:
          break
      elif msg.error():
        logger.error(f'ERROR: {msg.error()}')
      else:
        # Only write to file if not flushing messages
        if not flush:
          store_data(parse_row(msg, topic, logger), f, logger)
        data_count += 1

        # Send update messages every ~5 seconds
        end_time = perf_counter()
        if end_time - start_time > 5:
          logger.info(f'Number of events consumed: {data_count}')
          start_time = perf_counter()
  except KeyboardInterrupt:
    pass
  # Close snapshot file and write to log
  finally:
    lf = open(f'{os.path.dirname(__file__)}/../log.txt', 'a')
    msg = f'{date.today()}: Consumed {data_count} records. '

    if not flush:
      f.close()
      msg += 'Written to file.\n'
    else: msg += 'Flushed.\n'

    logger.info(msg)
    lf.write(msg)
    lf.close()

if __name__ == '__main__':
  # Create Consumer instance
  config, args = parse_config(is_consumer=True)
  consumer = Consumer(config)

  # Subscribe to topic
  topic = 'sensor-data'
  subscribe(topic, consumer, args)

  # Consume events
  consume_events(topic, consumer, args.flush, logger)
  consumer.close()
