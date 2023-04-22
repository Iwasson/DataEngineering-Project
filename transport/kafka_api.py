"""
Kafka API functionality common throughout producer & consumer files.
"""
import json
import os
from datetime import date
from time import perf_counter
from argparse import ArgumentParser, FileType, Namespace
from configparser import ConfigParser
from confluent_kafka import OFFSET_BEGINNING, Consumer, Producer, Message
from typing import List, Tuple


def parse_config(is_consumer: bool = False) -> Tuple:
  """
  Parse the command line for configs to create a consumer

  Returns a tuple
  """
  parser = ArgumentParser()
  parser.add_argument('config_file', type=FileType('r'))
  parser.add_argument('--reset', action='store_true')
  parser.add_argument('--flush', action='store_true')
  args = parser.parse_args()

  # Parse the configuration.
  # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
  config_parser = ConfigParser()
  config_parser.read_file(args.config_file)
  config = dict(config_parser['default'])
  if is_consumer: config.update(config_parser['consumer'])

  return config, args


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


def parse_row(msg: Message, topic: str, data: List[dict], logger) -> None:
  """
  Decode message, convert to json, append to dataset

  Returns None
  """
  key = msg.key().decode('utf-8')
  value = msg.value().decode('utf-8')
  data.append(json.loads(value))
  logger.debug(f"Consumed event from topic {topic}: key = {key}")


def store_data(data: List[dict], logger):
  """
  Store data in a snapshot file if any has been retrieved

  Returns an int
  """
  count = len(data)
  if count > 0:
    path = '../snapshots'
    file = f'{date.today()}.json'
    if not os.path.exists(path): os.makedirs(path)
  
    with open(f'{path}/{file}', 'w') as f:
      f.write(json.dumps(data))
    logger.info(f'{count} rows written to {file}')

  return count


def consume_events(topic: str, consumer: Consumer, flush: bool, logger) -> int:
  """
  Poll the topic, consuming events until an interrupt.
  Save captured data to a file unless flush flag given.

  Returns an int
  """
  data = []
  start = perf_counter()
  try:
    while True:
      msg = consumer.poll(1.0)
      if msg is None:
        # Initial message consumption may take up to
        # `session.timeout.ms` for the consumer group to
        # rebalance and start consuming
        logger.info('Waiting...')
      elif msg.error():
        logger.error(f'ERROR: {msg.error()}')
      else:
        parse_row(msg, topic, data, logger)

        # Send update messages every ~5 seconds
        end = perf_counter()
        if end - start > 5:
          logger.info(f'Number of events consumed: {len(data)}')
          start = perf_counter()
  except KeyboardInterrupt:
    pass
  finally:
    if flush: return len(data)
    return store_data(data, logger)


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
    key = f'{row["VEHICLE_ID"]} | {row["OPD_DATE"]}'
    producer.produce(topic, json.dumps(row), key, callback=delivery_callback)
    count += 1
    buffer_size -= 1

    # Flush the buffer to prevent overflow
    if buffer_size == 0:
      producer.poll(buffer_size)
      producer.flush()
      buffer_size = 100000
      logger.info(f'Flushing buffer. Number of records transmitted: {count}')

  # Block until the messages are sent.
  producer.poll(buffer_size)
  producer.flush()
  return count