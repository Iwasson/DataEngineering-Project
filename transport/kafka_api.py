import json
from time import perf_counter
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import OFFSET_BEGINNING

def parse_config(is_consumer = False):
  """
  Parse the command line for configs to create a consumer
  """
  parser = ArgumentParser()
  parser.add_argument('config_file', type=FileType('r'))
  parser.add_argument('--reset', action='store_true')
  args = parser.parse_args()

  # Parse the configuration.
  # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
  config_parser = ConfigParser()
  config_parser.read_file(args.config_file)
  config = dict(config_parser['default'])
  if is_consumer: config.update(config_parser['consumer'])

  return config, args

def subscribe(topic, consumer, args):

  def reset_offset(consumer, partitions):
    if args.reset:
      for p in partitions:
        p.offset = OFFSET_BEGINNING
      consumer.assign(partitions)

  topic = 'sensor-data'
  try:
    consumer.subscribe([topic], on_assign=reset_offset)
  except KeyError as exc:
    raise KeyError(f'Unable to subscribe to {topic}: {exc}')

def consume_events(topic, consumer, logger):
  data = []
  start = perf_counter()
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
      # Decode message, convert to json, append to dataset
      key = msg.key().decode('utf-8')
      value = msg.value().decode('utf-8')
      data.append(json.loads(value))
      logger.debug(f"Consumed event from topic {topic}: key = {key}")

      # Send update messages every ~5 seconds
      end = perf_counter()
      if end - start > 5:
        logger.info(f'Number of events consumed: {len(data)}')
        start = perf_counter()
  return data

def produce_events(topic, data, producer, logger):
  
  # Optional per-message delivery callback (triggered by poll() or flush())
  # when a message has been successfully delivered or permanently
  # failed delivery (after retries).
  def delivery_callback(err, msg):
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