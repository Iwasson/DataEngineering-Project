import json
import zlib
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer
from snapshot import get_snapshot
from loguru import logger

if __name__ == '__main__':
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    args = parser.parse_args()

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])

    # Create Producer instance
    producer = Producer(config)

    # Optional per-message delivery callback (triggered by poll() or flush())
    # when a message has been successfully delivered or permanently
    # failed delivery (after retries).
    def delivery_callback(err, msg):
      if err:
        logger.error(f'ERROR: Message failed delivery: {err}')
      else:
        logger.debug(f'Produced event to topic {topic}: key = {msg.key()}')

    topic = 'sensor-data'
    data = get_snapshot()

    # Compress each value and send to topic
    for key in data.keys():
      value = json.dumps(data[key])
      comp_val = zlib.compress(value.encode())

      producer.produce(topic, comp_val, key, callback=delivery_callback)
      
    # Block until the messages are sent.
    producer.poll(10000)
    producer.flush()
