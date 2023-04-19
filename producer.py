import json
from time import sleep
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
    # data = get_snapshot()
    with open('snapshots/trimet_data.json', 'r') as f:
       data = json.load(f)

    # Compress each value and send to topic
    # for key in data.keys():
      # value = json.dumps(data[key])
      # comp_val = zlib.compress(value.encode())
    buffer_size = 100000
    count = 0 
    for row in data:
      key = f'{row["VEHICLE_ID"]} | {row["OPD_DATE"]}'
      producer.produce(topic, json.dumps(row), key, callback=delivery_callback)
      count += 1

      # Flush the buffer to prevent overflow
      if count >= buffer_size:
        producer.poll(buffer_size)
        producer.flush()
        logger.info('Maximum buffer reached, flushing buffer')
        count = 0

    # for i in range(10):
    #   key = f'{data[i]["VEHICLE_ID"]} | {data[i]["OPD_DATE"]}'
    #   producer.produce(topic, json.dumps(data[i]), key, callback=delivery_callback)

      
    # Block until the messages are sent.
    producer.poll(buffer_size)
    producer.flush()
