import json
import sys
from time import perf_counter
from os import path
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Consumer, OFFSET_BEGINNING
from loguru import logger

logger.remove()
logger.add(sys.stderr, level='INFO')

if __name__ == '__main__':
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    parser.add_argument('--reset', action='store_true')
    args = parser.parse_args()

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])
    config.update(config_parser['consumer'])

    # Create Consumer instance
    consumer = Consumer(config)

    # Set up a callback to handle the '--reset' flag.
    def reset_offset(consumer, partitions):
        if args.reset:
            for p in partitions:
                p.offset = OFFSET_BEGINNING
            consumer.assign(partitions)

    # Subscribe to topic
    topic = 'sensor-data'
    try:
        consumer.subscribe([topic], on_assign=reset_offset)
    except KeyError as exc:
        raise KeyError(f'Unable to subscribe to {topic}: {exc}')

    # Poll for new messages from Kafka and print them.
    try:
        data = []
        start = perf_counter()
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                if len(data) > 0:
                    logger.info(f'Finished consuming events from {topic}. Storing data...')
                    break

                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                logger.info('Waiting...')
            elif msg.error():
                logger.error(f'ERROR: {msg.error()}')
            else:
                key = msg.key().decode('utf-8')
                value = msg.value().decode('utf-8')
                data.append(json.loads(value))

                end = perf_counter()
                logger.debug(f"Consumed event from topic {topic}: key = {key}")
                if end - start > 5:
                    logger.info(f'Number of events consumed: {len(data)}')
                    start = perf_counter()
    except KeyboardInterrupt:
        pass
    finally:
        if data != []:
            date = f'{data[0]["OPD_DATE"]}'
            with open(f'snapshots/{date}.json', 'w') as f:
                f.write(json.dumps(data))
        consumer.close()
        logger.info(f'Records consumed from {topic}: {len(data)}')