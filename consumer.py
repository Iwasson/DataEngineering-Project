import json
import zlib
from os import path
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Consumer, OFFSET_BEGINNING
from loguru import logger

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
    consumer.subscribe([topic], on_assign=reset_offset)

    # Poll for new messages from Kafka and print them.
    try:
        # invalid_rows = 0
        data = []
        # invalid_data = {}
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                logger.info('Finished consumption, storing data locally')
                break
            elif msg.error():
                logger.error(f'ERROR: {msg.error()}')
            else:
                # Decompress and decode key-value pair
                key = msg.key().decode('utf-8')
                # comp_val = zlib.decompress(msg.value())
                value = msg.value().decode('utf-8')
                data.append(json.loads(value))

                # Keep valid and invalid data separate
                # if 'NOT_GIVEN' in key:
                #     invalid_data[key] = value
                #     invalid_rows += len(value)
                # else:
                #     date = key.split(' | ')[1]
                #     if date not in valid_data.keys():
                #         valid_data[date] = []
                #     valid_data[date].append(dict({key: value}))
                #     valid_rows += len(value)
                logger.debug(f"Consumed event from topic {topic}: key = {key}")
    except KeyboardInterrupt:
        pass
    finally:
        if len(data) > 0:
            date = f'{data[0]["OPD_DATE"]}'
            with open(f'snapshots/{date}.json', 'w') as f:
                f.write(json.dumps(data))
            logger.info(f'Records consumed from {topic}: {len(data)}')
        # Store valid data in file denoted by date
        # for date in valid_data.keys():
        #     file = f'{dir}/snapshots/{date}.json'
        #     with open(file, 'a') as fp:
        #         fp.write(json.dumps(valid_data, indent=4))
        # logger.info(f'Valid rows read from {topic}: {valid_rows}')

        # Collect all invalid data in one file
        # if invalid_data != {}:
        #     file = f'{dir}/snapshots/invalid_data.json'
        #     with open(file, 'a') as fp:
        #         fp.write(json.dumps(invalid_data, indent=4))
        # logger.info(f'Invalid rows read from {topic}: {invalid_rows}')

        # logger.info(f'Total rows read: {invalid_rows + valid_rows}')

        # Leave group and commit final offsets
        consumer.close()