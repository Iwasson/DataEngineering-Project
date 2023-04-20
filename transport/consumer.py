"""
Primary Kafka consumer that collects single-record messages from
the sensor-data topic, organizing them into a list of dictionaries
which is then written to a file in the snapshots folder.

Snapshot filenames are derived from the date the consumer is run.
"""
import json
import sys
from datetime import date
from confluent_kafka import Consumer
from loguru import logger

import kafka_api as kapi

logger.remove()
logger.add(sys.stderr, level='INFO')

if __name__ == '__main__':
  # Create Consumer instance
  config, args = kapi.parse_config(is_consumer=True)
  consumer = Consumer(config)

  # Subscribe to topic
  topic = 'sensor-data'
  kapi.subscribe(topic, consumer, args)

  # Consume events
  try:
    data = kapi.consume_events(topic, consumer, logger)
  except KeyboardInterrupt:
    pass
  finally:
    # Store data in snapshot file
    if data != []:
      with open(f'../snapshots/{date.today()}.json', 'w') as f:
        f.write(json.dumps(data))

    consumer.close()
    logger.info(f'Records consumed from {topic}: {len(data)}')
