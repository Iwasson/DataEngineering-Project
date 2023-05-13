"""
- Every record must have a timestamp
- Every record must have a vehicle ID
- Every record must have an odometer reading
- The location indicated by the longitude and latitude values must be in Portland
- Every record with a particular trip ID must have matching vehicle IDs
- The time at a stop must be between 0 and 86400
- The odometer reading should never be greater than 1000000 miles
- The speed of a vehicle must not exceed 100 miles an hour
- The distance traveled by a vehicle in a day must not exceed 2,300,000 meters (60mph for 24hours)
- The timestamp should be earlier than the current datetime
"""

import sys
import os
import json
from pandas import DataFrame, Timestamp, Series
from typing import List
from loguru import logger
from datetime import datetime, timedelta

logger.remove()
logger.add(sys.stderr, level='INFO')

FILTERED_COLUMNS = ['GPS_SATELLITES', 'GPS_HDOP']


def filter_invalid_fields(data: List[dict]) -> List[dict]:
    """
    Filter out all rows that are invalid and cannot be analyzed.

    Returns a list of dictionaries
    """
    def is_invalid(row: dict) -> bool:
        """
        Determine if the current row has values for all necessary columns and
        that those values are within range.

        Returns a bool
        """
        valid = True

        # If all required values exist
        for key in row.keys():
            if key not in FILTERED_COLUMNS and row[key] is None:
                logger.debug(f'None value found for {key} belonging to {row}')
                valid = False
                break

        # If values are within range 
        if valid and not (                                                      \
            row['GPS_LONGITUDE'] >= -124 and row['GPS_LONGITUDE'] <= -120 and   \
            row['GPS_LATITUDE'] >= 43 and row['GPS_LATITUDE'] <= 47 and         \
            row['METERS'] >= 0 and row['METERS'] <= 1000000 and                 \
            row['ACT_TIME'] >= 0 ):               
            logger.debug(f'Row has one or more values out of range: {row}')
            valid = False

        return valid
    return list(filter(lambda row: is_invalid(row), data))


def log_write(msg: str) -> None:
    """
    Write to log file located in root dir

    Returns None
    """
    with open(f'{os.path.dirname(__file__)}/../../log.txt', 'a') as log:
      log.write(msg + '\n')


def filter_invalid_trips(df: DataFrame) -> DataFrame:

    # Get distinct indexes and isolate find any that have have repeat trip IDs
    indexes = df.index.unique()
    trip_ids = [ index[0] for index in indexes ] 
    duplicates = [ id for id in trip_ids if trip_ids.count(id) > 1 ]

    # Remove all records associated with invalid trips 
    bad_indexes = []
    try:
        assert len(duplicates) == 0
    except AssertionError:
        bad_indexes = [ index for index in indexes if index[0] in duplicates ]
        df = df.drop(index=bad_indexes)
        msg = f'Filtered rows with trip IDs that have more than one vehicle: {bad_indexes}'
        logger.info(msg)
        log_write(msg)

    return df


def create_timestamps(df: DataFrame) -> DataFrame:
  timestamps = []

  def decode_time(row: Series) -> None:
    # Extract datetime info
    date = row['OPD_DATE'].split(':')[0]
    seconds = row['ACT_TIME']

    # Convert seconds to a time
    delta = timedelta(seconds=seconds)
    delta_seconds = timedelta(seconds=delta.seconds)

    # Create timestamp and append to list of timestamps
    timestamp = datetime.strptime(f'{date}:{str(delta_seconds)}', '%d%b%Y:%H:%M:%S')
    timestamp += timedelta(delta.days)
    timestamps.append(Timestamp(timestamp))    

    # Log invalid timestamps
    try:
        assert timestamp < datetime.now()
    except AssertionError:
        msg = f'WARNING: Future timestamp {timestamp} associated with {row}'
        logger.warning(msg)
        log_write(msg)

  # Map function across rows to collect timestamps
  df.apply(decode_time, axis=1)

  # Insert timestamp column and drop redundant columns
  df.insert(1, "Timestamp", timestamps, True)
  return df.drop(columns=['OPD_DATE', 'ACT_TIME'])


def create_speeds(df: DataFrame) -> DataFrame:
    # Divide df into trips
    # For each trip
        # If only one reading, set speed to 0
        # Otherwise, start with the second reading
        # calculate the speed and set the speed
        # if index == 1: set speed of index 0 to current speed
    indexes = df.index.unique()
    trips = []
    for index in indexes:
        trips.append(df.loc[index])
    print(len(trips))

# def transform(data: List[dict]) -> DataFrame:
def transform() -> DataFrame:
    with open('../snapshots/test_data.json', 'r') as f:
        data: list = json.load(f)

    valid_data = filter_invalid_fields(data)
    msg = f'Filtered {len(data) - len(valid_data)} records that fail existence/range checks'
    logger.info(msg)            
    log_write(msg)

    df = DataFrame.from_records(
        valid_data,
        index=['EVENT_NO_TRIP', 'VEHICLE_ID'], 
        exclude=FILTERED_COLUMNS).sort_index()
    df = filter_invalid_trips(df)
    df = create_timestamps(df)
    df = create_speeds(df)
    # print(df)

    return df
transform()