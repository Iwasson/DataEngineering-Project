"""
- Every record must have a timestamp
- Every record must have a vehicle ID
- Every record must have an odometer reading
- The location indicated by the longitude and latitude values must be in Portland
- Every record with a particular trip ID must have matching vehicle IDs
- The time at a stop must be between 0 and 86400
- The odometer reading should never be greater than 1000000 miles
- The speed of a vehicle must not exceed 100 miles an hour
- The total distance traveled throughout a completed trip must be less than or equal to Portland's approximate diameter
- The timestamp should be earlier than the current datetime
"""

import sys
import os
import json
from pandas import DataFrame
from typing import List
from loguru import logger
from datetime import datetime, timedelta

logger.remove()
logger.add(sys.stderr, level='INFO')

FILTERED_COLUMNS = ['GPS_SATELLITES', 'GPS_HDOP']
INDEX = ['EVENT_NO_TRIP', 'VEHICLE_ID']


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
    combos = df.index.unique()
    trip_ids = [ index[0] for index in combos ] 
    duplicates = [ id for id in trip_ids if trip_ids.count(id) > 1 ]

    # Remove all records associated with invalid trips 
    bad_indexes = []
    try:
        assert len(duplicates) == 0
    except AssertionError:
        bad_indexes = [ index for index in combos if index[0] in duplicates ]
        msg = f'Filtered rows with trip IDs that have more than one vehicle: {bad_indexes}'
        logger.info(msg)
        log_write(msg)

    # Max trip length in meters derived from Portland's approximate area
    max_trip_length = 19400

    if len(bad_indexes) > 0: df = df.drop(index=bad_indexes)
    return df
    

# def transform(data: List[dict]) -> DataFrame:
def transform() -> DataFrame:
    with open('test_data.json', 'r') as f:
        data: list = json.load(f)

    valid_data = filter_invalid_fields(data)
    msg = f'Filtered {len(data) - len(valid_data)} records that fail existence/range checks'
    logger.info(msg)            
    log_write(msg)

    valid_data.append({
        "EVENT_NO_TRIP": 231428692,
        "EVENT_NO_STOP": 231428706,
        "OPD_DATE": "04JAN2023:00:00:00",
        "VEHICLE_ID": 1234,
        "METERS": 26948,
        "ACT_TIME": 57151,
        "GPS_LONGITUDE": -122.73209,
        "GPS_LATITUDE": 45.487085,
        "GPS_SATELLITES": 12.0,
        "GPS_HDOP": 0.7
    })
    df = DataFrame.from_records(valid_data, index=INDEX, exclude=FILTERED_COLUMNS)
    df = filter_invalid_trips(df)

    # Calculate the change in meters for each trip and validate
    # Create and validate timestamps
    # Create and validate speed 

    return df
transform()