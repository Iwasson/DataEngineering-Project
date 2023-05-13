"""
- Every record must have a timestamp
- Every record must have a vehicle ID
- Every record must have an odometer reading
- The location indicated by the longitude and latitude values must be in Portland
- Every record with a particular trip ID must have matching vehicle IDs (assuming a “trip” is defined as a route taken by a single vehicle)
- The time at a stop must be between 0 and 86400
- The odometer reading should never be greater than 250000 miles
- A vehicle must not travel more than 2000 miles in a day
- The total distance traveled throughout a completed route must be less than or equal to the length between the two furthest points in Oregon
- There should be fewer than one thousand different vehicles
"""

import sys
import pandas as pd
from typing import List
from loguru import logger

logger.remove()
logger.add(sys.stderr, level='INFO')

def filter_invalid(data: List[dict]) -> List[dict]:
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
            if key not in ['GPS_SATELLITES', 'GPS_HDOP'] and row[key] is None:
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

def validate(data: List[dict]):
    valid_data = filter_invalid(data)
    logger.info(f'Filtered {len(data) - len(valid_data)} records')            
    df = pd.DataFrame.from_records(valid_data)
    print(df)
