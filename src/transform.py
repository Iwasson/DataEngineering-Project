import sys
import os
from pandas import DataFrame, Timestamp, Series, concat
from typing import List
from loguru import logger
from datetime import datetime, timedelta

logger.remove()
logger.add(sys.stderr, level="INFO")

FILTERED_COLUMNS = ["GPS_SATELLITES", "GPS_HDOP"]
INDEX = ["EVENT_NO_TRIP", "VEHICLE_ID"]


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
                logger.debug(f"None value found for {key} belonging to {row}")
                valid = False
                break

        # If values are within range
        if valid and not (
            row["GPS_LONGITUDE"] >= -124
            and row["GPS_LONGITUDE"] <= -120
            and row["GPS_LATITUDE"] >= 43
            and row["GPS_LATITUDE"] <= 47
            and row["METERS"] >= 0
            and row["METERS"] <= 1000000
            and row["ACT_TIME"] >= 0
        ):
            logger.debug(f"Row has one or more values out of range: {row}")
            valid = False

        return valid

    return list(filter(lambda row: is_invalid(row), data))


def log_write(msg: str) -> None:
    """
    Write to log file located in root dir

    Returns None
    """
    with open(f"{os.path.dirname(__file__)}/../log.txt", "a") as log:
        log.write(msg + "\n")


def filter_invalid_trips(df: DataFrame) -> DataFrame:
    """
    Remove any trips that have multiple vehicle IDs

    Returns a Dataframe
    """
    # Get distinct indexes and isolate find any that have have repeat trip IDs
    indexes = df.index.unique()
    trip_ids = [index[0] for index in indexes]
    duplicates = [id for id in trip_ids if trip_ids.count(id) > 1]

    # Remove all records associated with invalid trips
    bad_indexes = []
    try:
        assert len(duplicates) == 0
    except AssertionError:
        bad_indexes = [index for index in indexes if index[0] in duplicates]
        df = df.drop(index=bad_indexes)
        msg = f"Filtered rows with trip IDs that have more than one vehicle: {bad_indexes}"
        logger.info(msg)
        log_write(msg)

    return df


def create_timestamp_column(df: DataFrame) -> DataFrame:
    """
    Derive and validate the timestamp for every row. Insert the list of
    timestamps into a new dataframe column

    Returns a Dataframe
    """
    timestamps = []

    def decode_time(row: Series) -> None:
        """
        Create timestamp, adjust datetime if seconds is greater than the
        number of seconds in a day.
        """
        date = row["OPD_DATE"].split(":")[0]
        seconds = row["ACT_TIME"]

        # Convert seconds to a time
        delta = timedelta(seconds=seconds)
        delta_seconds = timedelta(seconds=delta.seconds)

        # Create timestamp and append to list of timestamps
        timestamp = datetime.strptime(f"{date}:{str(delta_seconds)}", "%d%b%Y:%H:%M:%S")
        timestamp += timedelta(delta.days)
        timestamps.append(Timestamp(timestamp))

        # Log invalid timestamps
        try:
            assert timestamp < datetime.now()
        except AssertionError:
            msg = f"WARNING: Future timestamp {timestamp} associated with {row}"
            logger.warning(msg)
            log_write(msg)

    # Map function across rows to collect timestamps
    df.apply(decode_time, axis=1)

    # Insert timestamp column and drop redundant columns
    df.insert(1, "Timestamp", timestamps, True)
    return df.drop(columns=["OPD_DATE"])


def calculate_speeds(trip_df: DataFrame) -> List[float]:
    """
    Calculate speed for every stop and validate each speed value, setting
    the first speed value to that of the second.

    Returns a list of floats
    """
    speeds = []
    for i in range(1, trip_df.shape[0]):
        curr = trip_df.iloc[i]
        prev = trip_df.iloc[i - 1]
        speed = (curr["METERS"] - prev["METERS"]) / (
            curr["ACT_TIME"] - prev["ACT_TIME"]
        )

        # Validate speed
        try:
            assert speed < 45
        except AssertionError:
            msg = f'WARNING: unusual speed {speed} m/s associated with {curr}'
            logger.warning(msg)
            log_write(msg)

        speeds.append(speed)
        if i == 1:
            speeds.append(speed)
    return speeds


def create_speed_column(df: DataFrame) -> DataFrame:
    """
    Divide dataframe by trip, calculate speed for every row, add
    speed column to each trip dataframe, join all dataframes.

    Returns a Dataframe
    """
    indexes = df.index.unique()
    trip_dfs: list[DataFrame] = [df.loc[index] for index in indexes]
    for trip in trip_dfs:
        trip.insert(2, "Speed", calculate_speeds(trip), True)
    df = concat(trip_dfs)
    return df.drop(columns=['ACT_TIME'])


def transform(data: List[dict]) -> DataFrame:
    """
    Validate and transform data into a Dataframe that fits our Postgres schema

    Returns a Dataframe
    """
    valid_data = filter_invalid_fields(data)
    msg = f"Filtered {len(data) - len(valid_data)} records that fail existence/range checks"
    logger.info(msg)
    log_write(msg)

    if len(valid_data) == 0:
        msg = "No data to store"
        logger.warning(msg)
        raise Exception(msg)

    df = DataFrame.from_records(
        valid_data, index=INDEX, exclude=FILTERED_COLUMNS
    ).sort_index()

    logger.info("Filtering invalid trips...")
    df = filter_invalid_trips(df)
    logger.info("Creating timestamps...")
    df = create_timestamp_column(df)
    logger.info("Creating speeds...")
    df = create_speed_column(df)

    msg = f"Total records filtered: {len(data)- df.shape[0]}"
    logger.info(msg)
    log_write(msg)

    return df
