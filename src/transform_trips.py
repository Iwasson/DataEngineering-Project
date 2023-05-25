import sys
from datetime import datetime
from pandas import DataFrame
from loguru import logger
from typing import List

logger.remove()
logger.add(sys.stderr, level="INFO")

def validate_and_transform(df: DataFrame) -> DataFrame:
  """
  Remove rows that have invalid column values and assign
  the service key based on the date

  Returns a dataframe
  """
  invalid_indexes = []
  for i in range(df.shape[0]):
    row = df.iloc[i]

    try:
      int(row['event_number'])
      int(row['vehicle_number'])
      int(row['route_number'])
      assert row['direction'] in ['0', '1']
    except:
      invalid_indexes.append(i)
      continue
      
    # Use the date to account for holidays or invalid data in the
    # service_key column
    day = datetime.strptime(row['date'], "%Y-%m-%d").weekday()
    if day == 5:
      row['service_key'] = 'Saturday'
    elif day == 6:
      row['service_key'] = 'Sunday'
    else:
      row['service_key'] = 'Weekday'
    
  return df.drop(invalid_indexes, axis=0).drop(columns=["date"])
    

def transform_trips(data: List[dict]) -> DataFrame:
  """
  Create a dataframe from trip data, validate, and transform the
  dataframe for storage.

  Returns a dataframe
  """
  columns = [
    "event_number",
    "vehicle_number",
    "route_number",
    "service_key",
    "direction",
    "date"
  ]

  df = DataFrame.from_records(data, columns=columns)
  df = validate_and_transform(df)
  return df

