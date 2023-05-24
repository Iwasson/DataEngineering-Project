from pandas import DataFrame
from loguru import logger
from typing import List

"""
- Fields needed
  trip_id: ...,
  route_id: ...,
  vehicle_id
  service_key: ...,
  direction:

- Validations
  - All needed fields are not null for a given record
  - Direction is either 0 or 1
  - Service key is [Weekday, Saturday, Sunday]
"""

USED_COLS = ["event_number", "vehicle_number", "route_number"]

def filter_invalid_fields(data: List[dict]) -> List[dict]:
  def is_invalid(row: dict) -> bool:
    valid = True

    for key in row.keys():
      if key in 

def transform_stops(data: List[dict]) -> DataFrame:
  valid_data = filter_invalid_fields(data)

