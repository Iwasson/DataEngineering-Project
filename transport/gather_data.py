"""
This script will automatically retrieve data from 
http://www.psudataeng.com:8000/getBreadCrumbData

It will then parse this JSON data and add it to a SQLite3 DB 
in an effort to conserve disk space.
"""
import sys
import json
import requests
import sqlite3
from loguru import logger
from typing import List, Tuple

def download_data(url: str) -> json:
  """
  Uses requests to get data from a URL
  
  Returns a JSON object
  """
  try:
    data = requests.get(url)
    return data.json()
  except json.JSONDecodeError:
      logger.warning(f"Error Getting URL: {url}")
      logger.warning(f"Response: {data.text}")
      sys.exit(1)
   

def parse_data(data: json) -> List[tuple] :
  """
  Takes in the json data from the website and converts it
  into usable data for insertion into a DB and Kafka

  Returns a List of tuples
  """
  
  sql_data = []
  for row in data:
    tupe = sql_parse(row)
    sql_data.append(tupe)

  return sql_data


def sql_parse(row: dict) -> Tuple:
  """
  Convert non-string fields to the appropriate type and
  set empty fields to 0.

  Returns a tuple
  """

  cols = list(row.values())
  for index, col in enumerate(cols):
    if col == "":
        cols[index] = 0

  return (int(cols[0]),
        int(cols[1]),
        str(cols[2]),
        int(cols[3]),
        int(cols[4]),
        int(cols[5]),
        float(cols[6]),
        float(cols[7]),
        float(cols[8]),
        float(cols[9]))


def store_data(parsed_data: json) -> None:
  """
  Takes in a json object and stores that data in a sqlite3 DB
  Returns None
  """
  conn = sqlite3.connect("trimet.db")
  cur = conn.cursor()

  # creates a table if it does not exist
  create_sql = """CREATE TABLE IF NOT EXISTS trimet (EVENT_NO_TRIP integer, 
                                                    EVENT_NO_STOP integer, 
                                                    OPD_DATE text, 
                                                    VEHICLE_ID integer, 
                                                    METERS integer,
                                                    ACT_TIME integer, 
                                                    VELOCITY real, 
                                                    DIRECTION real, 
                                                    RADIO_QUALITY real, 
                                                    GPS_LONGITUDE real)
    """
  cur.execute(create_sql)
  
  insert_sql = """
  INSERT INTO trimet(EVENT_NO_TRIP, 
                    EVENT_NO_STOP, 
                    OPD_DATE, 
                    VEHICLE_ID, 
                    METERS,
                    ACT_TIME, 
                    VELOCITY, 
                    DIRECTION, 
                    RADIO_QUALITY, 
                    GPS_LONGITUDE)
  VALUES(?,?,?,?,?,?,?,?,?,?)
  """

  for row in parsed_data:
     cur.execute(insert_sql, row)
  conn.commit()


def gather_data():
  url = "http://www.psudataeng.com:8000/getBreadCrumbData"
  logger.info(f"Getting data from: {url}")
  data = download_data(url)
  logger.info(f"Got {len(data)} number of entries...")
  logger.info("Parsing data...")
  sql_data = parse_data(data)
  logger.info("Storing data in trimet.db...")
  store_data(sql_data)
  logger.info(f"Done!")
  return data
