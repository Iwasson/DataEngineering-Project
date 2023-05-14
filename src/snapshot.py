"""
This script will automatically retrieve data from 
http://www.psudataeng.com:8000/getBreadCrumbData

It will then parse this JSON data and add it to a SQLite3 DB 
in an effort to conserve disk space.
"""
import sys
import os
import json
import requests
import sqlite3
from datetime import date
from loguru import logger
from typing import List

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
    tupe = tuple(row.values())
    sql_data.append(tupe)

  return sql_data


def store_data(parsed_data: json) -> None:
  """
  Takes in a json object and stores that data in a sqlite3 DB
  Returns None
  """
  path = f"{os.path.dirname(__file__)}/../../archive/{date.today()}.db"
  conn = sqlite3.connect(path)
  cur = conn.cursor()

  # Prevent duplicate values when running producer more than once in a day
  cur.execute("""DROP TABLE IF EXISTS trimet""")
  create_sql = """CREATE TABLE trimet (EVENT_NO_TRIP integer, 
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

  logger.info(f"Writing data to {path}")
  for row in parsed_data:
     cur.execute(insert_sql, row)
  conn.commit()


def get_snapshot():
  url = "http://www.psudataeng.com:8000/getBreadCrumbData"
  logger.info(f"Getting data from: {url}")
  data = download_data(url)
  logger.info(f"Got {len(data)} number of entries...")
  logger.info("Parsing data...")
  sql_data = parse_data(data)
  #store_data(sql_data)
  logger.info(f"Done!")
  return data
