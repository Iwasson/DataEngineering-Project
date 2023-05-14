import sys, os
import psycopg2
from loguru import logger
from pandas import DataFrame, Timestamp, Series, concat
from dotenv import load_dotenv

load_dotenv()
POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASS = os.getenv('POSTGRES_PASS')

def connect():
  """
  Attempts to connect to a postgres database,
  returns the connection
  """
  connection = None
  try:
    connection = psycopg2.connect(
      database  = POSTGRES_USER,
      user      = POSTGRES_USER,
      password  = POSTGRES_PASS,
      host      = 'localhost',
      port      = '5432'
    )
  except (Exception, psycopg2.DatabaseError) as e:
    logger.error(e)
    sys.exit(1)
  return connection

def make_tables() -> None:
  """
  Constructs the postgres tables and their schemas
  if they do not already exist
  """
  conn = connect()
  cursor = conn.cursor()

  sql_service_type = "create type service_type as enum ('Weekday', 'Saturday', 'Sunday');"
  sql_tripdir_type = "create type tripdir_type as enum ('Out', 'Back');"

  sql_crumb = """
  CREATE TABLE IF NOT EXISTS BreadCrump (
    tstamp timestamp,
    latitude float,
    longitude float,
    speed float,
    trip_id integer,
    FOREIGN KEY (trip_id) REFERENCES Trip
  )
  """

  sql_trip = """
  CREATE TABLE IF NOT EXISTS Trip (
    trip_id integer,
    route_id integer,
    vehicle_id integer,
    service_key service_type,
    direction tripdir_type,
    PRIMARY KEY (trip_id)
  )
  """
  
  # attempt to create the types, this can fail so we will except it
  try:
    cursor.execute(sql_service_type)
    conn.commit()
  except (Exception, psycopg2.DatabaseError) as e:
    logger.warning(e)
    pass

  try:
    cursor.execute(sql_tripdir_type)
    conn.commit()
  except (Exception, psycopg2.DatabaseError) as e:
    logger.warning(e)
    pass

  # attempt to create the tables, the sql should prevent this from
  # duplicated tables, and no error should occur
  try:
    cursor.execute(sql_trip)
    cursor.execute(sql_crumb)
    conn.commit()
  except (Exception, psycopg2.DatabaseError) as e:
    logger.error(e)
    sys.exit(1)

def save_df_to_postgres(dataframe: DataFrame):
  """
  Saves a pandas dataframe to postgres. 
  """
  make_tables()

