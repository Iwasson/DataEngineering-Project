import re
import pandas as pd
import numpy as np
from urllib.request import urlopen
from bs4 import BeautifulSoup

url = "http://www.psudataeng.com:8000/getStopEvents"

def get_webdata(url: str) -> pd.DataFrame:
  """
  Gets all of the data from the HTML of a webpage
  """

  html = urlopen(url)
  soup = BeautifulSoup(html, 'html.parser')

  h1 = soup.find("h1")
  date = re.search(r'\d{4}-\d{2}-\d{2}', h1.string).group(0)
  
  dataframe = pd.DataFrame(columns=[
    "date",
    "event_number",
    "vehicle_number",
    "leave_time",
    "train",
    "route_number",
    "direction",
    "service_key",
    "trip_number",
    "stop_time",
    "arrive_time",
    "dwell",
    "location_id",
    "door",
    "lift",
    "ons",
    "offs",
    "estimated_load",
    "maximum_speed",
    "train_mileage",
    "pattern_distance",
    "location_distance",
    "x_coordinate",
    "y_coordinate",
    "data_source",
    "schedule_status"
  ])

  for header in soup.find_all("h2"):
    event_info = re.search(r'-?\d+', header.string).group(0)
    table = header.next_sibling

    for row in table.find_all("tr"):
      columns = row.find_all("td")

      if columns:
        event_Number      = event_info
        vehicle_number    = columns[0].string
        leave_time        = columns[1].string
        train             = columns[2].string
        route_number      = columns[3].string
        direction         = columns[4].string
        service_key       = columns[5].string
        trip_number       = columns[6].string
        stop_time         = columns[7].string
        arrive_time       = columns[8].string
        dwell             = columns[9].string
        location_id       = columns[10].string
        door              = columns[11].string
        lift              = columns[12].string
        ons               = columns[13].string
        offs              = columns[14].string
        estimated_load    = columns[15].string
        maximum_speed     = columns[16].string
        train_mileage     = columns[17].string
        pattern_distance  = columns[18].string
        location_distance = columns[19].string
        x_coordinate      = columns[20].string
        y_coordinate      = columns[21].string
        data_source       = columns[22].string
        schedule_status   = columns[23].string

        new_row = pd.DataFrame([{
          "date"              : date,
          "event_number"      : event_Number,
          "vehicle_number"    : vehicle_number,
          "leave_time"        : leave_time,
          "train"             : train,
          "route_number"      : route_number,
          "direction"         : direction,
          "service_key"       : service_key,
          "trip_number"       : trip_number,
          "stop_time"         : stop_time,
          "arrive_time"       : arrive_time,
          "dwell"             : dwell,
          "location_id"       : location_id,
          "door"              : door,
          "lift"              : lift,
          "ons"               : ons,
          "offs"              : offs,
          "estimated_load"    : estimated_load,
          "maximum_speed"     : maximum_speed,
          "train_mileage"     : train_mileage,
          "pattern_distance"  : pattern_distance,
          "location_distance" : location_distance,
          "x_coordinate"      : x_coordinate,
          "y_coordinate"      : y_coordinate,
          "data_source"       : data_source,
          "schedule_status"   : schedule_status
        }])

        dataframe = pd.concat([dataframe, new_row], ignore_index=True)
  
  return dataframe.to_dict('records')
