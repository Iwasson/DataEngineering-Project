import pandas as pd
from typing import List

# Every record must have a timestamp
# Every record must have a vehicle ID
# Every record must have an odometer reading
# The location indicated by the longitude and latitude values must be in Oregon
# Every record with a particular trip ID must have matching vehicle IDs (assuming a “trip” is defined as a route taken by a single vehicle)
# The time at a stop must be between 0 and 86400
# The odometer reading should never be greater than 250000 miles
# A vehicle must not travel more than 2000 miles in a day
# The total distance traveled throughout a completed route must be less than or equal to the length between the two furthest points in Oregon
# There should be fewer than one thousand different vehicles

def validate(data: List[dict]):
    df = pd.DataFrame.from_records(data)
    print(df)
