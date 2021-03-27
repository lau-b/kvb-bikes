import os
import requests
import pandas as pd
import pytz

from datetime import datetime
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path

## START PREPARATION ##
load_dotenv(f'{Path(__file__).parent}/.env')
tz = pytz.timezone('Europe/Berlin')
now = datetime.now(tz)

host = os.getenv('host')
user = os.getenv('user')
port = os.getenv('port')
db = os.getenv('db')
password = os.getenv('password')

connection_string = f'postgres://{user}:{password}@{host}:{port}/{db}'

engine = create_engine(connection_string)

stations_query = """INSERT INTO kvb_stations
                    SELECT
                        point_in_time,
                        bike_name,
                        longitude,
                        latitude,
                        bike_numbers
                    FROM kvb_import
                    WHERE bike_name NOT LIKE 'BIKE%%';"""

bikes_query = """INSERT INTO bikes
                 SELECT
                    point_in_time,
                    bike_name,
                    longitude,
                    latitude,
                    bike_numbers
                FROM kvb_import
                WHERE bike_name LIKE 'BIKE%%';"""

##  END PREPARATION #####

print(now, ': start scraping')
user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36'
headers = {'User-Agent': user_agent}
api_url = 'http://api.nextbike.net/maps/nextbike-live.xml?city=14'
resp = requests.get(api_url, headers=headers)

soup = BeautifulSoup(resp.text, 'xml')
df = pd.DataFrame(columns={
    'point_in_time': [],
    'bike_name': [],
    'longitude': [],
    'latitude': [],
    'bike_numbers': []
    })

for place in soup.findAll('place'):
    df = df.append({
        'point_in_time': now,
        'bike_name': place['name'],
        'longitude': place['lng'],
        'latitude': place['lat'],
        'bike_numbers': place['bike_numbers']
        },
        ignore_index=True)

# Writing data to database
df.to_sql('kvb_import', con=engine, if_exists='replace')
print(datetime.now(tz), ': data successfully written to database')


# Cleaning data in database
engine.execute(stations_query)
engine.execute(bikes_query)
print(datetime.now(tz), ': data cleaned')
print('------------------------------------------------------------------------')
