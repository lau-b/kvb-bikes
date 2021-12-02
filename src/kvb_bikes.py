import requests
import pytz
from prefect import task, Flow
from prefect.schedules import IntervalSchedule
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from sqlalchemy import create_engine


@task
def get_live_data():
    url = 'http://api.nextbike.net/maps/nextbike-live.xml?city=14'
    user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36'
    headers = {'User-Agent': user_agent}

    print('fetching live kvb data...')
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'xml')

    live_kvb_data = []
    timestamp = datetime.now(pytz.timezone('Europe/Berlin'))
    for place in soup.findAll('place'):
        live_kvb_data.append({
            'point_in_time': timestamp,
            'name': place['name'],
            'lng': place['lng'],
            'lat': place['lat'],
            'numbers': place['bike_numbers']
        })

    return live_kvb_data


@task
def extract_stations(data):
    print('extract station data from kvb dataset...')
    stations = []

    for row in data:
        # if the first 4 letters of a bike name isn't BIKE, its a station
        if row['name'][:4] != 'BIKE':
            stations.append(row)

    return stations


@task
def extract_bikes(data):
    print('extract bike data from kvb dataset...')
    bikes = []

    for row in data:
        if row['name'][:4] == 'BIKE':
            bikes.append(row)

    return bikes


@task
def load_data(data, table):

    connection_string = 'postgresql://postgres:postgres@localhost:5432/kvb'
    engine = create_engine(connection_string)

    print(f'load data into {table} table...')
    for row in data:
        query = f'''
            INSERT INTO {table} (point_in_time, name, lng, lat, numbers)
            VALUES ('{row['point_in_time']}', '{row['name']}', {row['lng']}, {row['lat']}, '{row['numbers']}')
        '''

        engine.execute(query)


schedule = IntervalSchedule(
    start_date=datetime.now(pytz.timezone('Europe/Berlin')),
    interval=timedelta(minutes=1)
)


def main():
    with Flow('kvb-bikes', schedule=schedule) as flow:
        live_kvb_data = get_live_data()
        live_data_from_stations = extract_stations(live_kvb_data)
        live_data_from_bikes = extract_bikes(live_kvb_data)

        load_data(live_data_from_stations, 'prefect_stations')
        load_data(live_data_from_bikes, 'prefect_bikes')

    flow.run()


if __name__ == '__main__':
    main()
