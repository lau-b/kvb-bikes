import requests 
from bs4 import BeautifulSoup

api_url = 'http://api.nextbike.net/maps/nextbike-live.xml?city=14'
resp = requests.get(api_url)
soup = BeautifulSoup(resp.text, 'xml')
for place in soup.findAll('place'):
    print(place['lat'], place['lng'], place['name'])
