# KVB Bikes in Cologne

Visualize the live location of the KVB bikes in Cologne.

## Exercises

- create a dataframe with columns `lat`, `lon`, `bike_number`, `name`
- use the `plotly` library to plot the live location of the bikes
- automatically update the map every 5 seconds

## Advanced Exercise

collect and visualize the live location of bikes over a period of several days!

- put the script on an ec2 server instance
- set up a cronjob that runs every 10 seconds
- send the results to a PostgreSQL database
- visualize the data in Metabase
- use the [Google Maps Directions API](https://developers.google.com/maps/documentation/directions/start) to calculate travel times and routes

## Hints

- examine the output of [this API](http://api.nextbike.net/maps/nextbike-live.xml?city=14) in your browser
- you can also get data for other cities by changing the `city` parameter in the url
- use the code example in `bikes.py` as a starting point
