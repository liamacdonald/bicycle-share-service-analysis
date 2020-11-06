from fitparse import FitFile
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
from datetime import timedelta
from datetime import time
import glob
import requests
import json
import zipfile
import os
import math
from sodapy import Socrata

df_list = []

## This function downloads a file using a url
def download_url(url, save_path, chunk_size=128):
    r = requests.get(url, stream=True)
    with open(save_path, 'wb') as fd:
        for chunk in r.iter_content(chunk_size=chunk_size):
            fd.write(chunk)

## This loops through the links where the data is and downloads the data for rides
for year in [2017,2018,2019]:
    for month in range(1,13):
        if month < 10:
            url = 'https://s3.amazonaws.com/tripdata/' + str(year) + '0' + str(month) + '-citibike-tripdata.csv.zip'
            path = str(year) + str(month) +'.zip'
            download_url(url, path)
        else:
            url = 'https://s3.amazonaws.com/tripdata/' + str(year) + str(month) + '-citibike-tripdata.csv.zip'
            path = str(year) + str(month) +'.zip'
            download_url(url, path)

## Loop through the zip files that contain the rider data
for path in glob.iglob('20*.zip'):
    zf = zipfile.ZipFile(path)
    ## Create dataframe list and put all the relevant data in that list
    for file in (name for name in zf.namelist()  if name[0:8] != '__MACOSX' ):
        print(file)
        df = pd.read_csv(zf.open(file) )
        df.columns = ['duration','start_date','end_date',
                      'start_station_id','start_station_name','start_station_lat','start_station_long' ,
                      'end_station_id','end_station_name','end_station_lat','end_station_long',
                      'bike_id', 'user_type','birth_year','gender']
        df = df[['start_date','end_date','start_station_id' ,'start_station_lat','start_station_long',
                 'end_station_id','end_station_lat','end_station_long']]
        df_list.append(df)
        zf.close()
    os.remove(path)

## Concat the data frames
df = pd.concat(df_list, sort = True).drop_duplicates().reset_index()

## Create a variable for the hour of the day trips are taken
df['start_hod'] = pd.to_datetime(df['start_date']).dt.hour
df['end_hod'] = pd.to_datetime(df['end_date']).dt.hour
## Remove the time from the date variables
df['start_date'] = df['start_date'].str[0:10]
df['end_date'] = df['end_date'].str[0:10]

## Aggregate the rides to represent the number of rides from each station every hour of every day in the time frame
rides = df.groupby(['start_station_id' , 'start_date','start_station_lat','start_station_long','start_hod'], as_index = False).index.count()
rides_ended = df.groupby(['end_station_id' , 'end_date','end_hod'], as_index = False).index.count()
rides.columns = ['station_id' , 'date','station_lat','station_long','hod','rides_began']
rides_ended.columns = ['station_id' , 'date','hod','rides_ended']
rides = pd.merge(rides,rides_ended, how = 'inner' , left_on = ['station_id','date','hod'] , right_on = ['station_id','date','hod'])
rides['excess_rides'] = rides['rides_ended'] - rides['rides_began']


## There are some problematic stations with location data outside new york
## Ideally this would be based of lat long city limits but this deals with all the problems
rides = rides[(rides['station_lat'] != 0) & (rides['station_id'] != 3036) &
              (rides['station_id'] != 3650) & (rides['station_id'] != 3488) &
              (rides['station_id'] != 3633)]



## Create dummy variables for each day of the week, month of the year and hour of day
rides['month'] = pd.to_datetime(rides['date']).dt.month
rides['dow'] = pd.to_datetime(rides['date']).dt.dayofweek 
rides_ended['month'] = pd.to_datetime(rides_ended['date']).dt.month
rides_ended['dow'] = pd.to_datetime(rides_ended['date']).dt.dayofweek 



## Create a location dataframe that will allow us to get altitude of each station
## We keep only the most up to date lat and long as these change over the dataset
location = rides[['station_id','station_lat','station_long']].drop_duplicates(subset='station_id', keep='last').reset_index().drop(columns = ['index'])

## Create the json file that will contain lat on long data
data = {}
data['locations'] = []
for i in location.index:
    data['locations'].append({
        'latitude': location.loc[i,'station_lat'],
        'longitude': location.loc[i,'station_long']
        })

## Initialize altitude column with nan values
location['altitude'] = np.nan

## While loop because sometimes the api is not accessed so make sure the code continues to run until it works
while location['altitude'].isnull().any():
    try:
        ## Request the elevation data from open-elevation
        url = 'https://api.open-elevation.com/api/v1/lookup'
        headers = {'Accept' : 'application/json', 'Content-Type' : 'application/json'}
        r = requests.post(url, data=json.dumps(data), headers=headers).json()

        ## Create the altitude column in station and fill it with the elevation values
        for i in location.index:
            location.loc[i,'altitude'] = pd.io.json.json_normalize(r, 'results')['elevation'].values[i]
    except json.JSONDecodeError:
        print('failed')
        continue

## Load transit data
client = Socrata("data.ny.gov", None)

results = client.get("i9wp-a4ja", limit=2000)
transit = pd.DataFrame.from_records(results)
transit = transit[['entrance_latitude' , 'entrance_longitude']]
transit.columns = ['lat','long']
transit['lat'] = transit['lat'].astype(float)
transit['long'] = transit['long'].astype(float)



## Create a distance variable for each bike station that gives distance to closest transit station
def distance_between_points(lat1,long1,lat2,long2):
    #Radius of the earth
    R = 6373.0
    ## Define differences
    dlat = math.radians(lat2) - math.radians(lat1)
    dlong = math.radians(long2) - math.radians(long1)

    ## Distance between points formula
    a = math.sin(dlat / 2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlong / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    distance = R * c
    return distance

## Create distance to transit column
location['distance_to_transit'] = np.nan

## Loop through each station and find the closest transit
for i in location.index:
    print("loc: " + str(i))
    distances = []

    ## Loop through each transit station and find distance to particular bike station
    for x in transit.index:
        print("subway: " + str(x))
        distances.append(distance_between_points(location.loc[i,'station_lat'],location.loc[i,'station_long'],
                                                 transit.loc[x,'lat'],transit.loc[x,'long']))
        
    ## Set the distance to transit as the distance to the closet transit station
    location.loc[i,'distance_to_transit'] = min(distances)
    
## Drop the lat and long columns, these are already in rides column
location = location.drop(columns = ['station_lat','station_long'])

## Join the rides summary dataframe and the station dataframe to create the new elevation data
rides = rides.join(location.set_index('station_id'),on = 'station_id')

## Get the weather data and join it to rides data
weather = pd.read_csv('ny_weather.csv')
weather = weather[['DATE','AWND','PRCP','SNOW','SNWD','TMAX','TMIN']]
weather.columns = ['date','wind_speed','precipitation','snow','snow_depth','max_temp','min_temp']
rides = rides.join(weather.set_index('date'),on = 'date')

## write the rides data to a csv
f = open('rides.csv', 'w')
f.write(rides.to_csv(index = False))
f.close()

