from fitparse import FitFile
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
from datetime import timedelta
from datetime import time
from math import sin, cos, sqrt, atan2, radians
import glob
import requests
import json
import zipfile
import os

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
df['hod'] = pd.to_datetime(df['start_date']).dt.hour

## Remove the time from the date variables
df['start_date'] = df['start_date'].str[0:10]
df['end_date'] = df['end_date'].str[0:10]

## Aggregate the rides to represent the number of rides from each station every hour of every day in the time frame
rides = df.groupby(['start_station_id' , 'start_date','start_station_lat','start_station_long','hod'], as_index = False).index.count()
rides.columns = ['start_station_id' , 'start_date','start_station_lat','start_station_long','hod','rides']

## Create dummy variables for each day of the week, month of the year and hour of day
for i in range(0,13):
    name = 'month_' + str(i)
    rides[name] = np.where(pd.to_datetime(rides['start_date']).dt.month == i , 1 , 0)
    
for i in range(0,7):
    name = 'day_' + str(i)
    rides[name] = np.where(pd.to_datetime(rides['start_date']).dt.dayofweek == i, 1 , 0)
for i in range(0,24):
    name = 'hod_' + str(i)
    rides[name] = np.where(rides['hod'] == i, 1 , 0)
rides = rides.drop(columns = ['hod'])

## Create a location dataframe that will allow us to get altitude of each station
location = df[['start_station_id','start_station_lat','start_station_long']].drop_duplicates().reset_index().drop(columns = ['index'])

## Create the json file that will contain lat on long data
data = {}
data['locations'] = []
for i in location.index:
    data['locations'].append({
        'latitude': location.loc[i,'start_station_lat'],
        'longitude': location.loc[i,'start_station_long']
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
transit = pd.read_csv('NYC_Transit_Subway_Entrance_And_Exit_Data.csv')





## Join the rides summary dataframe and the station dataframe to create the new elevation data
rides = rides.join(location[['start_station_id','altitude']].set_index('start_station_id'),on = 'start_station_id')


## Get the weather data and join it to rides data
weather = pd.read_csv('ny_weather.csv')
weather = weather[['DATE','AWND','PRCP','SNOW','SNWD','TMAX','TMIN']]
weather.columns = ['start_date','wind_speed','precipitation','snow','snow_depth','max_temp','min_temp']
rides = rides.join(weather.set_index('start_date'),on = 'start_date')


## write the rides data to a csv
f = open('rides.csv', 'w')
f.write(rides.to_csv(index = False))
f.close()
