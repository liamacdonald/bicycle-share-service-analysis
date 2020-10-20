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

df_list = []
## Loop through te zip files that contain the rider data
for path in glob.iglob('20*.zip'):
    zf = zipfile.ZipFile(path)
    ## Create dataframe list and put all the relevant data in that list
    for file in zf.namelist():
        df = pd.read_csv(zf.open(file),dtype = {'start_date': 'str'
                                                ,'end_date': 'str',
                                                'start_station_code': 'str',
                                                'end_station_code': 'str' ,
                                                'duration_sec' : int ,
                                                'is_member' : int})
        df_list.append(df)


## Concat those dataframes into one dataframe
df = pd.concat(df_list, sort = True).drop_duplicates().reset_index()

## This station code appears once in the data for 2019-08 need to figure out what it is but seems like a data error so dropping for now
## Shouldn't be a huge issue only accounts for 3 rides on one day
df = df[df['start_station_code'] != 'MTL-ECO5.1-01']

## REMOVE TIME FROM DATE VARIABLES
df['start_date'] = df['start_date'].str[0:10]
df['end_date'] = df['end_date'].str[0:10]


## AGGREGATE RIDE COUNTS BY DATE AND STATION CODE
beginning = df.groupby(['start_station_code' , 'start_date'], as_index = False).index.count()
ending = df.groupby(['end_station_code' , 'end_date'], as_index = False).index.count()
columns = ['station_code', 'date', 'trips']
beginning.columns = columns
ending.columns = columns
rides_by_station = beginning.join(ending.set_index(['station_code','date']),
                                  on = ['station_code','date'],
                                  lsuffix='_began',
                                  rsuffix='_ended')

## This is necessary for the join on station data later
rides_by_station['station_code'] = rides_by_station['station_code'].astype(int)

## A few dates/station combos have no rides ended so fill these with 0 instead of NA
rides_by_station['trips_ended'] = rides_by_station['trips_ended'].fillna(0)


## Bring in all station data
df_list = []
zf = zipfile.ZipFile('stations.zip')
for file in zf.namelist(): 
    station = pd.read_csv(zf.open(file))
    station.columns = ['station_code','name','lat','long']
    del station['name']
    df_list.append(station)


## Concat the station data and drop duplicates based on station code
station = pd.concat(df_list, sort = True).drop_duplicates(subset='station_code', keep="first").reset_index()


## Create the json file that will contain lat on long data
data = {}
data['locations'] = []
for i in station.index:
    data['locations'].append({
        'latitude': station.loc[i,'lat'],
        'longitude': station.loc[i,'long']
        })

## Initialize altitude column with nan values
station['altitude'] = np.nan

## While loop because sometimes the api is not accessed so make sure the code continues to run until it works
while station['altitude'].isnull().any():
    try:
        ## Request the elevation data from open-elevation
        url = 'https://api.open-elevation.com/api/v1/lookup'
        headers = {'Accept' : 'application/json', 'Content-Type' : 'application/json'}
        r = requests.post(url, data=json.dumps(data), headers=headers).json()

        ## Create the altitude column in station and fill it with the elevation values
        for i in station.index:
            station.loc[i,'altitude'] = pd.io.json.json_normalize(r, 'results')['elevation'].values[i]
        station.head()
    except json.JSONDecodeError:
        print('failed')
        continue


## Write the file to stations which now contains all the station data, we could get this from the rides by station data but it makes sense to store it here in case we don't need rides
## This file takes up almost no space
f = open('station.csv', 'w')
f.write(station.to_csv(index = False))
f.close()

## Join the rides summary dataframe and the station dataframe to create the new elevation data
rides = rides_by_station.join(station.set_index('station_code'),on = 'station_code')
f = open('rides_by_station.csv', 'w')
f.write(rides.to_csv(index = False))
f.close()
