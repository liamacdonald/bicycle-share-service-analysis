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

## Remove time from start date variable
df['start_date'] = df['start_date'].str[0:10]



## Aggregate ride counts by date
rides = df.groupby('start_date').index.count()
rides.columns = ['date' ,'trips']

## A dummy variable 1 if saturday or sunday zero otherwise
rides['weekend'] = np.where(pd.to_datetime(rides['date']).dt.dayofweek >= 5 , 1 ,0)
rides['open_dates'] = rides.groupby('station_code').date.min()
rides['month'] = pd.to_datetime(rides['date']).dt.month



## Get the open date of each station by finding the first day there was rides at that station
open_date = df.groupby('station_code').start_date.min()


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




## write the rides data to a csv
f = open('rides_by_station.csv', 'w')
f.write(rides.to_csv(index = False))
f.close()
