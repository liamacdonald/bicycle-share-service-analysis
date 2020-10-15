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

## LOOP THROUGH DATA FILES AND MAKE ON DATAFRAME WITH ALL RIDES

df_list = []

for file in glob.iglob(r'C:\Users\liama\OneDrive\data_projects\bicycle-share-montreal\Montreal*\OD*.csv'):
    df = pd.read_csv(file)
    df_list.append(df)


df = pd.concat(df_list, sort = True).drop_duplicates().reset_index()

## REMOVE TIME FROM DATE VARIABLES
df['start_date'] = df['start_date'].str[0:10]
df['end_date'] = df['end_date'].str[0:10]



## AGGREGATE RIDE COUNTS BY DATE AND STATION CODE
beginning = df.groupby(['start_station_code' , 'start_date'], as_index = False).index.count()
ending = df.groupby(['end_station_code' , 'end_date'], as_index = False).index.count()
columns = ['station_code', 'date', 'trips']
beginning.columns = columns
ending.columns = columns
rides_by_station = beginning.join(ending.set_index(['station_code','date']),on = ['station_code','date'], lsuffix='_began', rsuffix='_ended')




## BRING IN THE DATA FOR THE STATIONS THAT EXISTED IN 2019

station = pd.read_csv(r'C:\Users\liama\OneDrive\data_projects\bicycle-share-montreal\Montreal2019\Stations_2019.csv')
station.columns = ['station_code','name','lat','long']
del station['name']


## Create the json file that will contain lat on long data

data = {}
data['locations'] = []
for i in station.index:
    data['locations'].append({
        'latitude': station.loc[i,'lat'],
        'longitude': station.loc[i,'long']
        })

## Request the elevation data from open-elevation
url = 'https://api.open-elevation.com/api/v1/lookup'
headers = {'Accept' : 'application/json', 'Content-Type' : 'application/json'}
r = requests.post(url, data=json.dumps(data), headers=headers).json()

## Create the altitude column in station and fill it with the elevation values
station['altitude'] = 0
for i in station.index:
    station.loc[i,'altitude'] = pd.io.json.json_normalize(r, 'results')['elevation'].values[i]
station.head()




## Write the file to elevation.csv to save it 
f = open('elevation.csv', 'w')
f.write(station.to_csv(index = False))
f.close()

## Join the rides summary dataframe and the station dataframe to create the new elevation data
rides = rides_by_station.join(station.set_index('station_code'),on = 'station_code')
f = open('rides_by_station.csv', 'w')
f.write(rides.to_csv())
f.close()
