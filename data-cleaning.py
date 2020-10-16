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
## Loop through te zip files that contain the rider data.
for path in glob.iglob('20*.zip'):
    zf = zipfile.ZipFile(path)
    ## Create dataframe list and put all the relevant data in that list
    for file in (name for name in zf.namelist() if name[0] == 'O'):
        df = pd.read_csv(zf.open(file))
        df_list.append(df)


## Concat those dataframes into one data fram
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
zf = zipfile.ZipFile('stations.zip')
station = pd.read_csv(zf.open('Stations_2019.csv'))
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


## Write the file to stations which now contains all the station data
f = open('station.csv', 'w')
f.write(station.to_csv(index = False))
f.close()

## Join the rides summary dataframe and the station dataframe to create the new elevation data
rides = rides_by_station.join(station.set_index('station_code'),on = 'station_code')
f = open('rides_by_station.csv', 'w')
f.write(rides.to_csv())
f.close()
