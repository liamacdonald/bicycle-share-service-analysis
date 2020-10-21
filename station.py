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
    except json.JSONDecodeError:
        print('failed')
        continue


## write the rides data to a csv
f = open('station.csv', 'w')
f.write(station.to_csv(index = False))
f.close()

