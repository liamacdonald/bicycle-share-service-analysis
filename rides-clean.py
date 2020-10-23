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
                 'end_station_id','end_station_id','end_station_lat','end_station_long']]
        df_list.append(df)
        zf.close()
    os.remove(path)

## Concat the data frames
df = pd.concat(df_list, sort = True).drop_duplicates().reset_index()
error
## This station code appears once in the data for 2019-08 need to figure out what it is but seems like a data error so dropping for now
## Shouldn't be a huge issue only accounts for 3 rides on one day
df['hod'] = pd.to_datetime(rides['start_date']).dt.month
## REMOVE TIME FROM DATE VARIABLES
df['start_date'] = df['start_date'].str[0:10]
df['end_date'] = df['end_date'].str[0:10]

## AGGREGATE RIDE COUNTS BY DATE AND STATION CODE
rides = df.groupby(['start_station_id' , 'start_date','start_station_lat','start_station_long','hod'], as_index = False).index.count()

location = df[['start_station_id','start_station_lat','start_station_long']].drop_duplicates().reset_index()
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



## Join the rides summary dataframe and the station dataframe to create the new elevation data
rides = rides.join(station.set_index('station_code'),on = 'station_code')

## write the rides data to a csv
f = open('rides.csv', 'w')
f.write(rides.to_csv(index = False))
f.close()
