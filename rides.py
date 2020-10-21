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
#rides = pd.DataFrame(df.groupby('start_date').index.count()).reset_index()
rides = df.groupby('start_date').index.count().to_frame().reset_index()
rides.columns = ['date' ,'trips']

## A dummy variable 1 if saturday or sunday zero otherwise
rides['weekend'] = np.where(pd.to_datetime(rides['date']).dt.dayofweek >= 5 , 1 ,0)
rides['month'] = pd.to_datetime(rides['date']).dt.month


## Get the open date of each station by finding the first day there was rides at that station
open_date = df.groupby('start_station_code').start_date.min()
open_date = open_date[(open_date <= '2019-01-01') & (open_date >= '2018-01-01')]

## loop through start date and create 
for i, x in open_date.items():
    name = 'station_' + str(i)
    rides[name] = np.where(rides['date'] >= x , 1, 0)


f = open('rides.csv', 'w')
f.write(rides.to_csv(index = False))
f.close()
