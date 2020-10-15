from fitparse import FitFile
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
from datetime import timedelta
from datetime import time
import glob
import requests
from json import JSONDecodeError

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

## DEFINE A FUNCTION THAT WILL RETRIEVE ELEVATION FROM the open-elevation api

def get_elevation(lat, long):
    query = ('https://api.open-elevation.com/api/v1/lookup'
             f'?locations={lat},{long}')
    r = requests.get(query).json()  # json object, various ways you can extract value
    # one approach is to use pandas json functionality:
    elevation = pd.io.json.json_normalize(r, 'results')['elevation'].values[0]
    return elevation


## INITIALIZE THE ALTITUDE COLUMN AS NaN values
station['altitude'] = np.NaN

## While loop continues as long as there are NaN values in the altitude column
## This is necessary because the code fails to access the api and pull data resulting in random errors
while station['altitude'].isnull().any():
    ## The for loop goes through each row that has a null altitude value and tries to retrieve it but continues if there is an error
    for i in station[station['altitude'].isnull()].index:
        try:
            station.loc[i,'altitude'] = get_elevation(station.loc[i,'lat'],station.loc[i,'long'])
            print('success' + str(i))
        except JSONDecodeError:
            print('failed' + str(i))
            continue


## Write the file to elevation.csv to save it 
f = open('elevation.csv', 'w')
f.write(station.to_csv(index = False))
f.close()

## Join the rides summary dataframe and the station dataframe to create the new elevation data
rides = rides_by_station.join(station.set_index('station_code'),on = 'station_code')
f = open('rides_by_station.csv', 'w')
f.write(rides.to_csv())
f.close()
