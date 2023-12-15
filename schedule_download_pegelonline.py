import numpy as np
import pandas as pd
import time 
import datetime
import schedule
import requests
import json

def job():
    web = 'https://www.pegelonline.wsv.de/webservices/rest-api/v2//stations/85d686f1-55b2-4d36-8dba-3207b50901a7/W/measurements.json'

    # define the header, parameter, and start-end date to request api data 
    headers = {
        'Content-Type' : 'application/json',
        'Accept' : 'application/json', 
        'Accept-Charset' : 'utf-8'
        }


    endate = datetime.datetime.today().strftime("%Y-%m-%dT%H:%M:%S%z")      # define the current datetime 
    yesterday = datetime.datetime.today() - datetime.timedelta(days=10)      # define the previous datetime
    startdate = yesterday.strftime("%Y-%m-%dT%H:%M:%S%z")                   # change the datetime format 


    parameters_pegel = {
        'start' : f'{startdate}', 
        'end' : f'{endate}'
        }

    result = json.loads(requests.get(web, headers=headers, params=parameters_pegel).text) # request data from web API and load it in json format

    # dealing with the downloaded data 
    river = pd.DataFrame(result) # converting json format into dataframe 
    river['timestamp'] = pd.to_datetime(river['timestamp'], format = "%Y-%m-%dT%H:%M:%S%z", utc=True)
    river.to_csv(r'D:\repos\wasserweise\Exercise\River_PegelOnline\riverpirna.csv', mode='a', index=False, header=False) # adding the result into existing csv in the bottom row

    # clean and manage the data 
    river = pd.read_csv(r'D:\repos\wasserweise\Exercise\River_PegelOnline\riverpirna.csv')
    river['timestamp'] = pd.to_datetime(river['timestamp'], format = "%Y-%m-%d %H:%M:%S%z", utc=True) # change column format from string to datetime 
    riverclean = river.drop_duplicates(subset=['timestamp']) # remove file duplication 
    riverclean = river.sort_values('timestamp')
    riverclean.to_csv(r'D:\repos\wasserweise\Exercise\River_PegelOnline\riverpirna.csv', index=False) # save the clean data to replace the existing csv
    avghour = riverclean.resample('1H', on='timestamp').mean() #resample the data into average hour data

    # converting data into hourly sample data to be the same scale as well data 
    avghour.to_csv(r'D:\repos\wasserweise\Exercise\River_PegelOnline\riverpirna_avghour.csv')

schedule.every().day.at("19:45", "Europe/Berlin").do(job)

while 1:
    n = schedule.idle_seconds()
    if n is None:
        # no more jobs
        break
    elif n > 0:
        # sleep exactly the right amount of time
        time.sleep(n)
    schedule.run_pending()
    
schedule.clear()