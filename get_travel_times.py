# -*- coding: utf-8 -*-
"""
Created on Mon Sep 24 20:42:51 2018

@author: Alan.Toppen
"""


import os
import pandas as pd
import feather
import requests
import uuid
import polling
import time
import yaml
from datetime import datetime, timedelta
import zipfile
import io

def get_tmc_data(start_date, end_date, tmcs, key, initial_sleep_sec=0):
    
    # Allow sleep time to space out requests when running in a loop
    time.sleep(initial_sleep_sec)

    uri = 'http://kestrel.ritis.org:8080/{}'
   
    #----------------------------------------------------------  
    payload = {
      "dates": [
        {
          "end": end_date,
          "start": start_date
        }
      ],
      "dow": [
        2,
        3,
        4
      ],
      "dsFields": [
        {
          "columns": [
            "SPEED",
            "REFERENCE_SPEED",
            "TRAVEL_TIME_MINUTES",
            "CONFIDENCE_SCORE"
          ],
          "dataSource": "vpp_here",
          "qualityFilter": {
            "max": 1,
            "min": 0,
            "thresholds": [
              30,
              20,
              10
            ]
          }
        }
      ],
      "granularity": {
        "type": "minutes",
        "value": 60
      },
      "times": [
        {
          "end": None,
          "start": "00:00:00.000"
        }
      ],
      "tmcs": tmcs,
      "travelTimeUnits": "MINUTES",
      "uuid": str(uuid.uuid1())
    }  
    #----------------------------------------------------------    
    response = requests.post(uri.format('jobs/export'), 
                             params = {'key': key}, 
                             json = payload)
    print('response status code:', response.status_code)
    
    if response.status_code == 200: # Only if successful response

        # retry at intervals of 'step' until results return (status code = 200)
        results = polling.poll(
                lambda: requests.get(uri.format('jobs/export/results'), 
                               params = {'key': key, 'uuid': payload['uuid']}),
                check_success = lambda x: x.status_code == 200,
                step = 5,
                timeout = 300
                #poll_forever = True
                )
        print('results received')
        
        # Save results (binary zip file with one csv)
        zf_filename = payload['uuid'] +'.zip'
        with open(zf_filename, 'wb') as f:
            f.write(results.content)
        df = pd.read_csv(zf_filename)
        os.remove(zf_filename)
    
    else:
        df = pd.DataFrame()

    print('{} records'.format(len(df)))
    
    return df

if __name__=='__main__':

    with open('Monthly_Report_AWS.yaml') as yaml_file:
        cred = yaml.load(yaml_file)

    with open('Monthly_Report_calcs.yaml') as yaml_file:
        conf = yaml.load(yaml_file)

    start_date = conf['start_date'] #.strftime('%Y-%m-%d')
    # -----
    #start_date = '2018-09-01'
    # -----
    if start_date == 'yesterday': 
        # Make start_date the start of the month
        start_date = datetime.today() - timedelta(days=1)
        start_date = (start_date - timedelta(days=(start_date.day - 1))).strftime('%Y-%m-%d')
    end_date = conf['end_date'] #.strftime('%Y-%m-%d')
    # -----
    #end_date = '2018-09-30'
    # -----
    if end_date == 'yesterday': 
        end_date = datetime.today().strftime('%Y-%m-%d')


    tmc_fn = 'tmc_routes.feather'
    tmc_df = feather.read_dataframe(tmc_fn)
    tmc_dict = tmc_df.groupby(['Corridor'])['tmc'].apply(list).to_dict()
    
    
    #start_date = '2018-09-16'
    #end_date = '2018-09-25'
    
    df = pd.DataFrame()
    for corridor in tmc_dict.keys():
        print('\n' + corridor)
        tt_df = get_tmc_data(start_date, end_date, tmc_dict[corridor], cred['RITIS_KEY'], 10)
    
        if len(tt_df) > 0:
            df_ = (pd.merge(tt_df, tmc_df[['tmc','miles']], left_on=['tmc_code'], right_on=['tmc'])
                     .drop(columns=['tmc'])
                     .assign(Corridor = str(corridor)))
            df = pd.concat([df, df_])
            
    if len(df) > 0:
        df['reference_minutes'] = df['miles'] / df['reference_speed'] * 60
        df = df.reset_index(drop=True)
        fn = 'Inrix/For_Monthly_Report/tt_{}_TWTh.csv'.format(start_date.replace('-01',''))
        print(fn)
        #df.to_csv(fn)
        
        # Write raw data to a zipped csv
        with io.StringIO() as buff:
            df.to_csv(buff)
            with zipfile.ZipFile(fn + '.zip', mode='w', compression=zipfile.ZIP_DEFLATED) as zf:
                zf.writestr(os.path.basename(fn), buff.getvalue())
        
        # Summarize to tti, pti by hour and save to csv
        df_ = df.groupby(['Corridor', 'measurement_tstamp'], as_index=False)['travel_time_minutes', 'reference_minutes'].sum()
        df_['Hour'] = pd.to_datetime(df_['measurement_tstamp'])
        df_['Hour'] = df_['Hour'] - pd.to_timedelta(df_['Hour'].dt.day - 1, unit = 'days')
        
        desc = df_.groupby(['Corridor','Hour']).describe(percentiles = [0.90])
        
        tti = desc['travel_time_minutes']['mean'] / desc['reference_minutes']['mean']
        pti = desc['travel_time_minutes']['90%'] / desc['reference_minutes']['mean']
        
        summ_df = pd.DataFrame({'tti': tti, 'pti': pti})
        fn_ = fn.replace('_TWTh.csv', '_summary.csv')
        summ_df.to_csv(fn_)

