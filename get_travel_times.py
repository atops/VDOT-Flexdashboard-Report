# -*- coding: utf-8 -*-
"""
Created on Mon Sep 24 20:42:51 2018

@author: Alan.Toppen
"""


import sys
import pandas as pd
import numpy as np
import requests
import uuid
import polling
import time
import yaml
from datetime import datetime, timedelta
import pytz
from zipfile import ZipFile
import json
import io
import boto3
import dask.dataframe as dd

s3 = boto3.client('s3')



def is_success(response):
    try:
        x = json.loads(response.content.decode('utf-8'))
        return('state' in x.keys() and x['state']=='SUCCEEDED')
    except:
        return False


def get_tmc_data(start_date, end_date, tmcs, key, dow=[2,3,4], bin_minutes=60, initial_sleep_sec=0):
    
    # Allow sleep time to space out requests when running in a loop
    time.sleep(initial_sleep_sec)

    uri = 'http://pda-api.ritis.org:8080/{}'   
    
    #----------------------------------------------------------  
    payload = {
      "dates": [
        {
          "end": end_date,
          "start": start_date
        }
      ],
      "dow": dow,
      "dsFields": [
        {
          "columns": [
            "SPEED",
            "REFERENCE_SPEED",
            "TRAVEL_TIME_MINUTES",
            "CONFIDENCE_SCORE"
          ],
          "dataSource": "vpp_inrix",
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
        "value": bin_minutes
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
    print('travel times response status code:', response.status_code)
    
    if response.status_code == 200: # Only if successful response

        # retry at intervals of 'step' until results return (status code = 200)
        jobid = json.loads(response.content.decode('utf-8'))['id']

        polling.poll(
            lambda: requests.get(uri.format('jobs/status'), params = {'key': key, 'jobId': jobid}),
            check_success = is_success,
            step=10,
            timeout=600)

        results = requests.get(uri.format('jobs/export/results'), 
                               params = {'key': key, 'uuid': payload['uuid']})
        print('travel times results received')
        
        # Save results (binary zip file with one csv)
        with io.BytesIO() as f:
            f.write(results.content)
            f.seek(0)
            #df = pd.read_csv(f, compression='zip')       
            with ZipFile(f, 'r') as zf:
                df = pd.read_csv(zf.open('Readings.csv'))
                #tmci = pd.read_csv(zf.open('TMC_Identification.csv'))

    else:
        df = pd.DataFrame()

    print('{} travel times records'.format(len(df)))
    
    return df

def get_corridor_travel_times(df, corr_grouping, bucket, table_name):
    
    # -- Raw Hourly Travel Time Data --
    def uf(df):
        date_string = df.date.values[0]
        filename = 'travel_times_{}.parquet'.format(date_string)
        df = df.drop(columns=['date'])
        df.to_parquet(f's3://{bucket}/mark/{table_name}/date={date_string}/{filename}')
         
    # Write to parquet files and upload to S3
    df.groupby(['date']).apply(uf)


def get_corridor_travel_time_metrics(df, corr_grouping, bucket, table_name):
    
    df = df.groupby(corr_grouping + ['Hour'], as_index=False)[
        ['travel_time_minutes', 'reference_minutes', 'miles']].sum()

    # -- Travel Time Metrics Summarized by tti, pti by hour --
    df['Hour'] = df['Hour'].apply(lambda x: x.replace(day=1))
    df['speed'] = df['miles']/(df['travel_time_minutes']/60)

    desc = df.groupby(corr_grouping + ['Hour']).describe(percentiles = [0.90])
    tti = desc['travel_time_minutes']['mean'] / desc['reference_minutes']['mean']
    pti = desc['travel_time_minutes']['90%'] / desc['reference_minutes']['mean']
    bi = pti - tti
    speed = desc['speed']['mean']

    summ_df = pd.DataFrame({'tti': tti, 'pti': pti, 'bi': bi, 'speed_mph': speed})

    def uf(df): # upload parquet file
        date_string = df.date.values[0]
        filename = 'travel_time_metrics_{}.parquet'.format(date_string)
        df = df.drop(columns=['date'])
        df.to_parquet(f's3://{bucket}/mark/{table_name}/date={date_string}/{filename}')
            
    # Write to parquet files and upload to S3
    summ_df.reset_index().assign(date = lambda x: x.Hour.dt.date).groupby(['date']).apply(uf)


if __name__=='__main__':

    with open(sys.argv[1]) as yaml_file:
        tt_conf = yaml.load(yaml_file, Loader=yaml.Loader)

    with open('Monthly_Report_AWS.yaml') as yaml_file:
        cred = yaml.load(yaml_file, Loader=yaml.Loader)

    with open('Monthly_Report.yaml') as yaml_file:
        conf = yaml.load(yaml_file, Loader=yaml.Loader)

    # start_date is either the given day or the first day of the month
    start_date = conf['start_date']
    if start_date == 'yesterday': 
        start_date = (datetime.now(pytz.timezone('America/New_York')) - timedelta(days=7))
    start_date = start_date.strftime('%Y-%m-%d')

    # end date is either the given date + 1 or today. 
    # end_date is not included in the query results
    end_date = conf['end_date']
    if end_date == 'yesterday': 
        end_date = datetime.now(pytz.timezone('America/New_York')) - timedelta(days=1)
    end_date = (end_date + timedelta(days=1)).strftime('%Y-%m-%d')


    suff = tt_conf['table_suffix']
    cor_table = f'cor_travel_times_{suff}'
    cor_metrics_table = f'cor_travel_time_metrics_{suff}'
    sub_table = f'sub_travel_times_{suff}'
    sub_metrics_table = f'sub_travel_time_metrics_{suff}'
    
   
    tmc_df = (pd.read_excel(f"s3://{conf['bucket']}/{conf['corridors_TMCs_filename_s3']}", 
                            engine='openpyxl')
                .rename(columns={'length': 'miles'})
                .fillna(value={'Corridor': 'None', 'Subcorridor': 'None'}))
    tmc_df = tmc_df[tmc_df.Corridor != 'None']

    tmc_list = list(set(tmc_df.tmc.values))
    
    #start_date = '2019-11-01'
    #end_date = '2019-12-01'

    print(f'travel times: {start_date} - {end_date}')

    try:
        tt_df = get_tmc_data(
            start_date, 
            end_date, 
            tmc_list, 
            cred['RITIS_KEY'], 
            dow=tt_conf['dow'], 
            bin_minutes=tt_conf['bin_minutes'], 
            initial_sleep_sec=0
        )

    except Exception as e:
        print('ERROR retrieving tmc records')
        print(e)
        tt_df = pd.DataFrame()
    
    if len(tt_df) > 0:
        df = (pd.merge(tmc_df[['tmc', 'miles', 'Corridor', 'Subcorridor']], tt_df, left_on=['tmc'], right_on=['tmc_code'])
                .drop(columns=['tmc'])
                .sort_values(['Corridor', 'tmc_code', 'measurement_tstamp']))

        df['reference_minutes'] = df['miles'] / df['reference_speed'] * 60
        df = (df.reset_index(drop=True)
                .assign(measurement_tstamp = lambda x: pd.to_datetime(x.measurement_tstamp, format='%Y-%m-%d %H:%M:%S'),
                        date = lambda x: x.measurement_tstamp.dt.date)
                .rename(columns = {'measurement_tstamp': 'Hour'}))
        df = df.drop_duplicates()
             
        get_corridor_travel_times(
            df, ['Corridor'], conf['bucket'], cor_table)

        get_corridor_travel_times(
            df, ['Corridor', 'Subcorridor'], conf['bucket'], sub_table)
            
        months = list(set([pd.Timestamp(d).strftime('%Y-%m') for d in pd.date_range(start_date, end_date, freq='D')]))
 
        for yyyy_mm in months:
            try:
                df = dd.read_parquet(f"s3://{conf['bucket']}/mark/{cor_table}/date={yyyy_mm}-*/*").compute()
                if not df.empty:
                     get_corridor_travel_time_metrics(
                         df, ['Corridor'], conf['bucket'], cor_metrics_table)

                if not df.empty:
                    df = dd.read_parquet(f"s3://{conf['bucket']}/mark/{sub_table}/date={yyyy_mm}-*/*").compute()
                    get_corridor_travel_time_metrics(
                        df, ['Corridor', 'Subcorridor'], conf['bucket'], sub_metrics_table)
            except IndexError:
                print(f'No data for {yyyy_mm}')

    else:
        print('No records returned.')
