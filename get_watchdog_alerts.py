# -*- coding: utf-8 -*-
"""
get_watchdog_alerts.py

Created on Thu Jul 26 14:36:14 2018

@author: V0010894
"""

import pandas as pd
import numpy as np
import sqlalchemy as sq
import pyodbc
import os
import io
import boto3
import zipfile
import time
from datetime import datetime, timedelta
import polling
from retrying import retry
import yaml

from s3io import *
from pull_atspm_data import get_atspm_engine
from mark1_logger import mark1_logger

base_path = '.'

logs_path = os.path.join(base_path, 'logs')
if not os.path.exists(logs_path):
    os.mkdir(logs_path)
logger = mark1_logger(os.path.join(logs_path, f'get_watchdog_alerts_{datetime.today().strftime("%F")}.log'))


# Read Corridors File from S3
def get_corridors(bucket, filename):
    filename = os.path.splitext(filename)[0] + '.feather'
    corridors = read_feather(Bucket=bucket, Key=filename)
    corridors = (corridors[~corridors.SignalID.isna()]
                .assign(SignalID = lambda x: x.SignalID.astype('int'))
                .drop(['Description'], axis=1))

    return corridors


# Upload watchdog alerts to predetermined location in S3
@retry(wait_random_min=1000, wait_random_max=2000, stop_max_attempt_number=10)
def s3_upload_watchdog_alerts(df, conf, asof_date=None):

    bucket = conf['bucket']
    region = conf['region']

    s3key = f'mark/watchdog/SPMWatchDogErrorEvents_{region}.parquet'
    if asof_date:
        s3key = s3key.replace('.parquet', f'_{asof_date}.parquet')

    write_parquet(df, Bucket=bucket, Key=s3key)


@retry(wait_random_min=1000, wait_random_max=2000, stop_max_attempt_number=10)
def get_watchdog_alerts(engine, corridors, asof_date=None):

    # Query ATSPM Watchdog Alerts Table from ATSPM
    with engine.connect() as conn:
        query = 'SELECT * FROM SPMWatchDogErrorEvents' 
        if asof_date:
            query = query + f" WHERE TimeStamp >= '{asof_date}'"
 
        SPMWatchDogErrorEvents = pd.read_sql_query(query, con=conn)\
            .drop(columns=['ID'])\
            .drop_duplicates()

    # Join Watchdog Alerts with Corridors
    wd = SPMWatchDogErrorEvents.loc[SPMWatchDogErrorEvents.SignalID != 'null', ]
    wd = wd.loc[wd.TimeStamp > datetime.today() - timedelta(days=100)]
    wd = wd.fillna(value={'DetectorID': '0'})
    wd['Detector'] = np.vectorize(
            lambda a, b: a.replace(b, ''))(wd.DetectorID, wd.SignalID)
    wd = wd.drop(columns=['DetectorID'])
    wd = wd.loc[wd.SignalID.isin(corridors.SignalID.astype('str'))]
    wd.SignalID = wd.SignalID.astype('int')

    wd = (wd.set_index(['SignalID']).join(corridors.set_index(['SignalID']), how = 'left')
            .reset_index())
    wd = wd[~wd.Corridor.isna()]
    wd = wd.rename(columns = {'Phase': 'CallPhase',
                              'TimeStamp': 'Date'})

    # Clean up the Message into a new field: Alert
    wd.loc[wd.Message.str.contains('Force Offs'), 'Alert'] = 'Force Offs'
    wd.loc[wd.Message.str.contains('Count'), 'Alert'] = 'Count'
    wd.loc[wd.Message.str.contains('Max Outs'), 'Alert'] = 'Max Outs'
    wd.loc[wd.Message.str.contains('Pedestrian Activations'), 'Alert'] = 'Pedestrian Activations'
    wd.loc[wd.Message.str.contains('Missing Records'), 'Alert'] = 'Missing Records'
    
    # Enforce Data Types
    wd.Alert = wd.Alert.astype('str')
    wd.Detector = wd.Detector.astype('int')
    wd.CallPhase = wd.CallPhase.astype('str')
    wd.CallPhase = wd.CallPhase.astype('category')
    wd.ErrorCode = wd.ErrorCode.astype('category')
    wd.Zone = wd.Zone.astype('category')
    wd.Zone_Group = wd.Zone_Group.astype('category')
    
    wd.Corridor = wd.Corridor.astype('category')
    wd.Name = wd.Name.astype('category')
    wd.Date = wd.Date.dt.date
    
    wd = wd.reset_index(drop=True)
    wd = wd.filter(['Zone_Group', 'Zone', 'Corridor', 
                    'SignalID', 'CallPhase', 'Detector', 
                    'Alert', 'Name', 'Date'], axis = 1)

    return wd


def main():
   
    with open('Monthly_Report.yaml') as yaml_file:
        conf = yaml.load(yaml_file, Loader=yaml.Loader)
 
    with open('Monthly_Report_AWS.yaml') as yaml_file:
        cred = yaml.load(yaml_file, Loader=yaml.Loader)

    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    try:    
        atspm_engine = get_atspm_engine(
            username=cred['ATSPM_UID'], 
            password=cred['ATSPM_PWD'], 
            hostname=cred['ATSPM_HOST'], 
            database=cred['ATSPM_DB'],
            dsn=cred['ATSPM_DSN'])

        corridors = get_corridors(conf['bucket'], conf['corridors_filename_s3'])
    
        asof_date = (pd.Timestamp.today() - pd.Timedelta(7, unit='days')) - pd.offsets.MonthBegin(4)
        asof_date = asof_date.strftime('%F')

        wd = get_watchdog_alerts(atspm_engine, corridors, asof_date)
        logger.success('Watchdog alerts successfully queried')
    
    except Exception as e:
        logger.error(f'Could not query watchdog alerts - {str(e)}')

    try:
        # Write to s3 - WatchDog
        s3_upload_watchdog_alerts(wd, conf) # don't append date to file. only maintain one file.
        logger.success('Watchdog alerts successfully uploaded to s3')

    except Exception as e:
        logger.error(f'Could not save watchdog alerts to s3 - {str(e)}')
    

if __name__=='__main__':
    main()



