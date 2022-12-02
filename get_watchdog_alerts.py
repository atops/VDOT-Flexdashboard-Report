# -*- coding: utf-8 -*-
"""
get_watchdog_alerts.py

Created on Thu Jul 26 14:36:14 2018

@author: V0010894
"""

import pandas as pd
import numpy as np
import sqlalchemy as sq
import yaml
from datetime import datetime

import gcsio
from pull_atspm_data import get_atspm_engine


# Upload watchdog alerts to predetermined location in S3
def s3_upload_watchdog_alerts(df, bucket, region):

    key_prefix = 'kimley-horn'

    gcsio.s3_write_parquet(
        df, 
        Bucket=bucket, 
        Key=f'{key_prefix}/mark/watchdog/SPMWatchDogErrorEvents_{region}.parquet')


def get_watchdog_alerts(engine, corridors):

    # Query ATSPM Watchdog Alerts Table from ATSPM
    with engine.connect() as conn:
        SPMWatchDogErrorEvents = pd.read_sql_table('SPMWatchDogErrorEvents', con=conn)\
            .drop(columns=['ID'])\
            .drop_duplicates()

    # Join Watchdog Alerts with Corridors
    wd = SPMWatchDogErrorEvents.loc[SPMWatchDogErrorEvents.SignalID != 'null', ]
    wd = wd.fillna(value={'DetectorID': '0'})
    wd['Detector'] = np.vectorize(
            lambda a, b: a.replace(b, ''))(wd.DetectorID, wd.SignalID)
    wd = wd.drop(columns=['DetectorID'])
    wd.SignalID = wd.SignalID.astype('int')

    wd = (wd.set_index(['SignalID']).join(corridors.set_index(['SignalID']), how = 'left')
            .reset_index())
    wd = wd[~wd.Corridor.isna()]
    wd = wd.rename(columns = {'Phase': 'CallPhase',
                              'TimeStamp': 'Date'})

    # Clearn up the Message into a new field: Alert
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

    wd = wd.filter(['Zone_Group', 'Zone', 'Corridor',
                    'SignalID', 'CallPhase', 'Detector',
                    'Alert', 'Name', 'Date'], axis = 1)

    return wd
    #Zone_Group | Zone | Corridor | SignalID/CameraID | CallPhase | Detector | Date | Alert | Name


def main():

    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    with open('Monthly_Report_AWS.yaml') as yaml_file:
        cred = yaml.load(yaml_file, Loader=yaml.Loader)
    
    with open('Monthly_Report.yaml') as yaml_file:
        conf = yaml.load(yaml_file, Loader=yaml.Loader)

    try:
        engine = get_atspm_engine(cred)

        corridors = gcsio.get_corridors(
                conf['bucket'],
                conf['corridors_filename_s3'].replace('.xlsx', '.parquet'))

        wd = get_watchdog_alerts(engine, corridors)
        print(f'{now} - {len(wd)} watchdog alerts')

        try:
            # Write to Feather file - WatchDog
            s3_upload_watchdog_alerts(wd, conf['bucket'], conf['region'])
            print(f'{now} - successfully uploaded to s3')
        except Exception as e:
            print(f'{now} - ERROR: Could not upload to s3 - {str(e)}')

    except Exception as e:
        print(f'{now} - ERROR: Could not retrieve watchdog alerts - {str(e)}')


if __name__=='__main__':
    main()




