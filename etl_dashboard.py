# -*- coding: utf-8 -*-
"""
Created on Mon Nov 27 16:27:29 2017

@author: Alan.Toppen
"""
import sys
from datetime import datetime, timedelta
from multiprocessing import get_context
import pandas as pd
import sqlalchemy as sq
import time
import os
import itertools
import boto3
import yaml
import io
import re
import psutil

from spm_events import etl_main
from parquet_lib import read_parquet_file


'''
    df:
        SignalID [int64]
        TimeStamp [datetime]
        EventCode [str or int64]
        EventParam [str or int64]

    det_config:
        SignalID [int64]
        IP [str]
        PrimaryName [str]
        SecondaryName [str]
        Detector [int64]
        Call Phase [int64]
'''

def etl2(s, date_, det_config, conf):

    date_str = date_.strftime('%Y-%m-%d')

    det_config_good = det_config[det_config.SignalID==s]

    start_date = date_
    end_date = date_ + pd.DateOffset(days=1) - pd.DateOffset(seconds=0.1)


    t0 = time.time()

    try:
        bucket = conf['bucket']
        key = f'atspm/date={date_str}/atspm_{s}_{date_str}.parquet'
        df = read_parquet_file(bucket, key)


        if len(df)==0:
            print(f'{date_str} | {s} | No event data for this signal')


        if len(det_config_good)==0:
            print(f'{date_str} | {s} | No detector configuration data for this signal')

        if len(df) > 0 and len(det_config_good) > 0:

            c, d = etl_main(df, det_config_good)

            if len(c) > 0 and len(d) > 0:

                c.to_parquet(f's3://bucket}/cycles/date={date_str}/cd_{s}_{date_str}.parquet',
                             allow_truncated_timestamps=True)

                d.to_parquet(f's3://bucket}/detections/date={date_str}/de_{s}_{date_str}.parquet',
                             allow_truncated_timestamps=True)

            else:
                print(f'{date_str} | {s} | No cycles')


    except Exception as e:
        print(f'{s}: {e}')







def main(start_date, end_date):


    with open('Monthly_Report.yaml') as yaml_file:
        conf = yaml.load(yaml_file, Loader=yaml.Loader)

    #-----------------------------------------------------------------------------------------
    # Placeholder for manual override of start/end dates
    # start_date = '2021-05-04'
    # end_date = '2021-05-04'
    #-----------------------------------------------------------------------------------------

    dates = pd.date_range(start_date, end_date, freq='1D')

    corridors_filename = re.sub('\..*', '.feather', conf['corridors_filename_s3'])
    corridors = pd.read_feather(corridors_filename)
    corridors = corridors[~corridors.SignalID.isna()]

    signalids = list(corridors.SignalID.astype('int').values)
    # signalids = list(corridors.SignalID.values)
    
    region = os.environ['AWS_DEFAULT_REGION']
    bucket = conf['bucket']
    staging_dir = conf['athena']['staging_dir']
    
    #-----------------------------------------------------------------------------------------
    # Placeholder for manual override of signalids
    #signalids = [7053]
    #-----------------------------------------------------------------------------------------

    t0 = time.time()

    for date_ in dates:

        date_str = date_.strftime('%Y-%m-%d')

        print(date_str)

		# Use boto3 s3 client rather than pd.read_feather or pd.read_parquet to utilize ssl verify parameters
        objects = s3.list_objects(Bucket=bucket, Prefix=f'atspm_det_config_good/date={date_str}')
        keys = [obj['Key'] for obj in objects['Contents']]

        def f(key):
            with io.BytesIO() as data: 
                s3.download_fileobj(
                    Bucket=bucket, 
                    Key=key, Fileobj=data)  

                det_config_raw = (pd.read_feather(data)
                    .assign(SignalID = lambda x: x.SignalID.astype('int64'))
                    .assign(Detector = lambda x: x.Detector.astype('int64'))
                    .rename(columns={'CallPhase': 'Call Phase'}))
            return det_config_raw

        det_config_raw = pd.concat([f(k) for k in keys])

        try:
            with io.BytesIO() as data: 
                s3.download_fileobj(
                    Bucket=bucket,
                    Key=f'mark/bad_detectors/date={date_str}/bad_detectors_{date_str}.parquet', Fileobj=data)

                bad_detectors = (pd.read_parquet(data)
                    .assign(SignalID = lambda x: x.SignalID.astype('int64'))
                    .assign(Detector = lambda x: x.Detector.astype('int64')))


            # bad_detectors = pd.read_parquet(
            #     f's3://{bucket}/mark/bad_detectors/date={date_str}/bad_detectors_{date_str}.parquet')\
            #             .assign(SignalID = lambda x: x.SignalID.astype('int64'))\
            #             .assign(Detector = lambda x: x.Detector.astype('int64'))

            left = det_config_raw.set_index(['SignalID', 'Detector'])
            right = bad_detectors.set_index(['SignalID', 'Detector'])


            det_config = left.join(right, how='left')\
                .fillna(value={'Good_Day': 1})\
                .query('Good_Day == 1')\
                .groupby(['SignalID','Call Phase'])\
                .apply(lambda group: group.assign(CountDetector = group.CountPriority == group.CountPriority.min()))\
                .reset_index()

            print(det_config.head())

        except FileNotFoundError:
            det_config = pd.DataFrame()

        if len(det_config) > 0:
            nthreads = round(psutil.virtual_memory().total/1e9)  # ensure 1 MB memory per thread

            #-----------------------------------------------------------------------------------------
            with get_context('spawn').Pool(processes=nthreads) as pool:
                result = pool.starmap_async(
                    etl2, list(itertools.product(signalids, [date_], [det_config], [conf])), chunksize=(nthreads-1)*4)
                pool.close()
                pool.join()
            #-----------------------------------------------------------------------------------------
        else:
            print('No good detectors. Skip this day.')

    print(f'{len(signalids)} signals in {len(dates)} days. Done in {int((time.time()-t0)/60)} minutes')


    # Add a partition for each day
    for date_ in dates:

        date_str = date_.strftime('%Y-%m-%d')

        response_repair_cycledata = ath.start_query_execution(
            QueryString=f"ALTER TABLE cycledata ADD PARTITION (date = '{date_str}');",
            QueryExecutionContext={'Database': conf['athena']['database']},
            ResultConfiguration={'OutputLocation': conf['athena']['staging_dir']})

        response_repair_detection_events = ath.start_query_execution(
            QueryString=f"ALTER TABLE detectionevents ADD PARTITION (date = '{date_str}');",
            QueryExecutionContext={'Database': conf['athena']['database']},
            ResultConfiguration={'OutputLocation': conf['athena']['staging_dir']})


    # Check if the partitions for the last day were successfully added before moving on
    while True:
        response1 = s3.list_objects(
            Bucket=os.path.basename(conf['athena']['staging_dir']),
            Prefix=response_repair_cycledata['QueryExecutionId'])
        response2 = s3.list_objects(
            Bucket=os.path.basename(conf['athena']['staging_dir']),
            Prefix=response_repair_detection_events['QueryExecutionId'])

        if 'Contents' in response1 and 'Contents' in response2:
            print('done.')
            break
        else:
            time.sleep(2)
            print('.', end='')


if __name__=='__main__':

    with open('Monthly_Report.yaml') as yaml_file:
        conf = yaml.load(yaml_file, Loader=yaml.Loader)


	s3 = boto3.client('s3', verify=conf['ssl_cert'])
	ath = boto3.client('athena', verify=conf['ssl_cert'])


    if len(sys.argv) > 1:
        start_date = sys.argv[1]
        end_date = sys.argv[2]
    else:
        start_date = conf['start_date']
        end_date = conf['end_date']

    if start_date == 'yesterday':
        start_date = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    if end_date == 'yesterday':
        end_date = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')

    main(start_date, end_date)

