# -*- coding: utf-8 -*-
"""
Created on Mon Nov 27 16:27:29 2017

@author: Alan.Toppen
"""
import sys
from datetime import datetime, timedelta
from multiprocessing import get_context, Pool
import pandas as pd
import sqlalchemy as sq
import time
import posixpath
import itertools
import yaml
import re
import psutil

import s3io
from spm_events import etl_main
from config import get_date_from_string
from pull_atspm_data import get_atspm_engine, get_aurora_engine

'''
    df:
        SignalID [str]
        TimeStamp [datetime]
        EventCode [str or int64]
        EventParam [str or int64]

    det_config:
        SignalID [str]
        IP [str]
        PrimaryName [str]
        SecondaryName [str]
        Detector [int64]
        Call Phase [int64]
'''


def etl2(s, date_, det_config, conf):

    date_str = date_.strftime('%Y-%m-%d')

    det_config = det_config[det_config.SignalID==s]

    start_date = date_
    end_date = date_ + pd.DateOffset(days=1)

    t0 = time.time()

    try:
        bucket = conf['bucket']
        key_prefix = conf['key_prefix'] or ''

        if len(df)==0:
            print(f'{date_str} | {s} | No event data for this signal')

        if len(det_config)==0:
            print(f'{date_str} | {s} | No detector configuration data for this signal')

        if len(df) > 0 and len(det_config) > 0:

            c, d = etl_main(df, det_config)

            if len(c) > 0 and len(d) > 0:
                s3io.s3_write_parquet(
                    c, 
                    bucket, 
                    posixpath.join(key_prefix, f'cycles/date={date_str}/cd_{s}_{date_str}.parquet'), 
                    allow_truncated_timestamps=True)
                s3io.s3_write_parquet(
                    d, 
                    bucket, 
                    posixpath.join(key_prefix, f'detections/date={date_str}/de_{s}_{date_str}.parquet'), 
                    allow_truncated_timestamps=True)

            else:
                print(f'{date_str} | {s} | No cycles')

    except Exception as e:
        print(f'{s}: {e}')







def main(start_date, end_date, conf):

    #-----------------------------------------------------------------------------------------
    # Placeholder for manual override of start/end dates
    # start_date = '2021-05-04'
    # end_date = '2021-05-04'
    #-----------------------------------------------------------------------------------------

    dates = pd.date_range(start_date, end_date, freq='1D')

    with open('Monthly_Report_AWS.yaml') as yaml_file:
        cred = yaml.load(yaml_file, Loader=yaml.Loader)

    engine = get_aurora_engine(cred)
    with engine.connect() as conn:
        corridors = pd.read_sql_table('Corridors', conn)

    signalids = list(corridors.SignalID.values)

    athena_database = conf['athena']['database']
    staging_dir = conf['athena']['staging_dir']
    x = re.split('/+', staging_dir) # split path elements into a list
    athena_bucket = x[1] # first path element that's not s3:
    athena_prefix = '/'.join(x[2:])


    #-----------------------------------------------------------------------------------------
    # Placeholder for manual override of signalids
    #signalids = [7053]
    #-----------------------------------------------------------------------------------------

    t0 = time.time()

    for date_ in dates:

        date_str = date_.strftime('%Y-%m-%d')
        print(date_str)

        det_config = s3io.get_det_config(date_, conf)
        det_config = det_config.rename(columns={'CallPhase': 'Call Phase'})
        dcg = det_config.groupby(['SignalID', 'Call Phase'])['CountPriority']
        det_config = det_config.assign(CountDetector = det_config.CountPriority == dcg.transform(min))

        if len(det_config) > 0:
            nthreads = round(psutil.virtual_memory().total/1e9)  # ensure 1 MB memory per thread

            #-----------------------------------------------------------------------------------------
            # with Pool(processes=nthreads) as pool:
            with get_context('spawn').Pool(processes=nthreads) as pool:
                pool.starmap_async(
                    etl2, list(itertools.product(signalids, [date_], [det_config], [conf])), chunksize=(nthreads-1)*4)
                pool.close()
                pool.join()
            #-----------------------------------------------------------------------------------------
        else:
            print('No good detectors. Skip this day.')

    print(f'{len(signalids)} signals in {len(dates)} days. Done in {int((time.time()-t0)/60)} minutes')


    # Add a partition for each day. If more than ten days, update all partitions in one command.
    if len(dates) > 10:
        response_repair_cycledata = athena.start_query_execution(
            QueryString=f"MSCK REPAIR TABLE cycledata;",
            QueryExecutionContext={'Database': athena_database},
            ResultConfiguration={'OutputLocation': staging_dir})

        response_repair_detection_events = athena.start_query_execution(
            QueryString=f"MSCK REPAIR TABLE detectionevents",
            QueryExecutionContext={'Database': athena_database},
            ResultConfiguration={'OutputLocation': staging_dir})
    else:
        for date_ in dates:
            date_str = date_.strftime('%Y-%m-%d')
            response_repair_cycledata = athena.start_query_execution(
                QueryString=f"ALTER TABLE cycledata ADD PARTITION (date = '{date_str}');",
                QueryExecutionContext={'Database': athena_database},
                ResultConfiguration={'OutputLocation': staging_dir})

            response_repair_detection_events = athena.start_query_execution(
                QueryString=f"ALTER TABLE detectionevents ADD PARTITION (date = '{date_str}');",
                QueryExecutionContext={'Database': athena_database}, 
                ResultConfiguration={'OutputLocation': staging_dir})


    # Check if the partitions for the last day were successfully added before moving on
    while True:
        response1 = s3.list_objects(
            Bucket=athena_bucket,
            Prefix=posixpath.join(athena_prefix, response_repair_cycledata['QueryExecutionId']))
        response2 = s3.list_objects(
            Bucket=athena_bucket,
            Prefix=posixpath.join(athena_prefix, response_repair_detection_events['QueryExecutionId']))

        if 'Contents' in response1 and 'Contents' in response2:
            print('done.')
            break
        else:
            time.sleep(2)
            print('.', end='')


if __name__=='__main__':

    with open('Monthly_Report.yaml') as yaml_file:
        conf = yaml.load(yaml_file, Loader=yaml.Loader)

    if len(sys.argv) > 1:
        start_date = sys.argv[1]
        end_date = sys.argv[2]
    else:
        start_date = conf['start_date']
        end_date = conf['end_date']

    start_date = get_date_from_string(start_date)
    end_date = get_date_from_string(end_date)

    main(start_date, end_date, conf)

