# -*- coding: utf-8 -*-
"""
Created on Mon Nov 27 16:27:29 2017

@author: Alan.Toppen
"""
import sys
from datetime import datetime, timedelta
#from multiprocessing.dummy import Pool
from multiprocessing import Pool
import pandas as pd
import sqlalchemy as sq
import pyodbc
import time
import os
import itertools
from spm_events import etl_main
import boto3
import yaml
import feather
from glob import glob

os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'

s3 = boto3.client('s3')
ath = boto3.client('athena')


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

def etl2(s, date_):

    date_str = date_.strftime('%Y-%m-%d')


    det_config_good = det_config[det_config.SignalID==s]

    query = """SELECT * FROM Controller_Event_Log
               WHERE SignalID = '{}'
               AND EventCode in (1,4,5,6,8,9,31,81,82)
               AND (Timestamp BETWEEN '{}' AND '{}');
               """
    start_date = date_
    end_date = date_ + pd.DateOffset(days=1) - pd.DateOffset(seconds=0.1)


    t0 = time.time()

    print('{} | {} Starting...'.format(s, date_str))

    try:
        print('|{} reading from database...'.format(s))
        with engine.connect() as conn:
            df = pd.read_sql(sql=query.format(s, str(start_date)[:-3], str(end_date)[:-3]), con=conn)
            df = (df.rename(columns={'Timestamp':'TimeStamp'})
                    .assign(SignalID = df.SignalID.astype('int')))

        if len(df)==0:
            print('|{} no event data for this signal on {}.'.format(s, date_str))

        if len(det_config_good)==0:
            print('|{} no detector configuration data for this signal on {}.'.format(s, date_str))

        if len(df) > 0 and len(det_config_good) > 0:

            print('|{} creating cycles and detection events...'.format(s))
            c, d = etl_main(df, det_config_good)

            if len(c) > 0 and len(d) > 0:

                c.to_parquet('s3://vdot-spm/cycles/date={}/cd_{}_{}.parquet'.format(date_str, s, date_str),
                             allow_truncated_timestamps=True)

                d.to_parquet('s3://vdot-spm/detections/date={}/de_{}_{}.parquet'.format(date_str, s, date_str),
                             allow_truncated_timestamps=True)


                print('{}: {} seconds'.format(s, int(time.time()-t0)))
            else:
                print('{}: {} seconds -- no cycles'.format(s, int(time.time()-t0)))


    except Exception as e:
        print(s, e)





if __name__=='__main__':

    t0 = time.time()

    if os.name=='nt':

        uid = os.environ['VDOT_ATSPM_USERNAME']
        pwd = os.environ['VDOT_ATSPM_PASSWORD']

        engine = sq.create_engine('mssql+pyodbc://{}:{}@sqlodbc'.format(uid, pwd),
                                  pool_size=20)

    elif os.name=='posix':

        def connect():
            return pyodbc.connect(
                'Driver=FreeTDS;' +
                'SERVER={};'.format(os.environ['VDOT_ATSPM_SERVER_INSTANCE']) +
                #'DATABASE={};'.format(os.environ['VDOT_ATSPM_DB']) +
                'PORT=1433;' +
                'UID={};'.format(os.environ['VDOT_ATSPM_USERNAME']) +
                'PWD={};'.format(os.environ['VDOT_ATSPM_PASSWORD']) +
                'TDS_Version=8.0;')

        engine = sq.create_engine('mssql://', creator=connect)







    with open('Monthly_Report.yaml') as yaml_file:
        conf = yaml.load(yaml_file, Loader=yaml.Loader)


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

    #-----------------------------------------------------------------------------------------
    # Placeholder for manual override of start/end dates
    #start_date = '2018-10-01'
    #end_date = '2018-10-04'
    #-----------------------------------------------------------------------------------------

    dates = pd.date_range(start_date, end_date, freq='1D')

    corridors_filename = conf['corridors_filename']
    corridors = feather.read_dataframe(corridors_filename)
    corridors = corridors[~corridors.SignalID.isna()]

    signalids = list(corridors.SignalID.astype('int').values)

    t0 = time.time()

    for date_ in dates:

        date_str = date_.strftime('%Y-%m-%d')
        dc_fn = 'ATSPM_Det_Config_Good_{}.feather'.format(date_str)

        det_config = feather.read_dataframe(dc_fn)\
                    .assign(SignalID = lambda x: x.SignalID.astype('int64'))\
                    .assign(Detector = lambda x: x.Detector.astype('int64'))\
                    .rename(columns={'CallPhase': 'Call Phase'})

        bad_detectors = pd.read_feather('bad_detectors.feather')\
                    .assign(SignalID = lambda x: x.SignalID.astype('int64'))\
                    .assign(Detector = lambda x: x.Detector.astype('int64'))

        left = det_config
        right = bad_detectors[bad_detectors.Date==date_]

        det_config = pd.merge(left, right, how = 'outer', indicator = True)\
                       .loc[lambda x: x._merge=='left_only']\
                       .drop(['Date','_merge'], axis=1)\
                       .groupby(['SignalID','Call Phase'])\
                       .apply(lambda group: group.assign(CountDetector = group.CountPriority == group.CountPriority.min()))

        ncores = os.cpu_count()
        #-----------------------------------------------------------------------------------------
        with Pool(ncores * 5) as pool:
            asyncres = pool.starmap_async(etl2, list(itertools.product(signalids, [date_])))
            pool.close()
            pool.join()
        #-----------------------------------------------------------------------------------------



    os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'

    response_repair_cycledata = ath.start_query_execution(
                QueryString='MSCK REPAIR TABLE cycledata',
                QueryExecutionContext={'Database': 'vdot_spm'},
                ResultConfiguration={'OutputLocation': 's3://vdot-spm-athena'})
    response_repair_detection_events = ath.start_query_execution(
                QueryString='MSCK REPAIR TABLE detectionevents',
                QueryExecutionContext={'Database': 'vdot_spm'},
                ResultConfiguration={'OutputLocation': 's3://vdot-spm-athena'})

    print('\n{} signals in {} days. Done in {} minutes'.format(len(signalids), len(dates), int((time.time()-t0)/60)))


    while True:
        response1 = s3.list_objects(Bucket='vdot-spm-athena', Prefix=response_repair_cycledata['QueryExecutionId'])
        response2 = s3.list_objects(Bucket='vdot-spm-athena', Prefix=response_repair_detection_events['QueryExecutionId'])
        if 'Contents' in response1 and 'Contents' in response2:
            print('done.')
            break
        else:
            time.sleep(2)
            print('.', end='')
