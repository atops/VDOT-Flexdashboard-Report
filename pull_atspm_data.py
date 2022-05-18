# -*- coding: utf-8 -*-
"""
Created on Fri Nov 22 13:37:39 2019

@author: Alan Toppen
"""

# -*- coding: utf-8 -*-

from datetime import datetime, timedelta
from multiprocessing import get_context, Pool
import pandas as pd
import sqlalchemy as sq
import pyodbc
import sys
import time
import os
import re
import io
import itertools
import boto3
import yaml
import pprint
import urllib.parse
import urllib3

from s3io import *

pp = pprint.PrettyPrinter()



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


def get_atspm_engine(username, password, hostname = None, database = None, dsn = None):
    password = urllib.parse.quote_plus(password)
    if dsn is None:
        engine = sq.create_engine(rf'mssql+pymssql://{username}:{password}@{hostname}/{database}')
    else:
        # Should probably add some error handling here
        engine = sq.create_engine(rf'mssql+pyodbc://{username}:{password}@{dsn}')
    return engine


def get_asof(s, conn):
    Asof = pd.read_sql_query(
        f'''SELECT SignalID, MIN(Timestamp) as Asof 
           FROM Controller_Event_Log 
           WHERE EventCode = 0 AND SignalID = '{s}'
           GROUP BY SignalID''', con=conn)
    if len(Asof): 
        Asof.Asof = Asof.Asof.dt.date
    return Asof


def add_barriers(df):
    '''
    Where main street phases (1,6, 2,5) terminate and side street phases (4,7, 3,8)
    start, add a barrier event (EventCode = 31). For some reason, Springfields
    controllers dont have this code, which is used to identify the start of
    a new cycle.

    Returns
    -------
    TYPE pandas data frame
        data frame of raw atspm data with event code 31 events inserted

    '''
    df0 = df
    
    df = df[df.EventCode.isin([0,12])].rename(columns={'EventParam': 'Phase'})
    df.loc[df.Phase.isin([1,2,5,6]), 'MajMin'] = 'major'
    df.loc[df.Phase.isin([3,4,7,8]), 'MajMin'] = 'minor'
    
    df = df.drop(columns=['Phase']).drop_duplicates().set_index(['SignalID','Timestamp','EventCode']).unstack()
    df = df.MajMin.reset_index()
    
    df['EventCode'] = 31
    df.loc[(df[0]=='major') & (df[12]=='minor'), 'EventParam'] = 1
    df.loc[(df[0]=='minor') & (df[12]=='major'), 'EventParam'] = 2
    df = df.drop(columns=[0,12])
    df = df[~df.EventParam.isna()]
    df.EventParam = df.EventParam.astype('int')
    
    df = pd.concat([df0, df]).sort_values(['SignalID', 'Timestamp', 'EventCode', 'EventParam'])

    return df


def pull_raw_atspm_data(s, date_, engine, conf):

    try:
        query = """SELECT * FROM [Controller_Event_Log]
                   WHERE SignalID = '{}'
                   AND (Timestamp BETWEEN '{}' AND '{}');
                   """

        start_date = date_
        end_date = date_ + pd.DateOffset(days=1) - pd.DateOffset(seconds=0.1)

        t0 = time.time()
        date_str = date_.strftime('%Y-%m-%d')
        print(f'{s} | {date_str} Starting...')

        try:
            with engine.connect() as conn:
                df = pd.read_sql(
                    sql=query.format(
                        s.zfill(5),
                        re.sub('\d{3}$', '', str(start_date)),
                        re.sub('\d{3}$', '', str(end_date))),
                    con=conn)

            if len(df) == 0:
                print(f'|{s} no event data for this signal on {date_str}.')

            else:
                bucket = conf['bucket']

                df = add_barriers(df)
                df.SignalID = df.SignalID.astype('int')
                df.EventCode = df.EventCode.astype('int')
                df.EventParam = df.EventParam.astype('int')
                df = df.sort_values(['SignalID', 'Timestamp', 'EventCode', 'EventParam'])

                print(f'writing to files...{len(df)} records')
                
                df.to_parquet(f's3://{bucket}/atspm/date={date_str}/atspm_{s}_{date_str}.parquet')
                
                print(f'{s}: {round(time.time()-t0, 1)} seconds')

        except Exception as e:
            print(s, e)

    except Exception as e:
        print(s, e)


if __name__ == '__main__':
    try:
        with open('Monthly_Report_AWS.yaml') as yaml_file:
            cred = yaml.load(yaml_file, Loader=yaml.Loader)

        with open('Monthly_Report.yaml') as yaml_file:
            conf = yaml.load(yaml_file, Loader=yaml.FullLoader)

        engine = get_atspm_engine(
            username=cred['ATSPM_UID'], 
            password=cred['ATSPM_PWD'], 
            hostname=cred['ATSPM_HOST'], 
            database=cred['ATSPM_DB'],
            dsn=cred['ATSPM_DSN'])

        with engine.connect() as conn:
            Signals = (pd.read_sql_table('Signals', conn)
                        .sort_values(['SignalID', 'VersionID'])
                        .groupby('SignalID')
                        .tail(1))

            asof = pd.concat([get_asof(s, conn) for s in Signals.SignalID.values])
            Signals = pd.merge(Signals, asof, left_on='SignalID', right_on='SignalID', how='left')

        bucket = conf['bucket']
        region = conf['region']
        atspm_table = conf['athena']['atspm_table']
        athena_database = conf['athena']['database']
        staging_dir = conf['athena']['staging_dir']
        x = re.split('/+', staging_dir) # split path elements into a list
        athena_bucket = x[1] # first path element that's not s3:
        athena_prefix = '/'.join(x[2:])

        Signals.to_parquet(f's3://{bucket}/Signals_{region}.parquet')

        if len(sys.argv) == 3:
            start_date = sys.argv[1]
            end_date = sys.argv[2]

        elif len(sys.argv) == 2:
            start_date = sys.argv[1]
            end_date = sys.argv[1]

        elif len(sys.argv) == 1:
            start_date = conf['start_date']
            if start_date == 'yesterday':
                start_date = datetime.today().date() - timedelta(days=1)
                while True:
                    keys = get_keys(s3, bucket, prefix="atspm/date={}".format(start_date.strftime('%Y-%m-%d')))
                    try:
                        next(keys)
                        start_date = start_date + timedelta(days=1)
                        break
                    except StopIteration:
                        start_date = start_date - timedelta(days=1)
                start_date = min(start_date, datetime.today().date() - timedelta(days=1))
            end_date = conf['end_date']
            if end_date == 'yesterday':
                end_date = (datetime.today().date() - timedelta(days=1))

        else:
            sys.exit("Too many command line arguments")
        
        # Placeholder for manual override of start/end dates
        #start_date = '2020-01-01'
        #end_date = '2020-05-11'

        dates = pd.date_range(start_date, end_date, freq='1D')


        #signalids = list(Signals[Signals.SignalID != 'null'].SignalID.astype('int').values)
        signalids = [s.lstrip('0') for s in Signals.SignalID.values]
        #signalids = list(Signals.SignalID.values)

        t0 = time.time()
        for date_ in dates:
            # Workaround. Pool is failing silently. Can't troubleshoot.
            for s in signalids:
                pull_raw_atspm_data(s, date_, engine, conf)
            # with get_context('spawn').Pool(2) as pool:
            #     pool.starmap_async(pull_raw_atspm_data, list(itertools.product(signalids, [date_], [engine], [conf])))
            #     pool.close()
            #     pool.join()

            date_str = date_.strftime('%Y-%m-%d')

            partition_query = f'''ALTER TABLE {atspm_table} ADD PARTITION (date="{date_str}")
                                  location "s3://{bucket}/atspm/date={date_str}"'''

            print('Update Athena partitions:')
            response = athena.start_query_execution(
                    QueryString = partition_query,
                    QueryExecutionContext={'Database': athena_database},
                    ResultConfiguration={'OutputLocation': staging_dir})
            pp.pprint(response)

        print(f'\n{len(signalids)} signals in {len(dates)} days. Done in {int((time.time()-t0)/60)} minutes')
    except Exception as e:
        print(str(e))
