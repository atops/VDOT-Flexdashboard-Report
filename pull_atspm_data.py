# -*- coding: utf-8 -*-
"""
Created on Fri Nov 22 13:37:39 2019

@author: Alan Toppen
"""

# -*- coding: utf-8 -*-

from datetime import datetime, timedelta
from multiprocessing.dummy import Pool
#from multiprocessing import Pool
import pandas as pd
import sqlalchemy as sq
import sys
import time
import re
import itertools
import yaml
import urllib.parse

# import urllib3
# urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

import s3io



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


def get_atspm_engine_(username, password, hostname = None, database = None, dsn = None):
    password = urllib.parse.quote_plus(password)
    if dsn is None:
        engine = sq.create_engine(rf'mssql+pymssql://{username}:{password}@{hostname}/{database}')
    else:
        # Should probably add some error handling here
        engine = sq.create_engine(rf'mssql+pyodbc://{username}:{password}@{dsn}')
    return engine


def get_atspm_engine(cred):
    return get_atspm_engine_(
        username=cred['ATSPM_UID'], 
        password=cred['ATSPM_PWD'], 
        hostname=cred['ATSPM_HOST'], 
        database=cred['ATSPM_DB'],
        dsn=cred['ATSPM_DSN'])


def get_aurora_engine_(username, password, hostname = None, database = None, dsn = None):
    password = urllib.parse.quote_plus(password)
    if dsn is None:
        engine = sq.create_engine(rf'mysql+pymysql://{username}:{password}@{hostname}/{database}')
    else:
        # Should probably add some error handling here
        engine = sq.create_engine(rf'mysql+pyodbc://{username}:{password}@{dsn}')
    return engine


def get_aurora_engine(cred):
    return get_aurora_engine_(
        username=cred['RDS_USERNAME'], 
        password=cred['RDS_PASSWORD'], 
        hostname=cred['RDS_HOST'], 
        database=cred['RDS_DATABASE'],
        dsn=cred['RDS_DSN'])


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
        print('{} | {} Starting...'.format(s, date_str))

        try:
            with engine.connect() as conn:
                df = pd.read_sql(
                    sql=query.format(
                        s.zfill(5),
                        re.sub('\d{3}$', '', str(start_date)),
                        re.sub('\d{3}$', '', str(end_date))),
                    con=conn)

            if len(df) == 0:
                print('|{} no event data for this signal on {}.'.format(s, date_str))

            else:

                df = add_barriers(df)
                df = (df.assign(
                            SignalID = lambda x: x.SignalID.astype('int'),
                            EventCode = lambda x: x.EventCode.astype('int'),
                            EventParam = lambda x: x.EventParam.astype('int'))
                        .sort_values(
                    ['SignalID', 'Timestamp', 'EventCode', 'EventParam']))

                print('writing to files...{} records'.format(len(df)))
                
                key = f"{conf['key_prefix']}/atspm/date={date_str}/atspm_{s}_{date_str}.parquet"
                s3io.s3_write_parquet(df, Bucket=conf['bucket'], Key=key)
                print('{}: {} seconds'.format(s, round(time.time()-t0, 1)))

        except Exception as e:
            print(s, e)

    except Exception as e:
        print(s, e)


if __name__ == '__main__':
    try:
        with open('Monthly_Report_AWS.yaml') as yaml_file:
            cred = yaml.load(yaml_file, Loader=yaml.Loader)

        engine = get_atspm_engine(cred)

        with engine.connect() as conn:
            Signals = pd.read_sql_table('Signals', conn)

        with open('Monthly_Report.yaml') as yaml_file:
            conf = yaml.load(yaml_file, Loader=yaml.FullLoader)


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
                    keys = s3io.s3_list_objects(
                        Bucket=conf['bucket'],
                        Prefix="{conf['key_prefix']}atspm/date={start_date.strftime('%Y-%m-%d')}",
                        max_results=1)
                    if len(keys) > 0:
                        start_date = (start_date + timedelta(days=1))
                        break
                    else:
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

        t0 = time.time()
        for date_ in dates:
            procs = 2 # min(os.cpu_count()*2, 16)
            with Pool(processes=procs) as pool: #18
                pool.starmap_async(pull_raw_atspm_data, list(itertools.product(signalids, [date_], [engine], [conf])))
                pool.close()
                pool.join()

        print('\n{} signals in {} days. Done in {} minutes'.format(len(signalids), len(dates), int((time.time()-t0)/60)))
    except Exception as e:
        print(str(e))
