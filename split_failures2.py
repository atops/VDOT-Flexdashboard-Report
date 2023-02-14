# -*- coding: utf-8 -*-
"""
Created on Sat Dec  2 13:27:20 2017

@author: Alan.Toppen
"""

import os
import pandas as pd
import numpy as np
#import sqlalchemy as sq
from datetime import datetime, timedelta

import yaml
#import feather
from pandas.tseries.offsets import Day #, MonthEnd

from dask import delayed, compute

import boto3
import polling

ath = boto3.client('athena')
s3 = boto3.client('s3')
s3r = boto3.resource('s3')

def query_athena(query, database, output_bucket):

    response = ath.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database
        },
        ResultConfiguration={
            'OutputLocation': 's3://{}'.format(output_bucket)
        }
    )
    print ('Started query.')
    # Wait for s3 object to be created
    polling.poll(
            lambda: 'Contents' in s3.list_objects(Bucket=output_bucket,
                                                  Prefix=response['QueryExecutionId']),
            step=0.5,
            poll_forever=True)
    print ('Query complete.')
    key = '{}.csv'.format(response['QueryExecutionId'])
    s3.download_file(Bucket=output_bucket, Key=key, Filename=key)
    df = pd.read_csv(key)
    os.remove(key)

    print ('Results downloaded.')
    return df

## SPLIT FAILURES

def get_split_failures(start_date, end_date, signals_string):

    with open('Monthly_Report.yaml') as yaml_file:
        conf = yaml.load(yaml_file, Loader=yaml.Loader)
    athena = conf['athena']

    between_clause = "= '{}'".format(start_date.strftime('%Y-%m-%d'))

    cycle_query = """SELECT DISTINCT SignalID, Phase, PhaseStart
                     FROM {db}.CycleData
                     WHERE Phase not in (2,6)
                     AND EventCode = 9
                     AND date {b}
                     AND SignalID in {s}
                     """.format(db=athena['database'], b=between_clause, s=signals_string)

    detector_query = """SELECT DISTINCT SignalID, Phase, EventCode, DetTimeStamp as DetOn, DetDuration
                        FROM {db}.DetectionEvents
                        WHERE Phase not in (2,6)
                        AND date {b}
                        AND SignalID in {s}
                        """.format(db=athena['database'], b=between_clause, s=signals_string)

    print(between_clause)

    sor = (query_athena(cycle_query, athena['database'], os.path.basename(athena['staging_dir']))
            .assign(PhaseStart = lambda x: pd.to_datetime(x.PhaseStart)))

    det = (query_athena(detector_query, athena['database'], os.path.basename(athena['staging_dir']))
            .assign(DetOn = lambda x: pd.to_datetime(x.DetOn))
            .assign(DetOff = lambda x: x.DetOn + pd.to_timedelta(x.DetDuration, unit='s')))

    sf = (pd.merge_asof(sor.sort_values(['PhaseStart']),
                       det.sort_values(['DetOff']),
                       by=['SignalID','Phase'],
                       left_on=['PhaseStart'],
                       right_on=['DetOff'], direction = 'forward')
            .assign(pre = lambda x: (x.DetOn - x.PhaseStart).astype('timedelta64[s]'))
            .assign(post = lambda x: (x.DetOff - x.PhaseStart).astype('timedelta64[s]'))
            .assign(split_failure = lambda x: (np.where((x.pre<0) & (x.post>10) & (x.post-x.pre<200), 1, 0)))
            .filter(items=['SignalID','Phase','PhaseStart','EventCode','pre','post','split_failure']))

    sf = (sf.assign(Hour = lambda x: x.PhaseStart.dt.floor('H'))
            .groupby(['SignalID','Phase','Hour'])['split_failure']
            .agg(['sum','count'])
            .rename(columns={'sum':'sf', 'count':'cycles'})
            .assign(sf_freq = lambda x: x.sf.astype('float')/x.cycles.astype('float'))
            #.filter(items=['sf_freq'])
            .reset_index())

    return sf

def helper(date_, conf):
    start_date = date_
    end_date = start_date #+ pd.DateOffset(days=1) - pd.DateOffset(seconds=0.1)

    sf = get_split_failures(start_date, end_date, signals_string)

    # Moved into function since we're doing so many days. Reduce memory footprint
    if len(sf) > 0:
        sf.Hour = sf.Hour.dt.tz_localize(conf['timezone'], ambiguous=True)
        sf = sf.reset_index(drop=True)
        sf.to_feather('sf_{}.feather'.format(date_.strftime('%Y-%m-%d')))

if __name__=='__main__':

    with open('Monthly_Report.yaml') as yaml_file:
        conf = yaml.load(yaml_file, Loader=yaml.Loader)

    start_date = conf['start_date']
    if start_date == 'yesterday':
        start_date = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    end_date = conf['end_date']
    if end_date == 'yesterday':
        end_date = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')

    corridors = pd.read_feather(conf['corridors_filename'])
    corridors = corridors[~corridors.SignalID.isna()]

    signals_list = list(corridors.SignalID.values)

    signals_string = "({})".format(','.join(signals_list))

    dates = pd.date_range(start_date, end_date, freq=Day())



    results = []
    for d in dates:
        x = delayed(helper)(d, conf)
        results.append(x)
    compute(*results)

