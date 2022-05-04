# -*- coding: utf-8 -*-
"""
Created on Sat Feb 15 15:15:31 2020

@author: Alan.Toppen
"""

import yaml
import time
import sys
from datetime import datetime, timedelta
import boto3
import pandas as pd
import io
import re
from multiprocessing import get_context
import itertools
import pprint
import urllib3

from s3io import *


pp = pprint.PrettyPrinter()



def get_aog(signalid, date_, det_config, conf, per='H'):
    '''
    date_ [Timestamp]
    '''
    try:
        bucket = conf['bucket']
    
        date_str = date_.strftime('%Y-%m-%d')
        all_hours = pd.date_range(date_, periods=25, freq='H')
    
        detection_events = read_parquet(
            Bucket=bucket, 
            Key=f'detections/date={date_str}/de_{signalid}_{date_str}.parquet')
    
        df = (pd.merge(
                detection_events,
                det_config[det_config.DetectionTypeDesc.str.contains('Advanced Count')],
                on=['SignalID', 'Detector'],
                how='left'))
        df = df[~df.DetectionTypeDesc.isna()]
    
        if df.empty:
            print('#', end='')
            return pd.DataFrame()
        else:
            df_aog = (df.assign(Hour=lambda x: x.DetTimeStamp.dt.floor(per))
                      .rename(columns={'Detector': 'Arrivals',
                                       'EventCode': 'Interval'})
                      .groupby(['Hour', 'SignalID', 'Phase', 'Interval'])
                      .count()[['Arrivals']])
            df_aog['All_Arrivals'] = df_aog.groupby(level=[0, 1, 2]).transform('sum')
            df_aog['AOG'] = df_aog['Arrivals']/df_aog['All_Arrivals']
    
            aog = (df_aog.reset_index('Interval')
                   .query('Interval == 1')
                   .drop(columns=['Interval'])
                   .rename(columns={'Arrivals': 'Green_Arrivals'}))
    
            df_gc = (df[['SignalID', 'Phase', 'PhaseStart', 'EventCode']]
                     .drop_duplicates()
                     .rename(columns={'PhaseStart': 'IntervalStart',
                                      'EventCode': 'Interval'})
                     .assign(IntervalDuration=0)
                     .set_index(['SignalID', 'Phase', 'IntervalStart']))
    
            x = pd.DataFrame(
                    data={'Interval': None, 'IntervalDuration': 0},
                    index=pd.MultiIndex.from_product(
                            [df_gc.index.levels[0],
                             df_gc.index.levels[1],
                             all_hours],
                            names=['SignalID', 'Phase', 'IntervalStart']))
    
            df_gc = (pd.concat([df_gc, x])
                     .sort_index()
                     .ffill() # fill forward missing Intervals for on the hour rows
                     .reset_index(level=['IntervalStart']))
            df_gc['IntervalEnd'] = df_gc.groupby(level=['SignalID','Phase']).shift(-1)['IntervalStart']
            df_gc['IntervalDuration'] = (df_gc.IntervalEnd - df_gc.IntervalStart).dt.total_seconds()
            df_gc['Hour'] = df_gc.IntervalStart.dt.floor(per)
            df_gc = df_gc.groupby(['Hour', 'SignalID', 'Phase', 'Interval']).sum()
    
            df_gc['Duration'] = df_gc.groupby(level=[0, 1, 2]).transform('sum')
            df_gc['gC'] = df_gc['IntervalDuration']/df_gc['Duration']
            gC = (df_gc.reset_index('Interval')
                  .query('Interval == 1')
                  .drop(columns=['Interval'])
                  .rename(columns={'IntervalDuration': 'Green_Duration'}))
    
            aog = pd.concat([aog, gC], axis=1).assign(pr=lambda x: x.AOG/x.gC)
            aog = aog[~aog.Green_Arrivals.isna()]
            
            print('.', end='')
    
            return aog
    
    except Exception as e:
        print('{s}|{d}: Error--{e}'.format(e=e, s=signalid, d=date_str))
        return pd.DataFrame()


def main(start_date, end_date, conf):

    #-----------------------------------------------------------------------------------------
    # Placeholder for manual override of start/end dates
    # start_date = '2021-05-04'
    # end_date = '2021-05-04'
    #-----------------------------------------------------------------------------------------

    dates = pd.date_range(start_date, end_date, freq='1D')

    for date_ in dates:
        try:
            t0 = time.time()
            date_str = date_.strftime('%Y-%m-%d')
            print(date_str)
    
            det_config = get_det_config(date_, conf)
            signalids = get_signalids(date_, conf)


            print('1 hour')
            with get_context('spawn').Pool(24) as pool:
                results = pool.starmap_async(
                    get_aog,
                    list(itertools.product(signalids, [date_], [det_config], [conf])))
                pool.close()
                pool.join()

            dfs = results.get()

            df = (pd.concat(dfs)
                  .reset_index()[['SignalID', 'Phase', 'Hour', 'AOG', 'pr', 'All_Arrivals']]
                  .rename(columns={'Phase': 'CallPhase',
                                   'Hour': 'Date_Hour',
                                   'AOG': 'aog',
                                   'All_Arrivals': 'vol'})
                  .sort_values(['SignalID', 'Date_Hour', 'CallPhase'])
                  .fillna(value={'vol': 0})
                  .assign(SignalID=lambda x: x.SignalID.astype('str'),
                          CallPhase=lambda x: x.CallPhase.astype('str'),
                          vol=lambda x: x.vol.astype('int32')))

            write_parquet(
                df,
                Bucket=conf['bucket'], 
                Key=f'mark/arrivals_on_green/date={date_str}/aog_{date_str}.parquet')

            partition_query = '''ALTER TABLE arrivals_on_green add partition (date="{d}")
                                 location "s3://{b}/mark/arrivals_on_green/date={d}/"
                              '''.format(b=conf['bucket'], d=date_.date())

            response = athena.start_query_execution(
                    QueryString = partition_query,
                    QueryExecutionContext={'Database': conf['athena']['staging_dir']},
                    ResultConfiguration={'OutputLocation': conf['athena']['staging_dir']})
            print('')
            print('Update Athena partitions:')
            pp.pprint(response)

            num_signals = len(list(set(df.SignalID.values)))
            t1 = round(time.time() - t0, 1)
            print(f'\n{num_signals} signals done in {t1} seconds.')

        except Exception as e:
            print(f'{date_}: Error: {e}')



if __name__=='__main__':
    
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

    main(start_date, end_date, conf)

