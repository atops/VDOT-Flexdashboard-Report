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
#from multiprocessing.dummy import Pool
from multiprocessing import Pool
import itertools
import pprint

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


ath = boto3.client('athena', verify=False)
s3 = boto3.client('s3', verify=False)
pp = pprint.PrettyPrinter()


def get_keys(bucket, prefix, callback=lambda x: x):
    response = s3.list_objects_v2(
        Bucket=bucket, 
        Prefix=prefix)
    if 'Contents' in response.keys():
        for cont in response['Contents']:
            yield callback(cont['Key'])
    
    while 'NextContinuationToken' in response.keys():
        response = s3.list_objects_v2(
            Bucket=bucket, 
            Prefix=prefix, 
            ContinuationToken=response['NextContinuationToken'])
        for cont in response['Contents']:
            yield callback(cont['Key'])
            
            
def get_signalids(date_, conf):

    bucket = conf['bucket']
    prefix = 'detections/date={d}'.format(d=date_.strftime('%Y-%m-%d'))

    return get_keys(bucket, prefix, callback = lambda k: re.search('(?<=_)\d+(?=_)', k).group())


def get_det_config(date_, conf):
    '''
    date_ [Timestamp]
    conf [dict]
    '''

    def read_det_config(bucket, key):
    
        with io.BytesIO() as data:
            s3.download_fileobj(Bucket=bucket, Key=key, Fileobj=data)
            dc = pd.read_feather(data)[['SignalID', 'Detector', 'DetectionTypeDesc']]
        dc.loc[dc.DetectionTypeDesc.isna(), 'DetectionTypeDesc'] = '[]'
        return dc


    date_str = date_.strftime('%Y-%m-%d')

    bucket = conf['bucket']
    
    with io.BytesIO() as data:
        s3.download_fileobj(
            Bucket=bucket,
            Key=f'mark/bad_detectors/date={date_str}/bad_detectors_{date_str}.parquet',
            Fileobj=data)

        bd = pd.read_parquet(data).assign(
            SignalID = lambda x: x.SignalID.astype('int64'),
            Detector = lambda x: x.Detector.astype('int64'))

    dc_prefix = 'atspm_det_config_good/date={d}'.format(d=date_str)
    dc_keys = get_keys(bucket, dc_prefix)
    
    dc = pd.concat(list(map(lambda k: read_det_config(bucket, k), dc_keys)))
    
    df = pd.merge(dc, bd, how='outer', on=['SignalID','Detector']).fillna(value={'Good_Day': 1})
    df = df.loc[df.Good_Day==1].drop(columns=['Good_Day'])

    return df


def get_aog(signalid, date_, det_config, conf):
    '''
    date_ [Timestamp]
    '''
    try:
        date_str = date_.strftime('%Y-%m-%d')

        bucket = conf['bucket']

        all_hours = pd.date_range(date_, periods=25, freq='H')

        with io.BytesIO() as data:
            s3.download_fileobj(
                Bucket=bucket,
                Key=f'detections/date={date_str}/de_{signalid}_{date_str}.parquet',
                Fileobj=data)

            detection_events = pd.read_parquet(data)

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
            df_aog = (df.assign(Hour=lambda x: x.DetTimeStamp.dt.floor('H'))
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
                     .reset_index(level=['IntervalStart'])
                     .assign(IntervalEnd=lambda x: x.IntervalStart.shift(-1))
                     .assign(IntervalDuration=lambda x: (x.IntervalEnd - x.IntervalStart).dt.total_seconds())
                     .assign(Hour=lambda x: x.IntervalStart.dt.floor('H'))
                     .groupby(['Hour', 'SignalID', 'Phase', 'Interval']).sum())

            df_gc['Duration'] = df_gc.groupby(level=[0, 1, 2]).transform('sum')
            df_gc['gC'] = df_gc['IntervalDuration']/df_gc['Duration']
            gC = (df_gc.reset_index('Interval')
                  .query('Interval == 1')
                  .drop(columns=['Interval'])
                  .rename(columns={'IntervalDuration': 'Green_Duration'}))

            aog = pd.concat([aog, gC], axis=1).assign(pr=lambda x: x.AOG/x.gC)

            print('.', end='')

            return aog

    except Exception as e:
        print('{s}|{d}: Error--{e}'.format(e=e, s=signalid, d=date_str))
        return pd.DataFrame()


def main(start_date, end_date):

    with open('Monthly_Report.yaml') as yaml_file:
        conf = yaml.load(yaml_file, Loader=yaml.Loader)

    dates = pd.date_range(start_date, end_date, freq='1D')
    #dates = pd.date_range('2021-05-01', '2021-05-17')

    for date_ in dates:
      try:
        t0 = time.time()
        print(date_)
    
        print('Getting detector configuration...', end='')
        det_config = get_det_config(date_, conf)
        print('done.')
        print('Getting signals...', end='')
        signalids = get_signalids(date_, conf)
        print('done.')
    
        with Pool(8) as pool:
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
    
        with io.BytesIO() as data:
            df.to_parquet(data)
            data.seek(0)
            date_str = date_.strftime('%Y-%m-%d')
            s3.upload_fileobj(
                Fileobj=data,
                Bucket=conf['bucket'],
                Key=f'mark/arrivals_on_green/date={date_str}/aog_{date_str}.parquet')
        
        partition_query = '''ALTER TABLE arrivals_on_green add partition (date="{d}")
                             location "s3://{b}/mark/arrivals_on_green/date={d}/"
                          '''.format(b=conf['bucket'], d=date_.date())

        response = ath.start_query_execution(
                QueryString = partition_query,
                QueryExecutionContext={'Database': conf['athena']['staging_dir']},
                ResultConfiguration={'OutputLocation': conf['athena']['staging_dir']})
        print('')
        print('Update Athena partitions:')
        pp.pprint(response)
        
        num_signals = len(list(set(df.SignalID.values)))
        t1 = round(time.time() - t0, 1)
        print(f'{num_signals} signals done in {t1} seconds.')
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

    main(start_date, end_date)

