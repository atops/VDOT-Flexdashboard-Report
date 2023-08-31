import sys
import psutil
import yaml
import pandas as pd
import numpy as np
import time
from multiprocessing import get_context
from datetime import datetime, timedelta
import itertools
import posixpath

from config import get_date_from_string
from pull_atspm_data import get_atspm_engine
import gcsio



def weighted_mean(df, values, weights):
    d = df[values]
    w = df[weights]
    return (d * w).sum() / w.sum()



def get_approach_delay(signalid, date_, det_config, conf, cred, per='H'):

    date_str = date_.strftime('%F')

    bucket = conf['bucket']
    key_prefix = conf['key_prefix']

    engine = get_atspm_engine(cred)

    # Read eventlog, detections and cycle data
    with engine.connect() as conn:
        atspm = pd.read_sql_query(
            f"""SELECT SignalID, Timestamp, EventParam 
                FROM Controller_Event_Log 
                WHERE SignalID = '{signalid}'
                AND Timestamp >= '{date_str}' 
                AND Timestamp < '{(date_ + pd.Timedelta(1, unit='D')).strftime('%F')}' 
                AND EventCode = 132
                ORDER BY SignalID, Timestamp""", con=conn)
    atspm['Timestamp'] = pd.to_datetime(atspm.Timestamp)
    atspm['EventParam'] = atspm.EventParam.astype(int)
    
    # Get cycle lengths and max cycle length for filtering outliers
    cl = atspm.rename(columns={'EventParam': 'CycleLength'})
    cl.loc[cl.CycleLength==0, 'CycleLength'] = np.nan
    maxcl = cl.CycleLength.max() if cl.CycleLength.max() > 0 else 180  # if all free, use nominal cycle length for filtering outliers

    de = gcsio.s3_read_parquet(
        Bucket=conf['bucket'], 
        Key=posixpath.join(conf['key_prefix'], f'detections/date={date_str}/de_{signalid}_{date_str}.parquet'))
    de = pd.merge(de, det_config, on=['SignalID', 'Detector'], how='left')
    de = de.sort_values(['DetTimeStamp', 'SignalID', 'Phase'])
    
    cd = gcsio.s3_read_parquet(
        Bucket=conf['bucket'], 
        Key=posixpath.join(conf['key_prefix'], f'cycles/date={date_str}/cd_{signalid}_{date_str}.parquet'), 
        columns=['SignalID', 'Phase', 'EventCode', 'PhaseStart', 'TermType'])
    cd = cd[cd.EventCode==1].rename(columns={'PhaseStart': 'GreenStart'}).drop(columns=['EventCode'])
    cd = cd.sort_values(['GreenStart', 'SignalID', 'Phase'])

    # Delay per vehicle
    # Delay is the time between vehicle arrival (projected at stop bar) to next start of green interval, or 0 if arrival is during green interval.
    dl = pd.merge_asof(
        left=de, 
        right=cd, 
        left_on=['DetTimeStamp'], 
        right_on=['GreenStart'], 
        by=['SignalID', 'Phase'], 
        direction='forward')

    dl['delay'] = (dl.GreenStart - dl.DetTimeStamp).dt.total_seconds() # duration between arrival and subsequent start of green
    dl.loc[dl.EventCode==1, 'delay'] = 0 # No delay if arriving on green
    dl = dl.sort_values(['SignalID', 'Phase', 'DetTimeStamp'])

    # Filter outliers defined as delay > 3 x cycle length (use daily max cycle length for free)
    delay = pd.merge_asof(
        left=dl.sort_values(['CycleStart', 'SignalID', 'Phase']), 
        right=cl, left_on=['CycleStart'], 
        right_on=['Timestamp'], 
        by=['SignalID'], 
        direction='backward')

    delay['CycleLength'] = delay['CycleLength'].fillna(maxcl)
    delay = delay[delay.delay < delay.CycleLength * 3]

    if not delay.empty:
        # Hourly aggregation, weighted by volume (based on 'Advanced Count' detectors)
        delay['Hour'] = delay.DetTimeStamp.dt.floor(per)
        c = delay.groupby(['SignalID', 'Hour'])['delay'].count().rename('vol')
        z = delay.groupby(['SignalID', 'Phase', 'Hour'])['delay'].agg(['mean', 'count'])
        d = z.groupby(level=['SignalID', 'Hour'], group_keys=False).apply(weighted_mean, 'mean', 'count').rename('delay')
        x = pd.concat([d, c], axis=1)

        signalids = [signalid]
        all_hours = pd.date_range(date_str, pd.Timestamp(date_str) + pd.Timedelta(1, unit='days') - pd.Timedelta(1, unit='hours'), freq=per)

        return x.reindex(itertools.product(signalids, all_hours), fill_value=0)



def get_det_config_approach_delay(date_, conf):

    # Approach Delay uses 'Advanced Count' detectors only.
    det_config = gcsio.get_det_config(date_, conf)

    det_config = det_config[~det_config.DateAdded.isna()]
    det_config = det_config.loc[det_config.groupby(['SignalID', 'Detector']).DateAdded.idxmax()]

    det_config = det_config.set_index(['SignalID', 'CallPhase'])
    det_config['minCountPriority'] = det_config.CountPriority.groupby(level=['SignalID', 'CallPhase']).min()

    det_config['CountDetector'] = det_config['CountPriority'] == det_config['minCountPriority']
    det_config = det_config.drop(columns=['minCountPriority']).reset_index()

    det_config = det_config[(~det_config.DetectionTypeDesc.isna()) & (det_config.DetectionTypeDesc.str.contains('Advanced Count'))]

    return det_config



def main(start_date, end_date, conf, cred):

    dates = pd.date_range(start_date, end_date, freq='1D')

    for date_ in dates:
        try:
            t0 = time.time()

            date_str = date_.strftime('%Y-%m-%d')
            print(date_str)

            signalids = gcsio.get_signalids(date_, conf)
            det_config = gcsio.get_det_config(date_, conf)

            bucket = conf['bucket']
            key_prefix = conf['key_prefix']



            print('1 hour')
            nthreads = round(psutil.virtual_memory().total/1e9)  # ensure 1 MB memory per thread
            with get_context('spawn').Pool(processes=nthreads) as pool:
                results = pool.starmap_async(
                    get_approach_delay,
                    list(itertools.product(signalids, [date_], [det_config], [conf], [cred], ['H'])))
                pool.close()
                pool.join()

            dfs = results.get()

            df = pd.concat(dfs).reset_index()

            gcsio.s3_write_parquet(
                df,
                Bucket=bucket,
                Key=f'{key_prefix}/mark/approach_delay_1hr/date={date_str}/ad_{date_str}.parquet')

            num_signals = len(signalids)
            t1 = round(time.time() - t0, 1)
            print(f'\n{num_signals} signals done in {t1} seconds.')



            print('\n15 minutes')
            nthreads = round(psutil.virtual_memory().total/1e9)  # ensure 1 MB memory per thread
            with get_context('spawn').Pool(processes=nthreads) as pool:
                results = pool.starmap_async(
                    get_approach_delay,
                    list(itertools.product(signalids, [date_], [det_config], [conf], [cred], ['15min'])))
                pool.close()
                pool.join()

            dfs = results.get()

            df = pd.concat(dfs).reset_index()

            gcsio.s3_write_parquet(
                df,
                Bucket=bucket,
                Key=f'{key_prefix}/mark/approach_delay_15min/date={date_str}/ad_{date_str}.parquet')

            num_signals = len(signalids)
            t1 = round(time.time() - t0, 1)
            print(f'\n{num_signals} signals done in {t1} seconds.')


        except Exception as e:
            print(f'{date_}: Error: {e}')


if __name__=='__main__':

    with open('Monthly_Report.yaml') as yaml_file:
        conf = yaml.load(yaml_file, Loader=yaml.Loader)

    with open('Monthly_Report_AWS.yaml') as yaml_file:
        cred = yaml.load(yaml_file, Loader=yaml.Loader)

    if len(sys.argv) > 1:
        start_date = sys.argv[1]
        end_date = sys.argv[2]
    else:
        start_date = conf['start_date']
        end_date = conf['end_date']

    start_date = get_date_from_string(start_date)
    end_date = get_date_from_string(end_date)

    main(start_date, end_date, conf, cred)
