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
import os
import itertools
import yaml
import re
import psutil

import gcsio
from spm_events import etl_main
from config import get_date_from_string
from pull_atspm_data import get_atspm_engine

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

    det_config = det_config[det_config.SignalID==s]

    start_date = date_
    end_date = date_ + pd.DateOffset(days=1)

    t0 = time.time()

    try:
        bucket = conf['bucket']
        key_prefix = conf['key_prefix']

        with open('Monthly_Report_AWS.yaml') as yaml_file:
            cred = yaml.load(yaml_file, Loader=yaml.Loader)

        engine = get_atspm_engine(cred)

        atspm_query = f"""SELECT DISTINCT Timestamp, SignalID, EventCode, EventParam
                          FROM Controller_Event_Log
                          WHERE Timestamp >= '{start_date.strftime('%F')}' 
                          AND Timestamp < {end_date.strftime('%F')}
                          AND SignalID = '{s}' 
                          ORDER BY Timestamp, EventCode, EventParam"""

        with engine.connect() as conn:
            df = pd.read_sql_query(atspm_query, con=conn)

        if len(df)==0:
            print(f'{date_str} | {s} | No event data for this signal')

        if len(det_config)==0:
            print(f'{date_str} | {s} | No detector configuration data for this signal')

        if len(df) > 0 and len(det_config) > 0:

            c, d = etl_main(df, det_config)

            if len(c) > 0 and len(d) > 0:
                gcsio.s3_write_parquet(c, conf['bucket'], "{conf['key_prefix']}/cycles/date={date_str}/cd_{s}_{date_str}.parquet")
                gcsio.s3_write_parquet(d, conf['bucket'], "{conf['key_prefix']}/detections/date={date_str}/de_{s}_{date_str}.parquet")

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

    corridors_filename = re.sub('\..*', '.parquet', conf['corridors_filename_s3'])
    corridors = gcsio.get_corridors(conf['bucket'], f"{conf['key_prefix']}/{corridors_filename}", keep_signalids_as_strings = True)

    signalids = list(corridors.SignalID.values)
  

    #-----------------------------------------------------------------------------------------
    # Placeholder for manual override of signalids
    #signalids = [7053]
    #-----------------------------------------------------------------------------------------

    t0 = time.time()

    for date_ in dates:

        date_str = date_.strftime('%Y-%m-%d')
        print(date_str)

        det_config = (gcsio.get_det_config(date_, conf)
                     .rename(columns={'CallPhase': 'Call Phase'})
                     .groupby(['SignalID','Call Phase'])
                     .apply(lambda group: group.assign(CountDetector = group.CountPriority == group.CountPriority.min()))
                     .reset_index())

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


if __name__=='__main__':

    with open('Monthly_Report.yaml') as yaml_file:
        conf = yaml.load(yaml_file, Loader=yaml.Loader)

    if len(sys.argv) > 1:
        start_date = sys.argv[1]
        end_date = sys.argv[2]
    else:
        start_date = conf['start_date']
        end_date = conf['end_date']

    start_date = conf['start_date']
    end_date = conf['end_date']
    
    start_date = get_date_from_string(start_date)
    end_date = get_date_from_string(end_date)
    
    main(start_date, end_date, conf)

