# -*- coding: utf-8 -*-
"""
Created on Mon Sep 24 20:42:51 2018

@author: Alan.Toppen
"""

import os
import pandas as pd
import requests
import uuid
import polling
import time
import yaml
from datetime import datetime, timedelta
import zipfile
import io
import feather

pd.options.display.max_columns = 10


def get_tmc_data(start_date, end_date, tmcs, key, initial_sleep_sec=0):

    # Allow sleep time to space out requests when running in a loop
    time.sleep(initial_sleep_sec)

    uri = 'http://kestrel.ritis.org:8080/{}'

    # ----------------------------------------------------------
    payload = {
        "dates": [{
            "end": end_date,
            "start": start_date
        }],
        "dow": [2, 3, 4],
        "dsFields": [{
            "columns": [
                "SPEED", "REFERENCE_SPEED", "TRAVEL_TIME_MINUTES",
                "CONFIDENCE_SCORE"
            ],
            "dataSource":
            "vpp_inrix",
            "qualityFilter": {
                "max": 1,
                "min": 0,
                "thresholds": [30, 20, 10]
            }
        }],
        "granularity": {
            "type": "minutes",
            "value": 60
        },
        "times": [{
            "end": None,
            "start": "00:00:00.000"
        }],
        "tmcs":
        tmcs,
        "travelTimeUnits":
        "MINUTES",
        "uuid":
        str(uuid.uuid1())
    }
    # print('---------')
    # print(payload)
    # print('---------')
    # ----------------------------------------------------------
    response = requests.post(uri.format('jobs/export'),
                             params={'key': key},
                             json=payload)
    print('response status code:', response.status_code)

    if response.status_code == 200:  # Only if successful response

        # retry at intervals of 'step' until results return (status code = 200)
        results = polling.poll(
            lambda: requests.get(uri.format('jobs/export/results'),
                                 params={
                                     'key': key,
                                     'uuid': payload['uuid']
                                 }),
            check_success=lambda x: x.status_code == 200 and len(x.content
                                                                 ) > 0,
            step=5,
            timeout=300
            # poll_forever = True
        )
        print('results received')

        # Save results (binary zip file with one csv)
        with io.BytesIO() as f:
            f.write(results.content)
            f.seek(0)
            df = pd.read_csv(f, compression='zip')

        # zf_filename = payload['uuid'] +'.zip'
        # with open(zf_filename, 'wb') as f:
        #     f.write(results.content)
        # df = pd.read_csv(zf_filename)
        # os.remove(zf_filename)

    else:
        df = pd.DataFrame()

    print('{} records'.format(len(df)))

    return df


if __name__ == '__main__':

    with open('Monthly_Report_AWS.yaml') as yaml_file:
        cred = yaml.load(yaml_file, Loader=yaml.Loader)

    with open('Monthly_Report.yaml') as yaml_file:
        conf = yaml.load(yaml_file, Loader=yaml.Loader)

    # start_date is either the given day or the first day of the month
    start_date = conf['start_date']
    if start_date == 'yesterday':
        start_date = datetime.today() - timedelta(days=1)
    start_date = (start_date -
                  timedelta(days=(start_date.day - 1))).strftime('%Y-%m-%d')

    # end date is either the given date + 1 or today.
    # end_date is not included in the query results
    end_date = conf['end_date']
    if end_date == 'yesterday':
        end_date = datetime.today() - timedelta(days=1)
    end_date = (end_date + timedelta(days=1)).strftime('%Y-%m-%d')

    tmc_fn = 'tmc_routes.feather'
    tmc_df = feather.read_dataframe(tmc_fn)
    tmc_dict = tmc_df.groupby(['Corridor'])['tmc'].apply(list).to_dict()
    tmc_list = list(set(tmc_df.tmc.values))

    # start_date = '2019-07-01'
    # end_date = '2019-07-31'

    tt_df = get_tmc_data(start_date, end_date, tmc_list, cred['RITIS_KEY'], 0)

    if len(tt_df) > 0:
        df = (pd.merge(tmc_df[['tmc', 'miles', 'Corridor']],
                       tt_df,
                       left_on=['tmc'],
                       right_on=['tmc_code'
                                 ]).drop(columns=['tmc']).sort_values([
                                     'Corridor', 'tmc_code',
                                     'measurement_tstamp'
                                 ]))

        if len(df) > 0:
            df['reference_minutes'] = df['miles'] / df['reference_speed'] * 60
            df = df.reset_index(drop=True)
            yyyy_mm = datetime.strptime(start_date,
                                        '%Y-%m-%d').strftime('%Y-%m')
            fn = 'Inrix/For_Monthly_Report/tt_{}_TWTh.csv'.format(yyyy_mm)
            print(fn)

            # Write raw data to a zipped csv
            with io.StringIO() as buff:
                df.to_csv(buff)
                with zipfile.ZipFile(fn + '.zip',
                                     mode='w',
                                     compression=zipfile.ZIP_DEFLATED) as zf:
                    zf.writestr(os.path.basename(fn), buff.getvalue())

            # Summarize to tti, pti by hour and save to csv
            df_ = df.groupby(
                ['Corridor', 'measurement_tstamp'], as_index=False
            )['travel_time_minutes', 'reference_minutes'].sum()
            df_['Hour'] = pd.to_datetime(df_['measurement_tstamp'])
            df_['Hour'] = df_['Hour'] - pd.to_timedelta(df_['Hour'].dt.day - 1,
                                                        unit='days')

            desc = df_.groupby(['Corridor',
                                'Hour']).describe(percentiles=[0.90])

            tti = desc['travel_time_minutes']['mean'] / desc[
                'reference_minutes']['mean']
            pti = desc['travel_time_minutes']['90%'] / desc[
                'reference_minutes']['mean']

            summ_df = pd.DataFrame({'tti': tti, 'pti': pti})
            fn_ = fn.replace('_TWTh.csv', '_summary.csv')
            summ_df.to_csv(fn_)
