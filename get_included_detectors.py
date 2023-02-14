# -*- coding: utf-8 -*-
"""
Created on Wed Nov 27 10:49:04 2019

@author: alan.toppen
"""

import pandas as pd
import os
import yaml
import pyodbc
import sqlalchemy as sq
from datetime import datetime, timedelta
import time



def get_incld(conn, query, date_):
    t0 = time.time()

    sd = date_.strftime('%Y-%m-%d')
    ed = sd #(date_ + pd.DateOffset(days=1)).strftime('%Y-%m-%d')

    print(sd, end=': ')

    df = pd.read_sql(sql=query.format(sd, ed), con=conn)
    #df.to_csv('included_detectors_{}.csv'.format(sd))

    print('{} sec'.format(time.time() - t0))
    return df



def get_included_detectors(engine, date_string):

    #with open('Monthly_Report.yaml') as yaml_file:
    #    conf = yaml.load(yaml_file, yaml.Loader)

    end_date = datetime.strptime(date_string, '%Y-%m-%d')
    start_date = (end_date - timedelta(days=365)).strftime('%Y-%m-%d')
    end_date = end_date.strftime('%Y-%m-%d')

    dates = pd.date_range(start_date, end_date, freq='3D')

    query = """SELECT DISTINCT SignalID, EventParam as Detector
               FROM Controller_Event_Log WHERE EventCode = 82
               AND Timestamp between '{} 08:00:00' and '{} 09:00:00';
               """

    with engine.connect() as conn:

        dfs = [get_incld(conn, query, date_) for date_ in dates]
        df = pd.concat(dfs)[['SignalID','Detector']].drop_duplicates()

        df = df.sort_values(['SignalID','Detector'])\
               .set_index(['SignalID','Detector'])

        return df
