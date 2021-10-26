# -*- coding: utf-8 -*-
"""
Created on Wed Nov 27 10:48:52 2019

@author: alan.toppen
"""

import pandas as pd
import sqlalchemy as sq
import os
from datetime import datetime
import boto3
import yaml

from get_atspm_detectors import get_atspm_detectors, get_atspm_ped_detectors
from get_included_detectors import get_included_detectors
from pull_atspm_data import get_atspm_engine


# get Good/Authoritative Detector Config (det_config) and write to feather
def get_det_config(ad, engine, date_string):
    # -- ---------------------------------
    # -- Included detectors --
    #  SignalID/Detector pairs with vol > 0 over the past year
    #  over a sample of dates between 8am-9am

    incld = get_included_detectors(engine).assign(index = 1)
    incld.reset_index().to_feather('included_detectors.feather')
    #incld = pd.read_feather('included_detectors.feather').set_index(['SignalID','Detector'])

    # -- -------------------------------------------------- --
    # -- ATSPM Detector Config (from reduce function above) --

    #adc = ad[['SignalID',
    #          'Detector',
    #          'ProtectedPhaseNumber',
    #          'PermissivePhaseNumber',
    #          'TimeFromStopBar',
    #          'IPAddress']]
    #adc = adc.rename(columns={'ProtectedPhaseNumber': 'ProtPhase',
    #                          'PermissivePhaseNumber': 'PermPhase'})
    #adc['CallPhase'] = np.where(adc.ProtPhase > 0, adc.ProtPhase, adc.PermPhase)

    adc = ad

    try:
        adc = adc[adc.SignalID != 'null']
    except:
        pass
    adc = adc[~adc.Detector.isna()]
    adc = adc[~adc.CallPhase.isna()]

    adc.SignalID = adc.SignalID.astype('int')
    adc.Detector = adc.Detector.astype('int')
    adc.CallPhase = adc.CallPhase.astype('int')

    adc = adc.set_index(['SignalID','Detector'])

    det_config = adc.join(incld).rename(columns={'Unnamed: 0': 'in_cel'}).sort_index()  # .join(other=mdp, how='outer', lsuffix='_atspm', rsuffix='_maxtime').sort_index()

    det_config.TimeFromStopBar = det_config.TimeFromStopBar.fillna(0).round(1)

    if 'CountPriority' in det_config.columns:
        det_config.CountPriority = det_config.CountPriority.fillna(max(det_config.CountPriority) + 1)

    det_config = det_config.reset_index()

    return det_config


def nightly_config(engine, date_):
    s3 = boto3.client('s3', verify=False)

    date_string = date_.strftime('%Y-%m-%d')

    print("ATSPM Vehicle Detectors [1 of 3]")
    ad = get_atspm_detectors(engine, date_)
    ad_csv_filename = 'ATSPM_Det_Config_{}.csv'.format(date_string)
    ad.to_csv(ad_csv_filename)

    # upload to s3
    key = 'atspm_det_config/date={}/ATSPM_Det_Config_{}.csv'.format(date_string, REGION)
    s3.upload_file(Filename=ad_csv_filename, Bucket=BUCKET, Key=key)
    os.remove(ad_csv_filename)

    print("ATSPM Vehicle Detector Config [2 of 3]")
    det_config = get_det_config(ad, engine, date_string)
    dc_filename = 'ATSPM_Det_Config_Good_{}.feather'.format(date_string)
    det_config.to_feather(dc_filename)

    # upload to s3
    key = 'atspm_det_config_good/date={}/ATSPM_Det_Config_Good_{}.feather'.format(date_string, REGION)
    s3.upload_file(Filename=dc_filename, Bucket=BUCKET, Key=key)
    os.remove(dc_filename)
    
    print("ATSPM Pedestrian Detectors [3 of 3]")
    ped_config = get_atspm_ped_detectors(engine, date_)
    pc_filename = 'ATSPM_Ped_Config_{}.feather'.format(date_string)
    ped_config.to_feather(pc_filename)

    # upload to s3
    key = 'atspm_ped_config/date={}/ATSPM_Ped_Config_{}.feather'.format(date_string, REGION)
    s3.upload_file(Filename=pc_filename, Bucket=BUCKET, Key=key)
    os.remove(pc_filename)
    

if __name__=='__main__':
   
    with open('Monthly_Report.yaml') as yaml_file:
        conf = yaml.load(yaml_file, Loader=yaml.Loader)

    with open('Monthly_Report_AWS.yaml') as yaml_file:
        cred = yaml.load(yaml_file, Loader=yaml.Loader)
    
    BUCKET = conf['bucket']
    REGION = conf['region']

        
    
    # engine = sq.create_engine(f'mssql+pyodbc://{uid}:{pwd}@{dsn}')
    engine = get_atspm_engine(cred['ATSPM_UID'], cred['ATSPM_PWD'], dsn = cred['ATSPM_DSN'])

    date_ = datetime.today()
    nightly_config(engine, date_)


    # Code to go back and calculate past days
    """
    dates = pd.date_range('2020-10-14', '2021-09-08', freq='1D')

    for date_ in dates:
        print(date_)
        nightly_config(engine, date_)
    """
    
