# -*- coding: utf-8 -*-
"""
Created on Thu Jul 12 20:55:21 2018

@author: V0010894
"""

import pandas as pd
import sqlalchemy as sq
import os
import pyodbc

def get_atspm_detectors():

    if os.name=='nt':
        
        uid = os.environ['VDOT_ATSPM_USERNAME']
        pwd = os.environ['VDOT_ATSPM_PASSWORD']
        dsn = 'sqlodbc'
        
        engine = sq.create_engine('mssql+pyodbc://{}:{}@{}'.format(uid, pwd, dsn),
                                  pool_size=20)
    
    elif os.name=='posix':
        print(os.name)
        def connect():
            return pyodbc.connect(
                'Driver=FreeTDS;' + 
                'SERVER={};'.format(os.environ['VDOT_ATSPM_SERVER_INSTANCE']) +
                #'DATABASE={};'.format(os.environ['VDOT_ATSPM_DB']) +
                'PORT=1433;' +
                'UID={};'.format(os.environ['VDOT_ATSPM_USERNAME']) +
                'PWD={};'.format(os.environ['VDOT_ATSPM_PASSWORD']) +
                'TDS_Version=8.0;')
        
        engine = sq.create_engine('mssql://', creator=connect)
        
    with engine.connect() as conn:
        
        detectors = pd.read_sql_table('Detectors', con=conn)
        approaches = pd.read_sql_query("SELECT ApproachID, SignalID, DirectionTypeID, MPH, ProtectedPhaseNumber, IsProtectedPhaseOverlap, PermissivePhaseNumber FROM Approaches", con=conn)
        signals = pd.read_sql_table('Signals', con=conn)
    
    
    signals = signals[signals.SignalID != 'null']
    signals = signals[~signals.IPAddress.str.contains("^10\.10.")]
    
    df = (signals.set_index(['SignalID']).join(approaches.set_index(['SignalID']), how='outer')
            .sort_index())
    
    df = (df[~df.ApproachID.isna()]
            .assign(ApproachID = lambda x: x.ApproachID.astype('int64'))
            .reset_index())
    df_ = (df.set_index(['ApproachID']).join(detectors.set_index(['ApproachID']), how='outer')
             .assign(TimeFromStopBar = lambda x: x.DistanceFromStopBar/x.MPH*3600/5280)
             .rename(columns = {'DetChannel': 'Detector'}))
    
    return df_.reset_index()

def get_atspm_detectors_setback():
    
    df_ = get_atspm_detectors()[['SignalID','DetChannel','DistanceFromStopBar','MPH']]
    df_ = df_[~df_.DistanceFromStopBar.isna()]
    df_ = (df_.assign(TimeFromStopBar = lambda x: x.DistanceFromStopBar/x.MPH*3600/5280)
              .assign(DetChannel = lambda x: x.DetChannel.astype('int64'))
              .rename(columns = {'DetChannel': 'Detector'}))
    return df_
