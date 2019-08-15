# -*- coding: utf-8 -*-
"""
Created on Thu Jul 12 20:55:21 2018

@author: V0010894
"""

import pandas as pd
import sqlalchemy as sq
import os
import pyodbc


# Updated version that gets detector priority for counts
def get_atspm_detectors(date=None):

    if os.name == 'nt':

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

        detectiontypedetectors = pd.read_sql_table('DetectionTypeDetector', con=conn)
        detectiontypes = pd.read_sql_table('DetectionTypes', con=conn)
        lanetypes = pd.read_sql_table('LaneTypes', con=conn)
        detectionhardwares = pd.read_sql_table('DetectionHardwares', con=conn)
        movementtypes = pd.read_sql_table('MovementTypes', con=conn)
        directiontypes = pd.read_sql_table('DirectionTypes', con=conn)

        detectors = pd.read_sql_table('Detectors', con=conn)
        approaches = pd.read_sql_table("Approaches", con=conn)
        signals = pd.read_sql_table('Signals', con=conn)

    # Combine detection types tables
    detectiontypes2 = (detectiontypedetectors
         .merge(detectiontypes, on=['DetectionTypeID'])
         .drop(columns=['DetectionTypeID'])
         .pivot_table(index=['ID'], values=['Description'], aggfunc=lambda x: list(x))
         .rename(columns={'Description': 'DetectionTypeDesc'})
         .reset_index())

    # Rename reused field names (Description, Abbreviation)
    approaches = approaches.rename(columns={'Description': 'ApproachDesc'})
    movementtypes = movementtypes.rename(columns={'Description': 'MovementTypeDesc',
                                                  'Abbreviation': 'MovementTypeAbbr'})
    lanetypes = lanetypes.rename(columns={'Description': 'LaneTypeDesc',
                                          'Abbreviation': 'LaneTypeAbbr'})
    directiontypes = directiontypes.rename(columns={'Description': 'DirectionTypeDesc',
                                                    'Abbreviation': 'DirectionTypeAbbr'})

    detectionhardwares = detectionhardwares.rename(columns={'ID': 'DetectionHardwareID'})

    # Get config for a given date, if supplied
    if date is not None:
        detectors = detectors[(detectors.DateAdded <= date) & ((detectors.DateDisabled > date) | detectors.DateDisabled.isna())]
        signals = signals[signals.Start <= date]

    # Drop all but the latest version
    detectors = detectors.sort_values(['DetectorID','DateAdded','ID'])\
        .drop_duplicates(['DetectorID'], keep='last')

    signals = signals.sort_values(['SignalID','VersionID'])\
        .drop_duplicates(['SignalID'], keep='last')


    # Big merge
    df =(detectors.merge(movementtypes, on=['MovementTypeID'])
              .merge(detectionhardwares, on=['DetectionHardwareID'])
              .merge(lanetypes, on=['LaneTypeID'])
              .merge(detectiontypes2, on=['ID'])
              .merge(approaches, on=['ApproachID'])
              .merge(directiontypes, on=['DirectionTypeID'])
              .merge(signals, on=['SignalID'])
              .drop(columns=['MovementTypeID',
                             'LaneTypeID',
                             'DetectionHardwareID',
                             'ApproachID',
                             'DirectionTypeID',
                             'DisplayOrder_x',
                             'DisplayOrder_y',
                             'VersionID_x',
                             'VersionID_y',
                             'VersionActionId']))


    # Calculate time from stop bar. Assumed distance and MPH are entered
    df['TimeFromStopBar'] = df.DistanceFromStopBar/df.MPH*3600/5280
    df = df.rename(columns = {'DetChannel': 'Detector'})

    df['CallPhase'] = 0
    df.loc[df['MovementTypeAbbr'] == 'L', 'CallPhase'] = df.loc[df['MovementTypeAbbr'] == 'L', 'ProtectedPhaseNumber']
    df.loc[df['MovementTypeAbbr'] == 'T', 'CallPhase'] = df.loc[df['MovementTypeAbbr'] == 'T', 'ProtectedPhaseNumber']
    #df.loc[(df['MovementTypeAbbr'] == 'TR') & (df['PermissivePhaseNumber'] > 0) & (~df['PermissivePhaseNumber'].isna()), 'CallPhase'] = df.loc[df['MovementTypeAbbr'] == 'TR', 'PermissivePhaseNumber']
    #df.loc[(df['MovementTypeAbbr'] == 'TR') & ((df['PermissivePhaseNumber'] == 0) | df['PermissivePhaseNumber'].isna()), 'CallPhase'] = df.loc[df['MovementTypeAbbr'] == 'TR', 'ProtectedPhaseNumber']
    df.loc[(df['MovementTypeAbbr'] == 'TR') & (df['PermissivePhaseNumber'] > 0) & (~df['PermissivePhaseNumber'].isna()), 'CallPhase'] = df.loc[(df['MovementTypeAbbr'] == 'TR') & (df['PermissivePhaseNumber'] > 0) & (~df['PermissivePhaseNumber'].isna()), 'PermissivePhaseNumber']
    df.loc[(df['MovementTypeAbbr'] == 'TR') & ((df['PermissivePhaseNumber'] == 0) | df['PermissivePhaseNumber'].isna()), 'CallPhase'] = df.loc[(df['MovementTypeAbbr'] == 'TR') & ((df['PermissivePhaseNumber'] == 0) | df['PermissivePhaseNumber'].isna()), 'ProtectedPhaseNumber']
    df.loc[df['MovementTypeAbbr'] == 'R', 'CallPhase'] = df.loc[df['MovementTypeAbbr'] == 'R', 'ProtectedPhaseNumber']
    df.loc[df['MovementTypeAbbr'] == 'TL', 'CallPhase'] = df.loc[df['MovementTypeAbbr'] == 'TL', 'ProtectedPhaseNumber']

    df.DetectionTypeDesc = df.DetectionTypeDesc.astype('str')

    df['CountPriority'] = 3
    df.loc[(df.LaneTypeDesc == 'Vehicle') & (df.DetectionTypeDesc.str.contains('Lane-by-lane Count')), 'CountPriority'] = 2
    df.loc[(df.LaneTypeDesc == 'Vehicle') & (df.DetectionTypeDesc.str.contains('Advanced Count')), 'CountPriority'] = 1
    df.loc[df.LaneTypeDesc == 'Exit', 'CountPriority'] = 0

    return df


def get_atspm_ped_detectors():

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

        query = """SELECT Distinct SignalID, EventParam AS Detector
                   FROM Controller_Event_Log
                   WHERE Timestamp > '2018-01-01'
                   AND EventCode = 90
                   ORDER BY SignalID, EventParam"""

        df = pd.read_sql_query(query, con=conn)

    df['CallPhase'] = df['Detector']

    return df.reset_index()

def get_atspm_detectors_setback():

    df_ = get_atspm_detectors()[['SignalID','DetChannel','DistanceFromStopBar','MPH']]
    df_ = df_[~df_.DistanceFromStopBar.isna()]
    df_ = (df_.assign(TimeFromStopBar = lambda x: x.DistanceFromStopBar/x.MPH*3600/5280)
              .assign(DetChannel = lambda x: x.DetChannel.astype('int64'))
              .rename(columns = {'DetChannel': 'Detector'}))
    return df_

if __name__=='__main__':

    df = get_atspm_ped_detectors()
    print(df)
