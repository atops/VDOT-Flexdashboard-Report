# -*- coding: utf-8 -*-
"""
Created on Thu Aug  2 12:02:05 2018

@author: V0010894
"""

import pandas as pd
import numpy as np
from datetime import datetime
import boto3
import feather

# from get_maxtime_config_asyncio_polling import get_veh_det_plans, get_unit_info
# from get_maxtime_config_reduce import reduce_det_plans, reduce_unit_info
from get_atspm_detectors import get_atspm_detectors, get_atspm_ped_detectors
from get_included_detectors import get_included_detectors

# get Good/Authoritative Detector Config (det_config) and write to feather
def get_det_config(ad, date_string):
    # -- ---------------------------------
    # -- Included detectors --
    #  SignalID/Detector pairs with vol > 0 over the past year
    #  over a sample of dates between 8am-9am

    #incld = get_included_detectors().assign(index = 1)
    #feather.write_dataframe(incld.reset_index(), 'included_detectors.feather')
    incld = feather.read_dataframe('included_detectors.feather').set_index(['SignalID','Detector'])

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

    # Exclude those not in "included detectors"
    #adc = adc.join(incld)
    #adc = adc[~adc['index'].isna()]

    # -- --------------------------------------------------- --
    # -- Maxtime Detector Plans (from reduce function above) --

    #    mdp = mvdp[['SignalID','Detector','CallPhase','IP']]
    #
    #    mdp.SignalID = mdp.SignalID.astype('int')
    #    mdp.Detector = mdp.Detector.astype('int')
    #    mdp.CallPhase = mdp.CallPhase.astype('int')
    #
    #    mdp = mdp.set_index(['SignalID','Detector'])
    #
    #    # Exclude those not in "included detectors"
    #    #mdp = mdp.join(incld).rename(columns={'index': 'in_cel'})
    #    mdp = pd.merge(mdp, incld, how='outer', left_index=True, right_index=True)\
    #            .rename(columns={'index': 'in_cel'})
    #    mdp = mdp[~mdp['in_cel'].isna()]

    # -- --------------------------------------------------------- --
    # -- Combine ATSPM Detector Config and MaxTime Detector Config --

    #    det_config = pd.merge(adc, mdp, how='outer',
    #                          left_index=True, right_index=True,
    #                          suffixes=('_atspm', '_maxtime')).sort_index()

    det_config = adc.join(incld).rename(columns={'Unnamed: 0': 'in_cel'}).sort_index()  # .join(other=mdp, how='outer', lsuffix='_atspm', rsuffix='_maxtime').sort_index()

    det_config.TimeFromStopBar = det_config.TimeFromStopBar.fillna(0).round(1)

    if 'CountPriority' in det_config.columns:
        det_config.CountPriority = det_config.CountPriority.fillna(max(det_config.CountPriority) + 1)

    #det_config = det_config.reset_index()[['SignalID',
    #                                       'Detector',
    #                                       'CallPhase',
    #                                       'TimeFromStopBar',
    #                                       'CallPhase_atspm',
    #                                       'CallPhase_maxtime',
    #                                       'in_cel']]
    det_config = det_config.reset_index()

    return det_config

# get Good/Authoritative Detector Config (det_config) and write to feather
def get_det_config_older(adc, date_string):
    # -- ---------------------------------
    # -- Included detectors --
    #  SignalID/Detector pairs with vol > 0 over the past year
    #  over a sample of dates between 8am-9am
    incld = (pd.read_csv('included_detectors.csv')
               .sort_values(['SignalID', 'Detector'])
               .set_index(['SignalID', 'Detector']))
    # incld = get_included_detectors().assign(index = 1)

    # -- -------------------------------------------------- --
    # -- ATSPM Detector Config (from reduce function above) --

    adc = ad[['SignalID',
              'Detector',
              'ProtectedPhaseNumber',
              'PermissivePhaseNumber',
              'TimeFromStopBar',
              'IPAddress']]
    adc = adc.rename(columns={'ProtectedPhaseNumber': 'ProtPhase',
                              'PermissivePhaseNumber': 'PermPhase'})
    adc['CallPhase'] = np.where(adc.ProtPhase > 0, adc.ProtPhase, adc.PermPhase)

    try:
        adc = adc[adc.SignalID != 'null']
    except:
        pass
    adc = adc[~adc.Detector.isna()]
    adc = adc[~adc.CallPhase.isna()]

    adc.SignalID = adc.SignalID.astype('int')
    adc.Detector = adc.Detector.astype('int')
    adc.CallPhase = adc.CallPhase.astype('int')

    adc = adc.set_index(['SignalID', 'Detector'])

    # Exclude those not in "included detectors"
    # adc = adc.join(incld)
    # adc = adc[~adc['Unnamed: 0'].isna()].drop(columns=['Unnamed: 0'])

    # -- --------------------------------------------------- --
    # -- Maxtime Detector Plans (from reduce function above) --
    """
    mdp = mvdp[['SignalID','Detector','CallPhase']]

    mdp.SignalID = mdp.SignalID.astype('int')
    mdp.Detector = mdp.Detector.astype('int')
    mdp.CallPhase = mdp.CallPhase.astype('int')

    mdp = mdp.set_index(['SignalID','Detector'])

    # Exclude those not in "included detectors"
    mdp = mdp.join(incld).rename(columns={'Unnamed: 0': 'in_cel'})
    mdp = mdp[~mdp['in_cel'].isna()]
    """
    det_config = adc.join(incld).rename(columns={'Unnamed: 0': 'in_cel'}).sort_index()  # .join(other=mdp, how='outer', lsuffix='_atspm', rsuffix='_maxtime').sort_index()

    det_config.TimeFromStopBar = det_config.TimeFromStopBar.fillna(0).round(1)
    # det_config['CallPhase'] = np.where(det_config.CallPhase_maxtime.isna(),
    #                                    det_config.CallPhase_atspm,
    #                                    det_config.CallPhase_maxtime).astype('int')

    # det_config[((~det_config.CallPhase_maxtime.isna()) & (~det_config.in_cel.isna()))]

    # det_config = det_config[((~det_config.CallPhase_atspm.isna()) & (~det_config.CallPhase_maxtime.isna())) | ((~det_config.CallPhase_maxtime.isna()) & (~det_config.in_cel.isna()))]
    # -- --------------------------------------------------------- --
    # -- Combine ATSPM Detector Config and MaxTime Detector Config --

    det_config = det_config.reset_index()

    #det_config = det_config.reset_index()[['SignalID',
    #                                   'Detector',
    #                                   'CallPhase',
    #                                   'TimeFromStopBar',
    #                                   #'CallPhase_atspm',
    #                                   #'CallPhase_maxtime',
    #                                   'in_cel']]

    return det_config

#def get_ped_config()
#
#    with

if __name__=='__main__':
    s3 = boto3.client('s3')


for date_ in pd.date_range('2018-05-01', '2018-12-31'):
    date_string = date_.strftime('%Y-%m-%d')



    print(date_string)
    ad = get_atspm_detectors(date_)
    ad_csv_filename = 'ATSPM_Det_Config_{}.csv'.format(date_string)
    ad.to_csv(ad_csv_filename)



    det_config = get_det_config(ad, date_string)
    dc_filename = 'ATSPM_Det_Config_Good_{}.feather'.format(date_string)
    det_config.to_feather(dc_filename)
