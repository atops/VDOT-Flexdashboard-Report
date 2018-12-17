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


# get Good/Authoritative Detector Config (det_config) and write to feather
def get_det_config(adc, date_string):
    # -- ---------------------------------
    # -- Included detectors --
    #  SignalID/Detector pairs with vol > 0 over the past year
    #  over a sample of dates between 8am-9am
    incld = (pd.read_csv('included_detectors.csv')
               .sort_values(['SignalID', 'Detector'])
               .set_index(['SignalID', 'Detector']))

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

    det_config = det_config.reset_index()[['SignalID',
                                       'Detector',
                                       'CallPhase',
                                       'TimeFromStopBar',
                                       #'CallPhase_atspm',
                                       #'CallPhase_maxtime',
                                       'in_cel']]
    return det_config

#def get_ped_config()
#
#    with

s3 = boto3.client('s3')

date_string = datetime.today().strftime('%Y-%m-%d')



print("ATSPM Vehicle Detectors [1 of 3]")
ad = get_atspm_detectors()
ad_csv_filename = 'ATSPM_Det_Config_{}.csv'.format(date_string)
ad.to_csv(ad_csv_filename)

# upload to s3
#key = 'atspm_det_config/date={}/ATSPM_Det_Config.csv'.format(date_string)
#s3.upload_file(Filename=ad_csv_filename,
#               Bucket='gdot-devices',
#               Key=key)





print("ATSPM Vehicle Detector Config [2 of 3]")
det_config = get_det_config(ad, date_string)
dc_filename = 'ATSPM_Det_Config_Good_{}.feather'.format(date_string)
det_config.to_feather(dc_filename)

# upload to s3
#key = 'atspm_det_config_good/date={}/ATSPM_Det_Config_Good.feather'.format(date_string)
#s3.upload_file(Filename=dc_filename,
#               Bucket='gdot-devices',
#               Key=key)

print("ATSPM Pedestrian Detectors [3 of 3]")
ped_config = get_atspm_ped_detectors()
pc_filename = 'ATSPM_Ped_Config_{}.feather'.format(date_string)
feather.write_dataframe(ped_config, pc_filename)
"""
# Code to go back and calculate past days

mvdp = reduce_det_plans()
ad = get_atspm_detectors()
det_config = get_det_config(ad, mvdp, date_string)


dates = pd.date_range('2018-08-01', '2018-11-11', freq='1D')

for date_string in [d.strftime('%Y-%m-%d') for d in dates]:

    ad = pd.read_csv('ATSPM_Det_Config_{}.csv'.format(date_string))
    mvdp = pd.read_csv('MaxTime_Det_Plans_{}.csv'.format(date_string))

    det_config = get_det_config(ad, mvdp, date_string)
    dc_filename = 'ATSPM_Det_Config_Good_{}.feather'.format(date_string)
    det_config.to_feather(dc_filename)
"""
