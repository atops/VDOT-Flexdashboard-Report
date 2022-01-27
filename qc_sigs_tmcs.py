# -*- coding: utf-8 -*-
"""
Created on Tue Jan 25 14:56:42 2022

@author: Alan.Toppen
"""


import pandas as pd

tmc_corrs = pd.read_excel(r'c:\users\alan.toppen\Corridor_TMCs_Virginia.xlsx')[['Corridor', 'Subcorridor']]
sig_corrs = pd.read_excel(r'c:\users\alan.toppen\VDOT_Corridors_Latest.xlsx')[['Corridor', 'Subcorridor']]

tmc_corrs = tmc_corrs.drop_duplicates()
tmc_corrs['id'] = 'tmc'
sig_corrs = sig_corrs.drop_duplicates()
sig_corrs['id'] = 'sig'

df = pd.merge(sig_corrs, tmc_corrs, how='outer', on=['Corridor', 'Subcorridor'])
df[df.id_x.isna()]
df[df.id_y.isna()]
