#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# put lunaid on scored self report (ASR only)
# 20171023 for BTC

import pandas, numpy
import glob

## read sheet, make initials column
asrlog = pandas.read_excel("sheets/PET.xlsx",sheetname="ASR-YSR Log")
idlookup = asrlog[['Luna ID','ASR/YSR ADM ID']]
# throws warning, it's okay
idlookup['initials1'] =  \
        [ a['First Name'][0] + a['Last Name'][0] for i,a in asrlog.iterrows() ]


## read in all scored files
def fchar(c):
    return([' ' if pandas.isna(x) else x[0]  for x in c])

files=glob.glob('/Volumes/L/bea_res/PET-fMRI/Screening/*SR/*/*_scored.CSV')
master = pandas.concat([ pandas.read_csv(f) for f in files ] ).\
         assign(finit= fchar(master.firstname),
                linit=fchar(master.lastname)).\
         assign(initials2=master.finit+master.linit)
master['initials2'] = [ m['firstname'][0] + m['lastname'][0] for i,m in master.iterrows()]

## merge
d=pandas.merge(idlookup,master,left_on='ASR/YSR ADM ID',right_on='id')

d.to_csv('asr_ysr.csv')

## check: same number of rows, now mismatched initials
d.shape[0] == master.shape[0]
len(numpy.where(d['initials1'] != d['initials2'])[0])==0

## end (for cellmode)

