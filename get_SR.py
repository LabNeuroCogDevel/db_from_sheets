#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# put lunaid on scored self report (ASR only)
# 20171023 for BTC

import pandas
import numpy
import glob


# ## read in all scored files
def fchar(c):
    return([' ' if pandas.isna(x) else x[0] for x in c])


# ## read sheet, make initials column
# initially from
# /Volumes/L/bea_res/PET-fMRI/PET-fMRI_participation_flow.xlsx
asrlog = pandas.read_excel("sheets/PET.xlsx", sheet_name="ASR-YSR Log")
FL_id = [a['First Name'][0] + a['Last Name'][0] for i, a in asrlog.iterrows()]
idlookup = asrlog[['Luna ID', 'ASR/YSR ADM ID']].\
           assign(FL_id=FL_id)
idlookup = idlookup[idlookup['Luna ID'] != 'xxx']

# read ain all scored CSV files
files = glob.glob('/Volumes/L/bea_res/PET-fMRI/Screening/*SR/*/*_scored.CSV')
print(f"reading {len(files)} files")
all_csv = pandas.concat([pandas.read_csv(f) for f in files])
main = all_csv.\
       assign(finit=lambda d: fchar(d.firstname),
              linit=lambda d: fchar(d.lastname)).\
       assign(FL_score=lambda d: d.finit + d.linit)
# main['FL_score'] = [m['firstname'][0] + m['lastname'][0]
#                        for i, m in main.iterrows()]
print(f"have {main.shape[0]} rows for {idlookup.shape[0]} id entries")

# ## merge
d = pandas.merge(idlookup, main, left_on='ASR/YSR ADM ID', right_on='id')

# ## check: same number of rows, now mismatched initials
bad = d['FL_id'] != d['FL_score']
print(f"sanity checks: Name Initials dont match for {len(numpy.where(bad)[0])}")
badids = d[bad]['Luna ID'].values
badsr = d[bad]['ASR/YSR ADM ID'].values
inlog = asrlog[[x in badsr for x in asrlog['ASR/YSR ADM ID']]][['Luna ID','ASR/YSR ADM ID','First Name', 'Last Name']]
incsv = all_csv[[x in badsr for x in all_csv['id']]][['id','firstname', 'lastname']]
#print(d[bad][['Luna ID','ASR/YSR ADM ID', 'FL_id', 'FL_score']])
print(inlog)
print(incsv)

# assert d.shape[0] == main.shape[0]
# assert len(numpy.where(d['FL_id'] != d['FL_score'])[0]) == 0

print(f"writting asr_ysr.csv")
name_columns = ["firstname", "lastname", "middlename",
                "othername", "finit", "linit", "FL_score", "FL_id"]
d.drop(columns=name_columns).to_csv('asr_ysr.csv')

# ## end (for cellmode)
