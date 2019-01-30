#!/usr/bin/env bash
set -e
trap 'e=$?;[ $e -ne 0 ]&&echo "$0 exited with error $e ($(pwd))" >&2' EXIT

# 20170301WF - get data sheets as listed by sheets_pass.list

lookup="sheets_pass.list"
[ ! -r "$lookup" ] && echo 'no xlsx export/password lookup file' >&2 && exit 1

[ ! -d sheets ] && mkdir sheets
while read outf inf pass; do
 if [ -n "$pass" ]; then
   java -jar ./xlsxunpass.jar $inf sheets/$outf.xlsx $pass
 else
   cp $inf $outf.xlsx
 fi
done < $lookup

# 7T google packet
curl 'https://docs.google.com/spreadsheets/d/e/2PACX-1vTBsSDjJ27hO6nOFyyyHPlLnDCms3dLWgw92dVkeue7UB4o1wZ9tMMe1Z-EA1ZM1g16pQW4HiCb62gu/pub?output=xlsx' > sheets/7T_packet.xlsx
# mMR_PET PACKET
curl 'https://docs.google.com/spreadsheets/d/e/2PACX-1vT32ku-_l9DKLSYi8lHdeWCFolyjZy8V5NaXbYdb7vY6WWA9JFyo-9_XeCB4NvSofN3EaPEWwru51PU/pub?output=xlsx' > sheets/PETSheets.xlsx 
# Project 304 (PET) SubjectList
curl 'https://docs.google.com/spreadsheets/d/e/2PACX-1vSDBHMjYWXED7zRzw88AoiJho1Tt7rnDnnFVvE4sg8i-pfhhoOAECmI5KGvLUCAbeHYrYuux5p1eLjU/pub?output=xlsx' > sheets/PET_idref.xlsx 
# P5Sz_idlookup
curl 'https://docs.google.com/spreadsheets/d/e/2PACX-1vRlGXLx9ZqvQUBZ_koEExYZ94iKZ0hnXFdC75s4cYQNMSwVvFMtd_JluYuOL4h_H124amTFf3RcUZXP/pub?output=xlsx' > sheets/P5_CB_megref.xlsx
