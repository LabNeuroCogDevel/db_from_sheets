#!/usr/bin/env bash

#
# get all calendar events since 2015
#
caltsv=all_gcal_events.tsv
[ ! -s  $caltsv ] && 
 gcalcli --calendar "Luna Lab" agenda --tsv 2015-01-01 $(date +%F) | sort -nr > $caltsv

max=$(sort -nr $caltsv |awk -F"\t" '{print $1;exit}')
echo querying from $max onward, applying to $(wc -l $caltsv)
(
 gcalcli --calendar "Luna Lab" agenda --tsv $max $(date +%F)
 cat $caltsv;
) | sort -nr|uniq  > $caltsv.tmp
mv $caltsv.tmp $caltsv

echo finished, now have $(wc -l $caltsv)

